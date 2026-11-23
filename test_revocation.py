#!/usr/bin/env python3
"""
Pruebas del bloque 5 (Release 2): revocación de claves y recuperación de estaciones en WM_Central.

Tipo de prueba: WM_Central real con SQLite real y servidor HTTP real (API_Central); Kafka
SIMULADO (se capturan los mensajes publicados). Los flujos completos con Monitor y Engine
(revocación con el Monitor desconectado, reinicio del Monitor y de Central, riego interrumpido)
están en test_watering_flow.py (test_24 a test_27).

Comprueba:
  1. Revocación con un START pendiente: el operario recibe una denegación clara y no llega
     después el mensaje engañoso de "el Engine no confirmó en 10 segundos".
  2. El bloqueo de CENTRAL previo o durante la revocación se guarda en SQLite y se recupera tras
     reiniciar Central; la alerta climática se evalúa al re-autenticarse con las alertas vigentes.
  3. API_Central informa del estado real de revocación (keys_revoked) aunque la auditoría avance,
     y no expone huellas de credenciales.
  4. Antes de borrar la clave, Central publica una única orden BLOCK cifrada con ella (si existe).
  5. Engine real: tanto si llega antes el BLOCK de Kafka como si llega antes CLEAR_KEY del Monitor,
     el riego se detiene (en el segundo caso, por el propio CLEAR_KEY) y la estación queda bloqueada
     (HEALTH BLOCKED); el FINISHED queda pendiente para reenviarse con la clave nueva.
  6. Engine real: CLEAR_KEY durante un riego responde sin esperar a Kafka (aunque esté bloqueado),
     cierra la válvula (KEYS_CLEARED), conserva el FINISHED y rechaza órdenes y riegos nuevos.
"""

import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT_DIR, os.path.join(ROOT_DIR, "WM_Central"), os.path.join(ROOT_DIR, "WM_WS")):
    if p not in sys.path:
        sys.path.insert(0, p)

import WM_Central
import WM_WS_E
from common import TOPIC_REPLIES, TOPIC_COMMANDS, STATUS_OUT_OF_SERVICE, STATUS_AVAILABLE
from wm_crypto import (generate_symmetric_key, open_kafka_message, seal_kafka_message, next_kafka_seq,
                       KAFKA_KIND_COMMAND)

WS = "WS-01"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class CentralRevocationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_revocation_")
        self.db = os.path.join(self.tmp, "central.db")
        self.sent = []
        self.patches = [
            patch.object(WM_Central, "ensure_topics", lambda *a, **k: None),
            patch.object(WM_Central, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_Central, "send_kafka", lambda prod, topic, msg, **k: self.sent.append((topic, msg)) or True),
        ]
        for p in self.patches:
            p.start()
        self.centrals = []

    def tearDown(self):
        for c in self.centrals:
            c.running = False
            if getattr(c, "_web_server", None) is not None:
                c._web_server.shutdown()
                c._web_server.server_close()
        for p in reversed(self.patches):
            p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _central(self, web_port=0):
        c = WM_Central.WMCentral(socket_port=0, kafka_bootstrap="fake:9092", web_port=web_port, db_path=self.db)
        self.centrals.append(c)
        return c

    def _replies(self):
        return [m for t, m in self.sent if t == TOPIC_REPLIES and m.get("ws") == WS]

    def test_01_revocation_with_pending_start_denies_once_without_misleading_timeout(self):
        c = self._central()
        c.station_keys[WS] = generate_symmetric_key()
        with c.lock:
            st = c.stations[WS]
            st.update(status=STATUS_AVAILABLE, pending_start=True, pending_since=123.0,
                      operator="FO-01", transaction_id="TX-PEND")
        self.assertTrue(c.revoke_station_keys(WS))
        denies = [m for m in self._replies() if m.get("type") == "DENY"]
        self.assertEqual(len(denies), 1)
        self.assertIn("revocadas", denies[0]["msg"])
        self.assertEqual((st["pending_start"], st["operator"], st["transaction_id"]), (False, None, None))

        with patch("time.sleep", return_value=None):
            c.check_watering_timeout(WS, 123.0)
        self.assertEqual(len([m for m in self._replies() if m.get("type") == "DENY"]), 1,
                         "No debe llegar después el aviso de que el Engine no confirmó")

    def test_02_previous_blocks_survive_restart_and_weather_is_evaluated_on_reauth(self):
        c = self._central()
        with c.lock:
            c.stations[WS]["status"] = STATUS_AVAILABLE
            c.stations["WS-02"]["status"] = STATUS_AVAILABLE
        # WS-01: alerta climática durante la revocación. WS-02: bloqueo de CENTRAL durante la revocación
        c.revoke_station_keys(WS)
        c.handle_weather_alert(WS, "Oslo", -5.0, "Snow", True, "Helada")
        c.revoke_station_keys("WS-02")
        c.block_station("WS-02")
        self.assertTrue(c._keep_oos_after_reauth(WS, c.stations[WS].get("central_block", False)),
                        "Con la alerta vigente seguiría fuera de servicio al re-autenticarse")
        c.running = False

        c2 = self._central()  # reinicio de Central (las alertas climáticas no se guardan: WM_W las renueva)
        for ws in (WS, "WS-02"):
            self.assertEqual(c2.stations[ws]["status"], STATUS_OUT_OF_SERVICE)
            self.assertTrue(c2.stations[ws].get("keys_revoked"))
        self.assertFalse(c2._keep_oos_after_reauth(WS, c2.stations[WS].get("central_block", False)),
                         "Sin alerta vigente (cancelada) vuelve a servicio al re-autenticarse")
        c2.handle_weather_alert(WS, "Oslo", -6.0, "Snow", True, "Helada")  # WM_W renueva la alerta
        self.assertTrue(c2._keep_oos_after_reauth(WS, c2.stations[WS].get("central_block", False)))
        c2.handle_weather_alert(WS, "Oslo", 10.0, "Clear", False, "Normal")
        self.assertEqual(c2.stations[WS]["status"], STATUS_OUT_OF_SERVICE, "Sigue revocada: el clima no la restaura")
        self.assertFalse(c2._keep_oos_after_reauth(WS, c2.stations[WS].get("central_block", False)))
        self.assertTrue(c2._keep_oos_after_reauth("WS-02", c2.stations["WS-02"].get("central_block", False)),
                        "El bloqueo de CENTRAL se conserva tras reiniciar")

    def test_03_api_reports_real_revocation_state(self):
        port = free_port()
        c = self._central(web_port=port)
        threading.Thread(target=c.run_web_server, daemon=True).start()
        c._last_cred_fp[WS] = "f" * 64
        c.station_keys[WS] = generate_symmetric_key()
        c.revoke_station_keys(WS)
        for i in range(40):  # la auditoría avanza mucho más allá de las últimas 25 entradas
            c.audit_event(WS, "127.0.0.1", "PRUEBA", f"evento {i}")

        deadline = time.time() + 5
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=2) as r:
                    data = json.loads(r.read().decode("utf-8"))
                break
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.1)
        ws = next(s for s in data["stations"] if s["id"] == WS)
        self.assertTrue(ws["keys_revoked"])
        self.assertFalse(ws["has_key"])
        self.assertNotIn("KEY_REVOCATION", [a["action"] for a in data["audits"]])
        self.assertNotIn("f" * 64, json.dumps(data), "La API no debe exponer huellas de credenciales")
        other = next(s for s in data["stations"] if s["id"] != WS)
        self.assertFalse(other["keys_revoked"])

    def test_04_block_sent_with_current_key_before_deleting_it(self):
        c = self._central()
        key = generate_symmetric_key()
        c.station_keys[WS] = key
        self.assertTrue(c.revoke_station_keys(WS))
        cmds = [m for t, m in self.sent if t == TOPIC_COMMANDS]
        self.assertEqual(len(cmds), 1)
        inner, _ = open_kafka_message(key, cmds[0], KAFKA_KIND_COMMAND, WS)
        self.assertEqual((inner["cmd"], inner["reason"]), ("BLOCK", "KEYS_REVOKED"))
        self.assertNotIn(WS, c.station_keys)
        # Ya revocada (sin clave): ni una segunda revocación ni otras órdenes publican nada
        c.revoke_station_keys(WS)
        c.send_ws_command(WS, "ACTIVATE")
        self.assertEqual(len([m for t, m in self.sent if t == TOPIC_COMMANDS]), 1)

    def test_05_clear_key_and_kafka_block_in_either_order_stop_the_engine(self):
        for order in ("block_first", "clear_key_first"):
            with self.subTest(order=order):
                with patch.object(WM_WS_E, "make_producer", lambda *a, **k: "FAKE"), \
                     patch.object(WM_WS_E, "send_kafka", lambda *a, **k: True):
                    e = WM_WS_E.WSEngine("fake:9092", "127.0.0.1:1", state_dir=self.tmp)
                    e.ws_id, e.monitor_connected, e.central_connected = WS, True, True
                    key = generate_symmetric_key()
                    e.handle_monitor_request(f"SET_KEY#{key}")
                    e.start_watering("FO-01", 0, transaction_id=f"TX-{order}")
                    time.sleep(0.2)
                    block = seal_kafka_message(key, KAFKA_KIND_COMMAND, WS,
                                               {"cmd": "BLOCK", "reason": "KEYS_REVOKED"}, next_kafka_seq(0))

                    def deliver_block():
                        e.running = True
                        msgs = [block]

                        class OneShot:
                            def __iter__(self_inner):
                                yield type("Msg", (), {"value": msgs[0]})()
                                e.running = False

                            def commit(self_inner):
                                pass
                        with patch.object(WM_WS_E, "make_consumer", lambda *a, **k: OneShot()):
                            e.run_commands_consumer()
                        e.running = True

                    if order == "block_first":
                        deliver_block()
                        self.assertEqual(e.state, "BLOCKED", "El BLOCK de Kafka detiene el riego")
                        e.handle_monitor_request("CLEAR_KEY")
                    else:
                        e.handle_monitor_request("CLEAR_KEY")
                        self.assertNotEqual(e.state, "WATERING", "CLEAR_KEY detiene el riego de inmediato")
                        deliver_block()  # rechazado: el Engine ya no tiene clave
                        self.assertEqual(e.state, "AVAILABLE")
                        # El Monitor que envió CLEAR_KEY también recibió REVOKED: HEALTH BLOCKED a continuación
                        self.assertEqual(e.handle_monitor_request("HEALTH#NO_CENTRAL#BLOCKED"), "OK")
                        self.assertEqual(e.state, "BLOCKED", "Queda fuera de servicio por la vía del Monitor")
                    deadline = time.time() + 3
                    while time.time() < deadline and not e.pending_finished_events:
                        time.sleep(0.05)
                    self.assertEqual([p["transaction_id"] for p in e.pending_finished_events], [f"TX-{order}"])
                    e.running = False

    def test_06_clear_key_stops_watering_without_waiting_for_kafka(self):
        kafka_release = threading.Event()

        def blocked_kafka(*a, **k):          # Kafka bloqueado: cada envío tarda hasta que se libera
            kafka_release.wait(5)
            return True

        with patch.object(WM_WS_E, "make_producer", lambda *a, **k: "FAKE"), \
             patch.object(WM_WS_E, "send_kafka", blocked_kafka):
            e = WM_WS_E.WSEngine("fake:9092", "127.0.0.1:1", state_dir=self.tmp)
            e.ws_id, e.monitor_connected, e.central_connected = WS, True, True
            key = generate_symmetric_key()
            e.handle_monitor_request(f"SET_KEY#{key}")
            e.start_watering("FO-01", 0, transaction_id="TX-BAJA")
            time.sleep(1.3)                      # STARTED y telemetría retenidos en Kafka

            t0 = time.monotonic()
            self.assertEqual(e.handle_monitor_request("CLEAR_KEY"), "OK")
            self.assertLess(time.monotonic() - t0, 0.5, "CLEAR_KEY no debe esperar a Kafka")
            self.assertEqual(e.state, "AVAILABLE", "La válvula se cierra al recibir CLEAR_KEY")
            self.assertIsNone(e.symmetric_key)

            kafka_release.set()
            deadline = time.time() + 5
            while time.time() < deadline and not e.pending_finished_events:
                time.sleep(0.05)
            self.assertEqual([(p["transaction_id"], p["reason"]) for p in e.pending_finished_events],
                             [("TX-BAJA", "KEYS_CLEARED")])
            self.assertIsNone(e._open_command(seal_kafka_message(
                key, KAFKA_KIND_COMMAND, WS, {"cmd": "START", "op": "FO-02", "duration": 0}, next_kafka_seq(0))))
            e.start_watering("MANUAL", 0)
            self.assertNotEqual(e.state, "WATERING", "Sin clave no se admiten riegos nuevos")
            e.running = False


if __name__ == "__main__":
    unittest.main(verbosity=2)
