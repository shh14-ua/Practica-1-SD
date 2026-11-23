#!/usr/bin/env python3
"""
Pruebas del bloque 4 (Release 2): cifrado y autenticación de todos los mensajes Kafka entre
WM_Central y WM_WS_E (ws_commands y ws_events).

Tipo de prueba: criptografía REAL (Fernet de la librería 'cryptography'); Kafka SIMULADO
(no hay broker disponible): se capturan los mensajes que se publicarían y se entregan
directamente a los consumidores reales de Central y del Engine.

Comprueba:
  1. Formato: en Kafka solo viajan {"ws", "enc"}; ningún dato del mensaje en claro.
  2. Rechazo de mensajes en claro, manipulados, con otra clave, de otra WS o de otro sentido.
  3. Solo Fernet: sin 'cryptography' no se cifra ni se recurre a algoritmos propios.
  4. Engine: órdenes repetidas/antiguas rechazadas (también tras reiniciar con la misma clave),
     órdenes con clave revocada rechazadas tras CLEAR_KEY, sin clave no riega ni publica en claro,
     FINISHED pendiente reenviado cifrado al recibir una clave nueva.
  5. Central: órdenes cifradas, sin clave no se envían, eventos repetidos o de claves anteriores
     rechazados, reintento tras fallo de persistencia y auditoría limitada en frecuencia.
"""

import json
import os
import shutil
import stat
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT_DIR, os.path.join(ROOT_DIR, "WM_Central"), os.path.join(ROOT_DIR, "WM_WS")):
    if p not in sys.path:
        sys.path.insert(0, p)

import wm_crypto
from wm_crypto import (
    generate_symmetric_key, seal_kafka_message, open_kafka_message, next_kafka_seq,
    KAFKA_KIND_COMMAND, KAFKA_KIND_EVENT, CryptoUnavailableError,
)
import WM_Central
import WM_WS_E
from common import TOPIC_COMMANDS, TOPIC_EVENTS

WS = "WS-01"


def sealed_cmd(key, cmd, seq=None, ws=WS, **extra):
    return seal_kafka_message(key, KAFKA_KIND_COMMAND, ws, {"cmd": cmd, **extra},
                              seq if seq is not None else next_kafka_seq(0))


def sealed_evt(key, ev_type, seq=None, ws=WS, **extra):
    return seal_kafka_message(key, KAFKA_KIND_EVENT, ws, {"type": ev_type, **extra},
                              seq if seq is not None else next_kafka_seq(0))


class ScriptedConsumer:
    """Consumidor Kafka simulado que entrega una lista de mensajes y detiene al dueño."""

    def __init__(self, messages, owner):
        self.messages, self.owner = messages, owner

    def __iter__(self):
        for m in self.messages:
            yield type("Msg", (), {"value": m})()
        self.owner.running = False

    def commit(self):
        pass


# ==============================================================================
# 1-3. FORMATO, AUTENTICACIÓN Y DEPENDENCIA CRIPTOGRÁFICA
# ==============================================================================
class KafkaMessageFormatTests(unittest.TestCase):
    def setUp(self):
        self.key = generate_symmetric_key()

    def test_01_only_ws_and_ciphertext_travel_in_clear(self):
        msg = sealed_cmd(self.key, "START", op="FO-07", duration=15, transaction_id="TX-SECRETO-1")
        self.assertEqual(set(msg), {"ws", "enc"})
        raw = json.dumps(msg)
        for secret in ("START", "FO-07", "TX-SECRETO-1", "duration", "cmd", "seq"):
            self.assertNotIn(secret, raw)
        inner, reason = open_kafka_message(self.key, msg, KAFKA_KIND_COMMAND, WS)
        self.assertEqual(reason, "")
        self.assertEqual((inner["cmd"], inner["op"], inner["transaction_id"]), ("START", "FO-07", "TX-SECRETO-1"))

    def test_02_rejections(self):
        msg = sealed_cmd(self.key, "STOP")
        cases = {
            "en claro": ({"ws": WS, "cmd": "STOP"}, "NO_CIFRADO"),
            "manipulado": (dict(msg, enc=msg["enc"][:-6] + ("A" if msg["enc"][-6] != "A" else "B") + msg["enc"][-5:]),
                           "CLAVE_O_INTEGRIDAD"),
            "otra WS (visible)": (dict(msg, ws="WS-02"), "OTRA_WS"),
            "otra WS (interna)": (dict(sealed_cmd(self.key, "STOP", ws="WS-02"), ws=WS), "OTRA_WS"),
        }
        for name, (m, expected) in cases.items():
            inner, reason = open_kafka_message(self.key, m, KAFKA_KIND_COMMAND, WS)
            self.assertIsNone(inner, name)
            self.assertEqual(reason, expected, name)
        self.assertEqual(open_kafka_message(generate_symmetric_key(), msg, KAFKA_KIND_COMMAND, WS)[1],
                         "CLAVE_O_INTEGRIDAD", "clave distinta")
        self.assertEqual(open_kafka_message(self.key, msg, KAFKA_KIND_EVENT, WS)[1], "TIPO",
                         "una orden no puede aceptarse como evento")

    def test_03_fernet_only_without_custom_fallback(self):
        for name in ("_xor_crypt", "_derive_keys", "_keystream_block"):
            self.assertFalse(hasattr(wm_crypto, name), f"No debe existir el algoritmo propio {name}")
        token = wm_crypto.encrypt_message(self.key, "hola")[4:]
        from cryptography.fernet import Fernet
        self.assertEqual(Fernet(self.key.encode()).decrypt(token.encode()), b"hola")  # token Fernet estándar
        with patch.object(wm_crypto, "_HAS_CRYPTOGRAPHY", False):
            with self.assertRaises(CryptoUnavailableError):
                wm_crypto.require_cryptography()
            with self.assertRaises(CryptoUnavailableError):
                wm_crypto.encrypt_message(self.key, "hola")
            with self.assertRaises(CryptoUnavailableError):
                wm_crypto.decrypt_message(self.key, "ENC:" + token)
        self.assertFalse(wm_crypto.is_valid_symmetric_key("corta"))
        with self.assertRaises(ValueError):
            wm_crypto.encrypt_message("clave-no-valida", "hola")


# ==============================================================================
# 4. ENGINE
# ==============================================================================
class EngineKafkaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_engine_state_")
        self.sent = []
        self.patches = [
            patch.object(WM_WS_E, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_WS_E, "send_kafka", lambda prod, topic, msg, **k: self.sent.append((topic, msg)) or True),
        ]
        for p in self.patches:
            p.start()
        self.key = generate_symmetric_key()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _engine(self, with_key=True):
        e = WM_WS_E.WSEngine("fake:9092", "127.0.0.1:1", state_dir=self.tmp)
        e.ws_id, e.monitor_connected, e.central_connected = WS, True, True
        if with_key:
            self.assertEqual(e.handle_monitor_request(f"SET_KEY#{self.key}"), "OK")
        return e

    def _consume(self, engine, messages):
        engine.running = True
        with patch.object(WM_WS_E, "make_consumer", lambda *a, **k: ScriptedConsumer(messages, engine)):
            engine.run_commands_consumer()
        engine.running = True

    def test_04_engine_events_are_encrypted_and_authenticated(self):
        e = self._engine()
        self.assertTrue(e.send_event("TELEMETRY", flow=9.5, vol=1.2, op="FO-01", transaction_id="TX-1"))
        topic, msg = self.sent[-1]
        self.assertEqual(topic, TOPIC_EVENTS)
        self.assertEqual(set(msg), {"ws", "enc"})
        self.assertNotIn("TX-1", json.dumps(msg))
        inner, _ = open_kafka_message(self.key, msg, KAFKA_KIND_EVENT, WS)
        self.assertEqual((inner["type"], inner["vol"]), ("TELEMETRY", 1.2))
        e.send_event("TELEMETRY", flow=9.6, vol=1.4)
        second, _ = open_kafka_message(self.key, self.sent[-1][1], KAFKA_KIND_EVENT, WS)
        self.assertGreater(second["seq"], inner["seq"])

    def test_05_valid_command_accepted_replays_and_forgeries_rejected(self):
        e = self._engine()
        start = sealed_cmd(self.key, "START", op="FO-01", duration=0, transaction_id="TX-OK")
        self._consume(e, [start])
        self.assertEqual(e.state, "WATERING")
        self.assertEqual(e.current_tx_id, "TX-OK")
        e.finish_watering("MANUAL")
        self.assertEqual(e.state, "AVAILABLE")

        old = sealed_cmd(self.key, "START", seq=1, op="FO-02", duration=0)  # seq antiguo
        rejected = [
            start,                                                     # repetida
            old,
            {"ws": WS, "cmd": "START", "op": "FO-03", "duration": 0},    # en claro
            sealed_cmd(generate_symmetric_key(), "START", duration=0),  # otra clave
            dict(sealed_cmd(self.key, "START", ws="WS-02", duration=0), ws=WS),  # para otra WS
            seal_kafka_message(self.key, KAFKA_KIND_EVENT, WS, {"cmd": "START", "duration": 0},
                               next_kafka_seq(0)),                     # sentido contrario
        ]
        self._consume(e, rejected)
        self.assertEqual(e.state, "AVAILABLE", "Ninguna orden no válida debe iniciar un riego")

    def test_06_replay_rejected_after_engine_restart_with_same_key(self):
        e1 = self._engine()
        block = sealed_cmd(self.key, "BLOCK")
        self._consume(e1, [block])
        self.assertEqual(e1.state, "BLOCKED")
        state_file = os.path.join(self.tmp, f"{WS}.json")
        self.assertEqual(stat.S_IMODE(os.stat(state_file).st_mode), 0o600)
        with open(state_file) as f:
            content = f.read()
        self.assertNotIn(self.key, content, "El fichero de estado no debe contener la clave")

        # Reinicio del Engine: misma clave (el Monitor sigue conectado a Central)
        e2 = self._engine()
        self.assertEqual(e2.state, "AVAILABLE")
        self._consume(e2, [block])
        self.assertEqual(e2.state, "AVAILABLE", "La orden repetida no debe aceptarse tras reiniciar")
        self._consume(e2, [sealed_cmd(self.key, "BLOCK")])
        self.assertEqual(e2.state, "BLOCKED", "Una orden nueva sí debe aceptarse")

        # Con una clave nueva el contador empieza de cero (las órdenes de la clave anterior ya no descifran)
        new_key = generate_symmetric_key()
        e2.handle_monitor_request(f"SET_KEY#{new_key}")
        self.assertEqual(e2._last_cmd_seq, 0)

    def test_07_clear_key_rejects_revoked_key_commands_and_blocks_watering(self):
        e = self._engine()
        captured_start = sealed_cmd(self.key, "START", op="FO-01", duration=0)
        captured_activate = sealed_cmd(self.key, "ACTIVATE")
        self.assertEqual(e.handle_monitor_request("CLEAR_KEY"), "OK")
        self.assertIsNone(e.symmetric_key)

        self._consume(e, [captured_activate, captured_start])
        self.assertEqual(e.state, "AVAILABLE")
        self.assertIsNone(e.current_tx_id, "Sin clave no se acepta ninguna orden")

        e.start_watering("MANUAL", 0)
        self.assertNotEqual(e.state, "WATERING", "Sin clave tampoco se admite el riego manual")

        # Nueva autenticación: clave nueva; las órdenes con la clave revocada siguen rechazándose
        new_key = generate_symmetric_key()
        e.handle_monitor_request(f"SET_KEY#{new_key}")
        self._consume(e, [captured_start])
        self.assertNotEqual(e.state, "WATERING")
        self._consume(e, [seal_kafka_message(new_key, KAFKA_KIND_COMMAND, WS,
                                             {"cmd": "START", "op": "FO-01", "duration": 0}, next_kafka_seq(0))])
        self.assertEqual(e.state, "WATERING")

    def test_08_without_key_nothing_in_clear_finished_kept_and_flushed_with_new_key(self):
        e = self._engine()
        e.start_watering("FO-01", 0, transaction_id="TX-PEND")
        time.sleep(0.2)
        before = len(self.sent)
        e.handle_monitor_request("CLEAR_KEY")        # revocación o baja durante el riego: lo detiene
        self.assertNotEqual(e.state, "WATERING")
        self.assertFalse(e.send_event("TELEMETRY", flow=1.0, vol=0.1))
        deadline = time.time() + 3                   # el FINISHED se publica en segundo plano
        while time.time() < deadline and not e.pending_finished_events:
            time.sleep(0.05)
        self.assertEqual(len(self.sent), before, "Sin clave no se publica nada en Kafka")
        self.assertEqual([(p["transaction_id"], p["reason"]) for p in e.pending_finished_events],
                         [("TX-PEND", "KEYS_CLEARED")])

        new_key = generate_symmetric_key()
        e.handle_monitor_request(f"SET_KEY#{new_key}")  # la clave nueva dispara el reenvío de pendientes
        deadline = time.time() + 3
        while time.time() < deadline and e.pending_finished_events:
            time.sleep(0.05)
        inner, _ = open_kafka_message(new_key, self.sent[-1][1], KAFKA_KIND_EVENT, WS)
        self.assertEqual((inner["type"], inner["transaction_id"]), ("FINISHED", "TX-PEND"))
        self.assertEqual(e.pending_finished_events, [])

    def test_09_invalid_key_from_monitor_is_ignored(self):
        e = self._engine(with_key=False)
        self.assertEqual(e.handle_monitor_request("SET_KEY#no-es-una-clave"), "KO")
        self.assertIsNone(e.symmetric_key)


# ==============================================================================
# 5. CENTRAL
# ==============================================================================
class CentralKafkaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_central_kafka_")
        self.sent = []
        self.patches = [
            patch.object(WM_Central, "ensure_topics", lambda *a, **k: None),
            patch.object(WM_Central, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_Central, "send_kafka", lambda prod, topic, msg, **k: self.sent.append((topic, msg)) or True),
        ]
        for p in self.patches:
            p.start()
        self.central = WM_Central.WMCentral(socket_port=0, kafka_bootstrap="fake:9092", web_port=0,
                                            db_path=os.path.join(self.tmp, "central.db"))
        self.key = generate_symmetric_key()
        self.central.station_keys[WS] = self.key

    def tearDown(self):
        self.central.running = False
        for p in reversed(self.patches):
            p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _cmds(self):
        return [m for t, m in self.sent if t == TOPIC_COMMANDS]

    def _audits(self, action):
        return [a for a in self.central.recent_audits if a["action"] == action]

    def test_10_commands_encrypted_and_not_sent_without_key(self):
        self.assertTrue(self.central.send_ws_command(WS, "START", op="FO-01", duration=5, transaction_id="TX-9"))
        msg = self._cmds()[-1]
        self.assertEqual(set(msg), {"ws", "enc"})
        inner, _ = open_kafka_message(self.key, msg, KAFKA_KIND_COMMAND, WS)
        self.assertEqual((inner["cmd"], inner["transaction_id"]), ("START", "TX-9"))
        self.central.send_ws_command(WS, "STOP", reason="LEAK")
        second, _ = open_kafka_message(self.key, self._cmds()[-1], KAFKA_KIND_COMMAND, WS)
        self.assertGreater(second["seq"], inner["seq"])

        self.central.revoke_station_keys(WS)
        n = len(self._cmds())
        self.assertFalse(self.central.send_ws_command(WS, "ACTIVATE"))
        self.assertEqual(len(self._cmds()), n, "Sin clave vigente no se publica la orden")
        self.assertTrue(self._audits("KAFKA_CMD_NOT_SENT"))

    def test_11_events_validated_replays_and_old_keys_rejected(self):
        handled = []
        with patch.object(self.central, "handle_ws_event", side_effect=lambda ev: handled.append(ev)):
            ok = sealed_evt(self.key, "TELEMETRY", flow=9.0, vol=0.5)
            self.central._process_kafka_event(ok)
            self.central._process_kafka_event(ok)                                   # repetido
            self.central._process_kafka_event({"ws": WS, "type": "FINISHED", "vol": 99})  # en claro
            self.central._process_kafka_event(sealed_evt(generate_symmetric_key(), "FINISHED"))  # otra clave
            self.central._process_kafka_event(sealed_cmd(self.key, "START"))       # sentido contrario
            self.assertEqual([h["type"] for h in handled], ["TELEMETRY"])

            # Revocación: los eventos con la clave revocada se rechazan
            captured = sealed_evt(self.key, "FINISHED", transaction_id="TX-OLD")
            self.central.revoke_station_keys(WS)
            self.central._process_kafka_event(captured)
            # Nueva clave (re-autenticación): la clave anterior sigue sin valer
            new_key = generate_symmetric_key()
            self.central.station_keys[WS] = new_key
            self.central._process_kafka_event(captured)
            self.assertEqual(len(handled), 1)
            self.central._process_kafka_event(sealed_evt(new_key, "FINISHED", transaction_id="TX-NEW"))
            self.assertEqual(handled[-1]["transaction_id"], "TX-NEW")
        self.assertTrue(self._audits("KAFKA_REJECTED"))

    def test_12_seq_updated_only_after_successful_processing(self):
        ev = sealed_evt(self.key, "TELEMETRY", flow=1.0, vol=0.1)
        calls = {"n": 0}

        def flaky(_ev):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("fallo temporal")

        with patch.object(self.central, "handle_ws_event", side_effect=flaky):
            with self.assertRaises(RuntimeError):
                self.central._process_kafka_event(ev)
            self.central._process_kafka_event(ev)   # reintento del mismo mensaje (offset no confirmado)
            self.central._process_kafka_event(ev)   # ya procesado: repetido
        self.assertEqual(calls["n"], 2)

    def test_13_rejection_audit_is_rate_limited(self):
        bad_key = generate_symmetric_key()
        for _ in range(50):
            self.central._process_kafka_event(sealed_evt(bad_key, "TELEMETRY"))
        rejected = self._audits("KAFKA_REJECTED")
        self.assertEqual(len(rejected), 1, "50 rechazos seguidos deben generar un único registro")
        # Pasado el intervalo, el siguiente registro indica cuántos se omitieron
        for k in self.central._kafka_reject_audit:
            self.central._kafka_reject_audit[k][0] -= 31
        self.central._process_kafka_event(sealed_evt(bad_key, "TELEMETRY"))
        self.assertIn("49 mensajes similares omitidos", self._audits("KAFKA_REJECTED")[-1]["details"])

    def test_14_unknown_station_names_do_not_grow_rate_limit_table(self):
        for i in range(30):
            self.central._process_kafka_event({"ws": f"WS-X{i}", "enc": "ENC:basura"})
        self.assertLessEqual(len(self.central._kafka_reject_audit), 1)



# ==============================================================================
# 6. CIERRE DEL BLOQUE 4: CAMPOS EXTRA, DESFASE DE RELOJ Y CONFIRMACIÓN DE FINISHED
# ==============================================================================
class Block4ClosingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_b4_")
        self.sent = []
        sink = lambda prod, topic, msg, **k: self.sent.append((topic, msg)) or True
        self.patches = [
            patch.object(WM_WS_E, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_WS_E, "send_kafka", sink),
            patch.object(WM_Central, "ensure_topics", lambda *a, **k: None),
            patch.object(WM_Central, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_Central, "send_kafka", sink),
        ]
        for p in self.patches:
            p.start()
        self.key = generate_symmetric_key()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _engine(self):
        e = WM_WS_E.WSEngine("fake:9092", "127.0.0.1:1", state_dir=self.tmp)
        e.ws_id, e.monitor_connected, e.central_connected = WS, True, True
        e.handle_monitor_request(f"SET_KEY#{self.key}")
        return e

    def _central(self):
        c = WM_Central.WMCentral(socket_port=0, kafka_bootstrap="fake:9092", web_port=0,
                                 db_path=os.path.join(self.tmp, "central.db"))
        c.station_keys[WS] = self.key
        return c

    def _consume(self, engine, messages):
        engine.running = True
        with patch.object(WM_WS_E, "make_consumer", lambda *a, **k: ScriptedConsumer(messages, engine)):
            engine.run_commands_consumer()
        engine.running = True

    def _sent_events(self, key):
        out = []
        for topic, m in self.sent:
            if topic == TOPIC_EVENTS:
                inner, _ = open_kafka_message(key, m, KAFKA_KIND_EVENT, WS)
                if inner:
                    out.append(inner)
        return out

    def _sent_commands(self, key):
        return [open_kafka_message(key, m, KAFKA_KIND_COMMAND, WS)[0] for t, m in self.sent if t == TOPIC_COMMANDS]

    def test_15_extra_outer_fields_rejected(self):
        msg = sealed_cmd(self.key, "BLOCK")
        self.assertEqual(open_kafka_message(self.key, dict(msg, timestamp=1.0), KAFKA_KIND_COMMAND, WS)[1],
                         "CAMPOS_EXTRA")
        e = self._engine()
        self._consume(e, [dict(msg, op="FO-01")])
        self.assertEqual(e.state, "AVAILABLE", "El Engine no debe aceptar órdenes con campos en claro adicionales")
        c = self._central()
        handled = []
        with patch.object(c, "handle_ws_event", side_effect=handled.append):
            c._process_kafka_event(dict(sealed_evt(self.key, "TELEMETRY"), vol=5.0))
        self.assertEqual(handled, [])
        self.assertIn("CAMPOS_EXTRA", c.recent_audits[-1]["details"])

    def test_16_clock_skew_between_machines_does_not_matter(self):
        """Solo se comparan números del mismo emisor: un reloj 120 s atrasado o adelantado es indiferente."""
        e = self._engine()
        behind = time.time_ns() - 120 * 10**9           # Central (nube) 2 minutos por detrás del laboratorio
        self._consume(e, [sealed_cmd(self.key, "BLOCK", seq=behind)])
        self.assertEqual(e.state, "BLOCKED")
        self._consume(e, [sealed_cmd(self.key, "ACTIVATE", seq=behind + 1)])
        self.assertEqual(e.state, "AVAILABLE")
        c = self._central()
        handled = []
        ahead = time.time_ns() + 120 * 10**9            # Engine 2 minutos por delante de Central
        with patch.object(c, "handle_ws_event", side_effect=handled.append):
            c._process_kafka_event(sealed_evt(self.key, "TELEMETRY", seq=ahead))
            c._process_kafka_event(sealed_evt(self.key, "TELEMETRY", seq=ahead + 1))
        self.assertEqual(len(handled), 2)

    def test_17_engine_keeps_finished_until_central_confirms(self):
        e = self._engine()
        summary = {"op": "FO-01", "reason": "TIME", "vol": 1.0, "dur": 5, "transaction_id": "TX-A"}
        self.assertTrue(e.send_event("FINISHED", **summary))
        self.assertIn("TX-A", e._unconfirmed_finished)
        self._consume(e, [sealed_cmd(self.key, "ACK_FINISHED", transaction_id="TX-A")])
        self.assertNotIn("TX-A", e._unconfirmed_finished, "La confirmación de Central lo libera")

        # Sin confirmación y con cambio de clave (re-autenticación): se reenvía con la clave nueva
        e.send_event("FINISHED", **dict(summary, transaction_id="TX-B"))
        new_key = generate_symmetric_key()
        e.handle_monitor_request(f"SET_KEY#{new_key}")
        deadline = time.time() + 3
        while time.time() < deadline and not any(ev.get("transaction_id") == "TX-B" for ev in self._sent_events(new_key)):
            time.sleep(0.05)
        resent = [ev for ev in self._sent_events(new_key) if ev.get("transaction_id") == "TX-B"]
        self.assertEqual(len(resent), 1, "El FINISHED no confirmado debe reenviarse cifrado con la clave nueva")
        self.assertIn("TX-B", e._unconfirmed_finished, "Sigue conservándose hasta la confirmación")
        self.assertEqual(e.pending_finished_events, [])

        # Revocación (CLEAR_KEY): pasa a pendientes, no se publica sin clave y se reenvía con la siguiente
        e.handle_monitor_request("CLEAR_KEY")
        self.assertEqual([p["transaction_id"] for p in e.pending_finished_events], ["TX-B"])
        third_key = generate_symmetric_key()
        e.handle_monitor_request(f"SET_KEY#{third_key}")
        deadline = time.time() + 3
        while time.time() < deadline and e.pending_finished_events:
            time.sleep(0.05)
        self.assertEqual([ev["transaction_id"] for ev in self._sent_events(third_key)], ["TX-B"])
        self._consume(e, [sealed_cmd(third_key, "ACK_FINISHED", transaction_id="TX-B")])
        self.assertEqual(e._unconfirmed_finished, {})

    def test_18_central_confirms_and_stale_finished_never_alters_new_watering(self):
        c = self._central()
        with c.lock:
            st = c.stations[WS]
            st.update(status="REGANDO", transaction_id="TX-NEW", operator="FO-02", pending_start=False)
        stale = {"type": "FINISHED", "ws": WS, "op": "FO-01", "vol": 2.0, "dur": 9,
                 "reason": "TIME", "transaction_id": "TX-OLD"}
        for _ in range(2):  # tardío y después duplicado
            c.handle_ws_event(dict(stale))
            self.assertEqual((st["status"], st["transaction_id"], st["operator"]), ("REGANDO", "TX-NEW", "FO-02"))
        with c._get_db() as db:
            n = db.execute("SELECT COUNT(*) FROM watering_history WHERE transaction_id='TX-OLD'").fetchone()[0]
        self.assertEqual(n, 1)
        acks = [m for m in self._sent_commands(self.key) if m["cmd"] == "ACK_FINISHED"]
        self.assertEqual([a["transaction_id"] for a in acks], ["TX-OLD", "TX-OLD"])

        c.handle_ws_event(dict(stale, transaction_id="TX-NEW", op="FO-02"))
        self.assertEqual((st["status"], st["transaction_id"]), ("DISPONIBLE", None))
        self.assertEqual(self._sent_commands(self.key)[-1]["transaction_id"], "TX-NEW")
        c.running = False


if __name__ == "__main__":
    unittest.main(verbosity=2)
