#!/usr/bin/env python3
"""
Pruebas automatizadas locales para WaterManagement Release 1.
Verifican sin depender de servidores AWS:
  1. Recorrido completo de solicitud de riego: FO -> Kafka -> CENTRAL -> Kafka -> Engine/Monitor -> CENTRAL -> FO.
  2. Regresión de desfase de reloj entre máquinas (AWS vs PCs de laboratorio).
  3. Resiliencia ante mensajes corruptos / vacíos en Kafka (_safe_json_loads).
  4. Parser de comandos interactivos en WM_FO (variantes de 'r WS-01 10').
  5. Tolerancia a fallos exigida por la rúbrica:
     - Caída del Monitor (WM_WS_M) durante el riego y bloqueo de riego manual ("Averiada").
     - Caída / avería del Engine (WM_WS_E) durante el riego conservando el volumen regado.
     - Caída y recuperación del Operario (WM_FO) recibiendo el SUMMARY pendiente gracias al group_id determinista.
     - Caída de WM_Central ("Imposible conectar con WM_Central").
"""

import io
import os
import queue
import socket
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for subdir in ("WM_Central", "WM_WS", "WM_FO"):
    mod_path = os.path.join(ROOT_DIR, subdir)
    if mod_path not in sys.path:
        sys.path.insert(0, mod_path)

import common
import WM_Central
import WM_FO
import WM_WS_E
import WM_WS_M


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeKafkaMessage:
    def __init__(self, topic: str, value):
        self.topic = topic
        self.value = value


class FakeKafkaBroker:
    """Broker Kafka en memoria thread-safe con soporte de grupos de consumidores y offsets persistidos."""

    def __init__(self):
        self.lock = threading.RLock()
        self.topics = {}  # topic -> list of messages
        self.group_offsets = {}  # (group_id, topic) -> int
        self.consumers = []
        self.clock_skew_central = 0.0  # Segundos a restar al timestamp de CENTRAL

    def send(self, topic: str, msg: dict) -> bool:
        if isinstance(msg, dict) and self.clock_skew_central != 0.0:
            # Simular desfase de reloj en mensajes emitidos por CENTRAL
            if topic in (common.TOPIC_COMMANDS, common.TOPIC_REPLIES):
                msg = dict(msg)
                msg["timestamp"] = time.time() - self.clock_skew_central
        with self.lock:
            self.topics.setdefault(topic, []).append(msg)
        return True

    def make_consumer(self, bootstrap: str, topics, group_prefix: str = "wm",
                      auto_offset_reset: str = "latest", max_retries: int = 15,
                      group_id: str = None):
        gid = group_id if group_id else f"{group_prefix}-{int(time.time() * 1000000)}-{ len(self.consumers) }"
        consumer = FakeKafkaConsumer(self, topics, gid, auto_offset_reset)
        with self.lock:
            self.consumers.append(consumer)
        return consumer

    def stop_all(self):
        with self.lock:
            for c in self.consumers:
                c.active = False


class FakeKafkaConsumer:
    def __init__(self, broker: FakeKafkaBroker, topics, group_id: str, auto_offset_reset: str):
        self.broker = broker
        self.topics = list(topics)
        self.group_id = group_id
        self.active = True

        with self.broker.lock:
            for t in self.topics:
                key = (self.group_id, t)
                if key not in self.broker.group_offsets:
                    if auto_offset_reset == "earliest":
                        self.broker.group_offsets[key] = 0
                    else:
                        self.broker.group_offsets[key] = len(self.broker.topics.get(t, []))

    def __iter__(self):
        while self.active:
            delivered = False
            with self.broker.lock:
                for t in self.topics:
                    key = (self.group_id, t)
                    msgs = self.broker.topics.get(t, [])
                    idx = self.broker.group_offsets.get(key, 0)
                    if idx < len(msgs):
                        val = msgs[idx]
                        self.broker.group_offsets[key] = idx + 1
                        delivered = True
                        yield FakeKafkaMessage(t, val)
                        break
            if not delivered:
                time.sleep(0.02)


class WaterManagementFlowTests(unittest.TestCase):
    def setUp(self):
        self.broker = FakeKafkaBroker()
        self.active_instances = []
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db.close()

        # Parchear funciones Kafka en todos los módulos
        self.patches = [
            patch.object(common, "ensure_topics", lambda *a, **kw: None),
            patch.object(common, "make_producer", lambda *a, **kw: "FAKE_PRODUCER"),
            patch.object(common, "send_kafka", lambda prod, topic, msg, **kw: self.broker.send(topic, msg)),
            patch.object(common, "make_consumer", self.broker.make_consumer),
            patch.object(WM_Central, "ensure_topics", lambda *a, **kw: None),
            patch.object(WM_Central, "make_producer", lambda *a, **kw: "FAKE_PRODUCER"),
            patch.object(WM_Central, "send_kafka", lambda prod, topic, msg, **kw: self.broker.send(topic, msg)),
            patch.object(WM_Central, "make_consumer", self.broker.make_consumer),
            patch.object(WM_WS_E, "make_producer", lambda *a, **kw: "FAKE_PRODUCER"),
            patch.object(WM_WS_E, "send_kafka", lambda prod, topic, msg, **kw: self.broker.send(topic, msg)),
            patch.object(WM_WS_E, "make_consumer", self.broker.make_consumer),
            patch.object(WM_FO, "make_producer", lambda *a, **kw: "FAKE_PRODUCER"),
            patch.object(WM_FO, "send_kafka", lambda prod, topic, msg, **kw: self.broker.send(topic, msg)),
            patch.object(WM_FO, "make_consumer", self.broker.make_consumer),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for inst in self.active_instances:
            inst.running = False
        self.broker.stop_all()
        time.sleep(0.05)
        for p in reversed(self.patches):
            p.stop()
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)

    def _start_cluster(self, ws_id="WS-01"):
        central_port = get_free_port()
        engine_port = get_free_port()

        central = WM_Central.WMCentral(
            socket_port=central_port,
            kafka_bootstrap="localhost:9092",
            web_port=0,
            db_path=self.temp_db.name
        )
        self.active_instances.append(central)
        threading.Thread(target=central.run_socket_server, daemon=True).start()
        threading.Thread(target=central.run_kafka_requests_consumer, daemon=True).start()
        threading.Thread(target=central.run_kafka_events_consumer, daemon=True).start()
        threading.Thread(target=central.run_kafka_status_publisher, daemon=True).start()

        monitor = WM_WS_M.WSMonitor(
            engine_port=engine_port,
            central_addr=f"127.0.0.1:{central_port}",
            ws_id=ws_id,
            location="River Park"
        )
        self.active_instances.append(monitor)
        threading.Thread(target=monitor.run, daemon=True).start()

        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr=f"127.0.0.1:{engine_port}"
        )
        self.active_instances.append(engine)
        threading.Thread(target=engine.link_monitor, daemon=True).start()
        assigned = engine.assigned_event.wait(timeout=5.0)
        self.assertTrue(assigned, "El Monitor no asignó ID al Engine a tiempo")
        threading.Thread(target=engine.run_commands_consumer, daemon=True).start()
        time.sleep(0.1)

        # Esperar a que WS-01 pase a DISPONIBLE en CENTRAL
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if central.stations.get(ws_id, {}).get("status") == common.STATUS_AVAILABLE:
                break
            time.sleep(0.05)
        self.assertEqual(
            central.stations[ws_id]["status"],
            common.STATUS_AVAILABLE,
            "La estación WS-01 debería estar en estado DISPONIBLE tras conectar Monitor y Engine"
        )
        return central, monitor, engine

    def test_1_full_watering_flow_and_clock_skew(self):
        """Prueba el ciclo completo FO -> CENTRAL -> Engine -> CENTRAL -> FO incluso con 120s de desfase de reloj."""
        self.broker.clock_skew_central = 120.0  # Simula que el reloj de AWS va 2 minutos por detrás del laboratorio
        central, monitor, engine = self._start_cluster("WS-01")

        fo = WM_FO.FieldOperator("localhost:9092", "FO-01")
        threading.Thread(target=fo.listen_kafka, daemon=True).start()
        time.sleep(0.1)

        # Ejecutar solicitud de riego de 2 segundos (equivale a 'r WS-01 2')
        fo.request_service("WS-01", 2)

        # Verificar que el acuse de CENTRAL llega y el servicio termina
        self.assertTrue(fo.central_ack_event.wait(timeout=3.0), "FO-01 no recibió acuse de WM_Central")
        self.assertTrue(fo.service_done_event.wait(timeout=5.0), "El ciclo de riego en FO-01 no se completó")

        # Verificar que WS-01 volvió a DISPONIBLE y el riego quedó registrado en SQLite
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)
        with central._get_db() as db:
            rows = db.execute("SELECT * FROM watering_history WHERE ws_id = 'WS-01'").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["operator_id"], "FO-01")
        self.assertEqual(rows[0]["reason"], "TIME")
        self.assertGreater(rows[0]["total_vol"], 0.0)

        fo.running = False
        engine.running = False
        monitor.running = False
        central.running = False

    def test_2_safe_json_deserializer_and_fo_command_parser(self):
        """Prueba que _safe_json_loads ignora basura sin lanzar excepciones y el parser de FO acepta variantes."""
        self.assertIsNone(common._safe_json_loads(b""))
        self.assertIsNone(common._safe_json_loads(b"mensaje_no_json_de_prueba"))
        self.assertEqual(common._safe_json_loads(b'{"ws": "WS-01"}'), {"ws": "WS-01"})

        fo = WM_FO.FieldOperator("localhost:9092", "FO-01")
        calls = []
        fo.request_service = lambda ws, dur: calls.append((ws, dur))

        # Simular entrada interactiva con variantes: 'r WS-01 10', 'r ws-02, 15s', 's WS-01 5', 'q'
        inputs = iter(["r WS-01 10", "r ws-02, 15s", "s WS-01 5", "q"])
        with patch("builtins.input", lambda *a: next(inputs)):
            fo.run_interactive()

        self.assertEqual(calls, [("WS-01", 10), ("WS-02", 15), ("WS-01", 5)])

        # Verificar que WM_Central localiza WM_Central/dashboard.html
        central = WM_Central.WMCentral(
            socket_port=get_free_port(),
            kafka_bootstrap="localhost:9092",
            web_port=0,
            db_path=self.temp_db.name
        )
        self.active_instances.append(central)
        html = central._get_dashboard_html()
        self.assertNotIn("Error: dashboard.html no encontrado", html)
        self.assertIn("<html", html.lower())

        # Verificar que WM_FO localiza operaciones.txt tanto por nombre corto como por ruta WM_FO/operaciones.txt
        batch_calls = []
        fo.request_service = lambda ws, dur: (batch_calls.append((ws, dur)), fo.service_done_event.set())
        with patch("time.sleep", lambda *a: None):
            fo.run_file("operaciones.txt")
        self.assertGreater(len(batch_calls), 0)

    def test_3_fault_tolerance_monitor_disconnect_stops_watering_and_blocks_manual(self):
        """Rúbrica: Si cae el Monitor de una WS, CENTRAL marca Desconectada, el riego en curso se detiene
        y la WS no admite nuevos riegos ni siquiera manuales mostrando 'Averiada'."""
        central, monitor, engine = self._start_cluster("WS-01")

        # Iniciar riego de 15 segundos
        central.request_watering("FO-01", "WS-01", 15)
        deadline = time.time() + 3.0
        while time.time() < deadline and (
            engine.state != "WATERING" or central.stations["WS-01"]["status"] != common.STATUS_WATERING
        ):
            time.sleep(0.05)
        self.assertEqual(engine.state, "WATERING")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_WATERING)

        # Simular caída abrupta del Monitor cerrando sus sockets
        monitor.running = False
        if monitor.central_sock:
            monitor.central_sock.close()
        if monitor.engine_sock:
            monitor.engine_sock.close()

        deadline = time.time() + 3.0
        while time.time() < deadline:
            if central.stations["WS-01"]["status"] == common.STATUS_OFFLINE and engine.state == "AVAILABLE":
                break
            time.sleep(0.05)

        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OFFLINE)
        self.assertEqual(engine.state, "AVAILABLE")
        self.assertFalse(engine.monitor_connected)

        # Intentar riego manual ('s') con Monitor caído -> debe rechazarse con mensaje AVERIADA
        buf = io.StringIO()
        with redirect_stdout(buf):
            engine.start_watering("MANUAL", 0)
        output = buf.getvalue()
        self.assertIn("AVERIADA", output)
        self.assertEqual(engine.state, "AVAILABLE")

    def test_4_fault_tolerance_engine_leak_preserves_volume_and_recovers(self):
        """Rúbrica: Si falla el Engine durante el riego, se detiene el riego sin perder el volumen ya regado
        y al recuperar informa del estado final del riego realizado."""
        central, monitor, engine = self._start_cluster("WS-01")

        central.request_watering("FO-01", "WS-01", 15)
        # Esperar al menos 1 tick de telemetría (>1s) para acumular volumen > 0
        deadline = time.time() + 4.0
        while time.time() < deadline and engine.accumulated_vol == 0.0:
            time.sleep(0.1)
        self.assertGreater(engine.accumulated_vol, 0.0)

        # Simular avería en el Engine (equivalente a pulsar 'k' o caída de sensor)
        with engine.lock:
            engine.is_ko = True
            engine.finish_watering("LEAK")

        # Esperar a que el Monitor detecte KO (monitor.leak_active = True) y CENTRAL marque FUGA DETECTADA
        deadline = time.time() + 4.0
        while time.time() < deadline and (
            central.stations["WS-01"]["status"] != common.STATUS_LEAK or not monitor.leak_active
        ):
            time.sleep(0.1)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_LEAK)
        self.assertTrue(monitor.leak_active)

        # Verificar que el volumen regado antes del fallo se guardó en SQLite
        with central._get_db() as db:
            rows = db.execute("SELECT * FROM watering_history WHERE ws_id = 'WS-01' AND reason = 'LEAK'").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertGreater(rows[0]["total_vol"], 0.0)

        # Recuperar avería (is_ko = False) y verificar que vuelve a DISPONIBLE informando del riego previo
        with engine.lock:
            engine.is_ko = False
        deadline = time.time() + 4.0
        while time.time() < deadline and central.stations["WS-01"]["status"] != common.STATUS_AVAILABLE:
            time.sleep(0.1)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)
        self.assertTrue(
            any("Estado final del riego previo" in entry["message"] for entry in central.recent_logs),
            "CENTRAL debe registrar el estado final del riego previo al resolverse la avería"
        )

    def test_5_fault_tolerance_fo_crash_and_recovery_receives_summary(self):
        """Rúbrica: Si cae un FO durante el servicio, el riego continúa y al recuperar el FO ve el resultado."""
        central, monitor, engine = self._start_cluster("WS-01")

        fo1 = WM_FO.FieldOperator("localhost:9092", "FO-01")
        self.active_instances.append(fo1)
        threading.Thread(target=fo1.listen_kafka, daemon=True).start()
        time.sleep(0.1)

        # Enviar petición y simular caída de FO-01 inmediatamente después de que empiece el riego
        send_kafka_ok = self.broker.send(common.TOPIC_REQUESTS, {
            "op": "FO-01", "ws": "WS-01", "duration": 2, "timestamp": time.time()
        })
        self.assertTrue(send_kafka_ok)

        deadline = time.time() + 3.0
        while time.time() < deadline and engine.state != "WATERING":
            time.sleep(0.05)
        self.assertEqual(engine.state, "WATERING")

        # Cae FO-01 mientras WS-01 sigue regando
        fo1.running = False
        for c in self.broker.consumers:
            if c.group_id == "fo-client-FO-01":
                c.active = False

        # Esperar a que el riego termine con FO-01 caído
        deadline = time.time() + 4.0
        while time.time() < deadline and central.stations["WS-01"]["status"] != common.STATUS_AVAILABLE:
            time.sleep(0.1)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)

        # Recuperar FO-01 (nueva instancia con el mismo ID FO-01) y comprobar que recibe el SUMMARY pendiente
        fo1_recovered = WM_FO.FieldOperator("localhost:9092", "FO-01")
        self.active_instances.append(fo1_recovered)
        buf = io.StringIO()
        with redirect_stdout(buf):
            threading.Thread(target=fo1_recovered.listen_kafka, daemon=True).start()
            recovered_done = fo1_recovered.service_done_event.wait(timeout=3.0)
        output = buf.getvalue()

        self.assertTrue(recovered_done, "FO-01 recuperado no procesó el SUMMARY pendiente de su grupo Kafka")
        self.assertIn("FINALIZADO", output)

    def test_6_fault_tolerance_central_down_messages(self):
        """Rúbrica: Si WM_Central cae, las interfaces muestran 'Imposible conectar con WM_Central' y las WS no admiten nuevos riegos."""
        central, monitor, engine = self._start_cluster("WS-01")

        # Detener WM_Central (tanto sockets como consumidores Kafka)
        central.running = False
        self.broker.stop_all()
        if monitor.central_sock:
            monitor.central_sock.close()
            monitor.central_sock = None

        # Esperar a que el Monitor notifique HEALTH#NO_CENTRAL al Engine
        deadline = time.time() + 4.0
        while time.time() < deadline and engine.central_connected:
            time.sleep(0.1)
        self.assertFalse(engine.central_connected)

        # Intentar iniciar riego en Engine sin CENTRAL -> debe rechazarlo con 'Imposible conectar con WM_Central'
        buf = io.StringIO()
        with redirect_stdout(buf):
            engine.start_watering("MANUAL", 0)
        self.assertIn("Imposible conectar con WM_Central", buf.getvalue())
        self.assertEqual(engine.state, "AVAILABLE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
