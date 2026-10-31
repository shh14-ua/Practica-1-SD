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
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
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
                      group_id: str = None, delay: float = 2.0,
                      enable_auto_commit: bool = True):
        gid = group_id if group_id else f"{group_prefix}-{int(time.time() * 1000000)}-{ len(self.consumers) }"
        consumer = FakeKafkaConsumer(
            self, topics, gid, auto_offset_reset,
            enable_auto_commit=enable_auto_commit
        )
        with self.lock:
            self.consumers.append(consumer)
        return consumer

    def stop_all(self):
        with self.lock:
            for c in self.consumers:
                c.active = False


class FakeKafkaConsumer:
    def __init__(self, broker: FakeKafkaBroker, topics, group_id: str,
                 auto_offset_reset: str, enable_auto_commit: bool = True):
        self.broker = broker
        self.topics = list(topics)
        self.group_id = group_id
        self.enable_auto_commit = enable_auto_commit
        self.active = True
        self.read_offsets = {}

        with self.broker.lock:
            for t in self.topics:
                key = (self.group_id, t)
                if key not in self.broker.group_offsets:
                    if auto_offset_reset == "earliest":
                        self.broker.group_offsets[key] = 0
                    else:
                        self.broker.group_offsets[key] = len(self.broker.topics.get(t, []))
                self.read_offsets[key] = self.broker.group_offsets[key]

    def commit(self):
        with self.broker.lock:
            for key, offset in self.read_offsets.items():
                self.broker.group_offsets[key] = offset

    def __iter__(self):
        while self.active:
            delivered = False
            with self.broker.lock:
                for t in self.topics:
                    key = (self.group_id, t)
                    msgs = self.broker.topics.get(t, [])
                    idx = self.read_offsets.get(key, self.broker.group_offsets.get(key, 0))
                    if idx < len(msgs):
                        val = msgs[idx]
                        self.read_offsets[key] = idx + 1
                        if self.enable_auto_commit:
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
            if hasattr(inst, "ensure_central_connection"):
                inst.ensure_central_connection = lambda: False
            if getattr(inst, "_web_server", None) is not None:
                try:
                    inst._web_server.shutdown()
                    inst._web_server.server_close()
                except Exception:
                    pass
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

        # Esperar a que los consumidores de CENTRAL estén registrados antes de continuar
        deadline = time.time() + 3.0
        while time.time() < deadline:
            with self.broker.lock:
                gids = {c.group_id for c in self.broker.consumers}
            if "central-requests" in gids and "central-events" in gids:
                break
            time.sleep(0.02)

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

        # Esperar a que WS-01 pase a DISPONIBLE en CENTRAL, el Engine tenga enlace OK y su consumidor Kafka esté listo
        deadline = time.time() + 5.0
        while time.time() < deadline:
            with self.broker.lock:
                engine_consumer_ready = any(
                    c.group_id.startswith(f"engine-{ws_id}") for c in self.broker.consumers
                )
            if (
                central.stations.get(ws_id, {}).get("status") == common.STATUS_AVAILABLE
                and engine.central_connected
                and engine.monitor_connected
                and engine_consumer_ready
            ):
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
        monitor.ensure_central_connection = lambda: False
        if monitor.central_sock:
            monitor.central_sock.close()
            monitor.central_sock = None
        if monitor.engine_sock:
            monitor.engine_sock.close()
            monitor.engine_sock = None

        deadline = time.time() + 3.0
        while time.time() < deadline:
            if (
                central.stations["WS-01"]["status"] == common.STATUS_OFFLINE
                and engine.state == "AVAILABLE"
                and not engine.monitor_connected
            ):
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

        # Esperar a que el Monitor detecte KO, CENTRAL marque FUGA y se persista el registro en SQLite
        rows = []
        deadline = time.time() + 4.0
        while time.time() < deadline:
            with central._get_db() as db:
                rows = db.execute("SELECT * FROM watering_history WHERE ws_id = 'WS-01' AND reason = 'LEAK'").fetchall()
            if (
                central.stations["WS-01"]["status"] == common.STATUS_LEAK
                and monitor.leak_active
                and len(rows) == 1
            ):
                break
            time.sleep(0.05)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_LEAK)
        self.assertTrue(monitor.leak_active)
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

    def test_7_timer_independent_of_slow_kafka(self):
        """El cierre del riego no debe esperar a Kafka."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)

        engine.ws_id = "WS-TEST"
        engine.monitor_connected = True
        engine.central_connected = True

        original_send = engine.send_event

        def slow_send(ev_type, **kwargs):
            if ev_type == "TELEMETRY":
                time.sleep(3.0)
            return original_send(ev_type, **kwargs)

        engine.send_event = slow_send

        start = time.monotonic()
        engine.start_watering("FO-TEST", 2)

        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline:
            with engine.lock:
                if engine.state == "AVAILABLE":
                    break
            time.sleep(0.02)

        with engine.lock:
            self.assertEqual(engine.state, "AVAILABLE")

        elapsed = time.monotonic() - start
        self.assertLess(
            elapsed, 2.7,
            "El cierre del riego se ha retrasado esperando a Kafka"
        )

    def test_8_finished_after_telemetry(self):
        """Comprueba el orden de los eventos con telemetría lenta."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)

        engine.ws_id = "WS-TEST"
        engine.monitor_connected = True
        engine.central_connected = True

        original_send = engine.send_event

        def slow_send(ev_type, **kwargs):
            if ev_type == "TELEMETRY":
                time.sleep(3.0)
            return original_send(ev_type, **kwargs)

        engine.send_event = slow_send
        engine.start_watering("FO-TEST", 2)

        time.sleep(5.0)

        events = self.broker.topics.get(common.TOPIC_EVENTS, [])
        types = [
            e["type"]
            for e in events
            if e.get("ws") == "WS-TEST"
        ]

        self.assertIn("TELEMETRY", types)
        self.assertIn("FINISHED", types)
        self.assertLess(
            types.index("TELEMETRY"),
            types.index("FINISHED"),
            "La telemetría llegó después del evento FINISHED"
        )

    def test_9_activate_all_verifies_engine_and_keeps_monitor_only_in_leak(self):
        """Verifica que ACTIVATE (individual y ALL) comprueba cada estación por separado:
        - Con Monitor y Engine operativos -> pasa a DISPONIBLE (verde).
        - Con Monitor pero sin Engine -> pasa a FUGA (rojo) y registra el motivo.
        - Con Monitor y Engine en KO -> pasa a FUGA (rojo) y registra el motivo.
        - Sin Monitor -> permanece en DESCONECTADA."""
        central, monitor1, engine1 = self._start_cluster("WS-01")
        central_port = monitor1.central_port

        # Conectar WS-02 únicamente con Monitor (sin Engine)
        monitor2 = WM_WS_M.WSMonitor(
            engine_port=get_free_port(),
            central_addr=f"127.0.0.1:{central_port}",
            ws_id="WS-02",
            location="East Garden"
        )
        self.active_instances.append(monitor2)
        threading.Thread(target=monitor2.run, daemon=True).start()

        deadline = time.time() + 4.0
        while time.time() < deadline:
            if (
                "WS-02" in central.station_sockets
                and central.stations["WS-02"]["status"] == common.STATUS_LEAK
                and monitor2.leak_active is True
            ):
                break
            time.sleep(0.05)

        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)
        self.assertEqual(central.stations["WS-02"]["status"], common.STATUS_LEAK)
        self.assertFalse(central.stations["WS-02"]["engine_ok"])
        self.assertEqual(central.stations["WS-03"]["status"], common.STATUS_OFFLINE)

        # 1. Bloquear todas las estaciones (b ALL)
        central.block_station("ALL")
        deadline = time.time() + 3.0
        while time.time() < deadline and engine1.state != "BLOCKED":
            time.sleep(0.05)

        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)
        self.assertEqual(central.stations["WS-02"]["status"], common.STATUS_OUT_OF_SERVICE)
        self.assertEqual(central.stations["WS-03"]["status"], common.STATUS_OFFLINE)
        self.assertEqual(engine1.state, "BLOCKED")

        # 2. Activar todas las estaciones (a ALL)
        central.activate_station("ALL")
        deadline = time.time() + 3.0
        while time.time() < deadline and engine1.state != "AVAILABLE":
            time.sleep(0.05)

        # WS-01 tiene Monitor y Engine operativos -> DISPONIBLE
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)
        self.assertEqual(engine1.state, "AVAILABLE")
        # WS-02 tiene Monitor pero NO Engine -> FUGA (rojo) con motivo registrado
        self.assertEqual(central.stations["WS-02"]["status"], common.STATUS_LEAK)
        self.assertTrue(
            any(
                entry["ws"] == "WS-02" and "Engine no conectado al Monitor" in entry["message"]
                for entry in central.recent_logs
            ),
            "Debe registrarse que WS-02 pasa/se mantiene en FUGA porque el Engine no está conectado al Monitor"
        )
        # WS-03 no tiene Monitor -> DESCONECTADA
        self.assertEqual(central.stations["WS-03"]["status"], common.STATUS_OFFLINE)

        # 3. Si WS-01 entra en KO y se bloquea/activa, tampoco puede pasar a DISPONIBLE mientras siga en KO
        with engine1.lock:
            engine1.is_ko = True
        deadline = time.time() + 3.0
        while time.time() < deadline and central.stations["WS-01"]["status"] != common.STATUS_LEAK:
            time.sleep(0.05)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_LEAK)

        central.block_station("WS-01")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)
        central.activate_station("WS-01")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_LEAK)
        self.assertTrue(
            any(
                entry["ws"] == "WS-01" and "Avería / fuga detectada en Engine" in entry["message"]
                for entry in central.recent_logs
            )
        )

    def test_10_block_persists_until_explicit_activate_and_syncs_late_engine(self):
        """Verifica que:
        - Una estación en FUERA DE SERVICIO mantiene el bloqueo aunque el Monitor envíe LEAK o FIXED.
        - Si WM_WS_E arranca después de haberse ejecutado BLOCK en Central, la estación sigue en FUERA DE SERVICIO
          y el nuevo Engine queda sincronizado en estado BLOCKED (rechazando riegos manuales) hasta orden ACTIVATE."""
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

        # Arrancar solo Monitor para WS-01 (sin Engine)
        monitor = WM_WS_M.WSMonitor(
            engine_port=engine_port,
            central_addr=f"127.0.0.1:{central_port}",
            ws_id="WS-01",
            location="River Park"
        )
        self.active_instances.append(monitor)
        threading.Thread(target=monitor.run, daemon=True).start()

        deadline = time.time() + 4.0
        while time.time() < deadline:
            if "WS-01" in central.station_sockets and monitor.leak_active is True:
                break
            time.sleep(0.05)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_LEAK)

        # Ejecutar BLOCK en Central antes de que arranque el Engine
        central.block_station("WS-01")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        # Arrancar Engine DESPUÉS del BLOCK
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr=f"127.0.0.1:{engine_port}"
        )
        self.active_instances.append(engine)
        threading.Thread(target=engine.link_monitor, daemon=True).start()
        self.assertTrue(engine.assigned_event.wait(timeout=5.0))
        threading.Thread(target=engine.run_commands_consumer, daemon=True).start()

        # Esperar a que el Monitor detecte salud OK, notifique FIXED a Central y sincronice el bloqueo con el Engine
        deadline = time.time() + 4.0
        while time.time() < deadline:
            if (
                central.stations["WS-01"]["engine_ok"] is True
                and engine.state == "BLOCKED"
            ):
                break
            time.sleep(0.05)

        # Central debe seguir en FUERA DE SERVICIO (FIXED no debe desbloquearla sin orden explícita)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)
        self.assertTrue(central.stations["WS-01"]["engine_ok"])
        self.assertEqual(engine.state, "BLOCKED")

        # Intentar riego manual en el Engine recién arrancado -> debe rechazarse por estar FUERA DE SERVICIO
        buf = io.StringIO()
        with redirect_stdout(buf):
            engine.start_watering("MANUAL", 0)
        self.assertIn("FUERA DE SERVICIO", buf.getvalue())
        self.assertEqual(engine.state, "BLOCKED")

        # Ahora enviar orden explícita de activación desde Central
        central.activate_station("WS-01")
        deadline = time.time() + 3.0
        while time.time() < deadline and engine.state != "AVAILABLE":
            time.sleep(0.05)

        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)
        self.assertEqual(engine.state, "AVAILABLE")

    def test_11_dynamic_station_registration_auth_persistence_and_existing_preservation(self):
        """Requisito 18: Verifica:
        1. Rechazo de una estación no registrada (WS-07 antes del alta y WS-99).
        2. Alta dinámica de WS-07 sin reiniciar Central y conexión posterior de su Monitor y Engine hasta DISPONIBLE.
        3. Persistencia del alta en SQLite (se conserva ante un nuevo arranque de WMCentral).
        4. Si se intenta registrar una estación ya existente (WS-01), devuelve aviso (False) sin alterar
           su ubicación, su estado ni su conexión activa."""
        central, monitor1, engine1 = self._start_cluster("WS-01")
        central_port = monitor1.central_port

        # 1. Intentar autenticar WS-07 antes de darla de alta -> Central debe denegarla
        monitor7 = WM_WS_M.WSMonitor(
            engine_port=get_free_port(),
            central_addr=f"127.0.0.1:{central_port}",
            ws_id="WS-07",
            location="Central Park"
        )
        self.active_instances.append(monitor7)
        self.assertFalse(
            monitor7.ensure_central_connection(),
            "WS-07 no registrada debe ser rechazada en la autenticación"
        )
        self.assertNotIn("WS-07", central.stations)
        self.assertTrue(
            any(
                entry["ws"] == "WS-07" and "Autenticación DENEGADA" in entry["message"]
                for entry in central.recent_logs
            )
        )

        # 2. Intentar registrar una estación que ya existe (WS-01) -> aviso sin modificar ubicación, estado ni socket
        orig_sock = central.station_sockets.get("WS-01")
        orig_loc = central.stations["WS-01"]["location"]
        orig_status = central.stations["WS-01"]["status"]
        self.assertEqual(orig_status, common.STATUS_AVAILABLE)

        created_dup = central.register_station("WS-01", "Ubicacion Intrusa")
        self.assertFalse(created_dup)
        self.assertEqual(central.stations["WS-01"]["location"], orig_loc)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)
        self.assertIs(central.station_sockets.get("WS-01"), orig_sock)
        self.assertTrue(
            any(
                entry["ws"] == "WS-01" and "ya existe; no se modifica" in entry["message"]
                for entry in central.recent_logs
            )
        )

        # 3. Dar de alta dinámicamente WS-07 en Central sin reiniciar
        created_new = central.register_station("WS-07", "Central Park")
        self.assertTrue(created_new)
        self.assertIn("WS-07", central.stations)
        self.assertEqual(central.stations["WS-07"]["status"], common.STATUS_OFFLINE)
        self.assertEqual(central.stations["WS-07"]["location"], "Central Park")

        # 4. Conectar Monitor y Engine de WS-07 y verificar que pasa a DISPONIBLE sin afectar a WS-01
        threading.Thread(target=monitor7.run, daemon=True).start()
        engine7 = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr=f"127.0.0.1:{monitor7.engine_port}"
        )
        self.active_instances.append(engine7)
        threading.Thread(target=engine7.link_monitor, daemon=True).start()
        self.assertTrue(engine7.assigned_event.wait(timeout=5.0))
        threading.Thread(target=engine7.run_commands_consumer, daemon=True).start()

        deadline = time.time() + 5.0
        while time.time() < deadline:
            if central.stations["WS-07"]["status"] == common.STATUS_AVAILABLE:
                break
            time.sleep(0.05)

        self.assertEqual(central.stations["WS-07"]["status"], common.STATUS_AVAILABLE)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_AVAILABLE)

        # 5. Comprobar que otra estación no dada de alta (WS-99) sigue siendo rechazada
        monitor99 = WM_WS_M.WSMonitor(
            engine_port=get_free_port(),
            central_addr=f"127.0.0.1:{central_port}",
            ws_id="WS-99",
            location="Zona No Autorizada"
        )
        self.active_instances.append(monitor99)
        self.assertFalse(monitor99.ensure_central_connection())
        self.assertNotIn("WS-99", central.stations)

        # 6. Verificar persistencia en SQLite: una nueva instancia de WMCentral sobre la misma BD carga WS-07
        with central._get_db() as db:
            row7 = db.execute("SELECT id, location, status FROM stations WHERE id = 'WS-07'").fetchone()
        self.assertIsNotNone(row7)
        self.assertEqual(row7["id"], "WS-07")

        central_restarted = WM_Central.WMCentral(
            socket_port=get_free_port(),
            kafka_bootstrap="localhost:9092",
            web_port=0,
            db_path=self.temp_db.name
        )
        self.active_instances.append(central_restarted)
        self.assertIn("WS-07", central_restarted.stations)
        self.assertEqual(central_restarted.stations["WS-07"]["status"], common.STATUS_OFFLINE)

    def test_12_http_register_access_control_and_id_validation(self):
        """Verifica el control de acceso y validación de formato en el alta de estaciones:
        1. Valida que solo se acepten IDs con formato WS- seguido de al menos 2 dígitos (WS-07, WS-100),
           rechazando WS-7, WS-AB, ALL o cadenas vacías.
        2. Una petición HTTP register desde loopback (127.0.0.1) es autorizada (HTTP 200) y registra la estación.
        3. Una petición HTTP register desde una IP externa o desde el bridge Docker sin token es rechazada con HTTP 403.
        4. Con WM_ADMIN_TOKEN configurado, una petición desde el host AWS (bridge Docker) con X-Admin-Token válido
           es autorizada (HTTP 200), y sin él o con token incorrecto recibe HTTP 403."""
        import json
        import urllib.error
        import urllib.request

        web_port = get_free_port()
        central = WM_Central.WMCentral(
            socket_port=get_free_port(),
            kafka_bootstrap="localhost:9092",
            web_port=web_port,
            db_path=self.temp_db.name
        )
        self.active_instances.append(central)
        threading.Thread(target=central.run_web_server, daemon=True).start()

        # Esperar a que el servidor HTTP esté escuchando
        url_status = f"http://127.0.0.1:{web_port}/api/status"
        url_action = f"http://127.0.0.1:{web_port}/api/action"
        deadline = time.time() + 3.0
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url_status, timeout=1.0) as r:
                    if r.status == 200:
                        break
            except Exception:
                time.sleep(0.05)

        # 1. Validación de formato de identificador en register_station y vía HTTP
        self.assertFalse(central.register_station("WS-7", "Formato Invalido 1 digito"))
        self.assertFalse(central.register_station("WS-AB", "Formato Invalido letras"))
        self.assertFalse(central.register_station("ALL", "Reservado"))
        self.assertNotIn("WS-7", central.stations)
        self.assertNotIn("WS-AB", central.stations)

        req_bad = urllib.request.Request(
            url_action,
            data=json.dumps({"action": "register", "ws": "WS-7", "location": "Test"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx_bad:
            urllib.request.urlopen(req_bad, timeout=2.0)
        self.assertEqual(ctx_bad.exception.code, 400)
        self.assertNotIn("WS-7", central.stations)

        # 2. Petición autorizada desde loopback (127.0.0.1) con WS-07 y WS-100 -> HTTP 200
        for valid_ws in ("WS-07", "WS-100"):
            req_ok = urllib.request.Request(
                url_action,
                data=json.dumps({"action": "register", "ws": valid_ws, "location": "Central Park"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req_ok, timeout=2.0) as resp:
                self.assertEqual(resp.status, 200)
                body = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(body.get("created"))
            self.assertIn(valid_ws, central.stations)

        # 3. Petición externa no autorizada (simulando IP pública 203.0.113.50 o bridge Docker 172.18.0.1) -> HTTP 403
        orig_check = central._is_authorized_admin_request
        for external_ip in ("203.0.113.50", "172.18.0.1"):
            central._is_authorized_admin_request = (
                lambda _ip, headers=None, ip=external_ip: orig_check(ip, headers)
            )
            req_ext = urllib.request.Request(
                url_action,
                data=json.dumps({"action": "register", "ws": "WS-08", "location": "Intruso"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx_403:
                urllib.request.urlopen(req_ext, timeout=2.0)
            self.assertEqual(ctx_403.exception.code, 403)
            self.assertNotIn("WS-08", central.stations)

        # 4. Con WM_ADMIN_TOKEN configurado, petición desde bridge Docker (172.18.0.1) con token válido -> HTTP 200
        with patch.dict(os.environ, {"WM_ADMIN_TOKEN": "clave-secreta-central"}):
            central._is_authorized_admin_request = (
                lambda _ip, headers=None: orig_check("172.18.0.1", headers)
            )
            # Token incorrecto -> HTTP 403
            req_wrong_tok = urllib.request.Request(
                url_action,
                data=json.dumps({"action": "register", "ws": "WS-08", "location": "Central Park"}).encode("utf-8"),
                headers={"Content-Type": "application/json", "X-Admin-Token": "token-falso"},
                method="POST"
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx_wrong:
                urllib.request.urlopen(req_wrong_tok, timeout=2.0)
            self.assertEqual(ctx_wrong.exception.code, 403)
            self.assertNotIn("WS-08", central.stations)

            # Token correcto -> HTTP 200
            req_valid_tok = urllib.request.Request(
                url_action,
                data=json.dumps({"action": "register", "ws": "WS-08", "location": "Central Park"}).encode("utf-8"),
                headers={"Content-Type": "application/json", "X-Admin-Token": "clave-secreta-central"},
                method="POST"
            )
            with urllib.request.urlopen(req_valid_tok, timeout=2.0) as resp_tok:
                self.assertEqual(resp_tok.status, 200)
                body_tok = json.loads(resp_tok.read().decode("utf-8"))
                self.assertTrue(body_tok.get("created"))
            self.assertIn("WS-08", central.stations)


if __name__ == "__main__":
    unittest.main(verbosity=2)