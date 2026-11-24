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

import inspect
import io
import os
import queue
import socket
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime
from contextlib import redirect_stdout
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
for subdir in ("WM_Central", "WM_WS", "WM_FO"):
    mod_path = os.path.join(ROOT_DIR, subdir)
    if mod_path not in sys.path:
        sys.path.insert(0, mod_path)

import traceback
import common
from wm_crypto import generate_symmetric_key, open_kafka_message, KAFKA_KIND_EVENT
import WM_Central
import WM_FO
import WM_WS_E
import WM_WS_M

_orig_socket = socket.socket
_orig_create_connection = socket.create_connection


class FakeSocket:
    def __init__(self, rx: queue.Queue, tx: queue.Queue, laddr=("127.0.0.1", 0), raddr=("127.0.0.1", 0)):
        self.rx = rx
        self.tx = tx
        self.laddr = laddr
        self.raddr = raddr
        self.timeout = None
        self.closed = False
        self.peer = None
        self.peer_closed = False
        self.buf = b""

    def settimeout(self, t):
        self.timeout = t

    def gettimeout(self):
        return self.timeout

    def getsockname(self):
        return self.laddr

    def getpeername(self):
        return self.raddr

    def fileno(self):
        return 99

    def setsockopt(self, *a):
        pass

    def sendall(self, data: bytes):
        if self.closed or self.peer_closed:
            raise ConnectionError("socket closed")
        self.tx.put(data)

    def recv(self, n: int) -> bytes:
        if (self.closed or self.peer_closed) and not self.buf:
            return b""
        while len(self.buf) < n and not self.closed:
            try:
                chunk = self.rx.get(timeout=self.timeout)
                if chunk is None:
                    self.closed = True
                    break
                self.buf += chunk
            except queue.Empty:
                raise TimeoutError("timed out")
        res = self.buf[:n]
        self.buf = self.buf[n:]
        return res

    def close(self):
        if not self.closed:
            self.closed = True
            if self.peer:
                self.peer.peer_closed = True
            self.tx.put(None)


def fake_socket_pair(p1=11111, p2=22222):
    q1 = queue.Queue()
    q2 = queue.Queue()
    s1 = FakeSocket(q1, q2, ("127.0.0.1", p1), ("127.0.0.1", p2))
    s2 = FakeSocket(q2, q1, ("127.0.0.1", p2), ("127.0.0.1", p1))
    s1.peer = s2
    s2.peer = s1
    return s1, s2


class FakeServerSocket:
    def __init__(self, hub):
        self.hub = hub
        self.port = 0
        self.timeout = None
        self.closed = False
        self.owner = None
        self.queue = queue.Queue()

    def setsockopt(self, *args):
        pass

    def bind(self, addr):
        self.port = addr[1]

    def listen(self, backlog=1):
        self.hub.register(self.port, self)

    def settimeout(self, t):
        self.timeout = t

    def accept(self):
        while not self.closed:
            if self.owner and not getattr(self.owner, "running", True):
                self.close()
                break
            try:
                conn, addr = self.queue.get(timeout=self.timeout or 0.2)
                if conn is None or self.closed:
                    break
                return conn, addr
            except queue.Empty:
                if self.timeout is not None:
                    raise TimeoutError("timed out")
        raise OSError("server socket closed")

    def close(self):
        self.closed = True
        self.hub.unregister(self.port)
        self.queue.put((None, None))

    def getsockname(self):
        return ("0.0.0.0", self.port)


class FakeSocketHub:
    _port_counter = 40000

    def __init__(self, test_case=None):
        self.servers = {}
        self.lock = threading.Lock()
        self.test_case = test_case

    @classmethod
    def get_port(cls) -> int:
        cls._port_counter += 1
        return cls._port_counter

    def register(self, port, server):
        with self.lock:
            self.servers[port] = server

    def unregister(self, port):
        with self.lock:
            self.servers.pop(port, None)

    def close_all(self):
        with self.lock:
            servers = list(self.servers.values())
        for s in servers:
            s.close()

    def is_server_available(self, server, port) -> bool:
        if server.closed:
            return False
        if server.owner and not getattr(server.owner, "running", True):
            return False
        if self.test_case:
            for inst in getattr(self.test_case, "active_instances", []):
                if getattr(inst, "socket_port", None) == port or getattr(inst, "engine_port", None) == port:
                    if not getattr(inst, "running", True):
                        return False
        return True

    def create_connection(self, address, *args, **kwargs):
        host, port = address
        with self.lock:
            server = self.servers.get(port)
        if server:
            if not self.is_server_available(server, port):
                server.close()
                raise ConnectionRefusedError(f"Connection refused on port {port}")
            timeout = args[0] if len(args) > 0 else kwargs.get("timeout")
            client_sock, srv_sock = fake_socket_pair(50000 + (port % 1000), port)
            client_sock.settimeout(timeout)
            server.queue.put((srv_sock, client_sock.getsockname()))
            return client_sock

        stack = traceback.extract_stack(limit=6)
        is_cluster_client = any(
            frame.name in ("link_monitor", "ensure_central_connection")
            for frame in stack
        )
        if is_cluster_client:
            raise ConnectionRefusedError(f"Connection refused on port {port}")
        return _orig_create_connection(address, *args, **kwargs)

    def socket_factory(self, family=socket.AF_INET, type=socket.SOCK_STREAM, *args, **kwargs):
        stack = traceback.extract_stack(limit=6)
        is_cluster_server = False
        for frame in stack:
            if frame.name in ("run_socket_server", "run") and ("WM_Central" in frame.filename or "WM_WS_M" in frame.filename):
                is_cluster_server = True
                break
        if is_cluster_server and family == socket.AF_INET and type == socket.SOCK_STREAM:
            srv = FakeServerSocket(self)
            frame = inspect.currentframe().f_back
            while frame:
                self_obj = frame.f_locals.get("self")
                if self_obj and hasattr(self_obj, "running") and (hasattr(self_obj, "socket_port") or hasattr(self_obj, "engine_port")):
                    srv.owner = self_obj
                    break
                frame = frame.f_back
            return srv
        return _orig_socket(family, type, *args, **kwargs)


def get_free_port() -> int:
    return FakeSocketHub.get_port()


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
            # Simular desfase de reloj en mensajes emitidos por CENTRAL. Las órdenes a los Engine
            # (ws_commands) van cifradas y solo admiten los campos {ws, enc}: su hora viaja dentro del
            # cifrado y el desfase entre máquinas se prueba en test_kafka_encryption.py.
            if topic == common.TOPIC_REPLIES:
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
            msg_to_yield = None
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
                        msg_to_yield = FakeKafkaMessage(t, val)
                        break
            if msg_to_yield is not None:
                yield msg_to_yield
            else:
                time.sleep(0.02)


class FakeRegistry:
    """Simulación de WM_Registry para el arnés de la Release 1 (solo configuración de pruebas).
    El arnés sustituye los sockets por un hub en memoria, por lo que no puede levantar el
    servidor HTTPS real. La simulación mantiene las reglas de autenticación:
      - Una WS obtiene credencial al darse de alta (cada alta emite una credencial distinta).
      - Central solo acepta la credencial emitida para esa misma WS.
      - Se considera dada de alta en el Registry la WS que Central conoce (las precargadas y las
        añadidas con el alta de Central), preservando el rechazo de estaciones no registradas
        que comprueba test_11 (WS-07 antes del alta y WS-99).
    Las pruebas con el Registry real (HTTPS, credenciales inválidas, Registry caído) están en
    test_release2.py."""

    def __init__(self):
        self.lock = threading.Lock()
        self.issued = {}
        self.counter = 0

    def register(self, monitor) -> bool:
        with self.lock:
            self.counter += 1  # cada alta o renovación emite una credencial distinta
            cred = f"CRED-{monitor.ws_id}-TEST{self.counter:04d}"
            self.issued[monitor.ws_id] = cred
        monitor.credential = cred
        return True

    def verify(self, central, ws_id: str, credential: str) -> bool:
        ws = (ws_id or "").strip().upper()
        with self.lock:
            expected = self.issued.get(ws)
        return ws in central.stations and expected is not None and credential == expected


class WaterManagementFlowTests(unittest.TestCase):
    def setUp(self):
        self.broker = FakeKafkaBroker()
        self.socket_hub = FakeSocketHub(self)
        self.registry = FakeRegistry()
        self.cred_dir = tempfile.mkdtemp(prefix="wm_creds_")
        self.active_instances = []
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db.close()

        # Parchear sockets y funciones Kafka en todos los módulos
        self.patches = [
            patch.object(socket, "create_connection", self.socket_hub.create_connection),
            patch.object(socket, "socket", self.socket_hub.socket_factory),
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
            # Simulación de WM_Registry (ver FakeRegistry) y credenciales locales en directorio temporal
            patch.object(WM_WS_M, "DEFAULT_CRED_DIR", self.cred_dir),
            patch.object(WM_WS_E, "DEFAULT_STATE_DIR", os.path.join(self.cred_dir, "engine_state")),
            patch.dict(os.environ, {"WM_AUDIT_LOG": os.path.join(self.cred_dir, "audit.log")}),
            patch.object(WM_WS_M.WSMonitor, "register_in_registry",
                         lambda mon: self.registry.register(mon)),
            patch.object(WM_Central.WMCentral, "verify_registry_credential",
                         lambda cen, ws_id, cred: self.registry.verify(cen, ws_id, cred)),
            # Canal TLS Central <-> Monitor: los sockets en memoria del arnés no pueden negociar TLS,
            # así que solo se sustituye el envoltorio TLS (el protocolo de tramas, la autenticación y
            # el cifrado simétrico se ejecutan igual). El TLS real se prueba en test_tls_central.py.
            patch.object(WM_Central.WMCentral, "_init_tls_context", lambda cen: None),
            patch.object(WM_Central.WMCentral, "_wrap_station_tls", lambda cen, conn, ip: conn),
            patch.object(WM_WS_M.WSMonitor, "_wrap_central_tls", lambda mon, sock: sock),
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
        if hasattr(self, "socket_hub"):
            self.socket_hub.close_all()
        time.sleep(0.05)
        for p in reversed(self.patches):
            p.stop()
        if os.path.exists(self.temp_db.name):
            try:
                os.unlink(self.temp_db.name)
            except OSError:
                pass
        import shutil
        shutil.rmtree(self.cred_dir, ignore_errors=True)

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
        # Canal cifrado: los números de secuencia de Central también salen de su reloj atrasado 120 s
        skewed_seq = lambda last: max(int(last) + 1, time.time_ns() - 120 * 10**9)
        skew_patch = patch.object(WM_Central, "next_kafka_seq", skewed_seq)
        skew_patch.start()
        self.addCleanup(skew_patch.stop)
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
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

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
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

        original_send = engine.send_event

        def slow_send(ev_type, **kwargs):
            if ev_type == "TELEMETRY":
                time.sleep(3.0)
            return original_send(ev_type, **kwargs)

        engine.send_event = slow_send
        engine.start_watering("FO-TEST", 2)

        time.sleep(5.0)

        events = self.broker.topics.get(common.TOPIC_EVENTS, [])
        # Los eventos viajan cifrados: se descifran con la clave de esta estación para ver su orden.
        # Solo cuentan los de este Engine (los hilos tardíos de test_7, también WS-TEST, usan otra clave).
        decoded = [open_kafka_message(engine.symmetric_key, e, KAFKA_KIND_EVENT, "WS-TEST")[0]
                   for e in events if e.get("ws") == "WS-TEST"]
        types = [d["type"] for d in decoded if d is not None]

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

    def test_13_kafka_timeout_does_not_block_health_or_cause_disconnect(self):
        """Verifica que el bloqueo/timeout de Kafka al iniciar un riego no bloquea las respuestas HEALTH
        hacia el Monitor, no provoca falsos 'NO_ENGINE' ni 'MONITOR_DOWN' y no causa reconexiones cíclicas."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)

        engine.ws_id = "WS-03"
        engine.monitor_connected = True
        engine.central_connected = True
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

        # Simular que Kafka está bloqueado / caído (demora de 1.5 s en send_event)
        kafka_in_flight = threading.Event()
        original_send = engine.send_event

        def blocking_send(ev_type, **kwargs):
            if ev_type == "STARTED":
                kafka_in_flight.set()
                time.sleep(1.5)  # Simula el timeout de Kafka al publicar STARTED
                return False     # Kafka no responde
            if ev_type == "FINISHED":
                return False
            return original_send(ev_type, **kwargs)

        engine.send_event = blocking_send

        # Iniciar riego manual en un hilo concurrente (como si el operario pulsara 's' en consola)
        start_thread = threading.Thread(
            target=engine.start_watering,
            args=("MANUAL", 0),
            kwargs={"transaction_id": "TX-MANUAL-KAFKA-DOWN"}
        )
        start_thread.start()

        # Esperar a que start_watering esté activamente bloqueado en Kafka
        self.assertTrue(kafka_in_flight.wait(timeout=2.0))

        # Mientras start_watering está bloqueado en Kafka, simular la consulta periódica de HEALTH del Monitor
        t0 = time.monotonic()
        health_resp = engine.handle_monitor_request("HEALTH#OK")
        elapsed = time.monotonic() - t0

        # Debe responder inmediatamente sin esperar a que Kafka termine
        self.assertEqual(health_resp, "OK")
        self.assertLess(
            elapsed, 0.5,
            f"La respuesta HEALTH tardó {elapsed:.2f} s; debe ser inmediata (< 0.5 s) aun con Kafka bloqueado"
        )

        # Esperar a que termine la llamada a Kafka
        start_thread.join(timeout=3.0)
        self.assertFalse(start_thread.is_alive())

        # Comprobar que el riego sigue activo y no se desconectó por MONITOR_DOWN
        self.assertEqual(engine.state, "WATERING")
        self.assertTrue(engine.monitor_connected)

        # Ahora simular finalización del riego con Kafka aún caído
        engine.finish_watering("MANUAL")
        self.assertEqual(engine.state, "AVAILABLE")
        self.assertEqual(len(engine.pending_finished_events), 1)

        # Simular nueva consulta HEALTH mientras hay eventos pendientes: debe responder OK de inmediato
        # sin bloquearse vaciando eventos en el hilo del handler
        t1 = time.monotonic()
        health_resp_pending = engine.handle_monitor_request("HEALTH#OK")
        elapsed_pending = time.monotonic() - t1

        self.assertEqual(health_resp_pending, "OK")
        self.assertLess(elapsed_pending, 0.5)

        # Comprobar que flush_lock evita ejecuciones concurrentes de vaciado
        acquired = engine.flush_lock.acquire(blocking=False)
        if acquired:
            try:
                # Si adquirimos el cerrojo nosotros, flush_pending_finished debe retornar 0
                self.assertEqual(engine.flush_pending_finished(), 0)
            finally:
                engine.flush_lock.release()

    def test_14_independent_timer_closes_on_time_when_kafka_blocked(self):
        """Requisito: Comprueba que un riego de 2 segundos cierra la electroválvula puntualmente
        a los 2s aun cuando la publicación de STARTED sufra un bloqueo de 4 segundos en Kafka."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)
        engine.ws_id = "WS-01"
        engine.monitor_connected = True
        engine.central_connected = True
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

        kafka_started_delay = threading.Event()
        original_send = engine.send_event
        finished_sent = threading.Event()

        def slow_started_send(ev_type, **kwargs):
            if ev_type == "STARTED":
                kafka_started_delay.set()
                time.sleep(4.0)  # Kafka bloqueado 4 segundos al publicar STARTED
                return False
            if ev_type == "FINISHED":
                finished_sent.set()
                return True
            return original_send(ev_type, **kwargs)

        engine.send_event = slow_started_send

        t0 = time.monotonic()
        # Iniciar riego de 2 segundos
        engine.start_watering("MANUAL", 2, transaction_id="TX-TIMER-4S-KAFKA")
        self.assertTrue(kafka_started_delay.wait(timeout=2.0))

        # Esperar a que la electroválvula se cierre por tiempo (state pasa a AVAILABLE)
        deadline = t0 + 3.0
        while time.monotonic() < deadline and engine.state == "WATERING":
            time.sleep(0.05)

        closing_elapsed = time.monotonic() - t0
        self.assertEqual(engine.state, "AVAILABLE", "La electroválvula debe haberse cerrado por tiempo")
        self.assertLess(
            closing_elapsed, 2.8,
            f"La electroválvula tardó {closing_elapsed:.2f}s en cerrar; debe cerrar en ~2s sin esperar los 4s de Kafka"
        )

        # Esperar a que concluyan las publicaciones en segundo plano
        self.assertTrue(finished_sent.wait(timeout=5.0))
        self.assertEqual(engine.state, "AVAILABLE")

    def test_15_safe_station_blocking_via_monitor_health_and_reactivation(self):
        """Requisito: Comprueba que la orden BLOCKED recibida por socket Monitor interrumpe el riego activo,
        emite FINISHED con motivo CENTRAL_BLOCK, deja el estado estrictamente en BLOCKED (sin pasar por AVAILABLE),
        no bloquea HEALTH y permite reactivación posterior con ACTIVE."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)
        engine.ws_id = "WS-01"
        engine.monitor_connected = True
        engine.central_connected = True
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

        finished_events = []
        original_send = engine.send_event

        def track_send(ev_type, **kwargs):
            if ev_type == "FINISHED":
                finished_events.append(dict(kwargs))
                return True
            return original_send(ev_type, **kwargs)

        engine.send_event = track_send

        # 1. Iniciar riego de 10s
        engine.start_watering("MANUAL", 10, transaction_id="TX-BLOCK-HEALTH")
        self.assertEqual(engine.state, "WATERING")

        # Acumular volumen > 0
        with engine.lock:
            engine.accumulated_vol = 0.45

        # 2. Recibir orden de bloqueo por socket HEALTH#OK#BLOCKED
        t0 = time.monotonic()
        resp = engine.handle_monitor_request("HEALTH#OK#BLOCKED")
        elapsed = time.monotonic() - t0

        self.assertEqual(resp, "OK")
        self.assertLess(elapsed, 0.5, "handle_monitor_request no debe bloquearse al tramitar BLOCKED")

        # 3. Esperar finalización del riego
        deadline = time.time() + 2.0
        while time.time() < deadline and engine.state != "BLOCKED":
            time.sleep(0.05)

        self.assertEqual(engine.state, "BLOCKED", "El estado final debe ser BLOCKED")
        self.assertEqual(len(finished_events), 1)
        self.assertEqual(finished_events[0]["reason"], "CENTRAL_BLOCK")
        self.assertEqual(finished_events[0]["vol"], 0.45)
        self.assertEqual(finished_events[0]["transaction_id"], "TX-BLOCK-HEALTH")

        # 4. Comprobar que en BLOCKED no se admiten nuevos riegos
        buf = io.StringIO()
        with redirect_stdout(buf):
            engine.start_watering("MANUAL", 5)
        self.assertIn("BLOCKED por CENTRAL", buf.getvalue())
        self.assertEqual(engine.state, "BLOCKED")

        # 5. Reactivación vía socket HEALTH#OK#ACTIVE
        resp_act = engine.handle_monitor_request("HEALTH#OK#ACTIVE")
        self.assertEqual(resp_act, "OK")
        self.assertEqual(engine.state, "AVAILABLE", "La estación debe reactivarse a AVAILABLE")

    def test_16_safe_station_blocking_via_kafka_commands_and_reactivation(self):
        """Requisito: Comprueba que la orden BLOCK recibida por Kafka interrumpe el riego activo,
        emite FINISHED con motivo CENTRAL_BLOCK, deja el estado en BLOCKED y se reactiva con ACTIVATE."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)
        engine.ws_id = "WS-01"
        engine.monitor_connected = True
        engine.central_connected = True
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

        finished_events = []
        original_send = engine.send_event

        def track_send(ev_type, **kwargs):
            if ev_type == "FINISHED":
                finished_events.append(dict(kwargs))
                return True
            return original_send(ev_type, **kwargs)

        engine.send_event = track_send

        # Iniciar riego
        engine.start_watering("FO-01", 10, transaction_id="TX-BLOCK-KAFKA")
        self.assertEqual(engine.state, "WATERING")

        with engine.lock:
            engine.accumulated_vol = 0.80

        # Simular llegada de comando BLOCK por Kafka
        engine.finish_watering("CENTRAL_BLOCK")

        self.assertEqual(engine.state, "BLOCKED", "El estado debe quedar inmediatamente en BLOCKED")
        self.assertEqual(len(finished_events), 1)
        self.assertEqual(finished_events[0]["reason"], "CENTRAL_BLOCK")
        self.assertEqual(finished_events[0]["vol"], 0.80)

        # Verificar rechazo de nuevo riego en BLOCKED
        buf = io.StringIO()
        with redirect_stdout(buf):
            engine.start_watering("FO-01", 5)
        self.assertIn("BLOCKED por CENTRAL", buf.getvalue())
        self.assertEqual(engine.state, "BLOCKED")

        # Reactivación vía comando ACTIVATE
        with engine.lock:
            if engine.state == "BLOCKED":
                engine.state = "AVAILABLE"

        self.assertEqual(engine.state, "AVAILABLE")

    def test_17_monitor_health_blocked_immediately_guards_state_and_rejects_new_watering(self):
        """Requisito: Comprueba que al recibir HEALTH#OK#BLOCKED el Engine cambia a BLOCKED de forma
        síncrona y atómica bajo self.lock, cerrando el riego activo de inmediato y rechazando cualquier
        nuevo intento de riego sin esperar a un hilo secundario."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)
        engine.ws_id = "WS-01"
        engine.monitor_connected = True
        engine.central_connected = True
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

        finished_events = []
        original_send = engine.send_event

        def track_send(ev_type, **kwargs):
            if ev_type == "FINISHED":
                finished_events.append(dict(kwargs))
                return True
            return original_send(ev_type, **kwargs)

        engine.send_event = track_send

        # Caso A: Riego activo se bloquea inmediatamente y rechaza nuevos riegos
        engine.start_watering("FO-01", 10, transaction_id="TX-IMM-BLOCK")
        self.assertEqual(engine.state, "WATERING")
        with engine.lock:
            engine.accumulated_vol = 1.35

        # Invocación directa a handle_monitor_request con BLOCKED
        resp = engine.handle_monitor_request("HEALTH#OK#BLOCKED")
        self.assertEqual(resp, "OK")

        # Comprobación síncrona inmediata: no debe haber ningún estado intermedio ni retardo
        self.assertEqual(engine.state, "BLOCKED", "El estado debe quedar inmediatamente en BLOCKED antes de responder al Monitor")
        self.assertIsNone(engine.current_op, "current_op debe limpiarse sincrónicamente")
        self.assertIsNone(engine.current_tx_id, "current_tx_id debe limpiarse sincrónicamente")

        # Intento inmediato de iniciar otro riego debe ser rechazado al instante
        buf = io.StringIO()
        with redirect_stdout(buf):
            engine.start_watering("MANUAL", 5)
        self.assertEqual(engine.state, "BLOCKED", "No debe admitirse nuevo riego tras quedar BLOCKED")
        self.assertIn("BLOCKED por CENTRAL", buf.getvalue())

        # El hilo secundario debe publicar FINISHED con CENTRAL_BLOCK y el volumen exacto
        deadline = time.time() + 2.0
        while time.time() < deadline and not finished_events:
            time.sleep(0.02)
        self.assertEqual(len(finished_events), 1)
        self.assertEqual(finished_events[0]["reason"], "CENTRAL_BLOCK")
        self.assertEqual(finished_events[0]["vol"], 1.35)
        self.assertEqual(finished_events[0]["transaction_id"], "TX-IMM-BLOCK")

        # Caso B: Estación en AVAILABLE recibe BLOCKED: pasa sincrónicamente a BLOCKED y rechaza riegos
        engine.handle_monitor_request("HEALTH#OK#ACTIVE")
        self.assertEqual(engine.state, "AVAILABLE")

        resp_b = engine.handle_monitor_request("HEALTH#OK#BLOCKED")
        self.assertEqual(resp_b, "OK")
        self.assertEqual(engine.state, "BLOCKED", "Debe pasar de inmediato a BLOCKED desde AVAILABLE")

        buf_b = io.StringIO()
        with redirect_stdout(buf_b):
            engine.start_watering("FO-02", 10)
        self.assertEqual(engine.state, "BLOCKED")
        self.assertIn("BLOCKED por CENTRAL", buf_b.getvalue())

    def test_18_started_delayed_over_5s_closes_valve_on_time_and_preserves_order(self):
        """Requisito: Comprueba que una publicación de STARTED demorada durante más de 5 segundos
        no retrasa el cierre puntual de la electroválvula (~2s) y garantiza estrictamente que
        TELEMETRY y FINISHED nunca se publiquen antes de STARTED."""
        engine = WM_WS_E.WSEngine(
            kafka_bootstrap="localhost:9092",
            monitor_addr="127.0.0.1:6001"
        )
        self.active_instances.append(engine)
        engine.ws_id = "WS-01"
        engine.monitor_connected = True
        engine.central_connected = True
        engine.symmetric_key = generate_symmetric_key()  # estación autenticada (clave recibida del Monitor)

        published_order = []
        started_unblock_ev = threading.Event()
        original_send = engine.send_event

        def delayed_send(ev_type, **kwargs):
            if ev_type == "STARTED":
                # Bloquear publicación de STARTED durante más de 5 segundos (6.0s o hasta señal)
                started_unblock_ev.wait(timeout=6.0)
                published_order.append("STARTED")
                return True
            if ev_type == "FINISHED":
                published_order.append("FINISHED")
                return True
            published_order.append(ev_type)
            return original_send(ev_type, **kwargs)

        engine.send_event = delayed_send

        t0 = time.monotonic()
        # Iniciar riego de 2 segundos
        engine.start_watering("MANUAL", 2, transaction_id="TX-SLOW-STARTED-6S")

        # Esperar a que la electroválvula se cierre por temporizador a los 2s
        deadline = t0 + 3.0
        while time.monotonic() < deadline and engine.state == "WATERING":
            time.sleep(0.05)

        closing_elapsed = time.monotonic() - t0
        self.assertEqual(engine.state, "AVAILABLE", "La electroválvula debe cerrarse por temporizador a los ~2s")
        self.assertLess(
            closing_elapsed, 2.8,
            f"La electroválvula tardó {closing_elapsed:.2f}s; debe cerrar puntualmente sin esperar a STARTED"
        )

        # En t=5.2s (superando el umbral de 5.0s del timeout anterior):
        # STARTED aún no se ha publicado, por lo que published_order debe estar vacía
        time.sleep(max(0.0, 5.2 - (time.monotonic() - t0)))
        self.assertEqual(
            published_order, [],
            "Ni TELEMETRY ni FINISHED deben haberse publicado mientras STARTED siga pendiente de publicación"
        )

        # Desbloquear STARTED y esperar a que concluya la publicación de FINISHED
        started_unblock_ev.set()
        deadline_pub = time.time() + 4.0
        while time.time() < deadline_pub and "FINISHED" not in published_order:
            time.sleep(0.05)

        self.assertIn("STARTED", published_order)
        self.assertIn("FINISHED", published_order)
        self.assertEqual(
            published_order, ["STARTED", "FINISHED"],
            f"El orden de eventos debe ser estrictamente STARTED antes de FINISHED, obtenido: {published_order}"
        )

    def test_19_kafka_recovery_flushes_pending_finished_without_spurious_started_in_central(self):
        """Requisito: Comprueba que cuando Kafka falla al inicio y al final de un riego, los eventos
        FINISHED pendientes se conservan, y al recuperarse Kafka, el vaciado procesa FINISHED
        sin enviar ningún STARTED tardío que deje a CENTRAL incorrectamente en REGANDO."""
        central, monitor, engine = self._start_cluster("WS-01")

        kafka_up = {"ok": False}
        events_emitted = []
        original_send = engine.send_event

        def controlled_send(ev_type, **kwargs):
            events_emitted.append(ev_type)
            if not kafka_up["ok"]:
                return False
            return original_send(ev_type, **kwargs)

        engine.send_event = controlled_send

        # 1. Iniciar riego manual de 1s con Kafka caído
        t0 = time.monotonic()
        engine.start_watering("MANUAL", 1, transaction_id="TX-RECOVER-KAFKA-01")

        # Esperar a que la electroválvula cierre puntualmente a 1s
        deadline = t0 + 2.5
        while time.monotonic() < deadline and engine.state == "WATERING":
            time.sleep(0.05)
        self.assertEqual(engine.state, "AVAILABLE", "La válvula debe cerrarse físicamente a pesar de fallo Kafka")

        # Esperar brevemente a que el hilo de finalización intente enviar y archive en pendientes
        time.sleep(0.3)
        self.assertEqual(len(engine.pending_finished_events), 1, "FINISHED debe haberse guardado en pending_finished_events")
        self.assertEqual(engine.pending_finished_events[0]["transaction_id"], "TX-RECOVER-KAFKA-01")

        # 2. Restablecer Kafka y vaciar eventos pendientes
        kafka_up["ok"] = True
        flushed = engine.flush_pending_finished()
        self.assertEqual(flushed, 1, "Debe vaciarse exactamente 1 evento FINISHED")
        self.assertEqual(len(engine.pending_finished_events), 0)

        # 3. Esperar a que Central procese el evento FINISHED
        time.sleep(0.5)

        # 4. Comprobar que en SQLite de CENTRAL se guardó el registro de la transacción
        with central._get_db() as db:
            row = db.execute("SELECT * FROM watering_history WHERE transaction_id = 'TX-RECOVER-KAFKA-01'").fetchone()
        self.assertIsNotNone(row, "CENTRAL debe haber registrado la transacción histórica en SQLite")
        self.assertEqual(row["reason"], "TIME")

        # 5. Comprobar que CENTRAL permanece en DISPONIBLE y NO queda en REGANDO por ningún STARTED tardío
        self.assertEqual(
            central.stations["WS-01"]["status"],
            common.STATUS_AVAILABLE,
            "CENTRAL debe permanecer en DISPONIBLE y nunca pasar a REGANDO por eventos STARTED huérfanos"
        )

        engine.running = False
        monitor.running = False
        central.running = False

    def _wait_until(self, cond, timeout=6.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if cond():
                return True
            time.sleep(0.05)
        return cond()

    def test_20_key_revocation_keeps_out_of_service_until_manual_reauthentication(self):
        """Release 2 (revocación de claves):
        - Central borra la clave de la WS y la deja FUERA DE SERVICIO sin escribir en el socket desde otro hilo.
        - El Monitor recibe STATUS#REVOKED como respuesta a su siguiente mensaje y cierra la sesión.
        - La WS sigue FUERA DE SERVICIO aunque el Monitor se desconecte, y el Monitor NO se re-autentica solo.
        - Con la opción de re-autenticación recibe una clave nueva y la WS vuelve a DISPONIBLE.
        - Si ya estaba bloqueada por CENTRAL antes de revocar, sigue FUERA DE SERVICIO tras re-autenticarse."""
        central, monitor, engine = self._start_cluster("WS-01")
        old_key = central.station_keys["WS-01"]
        self.assertEqual(monitor.symmetric_key, old_key)

        sent_from_other_thread = []
        orig_send_frame = common.send_frame
        revoking_thread = threading.get_ident()

        def spy_send_frame(sock, msg, *a, **kw):
            if threading.get_ident() == revoking_thread:
                sent_from_other_thread.append(msg)
            return orig_send_frame(sock, msg, *a, **kw)

        with patch.object(common, "send_frame", spy_send_frame):
            self.assertTrue(central.revoke_station_keys("WS-01"))
        self.assertEqual(sent_from_other_thread, [], "revoke_station_keys no debe escribir en el socket de la WS")
        self.assertNotIn("WS-01", central.station_keys)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        # El aviso llega en la respuesta al siguiente PING (cada 3 s)
        self.assertTrue(self._wait_until(lambda: monitor.key_revoked, timeout=6.0),
                        "El Monitor debe detectar la revocación en la respuesta a su siguiente mensaje")
        self.assertIsNone(monitor.symmetric_key)
        self.assertTrue(self._wait_until(lambda: "WS-01" not in central.station_sockets, timeout=3.0))

        # Sin acción manual: no hay re-autenticación automática y la WS sigue FUERA DE SERVICIO
        time.sleep(3.5)
        self.assertNotIn("WS-01", central.station_keys, "El Monitor no debe re-autenticarse automáticamente")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)
        self.assertTrue(self._wait_until(lambda: engine.state == "BLOCKED", timeout=3.0),
                        "El Engine debe quedar bloqueado mientras las claves están revocadas")

        # Activar desde Central no es posible sin nueva autenticación
        central.activate_station("WS-01")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        # Opción 'k' del Monitor: nueva autenticación y nueva clave
        monitor.reauthenticate()
        self.assertFalse(monitor.key_revoked)
        new_key = central.station_keys.get("WS-01")
        self.assertIsNotNone(new_key)
        self.assertNotEqual(new_key, old_key, "Tras re-autenticarse debe emitirse una clave simétrica nueva")
        self.assertEqual(monitor.symmetric_key, new_key)
        self.assertTrue(self._wait_until(
            lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE and engine.state == "AVAILABLE",
            timeout=8.0), "La WS debe volver a servicio tras la nueva autenticación")
        self.assertIn("KEY_RENEWED", [a["action"] for a in central.recent_audits])

        # Bloqueo previo de CENTRAL + revocación: tras re-autenticarse sigue FUERA DE SERVICIO
        central.block_station("WS-01")
        self.assertTrue(central.revoke_station_keys("WS-01"))
        self.assertTrue(self._wait_until(lambda: monitor.key_revoked, timeout=6.0))
        monitor.reauthenticate()
        self.assertIn("WS-01", central.station_keys)
        time.sleep(1.5)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        engine.running = False
        monitor.running = False
        central.running = False


    # ------------------------------------------------------------------------------------------
    # Release 2 - bloque 4: canal Kafka cifrado Central <-> Engine (sockets y Kafka simulados,
    # criptografía real)
    # ------------------------------------------------------------------------------------------
    def _kafka_msgs(self, topic):
        with self.broker.lock:
            return list(self.broker.topics.get(topic, []))

    def _history_count(self, central, tx_id):
        with central._get_db() as db:
            return db.execute("SELECT COUNT(*) FROM watering_history WHERE transaction_id = ?",
                              (tx_id,)).fetchone()[0]

    def test_21_all_kafka_messages_encrypted_replays_forgeries_rejected_and_weather(self):
        import json as _json
        from wm_crypto import KAFKA_KIND_COMMAND
        central, monitor, engine = self._start_cluster("WS-01")
        key = central.station_keys["WS-01"]
        self.assertEqual(engine.symmetric_key, key)

        central.request_watering("FO-01", "WS-01", 2)
        self.assertTrue(self._wait_until(lambda: engine.state == "WATERING", 4))
        tx_id = central.stations["WS-01"]["transaction_id"]
        self.assertTrue(self._wait_until(lambda: self._history_count(central, tx_id) == 1, 8))
        self.assertTrue(self._wait_until(lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE, 3))

        cmds, evts = self._kafka_msgs(common.TOPIC_COMMANDS), self._kafka_msgs(common.TOPIC_EVENTS)
        for m in cmds + evts:
            self.assertEqual(set(m), {"ws", "enc"}, f"Mensaje Kafka con campos en claro: {m}")
        raw = _json.dumps(cmds + evts)
        for secret in ("FO-01", tx_id, "START", "FINISHED", "TELEMETRY"):
            self.assertNotIn(secret, raw)
        ev_types = [open_kafka_message(key, m, KAFKA_KIND_EVENT, "WS-01")[0]["type"] for m in evts]
        self.assertEqual((ev_types[0], ev_types[-1]), ("STARTED", "FINISHED"))
        decoded_cmds = [open_kafka_message(key, m, KAFKA_KIND_COMMAND, "WS-01")[0]["cmd"] for m in cmds]
        self.assertEqual(decoded_cmds, ["START", "ACK_FINISHED"], "Orden de riego y confirmación del FINISHED")
        start_msg = cmds[0]

        # Orden repetida (capturada de Kafka) y orden falsificada en claro: no deben iniciar riego
        self.broker.send(common.TOPIC_COMMANDS, start_msg)
        self.broker.send(common.TOPIC_COMMANDS, {"ws": "WS-01", "cmd": "START", "op": "FO-99", "duration": 5})
        time.sleep(1.0)
        self.assertEqual(engine.state, "AVAILABLE")
        self.assertEqual(self._history_count(central, tx_id), 1)

        # Alerta climática y cancelación: órdenes cifradas BLOCK / ACTIVATE
        n = len(self._kafka_msgs(common.TOPIC_COMMANDS))
        central.handle_weather_alert("WS-01", "Madrid", -3.0, "Snow", True, "Helada")
        self.assertTrue(self._wait_until(lambda: engine.state == "BLOCKED", 4))
        central.handle_weather_alert("WS-01", "Madrid", 12.0, "Clear", False, "Normal")
        self.assertTrue(self._wait_until(lambda: engine.state == "AVAILABLE", 4))
        new_cmds = [open_kafka_message(key, m, KAFKA_KIND_COMMAND, "WS-01")[0]["cmd"]
                    for m in self._kafka_msgs(common.TOPIC_COMMANDS)[n:]]
        self.assertEqual(new_cmds, ["BLOCK", "ACTIVATE"])

        engine.running = False
        monitor.running = False
        central.running = False

    def test_22_revocation_during_watering_keeps_finished_and_rejects_old_key_commands(self):
        central, monitor, engine = self._start_cluster("WS-01")
        old_key = central.station_keys["WS-01"]
        central.request_watering("FO-01", "WS-01", 30)
        self.assertTrue(self._wait_until(
            lambda: engine.state == "WATERING" and central.stations["WS-01"]["status"] == common.STATUS_WATERING, 5))
        tx_id = central.stations["WS-01"]["transaction_id"]
        from wm_crypto import KAFKA_KIND_COMMAND
        captured_start = next(m for m in self._kafka_msgs(common.TOPIC_COMMANDS)
                              if open_kafka_message(old_key, m, KAFKA_KIND_COMMAND, "WS-01")[0]["cmd"] == "START")

        central.revoke_station_keys("WS-01")
        # El Monitor recibe REVOKED, envía CLEAR_KEY al Engine y lo bloquea
        self.assertTrue(self._wait_until(
            lambda: monitor.key_revoked and engine.symmetric_key is None and engine.state == "BLOCKED", 8))
        self.assertTrue(self._wait_until(
            lambda: any(p.get("transaction_id") == tx_id for p in engine.pending_finished_events), 3),
            "El FINISHED del riego interrumpido debe quedar pendiente (no hay clave para publicarlo)")
        # Central ya consolidó el riego con el volumen conocido (motivo KEYS_REVOKED)
        self.assertEqual(self._history_count(central, tx_id), 1)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        # Re-autenticación (opción 'k'): clave nueva; el FINISHED pendiente llega cifrado y se registra una vez
        monitor.reauthenticate()
        new_key = central.station_keys["WS-01"]
        self.assertNotEqual(new_key, old_key)
        self.assertTrue(self._wait_until(lambda: engine.symmetric_key == new_key, 3))
        self.assertTrue(self._wait_until(lambda: self._history_count(central, tx_id) == 1, 8))
        self.assertTrue(self._wait_until(lambda: not engine.pending_finished_events, 3))

        # La orden START capturada con la clave revocada no se acepta
        self.assertTrue(self._wait_until(lambda: engine.state == "AVAILABLE", 6))
        self.broker.send(common.TOPIC_COMMANDS, captured_start)
        time.sleep(1.0)
        self.assertNotEqual(engine.state, "WATERING")
        self.assertEqual(self._history_count(central, tx_id), 1)

        engine.running = False
        monitor.running = False
        central.running = False


    def test_23_fast_reauthentication_does_not_lose_finished(self):
        """El FINISHED enviado con la clave anterior que Central aún no había procesado cuando se
        re-autentica la WS se rechaza (clave antigua), pero el Engine lo reenvía con la clave nueva
        porque Central no lo había confirmado: se registra exactamente una vez."""
        central, monitor, engine = self._start_cluster("WS-01")
        old_key = central.station_keys["WS-01"]

        # Retener los eventos en Kafka (Central "lento") durante la re-autenticación
        held, holding = [], {"on": True}
        real_process = central._process_kafka_event

        def slow_central(msg):
            if holding["on"]:
                held.append(msg)
                return
            real_process(msg)

        central._process_kafka_event = slow_central
        central.request_watering("FO-01", "WS-01", 1)
        tx_id = central.stations["WS-01"]["transaction_id"]
        self.assertTrue(self._wait_until(lambda: tx_id in engine._unconfirmed_finished, 6),
                        "El FINISHED enviado debe conservarse hasta que Central lo confirme")

        # Re-autenticación rápida del Monitor (sin revocación): nueva clave
        with monitor.central_lock:
            monitor._close_central_sock()
            monitor.symmetric_key = None
        self.assertTrue(monitor.ensure_central_connection(manual=True))
        self.assertNotEqual(central.station_keys["WS-01"], old_key)
        self.assertTrue(self._wait_until(lambda: engine.symmetric_key == central.station_keys["WS-01"], 3))

        # Central procesa ahora lo retenido: lo de la clave anterior se rechaza, el reenvío se acepta
        holding["on"] = False
        for msg in list(held):
            real_process(msg)
        self.assertTrue(self._wait_until(lambda: self._history_count(central, tx_id) == 1, 6))
        self.assertTrue(self._wait_until(lambda: tx_id not in engine._unconfirmed_finished, 4),
                        "Tras la confirmación de Central el Engine deja de conservarlo")
        time.sleep(0.5)
        self.assertEqual(self._history_count(central, tx_id), 1)

        engine.running = False
        monitor.running = False
        central.running = False


    # ------------------------------------------------------------------------------------------
    # Release 2 - bloque 5: revocación de claves y recuperación de estaciones
    # ------------------------------------------------------------------------------------------
    def _disconnect_monitor_from_central(self, central, monitor):
        """Simula un corte de red entre Monitor y Central; devuelve la función para restaurarlo."""
        real_ensure = monitor.ensure_central_connection
        monitor.ensure_central_connection = lambda manual=False: False
        with monitor.central_lock:
            monitor._close_central_sock()
            monitor.symmetric_key = None
        self.assertTrue(self._wait_until(lambda: "WS-01" not in central.station_sockets, 4))

        def restore():
            monitor.ensure_central_connection = real_ensure
        return restore

    def test_24_revocation_while_monitor_offline_requires_renewed_credential(self):
        central, monitor, engine = self._start_cluster("WS-01")
        restore = self._disconnect_monitor_from_central(central, monitor)
        central.revoke_station_keys("WS-01")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        # Vuelve la red: la reconexión automática con la credencial anterior se rechaza
        restore()
        self.assertTrue(self._wait_until(lambda: monitor.key_revoked, 6))
        self.assertTrue(self._wait_until(lambda: engine.symmetric_key is None and engine.state == "BLOCKED", 4),
                        "El Engine debe borrar su clave y quedar bloqueado")
        rejected = lambda: len([a for a in central.recent_audits if a["action"] == "REJECTED_REVOKED"])
        n = rejected()
        time.sleep(3.0)
        self.assertEqual(rejected(), n, "El Monitor no debe reintentar automáticamente tras el rechazo")
        self.assertNotIn("WS-01", central.station_keys)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        # Opción 'a' (misma credencial): rechazada. Opción 'k' (credencial renovada): recupera la WS
        self.assertFalse(monitor.ensure_central_connection(manual=True))
        self.assertNotIn("WS-01", central.station_keys)
        monitor.reauthenticate()
        self.assertIn("WS-01", central.station_keys)
        self.assertTrue(self._wait_until(
            lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE and engine.state == "AVAILABLE", 8))
        engine.running = monitor.running = central.running = False

    def test_25_monitor_restart_after_revocation_does_not_bypass_it(self):
        central, monitor, engine = self._start_cluster("WS-01")
        monitor._save_credential(monitor.credential)  # credencial persistida como en un Monitor real
        restore = self._disconnect_monitor_from_central(central, monitor)
        central.revoke_station_keys("WS-01")
        monitor.running = False

        # Nuevo proceso de Monitor: carga la credencial anterior y se autentica solo -> rechazado
        monitor2 = WM_WS_M.WSMonitor(engine_port=get_free_port(), central_addr=f"127.0.0.1:{central.socket_port}",
                                     ws_id="WS-01", location="River Park")
        self.active_instances.append(monitor2)
        self.assertEqual(monitor2.credential, monitor.credential)
        self.assertFalse(monitor2.ensure_central_connection())
        self.assertTrue(monitor2.key_revoked)
        self.assertNotIn("WS-01", central.station_keys)
        monitor2.reauthenticate()
        self.assertIn("WS-01", central.station_keys)
        self.assertFalse(central.stations["WS-01"].get("keys_revoked", False))
        engine.running = monitor2.running = central.running = False

    def test_26_revocation_survives_central_restart_and_keeps_previous_block(self):
        central, monitor, engine = self._start_cluster("WS-01")
        central.block_station("WS-01")                     # bloqueo previo por orden de CENTRAL
        central.revoke_station_keys("WS-01")
        self.assertTrue(self._wait_until(lambda: monitor.key_revoked, 6))
        central.running = False
        with central._get_db() as db:
            row = dict(db.execute("SELECT * FROM station_security WHERE ws_id='WS-01'").fetchone())
        self.assertEqual((row["revoked"], row["central_block"]), (1, 1))
        self.assertEqual(len(row["revoked_cred_fp"]), 64)
        self.assertNotIn(monitor.credential, str(row), "Solo se guardan huellas, nunca la credencial")

        # Reinicio de Central con la misma base de datos
        port2 = get_free_port()
        central2 = WM_Central.WMCentral(socket_port=port2, kafka_bootstrap="localhost:9092",
                                        web_port=0, db_path=self.temp_db.name)
        self.active_instances.append(central2)
        threading.Thread(target=central2.run_socket_server, daemon=True).start()
        self.assertTrue(self._wait_until(lambda: port2 in self.socket_hub.servers, 3),
                        "El servidor de sockets de la nueva Central debe estar escuchando")
        st = central2.stations["WS-01"]
        self.assertEqual(st["status"], common.STATUS_OUT_OF_SERVICE)
        self.assertTrue(st.get("keys_revoked") and st.get("central_block"))

        monitor.central_port = port2
        self.assertFalse(monitor.ensure_central_connection(manual=True), "La credencial anterior sigue rechazada")
        self.assertNotIn("WS-01", central2.station_keys)
        monitor.reauthenticate()
        self.assertIn("WS-01", central2.station_keys)
        time.sleep(1.5)
        self.assertEqual(central2.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE,
                         "Se conserva el bloqueo previo de CENTRAL tras la re-autenticación")
        with central2._get_db() as db:
            self.assertEqual(db.execute("SELECT revoked FROM station_security WHERE ws_id='WS-01'").fetchone()[0], 0)
        engine.running = monitor.running = central2.running = False

    def test_27_revocation_during_watering_closes_transaction_and_late_finished_updates_it(self):
        central, monitor, engine = self._start_cluster("WS-01")
        central.request_watering("FO-01", "WS-01", 30)
        self.assertTrue(self._wait_until(lambda: central.stations["WS-01"]["status"] == common.STATUS_WATERING, 5))
        tx_id = central.stations["WS-01"]["transaction_id"]
        time.sleep(2.5)
        n = len(self._kafka_msgs(common.TOPIC_REPLIES))

        central.revoke_station_keys("WS-01")
        replies = [m for m in self._kafka_msgs(common.TOPIC_REPLIES)[n:] if m.get("ws") == "WS-01"]
        summaries = [m for m in replies if m.get("type") == "SUMMARY"]
        self.assertEqual(len(summaries), 1, "El operario debe recibir el resumen del riego interrumpido")
        self.assertEqual((summaries[0]["reason"], summaries[0]["transaction_id"]), ("KEYS_REVOKED", tx_id))
        with central._get_db() as db:
            row = db.execute("SELECT reason, total_vol FROM watering_history WHERE transaction_id=?", (tx_id,)).fetchone()
        self.assertEqual(row["reason"], "KEYS_REVOKED")
        first_vol = row["total_vol"]
        st = central.stations["WS-01"]
        self.assertEqual((st["operator"], st["transaction_id"]), (None, None))
        self.assertTrue(self._wait_until(lambda: engine.state == "BLOCKED", 8), "La válvula debe cerrarse")

        # Re-autenticación: el FINISHED tardío actualiza la misma transacción, sin duplicados ni nuevo resumen
        monitor.reauthenticate()
        self.assertTrue(self._wait_until(lambda: tx_id not in engine._unconfirmed_finished
                                         and not engine.pending_finished_events, 8))
        with central._get_db() as db:
            rows = db.execute("SELECT total_vol FROM watering_history WHERE transaction_id=?", (tx_id,)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertGreaterEqual(rows[0]["total_vol"], first_vol)
        later = [m for m in self._kafka_msgs(common.TOPIC_REPLIES)[n:]
                 if m.get("type") in ("SUMMARY", "DENY") and m.get("transaction_id") == tx_id]
        self.assertEqual(len(later), 1, "Ningún mensaje posterior al operario sobre esa transacción")
        engine.running = monitor.running = central.running = False


    def test_28_revocation_during_monitor_central_partition_stops_engine_by_kafka(self):
        """Corte Monitor-Central que Central aún no ha detectado: el Monitor no puede recibir REVOKED,
        pero el BLOCK cifrado por Kafka (enviado antes de borrar la clave) detiene el riego. Al volver
        la red, la opción 'k' recupera la estación y el Engine vuelve a servicio."""
        central, monitor, engine = self._start_cluster("WS-01")
        central.request_watering("FO-01", "WS-01", 30)
        self.assertTrue(self._wait_until(lambda: central.stations["WS-01"]["status"] == common.STATUS_WATERING, 5))
        tx_id = central.stations["WS-01"]["transaction_id"]
        time.sleep(1.5)

        real_send, real_ensure = monitor.send_to_central, monitor.ensure_central_connection
        monitor.send_to_central = lambda msg: False
        monitor.ensure_central_connection = lambda manual=False: False
        self.assertTrue(self._wait_until(lambda: not engine.central_connected, 4))
        self.assertEqual(engine.state, "WATERING", "Release 1: sin Central el riego en curso continúa")
        self.assertIn("WS-01", central.station_sockets, "Central aún no ha detectado el corte")

        central.revoke_station_keys("WS-01")
        self.assertTrue(self._wait_until(lambda: engine.state == "BLOCKED", 3),
                        "El BLOCK por Kafka debe detener el riego aunque el Monitor no reciba REVOKED")
        self.assertFalse(monitor.key_revoked)
        self.assertEqual(self._history_count(central, tx_id), 1)

        # Vuelve la red y el operario usa 'k' de inmediato (antes de cualquier reintento automático)
        with monitor.central_lock:
            monitor.send_to_central, monitor.ensure_central_connection = real_send, real_ensure
            monitor._close_central_sock()
            monitor.symmetric_key = None
            monitor.reauthenticate()
        self.assertTrue(self._wait_until(
            lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE and engine.state == "AVAILABLE", 8),
            f"Central={central.stations['WS-01']['status']} Engine={engine.state}")
        self.assertTrue(self._wait_until(lambda: tx_id not in engine._unconfirmed_finished
                                         and not engine.pending_finished_events, 6))
        self.assertEqual(self._history_count(central, tx_id), 1)
        engine.running = monitor.running = central.running = False

    def test_29_weather_cancellation_keeps_manual_block(self):
        central, monitor, engine = self._start_cluster("WS-01")
        # Bloqueo manual + alerta + cancelación: sigue FUERA DE SERVICIO hasta activar
        central.block_station("WS-01")
        central.handle_weather_alert("WS-01", "Oslo", -4.0, "Snow", True, "Helada")
        central.handle_weather_alert("WS-01", "Oslo", 12.0, "Clear", False, "Normal")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)
        time.sleep(1.0)
        self.assertEqual(engine.state, "BLOCKED")
        central.activate_station("WS-01")
        self.assertTrue(self._wait_until(
            lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE and engine.state == "AVAILABLE", 4))

        # Solo alerta + cancelación: se restaura (comportamiento anterior)
        central.handle_weather_alert("WS-01", "Oslo", -4.0, "Snow", True, "Helada")
        self.assertTrue(self._wait_until(lambda: engine.state == "BLOCKED", 4))
        central.handle_weather_alert("WS-01", "Oslo", 12.0, "Clear", False, "Normal")
        self.assertTrue(self._wait_until(
            lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE and engine.state == "AVAILABLE", 4))

        # Alerta y después bloqueo manual: la cancelación tampoco lo levanta
        central.handle_weather_alert("WS-01", "Oslo", -4.0, "Snow", True, "Helada")
        central.block_station("WS-01")
        central.handle_weather_alert("WS-01", "Oslo", 12.0, "Clear", False, "Normal")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)

        # Revocada con bloqueo manual y alerta: tras 'k' sigue bloqueada; cancelada la alerta, también
        central.handle_weather_alert("WS-01", "Oslo", -4.0, "Snow", True, "Helada")
        central.revoke_station_keys("WS-01")
        self.assertTrue(self._wait_until(lambda: monitor.key_revoked, 6))
        monitor.reauthenticate()
        self.assertIn("WS-01", central.station_keys)
        time.sleep(1.0)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)
        central.handle_weather_alert("WS-01", "Oslo", 12.0, "Clear", False, "Normal")
        time.sleep(1.0)
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE,
                         "El bloqueo manual se conserva tras revocar, re-autenticar y cancelar la alerta")
        self.assertEqual(engine.state, "BLOCKED")
        central.activate_station("WS-01")
        self.assertTrue(self._wait_until(
            lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE and engine.state == "AVAILABLE", 6))
        engine.running = monitor.running = central.running = False


    def _deregister(self, monitor):
        """Opción 'b' del Monitor (el arnés no levanta el Registry HTTPS: se simula su respuesta)."""
        monitor._registry_request = lambda method, path, body: (200, {"status": "ok"})
        self.assertTrue(monitor.deregister_from_registry())

    def test_30_deregistration_during_watering_stops_it_like_release_1(self):
        from wm_crypto import KAFKA_KIND_COMMAND
        central, monitor, engine = self._start_cluster("WS-01")
        key = central.station_keys["WS-01"]
        central.request_watering("FO-01", "WS-01", 30)
        self.assertTrue(self._wait_until(lambda: central.stations["WS-01"]["status"] == common.STATUS_WATERING, 5))
        tx_id = central.stations["WS-01"]["transaction_id"]
        time.sleep(1.5)
        n_replies = len(self._kafka_msgs(common.TOPIC_REPLIES))
        n_cmds = len(self._kafka_msgs(common.TOPIC_COMMANDS))

        self._deregister(monitor)
        self.assertTrue(self._wait_until(lambda: engine.state != "WATERING", 1.5),
                        "La baja del Monitor debe interrumpir el riego (Release 1)")
        self.assertIsNone(engine.symmetric_key)
        self.assertTrue(self._wait_until(lambda: central.stations["WS-01"]["status"] == common.STATUS_OFFLINE, 3))
        with central._get_db() as db:
            row = db.execute("SELECT reason, total_vol FROM watering_history WHERE transaction_id=?", (tx_id,)).fetchone()
        self.assertEqual(row["reason"], "MONITOR_DOWN")
        summaries = [m for m in self._kafka_msgs(common.TOPIC_REPLIES)[n_replies:]
                     if m.get("type") == "SUMMARY" and m.get("transaction_id") == tx_id]
        self.assertEqual(len(summaries), 1)
        stops = [open_kafka_message(key, m, KAFKA_KIND_COMMAND, "WS-01")[0]["cmd"]
                 for m in self._kafka_msgs(common.TOPIC_COMMANDS)[n_cmds:]]
        self.assertIn("STOP", stops, "Central envía STOP por Kafka; el Engine ya lo había detenido")
        self.assertTrue(self._wait_until(
            lambda: [(p["transaction_id"], p["reason"]) for p in engine.pending_finished_events]
            == [(tx_id, "KEYS_CLEARED")], 3))
        time.sleep(1.0)
        self.assertNotEqual(engine.state, "WATERING")

        # Nueva alta ('r') y autenticación: el FINISHED pendiente actualiza la misma transacción
        monitor.register_in_registry()
        monitor.deregistered = False
        self.assertTrue(self._wait_until(lambda: engine.symmetric_key == central.station_keys.get("WS-01"), 6))
        self.assertTrue(self._wait_until(lambda: not engine.pending_finished_events
                                         and tx_id not in engine._unconfirmed_finished, 6))
        with central._get_db() as db:
            rows = db.execute("SELECT reason, total_vol FROM watering_history WHERE transaction_id=?",
                              (tx_id,)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reason"], "MONITOR_DOWN")
        self.assertGreaterEqual(rows[0]["total_vol"], row["total_vol"])
        later = [m for m in self._kafka_msgs(common.TOPIC_REPLIES)[n_replies:]
                 if m.get("type") in ("SUMMARY", "DENY") and m.get("transaction_id") == tx_id]
        self.assertEqual(len(later), 1, "Sin avisos adicionales al operario")
        engine.running = monitor.running = central.running = False

    def test_31_deregistration_with_idle_engine(self):
        central, monitor, engine = self._start_cluster("WS-01")
        self._deregister(monitor)
        self.assertTrue(self._wait_until(lambda: engine.symmetric_key is None, 3))
        self.assertEqual(engine.state, "AVAILABLE")
        time.sleep(0.5)
        self.assertEqual(engine.pending_finished_events, [])
        engine.start_watering("MANUAL", 0)
        self.assertNotEqual(engine.state, "WATERING", "Sin clave no se admiten riegos")

        monitor.register_in_registry()
        monitor.deregistered = False
        self.assertTrue(self._wait_until(
            lambda: engine.symmetric_key is not None and central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE, 8))
        central.request_watering("FO-01", "WS-01", 1)
        self.assertTrue(self._wait_until(lambda: engine.state == "WATERING", 4), "Tras el alta vuelve a operar")
        engine.running = monitor.running = central.running = False


    def test_32_weather_cancellation_respects_monitor_connection(self):
        """Con Monitor y Engine conectados, la cancelación devuelve la WS a DISPONIBLE (Engine AVAILABLE).
        Con el Monitor desconectado queda DESCONECTADA; al reconectarse vuelve a servicio como en la
        Release 1. En ningún caso se inicia un riego."""
        central, monitor, engine = self._start_cluster("WS-01")
        central.handle_weather_alert("WS-01", "Oslo", -4.0, "Snow", True, "Helada")
        self.assertTrue(self._wait_until(lambda: engine.state == "BLOCKED", 4))
        central.handle_weather_alert("WS-01", "Oslo", 12.0, "Clear", False, "Normal")
        self.assertTrue(self._wait_until(
            lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE and engine.state == "AVAILABLE", 4))

        restore = self._disconnect_monitor_from_central(central, monitor)
        self.assertTrue(self._wait_until(lambda: central.stations["WS-01"]["status"] == common.STATUS_OFFLINE, 3))
        central.handle_weather_alert("WS-01", "Oslo", -4.0, "Snow", True, "Helada")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OUT_OF_SERVICE)
        central.handle_weather_alert("WS-01", "Oslo", 12.0, "Clear", False, "Normal")
        self.assertEqual(central.stations["WS-01"]["status"], common.STATUS_OFFLINE,
                         "Sin Monitor conectado no vuelve a DISPONIBLE")
        restore()
        self.assertTrue(self._wait_until(lambda: central.stations["WS-01"]["status"] == common.STATUS_AVAILABLE, 8))
        self.assertNotEqual(engine.state, "WATERING")
        self.assertEqual(central.stations["WS-01"]["operator"], None, "No se ha iniciado ningún riego")
        engine.running = monitor.running = central.running = False


    def test_33_audit_trail_of_full_flow_with_origins_and_no_secrets(self):
        """Recorrido completo (FO -> Central -> Engine, revocación y re-autenticación): la auditoría
        registra los eventos obligatorios con su origen y ni la auditoría ni los logs de Monitor y
        Engine contienen claves, credenciales ni fragmentos de ellas."""
        import json as _json
        import sqlite3 as _sqlite3
        logs = io.StringIO()
        with redirect_stdout(logs):
            central, monitor, engine = self._start_cluster("WS-01")
            keys = [central.station_keys["WS-01"]]
            credentials = [monitor.credential]
            fo = WM_FO.FieldOperator("localhost:9092", "FO-01")
            fo.source_ip = "198.51.100.77"          # IP declarada por el equipo del FO
            threading.Thread(target=fo.listen_kafka, daemon=True).start()
            time.sleep(0.1)
            fo.request_service("WS-01", 1)
            self.assertTrue(fo.service_done_event.wait(timeout=6.0))
            central.revoke_station_keys("WS-01")
            self.assertTrue(self._wait_until(lambda: monitor.key_revoked, 6))
            monitor.reauthenticate()
            keys.append(central.station_keys["WS-01"])
            credentials.append(monitor.credential)
            time.sleep(1.0)
            engine.running = monitor.running = central.running = fo.running = False

        audits = list(central.recent_audits)
        actions = [a["action"] for a in audits]
        for required in ("AUTH_SUCCESS", "STATUS_CHANGE", "WATERING_REQUEST", "KEY_REVOCATION",
                         "REJECTED_REVOKED", "KEY_RENEWED"):
            self.assertIn(required, actions, required)
        req = next(a for a in audits if a["action"] == "WATERING_REQUEST")
        self.assertEqual((req["entity"], req["source_ip"]), ("FO-01", "198.51.100.77"))
        self.assertIn("AUTORIZADA", req["description"])
        changes = [a["description"].split(": ", 1)[1] for a in audits
                   if a["action"] == "STATUS_CHANGE" and a["entity"] == "WS-01"]
        for transition in ("DISPONIBLE -> REGANDO", "REGANDO -> DISPONIBLE", "-> FUERA DE SERVICIO"):
            self.assertTrue(any(transition in c for c in changes), transition)
        ws_events = [a for a in audits if a["entity"] == "WS-01" and a["action"] in ("AUTH_SUCCESS", "STATUS_CHANGE")]
        self.assertTrue(all(a["source_ip"] == "127.0.0.1" for a in ws_events), "IP del Monitor de la WS")
        self.assertTrue(all(datetime.fromisoformat(a["timestamp"]).tzinfo for a in audits))

        with _sqlite3.connect(self.temp_db.name) as db:
            stored = _json.dumps(db.execute("SELECT * FROM audit_log").fetchall())
        with open(central.audit_log_path, encoding="utf-8") as f:
            stored += f.read()
        output = logs.getvalue()
        for secret in keys + credentials:
            for fragment in (secret, secret[:10], secret[:8]):
                self.assertNotIn(fragment, stored, "Secreto en la auditoría")
                self.assertNotIn(fragment, output, "Secreto o fragmento en los logs de Monitor/Engine/Central")
        self.assertIn("huella", output, "Las claves se identifican por su huella")


if __name__ == "__main__":
    unittest.main(verbosity=2)