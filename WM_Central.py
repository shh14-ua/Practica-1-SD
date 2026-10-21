#!/usr/bin/env python3
"""
WM_Central: Módulo central de control y gobierno de la red de riego WaterManagement.
Funcionalidades:
  - Servidor de sockets TCP para autenticación y supervisión de estaciones de riego (WM_WS_M).
  - Integración con Apache Kafka para mensajería con operarios (WM_FO) y motores (WM_WS_E).
  - Base de Datos relacional SQLite para operarios, estaciones e historial de riegos.
  - Cuadro de mandos / panel en tiempo real en consola (códigos de color ANSI requeridos).
  - Interfaz web interactiva en tiempo real (según Figuras 1 y 2 del enunciado), ideal para Railway y navegador.
  - Comandos interactivos de operador central: iniciar riego, bloquear (fuera de servicio), activar.
"""

import argparse
import http.server
import json
import os
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

from common import (
    STATUS_AVAILABLE, STATUS_WATERING, STATUS_LEAK, STATUS_OUT_OF_SERVICE, STATUS_OFFLINE,
    STATUS_STYLES, COLOR_RESET, COLOR_BOLD, COLOR_BLINK, format_status,
    server_handshake, serve_one_socket,
    TOPIC_REQUESTS, TOPIC_REPLIES, TOPIC_COMMANDS, TOPIC_EVENTS, TOPIC_STATUS,
    ensure_topics, make_producer, send_kafka, make_consumer,
    parse_addr, parse_host_port_pair
)

class WMCentral:
    def __init__(self, socket_port: int, kafka_bootstrap: str, web_port: int = 0, db_path: str = "central.db"):
        self.socket_port = socket_port
        self.kafka_bootstrap = kafka_bootstrap
        self.web_port = web_port
        self.db_path = db_path
        self.lock = threading.RLock()
        self.running = True

        # Conexiones activas de sockets con monitores {ws_id: socket}
        self.station_sockets: Dict[str, socket.socket] = {}

        # Log en memoria para el panel y web
        self.recent_logs: List[Dict[str, str]] = []

        # Inicialización de la base de datos SQLite
        self._init_db()

        # Carga en memoria del estado de las estaciones desde la base de datos
        # IMPORTANTE (según punto 1 del enunciado): Ante reinicio, todas las estaciones conocidas
        # se inicializan en estado DESCONECTADA hasta que conecten por socket.
        self.stations: Dict[str, Dict[str, Any]] = {}
        self._load_stations_from_db()

        # Inicialización de Kafka
        print(f"[CENTRAL] Configurando tópicos de Kafka en {self.kafka_bootstrap}...")
        ensure_topics(self.kafka_bootstrap)
        self.producer = make_producer(self.kafka_bootstrap)

    # --------------------------------------------------------------------------
    # GESTIÓN DE BASE DE DATOS SQLITE
    # --------------------------------------------------------------------------
    def _get_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self.lock, self._get_db() as db:
            # Tabla de operarios autorizados
            db.execute("""
                CREATE TABLE IF NOT EXISTS operators (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)
            # Tabla de estaciones de riego
            db.execute("""
                CREATE TABLE IF NOT EXISTS stations (
                    id TEXT PRIMARY KEY,
                    location TEXT NOT NULL,
                    status TEXT NOT NULL
                )
            """)
            # Tabla de auditoría / historial de riegos
            db.execute("""
                CREATE TABLE IF NOT EXISTS watering_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ws_id TEXT NOT NULL,
                    operator_id TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    total_vol REAL NOT NULL,
                    duration_sec INTEGER NOT NULL,
                    reason TEXT NOT NULL
                )
            """)

            # Operarios iniciales de ejemplo si la tabla está vacía
            cur = db.execute("SELECT COUNT(*) FROM operators")
            if cur.fetchone()[0] == 0:
                initial_ops = [
                    ("FO-01", "Operario 1 - Juan Pérez"),
                    ("FO-02", "Operario 2 - María Gómez"),
                    ("FO-03", "Operario 3 - Carlos Ruiz"),
                    ("FO-04", "Operario 4 - Laura Sanz"),
                    ("FO-05", "Operario 5 - Pedro Marín"),
                ]
                db.executemany("INSERT INTO operators VALUES (?, ?)", initial_ops)

            # Estaciones iniciales de ejemplo si la tabla está vacía
            cur = db.execute("SELECT COUNT(*) FROM stations")
            if cur.fetchone()[0] == 0:
                initial_stations = [
                    ("WS-01", "River Park Norte", STATUS_OFFLINE),
                    ("WS-02", "River Park Sur", STATUS_OFFLINE),
                    ("WS-03", "Riverside Gardens", STATUS_OFFLINE),
                    ("WS-04", "Central Park Este", STATUS_OFFLINE),
                    ("WS-05", "North Roundabout", STATUS_OFFLINE),
                ]
                db.executemany("INSERT INTO stations VALUES (?, ?, ?)", initial_stations)

            # Marcamos todas las estaciones como DESCONECTADAS al arrancar
            db.execute("UPDATE stations SET status = ?", (STATUS_OFFLINE,))
            db.commit()

    def _load_stations_from_db(self):
        with self.lock, self._get_db() as db:
            rows = db.execute("SELECT id, location, status FROM stations").fetchall()
            for r in rows:
                self.stations[r["id"]] = {
                    "id": r["id"],
                    "location": r["location"],
                    "status": STATUS_OFFLINE,
                    "flow": 0.0,
                    "vol": 0.0,
                    "operator": None,
                    "start_time": None,
                    "pending_start": False,
                    "pending_since": None,
                    "last_active": time.time(),
                }

    def _update_station_db(self, ws_id: str, location: Optional[str] = None, status: Optional[str] = None):
        with self.lock, self._get_db() as db:
            if location and status:
                db.execute(
                    "INSERT INTO stations (id, location, status) VALUES (?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET location=excluded.location, status=excluded.status",
                    (ws_id, location, status)
                )
            elif status:
                db.execute("UPDATE stations SET status = ? WHERE id = ?", (status, ws_id))
            elif location:
                db.execute("UPDATE stations SET location = ? WHERE id = ?", (location, ws_id))
            db.commit()

    def _record_watering_history(self, ws_id: str, op_id: str, start_time: str, end_time: str,
                                 total_vol: float, duration_sec: int, reason: str):
        with self.lock, self._get_db() as db:
            db.execute("""
                INSERT INTO watering_history (ws_id, operator_id, start_time, end_time, total_vol, duration_sec, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ws_id, op_id or "MANUAL", start_time or "", end_time or "", total_vol, duration_sec, reason))
            db.commit()

    # --------------------------------------------------------------------------
    # LOGGING Y NOTIFICACIONES
    # --------------------------------------------------------------------------
    def log(self, message: str, ws_id: Optional[str] = None):
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{timestamp}]"
        if ws_id:
            prefix += f" [{ws_id}]"
        formatted_log = f"{prefix} {message}"
        print(formatted_log, flush=True)

        with self.lock:
            self.recent_logs.append({
                "time": timestamp,
                "ws": ws_id or "CENTRAL",
                "message": message
            })
            if len(self.recent_logs) > 50:
                self.recent_logs.pop(0)

    def notify_operator(self, op: str, typ: str, ws_id: str, msg: str, quiet: bool = False, **extra):
        """Envía notificación al operario de campo vía Kafka (Tópico fo_replies) y a la pantalla de Central."""
        if not quiet and msg:
            self.log(f"-> Operario {op}: {msg}", ws_id=ws_id)
        if op and op not in ("CENTRAL", "MANUAL"):
            payload = {
                "op": op,
                "type": typ,
                "ws": ws_id,
                "msg": msg,
                "timestamp": time.time(),
                **extra
            }
            send_kafka(self.producer, TOPIC_REPLIES, payload)

    def send_ws_command(self, ws_id: str, cmd: str, **extra):
        """Envía una orden al motor de la estación vía Kafka (Tópico ws_commands)."""
        payload = {"ws": ws_id, "cmd": cmd, "timestamp": time.time(), **extra}
        send_kafka(self.producer, TOPIC_COMMANDS, payload)

    def set_station_status(self, ws_id: str, new_status: str):
        with self.lock:
            if ws_id in self.stations:
                old_status = self.stations[ws_id]["status"]
                self.stations[ws_id]["status"] = new_status
                self._update_station_db(ws_id, status=new_status)
                if old_status != new_status:
                    self.log(f"Estado cambiado: {old_status} -> {new_status}", ws_id=ws_id)

    # --------------------------------------------------------------------------
    # SERVIDOR DE SOCKETS (COMUNICACIÓN CON WM_WS_M)
    # --------------------------------------------------------------------------
    def run_socket_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.socket_port))
        server.listen(10)
        self.log(f"Servidor de Sockets escuchando en puerto {self.socket_port} para monitores (WM_WS_M)")

        while self.running:
            try:
                conn, addr = server.accept()
                threading.Thread(target=self._handle_station_socket, args=(conn, addr), daemon=True).start()
            except OSError:
                break

    def _handle_station_socket(self, conn: socket.socket, addr):
        station_id = None
        try:
            server_handshake(conn)
            conn.settimeout(15.0)  # Si no hay mensajes o PING en 15s, se considera desconectada

            def socket_handler(raw_msg: str) -> str:
                nonlocal station_id
                parts = raw_msg.strip().split("#")
                cmd = parts[0].upper()

                # Registro y autenticación inicial
                if cmd in ("REGISTRO", "AUTH") and len(parts) >= 2:
                    st_id = parts[1]
                    location = parts[2] if len(parts) >= 3 else "Ubicación desconocida"
                    station_id = st_id
                    with self.lock:
                        self.station_sockets[st_id] = conn
                        if st_id not in self.stations:
                            self.stations[st_id] = {
                                "id": st_id, "location": location, "status": STATUS_LEAK,
                                "flow": 0.0, "vol": 0.0, "operator": None, "start_time": None,
                                "pending_start": False,
                                "pending_since": None,
                                "last_active": time.time(),
                            }
                            self._update_station_db(st_id, location=location, status=STATUS_LEAK)
                            self.log(f"Nueva estación dada de alta: {st_id} ({location})", ws_id=st_id)
                        else:
                            st = self.stations[st_id]
                            st["location"] = location
                            st["last_active"] = time.time()

                            if st["status"] != STATUS_OUT_OF_SERVICE:
                                self.set_station_status(st_id, STATUS_LEAK)

                            self._update_station_db(
                                st_id,
                                location=location,
                                status=st["status"]
                            )
                            self.log(
                                f"Estación conectada y AUTENTICADA: estado {st['status']}",
                                ws_id=st_id
                            )
                    return f"STATUS#OK#Estacion {st_id} registrada y autenticada correctamente"

                if not station_id:
                    return "STATUS#KO#No autenticado"

                # Comprobación de latido (Keepalive / Heartbeat)
                if cmd == "PING":
                    with self.lock:
                        if station_id in self.stations:
                            self.stations[station_id]["last_active"] = time.time()
                    return "OK"

                # Detección de avería / fuga reportada por el monitor
                if cmd == "LEAK":
                    with self.lock:
                        st = self.stations.get(station_id)
                        if st:
                            was_watering = st["status"] == STATUS_WATERING
                            self.set_station_status(station_id, STATUS_LEAK)
                            self.log("¡AVERÍA / FUGA DETECTADA!", ws_id=station_id)
                            if was_watering:
                                self.notify_operator(st["operator"], "STEP", station_id,
                                                     "Fuga detectada: RIEGO INTERRUMPIDO INMEDIATAMENTE")
                                self.send_ws_command(station_id, "STOP", reason="LEAK")
                    return "OK"

                # Avería resuelta
                if cmd in ("FIXED", "OK_LEAK"):
                    with self.lock:
                        if station_id in self.stations and self.stations[station_id]["status"] == STATUS_LEAK:
                            self.set_station_status(station_id, STATUS_AVAILABLE)
                            self.log("Avería de fuga RESUELTA. Estación disponible de nuevo.", ws_id=station_id)
                    return "OK"

                return "STATUS#KO#Comando no reconocido"

            while self.running:
                serve_one_socket(conn, socket_handler)

        except (OSError, ConnectionError):
            pass
        finally:
            conn.close()
            if station_id:
                with self.lock:
                    if self.station_sockets.get(station_id) == conn:
                        del self.station_sockets[station_id]
                        st = self.stations.get(station_id)
                        
                        if st:
                            st["pending_start"] = False
                            st["pending_since"] = None
                            if st["status"] == STATUS_WATERING:
                                self.notify_operator(st["operator"], "SUMMARY", station_id,
                                                     "Estación DESCONECTADA súbitamente: riego interrumpido")
                            st["operator"] = None
                            st["flow"] = 0.0
                            st["vol"] = 0.0
                            self.set_station_status(station_id, STATUS_OFFLINE)
                            self.log("Conexión de socket cerrada: estación DESCONECTADA", ws_id=station_id)

    # --------------------------------------------------------------------------
    # GESTIÓN DE RIEGO (KAFKA: SOLICITUDES Y TELEMETRÍA)
    # --------------------------------------------------------------------------
    def request_watering(self, op: str, ws_id: str, duration: int):
        """Procesa una solicitud de riego (desde FO o desde CENTRAL). Soporta una WS o 'ALL'."""
        if ws_id.strip().upper() == "ALL":
            with self.lock:
                targets = list(self.stations.keys())
            for target in targets:
                self.request_watering(op, target, duration)
            return

        with self.lock:
            self.notify_operator(op, "STEP", ws_id, f"Petición de riego recibida para {ws_id}")

            # 1. Validar operario
            if op != "CENTRAL":
                with self._get_db() as db:
                    registered = db.execute("SELECT 1 FROM operators WHERE id = ?", (op,)).fetchone()
                    if not registered:
                        self.notify_operator(op, "DENY", ws_id, f"DENEGADO: El operario '{op}' no está registrado en CENTRAL")
                        return

            # 2. Validar estación
            st = self.stations.get(ws_id)
            if not st:
                self.notify_operator(op, "DENY", ws_id, f"DENEGADO: La estación '{ws_id}' no existe en el sistema")
                return

            # 3. Comprobar disponibilidad
            if st["pending_start"]:
                self.notify_operator(
                    op, "DENY", ws_id,
                    "DENEGADO: La estación tiene una solicitud de riego pendiente de confirmación"
                )
                return
            
            if st["status"] != STATUS_AVAILABLE:
                motivo = f"estación no disponible ({st['status']})"
                if st["status"] == STATUS_WATERING:
                    motivo = f"ya está regando para el operario {st.get('operator')}"
                elif st["status"] == STATUS_LEAK:
                    motivo = "presenta una fuga detectada"
                elif st["status"] == STATUS_OUT_OF_SERVICE:
                    motivo = "está fuera de servicio por orden central"
                elif st["status"] == STATUS_OFFLINE:
                    motivo = "está desconectada de la red"
                self.notify_operator(op, "DENY", ws_id, f"DENEGADO: {motivo}")
                return

            # 4. Autorizar e iniciar
            
            self.notify_operator(
                op, "STEP", ws_id,
                "Estación disponible. Enviando orden de apertura de electroválvula..."
            )

            st["pending_start"] = True
            st["pending_since"] = time.time()
            st["operator"] = op

            self.send_ws_command(ws_id, "START", op=op, duration=duration)
            threading.Thread(
                target=self.check_watering_timeout,
                args=(ws_id, st["pending_since"]),
                daemon=True
            ).start()

    def check_watering_timeout(self, ws_id: str, request_time: float):
        """Cancela una solicitud si el Engine no confirma en 10 segundos."""
        time.sleep(10)

        with self.lock:
            st = self.stations.get(ws_id)

            if not st or not st["pending_start"]:
                return

            if st["pending_since"] != request_time:
                return

            st["pending_start"] = False
            st["pending_since"] = None
            op = st["operator"]
            st["operator"] = None

            self.notify_operator(
                op, "DENY", ws_id,
                "DENEGADO: El Engine no confirmó el inicio del riego en 10 segundos"
            )

    def handle_ws_event(self, ev: dict):
        """Procesa eventos y telemetría emitidos por el motor de la estación (WM_WS_E)."""
        ws_id = ev.get("ws")
        ev_type = ev.get("type")
        if not ws_id or ws_id not in self.stations:
            return

        with self.lock:
            st = self.stations[ws_id]
            if ev_type == "STARTED":
                if (
                    st["status"] in (STATUS_OFFLINE, STATUS_LEAK, STATUS_OUT_OF_SERVICE)
                    and not st["pending_start"]
                ):
                    self.send_ws_command(ws_id, "STOP", reason="NOT_AVAILABLE")
                    return
                st["pending_start"] = False
                st["pending_since"] = None
                st["operator"] = ev.get("op", "MANUAL")
                st["flow"] = 0.0
                st["vol"] = 0.0
                st["start_time"] = datetime.now().isoformat()

                self.set_station_status(ws_id, STATUS_WATERING)
                self.log(f"Riego iniciado en estación (operario: {st['operator']})", ws_id=ws_id)

                if st["operator"] != "MANUAL":
                    self.notify_operator(
                        st["operator"], "AUTH", ws_id,
                        f"AUTORIZADO: Riego iniciado en {ws_id}. Confirmación recibida del Engine"
                    )
            elif ev_type == "TELEMETRY":
                st["flow"] = float(ev.get("flow", 0.0))
                st["vol"] = float(ev.get("vol", 0.0))
                st["last_active"] = time.time()
                # Notificar al operario asociado sin ensuciar el log general de CENTRAL
                self.notify_operator(st["operator"], "TELEMETRY", ws_id, "", quiet=True,
                                     flow=st["flow"], vol=st["vol"])

            elif ev_type == "FINISHED":
                st["pending_start"] = False
                st["pending_since"] = None
                total_vol = float(ev.get("vol", st["vol"]))
                dur = int(ev.get("dur", 0))
                reason = ev.get("reason", "OK")
                summary_msg = f"Riego FINALIZADO ({reason}): {total_vol:.2f} L consumidos en {dur} s"
                self.notify_operator(st["operator"], "SUMMARY", ws_id, summary_msg,
                                     total_vol=total_vol, duration=dur, reason=reason)

                # Registrar en base de datos
                self._record_watering_history(
                    ws_id=ws_id,
                    op_id=st["operator"] or "MANUAL",
                    start_time=st["start_time"] or datetime.now().isoformat(),
                    end_time=datetime.now().isoformat(),
                    total_vol=total_vol,
                    duration_sec=dur,
                    reason=reason
                )

                st["operator"] = None
                st["flow"] = 0.0
                if st["status"] == STATUS_WATERING:
                    self.set_station_status(ws_id, STATUS_AVAILABLE)

    # --------------------------------------------------------------------------
    # CONSUMIDORES Y PUBLICADORES KAFKA
    # --------------------------------------------------------------------------
    def run_kafka_requests_consumer(self):
        consumer = make_consumer(self.kafka_bootstrap, [TOPIC_REQUESTS], group_prefix="central-requests")
        for msg in consumer:
            if not self.running:
                break
            data = msg.value
            op = data.get("op")
            ws_id = data.get("ws")
            duration = int(data.get("duration", 10))
            if op and ws_id:
                self.request_watering(op, ws_id, duration)

    def run_kafka_events_consumer(self):
        consumer = make_consumer(self.kafka_bootstrap, [TOPIC_EVENTS], group_prefix="central-events")
        for msg in consumer:
            if not self.running:
                break
            self.handle_ws_event(msg.value)

    def run_kafka_status_publisher(self):
        """Publica periódicamente el snapshot del estado de todas las estaciones en Kafka (TOPIC_STATUS)."""
        while self.running:
            with self.lock:
                snapshot = [
                    {
                        "id": s["id"],
                        "location": s["location"],
                        "status": s["status"],
                        "flow": s["flow"],
                        "vol": s["vol"],
                        "operator": s["operator"]
                    }
                    for s in self.stations.values()
                ]
            send_kafka(self.producer, TOPIC_STATUS, snapshot)
            time.sleep(2.0)

    # --------------------------------------------------------------------------
    # ÓRDENES ARBITRARIAS DE CENTRAL
    # --------------------------------------------------------------------------
    def _target_stations(self, target: str) -> List[str]:
        target = target.strip().upper()
        if target == "ALL":
            return list(self.stations.keys())
        return [target] if target in self.stations else []

    def block_station(self, target: str):
        """Pone una o todas las WS fuera de servicio (estado NARANJA)."""
        stations = self._target_stations(target)
        if not stations:
            self.log(f"No se encontró la estación '{target}' para bloquear.")
            return

        for ws_id in stations:
            with self.lock:
                st = self.stations[ws_id]
                if st["status"] == STATUS_OFFLINE:
                    self.log(f"No se puede bloquear {ws_id}: está desconectada.", ws_id=ws_id)
                    continue
                if st["status"] == STATUS_WATERING:
                    self.notify_operator(st["operator"], "STEP", ws_id,
                                         "Estación bloqueada por CENTRAL: Riego detenido")
                st["pending_start"] = False
                st["pending_since"] = None
                self.set_station_status(ws_id, STATUS_OUT_OF_SERVICE)
                self.send_ws_command(ws_id, "BLOCK")
                self.log("Puesta en FUERA DE SERVICIO por orden de CENTRAL", ws_id=ws_id)

    def activate_station(self, target: str):
        """Reactiva una o todas las WS desde Fuera de Servicio a DISPONIBLE (VERDE)."""
        stations = self._target_stations(target)
        if not stations:
            self.log(f"No se encontró la estación '{target}' para activar.")
            return

        for ws_id in stations:
            with self.lock:
                st = self.stations[ws_id]
                if st["status"] == STATUS_OUT_OF_SERVICE:
                    self.set_station_status(ws_id, STATUS_AVAILABLE)
                    self.send_ws_command(ws_id, "ACTIVATE")
                    self.log("Activada y en servicio (DISPONIBLE)", ws_id=ws_id)
                elif st["status"] == STATUS_OFFLINE:
                    self.log(f"No se puede activar {ws_id}: no hay conexión física con el monitor", ws_id=ws_id)

    # --------------------------------------------------------------------------
    # PANEL DE MONITORIZACIÓN EN CONSOLA (CÓDIGOS DE COLOR ANSI)
    # --------------------------------------------------------------------------
    def print_panel(self):
        with self.lock:
            print("\n" + "=" * 95)
            print(f" {COLOR_BOLD}WM_CENTRAL — PANEL DE MONITORIZACIÓN Y CONTROL EN TIEMPO REAL{COLOR_RESET}")
            print("=" * 95)
            header = f"{'ID':<9}{'UBICACIÓN':<22}{'ESTADO':<22}{'CAUDAL(L/min)':>14}{'VOL.ACUM(L)':>14}  {'OPERARIO'}"
            print(f"{COLOR_BOLD}{header}{COLOR_RESET}")
            print("-" * 95)

            total_available = 0
            total_watering = 0
            total_leak = 0
            total_blocked = 0
            total_offline = 0

            for ws_id, st in sorted(self.stations.items()):
                stat = st["status"]
                if stat == STATUS_AVAILABLE: total_available += 1
                elif stat == STATUS_WATERING: total_watering += 1
                elif stat == STATUS_LEAK: total_leak += 1
                elif stat == STATUS_OUT_OF_SERVICE: total_blocked += 1
                elif stat == STATUS_OFFLINE: total_offline += 1

                # Formateo de estado con su color exacto
                status_colored = format_status(stat)
                # Si está regando, mostrar caudal y volumen parpadeando
                flow_str = f"{st['flow']:>14.1f}"
                vol_str = f"{st['vol']:>14.1f}"
                op_str = st['operator'] or "—"

                print(f"{ws_id:<9}{st['location'][:20]:<22}{status_colored:<31}{flow_str}{vol_str}  {op_str}")

            print("-" * 95)
            print(f" Resumen: {total_available} Disponibles | {total_watering} Regando | "
                  f"{total_leak} Fugas | {total_blocked} Fuera de Servicio | {total_offline} Desconectadas")
            print("=" * 95 + "\n")

    # --------------------------------------------------------------------------
    # INTERFAZ WEB EMBEBIDA EN TIEMPO REAL (HTML / SSE / REST)
    # --------------------------------------------------------------------------
    def run_web_server(self):
        central_self = self

        class DashboardHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                return  # Silenciar logs HTTP en consola

            def do_GET(self):
                if self.path == "/" or self.path.startswith("/index"):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(central_self._get_dashboard_html().encode("utf-8"))

                elif self.path == "/api/status":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    with central_self.lock:
                        payload = {
                            "stations": list(central_self.stations.values()),
                            "logs": central_self.recent_logs[-20:],
                            "timestamp": datetime.now().strftime("%H:%M:%S")
                        }
                    self.wfile.write(json.dumps(payload).encode("utf-8"))

                elif self.path == "/api/history":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.end_headers()
                    with central_self._get_db() as db:
                        rows = db.execute("""
                            SELECT id, ws_id, operator_id, start_time, end_time, total_vol, duration_sec, reason
                            FROM watering_history ORDER BY id DESC LIMIT 20
                        """).fetchall()
                        data = [dict(r) for r in rows]
                    self.wfile.write(json.dumps(data).encode("utf-8"))
                else:
                    self.send_response(404)
                    self.end_headers()

            def do_POST(self):
                content_len = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_len).decode("utf-8")
                try:
                    data = json.loads(body)
                except Exception:
                    data = {}

                if self.path == "/api/action":
                    action = data.get("action")
                    ws_id = data.get("ws")
                    dur = int(data.get("duration", 10))
                    op = data.get("op", "CENTRAL")

                    if action == "start" and ws_id:
                        central_self.request_watering(op, ws_id, dur)
                    elif action == "block" and ws_id:
                        central_self.block_station(ws_id)
                    elif action == "activate" and ws_id:
                        central_self.activate_station(ws_id)

                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status": "ok"}')
                else:
                    self.send_response(404)
                    self.end_headers()

        server = http.server.ThreadingHTTPServer(("0.0.0.0", self.web_port), DashboardHandler)
        self.log(f"Panel Web interactivo disponible en http://0.0.0.0:{self.web_port}/")
        server.serve_forever()

    def _get_dashboard_html(self) -> str:
        html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        if os.path.isfile(html_path):
            with open(html_path, "r", encoding="utf-8") as f:
                return f.read()
        return "<h1>Error: dashboard.html no encontrado</h1>"


    # --------------------------------------------------------------------------
    # BUCLE PRINCIPAL Y MENÚ INTERACTIVO EN CONSOLA
    # --------------------------------------------------------------------------
    def run_cli_menu(self):
        help_text = (
            f"\n{COLOR_BOLD}COMANDOS DE CENTRAL:{COLOR_RESET}\n"
            f"  {COLOR_BOLD}p{COLOR_RESET}               -> Mostrar Panel de Monitorización con colores\n"
            f"  {COLOR_BOLD}s <WS> [seg]{COLOR_RESET}    -> Iniciar orden de riego (ej: s WS-01 15)\n"
            f"  {COLOR_BOLD}b <WS|ALL>{COLOR_RESET}      -> Bloquear estación (Fuera de Servicio)\n"
            f"  {COLOR_BOLD}a <WS|ALL>{COLOR_RESET}      -> Activar estación (vuelve a DISPONIBLE)\n"
            f"  {COLOR_BOLD}h{COLOR_RESET}               -> Ver histórico de riegos en BD\n"
            f"  {COLOR_BOLD}q{COLOR_RESET}               -> Salir\n"
        )
        print(help_text)

        while self.running:
            try:
                line = input("WM_Central> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            if cmd == "p":
                self.print_panel()
            elif cmd == "s" and len(parts) >= 2:
                ws_id = parts[1].upper()
                duration = int(parts[2]) if len(parts) >= 3 else 10
                self.request_watering("CENTRAL", ws_id, duration)
            elif cmd == "b" and len(parts) >= 2:
                self.block_station(parts[1])
            elif cmd == "a" and len(parts) >= 2:
                self.activate_station(parts[1])
            elif cmd == "h":
                with self._get_db() as db:
                    rows = db.execute("SELECT * FROM watering_history ORDER BY id DESC LIMIT 10").fetchall()
                    print(f"\n{COLOR_BOLD}ÚLTIMOS RIEGOS REGISTRADOS EN BD:{COLOR_RESET}")
                    for r in rows:
                        print(f" ID:{r['id']} | WS:{r['ws_id']} | Op:{r['operator_id']} | "
                              f"{r['total_vol']:.2f}L en {r['duration_sec']}s ({r['reason']})")
                    print()
            elif cmd == "q":
                self.running = False
                break
            else:
                print(help_text)

    def start(self):
        # 1. Hilo del Servidor de Sockets para monitores
        threading.Thread(target=self.run_socket_server, daemon=True).start()

        # 2. Hilos de Consumidores y Publicadores Kafka
        threading.Thread(target=self.run_kafka_requests_consumer, daemon=True).start()
        threading.Thread(target=self.run_kafka_events_consumer, daemon=True).start()
        threading.Thread(target=self.run_kafka_status_publisher, daemon=True).start()

        # 3. Hilo del Servidor Web (si web_port > 0)
        if self.web_port > 0:
            threading.Thread(target=self.run_web_server, daemon=True).start()

        self.log(f"WM_Central en marcha. Sockets puerto {self.socket_port}, Kafka en {self.kafka_bootstrap}")

        # Si stdin es interactivo, mostramos el menú CLI; de lo contrario, mantenemos el proceso vivo
        if sys.stdin.isatty():
            self.run_cli_menu()
        else:
            self.log("Modo no interactivo (p. ej. en contenedor Docker o Railway). Panel impreso periódicamente.")
            while self.running:
                time.sleep(30)
                self.print_panel()


# ==============================================================================
# ENTRADA PRINCIPAL
# ==============================================================================
def main():
    # Detectar puerto web automático si Railway o el entorno define PORT
    env_port = os.getenv("PORT")
    default_web = int(env_port) if env_port and env_port.isdigit() else 8080

    parser = argparse.ArgumentParser(
        description="WM_Central: Módulo central de control de riego WaterManagement."
    )
    parser.add_argument("args", nargs="*", help="[puerto_sockets] [kafka_host:puerto] o [puerto_sockets] [kafka_host] [kafka_puerto]")
    parser.add_argument("--web-port", type=int, default=default_web, help="Puerto para el cuadro de mandos Web (por defecto 8080 o $PORT)")
    parser.add_argument("--db", default="central.db", help="Ruta del fichero SQLite (por defecto central.db)")

    parsed = parser.parse_args()
    raw_args = parsed.args

    if not raw_args:
        print("Uso: python WM_Central.py <puerto_sockets> <kafka_host:puerto> [--web-port PUERTO]")
        print("Ejemplo: python WM_Central.py 5000 localhost:9092")
        sys.exit(1)

    try:
        # Primer argumento: puerto de sockets
        socket_port = int(raw_args[0])
        # Argumentos restantes: Kafka host y puerto
        if len(raw_args) == 2:
            kafka_bootstrap = raw_args[1]
        elif len(raw_args) >= 3:
            kafka_bootstrap = f"{raw_args[1]}:{raw_args[2]}"
        else:
            kafka_bootstrap = "localhost:9092"
    except Exception as e:
        print(f"Error procesando argumentos: {e}")
        sys.exit(1)

    central = WMCentral(
        socket_port=socket_port,
        kafka_bootstrap=kafka_bootstrap,
        web_port=parsed.web_port,
        db_path=parsed.db
    )
    central.start()


if __name__ == "__main__":
    main()
