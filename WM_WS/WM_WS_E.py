#!/usr/bin/env python3
"""
WM_WS_E: Motor de la Estación de Riego (Watering Station Engine).
Funcionalidades:
  - Conexión por socket al Monitor (WM_WS_M) para recibir asignación de ID de estación.
  - Respuesta al chequeo de salud (HEALTH) enviado por el Monitor.
  - Simulación de avería interactiva: tecla 'k' conmuta entre estado normal y KO (fuga simulada).
  - Simulación de actuador (electroválvula) y caudalímetro (flujo ~8-12 L/min).
  - Publicación periódica (cada segundo) de telemetría por Apache Kafka (ws_events).
  - Recepción de órdenes centrales por Kafka (ws_commands): START, STOP, BLOCK, ACTIVATE.
  - Control manual interactivo desde la propia estación: iniciar riego manual y finalizar suministro.
"""

import argparse
import os
import random
import socket
import sys
import threading
import time
from typing import Optional

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common import (
    client_handshake, serve_one_socket, parse_addr,
    TOPIC_COMMANDS, TOPIC_EVENTS,
    make_producer, send_kafka, make_consumer,
    COLOR_BOLD, COLOR_RESET
)

class WSEngine:
    def __init__(self, kafka_bootstrap: str, monitor_addr: str):
        self.kafka_bootstrap = kafka_bootstrap
        self.monitor_host, self.monitor_port = parse_addr(monitor_addr)
        self.lock = threading.RLock()
        self.running = True

        self.ws_id: Optional[str] = None
        self.assigned_event = threading.Event()

        # Estados posibles del motor: AVAILABLE, WATERING, BLOCKED
        self.state = "AVAILABLE"
        self.is_ko = False  # Simulación de avería
        self.monitor_connected = False
        self.central_connected = True

        # Variables de telemetría durante el riego
        self.current_op: Optional[str] = None
        self.accumulated_vol = 0.0
        self.watering_start_time = 0.0
        self.duration_limit = 0
        self.session_id = 0
        self.last_interrupted_summary: Optional[dict] = None

        # Conexión con Kafka
        self.producer = make_producer(self.kafka_bootstrap)

    def log(self, msg: str):
        id_str = self.ws_id or "NO-ASIGNADO"
        print(f"[{time.strftime('%H:%M:%S')}] [ENGINE {id_str}] {msg}", flush=True)

    def send_event(self, ev_type: str, **extra) -> bool:
        if not self.ws_id:
            return False
        payload = {
            "type": ev_type,
            "ws": self.ws_id,
            "timestamp": time.time(),
            **extra
        }
        return send_kafka(self.producer, TOPIC_EVENTS, payload)

    # --------------------------------------------------------------------------
    # CONEXIÓN Y SUPERVISIÓN CON EL MONITOR
    # --------------------------------------------------------------------------
    def link_monitor(self):
        """Mantiene el enlace de sockets con el monitor WM_WS_M."""
        while self.running:
            try:
                sock = socket.create_connection((self.monitor_host, self.monitor_port), timeout=5.0)
                client_handshake(sock)
                sock.settimeout(10.0)

                def monitor_handler(req: str) -> str:
                    parts = req.strip().split("#")
                    cmd = parts[0].upper()

                    if cmd == "ASSIGN" and len(parts) >= 2:
                        with self.lock:
                            self.ws_id = parts[1].strip().upper()
                            self.monitor_connected = True
                            self.assigned_event.set()
                        self.log(f"ID asignado por el Monitor: {self.ws_id}")
                        return "OK"

                    if cmd == "HEALTH":
                        with self.lock:
                            self.monitor_connected = True
                            if len(parts) >= 2:
                                central_flag = parts[1].strip().upper()
                                if central_flag == "NO_CENTRAL":
                                    if self.central_connected:
                                        self.central_connected = False
                                        self.log(
                                            "ERROR: Imposible conectar con WM_Central. "
                                            "El riego en curso continuará hasta finalizar, pero no se admitirán nuevos riegos."
                                        )
                                elif central_flag == "OK":
                                    if not self.central_connected:
                                        self.central_connected = True
                                        self.log("Conexión con WM_Central restablecida.")
                                        if self.last_interrupted_summary and not self.is_ko:
                                            s = self.last_interrupted_summary
                                            self.log(
                                                f"[RECUPERACIÓN] Reportando estado final del riego previo: "
                                                f"{s['vol']:.2f} L en {s['dur']} s (Op: {s['op']})"
                                            )
                                            self.send_event("FINISHED", **s)
                                            self.last_interrupted_summary = None

                            if self.is_ko:
                                # Si hay avería mientras riega, se interrumpe inmediatamente
                                if self.state == "WATERING":
                                    self.finish_watering("LEAK")
                                return "KO"
                            return "OK"

                    return "KO"

                while self.running:
                    serve_one_socket(sock, monitor_handler)

            except (OSError, ConnectionError):
                with self.lock:
                    was_connected = self.monitor_connected
                    self.monitor_connected = False
                    if self.state == "WATERING":
                        self.log("AVERIADA: Se perdió la conexión con el Monitor durante el riego. Deteniendo riego...")
                        self.finish_watering("MONITOR_DOWN")
                if was_connected:
                    self.log("AVERIADA: Enlace con Monitor perdido. La estación no admite riegos. Reintentando...")
                else:
                    self.log("AVERIADA: Enlace con Monitor no disponible. Reintentando...")
                time.sleep(2.0)

    # --------------------------------------------------------------------------
    # LÓGICA DE RIEGO Y TELEMETRÍA
    # --------------------------------------------------------------------------
    def start_watering(self, op: str, duration: int):
        with self.lock:
            if not self.monitor_connected:
                self.log("AVERIADA: Monitor desconectado. La estación no admite riegos (ni siquiera manuales).")
                return
            if not self.central_connected:
                self.log("ERROR: Imposible conectar con WM_Central. No se admiten nuevos riegos.")
                return
            if self.is_ko:
                self.log("AVERIADA: No se puede iniciar riego: sensor en estado de avería (KO).")
                return
            if self.state != "AVAILABLE":
                self.log(f"No se puede iniciar riego: estado actual es {self.state}")
                return

            self.state = "WATERING"
            self.current_op = op
            self.accumulated_vol = 0.0
            self.watering_start_time = time.time()
            self.duration_limit = duration
            self.session_id += 1

            self.send_event("STARTED", op=op, duration=duration)
            self.log(f">>> ELECTROVÁLVULA ABIERTA. Riego iniciado por {op} (Límite: {duration or 'manual'} s)")

            # Iniciar hilo de bombeo y telemetría
            threading.Thread(target=self._water_loop, args=(self.session_id,), daemon=True).start()

    def _water_loop(self, sid: int):
        while self.running:
            time.sleep(1.0)
            with self.lock:
                if self.session_id != sid or self.state != "WATERING":
                    break

                # Simulación de caudalímetro: ~8 a 12 L/min
                flow = round(random.uniform(8.5, 11.5), 2)
                # En 1 segundo se consumen (flow / 60) litros
                self.accumulated_vol += flow / 60.0
                curr_vol = round(self.accumulated_vol, 2)

                self.send_event("TELEMETRY", flow=flow, vol=curr_vol, op=self.current_op)
                self.log(f"   [Telemetría] Caudal: {flow:.1f} L/min | Acumulado: {curr_vol:.2f} L")

                # Comprobación de tiempo límite
                elapsed = time.time() - self.watering_start_time
                if self.duration_limit > 0 and elapsed >= self.duration_limit:
                    self.finish_watering("TIME")
                    break

    def finish_watering(self, reason: str):
        with self.lock:
            if self.state != "WATERING":
                return

            elapsed = max(1, int(round(time.time() - self.watering_start_time)))
            total_vol = round(self.accumulated_vol, 2)
            op = self.current_op

            self.state = "AVAILABLE"
            self.current_op = None

            summary_data = {"op": op, "reason": reason, "vol": total_vol, "dur": elapsed}
            sent = self.send_event("FINISHED", **summary_data)
            if reason in ("LEAK", "MONITOR_DOWN") or not self.central_connected or not sent:
                self.last_interrupted_summary = summary_data

            self.log(f"<<< ELECTROVÁLVULA CERRADA (Motivo: {reason}). Total: {total_vol} L en {elapsed} s")

    # --------------------------------------------------------------------------
    # CONSUMO DE ÓRDENES KAFKA (TOPIC_COMMANDS)
    # --------------------------------------------------------------------------
    def run_commands_consumer(self):
        while self.running:
            try:
                consumer = make_consumer(
                    self.kafka_bootstrap,
                    [TOPIC_COMMANDS],
                    group_prefix=f"engine-{self.ws_id}",
                    auto_offset_reset="latest"
                )
                for msg in consumer:
                    if not self.running:
                        break
                    data = msg.value
                    if not isinstance(data, dict):
                        continue

                    target_ws = str(data.get("ws", "")).strip().upper()
                    if target_ws != self.ws_id:
                        continue

                    cmd = str(data.get("cmd", "")).strip().upper()
                    with self.lock:
                        if cmd == "START":
                            op = str(data.get("op", "CENTRAL")).strip().upper()
                            duration = int(data.get("duration", 10))
                            self.start_watering(op, duration)

                        elif cmd == "STOP":
                            reason = data.get("reason", "CENTRAL_STOP")
                            self.finish_watering(reason)

                        elif cmd == "BLOCK":
                            self.finish_watering("CENTRAL_BLOCK")
                            self.state = "BLOCKED"
                            self.log("Estación puesta FUERA DE SERVICIO por orden de CENTRAL")

                        elif cmd == "ACTIVATE":
                            if self.state == "BLOCKED":
                                self.state = "AVAILABLE"
                                self.log("Estación REACTIVADA por CENTRAL: lista para prestar servicio")
            except Exception as exc:
                if self.running:
                    self.log(f"Aviso en consumidor Kafka de órdenes ({exc}). Reintentando...")
                    time.sleep(2.0)

    # --------------------------------------------------------------------------
    # INTERFAZ DE USUARIO EN TIEMPO REAL
    # --------------------------------------------------------------------------
    def run_interactive_menu(self):
        menu_text = (
            f"\n{COLOR_BOLD}COMANDOS EN ESTACIÓN ({self.ws_id}):{COLOR_RESET}\n"
            f"  {COLOR_BOLD}k{COLOR_RESET} -> Alternar avería / fuga (KO <-> OK)\n"
            f"  {COLOR_BOLD}s{COLOR_RESET} -> Iniciar riego MANUAL desde la propia estación\n"
            f"  {COLOR_BOLD}f{COLOR_RESET} -> Finalizar suministro de riego en curso\n"
            f"  {COLOR_BOLD}q{COLOR_RESET} -> Salir\n"
        )
        print(menu_text, flush=True)

        while self.running:
            try:
                line = input().strip().lower()
            except EOFError:
                self.log("Entrada estándar cerrada; manteniendo motor activo en segundo plano.")
                while self.running:
                    time.sleep(1)
                break
            except KeyboardInterrupt:
                break
            if not line:
                continue

            if line == "k":
                with self.lock:
                    self.is_ko = not self.is_ko
                    if self.is_ko:
                        self.log("\033[91;1m[SIMULACIÓN] Sensor en estado de AVERÍA (KO / Fuga activa)\033[0m")
                        if self.state == "WATERING":
                            self.finish_watering("LEAK")
                    else:
                        self.log("\033[92;1m[SIMULACIÓN] Avería RESUELTA (Sensor en estado OK)\033[0m")
                        if self.last_interrupted_summary:
                            s = self.last_interrupted_summary
                            self.log(
                                f"[RECUPERACIÓN] Estado final del riego previo antes de la avería: "
                                f"{s['vol']:.2f} L consumidos en {s['dur']} s (Operario: {s['op']})"
                            )
                            self.last_interrupted_summary = None

            elif line == "s":
                self.start_watering("MANUAL", 0)

            elif line == "f":
                self.finish_watering("MANUAL")

            elif line == "q":
                self.running = False
                break
            else:
                print(menu_text, flush=True)

    def start(self):
        # 1. Iniciar hilo de enlace de sockets con Monitor
        threading.Thread(target=self.link_monitor, daemon=True).start()

        self.log(f"Esperando conexión y asignación de ID desde Monitor en {self.monitor_host}:{self.monitor_port}...")
        self.assigned_event.wait()

        # 2. Iniciar consumidor de órdenes de Kafka
        threading.Thread(target=self.run_commands_consumer, daemon=True).start()

        try:
            self.run_interactive_menu()
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            with self.lock:
                if self.state == "WATERING":
                    self.finish_watering("LEAK")


# ==============================================================================
# ENTRADA PRINCIPAL
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="WM_WS_E: Motor de la Estación de Riego (Watering Station Engine)."
    )
    parser.add_argument("args", nargs="*", help="[kafka_host:puerto] [monitor_host:puerto] o con argumentos separados")
    parsed = parser.parse_args()
    raw = parsed.args

    if len(raw) < 2:
        print("Uso: python WM_WS_E.py <kafka_host:puerto> <monitor_host:puerto>")
        print("Ejemplo: python WM_WS_E.py localhost:9092 localhost:6001")
        sys.exit(1)

    try:
        kafka_bootstrap = raw[0]
        monitor_addr = raw[1]
    except Exception as e:
        print(f"Error procesando argumentos: {e}")
        sys.exit(1)

    engine = WSEngine(kafka_bootstrap=kafka_bootstrap, monitor_addr=monitor_addr)
    engine.start()


if __name__ == "__main__":
    main()
