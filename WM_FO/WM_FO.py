#!/usr/bin/env python3
"""
WM_FO: Aplicación de Operario de Campo (Field Operator).
Funcionalidades:
  - Conexión al gestor de colas Apache Kafka.
  - Envío de solicitudes de activación de riego a WM_Central (fo_requests).
  - Recepción de notificaciones de progreso (STEP), autorización (AUTH), denegación (DENY)
    y telemetría en tiempo real (fo_replies).
  - Recepción de resumen final (SUMMARY) al concluir el servicio (éxito o avería).
  - Consulta del estado de todas las estaciones disponibles (ws_status).
  - Modo por lotes (--file operaciones.txt): procesa solicitudes secuencialmente, esperando
    a la conclusión de cada servicio y aguardando exactamente 4 segundos antes del siguiente.
  - Modo interactivo por consola con comandos claros.
"""

import argparse
import os
import sys
import threading
import time
from typing import List, Dict, Any

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common import (
    TOPIC_REQUESTS, TOPIC_REPLIES, TOPIC_STATUS,
    make_producer, send_kafka, make_consumer,
    COLOR_BOLD, COLOR_RESET, format_status
)

class FieldOperator:
    def __init__(self, kafka_bootstrap: str, op_id: str):
        self.kafka_bootstrap = kafka_bootstrap
        self.op_id = op_id.strip().upper()
        self.running = True

        self.service_done_event = threading.Event()
        self.central_ack_event = threading.Event()
        self.last_service_result = None
        self.stations_status: List[Dict[str, Any]] = []
        self.last_central_status_ts: float = 0.0

        self.producer = make_producer(self.kafka_bootstrap)
        # Usamos un group_id determinista por operario para que, si el FO se cierra en mitad
        # de un riego y se vuelve a abrir, recupere automáticamente el resultado de su servicio.
        self.consumer = make_consumer(
            self.kafka_bootstrap,
            [TOPIC_REPLIES, TOPIC_STATUS],
            group_prefix=f"fo-{self.op_id}",
            auto_offset_reset="latest",
            group_id=f"fo-client-{self.op_id}"
        )

    def log(self, msg: str):
        print(f"[{time.strftime('%H:%M:%S')}] [{self.op_id}] {msg}", flush=True)

    # --------------------------------------------------------------------------
    # ESCUCHA DE MENSAJES Y TELEMETRÍA DESDE KAFKA
    # --------------------------------------------------------------------------
    def listen_kafka(self):
        while self.running:
            try:
                for msg in self.consumer:
                    if not self.running:
                        break
                    topic = msg.topic
                    val = msg.value
                    if val is None:
                        continue

                    # Estado global de estaciones
                    if topic == TOPIC_STATUS:
                        if isinstance(val, list):
                            self.stations_status = val
                            self.last_central_status_ts = time.time()
                        continue

                    if not isinstance(val, dict):
                        continue

                    # Mensajes dirigidos a este operario (comparación insensible a mayúsculas)
                    target_op = str(val.get("op", "")).strip().upper()
                    if target_op != self.op_id:
                        continue

                    self.last_central_status_ts = time.time()
                    self.central_ack_event.set()

                    msg_type = val.get("type")
                    ws_id = val.get("ws")
                    text = val.get("msg", "")

                    if msg_type == "STEP":
                        print(f"\033[96m[{ws_id}] -> {text}\033[0m", flush=True)

                    elif msg_type == "AUTH":
                        print(f"\033[92;1m[{ws_id}] -> {text}\033[0m", flush=True)

                    elif msg_type == "DENY":
                        print(f"\033[91;1m[{ws_id}] -> {text}\033[0m", flush=True)
                        self.last_service_result = "DENIED"
                        self.service_done_event.set()

                    elif msg_type == "TELEMETRY":
                        flow = val.get("flow", 0.0)
                        vol = val.get("vol", 0.0)
                        print(f"   [{ws_id}] Telemetría en directo: {flow:.1f} L/min | Acumulado: {vol:.2f} L", flush=True)

                    elif msg_type == "SUMMARY":
                        print(f"\n\033[93;1m==================================================================")
                        print(f" RESUMEN DEL SERVICIO [{ws_id}]:\n {text}")
                        print(f"==================================================================\033[0m\n", flush=True)
                        self.last_service_result = "FINISHED"
                        self.service_done_event.set()
            except Exception as exc:
                if self.running:
                    print(f"[KAFKA] Aviso en consumidor de {self.op_id}: {exc}. Reintentando...", file=sys.stderr, flush=True)
                    time.sleep(1.0)

    # --------------------------------------------------------------------------
    # SOLICITUDES DE RIEGO
    # --------------------------------------------------------------------------
    def _watch_central_ack(self, ws_id: str):
        """Comprueba que WM_Central responda a la solicitud; si está caída, informa al usuario."""
        if not self.central_ack_event.wait(timeout=6.0):
            print(
                f"\033[91;1m[{ws_id}] -> ERROR: Imposible conectar con WM_Central "
                f"(no se recibió respuesta del sistema central).\033[0m",
                flush=True
            )
            self.last_service_result = "NO_CENTRAL"
            self.service_done_event.set()

    def request_service(self, ws_id: str, duration: int):
        clean_ws = ws_id.strip().rstrip(",").upper()
        self.service_done_event.clear()
        self.central_ack_event.clear()
        self.last_service_result = None
        self.log(f"Enviando solicitud de riego para {clean_ws} ({duration} s)...")
        payload = {
            "op": self.op_id,
            "ws": clean_ws,
            "duration": duration,
            "timestamp": time.time()
        }
        ok = send_kafka(self.producer, TOPIC_REQUESTS, payload)
        if not ok:
            print(
                f"\033[91;1m[{clean_ws}] -> ERROR: No se pudo enviar la solicitud al broker Kafka.\033[0m",
                flush=True
            )
            self.last_service_result = "KAFKA_ERROR"
            self.service_done_event.set()
            return
        threading.Thread(target=self._watch_central_ack, args=(clean_ws,), daemon=True).start()

    # --------------------------------------------------------------------------
    # MODO AUTOMATIZADO POR FICHERO (REQUISITO DE 4 SEGUNDOS DE ESPERA)
    # --------------------------------------------------------------------------
    def run_file(self, file_path: str):
        if not os.path.isfile(file_path):
            alt_in_module = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path)
            alt_in_root = os.path.join(ROOT_DIR, file_path)
            if os.path.isfile(alt_in_module):
                file_path = alt_in_module
            elif os.path.isfile(alt_in_root):
                file_path = alt_in_root
            else:
                print(f"Error: El archivo '{file_path}' no existe.")
                return

        with open(file_path, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]

        self.log(f"Iniciando ejecución por lotes: {len(lines)} peticiones encontradas en '{file_path}'.")

        for idx, line in enumerate(lines, 1):
            parts = [p.strip() for p in line.split(",")]
            ws_id = parts[0]
            dur_str = parts[1].rstrip("sS") if len(parts) > 1 else "10"
            dur = int(dur_str) if dur_str.isdigit() else 10

            print(f"\n--- [LOTE {idx}/{len(lines)}] Solicitando servicio en {ws_id} durante {dur} s ---")
            self.request_service(ws_id, dur)

            # Esperar a la conclusión del servicio (éxito o denegación/fracaso)
            # Damos un timeout razonable: duración del riego + 25s de margen
            finished_in_time = self.service_done_event.wait(timeout=dur + 25)
            if not finished_in_time:
                self.log(f"Aviso: Timeout esperando conclusión del servicio en {ws_id}.")

            # REQUISITO DEL ENUNCIADO:
            # "tras su conclusión (sea en éxito o fracaso), esperará 4 segundos y pasará a solicitar el siguiente servicio."
            if idx < len(lines):
                self.log("Servicio concluido. Esperando 4 segundos antes de solicitar el siguiente servicio...")
                time.sleep(4.0)

        self.log("Procesamiento del fichero por lotes completado con éxito.")

    # --------------------------------------------------------------------------
    # LISTADO DE ESTACIONES DISPONIBLES
    # --------------------------------------------------------------------------
    def list_stations(self):
        print("\n" + "=" * 70)
        print(f" {COLOR_BOLD}ESTACIONES DE RIEGO EN LA RED{COLOR_RESET}")
        print("=" * 70)
        if not self.stations_status:
            print("  (Aún no se ha recibido información de estado desde CENTRAL)")
        else:
            if self.last_central_status_ts and (time.time() - self.last_central_status_ts > 8.0):
                print("  \033[91;1m[AVISO] Imposible conectar con WM_Central (datos en caché sin actualizar)\033[0m")
            print(f"{'ID':<8}{'UBICACIÓN':<22}{'ESTADO':<22}{'CAUDAL':>10}")
            print("-" * 70)
            for s in sorted(self.stations_status, key=lambda x: x["id"]):
                colored_st = format_status(s["status"])
                flow_s = f"{s.get('flow', 0.0):.1f} L/m" if s["status"] == "REGANDO" else "—"
                print(f"{s['id']:<8}{s['location'][:20]:<22}{colored_st:<31}{flow_s:>10}")
        print("=" * 70 + "\n")

    # --------------------------------------------------------------------------
    # MENÚ INTERACTIVO EN CONSOLA
    # --------------------------------------------------------------------------
    def run_interactive(self):
        help_text = (
            f"\n{COLOR_BOLD}COMANDOS OPERARIO ({self.op_id}):{COLOR_RESET}\n"
            f"  {COLOR_BOLD}r <WS> [seg]{COLOR_RESET}  -> Solicitar riego en estación (ej: r WS-01 15)\n"
            f"  {COLOR_BOLD}l{COLOR_RESET}             -> Listar estaciones y disponibilidad\n"
            f"  {COLOR_BOLD}q{COLOR_RESET}             -> Salir\n"
        )
        print(help_text, flush=True)

        while self.running:
            try:
                line = input(f"{self.op_id}> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                continue

            parts = line.replace(",", " ").split()
            cmd = parts[0].lower()

            if cmd in ("r", "s") and len(parts) >= 2:
                ws_id = parts[1].strip().upper()
                dur_str = parts[2].rstrip("sS") if len(parts) >= 3 else "10"
                duration = int(dur_str) if dur_str.isdigit() else 10
                self.request_service(ws_id, duration)
            elif cmd == "l":
                self.list_stations()
            elif cmd == "q":
                self.running = False
                break
            else:
                print(help_text, flush=True)

    def start(self, batch_file: str = None):
        threading.Thread(target=self.listen_kafka, daemon=True).start()
        time.sleep(0.5)  # Breve pausa tras el poll inicial del consumidor

        if batch_file:
            self.run_file(batch_file)
            if not sys.stdin.isatty():
                return

        self.run_interactive()


# ==============================================================================
# ENTRADA PRINCIPAL
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="WM_FO: Aplicación de Operario de Campo (Field Operator)."
    )
    parser.add_argument("args", nargs="*", help="[kafka_host:puerto] [op_id] o con host y puerto separados")
    parser.add_argument("--file", help="Ruta del fichero de operaciones por lotes (ej: operaciones.txt)")

    parsed = parser.parse_args()
    raw = parsed.args

    if len(raw) < 2:
        print("Uso: python WM_FO.py <kafka_host:puerto> <ID_operario> [--file operaciones.txt]")
        print("Ejemplo: python WM_FO.py localhost:9092 FO-01 --file operaciones.txt")
        sys.exit(1)

    try:
        kafka_bootstrap = raw[0]
        op_id = raw[1]
    except Exception as e:
        print(f"Error procesando argumentos: {e}")
        sys.exit(1)

    fo = FieldOperator(kafka_bootstrap=kafka_bootstrap, op_id=op_id)
    fo.start(batch_file=parsed.file)


if __name__ == "__main__":
    main()
