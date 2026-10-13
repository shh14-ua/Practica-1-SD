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

from common import (
    TOPIC_REQUESTS, TOPIC_REPLIES, TOPIC_STATUS,
    make_producer, send_kafka, make_consumer,
    COLOR_BOLD, COLOR_RESET, format_status
)

class FieldOperator:
    def __init__(self, kafka_bootstrap: str, op_id: str):
        self.kafka_bootstrap = kafka_bootstrap
        self.op_id = op_id.strip()
        self.running = True

        self.service_done_event = threading.Event()
        self.last_service_result = None
        self.stations_status: List[Dict[str, Any]] = []

        self.start_time = time.time() - 2.0
        self.producer = make_producer(self.kafka_bootstrap)
        self.consumer = make_consumer(
            self.kafka_bootstrap,
            [TOPIC_REPLIES, TOPIC_STATUS],
            group_prefix=f"fo-{self.op_id}",
            auto_offset_reset="earliest"
        )

    def log(self, msg: str):
        print(f"[{time.strftime('%H:%M:%S')}] [{self.op_id}] {msg}", flush=True)

    # --------------------------------------------------------------------------
    # ESCUCHA DE MENSAJES Y TELEMETRÍA DESDE KAFKA
    # --------------------------------------------------------------------------
    def listen_kafka(self):
        for msg in self.consumer:
            if not self.running:
                break
            topic = msg.topic
            val = msg.value

            # Estado global de estaciones
            if topic == TOPIC_STATUS:
                self.stations_status = val
                continue

            # Mensajes dirigidos a este operario
            if val.get("op") != self.op_id:
                continue

            msg_time = val.get("timestamp")
            if msg_time and isinstance(msg_time, (int, float)) and msg_time < self.start_time:
                continue

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

    # --------------------------------------------------------------------------
    # SOLICITUDES DE RIEGO
    # --------------------------------------------------------------------------
    def request_service(self, ws_id: str, duration: int):
        self.service_done_event.clear()
        self.last_service_result = None
        self.log(f"Enviando solicitud de riego para {ws_id} ({duration} s)...")
        payload = {
            "op": self.op_id,
            "ws": ws_id.strip().upper(),
            "duration": duration,
            "timestamp": time.time()
        }
        send_kafka(self.producer, TOPIC_REQUESTS, payload)

    # --------------------------------------------------------------------------
    # MODO AUTOMATIZADO POR FICHERO (REQUISITO DE 4 SEGUNDOS DE ESPERA)
    # --------------------------------------------------------------------------
    def run_file(self, file_path: str):
        if not os.path.isfile(file_path):
            print(f"Error: El archivo '{file_path}' no existe.")
            return

        with open(file_path, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]

        self.log(f"Iniciando ejecución por lotes: {len(lines)} peticiones encontradas en '{file_path}'.")

        for idx, line in enumerate(lines, 1):
            parts = [p.strip() for p in line.split(",")]
            ws_id = parts[0]
            dur = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10

            print(f"\n--- [LOTE {idx}/{len(lines)}] Solicitando servicio en {ws_id} durante {dur} s ---")
            self.request_service(ws_id, dur)

            # Esperar a la conclusión del servicio (éxito o denegación/fracaso)
            # Damos un timeout razonable: duración del riego + 20s de margen
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
        print(help_text)

        while self.running:
            try:
                line = input(f"{self.op_id}> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            if cmd == "r" and len(parts) >= 2:
                ws_id = parts[1]
                duration = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 10
                self.request_service(ws_id, duration)
            elif cmd == "l":
                self.list_stations()
            elif cmd == "q":
                self.running = False
                break
            else:
                print(help_text)

    def start(self, batch_file: str = None):
        threading.Thread(target=self.listen_kafka, daemon=True).start()
        time.sleep(1.0)  # Breve pausa para asegurar subscripción a topics

        if batch_file:
            self.run_file(batch_file)
            if not sys.stdin.isatty():
                return

        if sys.stdin.isatty():
            self.run_interactive()
        else:
            while self.running:
                time.sleep(1)


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
