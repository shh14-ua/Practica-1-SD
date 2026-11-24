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
  - Release 2: todos los mensajes Kafka con Central van cifrados y autenticados con la clave
    simétrica de la WS (recibida del Monitor). Se rechazan órdenes manipuladas, de otra WS,
    cifradas con otra clave o repetidas (número de secuencia persistido por clave).
  - Control manual interactivo desde la propia estación: iniciar riego manual y finalizar suministro.
"""

import argparse
import json
import os
import random
import socket
import sys
import threading
import time
import uuid
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
from wm_crypto import (
    require_cryptography, is_valid_symmetric_key, key_fingerprint,
    seal_kafka_message, open_kafka_message, next_kafka_seq, KAFKA_KIND_COMMAND, KAFKA_KIND_EVENT
)

# Último número de secuencia de órdenes aceptado, por clave (solo su huella), para que tras
# reiniciar el Engine con la misma clave no se acepten órdenes repetidas
DEFAULT_STATE_DIR = os.getenv("WM_ENGINE_STATE_DIR", os.path.join(ROOT_DIR, "WM_WS", ".engine_state"))

class WSEngine:
    def __init__(self, kafka_bootstrap: str, monitor_addr: str, state_dir: Optional[str] = None):
        self.kafka_bootstrap = kafka_bootstrap
        self.state_dir = state_dir
        self.monitor_host, self.monitor_port = parse_addr(monitor_addr)
        self.lock = threading.RLock()
        self.event_lock = threading.RLock()
        self.pending_telemetry = 0
        self.telemetry_done = threading.Condition(self.lock)
        self.running = True

        self.ws_id: Optional[str] = None
        self.assigned_event = threading.Event()

        # Estados posibles del motor: AVAILABLE, WATERING, BLOCKED
        self.state = "AVAILABLE"
        self.is_ko = False  # Simulación de avería
        self.monitor_connected = False
        self.central_connected = True
        self.symmetric_key: Optional[str] = None  # Clave simétrica para cifrado (Release 2)
        self._last_cmd_seq = 0   # último seq de orden aceptado con la clave vigente
        self._last_evt_seq = 0   # último seq de evento emitido
        self._reject_log_ts: dict = {}
        # FINISHED publicados que Central aún no ha confirmado (ACK_FINISHED) {transaction_id: resumen}.
        # Si la clave cambia antes de la confirmación, vuelven a la cola de pendientes y se
        # reenvían cifrados con la clave nueva (Central descarta los duplicados por transaction_id).
        self._unconfirmed_finished: dict = {}

        # Variables de telemetría durante el riego
        self.current_op: Optional[str] = None
        self.current_tx_id: Optional[str] = None
        self.accumulated_vol = 0.0
        self.watering_start_time = 0.0
        self.duration_limit = 0
        self.session_id = 0
        self.watering_deadline = 0.0
        self.timer_event = threading.Event()
        self.started_event = threading.Event()
        self.started_sent = False
        self.last_interrupted_summary: Optional[dict] = None
        self.pending_finished_events: list = []
        self.flush_lock = threading.Lock()

        # Conexión con Kafka
        self.producer = make_producer(self.kafka_bootstrap)

    def log(self, msg: str):
        id_str = self.ws_id or "NO-ASIGNADO"
        print(f"[{time.strftime('%H:%M:%S')}] [ENGINE {id_str}] {msg}", flush=True)

    def send_event(self, ev_type: str, **extra) -> bool:
        """Publica un evento en ws_events cifrado y autenticado con la clave de la WS.
        Sin clave vigente no se publica nada (nunca en claro): devuelve False y los FINISHED
        quedan pendientes para reenviarse cuando llegue una clave nueva."""
        with self.event_lock:
            with self.lock:
                ws_id, key = self.ws_id, self.symmetric_key
            if not ws_id or not key:
                return False

            payload = {"type": ev_type, "timestamp": time.time(), **extra}
            self._last_evt_seq = next_kafka_seq(self._last_evt_seq)
            message = seal_kafka_message(key, KAFKA_KIND_EVENT, ws_id, payload, self._last_evt_seq)
            sent = send_kafka(self.producer, TOPIC_EVENTS, message)
            if sent and ev_type == "FINISHED" and extra.get("transaction_id"):
                with self.lock:
                    self._unconfirmed_finished[extra["transaction_id"]] = dict(extra)
            return sent

    def _requeue_unconfirmed_finished_locked(self):
        """Pasa a la cola de pendientes los FINISHED sin confirmar (se llama al cambiar de clave:
        los enviados con la clave anterior pueden no haber llegado a procesarse en Central)."""
        for tx_id, summary in self._unconfirmed_finished.items():
            if not any(p.get("transaction_id") == tx_id for p in self.pending_finished_events):
                self.pending_finished_events.append(dict(summary))
        self._unconfirmed_finished.clear()

    def _confirm_finished(self, tx_id: str):
        """Central confirmó el registro del FINISHED: deja de conservarse para reenvío."""
        with self.lock:
            known = self._unconfirmed_finished.pop(tx_id, None) is not None
            before = len(self.pending_finished_events)
            self.pending_finished_events = [p for p in self.pending_finished_events
                                            if p.get("transaction_id") != tx_id]
            known = known or len(self.pending_finished_events) != before
            if self.last_interrupted_summary and self.last_interrupted_summary.get("transaction_id") == tx_id:
                self.last_interrupted_summary = None
        if known:
            self.log(f"Central confirmó el registro del riego (transaction_id={tx_id}).")

    # --------------------------------------------------------------------------
    # CLAVE SIMÉTRICA Y PROTECCIÓN FRENTE A ÓRDENES REPETIDAS
    # --------------------------------------------------------------------------
    def _state_file(self) -> str:
        return os.path.join(self.state_dir or DEFAULT_STATE_DIR, f"{self.ws_id}.json")

    def _load_last_cmd_seq(self, key: str) -> int:
        """Último seq de orden aceptado con esta clave en una ejecución anterior (0 si no consta)."""
        try:
            with open(self._state_file(), "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("key_id") == key_fingerprint(key):
                return int(data.get("last_cmd_seq", 0))
        except (OSError, ValueError, TypeError, AttributeError):
            pass
        return 0

    def _save_last_cmd_seq(self, key: str, seq: int):
        """Guarda de forma atómica (0600) la huella de la clave y el último seq aceptado."""
        path = self._state_file()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"key_id": key_fingerprint(key), "last_cmd_seq": int(seq)}, f)
            os.replace(tmp, path)
        except OSError as exc:
            self.log(f"Aviso: no se pudo guardar el estado de órdenes aceptadas ({exc}).")

    def _set_key(self, key: str) -> bool:
        if not is_valid_symmetric_key(key):
            self.log("Clave simétrica recibida del Monitor NO válida: se ignora.")
            return False
        with self.lock:
            if key != self.symmetric_key:
                self.symmetric_key = key
                self._last_cmd_seq = self._load_last_cmd_seq(key)
                self._requeue_unconfirmed_finished_locked()
            has_pending = bool(self.pending_finished_events)
        self.log(f"Clave simétrica recibida del Monitor (huella {key_fingerprint(key)[:8]}). Canal Kafka cifrado activado.")
        if has_pending:
            self.trigger_flush_pending()  # reenvía con la clave nueva los FINISHED no confirmados
        return True

    def _clear_key(self):
        """CLEAR_KEY (revocación o baja): la estación deja de estar autorizada. En la misma operación
        se borra la clave y se cierra cualquier riego en curso (como la Release 1 ante la baja del
        Monitor), sin depender de que llegue una orden por Kafka que ya no podría descifrarse.
        El FINISHED se publica en segundo plano para no retener la respuesta al Monitor; sin clave
        queda pendiente y se reenvía cuando llegue una clave nueva."""
        with self.lock:
            had_key = self.symmetric_key is not None
            self.symmetric_key = None
            self._last_cmd_seq = 0
            self._requeue_unconfirmed_finished_locked()
            summary, started_ev = self._close_watering_locked("KEYS_CLEARED")
        if had_key:
            self.log("Clave simétrica ELIMINADA por el Monitor (revocación o baja). "
                     "No se aceptan órdenes ni nuevos riegos hasta recibir una clave nueva.")
        if summary:
            threading.Thread(target=self._publish_finished, args=(summary, started_ev), daemon=True).start()

    def _log_rejected_command(self, reason: str):
        """Informa de órdenes rechazadas como máximo una vez cada 10 s por motivo."""
        now = time.time()
        if now - self._reject_log_ts.get(reason, 0.0) >= 10.0:
            self._reject_log_ts[reason] = now
            self.log(f"Orden Kafka RECHAZADA ({reason}).")

    def _open_command(self, message: dict) -> Optional[dict]:
        """Descifra y valida una orden de ws_commands dirigida a esta WS. Devuelve su contenido
        o None si no es auténtica, no tiene clave vigente o es repetida/antigua."""
        with self.lock:
            key, last = self.symmetric_key, self._last_cmd_seq
        if not key:
            self._log_rejected_command("SIN_CLAVE")
            return None
        inner, reason = open_kafka_message(key, message, KAFKA_KIND_COMMAND, self.ws_id)
        if inner is None:
            self._log_rejected_command(reason)
            return None
        seq = inner["seq"]
        with self.lock:
            if self.symmetric_key != key or seq <= self._last_cmd_seq:
                self._log_rejected_command("REPETIDO" if self.symmetric_key == key else "CLAVE_CAMBIADA")
                return None
            self._last_cmd_seq = seq
        self._save_last_cmd_seq(key, seq)
        return inner

    def trigger_flush_pending(self):
        """Lanza el vaciado de eventos pendientes en segundo plano sin bloquear el hilo del socket."""
        threading.Thread(target=self.flush_pending_finished, daemon=True).start()

    def flush_pending_finished(self) -> int:
        """Reenvía eventos FINISHED que quedaron pendientes por fallo temporal de Kafka o de Central.
        Garantiza mediante flush_lock que no se ejecuten múltiples vaciados simultáneamente."""
        if not self.flush_lock.acquire(blocking=False):
            return 0
        try:
            with self.lock:
                pending = list(self.pending_finished_events)
                interrupted = self.last_interrupted_summary
                if interrupted and not any(p.get("transaction_id") == interrupted.get("transaction_id") for p in pending):
                    pending.insert(0, interrupted)
            if not pending:
                return 0

            flushed = 0
            successfully_sent_tx = set()
            for summary in pending:
                tx_id = summary.get("transaction_id")
                if self.send_event("FINISHED", **summary):
                    flushed += 1
                    successfully_sent_tx.add(tx_id)
                    with self.lock:
                        if (
                            self.last_interrupted_summary
                            and self.last_interrupted_summary.get("transaction_id") == tx_id
                        ):
                            self.last_interrupted_summary = None

            with self.lock:
                unique_tx = set()
                filtered = []
                for s in self.pending_finished_events:
                    tid = s.get("transaction_id")
                    if tid not in successfully_sent_tx and tid not in unique_tx:
                        unique_tx.add(tid)
                        filtered.append(s)
                self.pending_finished_events = filtered
            return flushed
        finally:
            self.flush_lock.release()

    # --------------------------------------------------------------------------
    # CONEXIÓN Y SUPERVISIÓN CON EL MONITOR
    # --------------------------------------------------------------------------
    def handle_monitor_request(self, req: str) -> str:
        """Procesa una trama recibida desde el Monitor por socket TCP y genera la respuesta.
        Garantiza que ninguna operación bloqueante de Kafka demore la respuesta HEALTH."""
        parts = req.strip().split("#")
        cmd = parts[0].upper()

        if cmd == "ASSIGN" and len(parts) >= 2:
            with self.lock:
                self.ws_id = parts[1].strip().upper()
                self.monitor_connected = True
                self.assigned_event.set()
            self.log(f"ID asignado por el Monitor: {self.ws_id}")
            return "OK"

        if cmd == "SET_KEY" and len(parts) >= 2:
            return "OK" if self._set_key(parts[1].strip()) else "KO"

        if cmd == "CLEAR_KEY":
            self._clear_key()
            return "OK"

        if cmd == "HEALTH":
            trigger_flush = False
            summary_to_publish = None
            started_ev_to_publish = None

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

                if len(parts) >= 3:
                    block_flag = parts[2].strip().upper()
                    if block_flag == "BLOCKED" and self.state != "BLOCKED":
                        self.log("Estación puesta FUERA DE SERVICIO por orden de CENTRAL")
                        if self.state == "WATERING":
                            summary_to_publish, started_ev_to_publish = self._close_watering_locked("CENTRAL_BLOCK")
                        else:
                            self.state = "BLOCKED"
                    elif block_flag == "ACTIVE" and self.state == "BLOCKED":
                        self.state = "AVAILABLE"
                        self.log("Estación REACTIVADA por CENTRAL: lista para prestar servicio")

                if (self.pending_finished_events or self.last_interrupted_summary) and not self.is_ko:
                    trigger_flush = True

                if self.is_ko:
                    if self.state == "WATERING":
                        summary_to_publish, started_ev_to_publish = self._close_watering_locked("LEAK")
                    if summary_to_publish:
                        threading.Thread(
                            target=self._publish_finished,
                            args=(summary_to_publish, started_ev_to_publish),
                            daemon=True
                        ).start()
                    return "KO"

            if summary_to_publish:
                threading.Thread(
                    target=self._publish_finished,
                    args=(summary_to_publish, started_ev_to_publish),
                    daemon=True
                ).start()

            if trigger_flush:
                self.trigger_flush_pending()

            return "OK"

        return "KO"

    def link_monitor(self):
        """Mantiene el enlace de sockets con el monitor WM_WS_M."""
        while self.running:
            try:
                sock = socket.create_connection((self.monitor_host, self.monitor_port), timeout=5.0)
                client_handshake(sock)
                sock.settimeout(10.0)

                while self.running:
                    serve_one_socket(sock, self.handle_monitor_request)

            except (OSError, ConnectionError):
                with self.lock:
                    was_connected = self.monitor_connected
                    self.monitor_connected = False
                    watering_to_stop = (self.state == "WATERING")
                if watering_to_stop:
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
    def start_watering(self, op: str, duration: int, transaction_id: Optional[str] = None):
        with self.lock:
            if not self.monitor_connected:
                self.log("AVERIADA: Monitor desconectado. La estación no admite riegos (ni siquiera manuales).")
                return
            if not self.central_connected:
                self.log("ERROR: Imposible conectar con WM_Central. No se admiten nuevos riegos.")
                return
            if not self.symmetric_key:
                self.log("No se admiten riegos: la estación no tiene clave simétrica vigente "
                         "(sin autenticar o claves revocadas por CENTRAL).")
                return
            if self.is_ko:
                self.log("AVERIADA: No se puede iniciar riego: sensor en estado de avería (KO).")
                return
            if self.state != "AVAILABLE":
                if self.state == "BLOCKED":
                    self.log("No se puede iniciar riego: la estación está FUERA DE SERVICIO (BLOCKED por CENTRAL)")
                else:
                    self.log(f"No se puede iniciar riego: estado actual es {self.state}")
                return

            self.state = "WATERING"
            self.current_op = op
            self.current_tx_id = (
                str(transaction_id).strip()
                if transaction_id
                else f"{self.ws_id}-{op}-{uuid.uuid4().hex[:12]}"
            )
            self.accumulated_vol = 0.0
            self.watering_start_time = time.monotonic()
            self.duration_limit = duration
            self.watering_deadline = (
                self.watering_start_time + duration if duration > 0 else 0.0
            )
            self.session_id += 1
            self.timer_event = threading.Event()
            self.started_event = threading.Event()
            self.started_sent = False

            sid = self.session_id
            tx_id = self.current_tx_id
            deadline = self.watering_deadline
            timer_ev = self.timer_event
            started_ev = self.started_event

        self.log(f">>> ELECTROVÁLVULA ABIERTA. Riego iniciado por {op} (Límite: {duration or 'manual'} s)")

        # 1. Iniciar hilo de bombeo y telemetría
        threading.Thread(target=self._water_loop, args=(sid, started_ev), daemon=True).start()

        # 2. Iniciar temporizador independiente de inmediato para controlar la duración desde la apertura
        if duration > 0:
            threading.Thread(
                target=self._watering_timer,
                args=(sid, deadline, timer_ev),
                daemon=True
            ).start()

        # 3. Publicar el evento STARTED en segundo plano para no demorar el temporizador ni bloquear al invocador
        threading.Thread(
            target=self._publish_started,
            args=(sid, op, duration, tx_id, started_ev),
            daemon=True
        ).start()

    def _publish_started(self, sid: int, op: str, duration: int, tx_id: str, started_ev: threading.Event):
        try:
            sent = self.send_event("STARTED", op=op, duration=duration, transaction_id=tx_id)
            if self.session_id == sid:
                self.started_sent = bool(sent)
        finally:
            started_ev.set()

    def _watering_timer(self, sid: int, deadline: float, stop_event: threading.Event):
        remaining = max(0.0, deadline - time.monotonic())

        if stop_event.wait(timeout=remaining):
            return

        should_finish = False
        with self.lock:
            if self.session_id == sid and self.state == "WATERING":
                should_finish = True

        if should_finish:
            self.finish_watering("TIME")

    def _water_loop(self, sid: int, started_ev: threading.Event):
        while self.running:
            time.sleep(1.0)

            with self.lock:
                if self.session_id != sid or self.state != "WATERING":
                    break

                elapsed = time.monotonic() - self.watering_start_time

                if self.duration_limit > 0 and elapsed >= self.duration_limit:
                    break

                # Simular el caudal y actualizar el volumen acumulado siempre (físico).
                flow = round(random.uniform(8.5, 11.5), 2)
                self.accumulated_vol += flow / 60.0
                curr_vol = round(self.accumulated_vol, 2)
                op = self.current_op
                tx_id = self.current_tx_id

                # Solo emitir telemetría si STARTED ya concluyó su intento y tuvo éxito
                can_send_telemetry = started_ev.is_set() and getattr(self, "started_sent", False)
                if can_send_telemetry:
                    self.pending_telemetry += 1

            if not can_send_telemetry:
                continue

            # Enviar telemetría fuera del bloqueo.
            try:
                self.send_event(
                    "TELEMETRY",
                    flow=flow,
                    vol=curr_vol,
                    op=op,
                    transaction_id=tx_id
                )
                self.log(
                    f"   [Telemetría] Caudal: {flow:.1f} L/min | "
                    f"Acumulado: {curr_vol:.2f} L"
                )
            finally:
                with self.telemetry_done:
                    self.pending_telemetry -= 1
                    self.telemetry_done.notify_all()

    def _close_watering_locked(self, reason: str) -> tuple[Optional[dict], Optional[threading.Event]]:
        """Cierra el riego físicamente y actualiza el estado atómicamente bajo self.lock.
        Devuelve (summary_data, started_event) para su posterior publicación fuera del cerrojo."""
        if self.state != "WATERING":
            if reason == "CENTRAL_BLOCK":
                self.state = "BLOCKED"
            return None, None

        elapsed = max(
            0,
            int(round(time.monotonic() - self.watering_start_time))
        )
        total_vol = round(self.accumulated_vol, 2)
        op = self.current_op
        tx_id = self.current_tx_id or f"{self.ws_id}-{op}-{uuid.uuid4().hex[:12]}"
        started_ev = getattr(self, "started_event", None)

        # Cerrar el riego antes de cualquier espera de Kafka.
        was_weather_block = bool(getattr(self, "pending_weather_block", False))
        self.pending_weather_block = False
        self.state = "BLOCKED" if (reason in ("CENTRAL_BLOCK", "WEATHER_ALERT") or was_weather_block) else "AVAILABLE"
        self.current_op = None
        self.current_tx_id = None
        self.timer_event.set()

        summary_data = {
            "op": op,
            "reason": reason,
            "vol": total_vol,
            "dur": elapsed,
            "transaction_id": tx_id,
        }

        self.log(
            f"<<< ELECTROVÁLVULA CERRADA (Motivo: {reason}). "
            f"Total: {total_vol} L en {elapsed} s"
        )
        return summary_data, started_ev

    def _publish_finished(self, summary_data: dict, started_ev: Optional[threading.Event]):
        """Publica el evento FINISHED tras garantizar el orden estricto de eventos."""
        # 1. Esperar a que la publicación de STARTED haya concluido su intento
        if started_ev is not None:
            started_ev.wait()

        # 2. Esperar a que se publiquen las mediciones de telemetría pendientes.
        with self.telemetry_done:
            while self.pending_telemetry > 0:
                self.telemetry_done.wait()

        # 3. Publicar el evento final después de STARTED y de la telemetría.
        sent = self.send_event("FINISHED", **summary_data)
        reason = summary_data.get("reason", "")
        tx_id = summary_data.get("transaction_id")

        if reason in ("LEAK", "MONITOR_DOWN") or not self.central_connected or not sent:
            with self.lock:
                self.last_interrupted_summary = summary_data
                if not sent or not self.central_connected:
                    if not any(p.get("transaction_id") == tx_id for p in self.pending_finished_events):
                        self.pending_finished_events.append(summary_data)

    def finish_watering(self, reason: str):
        with self.lock:
            summary_data, started_ev = self._close_watering_locked(reason)

        if summary_data:
            self._publish_finished(summary_data, started_ev)

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
                    if not isinstance(msg.value, dict):
                        continue

                    target_ws = str(msg.value.get("ws", "")).strip().upper()
                    if target_ws != self.ws_id:
                        continue
                    data = self._open_command(msg.value)
                    if data is None:
                        continue

                    cmd = str(data.get("cmd", "")).strip().upper()
                    if cmd == "START":
                        op = str(data.get("op", "CENTRAL")).strip().upper()
                        duration = int(data.get("duration", 10))
                        tx_id = data.get("transaction_id")
                        self.start_watering(op, duration, transaction_id=tx_id)

                    elif cmd == "STOP":
                        reason = data.get("reason", "CENTRAL_STOP")
                        if reason == "WEATHER_ALERT":
                            self.log("Aviso de alerta meteorológica: el riego finalizará con normalidad y la estación quedará fuera de servicio.")
                            # Si está regando con duración programada, dejamos que concluya normalmente
                            # y marcamos pending_weather_block para pasar a BLOCKED al terminar.
                            with self.lock:
                                if self.state == "WATERING" and self.duration_limit > 0:
                                    self.pending_weather_block = True
                                else:
                                    self.pending_weather_block = False
                            if not getattr(self, "pending_weather_block", False):
                                self.finish_watering("WEATHER_ALERT")
                                with self.lock:
                                    self.state = "BLOCKED"
                        else:
                            self.finish_watering(reason)

                    elif cmd == "BLOCK":
                        with self.lock:
                            self.pending_weather_block = False
                        self.finish_watering("CENTRAL_BLOCK")
                        self.log("Estación puesta FUERA DE SERVICIO por orden de CENTRAL")

                    elif cmd == "ACK_FINISHED":
                        tx_ack = data.get("transaction_id")
                        if tx_ack:
                            self._confirm_finished(str(tx_ack))

                    elif cmd == "ACTIVATE":
                        with self.lock:
                            self.pending_weather_block = False
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
                    ko_now = self.is_ko
                    if ko_now:
                        self.log("\033[91;1m[SIMULACIÓN] Sensor en estado de AVERÍA (KO / Fuga activa)\033[0m")
                    else:
                        self.log("\033[92;1m[SIMULACIÓN] Avería RESUELTA (Sensor en estado OK)\033[0m")
                        if self.last_interrupted_summary:
                            s = self.last_interrupted_summary
                            self.log(
                                f"[RECUPERACIÓN] Estado final del riego previo antes de la avería: "
                                f"{s['vol']:.2f} L consumidos en {s['dur']} s (Operario: {s['op']})"
                            )
                if ko_now:
                    self.finish_watering("LEAK")
                else:
                    self.trigger_flush_pending()

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
                to_finish = (self.state == "WATERING")
            if to_finish:
                self.finish_watering("LEAK")


# ==============================================================================
# ENTRADA PRINCIPAL
# ==============================================================================
def main():
    try:
        require_cryptography()
    except RuntimeError as exc:
        print(f"[ENGINE] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

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
