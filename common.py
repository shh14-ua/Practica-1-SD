#!/usr/bin/env python3
"""
Módulo común para la práctica WaterManagement (Sistemas Distribuidos).
Contiene:
  - Protocolo de tramas por sockets: <STX><DATA><ETX><LRC> con cálculo byte a byte por XOR.
  - Handshake de conexión por sockets (ENQ / ACK).
  - Definición de estados, códigos ANSI de colores según especificación.
  - Tópicos y helpers de Apache Kafka (productor y consumidor).
  - Utilidades de parseo de direcciones y argumentos CLI flexibles.
"""

import json
import socket
import sys
import time
import uuid
from typing import Optional, Tuple

# Importación de Kafka (librería kafka-python-ng)
try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
except ImportError:
    pass

# ==============================================================================
# 1. PROTOCOLO DE TRAMAS POR SOCKETS (<STX><DATA><ETX><LRC>)
# ==============================================================================
STX = b"\x02"  # Start of Text
ETX = b"\x03"  # End of Text
ENQ = b"\x05"  # Enquiry (Handshake)
ACK = b"\x06"  # Acknowledge (Confirmación positiva)
NACK = b"\x15" # Negative Acknowledge (Error de verificación)
EOT = b"\x04"  # End of Transmission

def lrc(data: bytes) -> bytes:
    """Calcula el Longitudinal Redundancy Check (LRC) como XOR byte a byte."""
    xor_sum = 0
    for byte in data:
        xor_sum ^= byte
    return bytes([xor_sum])

def _read_exact(sock: socket.socket, num_bytes: int) -> bytes:
    """Lee exactamente num_bytes del socket. Lanza ConnectionError si el socket se cierra."""
    buffer = b""
    while len(buffer) < num_bytes:
        chunk = sock.recv(num_bytes - len(buffer))
        if not chunk:
            raise ConnectionError("El socket se cerró mientras se esperaba información.")
        buffer += chunk
    return buffer

def send_frame(sock: socket.socket, message: str, retries: int = 3) -> bool:
    """
    Envía una trama con el formato <STX><DATA><ETX><LRC>.
    Espera confirmación inmediata <ACK>. Si recibe <NACK>, reintenta hasta 'retries' veces.
    """
    payload = message.encode("utf-8")
    frame = STX + payload + ETX + lrc(payload)
    for _ in range(retries):
        try:
            sock.sendall(frame)
            resp = _read_exact(sock, 1)
            if resp == ACK:
                return True
        except (OSError, ConnectionError):
            break
    return False

def recv_frame(sock: socket.socket) -> Optional[str]:
    """
    Recibe una trama con el formato <STX><DATA><ETX><LRC>.
    Comprueba el LRC:
      - Si es válido: responde <ACK> y devuelve el contenido de DATA decodificado en UTF-8.
      - Si es inválido: responde <NACK> y devuelve None.
    """
    try:
        first_byte = _read_exact(sock, 1)
        # Sincronización en caso de bytes espurios: avanzar hasta encontrar STX
        while first_byte != STX:
            first_byte = _read_exact(sock, 1)

        data = b""
        while True:
            byte = _read_exact(sock, 1)
            if byte == ETX:
                break
            data += byte

        expected_lrc = _read_exact(sock, 1)
        actual_lrc = lrc(data)

        if expected_lrc == actual_lrc:
            sock.sendall(ACK)
            return data.decode("utf-8", errors="replace")
        else:
            sock.sendall(NACK)
            return None
    except (OSError, ConnectionError):
        return None

def recv_msg(sock: socket.socket) -> str:
    """Lee mensajes hasta recibir una trama íntegra y válida con ACK."""
    while True:
        msg = recv_frame(sock)
        if msg is not None:
            return msg
        # Si se devolvió None porque el socket murió, _read_exact lanza excepción
        # Si fue un NACK por corrupción de datos, el emisor reintentará.

def request_socket(sock: socket.socket, request_msg: str) -> str:
    """Cliente: envía una solicitud <REQUEST> y espera la respuesta <ANSWER>."""
    if not send_frame(sock, request_msg):
        raise ConnectionError("Fallo al enviar trama por socket (NACK o timeout).")
    return recv_msg(sock)

def serve_one_socket(sock: socket.socket, handler_func) -> None:
    """Servidor: recibe una petición, aplica handler_func(peticion) y devuelve la respuesta."""
    req = recv_msg(sock)
    res = handler_func(req)
    if res is not None:
        send_frame(sock, str(res))

def client_handshake(sock: socket.socket) -> None:
    """Inicia sesión enviando <ENQ> y esperando <ACK> del servidor."""
    sock.sendall(ENQ)
    resp = _read_exact(sock, 1)
    if resp != ACK:
        raise ConnectionError(f"Handshake rechazado por el servidor: respuesta {resp!r}")

def server_handshake(sock: socket.socket) -> None:
    """Espera <ENQ> del cliente y responde con <ACK>."""
    req = _read_exact(sock, 1)
    if req != ENQ:
        raise ConnectionError(f"Handshake inválido: se esperaba ENQ y se recibió {req!r}")
    sock.sendall(ACK)


# ==============================================================================
# 2. CONSTANTES DE ESTADO Y ESTILOS DE TERMINAL
# ==============================================================================
STATUS_AVAILABLE = "DISPONIBLE"
STATUS_WATERING = "REGANDO"
STATUS_LEAK = "FUGA"
STATUS_OUT_OF_SERVICE = "FUERA DE SERVICIO"
STATUS_OFFLINE = "DESCONECTADA"

# Códigos ANSI para la consola de CENTRAL
COLOR_RESET = "\033[0m"
COLOR_BOLD = "\033[1m"
COLOR_BLINK = "\033[5m"

# Colores de fondo especificados en la práctica:
# - Disponible: Fondo verde
# - Regando: Fondo verde parpadeando
# - Fuga: Fondo rojo
# - Fuera de servicio: Fondo naranja / amarillo con leyenda
# - Desconectada: Fondo gris
STATUS_STYLES = {
    STATUS_AVAILABLE: "\033[42;30m",               # Verde con texto negro
    STATUS_WATERING: "\033[5;42;30;1m",            # Verde parpadeante, negrita, texto negro
    STATUS_LEAK: "\033[41;97;1m",                  # Rojo con texto blanco en negrita
    STATUS_OUT_OF_SERVICE: "\033[48;5;208;30;1m",  # Naranja con texto negro en negrita
    STATUS_OFFLINE: "\033[100;97m",                # Gris con texto blanco
}

def format_status(status: str) -> str:
    """Devuelve el estado formateado con su color correspondiente y cierre ANSI."""
    style = STATUS_STYLES.get(status, "\033[7m")
    return f"{style} {status} {COLOR_RESET}"


# ==============================================================================
# 3. APACHE KAFKA (TÓPICOS Y CONEXIÓN)
# ==============================================================================
TOPIC_REQUESTS = "fo_requests"   # Field Operator -> CENTRAL (solicitudes de riego)
TOPIC_REPLIES = "fo_replies"     # CENTRAL -> Field Operator (estados, telemetría y respuestas)
TOPIC_COMMANDS = "ws_commands"   # CENTRAL -> WS Engine (START, STOP, BLOCK, ACTIVATE)
TOPIC_EVENTS = "ws_events"       # WS Engine -> CENTRAL (STARTED, TELEMETRY, FINISHED)
TOPIC_STATUS = "ws_status"       # CENTRAL -> Broadcast general (snapshot del estado de todas las WS)

ALL_TOPICS = [TOPIC_REQUESTS, TOPIC_REPLIES, TOPIC_COMMANDS, TOPIC_EVENTS, TOPIC_STATUS]

def ensure_topics(bootstrap: str, topics=ALL_TOPICS, max_retries: int = 15, delay: float = 2.0) -> bool:
    """Verifica y crea los tópicos necesarios en el broker Kafka."""
    for attempt in range(1, max_retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="wm-admin")
            existing = admin.list_topics()
            new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1)
                          for t in topics if t not in existing]
            if new_topics:
                admin.create_topics(new_topics)
            admin.close()
            return True
        except (NoBrokersAvailable, Exception) as exc:
            if attempt == max_retries:
                print(f"[KAFKA] Aviso: no se pudieron crear tópicos automáticamente ({exc}). "
                      f"Se asumirá creación automática en broker.", file=sys.stderr)
                return False
            time.sleep(delay)
    return False

def make_producer(bootstrap: str, max_retries: int = 15, delay: float = 2.0) -> KafkaProducer:
    """Crea y devuelve una instancia conectada de KafkaProducer con serialización JSON."""
    for attempt in range(1, max_retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
        except NoBrokersAvailable:
            if attempt % 3 == 0 or attempt == 1:
                print(f"[KAFKA] Esperando broker en {bootstrap}... (intento {attempt}/{max_retries})")
            time.sleep(delay)
    raise ConnectionError(f"No se pudo conectar al broker Kafka en {bootstrap}")

def send_kafka(producer: KafkaProducer, topic: str, message_dict: dict) -> None:
    """Envía un diccionario como mensaje JSON al tópico indicado y realiza flush."""
    try:
        producer.send(topic, message_dict)
        producer.flush()
    except Exception as exc:
        print(f"[KAFKA ERROR] Error enviando mensaje a '{topic}': {exc}", file=sys.stderr)

def make_consumer(bootstrap: str, topics, group_prefix: str = "wm", auto_offset_reset: str = "latest") -> KafkaConsumer:
    """
    Crea un KafkaConsumer para los tópicos indicados.
    Usa un group_id único para garantizar que cada réplica/operario reciba los mensajes en tiempo real.
    """
    group_id = f"{group_prefix}-{uuid.uuid4().hex[:8]}"
    return KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


# ==============================================================================
# 4. PARSEO FLEXIBLE DE DIRECCIONES Y LÍNEA DE COMANDOS
# ==============================================================================
def parse_addr(address_str: str, default_port: Optional[int] = None) -> Tuple[str, int]:
    """
    Convierte una cadena en una tupla (host, puerto).
    Acepta formatos como: 'localhost:5000', '192.168.1.10:9092', '5000' (con default_port).
    """
    address_str = address_str.strip()
    if ":" in address_str:
        host, port_str = address_str.rsplit(":", 1)
        return host, int(port_str)
    elif address_str.isdigit() and default_port is None:
        # Se pasó únicamente un número de puerto
        return "0.0.0.0", int(address_str)
    elif default_port is not None:
        return address_str, default_port
    else:
        raise ValueError(f"Formato de dirección inválido: '{address_str}'. Se esperaba 'host:puerto'.")

def parse_host_port_pair(args_list: list, start_idx: int) -> Tuple[str, int, int]:
    """
    Consume 1 o 2 elementos de args_list dependiendo de si el argumento viene como:
      - 'host:puerto' (1 elemento)
      - 'host' 'puerto' (2 elementos)
    Devuelve (host, puerto, nuevo_indice).
    """
    first = args_list[start_idx]
    if ":" in first:
        host, port = parse_addr(first)
        return host, port, start_idx + 1
    # Comprobar si el siguiente es un puerto numérico
    if start_idx + 1 < len(args_list) and args_list[start_idx + 1].isdigit():
        return first, int(args_list[start_idx + 1]), start_idx + 2
    # Caso por defecto si solo hay número
    if first.isdigit():
        return "0.0.0.0", int(first), start_idx + 1
    raise ValueError(f"No se pudo determinar host y puerto a partir del argumento '{first}'.")
