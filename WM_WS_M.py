#!/usr/bin/env python3
"""
WM_WS_M: Monitor de la Estación de Riego (Watering Station Monitor).
Funcionalidades:
  - Conexión por socket cliente hacia WM_Central para autenticación y registro.
  - Servidor de socket TCP para conectar y supervisar al motor de la estación (WM_WS_E).
  - Asignación del ID de estación al motor conectado.
  - Sondeo de salud continuo cada segundo (HEALTH) hacia el motor.
  - Notificación de avería/fuga (LEAK) a WM_Central si el motor responde KO o no responde.
  - Notificación de avería resuelta (FIXED) a WM_Central cuando la salud vuelve a OK.
  - Envío periódico de latidos (PING) hacia WM_Central para mantener viva la conexión.
"""

import argparse
import socket
import sys
import time
from typing import Optional

from common import (
    client_handshake, server_handshake, request_socket,
    parse_addr
)

class WSMonitor:
    def __init__(self, engine_port: int, central_addr: str, ws_id: str, location: str = "Sin ubicación"):
        self.engine_port = engine_port
        self.central_host, self.central_port = parse_addr(central_addr)
        self.ws_id = ws_id.strip().upper()
        self.location = location
        self.running = True

        self.central_sock: Optional[socket.socket] = None
        self.engine_sock: Optional[socket.socket] = None
        self.leak_active = True

    def log(self, msg: str):
        print(f"[{time.strftime('%H:%M:%S')}] [MONITOR {self.ws_id}] {msg}", flush=True)

    # --------------------------------------------------------------------------
    # CONEXIÓN CON CENTRAL
    # --------------------------------------------------------------------------
    def ensure_central_connection(self) -> bool:
        if self.central_sock is not None:
            return True

        try:
            sock = socket.create_connection((self.central_host, self.central_port), timeout=5.0)
            client_handshake(sock)
            sock.settimeout(6.0)

            # Enviamos trama de registro y autenticación según el anexo
            reg_msg = f"REGISTRO#{self.ws_id}#{self.location}"
            resp = request_socket(sock, reg_msg)

            if "OK" in resp:
                self.central_sock = sock
                self.log(f"Autenticado correctamente en CENTRAL ({self.central_host}:{self.central_port})")
                # Si reconectamos y había fuga, lo re-notificamos inmediatamente
                if self.leak_active:
                    self.send_to_central(f"LEAK#{self.ws_id}")
                return True
            else:
                self.log(f"CENTRAL rechazó el registro: {resp}")
                sock.close()
                return False
        except (OSError, ConnectionError) as exc:
            self.log(f"Aviso: CENTRAL no disponible ({exc}). Reintentando conexión...")
            if self.central_sock:
                try: self.central_sock.close()
                except Exception: pass
            self.central_sock = None
            return False

    def send_to_central(self, msg: str) -> bool:
        """Envía un mensaje por socket a CENTRAL y espera confirmación."""
        if not self.ensure_central_connection():
            return False
        try:
            resp = request_socket(self.central_sock, msg)
            return "OK" in resp
        except (OSError, ConnectionError):
            self.log("Se perdió la conexión con CENTRAL al enviar mensaje.")
            try: self.central_sock.close()
            except Exception: pass
            self.central_sock = None
            return False

    # --------------------------------------------------------------------------
    # BUCLE PRINCIPAL DE MONITORIZACIÓN
    # --------------------------------------------------------------------------
    def run(self):
        # 1. Crear servidor de sockets para el Engine
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.engine_port))
        srv.listen(1)
        srv.settimeout(1.0)
        self.log(f"Servidor de sockets escuchando en puerto {self.engine_port} a la espera de WM_WS_E...")

        tick = 0
        while self.running:
            # Mantener conexión con CENTRAL
            self.ensure_central_connection()

            # Aceptar conexión del Engine si no está conectado
            if self.engine_sock is None:
                try:
                    c, addr = srv.accept()
                    server_handshake(c)
                    c.settimeout(3.0)
                    # Asignar el ID de estación al Engine según especificación
                    resp = request_socket(c, f"ASSIGN#{self.ws_id}")
                    if "OK" in resp:
                        self.engine_sock = c
                        self.log(f"Engine conectado desde {addr} y asignado con ID {self.ws_id}")
                    else:
                        c.close()
                except (OSError, ConnectionError):
                    pass
            else:
                time.sleep(1.0)

            health = "KO"

            if self.engine_sock:
                try:
                    health = request_socket(self.engine_sock, "HEALTH")
                except (OSError, ConnectionError):
                    self.log("El Engine dejó de responder a la comprobación de salud.")
                    try:
                        self.engine_sock.close()
                    except Exception:
                        pass
                    self.engine_sock = None
                    health = "KO"

            current_is_leak = (health != "OK")

            # Si cambia el estado de fuga, informar a CENTRAL inmediatamente
            if current_is_leak != self.leak_active:
                if current_is_leak:
                    self.log("¡AVERÍA / FUGA DETECTADA! Notificando a CENTRAL...")
                    if self.send_to_central(f"LEAK#{self.ws_id}"):
                        self.leak_active = True
                else:
                    self.log("Avería resuelta (salud OK). Notificando a CENTRAL...")
                    if self.send_to_central(f"FIXED#{self.ws_id}"):
                        self.leak_active = False

            # Latido periódico hacia CENTRAL cada 3 ticks (3 segundos)
            tick += 1
            if tick % 3 == 0:
                self.send_to_central(f"PING#{self.ws_id}")


# ==============================================================================
# ENTRADA PRINCIPAL
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="WM_WS_M: Monitor de la Estación de Riego (Watering Station Monitor)."
    )
    parser.add_argument("args", nargs="*", help="[puerto_engine] [central_host:puerto] [ws_id] o con argumentos separados")
    parser.add_argument("--loc", default="Parque Municipal", help="Ubicación física de la estación (por defecto Parque Municipal)")

    parsed = parser.parse_args()
    raw = parsed.args

    if len(raw) < 3:
        print("Uso: python WM_WS_M.py <puerto_engine> <central_host:puerto> <ID_WS> [--loc UBICACION]")
        print("Ejemplo: python WM_WS_M.py 6001 localhost:5000 WS-01 --loc 'River Park'")
        sys.exit(1)

    try:
        engine_port = int(raw[0])
        central_addr = raw[1]
        ws_id = raw[2]
    except Exception as e:
        print(f"Error parseando argumentos: {e}")
        sys.exit(1)

    monitor = WSMonitor(
        engine_port=engine_port,
        central_addr=central_addr,
        ws_id=ws_id,
        location=parsed.loc
    )
    monitor.run()


if __name__ == "__main__":
    main()
