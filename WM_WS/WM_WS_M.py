#!/usr/bin/env python3
"""
WM_WS_M: Monitor de la Estación de Riego (Release 2 - Seguridad y API REST).
Funcionalidades:
  - Registro seguro previo en WM_Registry a través de canal cifrado HTTPS/SSL.
  - Recepción y almacenamiento de credenciales seguras de acceso otorgadas por el Registry.
  - Conexión por socket cliente hacia WM_Central y autenticación mediante credenciales.
  - Recepción de la clave de cifrado simétrico ÚNICA asignada por la Central.
  - Cifrado simétrico de todas las comunicaciones por socket hacia la Central.
  - Detección y gestión de revocación de claves por supuesta vulnerabilidad.
  - Servidor de socket TCP para conectar y supervisar al motor de la estación (WM_WS_E).
  - Transmisión de la clave simétrica y del ID de estación al Engine conectado.
  - Menú interactivo por consola (opciones de registro, autenticación y diagnóstico).
"""

import argparse
import json
import os
import socket
import ssl
import sys
import threading
import time
import urllib.request
from typing import Optional

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common import (
    client_handshake, server_handshake, request_socket,
    parse_addr, COLOR_BOLD, COLOR_RESET
)
from wm_crypto import (
    encrypt_message, decrypt_message, is_encrypted
)


class WSMonitor:
    def __init__(self, engine_port: int, central_addr: str, ws_id: str,
                 location: str = "Parque Municipal", registry_url: str = "https://localhost:5500"):
        self.engine_port = engine_port
        self.central_host, self.central_port = parse_addr(central_addr)
        self.ws_id = ws_id.strip().upper()
        self.location = location
        self.registry_url = registry_url.rstrip("/")
        self.running = True

        # Credenciales obtenidas de WM_Registry (Release 2)
        self.credential: Optional[str] = None

        # Clave simétrica única asignada por WM_Central tras autenticación (Release 2)
        self.symmetric_key: Optional[str] = None
        self.key_revoked: bool = False

        self.central_sock: Optional[socket.socket] = None
        self.engine_sock: Optional[socket.socket] = None
        self.leak_active = True
        self.leak_reason: str = "NO_ENGINE"
        self.central_blocked: bool = False
        self.engine_block_synced: Optional[bool] = None

    def log(self, msg: str):
        print(f"[{time.strftime('%H:%M:%S')}] [MONITOR {self.ws_id}] {msg}", flush=True)

    # --------------------------------------------------------------------------
    # 1. REGISTRO SEGURO EN WM_REGISTRY (HTTPS / REST)
    # --------------------------------------------------------------------------
    def register_in_registry(self) -> bool:
        """Realiza la petición de alta en WM_Registry a través de un canal seguro HTTPS."""
        self.log(f"Conectando a WM_Registry en {self.registry_url} vía HTTPS seguro...")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # Acepta certificado autofirmado del laboratorio

        payload = json.dumps({"id": self.ws_id, "location": self.location}).encode("utf-8")
        req = urllib.request.Request(
            f"{self.registry_url}/api/stations",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=4.0) as resp:
                if resp.status in (200, 201):
                    raw = resp.read().decode("utf-8")
                    data = json.loads(raw)
                    st_data = data.get("data", {})
                    self.credential = st_data.get("credential")
                    self.key_revoked = False
                    self.log(f"¡Alta completada en WM_Registry! Credencial obtenida: {self.credential}")
                    return True
                else:
                    self.log(f"Error al registrar en WM_Registry: Código HTTP {resp.status}")
                    return False
        except Exception as exc:
            self.log(f"Fallo de conexión segura con WM_Registry ({exc}). Reintentando...")
            return False

    # --------------------------------------------------------------------------
    # 2. CONEXIÓN Y AUTENTICACIÓN EN CENTRAL (CON CREDENCIALES Y CLAVE SIMÉTRICA)
    # --------------------------------------------------------------------------
    def ensure_central_connection(self) -> bool:
        if self.central_sock is not None and self.symmetric_key is not None:
            return True

        # Si aún no tenemos credencial de Registry, realizamos el alta primero
        if not self.credential:
            if not self.register_in_registry():
                return False

        try:
            sock = socket.create_connection((self.central_host, self.central_port), timeout=5.0)
            client_handshake(sock)
            sock.settimeout(6.0)

            # Enviamos solicitud de autenticación con la credencial del Registry (Release 2)
            auth_msg = f"AUTH#{self.ws_id}#{self.credential}#{self.location}"
            resp = request_socket(sock, auth_msg)

            if resp.startswith("STATUS#OK#"):
                parts = resp.split("#")
                # Parts: STATUS, OK, <sym_key>, <mensaje>
                if len(parts) >= 4 and len(parts[2]) >= 16:
                    self.symmetric_key = parts[2].strip()
                    self.key_revoked = False
                    self.log(f"Autenticado en Central con éxito. Clave simétrica única recibida ({self.symmetric_key[:10]}...)")
                else:
                    self.log(f"Autenticado en Central pero respuesta no incluyó clave simétrica válida: {resp}")

                self.central_sock = sock
                self.central_blocked = "#BLOCKED" in resp
                self.leak_active = None

                # Transmitimos la clave simétrica al Engine si ya está conectado
                self._send_key_to_engine()
                return True
            else:
                self.log(f"Central DENEGÓ la autenticación: {resp}")
                sock.close()
                self.central_sock = None
                return False

        except (OSError, ConnectionError) as exc:
            self.log(f"Imposible conectar con WM_Central ({exc}). Reintentando conexión...")
            if self.central_sock:
                try: self.central_sock.close()
                except Exception: pass
            self.central_sock = None
            return False

    def send_to_central(self, msg: str) -> bool:
        """
        Envía un mensaje a CENTRAL cifrándolo simétricamente si la clave está activa.
        Descifra la respuesta recibida y detecta revocaciones de clave.
        """
        if not self.ensure_central_connection():
            return False

        outgoing_msg = msg
        if self.symmetric_key:
            outgoing_msg = encrypt_message(self.symmetric_key, msg)

        try:
            resp = request_socket(self.central_sock, outgoing_msg)

            # Si la respuesta viene cifrada, la desciframos
            if is_encrypted(resp) and self.symmetric_key:
                decrypted = decrypt_message(self.symmetric_key, resp)
                if decrypted is not None:
                    resp = decrypted

            # Detección de revocación de claves por supuesta vulnerabilidad (Release 2)
            if "REVOKED" in resp:
                self.log("¡ATENCIÓN! Central REVOCÓ las claves simétricas de esta estación. Estación en FUERA DE SERVICIO.")
                self.symmetric_key = None
                self.key_revoked = True
                self.central_blocked = True
                try: self.central_sock.close()
                except Exception: pass
                self.central_sock = None
                return False

            self.central_blocked = "#BLOCKED" in resp
            return "OK" in resp

        except (OSError, ConnectionError):
            self.log("Se perdió la conexión con WM_Central al transmitir mensaje.")
            if self.central_sock:
                try: self.central_sock.close()
                except Exception: pass
            self.central_sock = None
            return False

    def reauthenticate(self):
        """Opción de menú para renovar credencial y volver a autenticarse tras revocación de claves."""
        self.log("Iniciando proceso de re-autenticación y obtención de nuevas claves...")
        if self.central_sock:
            try: self.central_sock.close()
            except Exception: pass
            self.central_sock = None
        self.symmetric_key = None

        # 1. Renovar o confirmar registro en Registry
        if self.register_in_registry():
            # 2. Conectar y autenticar en Central
            if self.ensure_central_connection():
                self.log("Re-autenticación exitosa. Nuevas claves simétricas en funcionamiento.")
            else:
                self.log("Fallo al autenticar en Central con las nuevas credenciales.")
        else:
            self.log("Fallo al contactar con WM_Registry.")

    # --------------------------------------------------------------------------
    # 3. COMUNICACIÓN CON ENGINE (WM_WS_E)
    # --------------------------------------------------------------------------
    def _send_key_to_engine(self):
        """Envía la clave simétrica al Engine por socket."""
        if self.engine_sock and self.symmetric_key:
            try:
                request_socket(self.engine_sock, f"SET_KEY#{self.symmetric_key}")
            except Exception:
                pass

    def _build_health_cmd(self, central_ok: bool) -> str:
        base_flag = "OK" if central_ok else "NO_CENTRAL"
        if self.engine_block_synced != self.central_blocked:
            if self.central_blocked:
                return f"HEALTH#{base_flag}#BLOCKED"
            if self.engine_block_synced is not None:
                return f"HEALTH#{base_flag}#ACTIVE"
        return f"HEALTH#{base_flag}"

    def _sync_engine_block_if_needed(self, central_ok: bool):
        if self.engine_sock is None or self.engine_block_synced == self.central_blocked:
            return
        try:
            cmd = f"HEALTH#{'OK' if central_ok else 'NO_CENTRAL'}#{'BLOCKED' if self.central_blocked else 'ACTIVE'}"
            request_socket(self.engine_sock, cmd)
            self.engine_block_synced = self.central_blocked
        except (OSError, ConnectionError):
            pass

    # --------------------------------------------------------------------------
    # 4. BUCLE PRINCIPAL DE MONITORIZACIÓN
    # --------------------------------------------------------------------------
    def run(self):
        """Alias de compatibilidad con pruebas de la Release 1."""
        return self.run_monitoring_loop()

    def run_monitoring_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.engine_port))
        srv.listen(1)
        srv.settimeout(1.0)
        self.log(f"Servidor de sockets escuchando en puerto {self.engine_port} a la espera de WM_WS_E...")

        tick = 0
        while self.running:
            # Mantener conexión y autenticación con CENTRAL
            central_ok = self.ensure_central_connection()

            # Aceptar conexión del Engine si no está conectado
            if self.engine_sock is None:
                try:
                    c, addr = srv.accept()
                    server_handshake(c)
                    c.settimeout(3.0)
                    resp = request_socket(c, f"ASSIGN#{self.ws_id}")
                    if "OK" in resp:
                        self.engine_sock = c
                        self.engine_block_synced = None
                        self.log(f"Engine conectado desde {addr} y asignado con ID {self.ws_id}")
                        self._send_key_to_engine()
                    else:
                        c.close()
                except (OSError, ConnectionError):
                    pass
            else:
                time.sleep(1.0)

            health = "KO"
            leak_reason = "NO_ENGINE"

            if self.engine_sock:
                try:
                    health_cmd = self._build_health_cmd(central_ok)
                    health = request_socket(self.engine_sock, health_cmd)
                    self.engine_block_synced = self.central_blocked
                    leak_reason = "OK" if health == "OK" else "SENSOR_KO"
                except (OSError, ConnectionError):
                    self.log("El Engine dejó de responder a la comprobación de salud.")
                    try: self.engine_sock.close()
                    except Exception: pass
                    self.engine_sock = None
                    self.engine_block_synced = None
                    health = "KO"
                    leak_reason = "NO_ENGINE"

            prev_reason = self.leak_reason
            self.leak_reason = leak_reason
            current_is_leak = (health != "OK")

            # Notificar cambios de salud / averías a Central
            if current_is_leak != self.leak_active or (current_is_leak and leak_reason != prev_reason):
                if current_is_leak:
                    self.log(f"¡AVERÍA / FUGA DETECTADA! ({leak_reason}) Notificando a CENTRAL (cifrado)...")
                    if self.send_to_central(f"LEAK#{self.ws_id}#{leak_reason}"):
                        self.leak_active = True
                        self._sync_engine_block_if_needed(central_ok)
                else:
                    self.log("Avería resuelta (salud OK). Notificando a CENTRAL (cifrado)...")
                    if self.send_to_central(f"FIXED#{self.ws_id}"):
                        self.leak_active = False
                        self._sync_engine_block_if_needed(central_ok)

            # Latido periódico PING cada 3 ticks (3 segundos) cifrado simétricamente
            tick += 1
            if tick % 3 == 0:
                state_code = "KO" if current_is_leak else "OK"
                if self.send_to_central(f"PING#{self.ws_id}#{state_code}#{leak_reason}"):
                    self._sync_engine_block_if_needed(central_ok)

    # --------------------------------------------------------------------------
    # 5. MENÚ INTERACTIVO EN CONSOLA (SEGÚN ESPECIFICACIÓN PÁG. 4)
    # --------------------------------------------------------------------------
    def run_cli_menu(self):
        time.sleep(0.5)
        print("\n" + "=" * 60)
        print(f"  {COLOR_BOLD}MONITOR ESTACIÓN {self.ws_id} — MENÚ DE CONTROL{COLOR_RESET}")
        print("  Opciones disponibles:")
        print("    r  -> Registrarse en WM_Registry (Canal seguro HTTPS)")
        print("    a  -> Autenticarse en WM_Central y recibir clave simétrica")
        print("    k  -> Re-autenticarse ante revocación de claves")
        print("    s  -> Ver estado de seguridad (credencial y clave simétrica)")
        print("    q  -> Salir")
        print("=" * 60 + "\n")

        while self.running:
            try:
                line = sys.stdin.readline()
                if not line:
                    break
                cmd = line.strip().lower()

                if cmd == "r":
                    self.register_in_registry()
                elif cmd == "a":
                    self.ensure_central_connection()
                elif cmd == "k":
                    self.reauthenticate()
                elif cmd == "s":
                    print(f"\n--- ESTADO DE SEGURIDAD [{self.ws_id}] ---")
                    print(f"  • Ubicación:      {self.location}")
                    print(f"  • Registry URL:   {self.registry_url}")
                    print(f"  • Credencial:     {self.credential or 'No registrada aún'}")
                    print(f"  • Clave simétrica:{self.symmetric_key[:12] + '...' if self.symmetric_key else 'Sin clave / Revocada'}")
                    print(f"  • Clave revocada: {'SÍ (Fuera de Servicio)' if self.key_revoked else 'NO'}")
                    print(f"  • Central Socket: {'Conectado' if self.central_sock else 'Desconectado'}")
                    print(f"  • Engine Socket:  {'Conectado' if self.engine_sock else 'Desconectado'}\n")
                elif cmd == "q":
                    print("Cerrando WM_WS_M...")
                    self.running = False
                    os._exit(0)
                elif cmd:
                    print("Opción no válida. Use 'r', 'a', 'k', 's' o 'q'.")
            except Exception as e:
                print(f"Error procesando comando: {e}")


def main():
    parser = argparse.ArgumentParser(description="WM_WS_M: Monitor de la Estación de Riego (Release 2)")
    parser.add_argument("args", nargs="*", help="[puerto_engine] [central_host:puerto] [ws_id]")
    parser.add_argument("--loc", default="Parque Municipal", help="Ubicación física de la estación")
    parser.add_argument("--registry", default="https://localhost:5500", help="URL de WM_Registry (HTTPS)")

    parsed = parser.parse_args()
    raw = parsed.args

    if len(raw) < 3:
        print("Uso: python WM_WS_M.py <puerto_engine> <central_host:puerto> <ID_WS> [--loc UBICACION] [--registry URL]")
        print("Ejemplo: python WM_WS_M.py 6001 localhost:5000 WS-01 --loc 'River Park' --registry https://localhost:5500")
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
        location=parsed.loc,
        registry_url=parsed.registry
    )

    t = threading.Thread(target=monitor.run_monitoring_loop, daemon=True)
    t.start()

    if sys.stdin.isatty():
        monitor.run_cli_menu()
    else:
        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()
