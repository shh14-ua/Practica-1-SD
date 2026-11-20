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
import urllib.error
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

DEFAULT_REGISTRY_CA = os.getenv(
    "WM_REGISTRY_CA", os.path.join(ROOT_DIR, "WM_Registry", "certs", "registry_cert.pem")
)
DEFAULT_CRED_DIR = os.getenv("WM_WS_CRED_DIR", os.path.join(ROOT_DIR, "WM_WS", ".credentials"))


def mask_secret(value: Optional[str]) -> str:
    """Muestra solo el inicio de un secreto para logs y consola."""
    if not value:
        return "—"
    return value[:12] + "…" if len(value) > 12 else "…"


class WSMonitor:
    def __init__(self, engine_port: int, central_addr: str, ws_id: str,
                 location: str = "Parque Municipal", registry_url: str = "https://localhost:5500",
                 registry_ca: Optional[str] = None, cred_file: Optional[str] = None):
        self.engine_port = engine_port
        self.central_host, self.central_port = parse_addr(central_addr)
        self.ws_id = ws_id.strip().upper()
        self.location = location
        self.registry_url = registry_url.rstrip("/")
        # Certificado del Registry usado como CA de confianza para validar el canal HTTPS
        self.registry_ca = registry_ca or DEFAULT_REGISTRY_CA
        self.running = True

        # Credenciales obtenidas de WM_Registry (Release 2), persistidas en un fichero local (0600)
        # para que un reinicio del Monitor no obligue a una baja administrativa.
        self.cred_file = cred_file or os.path.join(DEFAULT_CRED_DIR, f"{self.ws_id}.cred")
        self.credential: Optional[str] = self._load_credential()
        # Tras una baja voluntaria no se vuelve a dar de alta automáticamente (solo con la opción 'r')
        self.deregistered: bool = False

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
    def _registry_ssl_context(self) -> ssl.SSLContext:
        """Contexto TLS que valida el certificado del Registry (cadena y nombre de host)."""
        if not os.path.isfile(self.registry_ca):
            raise FileNotFoundError(
                f"No se encuentra el certificado del Registry ({self.registry_ca}). "
                f"Cópielo desde WM_Registry o indique su ruta con --registry-ca."
            )
        return ssl.create_default_context(cafile=self.registry_ca)

    def _load_credential(self) -> Optional[str]:
        try:
            with open(self.cred_file, "r", encoding="utf-8") as f:
                value = f.read().strip()
                return value or None
        except OSError:
            return None

    def _save_credential(self, value: str):
        try:
            os.makedirs(os.path.dirname(self.cred_file) or ".", exist_ok=True)
            fd = os.open(self.cred_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(value)
            try:
                os.chmod(self.cred_file, 0o600)
            except OSError:
                pass
        except OSError as exc:
            self.log(f"Aviso: no se pudo guardar la credencial en {self.cred_file} ({exc}).")

    def _delete_credential_file(self):
        try:
            os.remove(self.cred_file)
        except OSError:
            pass

    def _registry_request(self, method: str, path: str, body: dict):
        """Petición HTTPS al Registry validando su certificado. Devuelve (código, json)."""
        req = urllib.request.Request(
            f"{self.registry_url}{path}",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method=method
        )
        try:
            with urllib.request.urlopen(req, context=self._registry_ssl_context(), timeout=4.0) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
        except urllib.error.HTTPError as err:
            try:
                return err.code, json.loads(err.read().decode("utf-8") or "{}")
            except Exception:
                return err.code, {}

    def register_in_registry(self) -> bool:
        """Alta en WM_Registry por HTTPS (con validación de certificado).
        Si ya hay credencial se presenta para renovarla; el Registry emite una nueva."""
        self.log(f"Conectando a WM_Registry en {self.registry_url} vía HTTPS seguro...")
        payload = {"id": self.ws_id, "location": self.location}
        if self.credential:
            payload["credential"] = self.credential
        try:
            status, data = self._registry_request("POST", "/api/stations", payload)
        except Exception as exc:
            self.log(f"Fallo de conexión segura con WM_Registry ({exc}). Reintentando...")
            return False

        if status in (200, 201):
            new_cred = (data.get("data") or {}).get("credential")
            if not new_cred:
                self.log("Respuesta del Registry sin credencial. Alta no completada.")
                return False
            self.credential = new_cred
            self._save_credential(new_cred)
            self.deregistered = False
            self.key_revoked = False
            accion = "Alta completada" if status == 201 else "Credencial renovada"
            self.log(f"¡{accion} en WM_Registry! Credencial obtenida: {mask_secret(self.credential)}")
            return True
        if status == 403:
            self.log(f"WM_Registry RECHAZÓ el alta: {data.get('message', 'credencial actual requerida')}")
            return False
        self.log(f"Error al registrar en WM_Registry: Código HTTP {status} ({data.get('message', '')})")
        return False

    def deregister_from_registry(self) -> bool:
        """Baja voluntaria de la estación en WM_Registry presentando su credencial actual."""
        if not self.credential:
            self.log("No hay credencial: la estación no está dada de alta desde este Monitor.")
            return False
        try:
            status, data = self._registry_request("DELETE", f"/api/stations/{self.ws_id}",
                                                  {"credential": self.credential})
        except Exception as exc:
            self.log(f"Fallo de conexión segura con WM_Registry ({exc}).")
            return False
        if status == 200 or status == 404:
            self.log("Estación dada de baja en WM_Registry." if status == 200
                     else "La estación ya no figuraba en WM_Registry.")
            self.credential = None
            self._delete_credential_file()
            self.deregistered = True
            # Sin registro no puede seguir prestando servicio: se cierra la sesión con Central
            if self.central_sock:
                try: self.central_sock.close()
                except Exception: pass
                self.central_sock = None
            self.symmetric_key = None
            return True
        self.log(f"WM_Registry rechazó la baja (HTTP {status}): {data.get('message', '')}")
        return False

    # --------------------------------------------------------------------------
    # 2. CONEXIÓN Y AUTENTICACIÓN EN CENTRAL (CON CREDENCIALES Y CLAVE SIMÉTRICA)
    # --------------------------------------------------------------------------
    def ensure_central_connection(self) -> bool:
        if self.central_sock is not None and self.symmetric_key is not None:
            return True

        # Si aún no tenemos credencial de Registry, realizamos el alta primero
        # (salvo tras una baja voluntaria: entonces solo se da de alta con la opción 'r')
        if not self.credential:
            if self.deregistered or not self.register_in_registry():
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
        print("    b  -> Darse de baja en WM_Registry")
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
                elif cmd == "b":
                    self.deregister_from_registry()
                elif cmd == "a":
                    self.ensure_central_connection()
                elif cmd == "k":
                    self.reauthenticate()
                elif cmd == "s":
                    print(f"\n--- ESTADO DE SEGURIDAD [{self.ws_id}] ---")
                    print(f"  • Ubicación:      {self.location}")
                    print(f"  • Registry URL:   {self.registry_url}")
                    print(f"  • Credencial:     {mask_secret(self.credential) if self.credential else 'No registrada aún'}")
                    print(f"  • CA del Registry:{self.registry_ca}")
                    print(f"  • Clave simétrica:{self.symmetric_key[:12] + '...' if self.symmetric_key else 'Sin clave / Revocada'}")
                    print(f"  • Clave revocada: {'SÍ (Fuera de Servicio)' if self.key_revoked else 'NO'}")
                    print(f"  • Central Socket: {'Conectado' if self.central_sock else 'Desconectado'}")
                    print(f"  • Engine Socket:  {'Conectado' if self.engine_sock else 'Desconectado'}\n")
                elif cmd == "q":
                    print("Cerrando WM_WS_M...")
                    self.running = False
                    os._exit(0)
                elif cmd:
                    print("Opción no válida. Use 'r', 'b', 'a', 'k', 's' o 'q'.")
            except Exception as e:
                print(f"Error procesando comando: {e}")


def main():
    parser = argparse.ArgumentParser(description="WM_WS_M: Monitor de la Estación de Riego (Release 2)")
    parser.add_argument("args", nargs="*", help="[puerto_engine] [central_host:puerto] [ws_id]")
    parser.add_argument("--loc", default="Parque Municipal", help="Ubicación física de la estación")
    parser.add_argument("--registry", default="https://localhost:5500", help="URL de WM_Registry (HTTPS)")
    parser.add_argument("--registry-ca", default=None,
                        help="Certificado del Registry para validar HTTPS (por defecto $WM_REGISTRY_CA o "
                             "WM_Registry/certs/registry_cert.pem)")
    parser.add_argument("--cred-file", default=None,
                        help="Fichero local donde se guarda la credencial (por defecto WM_WS/.credentials/<ID>.cred)")

    parsed = parser.parse_args()
    raw = parsed.args

    if len(raw) < 3:
        print("Uso: python WM_WS_M.py <puerto_engine> <central_host:puerto> <ID_WS> [--loc UBICACION] [--registry URL] [--registry-ca CERT]")
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
        registry_url=parsed.registry,
        registry_ca=parsed.registry_ca,
        cred_file=parsed.cred_file
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
