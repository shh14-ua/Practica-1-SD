#!/usr/bin/env python3
"""
WM_Registry: Módulo de Registro Seguro de Estaciones de Riego (Release 2).
Funcionalidades:
  - Servidor API REST sobre canal seguro HTTPS/SSL.
  - Gestión de alta (POST / PUT), baja (DELETE) y consulta (GET) de estaciones de riego (WS).
  - Generación segura de credenciales de autenticación para cada estación.
  - Endpoint de verificación de credenciales para consumo por parte de WM_Central.
  - Persistencia de estaciones registradas en base de datos SQLite (registry.db).
  - Interfaz de consola interactiva para el administrador.
"""

import argparse
import hmac
import http.server
import ipaddress
import json
import os
import re
import socket
import sqlite3
import ssl
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from common import safe_db_path
from wm_crypto import generate_credential

STATION_ID_RE = re.compile(r"^WS-\d{2,}$")


class RegistryDatabase:
    """Gestor de persistencia en SQLite para el registro de estaciones."""

    def __init__(self, db_path: str = "registry.db"):
        self.db_path = safe_db_path(db_path)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self.lock, self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS registry_stations (
                    id TEXT PRIMARY KEY,
                    location TEXT NOT NULL,
                    credential TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    status TEXT NOT NULL
                )
            """)

    def register_station(self, ws_id: str, location: str, presented_credential: str = "",
                         admin: bool = False) -> Dict[str, Any]:
        """Da de alta una estación nueva o renueva la credencial de una ya registrada.
        - Estación nueva: se crea y se emite una credencial.
        - Estación existente: solo se renueva si se acredita con su credencial actual
          (se emite una nueva y la anterior deja de ser válida). Sin ella se rechaza
          sin modificar nada ("forbidden"), de modo que conocer el ID no basta.
        - admin=True (consola local del Registry): devuelve la credencial existente sin cambios."""
        clean_id = ws_id.strip().upper()
        now = datetime.now().isoformat()
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM registry_stations WHERE id = ?", (clean_id,)
            ).fetchone()

            if row:
                if admin:
                    return {"id": clean_id, "location": row["location"], "credential": row["credential"],
                            "created": False, "renewed": False, "registered_at": row["registered_at"]}
                if not presented_credential or not hmac.compare_digest(
                        row["credential"].encode("utf-8"), presented_credential.strip().encode("utf-8")):
                    return {"id": clean_id, "forbidden": True}
                credential = generate_credential(prefix=f"CRED-{clean_id}")
                self.conn.execute(
                    "UPDATE registry_stations SET location = ?, credential = ?, status = 'ACTIVE' WHERE id = ?",
                    (location, credential, clean_id)
                )
                return {
                    "id": clean_id,
                    "location": location,
                    "credential": credential,
                    "created": False,
                    "renewed": True,
                    "registered_at": row["registered_at"]
                }
            else:
                credential = generate_credential(prefix=f"CRED-{clean_id}")
                self.conn.execute(
                    "INSERT INTO registry_stations (id, location, credential, registered_at, status) "
                    "VALUES (?, ?, ?, ?, 'ACTIVE')",
                    (clean_id, location, credential, now)
                )
                return {
                    "id": clean_id,
                    "location": location,
                    "credential": credential,
                    "created": True,
                    "renewed": False,
                    "registered_at": now
                }

    def unregister_station(self, ws_id: str, presented_credential: str = "", admin: bool = False) -> str:
        """Da de baja una estación eliminando su registro.
        Por REST exige la credencial actual de la estación; desde la consola local (admin=True) no.
        Devuelve 'deleted', 'not_found' o 'forbidden'."""
        clean_id = ws_id.strip().upper()
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT credential FROM registry_stations WHERE id = ?", (clean_id,)
            ).fetchone()
            if not row:
                return "not_found"
            if not admin and (not presented_credential or not hmac.compare_digest(
                    row["credential"].encode("utf-8"), presented_credential.strip().encode("utf-8"))):
                return "forbidden"
            self.conn.execute("DELETE FROM registry_stations WHERE id = ?", (clean_id,))
            return "deleted"

    def get_station(self, ws_id: str, include_credential: bool = False) -> Optional[Dict[str, Any]]:
        clean_id = ws_id.strip().upper()
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM registry_stations WHERE id = ?", (clean_id,)
            ).fetchone()
            if row:
                data = dict(row)
                if not include_credential:
                    data.pop("credential", None)
                return data
        return None

    def list_stations(self, include_credential: bool = False) -> list:
        with self.lock:
            rows = self.conn.execute(
                "SELECT id, location, credential, registered_at, status FROM registry_stations ORDER BY id"
            ).fetchall()
            result = [dict(r) for r in rows]
            if not include_credential:
                for r in result:
                    r.pop("credential", None)
            return result

    def verify_credential(self, ws_id: str, credential: str) -> bool:
        clean_id = ws_id.strip().upper()
        clean_cred = credential.strip()
        with self.lock:
            row = self.conn.execute(
                "SELECT credential, status FROM registry_stations WHERE id = ?", (clean_id,)
            ).fetchone()
            if row and row["status"] == "ACTIVE" and clean_cred:
                return hmac.compare_digest(row["credential"].encode("utf-8"), clean_cred.encode("utf-8"))
        return False


class RegistryRequestHandler(http.server.BaseHTTPRequestHandler):
    """Manejador de la API REST HTTPS para el registro seguro."""

    server_instance = None  # Se asigna en el constructor de WMRegistry

    def log_message(self, format, *args):
        # Silenciamos el log por defecto para no saturar la consola interactiva
        pass

    def _send_json(self, status_code: int, data: Any):
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()
        self.wfile.write(payload)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        reg = self.server_instance
        path = self.path.split("?")[0].rstrip("/")

        if path in ("/api/stations", "/stations"):
            stations = reg.db.list_stations()
            self._send_json(200, {"status": "ok", "count": len(stations), "stations": stations})
        elif path.startswith("/api/stations/") or path.startswith("/stations/"):
            ws_id = path.split("/")[-1].upper()
            st = reg.db.get_station(ws_id)
            if st:
                self._send_json(200, {"status": "ok", "station": st})
            else:
                self._send_json(404, {"status": "error", "message": f"Estación '{ws_id}' no encontrada"})
        elif path in ("", "/"):
            self._send_json(200, {
                "service": "WM_Registry",
                "version": "2.0 (Release 2)",
                "security": "HTTPS / TLS",
                "endpoints": [
                    "POST   /api/stations     - Alta (id, location) o renovación (id, location, credential actual)",
                    "PUT    /api/stations/:id - Igual que POST (alta o renovación)",
                    "GET    /api/stations     - Listar estaciones registradas (sin credenciales)",
                    "GET    /api/stations/:id - Consultar una estación",
                    "DELETE /api/stations/:id - Baja de una estación (cuerpo: credential actual)",
                    "POST   /api/verify       - Verificar credencial (id, credential)"
                ]
            })
        else:
            self._send_json(404, {"status": "error", "message": "Ruta no encontrada"})

    MAX_BODY = 64 * 1024

    def _read_json_body(self) -> Optional[Dict[str, Any]]:
        """Lee y decodifica el cuerpo JSON. Devuelve None (y responde 400/413) si es inválido."""
        try:
            content_len = int(self.headers.get("Content-Length", 0) or 0)
        except ValueError:
            self._send_json(400, {"status": "error", "message": "Cabecera Content-Length inválida"})
            return None
        if content_len < 0 or content_len > self.MAX_BODY:
            self._send_json(413, {"status": "error", "message": "Cuerpo de petición demasiado grande o inválido"})
            return None
        body = self.rfile.read(content_len).decode("utf-8", errors="replace") if content_len else ""
        try:
            data = json.loads(body) if body else {}
        except Exception:
            self._send_json(400, {"status": "error", "message": "Cuerpo de petición no es JSON válido"})
            return None
        if not isinstance(data, dict):
            self._send_json(400, {"status": "error", "message": "El cuerpo debe ser un objeto JSON"})
            return None
        return data

    def _client_ip(self) -> str:
        return self.client_address[0] if self.client_address else "desconocida"

    def _handle_register(self, data: Dict[str, Any]):
        """Alta de una estación nueva o renovación de credencial (requiere la credencial actual)."""
        reg = self.server_instance
        ws_id = str(data.get("id") or data.get("ws_id") or "").strip().upper()
        location = str(data.get("location") or "Parque Municipal").strip()
        presented = str(data.get("credential") or "").strip()

        if not ws_id:
            self._send_json(400, {"status": "error", "message": "Falta el campo obligatorio 'id'"})
            return
        if not STATION_ID_RE.fullmatch(ws_id):
            self._send_json(400, {
                "status": "error",
                "message": f"Formato de ID inválido: '{ws_id}'. Debe ser WS- seguido de dígitos (ej: WS-01)"
            })
            return

        result = reg.db.register_station(ws_id, location, presented_credential=presented)
        client_ip = self._client_ip()
        if result.get("forbidden"):
            reg.log(f"Alta/renovación RECHAZADA para {ws_id} desde {client_ip}: ya registrada y sin credencial actual válida.")
            self._send_json(403, {
                "status": "error",
                "message": (f"La estación {ws_id} ya está registrada. Para renovar su credencial debe presentar "
                            f"la credencial actual. Si se ha perdido, solicite la baja administrativa en WM_Registry.")
            })
            return

        if result["created"]:
            reg.log(f"Alta procesada para estación {ws_id} ({location}) desde {client_ip}. Credencial emitida.")
            message = f"Estación {ws_id} dada de alta con éxito en el Registro"
        else:
            reg.log(f"Credencial RENOVADA para estación {ws_id} ({location}) desde {client_ip}.")
            message = f"Credencial de la estación {ws_id} renovada. La anterior deja de ser válida"
        self._send_json(201 if result["created"] else 200, {"status": "ok", "message": message, "data": result})

    def do_POST(self):
        reg = self.server_instance
        path = self.path.split("?")[0].rstrip("/")

        data = self._read_json_body()
        if data is None:
            return

        # Alta de estación: POST /api/stations
        if path in ("/api/stations", "/stations", "/api/register"):
            self._handle_register(data)

        # Verificación de credencial: POST /api/verify
        elif path in ("/api/verify", "/verify"):
            ws_id = str(data.get("id") or "").strip().upper()
            credential = str(data.get("credential") or "").strip()

            if not ws_id or not credential:
                self._send_json(400, {"status": "error", "message": "Campos 'id' y 'credential' son obligatorios"})
                return

            is_valid = reg.db.verify_credential(ws_id, credential)
            st = reg.db.get_station(ws_id) if is_valid else None
            self._send_json(200, {
                "status": "ok",
                "valid": is_valid,
                "ws_id": ws_id,
                "location": st["location"] if st else None
            })
        else:
            self._send_json(404, {"status": "error", "message": "Endpoint no encontrado"})

    def do_PUT(self):
        # PUT /api/stations/{id}: alta o renovación (mismas reglas que POST /api/stations)
        path = self.path.split("?")[0].rstrip("/")
        if path.startswith("/api/stations/") or path.startswith("/stations/"):
            data = self._read_json_body()
            if data is None:
                return
            data["id"] = path.split("/")[-1].upper()
            self._handle_register(data)
        else:
            self._send_json(404, {"status": "error", "message": "Endpoint no encontrado"})

    def do_DELETE(self):
        # DELETE /api/stations/{id} con cuerpo {"credential": "..."}: baja solicitada por la propia estación
        reg = self.server_instance
        path = self.path.split("?")[0].rstrip("/")
        if path.startswith("/api/stations/") or path.startswith("/stations/"):
            data = self._read_json_body()
            if data is None:
                return
            ws_id = path.split("/")[-1].upper()
            outcome = reg.db.unregister_station(ws_id, presented_credential=str(data.get("credential") or ""))
            client_ip = self._client_ip()
            if outcome == "deleted":
                reg.log(f"Baja procesada para la estación {ws_id} desde {client_ip}")
                self._send_json(200, {"status": "ok", "message": f"Estación {ws_id} dada de baja del Registro"})
            elif outcome == "forbidden":
                reg.log(f"Baja RECHAZADA para {ws_id} desde {client_ip}: credencial ausente o inválida.")
                self._send_json(403, {"status": "error",
                                      "message": f"Baja denegada: se requiere la credencial actual de {ws_id}"})
            else:
                self._send_json(404, {"status": "error", "message": f"Estación {ws_id} no estaba registrada"})
        else:
            self._send_json(404, {"status": "error", "message": "Endpoint no encontrado"})


class WMRegistry:
    """Módulo principal WM_Registry."""

    DEFAULT_SAN = ("localhost", "registry", "127.0.0.1")

    def __init__(self, port: int = 5500, db_path: str = "registry.db",
                 cert_file: Optional[str] = None, key_file: Optional[str] = None,
                 san: Optional[list] = None):
        self.port = port
        # Nombres/IP que cubrirá el certificado (además de los locales por defecto).
        # Deben incluir el host/IP con el que los clientes contactan con el Registry.
        extra = [x.strip() for x in (san or []) if x and x.strip()]
        self.san_names = list(dict.fromkeys(list(self.DEFAULT_SAN) + extra))
        self.db = RegistryDatabase(db_path)
        self.running = True

        # Configuración SSL/TLS
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.cert_file = cert_file or os.path.join(base_dir, "certs", "registry_cert.pem")
        self.key_file = key_file or os.path.join(base_dir, "certs", "registry_key.pem")

        RegistryRequestHandler.server_instance = self

    def log(self, msg: str):
        print(f"[{time.strftime('%H:%M:%S')}] [WM_REGISTRY] {msg}", flush=True)

    def _san_entries(self) -> list:
        from cryptography import x509
        entries = []
        for name in self.san_names:
            try:
                entries.append(x509.IPAddress(ipaddress.ip_address(name)))
            except ValueError:
                entries.append(x509.DNSName(name))
        return entries

    def _cert_covers_san(self) -> bool:
        """Comprueba que el certificado actual incluye todos los nombres/IP configurados."""
        try:
            from cryptography import x509
            with open(self.cert_file, "rb") as f:
                cert = x509.load_pem_x509_certificate(f.read())
            ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
            present = {str(v) for v in ext.get_values_for_type(x509.DNSName)}
            present |= {str(v) for v in ext.get_values_for_type(x509.IPAddress)}
            return all(str(ipaddress.ip_address(n)) in present if self._is_ip(n) else n in present
                       for n in self.san_names)
        except Exception:
            return False

    @staticmethod
    def _is_ip(name: str) -> bool:
        try:
            ipaddress.ip_address(name)
            return True
        except ValueError:
            return False

    def _restrict_key_permissions(self):
        """Deja la clave privada legible y modificable solo por su propietario (0600)."""
        try:
            if os.stat(self.key_file).st_mode & 0o777 != 0o600:
                os.chmod(self.key_file, 0o600)
                self.log(f"Permisos de la clave privada corregidos a 0600 ({self.key_file}).")
        except OSError as exc:
            self.log(f"Aviso: no se pudieron restringir los permisos de la clave privada ({exc}).")

    def _ensure_tls_certificates(self):
        """Garantiza que existan un certificado X.509 y su clave privada RSA correspondiente.
        Si la clave privada no existe (p. ej. ignorada en Git por seguridad) o no coincide con el
        certificado, genera automáticamente un par autofirmado local con permisos restrictivos."""
        cert_dir = os.path.dirname(os.path.abspath(self.cert_file))
        os.makedirs(cert_dir, exist_ok=True)

        need_gen = not (os.path.isfile(self.cert_file) and os.path.isfile(self.key_file))
        if not need_gen:
            try:
                test_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                test_ctx.load_cert_chain(self.cert_file, self.key_file)
            except Exception:
                need_gen = True
        if not need_gen and not self._cert_covers_san():
            self.log("El certificado existente no cubre todos los nombres/IP solicitados (--san). Se regenera.")
            need_gen = True

        if not need_gen:
            # Se reutiliza el par existente: la clave privada debe quedar igualmente con permisos 0600
            self._restrict_key_permissions()
            return

        self.log("Generando par de claves RSA-2048 y certificado X.509 autofirmado para HTTPS...")
        try:
            from datetime import timezone, timedelta
            from cryptography import x509
            from cryptography.x509.oid import NameOID
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COUNTRY_NAME, "ES"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "WaterManagement SD"),
                x509.NameAttribute(NameOID.COMMON_NAME, "WM_Registry"),
            ])
            now_utc = datetime.now(timezone.utc)
            cert = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(issuer)
                .public_key(private_key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now_utc - timedelta(days=1))
                .not_valid_after(now_utc + timedelta(days=3650))
                .add_extension(
                    x509.SubjectAlternativeName(self._san_entries()),
                    critical=False,
                )
                .sign(private_key, hashes.SHA256())
            )

            key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
            cert_bytes = cert.public_bytes(serialization.Encoding.PEM)

            with open(self.key_file, "wb") as kf:
                kf.write(key_bytes)
            try:
                os.chmod(self.key_file, 0o600)
            except Exception:
                pass

            with open(self.cert_file, "wb") as cf:
                cf.write(cert_bytes)

            self.log(f"Certificado TLS y clave privada generados correctamente en {cert_dir} "
                     f"(válido para: {', '.join(self.san_names)})")
            self.log(f"Copie {self.cert_file} a los clientes (WM_WS_M y WM_Central) para que validen el Registry.")
        except Exception as gen_err:
            raise RuntimeError(
                f"No se pudieron generar los certificados TLS para WM_Registry: {gen_err}"
            ) from gen_err

    def run_server(self):
        self._ensure_tls_certificates()

        server_address = ("0.0.0.0", self.port)
        httpd = http.server.ThreadingHTTPServer(server_address, RegistryRequestHandler)

        # Envolvemos el socket con TLS/HTTPS obligatorio (sin fallback a HTTP sin cifrar)
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_ctx.load_cert_chain(self.cert_file, self.key_file)
            httpd.socket = ssl_ctx.wrap_socket(httpd.socket, server_side=True)
        except Exception as e:
            httpd.server_close()
            raise RuntimeError(
                f"ERROR CRÍTICO DE SEGURIDAD: No se pudo iniciar WM_Registry en HTTPS ({e}). "
                f"Se aborta el inicio para evitar comunicación HTTP sin cifrar."
            ) from e

        self.log(f"Servidor de Registro iniciado en HTTPS://0.0.0.0:{self.port}/")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            httpd.server_close()

    def run_cli(self):
        """Menú de consola interactivo para gestionar el Registro."""
        time.sleep(0.5)
        print("\n" + "=" * 60)
        print("  WM_REGISTRY - Panel de Control del Registro Seguro")
        print("  Comandos disponibles:")
        print("    l               -> Listar estaciones registradas")
        print("    r <ID> [ubic]   -> Dar de alta una estación (ej: r WS-01 Madrid)")
        print("    d <ID>          -> Dar de baja una estación (ej: d WS-01)")
        print("    c <ID>          -> Consultar credencial de una estación")
        print("    q               -> Salir")
        print("=" * 60 + "\n")

        while self.running:
            try:
                line = sys.stdin.readline()
                if not line:
                    break
                parts = line.strip().split()
                if not parts:
                    continue
                cmd = parts[0].lower()

                if cmd == "l":
                    stations = self.db.list_stations(include_credential=True)
                    print(f"\n--- Estaciones registradas ({len(stations)}) ---")
                    for s in stations:
                        print(f"  • ID: {s['id']:<8} | Ubicación: {s['location']:<20} | Credencial: {s['credential']} | Estado: {s['status']}")
                    print()
                elif cmd == "r" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    loc = " ".join(parts[2:]) if len(parts) > 2 else "Parque Central"
                    res = self.db.register_station(ws_id, loc, admin=True)
                    print(f"[OK] Estación {ws_id} registrada. Credencial: {res['credential']}")
                elif cmd == "d" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    if self.db.unregister_station(ws_id, admin=True) == "deleted":
                        print(f"[OK] Estación {ws_id} dada de baja del Registro.")
                    else:
                        print(f"[ERROR] Estación {ws_id} no encontrada.")
                elif cmd == "c" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    st = self.db.get_station(ws_id, include_credential=True)
                    if st:
                        print(f"Estación: {st['id']} | Ubicación: {st['location']} | Credencial: {st['credential']}")
                    else:
                        print(f"Estación {ws_id} no encontrada.")
                elif cmd == "q":
                    print("Cerrando WM_Registry...")
                    os._exit(0)
                else:
                    print("Comando no reconocido. Use 'l', 'r <ID> [ubic]', 'd <ID>', 'c <ID>', 'q'.")
            except Exception as e:
                print(f"Error procesando comando: {e}")


def main():
    parser = argparse.ArgumentParser(description="WM_Registry: Servicio de Registro Seguro (HTTPS API REST)")
    parser.add_argument("port", nargs="?", type=int, default=5500, help="Puerto HTTPS (por defecto 5500)")
    parser.add_argument("--db", default="registry.db", help="Ruta de la base de datos SQLite (por defecto registry.db)")
    parser.add_argument("--cert", default=None, help="Ruta del certificado SSL PEM")
    parser.add_argument("--key", default=None, help="Ruta de la clave privada SSL PEM")
    parser.add_argument("--san", default=os.getenv("WM_REGISTRY_SAN", ""),
                        help="Nombres DNS/IP adicionales del certificado separados por comas "
                             "(ej: 192.168.1.20,registry.miapp.com). También: $WM_REGISTRY_SAN")
    args = parser.parse_args()

    registry = WMRegistry(port=args.port, db_path=args.db, cert_file=args.cert, key_file=args.key,
                          san=args.san.split(","))

    # Iniciar servidor HTTPS en un hilo
    server_thread = threading.Thread(target=registry.run_server, daemon=True)
    server_thread.start()

    # Si la consola es interactiva, abrimos el menú CLI; si no, mantenemos vivo el proceso
    if sys.stdin.isatty():
        registry.run_cli()
    else:
        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()
