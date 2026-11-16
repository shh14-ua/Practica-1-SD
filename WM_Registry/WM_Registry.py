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
import http.server
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

    def register_station(self, ws_id: str, location: str) -> Dict[str, Any]:
        """Da de alta una estación o actualiza sus credenciales."""
        clean_id = ws_id.strip().upper()
        now = datetime.now().isoformat()
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM registry_stations WHERE id = ?", (clean_id,)
            ).fetchone()

            if row:
                credential = row["credential"]
                self.conn.execute(
                    "UPDATE registry_stations SET location = ?, status = 'ACTIVE' WHERE id = ?",
                    (location, clean_id)
                )
                return {
                    "id": clean_id,
                    "location": location,
                    "credential": credential,
                    "created": False,
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
                    "registered_at": now
                }

    def unregister_station(self, ws_id: str) -> bool:
        """Da de baja una estación eliminando su registro."""
        clean_id = ws_id.strip().upper()
        with self.lock, self.conn:
            cur = self.conn.execute("DELETE FROM registry_stations WHERE id = ?", (clean_id,))
            return cur.rowcount > 0

    def get_station(self, ws_id: str) -> Optional[Dict[str, Any]]:
        clean_id = ws_id.strip().upper()
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM registry_stations WHERE id = ?", (clean_id,)
            ).fetchone()
            if row:
                return dict(row)
        return None

    def list_stations(self) -> list:
        with self.lock:
            rows = self.conn.execute(
                "SELECT id, location, credential, registered_at, status FROM registry_stations ORDER BY id"
            ).fetchall()
            return [dict(r) for r in rows]

    def verify_credential(self, ws_id: str, credential: str) -> bool:
        clean_id = ws_id.strip().upper()
        clean_cred = credential.strip()
        with self.lock:
            row = self.conn.execute(
                "SELECT credential, status FROM registry_stations WHERE id = ?", (clean_id,)
            ).fetchone()
            if row and row["status"] == "ACTIVE":
                return row["credential"] == clean_cred
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
                    "POST   /api/stations     - Dar de alta una estación (recibe id, location)",
                    "GET    /api/stations     - Listar estaciones registradas",
                    "GET    /api/stations/:id - Consultar una estación",
                    "DELETE /api/stations/:id - Dar de baja una estación",
                    "POST   /api/verify       - Verificar credencial (id, credential)"
                ]
            })
        else:
            self._send_json(404, {"status": "error", "message": "Ruta no encontrada"})

    def do_POST(self):
        reg = self.server_instance
        path = self.path.split("?")[0].rstrip("/")

        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len).decode("utf-8", errors="replace")
        try:
            data = json.loads(body) if body else {}
        except Exception:
            self._send_json(400, {"status": "error", "message": "Cuerpo de petición no es JSON válido"})
            return

        # Alta de estación: POST /api/stations
        if path in ("/api/stations", "/stations", "/api/register"):
            ws_id = str(data.get("id") or data.get("ws_id") or "").strip().upper()
            location = str(data.get("location") or "Parque Municipal").strip()

            if not ws_id:
                self._send_json(400, {"status": "error", "message": "Falta el campo obligatorio 'id'"})
                return

            if not STATION_ID_RE.fullmatch(ws_id):
                self._send_json(400, {
                    "status": "error",
                    "message": f"Formato de ID inválido: '{ws_id}'. Debe ser WS- seguido de dígitos (ej: WS-01)"
                })
                return

            result = reg.db.register_station(ws_id, location)
            client_ip = self.client_address[0] if self.client_address else "desconocida"
            reg.log(f"Alta procesada para estación {ws_id} ({location}) desde {client_ip}. Credencial asignada.")
            self._send_json(201 if result["created"] else 200, {
                "status": "ok",
                "message": f"Estación {ws_id} dada de alta con éxito en el Registro",
                "data": result
            })

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
        # PUT /api/stations/{id} también permite alta o actualización
        path = self.path.split("?")[0].rstrip("/")
        if path.startswith("/api/stations/") or path.startswith("/stations/"):
            ws_id = path.split("/")[-1].upper()
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len).decode("utf-8", errors="replace")
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {}
            data["id"] = ws_id
            self.path = "/api/stations"
            self.rfile = sys.stdin  # dummy
            self.do_POST()
        else:
            self._send_json(404, {"status": "error", "message": "Endpoint no encontrado"})

    def do_DELETE(self):
        reg = self.server_instance
        path = self.path.split("?")[0].rstrip("/")
        if path.startswith("/api/stations/") or path.startswith("/stations/"):
            ws_id = path.split("/")[-1].upper()
            success = reg.db.unregister_station(ws_id)
            if success:
                reg.log(f"Baja procesada para la estación {ws_id}")
                self._send_json(200, {"status": "ok", "message": f"Estación {ws_id} dada de baja del Registro"})
            else:
                self._send_json(404, {"status": "error", "message": f"Estación {ws_id} no estaba registrada"})
        else:
            self._send_json(404, {"status": "error", "message": "Endpoint no encontrado"})


class WMRegistry:
    """Módulo principal WM_Registry."""

    def __init__(self, port: int = 5500, db_path: str = "registry.db",
                 cert_file: Optional[str] = None, key_file: Optional[str] = None):
        self.port = port
        self.db = RegistryDatabase(db_path)
        self.running = True

        # Configuración SSL/TLS
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.cert_file = cert_file or os.path.join(base_dir, "certs", "registry_cert.pem")
        self.key_file = key_file or os.path.join(base_dir, "certs", "registry_key.pem")

        RegistryRequestHandler.server_instance = self

    def log(self, msg: str):
        print(f"[{time.strftime('%H:%M:%S')}] [WM_REGISTRY] {msg}", flush=True)

    def run_server(self):
        server_address = ("0.0.0.0", self.port)
        httpd = http.server.ThreadingHTTPServer(server_address, RegistryRequestHandler)

        # Envolvemos el socket con TLS/HTTPS
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_ctx.load_cert_chain(self.cert_file, self.key_file)
            httpd.socket = ssl_ctx.wrap_socket(httpd.socket, server_side=True)
            proto = "HTTPS"
        except Exception as e:
            self.log(f"AVISO: No se pudo iniciar en HTTPS ({e}). Iniciando en HTTP estándar...")
            proto = "HTTP"

        self.log(f"Servidor de Registro iniciado en {proto}://0.0.0.0:{self.port}/")
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
                    stations = self.db.list_stations()
                    print(f"\n--- Estaciones registradas ({len(stations)}) ---")
                    for s in stations:
                        print(f"  • ID: {s['id']:<8} | Ubicación: {s['location']:<20} | Credencial: {s['credential']} | Estado: {s['status']}")
                    print()
                elif cmd == "r" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    loc = " ".join(parts[2:]) if len(parts) > 2 else "Parque Central"
                    res = self.db.register_station(ws_id, loc)
                    print(f"[OK] Estación {ws_id} registrada. Credencial: {res['credential']}")
                elif cmd == "d" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    if self.db.unregister_station(ws_id):
                        print(f"[OK] Estación {ws_id} dada de baja del Registro.")
                    else:
                        print(f"[ERROR] Estación {ws_id} no encontrada.")
                elif cmd == "c" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    st = self.db.get_station(ws_id)
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
    args = parser.parse_args()

    registry = WMRegistry(port=args.port, db_path=args.db, cert_file=args.cert, key_file=args.key)

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
