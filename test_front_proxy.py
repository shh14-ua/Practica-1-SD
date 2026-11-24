#!/usr/bin/env python3
"""
Pruebas del Front (bloque 6, Release 2): servidor Node.js real (front/server.js) como proxy de
API_Central real (HTTP) y de servidores auxiliares que registran lo que reciben.

Tipo de prueba: REAL (procesos Node.js y servidores HTTP/HTTPS reales). Kafka SIMULADO en Central.
Se omiten si Node.js no está instalado.

Comprueba:
  1. El Front sirve la página y reenvía las consultas GET a API_Central.
  2. Usuarios públicos no pueden usar las notificaciones climáticas ni otras escrituras a través
     del Front; las acciones de control exigen el token de administración aunque el Front esté en
     la misma máquina que Central.
  3. Cabeceras reenviadas: X-Forwarded-For con la IP real (no la que diga el cliente), nunca
     X-Weather-Token, y X-Admin-Token solo para /api/action.
  4. 502 si API_Central no está disponible; verificación del certificado si API_Central usa HTTPS.
  5. El JavaScript de las páginas es válido y usa campos que la API realmente devuelve.
"""

import http.server
import json
import os
import re
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT_DIR, os.path.join(ROOT_DIR, "WM_Central")):
    if p not in sys.path:
        sys.path.insert(0, p)

import WM_Central
from wm_crypto import ensure_tls_certificate, generate_symmetric_key
from common import STATUS_AVAILABLE

NODE = shutil.which("node")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(method, url, body=None, headers=None, raw=None, context=None):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=5, context=context) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as he:
        return he.code, he.read().decode("utf-8")


class CaptureHandler(http.server.BaseHTTPRequestHandler):
    """Backend auxiliar que responde 200 y guarda las cabeceras recibidas."""
    seen = []

    def log_message(self, *a):
        pass

    def _reply(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length:
            self.rfile.read(length)
        CaptureHandler.seen.append((self.command, self.path, {k.lower(): v for k, v in self.headers.items()}))
        body = b'{"status": "ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = do_DELETE = _reply


@unittest.skipUnless(NODE, "Node.js no está instalado")
class FrontProxyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_front_")
        self.procs, self.servers = [], []
        self.admin_token = secrets.token_urlsafe(24)
        self.weather_token = secrets.token_urlsafe(24)
        self.env = patch.dict(os.environ, {"WM_ADMIN_TOKEN": self.admin_token,
                                           "WM_WEATHER_TOKEN": self.weather_token})
        self.env.start()
        self.patches = [
            patch.object(WM_Central, "ensure_topics", lambda *a, **k: None),
            patch.object(WM_Central, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_Central, "send_kafka", lambda *a, **k: True),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for proc in self.procs:
            proc.terminate()
            proc.wait(5)
        for srv in self.servers:
            srv.shutdown()
            srv.server_close()
        for p in reversed(self.patches):
            p.stop()
        self.env.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _wait(self, url, context=None):
        deadline = time.time() + 8
        while time.time() < deadline:
            try:
                urllib.request.urlopen(url, timeout=1, context=context)
                return
            except urllib.error.HTTPError:
                return
            except Exception:
                time.sleep(0.1)
        self.fail(f"No responde {url}")

    def _central(self):
        port = free_port()
        c = WM_Central.WMCentral(socket_port=0, kafka_bootstrap="fake:9092", web_port=port,
                                 db_path=os.path.join(self.tmp, "central.db"))
        threading.Thread(target=c.run_web_server, daemon=True).start()
        self.servers.append(type("S", (), {"shutdown": lambda s: c._web_server.shutdown(),
                                           "server_close": lambda s: c._web_server.server_close()})())
        self._wait(f"http://127.0.0.1:{port}/api/status")
        return c, f"http://127.0.0.1:{port}"

    def _front(self, central_url, extra_env=None):
        port = free_port()
        env = {k: v for k, v in os.environ.items() if k not in ("WM_ADMIN_TOKEN", "WM_WEATHER_TOKEN")}
        env.update(extra_env or {})
        proc = subprocess.Popen([NODE, os.path.join(ROOT_DIR, "front", "server.js"), str(port), central_url],
                                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(proc)
        base = f"http://127.0.0.1:{port}"
        self._wait(base + "/config.js")
        return base

    def _capture_backend(self, tls_cert=None, tls_key=None):
        CaptureHandler.seen = []
        port = free_port()
        srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), CaptureHandler)
        scheme = "http"
        if tls_cert:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(tls_cert, tls_key)
            srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
            scheme = "https"
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.servers.append(srv)
        return f"{scheme}://127.0.0.1:{port}"

    # ------------------------------------------------------------------ 1-2
    def test_01_static_queries_and_public_restrictions(self):
        central, central_url = self._central()
        front = self._front(central_url)
        code, page = call("GET", front + "/")
        self.assertEqual(code, 200)
        self.assertIn("<html", page.lower())
        code, cfg = call("GET", front + "/config.js")
        self.assertNotIn(self.admin_token, cfg)
        self.assertNotIn(self.weather_token, cfg)
        code, body = call("GET", front + "/..%2F..%2FWM_Central%2FWM_Central.py")
        self.assertNotIn("class WMCentral", body, "Protección frente a rutas fuera de public/")

        for path in ("/api/status", "/api/stations", "/api/stations/WS-01", "/api/operators", "/api/waterings"):
            code, body = call("GET", front + path)
            self.assertEqual(code, 200, path)
            self.assertEqual(json.loads(body), json.loads(call("GET", central_url + path)[1]) if path != "/api/status"
                             else json.loads(body), path)
        self.assertEqual(call("GET", front + "/api/stations/WS-99")[0], 404)

        # Notificaciones climáticas: nunca a través del Front (aunque se presente el secreto de WM_W)
        alert = {"ws_id": "WS-01", "city": "Oslo", "temp": -5, "condition": "Snow", "alert": True, "reason": "x"}
        wt = {"X-Weather-Token": self.weather_token}
        for method, path in (("POST", "/api/weather/alert"), ("PUT", "/api/weather/WS-01"),
                             ("DELETE", "/api/weather/WS-01/alert"), ("PUT", "/api/action")):
            self.assertEqual(call(method, front + path, alert, wt)[0], 403, (method, path))
        self.assertNotIn("WS-01", central.weather_alerts)

    def test_02_actions_need_admin_token_even_with_front_on_central_host(self):
        central, central_url = self._central()
        with central.lock:
            central.station_keys["WS-01"] = generate_symmetric_key()
            central.station_sockets["WS-01"] = object()
            central.stations["WS-01"].update(status=STATUS_AVAILABLE, engine_ok=True)
        front = self._front(central_url)
        body = {"action": "revoke_keys", "ws": "WS-01"}
        for headers in ({}, {"X-Forwarded-For": ""}, {"X-Forwarded-For": "127.0.0.1"}, {"X-Admin-Token": "mal"}):
            self.assertEqual(call("POST", front + "/api/action", body, headers)[0], 403, headers)
        self.assertFalse(central.stations["WS-01"].get("keys_revoked"), "Sin token no hay efecto")
        code, res = call("POST", front + "/api/action", body, {"X-Admin-Token": self.admin_token})
        self.assertEqual(code, 200, res)
        self.assertTrue(central.stations["WS-01"].get("keys_revoked"))
        self.assertEqual(call("POST", front + "/api/action", {"action": "x", "ws": "WS-01"},
                              {"X-Admin-Token": self.admin_token})[0], 400)

    # ------------------------------------------------------------------ 3
    def test_03_forwarded_headers(self):
        backend = self._capture_backend()
        front = self._front(backend)
        call("GET", front + "/api/status", headers={"X-Forwarded-For": "127.0.0.1", "X-Admin-Token": "t",
                                                     "X-Weather-Token": "w"})
        call("POST", front + "/api/action", {"action": "block", "ws": "WS-01"},
             {"X-Admin-Token": "t", "X-Weather-Token": "w", "X-Forwarded-For": "127.0.0.1"})
        (m1, p1, h1), (m2, p2, h2) = CaptureHandler.seen
        self.assertEqual((m1, p1, m2, p2), ("GET", "/api/status", "POST", "/api/action"))
        for h in (h1, h2):
            self.assertIn(h["x-forwarded-for"], ("127.0.0.1", "::ffff:127.0.0.1", "::1"))
            self.assertNotIn(",", h["x-forwarded-for"], "La IP la pone el proxy, no se concatena la del cliente")
            self.assertNotIn("x-weather-token", h)
        self.assertNotIn("x-admin-token", h1, "El token de administración solo se reenvía a /api/action")
        self.assertEqual(h2["x-admin-token"], "t")

    # ------------------------------------------------------------------ 4
    def test_04_bad_gateway_and_https_verification(self):
        front = self._front(f"http://127.0.0.1:{free_port()}")
        self.assertEqual(call("GET", front + "/api/status")[0], 502)

        cert, key = os.path.join(self.tmp, "api_cert.pem"), os.path.join(self.tmp, "api_key.pem")
        ensure_tls_certificate(cert, key, "API_Central", ["127.0.0.1", "localhost"])
        backend = self._capture_backend(cert, key)
        front_no_ca = self._front(backend)
        self.assertEqual(call("GET", front_no_ca + "/api/status")[0], 502,
                         "Certificado autofirmado no confiable: el proxy debe rechazarlo")
        front_ca = self._front(backend, {"CENTRAL_API_CA": cert})
        self.assertEqual(call("GET", front_ca + "/api/status")[0], 200, "Con la CA configurada se acepta")

    # ------------------------------------------------------------------ 5
    def test_05_pages_javascript_valid_and_fields_exist_in_api(self):
        central, central_url = self._central()
        station = json.loads(call("GET", central_url + "/api/status")[1])["stations"][0]
        for page in (os.path.join(ROOT_DIR, "front", "public", "index.html"),
                     os.path.join(ROOT_DIR, "WM_Central", "dashboard.html")):
            with open(page, encoding="utf-8") as f:
                html = f.read()
            js = "\n".join(re.findall(r"<script>(.*?)</script>", html, re.S))
            path = os.path.join(self.tmp, "page.js")
            with open(path, "w", encoding="utf-8") as f:
                f.write(js)
            self.assertEqual(subprocess.run([NODE, "--check", path], capture_output=True).returncode, 0, page)
            self.assertNotIn(self.admin_token, html)
            self.assertIn("X-Admin-Token", html, "Las acciones envían el token introducido por el usuario")
            self.assertIn("sessionStorage", html)
        with open(os.path.join(ROOT_DIR, "front", "public", "index.html"), encoding="utf-8") as f:
            html = f.read()
        for field in set(re.findall(r"\bs\.([a-z_]+)\b", html)):
            if field in ("id", "length", "map", "filter", "forEach", "some", "split", "slice"):
                continue
            self.assertIn(field, station, f"El Front usa s.{field}, que la API no devuelve")
        self.assertEqual(subprocess.run([NODE, "--check", os.path.join(ROOT_DIR, "front", "server.js")],
                                        capture_output=True).returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
