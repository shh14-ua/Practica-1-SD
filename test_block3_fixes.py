#!/usr/bin/env python3
"""
Pruebas de las correcciones de cierre del bloque 3 (Release 2).

  1. Enlace Monitor <-> Engine solo local (REAL: sockets TCP reales): un equipo remoto no puede
     conectarse al puerto del Engine ni recibir la clave simétrica; el Engine local sí.
  2. .dockerignore (SIMULACIÓN de las reglas de Docker, sin Docker): calcula qué ficheros del árbol
     de trabajo entrarían en la imagen con los COPY del Dockerfile y comprueba que no entran claves,
     credenciales, entornos virtuales, cachés ni bases de datos, y sí los certificados públicos.
  3. Certificado del Registry estable (REAL: generación y servidor HTTPS reales en un directorio
     temporal): se genera una vez y se reutiliza en los arranques siguientes; solo se regenera si
     falta, no forma pareja o no cubre los SAN pedidos. El certificado no está versionado en Git.
"""

import hashlib
import os
import re
import socket
import ssl
import stat
import subprocess
import sys
import tempfile
import shutil
import threading
import time
import unittest
import urllib.request

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT_DIR, os.path.join(ROOT_DIR, "WM_WS")):
    if p not in sys.path:
        sys.path.insert(0, p)

import WM_WS_M
from common import client_handshake, recv_msg, send_frame
from WM_Registry.WM_Registry import WMRegistry


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def non_loopback_ip():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("192.0.2.1", 9))  # no envía tráfico: solo elige la interfaz de salida
            ip = s.getsockname()[0]
        return None if ip.startswith("127.") else ip
    except OSError:
        return None


def sha(path) -> str:
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


# ==============================================================================
# 1. ENLACE MONITOR <-> ENGINE SOLO LOCAL
# ==============================================================================
class MonitorEngineLocalLinkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_link_")
        self.port = free_port()
        self.mon = WM_WS_M.WSMonitor(engine_port=self.port, central_addr="127.0.0.1:1", ws_id="WS-41",
                                     cred_file=os.path.join(self.tmp, "WS-41.cred"))
        # Sin Central: solo interesa el enlace con el Engine. Clave simétrica de prueba ya recibida.
        self.mon.ensure_central_connection = lambda manual=False: False
        self.mon.send_to_central = lambda msg: False
        self.mon.symmetric_key = "CLAVE-DE-PRUEBA-" + "x" * 28
        threading.Thread(target=self.mon.run_monitoring_loop, daemon=True).start()
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        time.sleep(1.2)  # deja que el Monitor descarte la conexión de sondeo

    def tearDown(self):
        self.mon.running = False
        time.sleep(1.2)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_01_remote_host_cannot_reach_engine_port(self):
        ip = non_loopback_ip()
        if not ip:
            self.skipTest("La máquina no tiene una interfaz de red distinta de loopback")
        with self.assertRaises(ConnectionRefusedError):
            socket.create_connection((ip, self.port), timeout=3)
        self.assertIsNone(self.mon.engine_sock)

    def test_02_local_engine_still_receives_id_and_key(self):
        s = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        client_handshake(s)
        self.assertEqual(recv_msg(s), "ASSIGN#WS-41")
        send_frame(s, "OK")
        self.assertEqual(recv_msg(s), f"SET_KEY#{self.mon.symmetric_key}")
        send_frame(s, "OK")
        s.close()


# ==============================================================================
# 2. .DOCKERIGNORE (simulación de las reglas de Docker)
# ==============================================================================
def _pattern_to_regex(pattern: str):
    """Traduce un patrón de .dockerignore a expresión regular (semántica de moby/patternmatcher:
    '*' y '?' no cruzan '/', '**' cruza cualquier número de carpetas, patrón anclado a la raíz)."""
    pattern = os.path.normpath(pattern.strip()).lstrip("/")
    out, i = "", 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out += "(?:.*/)?"; i += 3
        elif pattern.startswith("**", i):
            out += ".*"; i += 2
        elif pattern[i] == "*":
            out += "[^/]*"; i += 1
        elif pattern[i] == "?":
            out += "[^/]"; i += 1
        elif pattern[i] == "[":
            j = pattern.index("]", i)
            out += pattern[i:j + 1]; i = j + 1
        else:
            out += re.escape(pattern[i]); i += 1
    return re.compile(out + r"\Z")


def load_dockerignore(path):
    rules = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                rules.append(_pattern_to_regex(line))
    return rules


def is_excluded(rel_path: str, rules) -> bool:
    """Excluido si el fichero o cualquiera de sus carpetas padre coincide con algún patrón."""
    parts = rel_path.split("/")
    prefixes = ["/".join(parts[:k]) for k in range(1, len(parts) + 1)]
    return any(r.match(p) for r in rules for p in prefixes)


def dockerfile_copy_sources(dockerfile):
    sources = []
    with open(dockerfile, encoding="utf-8") as f:
        for line in f:
            parts = line.split()
            if parts and parts[0].upper() == "COPY":
                sources += [s.rstrip("/") for s in parts[1:-1]]
    return sources


def files_copied_into_image(root, rules):
    copied = []
    for src in dockerfile_copy_sources(os.path.join(root, "Dockerfile")):
        full = os.path.join(root, src)
        if os.path.isfile(full):
            candidates = [src]
        else:
            candidates = [os.path.relpath(os.path.join(d, f), root).replace(os.sep, "/")
                          for d, _, fs in os.walk(full) for f in fs]
        copied += [c for c in candidates if not is_excluded(c, rules)]
    return sorted(set(copied))


class DockerIgnoreTests(unittest.TestCase):
    def setUp(self):
        self.rules = load_dockerignore(os.path.join(ROOT_DIR, ".dockerignore"))

    def test_03_sensitive_paths_are_excluded(self):
        excluded = [
            "WM_Registry/certs/registry_key.pem", "WM_Central/certs/central_key.pem", "otro/servidor.key",
            "WM_WS/.credentials/WS-01.cred", "WM_WS/WS-02.cred", ".env", "WM_Central/.env.prod",
            ".venv/bin/python", "venv/lib/x.py", ".git/config", ".claude/settings.local.json",
            "WM_Central/__pycache__/WM_Central.cpython-312.pyc", "common.pyc",
            "data/central.db", "central.db", "WM_Registry/registry.db", "audit.log", "WM_Central/audit.log",
            "front/node_modules/express/index.js", "enunciado/x.pdf:Zone.Identifier",
        ]
        for path in excluded:
            self.assertTrue(is_excluded(path, self.rules), f"Debería excluirse de la imagen: {path}")

    def test_04_required_and_public_files_are_kept(self):
        kept = [
            "requirements.txt", "common.py", "wm_crypto.py", "Dockerfile",
            "WM_Central/WM_Central.py", "WM_Central/dashboard.html", "WM_Central/certs/central_cert.pem",
            "WM_Registry/WM_Registry.py", "WM_Registry/certs/registry_cert.pem",
            "WM_W/WM_W.py", "WM_W/ciudades.json", "WM_WS/WM_WS_M.py", "WM_WS/WM_WS_E.py",
            "WM_FO/WM_FO.py", "WM_FO/operaciones.txt",
        ]
        for path in kept:
            self.assertFalse(is_excluded(path, self.rules), f"No debería excluirse: {path}")

    def test_05_real_working_tree_image_content(self):
        copied = files_copied_into_image(ROOT_DIR, self.rules)
        bad = [f for f in copied if re.search(r"(_key\.pem|\.key|\.cred|\.pyc|\.db|\.log)$|__pycache__|/\.credentials/", f)]
        self.assertEqual(bad, [], f"Ficheros sensibles que entrarían en la imagen: {bad}")
        for needed in ("requirements.txt", "common.py", "wm_crypto.py", "WM_Central/WM_Central.py",
                       "WM_Registry/WM_Registry.py", "WM_W/ciudades.json", "WM_WS/WM_WS_M.py",
                       "WM_WS/WM_WS_E.py", "WM_FO/WM_FO.py"):
            self.assertIn(needed, copied)
        if os.path.isfile(os.path.join(ROOT_DIR, "WM_Registry/certs/registry_cert.pem")):
            self.assertIn("WM_Registry/certs/registry_cert.pem", copied)


# ==============================================================================
# 3. CERTIFICADO DEL REGISTRY ESTABLE
# ==============================================================================
class RegistryCertificateStabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_regcert_")
        self.cert = os.path.join(self.tmp, "registry_cert.pem")
        self.key = os.path.join(self.tmp, "registry_key.pem")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _registry(self, san=None, port=0):
        return WMRegistry(port=port, db_path=os.path.join(self.tmp, "r.db"),
                          cert_file=self.cert, key_file=self.key, san=san)

    def test_06_generated_once_then_reused(self):
        self._registry()._ensure_tls_certificates()
        first = (sha(self.cert), sha(self.key))
        self.assertEqual(stat.S_IMODE(os.stat(self.key).st_mode), 0o600)
        for _ in range(3):
            self._registry()._ensure_tls_certificates()
            self.assertEqual((sha(self.cert), sha(self.key)), first, "No debe cambiar en arranques sucesivos")
        # Con SAN extra ya cubiertos tampoco cambia
        self._registry(san=["registry.example.com"])._ensure_tls_certificates()
        second = sha(self.cert)
        self._registry(san=["registry.example.com"])._ensure_tls_certificates()
        self.assertEqual(sha(self.cert), second)

    def test_07_regenerated_only_when_needed(self):
        self._registry()._ensure_tls_certificates()
        h = sha(self.cert)
        self._registry(san=["10.0.0.5"])._ensure_tls_certificates()  # nombre nuevo pedido
        self.assertNotEqual(sha(self.cert), h)
        h = sha(self.cert)
        os.remove(self.key)                                          # falta la clave
        self._registry(san=["10.0.0.5"])._ensure_tls_certificates()
        self.assertNotEqual(sha(self.cert), h)
        with open(self.key, "w") as f:                               # clave que no forma pareja
            f.write("no es una clave")
        h = sha(self.cert)
        self._registry(san=["10.0.0.5"])._ensure_tls_certificates()
        self.assertNotEqual(sha(self.cert), h)

    def test_08_https_server_restart_keeps_certificate_and_validation(self):
        hashes = []
        for _ in range(2):
            port = free_port()
            reg = self._registry(port=port)
            threading.Thread(target=reg.run_server, daemon=True).start()
            deadline = time.time() + 10
            while True:
                try:
                    ctx = ssl.create_default_context(cafile=self.cert)  # validación completa: cadena y nombre
                    with urllib.request.urlopen(f"https://127.0.0.1:{port}/api/stations", context=ctx, timeout=2) as r:
                        self.assertEqual(r.status, 200)
                    break
                except Exception:
                    if time.time() > deadline:
                        raise
                    time.sleep(0.1)
            hashes.append(sha(self.cert))
        self.assertEqual(hashes[0], hashes[1], "Reiniciar el Registry no debe cambiar su certificado")
        # Un certificado ajeno sigue sin ser aceptado
        other = os.path.join(self.tmp, "otro")
        os.makedirs(other)
        WMRegistry(port=0, db_path=os.path.join(other, "r.db"), cert_file=os.path.join(other, "c.pem"),
                   key_file=os.path.join(other, "k.pem"))._ensure_tls_certificates()
        with self.assertRaises(Exception):
            urllib.request.urlopen(f"https://127.0.0.1:{port}/api/stations",
                                   context=ssl.create_default_context(cafile=os.path.join(other, "c.pem")),
                                   timeout=2)

    def test_10_reused_private_key_is_restricted_to_0600(self):
        self._registry()._ensure_tls_certificates()
        before = (sha(self.cert), sha(self.key))
        os.chmod(self.key, 0o644)  # p. ej. una clave creada o copiada con permisos amplios
        self._registry()._ensure_tls_certificates()
        self.assertEqual(stat.S_IMODE(os.stat(self.key).st_mode), 0o600)
        self.assertEqual((sha(self.cert), sha(self.key)), before, "Debe reutilizarse el mismo par, sin regenerar")

    def test_09_certificate_not_tracked_by_git(self):
        def git(*args):
            return subprocess.run(["git", *args], cwd=ROOT_DIR, capture_output=True, text=True)
        if git("rev-parse", "--is-inside-work-tree").returncode != 0:
            self.skipTest("No es un repositorio Git")
        tracked = git("ls-files", "WM_Registry/certs", "WM_Central/certs").stdout.split()
        self.assertEqual([t for t in tracked if t.endswith(".pem")], [])
        for path in ("WM_Registry/certs/registry_cert.pem", "WM_Registry/certs/registry_key.pem",
                     "WM_Central/certs/central_cert.pem", "WM_Central/certs/central_key.pem"):
            self.assertEqual(git("check-ignore", "-q", path).returncode, 0, f"{path} debe estar ignorado")


if __name__ == "__main__":
    unittest.main(verbosity=2)
