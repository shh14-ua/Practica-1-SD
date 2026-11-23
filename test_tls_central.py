#!/usr/bin/env python3
"""
Pruebas del canal TLS WM_Central <-> WM_WS_M (Release 2, bloque 3).

Se ejecutan con sockets TCP reales, TLS real (módulo ssl / OpenSSL), certificados X.509 reales
y un WM_Registry HTTPS real. Solo se sustituye Kafka (no interviene en este canal).
Los certificados, bases de datos y credenciales se crean en un directorio temporal.

Comprueba:
  1. Generación del certificado de Central con SAN configurables y clave privada 0600.
  2. Monitor real: TLS con validación de certificado y nombre, AUTH y entrega de clave simétrica,
     y mensajes PING/LEAK/FIXED cifrados sobre TLS.
  3. Rechazo de clientes sin TLS (auditoría TLS_FAILED) sin llegar a procesar AUTH.
  4. Rechazo de certificados no válidos: CA distinta, nombre de host fuera del SAN, CA ausente.
  5. Revocación sobre TLS: aviso como respuesta del protocolo, FUERA DE SERVICIO persistente,
     sin re-autenticación automática y re-autenticación manual con clave nueva.
  6. Acceso concurrente al socket del Monitor (bucle y menú a la vez) sin corromper el canal.
  7. Cierre de la conexión si Central acepta la autenticación sin una clave simétrica válida.
  8. Central no abre el puerto si no puede preparar TLS (sin alternativa en claro).
"""

import os
import shutil
import socket
import ssl
import stat
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT_DIR, os.path.join(ROOT_DIR, "WM_Central"), os.path.join(ROOT_DIR, "WM_WS")):
    if p not in sys.path:
        sys.path.insert(0, p)

import wm_crypto
import WM_Central
import WM_WS_M
from common import lrc, send_frame, recv_msg, server_handshake, STATUS_OUT_OF_SERVICE, STATUS_LEAK
from WM_Registry.WM_Registry import WMRegistry


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(cond, timeout=5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    return cond()


class CentralTLSTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="wm_tls_")
        # Kafka sustituido: no forma parte del canal Central <-> Monitor
        cls.kafka_patches = [
            patch.object(WM_Central, "ensure_topics", lambda *a, **kw: None),
            patch.object(WM_Central, "make_producer", lambda *a, **kw: "FAKE_PRODUCER"),
            patch.object(WM_Central, "send_kafka", lambda *a, **kw: True),
        ]
        for p in cls.kafka_patches:
            p.start()

        # WM_Registry HTTPS real con su propio certificado temporal
        cls.reg_port = free_port()
        cls.reg_cert = os.path.join(cls.tmp, "registry_cert.pem")
        cls.registry = WMRegistry(port=cls.reg_port, db_path=os.path.join(cls.tmp, "registry.db"),
                                  cert_file=cls.reg_cert, key_file=os.path.join(cls.tmp, "registry_key.pem"))
        threading.Thread(target=cls.registry.run_server, daemon=True).start()

        # WM_Central real con TLS (certificado temporal con un SAN extra de despliegue cloud)
        cls.central_port = free_port()
        cls.central_cert = os.path.join(cls.tmp, "central_cert.pem")
        cls.central_key = os.path.join(cls.tmp, "central_key.pem")
        cls.central = WM_Central.WMCentral(
            socket_port=cls.central_port, kafka_bootstrap="fake:9092", web_port=0,
            db_path=os.path.join(cls.tmp, "central.db"),
            tls_cert=cls.central_cert, tls_key=cls.central_key,
            tls_san=["wm-central.example.com", "10.20.30.40"],
        )
        cls.central.registry_url = f"https://127.0.0.1:{cls.reg_port}"
        cls.central.registry_ca = cls.reg_cert
        cls.central.registry_db_path = os.path.join(cls.tmp, "no_existe.db")  # fuerza la consulta HTTPS
        threading.Thread(target=cls.central.run_socket_server, daemon=True).start()

        assert wait_until(lambda: os.path.isfile(cls.reg_cert), 10), "El Registry no generó su certificado"
        assert wait_until(lambda: cls._port_open(cls.central_port) and cls._port_open(cls.reg_port), 10)

    @classmethod
    def tearDownClass(cls):
        cls.central.running = False
        for p in reversed(cls.kafka_patches):
            p.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    @staticmethod
    def _port_open(port) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return True
        except OSError:
            return False

    def _monitor(self, ws_id, central_host="127.0.0.1", central_ca=None):
        return WM_WS_M.WSMonitor(
            engine_port=free_port(), central_addr=f"{central_host}:{self.central_port}", ws_id=ws_id,
            location="Zona TLS", registry_url=f"https://127.0.0.1:{self.reg_port}",
            registry_ca=self.reg_cert, cred_file=os.path.join(self.tmp, f"{ws_id}.cred"),
            central_ca=central_ca or self.central_cert,
        )

    def _audits(self, action):
        return [a for a in list(self.central.recent_audits) if a["action"] == action]

    def _client_ctx(self, cafile):
        return ssl.create_default_context(cafile=cafile)

    # ------------------------------------------------------------------ 1
    def test_01_certificate_san_and_private_key_permissions(self):
        from cryptography import x509
        with open(self.central_cert, "rb") as f:
            cert = x509.load_pem_x509_certificate(f.read())
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        names = set(san.get_values_for_type(x509.DNSName)) | {str(i) for i in san.get_values_for_type(x509.IPAddress)}
        self.assertTrue({"localhost", "central", "127.0.0.1", "wm-central.example.com", "10.20.30.40"} <= names)
        self.assertEqual(stat.S_IMODE(os.stat(self.central_key).st_mode), 0o600)
        # Reutiliza el par si ya cubre los SAN y lo regenera si se pide un nombre nuevo
        self.assertFalse(wm_crypto.ensure_tls_certificate(self.central_cert, self.central_key, "WM_Central",
                                                          ["localhost", "127.0.0.1"]))
        c2, k2 = os.path.join(self.tmp, "c2.pem"), os.path.join(self.tmp, "k2.pem")
        self.assertTrue(wm_crypto.ensure_tls_certificate(c2, k2, "WM_Central", ["localhost"]))
        self.assertFalse(wm_crypto.ensure_tls_certificate(c2, k2, "WM_Central", ["localhost"]))
        self.assertTrue(wm_crypto.ensure_tls_certificate(c2, k2, "WM_Central", ["localhost", "nuevo.example.com"]))

    # ------------------------------------------------------------------ 2
    def test_02_monitor_tls_auth_key_delivery_and_encrypted_messages(self):
        mon = self._monitor("WS-31")
        self.assertTrue(mon.register_in_registry())
        self.assertTrue(mon.ensure_central_connection())
        self.assertIsInstance(mon.central_sock, ssl.SSLSocket)
        self.assertIn(mon.central_sock.version(), ("TLSv1.2", "TLSv1.3"))
        self.assertEqual(mon.symmetric_key, self.central.station_keys["WS-31"])
        self.assertTrue(self._audits("AUTH_SUCCESS"))

        self.assertTrue(mon.send_to_central("PING#WS-31#OK#OK"))
        self.assertTrue(mon.send_to_central("LEAK#WS-31#SENSOR_KO"))
        self.assertEqual(self.central.stations["WS-31"]["status"], STATUS_LEAK)
        self.assertTrue(mon.send_to_central("FIXED#WS-31"))
        self.assertEqual(self.central.stations["WS-31"]["status"], "DISPONIBLE")
        mon.central_sock.close()

    # ------------------------------------------------------------------ 3
    def test_03_plaintext_clients_are_rejected_and_audited(self):
        before_fail = len(self._audits("TLS_FAILED"))
        before_auth = len(self._audits("AUTH_SUCCESS")) + len(self._audits("AUTH_FAILED"))

        # a) Cliente del protocolo de la Release 1 en claro (ENQ + trama AUTH)
        s = socket.create_connection(("127.0.0.1", self.central_port), timeout=5)
        payload = b"AUTH#WS-32#CRED-EN-CLARO#Zona"
        s.sendall(b"\x05" + b"\x02" + payload + b"\x03" + lrc(payload))
        try:
            reply = s.recv(16)
        except ConnectionResetError:
            reply = b""
        self.assertEqual(reply, b"", "Central no debe responder (ni ACK) a un cliente sin TLS")
        s.close()

        # b) Cliente que solo envía ENQ y espera (Central corta al vencer el plazo del handshake TLS)
        s2 = socket.create_connection(("127.0.0.1", self.central_port), timeout=15)
        s2.sendall(b"\x05")
        self.assertEqual(s2.recv(1), b"", "Central no debe responder ACK a un cliente sin TLS")
        s2.close()

        self.assertTrue(wait_until(lambda: len(self._audits("TLS_FAILED")) >= before_fail + 2, 3))
        last = self._audits("TLS_FAILED")[-1]
        self.assertEqual(last["source_ip"], "127.0.0.1")
        after_auth = len(self._audits("AUTH_SUCCESS")) + len(self._audits("AUTH_FAILED"))
        self.assertEqual(after_auth, before_auth, "Ningún AUTH en claro debe llegar a procesarse")
        self.assertNotIn("WS-32", self.central.station_keys)

    # ------------------------------------------------------------------ 4
    def test_04_invalid_certificates_are_rejected(self):
        # a) CA distinta: certificado autofirmado ajeno con los mismos nombres
        other_cert, other_key = os.path.join(self.tmp, "otro_cert.pem"), os.path.join(self.tmp, "otro_key.pem")
        wm_crypto.ensure_tls_certificate(other_cert, other_key, "WM_Central", ["localhost", "127.0.0.1"])
        before = len(self._audits("TLS_FAILED"))
        raw = socket.create_connection(("127.0.0.1", self.central_port), timeout=3)
        with self.assertRaises(ssl.SSLCertVerificationError) as ctx:
            self._client_ctx(other_cert).wrap_socket(raw, server_hostname="127.0.0.1")
        self.assertIn("self-signed", ctx.exception.verify_message)
        raw.close()
        self.assertTrue(wait_until(lambda: len(self._audits("TLS_FAILED")) > before, 3),
                        "Central debe auditar el handshake rechazado por el cliente")

        mon = self._monitor("WS-33", central_ca=other_cert)
        self.assertTrue(mon.register_in_registry())
        self.assertFalse(mon.ensure_central_connection())
        self.assertIsNone(mon.central_sock)
        self.assertNotIn("WS-33", self.central.station_keys)

        # b) Nombre de host que no figura en el SAN del certificado de Central
        raw = socket.create_connection(("127.0.0.1", self.central_port), timeout=3)
        with self.assertRaises(ssl.SSLCertVerificationError) as ctx:
            self._client_ctx(self.central_cert).wrap_socket(raw, server_hostname="host-no-autorizado.example")
        self.assertIn("Hostname mismatch", ctx.exception.verify_message)
        raw.close()

        # c) CA de Central ausente: el Monitor no se conecta (ni en claro)
        mon2 = self._monitor("WS-34", central_ca=os.path.join(self.tmp, "no_existe.pem"))
        self.assertTrue(mon2.register_in_registry())
        self.assertFalse(mon2.ensure_central_connection())
        self.assertNotIn("WS-34", self.central.station_keys)

    def test_05_hostname_validation_with_real_non_loopback_address(self):
        """El Monitor se conecta por una IP real de la máquina que no está en el SAN: debe rechazarse."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("192.0.2.1", 9))  # no envía tráfico: solo elige la interfaz de salida
                lan_ip = s.getsockname()[0]
        except OSError:
            self.skipTest("Sin interfaz de red distinta de loopback")
        if lan_ip.startswith("127.") or lan_ip in ("10.20.30.40",):
            self.skipTest("Sin interfaz de red distinta de loopback")
        mon = self._monitor("WS-35", central_host=lan_ip)
        self.assertTrue(mon.register_in_registry())
        self.assertFalse(mon.ensure_central_connection())
        self.assertNotIn("WS-35", self.central.station_keys)

    # ------------------------------------------------------------------ 5
    def test_06_revocation_over_tls(self):
        mon = self._monitor("WS-36")
        self.assertTrue(mon.register_in_registry())
        self.assertTrue(mon.ensure_central_connection())
        old_key = mon.symmetric_key
        self.assertTrue(mon.send_to_central("PING#WS-36#OK#OK"))

        self.assertTrue(self.central.revoke_station_keys("WS-36"))
        self.assertEqual(self.central.stations["WS-36"]["status"], STATUS_OUT_OF_SERVICE)

        # El aviso llega como respuesta al siguiente mensaje (sin escrituras desde otro hilo)
        self.assertFalse(mon.send_to_central("PING#WS-36#OK#OK"))
        self.assertTrue(mon.key_revoked)
        self.assertIsNone(mon.symmetric_key)
        self.assertIsNone(mon.central_sock)
        self.assertTrue(wait_until(lambda: "WS-36" not in self.central.station_sockets, 3))
        self.assertEqual(self.central.stations["WS-36"]["status"], STATUS_OUT_OF_SERVICE,
                         "Debe seguir FUERA DE SERVICIO aunque el Monitor se haya desconectado")
        self.assertTrue(self._audits("REJECTED_REVOKED"))

        # Sin re-autenticación automática
        self.assertFalse(mon.ensure_central_connection())
        self.assertFalse(mon.send_to_central("PING#WS-36#OK#OK"))
        self.assertNotIn("WS-36", self.central.station_keys)

        # Re-autenticación manual (opción 'k'): credencial renovada en el Registry y clave nueva
        mon.reauthenticate()
        self.assertFalse(mon.key_revoked)
        self.assertIsNotNone(mon.symmetric_key)
        self.assertNotEqual(mon.symmetric_key, old_key)
        self.assertEqual(mon.symmetric_key, self.central.station_keys["WS-36"])
        self.assertNotEqual(self.central.stations["WS-36"]["status"], STATUS_OUT_OF_SERVICE)
        self.assertTrue(mon.send_to_central("PING#WS-36#OK#OK"))
        self.assertTrue(self._audits("KEY_RENEWED"))
        mon.central_sock.close()

    # ------------------------------------------------------------------ 6
    def test_07_concurrent_use_of_monitor_socket(self):
        mon = self._monitor("WS-37")
        self.assertTrue(mon.register_in_registry())
        self.assertTrue(mon.ensure_central_connection())
        results, errors = [], []

        def worker(n):
            try:
                for _ in range(n):
                    results.append(mon.send_to_central("PING#WS-37#OK#OK"))
            except Exception as exc:  # pragma: no cover - solo si el canal se corrompe
                errors.append(exc)

        def menu_like():
            try:
                for _ in range(10):
                    results.append(mon.ensure_central_connection(manual=True))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(25,)) for _ in range(3)]
        threads.append(threading.Thread(target=menu_like))
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 85)
        self.assertTrue(all(results), "Ningún envío concurrente debe fallar ni corromper la sesión TLS")
        self.assertIsNotNone(mon.central_sock)
        mon.central_sock.close()

    # ------------------------------------------------------------------ 7
    def test_08_connection_closed_when_auth_ok_without_valid_key(self):
        """Servidor TLS de prueba que acepta AUTH pero devuelve una clave inválida."""
        port = free_port()
        srv_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        srv_ctx.load_cert_chain(self.central_cert, self.central_key)
        lsock = socket.socket()
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(("127.0.0.1", port))
        lsock.listen(5)
        closed_by_client = []

        def serve():
            for _ in range(2):
                conn, _ = lsock.accept()
                with srv_ctx.wrap_socket(conn, server_side=True) as c:
                    c.settimeout(5)
                    server_handshake(c)
                    recv_msg(c)
                    send_frame(c, "STATUS#OK#corta#Autenticada sin clave")
                    try:
                        closed_by_client.append(c.recv(1) == b"")
                    except OSError:
                        closed_by_client.append(True)

        t = threading.Thread(target=serve, daemon=True)
        t.start()
        mon = WM_WS_M.WSMonitor(engine_port=free_port(), central_addr=f"127.0.0.1:{port}", ws_id="WS-38",
                                cred_file=os.path.join(self.tmp, "WS-38.cred"), central_ca=self.central_cert)
        mon.credential = "CRED-PRUEBA"
        self.assertFalse(mon.ensure_central_connection())
        self.assertIsNone(mon.central_sock)
        self.assertFalse(mon.ensure_central_connection())  # segundo intento: tampoco deja nada abierto
        t.join(10)
        lsock.close()
        self.assertEqual(closed_by_client, [True, True], "El Monitor debe cerrar cada conexión fallida")

    # ------------------------------------------------------------------ 8
    def test_09_central_refuses_to_open_port_without_tls(self):
        c_dir = os.path.join(self.tmp, "sin_tls")
        central = WM_Central.WMCentral(
            socket_port=free_port(), kafka_bootstrap="fake:9092", web_port=0,
            db_path=os.path.join(self.tmp, "central2.db"),
            tls_cert=os.path.join(c_dir, "c.pem"), tls_key=os.path.join(c_dir, "k.pem"))
        with patch.object(wm_crypto, "_HAS_CRYPTOGRAPHY", False):
            with self.assertRaises(RuntimeError):
                central._init_tls_context()
            with self.assertRaises(SystemExit):
                central.start()
        self.assertFalse(self._port_open(central.socket_port))


if __name__ == "__main__":
    unittest.main(verbosity=2)
