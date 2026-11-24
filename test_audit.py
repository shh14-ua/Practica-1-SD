#!/usr/bin/env python3
"""
Pruebas del bloque 7 (Release 2): registro de auditoría de WM_Central (apartado 4 del enunciado).

Tipo de prueba: WM_Central real con SQLite real, fichero real y servidor HTTP real (API_Central);
Kafka SIMULADO (consumidores con mensajes preparados y productor que captura o falla a voluntad).
El recorrido completo con Monitor y Engine está en test_watering_flow.py (test_33).

Comprueba:
  1. Formato: fecha y hora ISO 8601 con zona horaria, entidad, origen, acción y descripción; mismo
     contenido en SQLite y en el fichero; compatibilidad con registros antiguos.
  2. Eventos obligatorios: STATUS_CHANGE, STATION_BLOCKED/ACTIVATED/REGISTERED, WATERING_REQUEST,
     WATERING_TIMEOUT, KAFKA_SEND_ERROR, rechazos de Kafka, sin duplicados.
  3. Origen: IP del Monitor (o DESCONOCIDO), IP declarada por el FO, CLI ("CENTRAL") y API (IP del
     socket; X-Forwarded-For solo desde un proxy de confianza).
  4. API_AUTH_DENIED, WEATHER_AUTH_DENIED y API_ERROR, limitados en frecuencia sin perder el recuento.
  5. Persistencia: fichero junto a la BD (o WM_AUDIT_LOG), recuperación tras reiniciar y aviso
     (nunca silencioso) si falla la escritura.
"""

import io
import json
import os
import re
import secrets
import shutil
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from contextlib import redirect_stderr
from datetime import datetime
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT_DIR, os.path.join(ROOT_DIR, "WM_Central")):
    if p not in sys.path:
        sys.path.insert(0, p)

import WM_Central
from common import STATUS_AVAILABLE, STATUS_OUT_OF_SERVICE, STATUS_LEAK
from wm_crypto import generate_symmetric_key, seal_kafka_message, next_kafka_seq, KAFKA_KIND_EVENT


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ScriptedConsumer:
    def __init__(self, messages, owner):
        self.messages, self.owner = messages, owner

    def __iter__(self):
        for m in self.messages:
            yield type("Msg", (), {"value": m})()
        self.owner.running = False

    def commit(self):
        pass


class AuditTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_audit_")
        self.db = os.path.join(self.tmp, "central.db")
        self.kafka_ok = True
        self.admin_token = secrets.token_urlsafe(24)
        self.weather_token = secrets.token_urlsafe(24)
        self.env = patch.dict(os.environ, {"WM_ADMIN_TOKEN": self.admin_token,
                                           "WM_WEATHER_TOKEN": self.weather_token,
                                           "WM_AUDIT_LOG": "", "WM_TRUSTED_PROXIES": ""})
        self.env.start()
        self.patches = [
            patch.object(WM_Central, "ensure_topics", lambda *a, **k: None),
            patch.object(WM_Central, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_Central, "send_kafka", lambda *a, **k: self.kafka_ok),
        ]
        for p in self.patches:
            p.start()
        self.centrals = []

    def tearDown(self):
        for c in self.centrals:
            c.running = False
            if getattr(c, "_web_server", None) is not None:
                c._web_server.shutdown()
                c._web_server.server_close()
        for p in reversed(self.patches):
            p.stop()
        self.env.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def central(self, web=False):
        port = free_port() if web else 0
        c = WM_Central.WMCentral(socket_port=0, kafka_bootstrap="fake:9092", web_port=port, db_path=self.db)
        self.centrals.append(c)
        if web:
            threading.Thread(target=c.run_web_server, daemon=True).start()
            c.base = f"http://127.0.0.1:{port}"
            deadline = time.time() + 5
            while time.time() < deadline:
                try:
                    urllib.request.urlopen(c.base + "/api/status", timeout=1)
                    break
                except Exception:
                    time.sleep(0.05)
        return c

    @staticmethod
    def http(c, method, path, body=None, headers=None):
        req = urllib.request.Request(c.base + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as he:
            return he.code, json.loads(he.read().decode() or "{}")

    @staticmethod
    def audits(c, action, entity=None):
        return [a for a in c.recent_audits if a["action"] == action and (entity is None or a["entity"] == entity)]

    @staticmethod
    def connect(c, ws, ip="192.0.2.10"):
        """Simula una WS autenticada desde 'ip' con Monitor conectado y Engine operativo."""
        with c.lock:
            c.station_keys[ws] = generate_symmetric_key()
            c.station_sockets[ws] = object()
            c._station_ip[ws] = ip
            c.stations[ws].update(status=STATUS_AVAILABLE, engine_ok=True, engine_reason="OK")
        return c.station_keys[ws]


class AuditFormatAndPersistenceTests(AuditTestBase):
    def test_01_iso8601_format_db_and_file_consistent(self):
        c = self.central()
        c.audit_event("WS-01", "192.0.2.10", "PRUEBA", "línea 1\nlínea falsa [X] [Y]", "a\r\nb")
        a = c.recent_audits[-1]
        ts = datetime.fromisoformat(a["timestamp"])
        self.assertIsNotNone(ts.tzinfo, "La fecha debe incluir zona horaria")
        self.assertNotIn("\n", a["description"] + a["details"], "Cada evento en una sola línea")
        with sqlite3.connect(self.db) as db:
            row = db.execute("SELECT timestamp, entity, source_ip, action, description, details FROM audit_log "
                             "ORDER BY id DESC LIMIT 1").fetchone()
        self.assertEqual(row, tuple(a[k] for k in ("timestamp", "entity", "source_ip", "action", "description", "details")))
        path = os.path.join(self.tmp, "audit.log")
        self.assertEqual(c.audit_log_path, path, "audit.log junto a la base de datos de Central")
        with open(path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        self.assertEqual(lines[-1], f"[{a['timestamp']}] [WS-01@192.0.2.10] [PRUEBA] {a['description']} | {a['details']}")

    def test_02_restart_recovers_last_100_and_legacy_rows(self):
        c = self.central()
        with sqlite3.connect(self.db) as db:  # registro con el formato anterior (sin zona horaria)
            db.execute("INSERT INTO audit_log (timestamp, entity, source_ip, action, description, details) "
                       "VALUES ('2026-11-01 10:00:00', 'WS-01', '10.0.0.1', 'AUTH_SUCCESS', 'antiguo', '')")
        for i in range(105):
            c.audit_event("WS-02", "CENTRAL", "PRUEBA", f"evento {i}")
        c.running = False
        c2 = self.central(web=True)
        self.assertEqual(len(c2.recent_audits), 100)
        self.assertEqual(c2.recent_audits[-1]["description"], "evento 104")
        code, api = self.http(c2, "GET", "/api/audit")
        self.assertEqual((code, api[-1]["description"]), (200, "evento 104"), "La API muestra lo anterior al reinicio")
        self.assertEqual(self.http(c2, "GET", "/api/status")[1]["audits"][-1]["description"], "evento 104")
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT timestamp FROM audit_log WHERE description='antiguo'").fetchone()[0],
                             "2026-11-01 10:00:00", "Los registros antiguos se conservan sin cambios")

    def test_03_configurable_path_and_failures_are_reported(self):
        custom = os.path.join(self.tmp, "otra", "auditoria.log")
        os.makedirs(os.path.dirname(custom))
        with patch.dict(os.environ, {"WM_AUDIT_LOG": custom}):
            c = self.central()
        c.audit_event("WS-01", "CENTRAL", "PRUEBA", "a fichero configurado")
        with open(custom, encoding="utf-8") as f:
            self.assertIn("a fichero configurado", f.read())

        c.audit_log_path = self.tmp  # un directorio: la escritura falla
        err = io.StringIO()
        with redirect_stderr(err):
            c.audit_event("WS-01", "CENTRAL", "PRUEBA", "evento con fichero roto")
            c.audit_event("WS-01", "CENTRAL", "PRUEBA", "segundo evento con fichero roto")
        self.assertIn("no se pudo guardar un evento de auditoría en fichero", err.getvalue())
        self.assertEqual(err.getvalue().count("fichero"), 1, "Aviso limitado, sin repetirse en cada evento")
        self.assertTrue(any("auditoría en fichero" in l["message"] for l in c.recent_logs), "Visible en API y Front")
        with sqlite3.connect(self.db) as db:
            self.assertTrue(db.execute("SELECT 1 FROM audit_log WHERE description='evento con fichero roto'").fetchone(),
                            "El evento no se pierde: queda en SQLite")

        real_get_db = c._get_db
        err = io.StringIO()
        with patch.object(c, "_get_db", side_effect=sqlite3.OperationalError("disk I/O error")), redirect_stderr(err):
            c.audit_event("WS-01", "CENTRAL", "PRUEBA", "evento con SQLite caído")
        self.assertIn("en SQLite (OperationalError)", err.getvalue())
        self.assertNotIn("evento con SQLite caído", err.getvalue(), "El aviso no incluye el contenido del evento")
        self.assertEqual(c.recent_audits[-1]["description"], "evento con SQLite caído")


class AuditEventsTests(AuditTestBase):
    def test_04_status_changes_with_monitor_origin_and_no_duplicates(self):
        c = self.central()
        self.connect(c, "WS-01", "192.0.2.10")
        n = len(self.audits(c, "STATUS_CHANGE"))
        c.set_station_status("WS-01", STATUS_LEAK)
        c.set_station_status("WS-01", STATUS_LEAK)  # sin cambio: sin registro
        changes = self.audits(c, "STATUS_CHANGE")[n:]
        self.assertEqual(len(changes), 1)
        self.assertEqual((changes[0]["entity"], changes[0]["source_ip"]), ("WS-01", "192.0.2.10"))
        self.assertIn("DISPONIBLE -> FUGA", changes[0]["description"])
        c.set_station_status("WS-03", STATUS_OUT_OF_SERVICE)  # WS sin Monitor conocido
        self.assertEqual(self.audits(c, "STATUS_CHANGE", "WS-03")[-1]["source_ip"], "DESCONOCIDO")

    def test_05_cli_block_activate_register(self):
        c = self.central()
        self.connect(c, "WS-01")
        c.block_station("WS-01")
        c.activate_station("WS-01")
        self.assertTrue(c.register_station("WS-09", "Huerto"))
        for action, ws in (("STATION_BLOCKED", "WS-01"), ("STATION_ACTIVATED", "WS-01"), ("STATION_REGISTERED", "WS-09")):
            ev = self.audits(c, action, ws)
            self.assertEqual(len(ev), 1, action)
            self.assertEqual(ev[0]["source_ip"], "CENTRAL", "Acciones de la consola de Central")
        c.block_station("WS-02")  # desconectada: no se bloquea ni se audita como bloqueada
        self.assertEqual(self.audits(c, "STATION_BLOCKED", "WS-02"), [])

    def test_06_fo_requests_with_declared_ip_and_timeout(self):
        c = self.central()
        self.connect(c, "WS-01")
        msgs = [
            {"op": "FO-01", "ws": "WS-01", "duration": 10, "source_ip": "198.51.100.23"},
            {"op": "FO-02", "ws": "WS-02", "duration": 5, "source_ip": "198.51.100.24"},
            {"op": "FO-03", "ws": "WS-01", "duration": 5},
            {"op": "FO-01", "ws": "WS-02", "duration": 5, "source_ip": "1.2.3"},
        ]
        c.running = True
        with patch.object(WM_Central, "make_consumer", lambda *a, **k: ScriptedConsumer(msgs, c)):
            c.run_kafka_requests_consumer()
        req = self.audits(c, "WATERING_REQUEST")
        self.assertEqual([(r["entity"], r["source_ip"]) for r in req],
                         [("FO-01", "198.51.100.23"), ("FO-02", "198.51.100.24"),
                          ("FO-03", "DESCONOCIDO"), ("FO-01", "DESCONOCIDO")])
        self.assertIn("AUTORIZADA", req[0]["description"])
        self.assertIn("IP declarada por el FO (no verificada por Kafka)", req[0]["details"])
        self.assertIn("DENEGADA", req[1]["description"])
        self.assertIn("desconectada", req[1]["details"])
        self.assertIn("no declaró una IP válida", req[2]["details"])

        pending_since = c.stations["WS-01"]["pending_since"]
        with patch("time.sleep", return_value=None):
            c.check_watering_timeout("WS-01", pending_since)
        timeout = self.audits(c, "WATERING_TIMEOUT", "WS-01")
        self.assertEqual(len(timeout), 1)
        self.assertIn("FO-01", timeout[0]["description"])

    def test_07_kafka_send_errors_and_rejections_origin(self):
        c = self.central()
        key = self.connect(c, "WS-01", "192.0.2.10")
        self.kafka_ok = False
        for _ in range(5):
            self.assertFalse(c.send_ws_command("WS-01", "STOP", reason="LEAK"))
        errors = self.audits(c, "KAFKA_SEND_ERROR", "WS-01")
        self.assertEqual(len(errors), 1, "Errores repetidos limitados en frecuencia")
        self.assertEqual(errors[0]["source_ip"], "CENTRAL")
        self.kafka_ok = True
        c.send_ws_command("WS-01", "BLOCK")
        self.assertEqual(len(self.audits(c, "KAFKA_SEND_ERROR")), 1)

        c._process_kafka_event(seal_kafka_message(generate_symmetric_key(), KAFKA_KIND_EVENT, "WS-01",
                                                  {"type": "TELEMETRY"}, next_kafka_seq(0)))
        self.assertEqual(self.audits(c, "KAFKA_REJECTED", "WS-01")[-1]["source_ip"], "192.0.2.10",
                         "Evento Kafka de una WS: IP de su Monitor")
        c._process_kafka_event({"ws": "WS-04", "enc": "ENC:x"})
        self.assertEqual(self.audits(c, "KAFKA_REJECTED", "WS-04")[-1]["source_ip"], "DESCONOCIDO")


class AuditApiOriginTests(AuditTestBase):
    def test_08_api_origin_and_untrusted_forwarded_for(self):
        c = self.central(web=True)
        self.connect(c, "WS-01")
        self.connect(c, "WS-02")
        tok = {"X-Admin-Token": self.admin_token}
        # Directa desde la máquina de Central
        self.assertEqual(self.http(c, "POST", "/api/action", {"action": "block", "ws": "WS-01"})[0], 200)
        ev = self.audits(c, "STATION_BLOCKED", "WS-01")[-1]
        self.assertEqual(ev["source_ip"], "127.0.0.1")
        # X-Forwarded-For sin proxy de confianza: se anota como no confiable, el origen es el socket
        self.assertEqual(self.http(c, "POST", "/api/action", {"action": "activate", "ws": "WS-01"},
                                   {**tok, "X-Forwarded-For": "203.0.113.9"})[0], 200)
        ev = self.audits(c, "STATION_ACTIVATED", "WS-01")[-1]
        self.assertEqual(ev["source_ip"], "127.0.0.1")
        self.assertIn("X-Forwarded-For no confiable: 203.0.113.9", ev["details"])
        # Desde un proxy de confianza (el Front): se registra la IP del cliente
        with patch.dict(os.environ, {"WM_TRUSTED_PROXIES": "127.0.0.1"}):
            self.http(c, "POST", "/api/action", {"action": "revoke_keys", "ws": "WS-02"},
                      {**tok, "X-Forwarded-For": "203.0.113.9"})
            self.http(c, "POST", "/api/action", {"action": "block", "ws": "WS-01"},
                      {**tok, "X-Forwarded-For": "no-es-una-ip"})
        rev = self.audits(c, "KEY_REVOCATION", "WS-02")[-1]
        self.assertEqual(rev["source_ip"], "203.0.113.9")
        self.assertIn("vía proxy de confianza 127.0.0.1", rev["details"])
        blk = self.audits(c, "STATION_BLOCKED", "WS-01")[-1]
        self.assertEqual(blk["source_ip"], "127.0.0.1")
        self.assertIn("valor no válido", blk["details"])
        self.http(c, "POST", "/api/action", {"action": "start", "ws": "WS-03", "duration": 5}, tok)
        self.assertEqual(self.audits(c, "WATERING_REQUEST", "CENTRAL")[-1]["source_ip"], "127.0.0.1")

    def test_09_auth_denials_and_api_errors_audited_and_rate_limited(self):
        c = self.central(web=True)
        fwd = {"X-Forwarded-For": "203.0.113.50"}
        for _ in range(8):
            self.assertEqual(self.http(c, "POST", "/api/action", {"action": "block", "ws": "WS-01"}, fwd)[0], 403)
            self.assertEqual(self.http(c, "PUT", "/api/weather/WS-01", {"temp": -4, "alert": True},
                                       {"X-Weather-Token": "malo"})[0], 401)
        denied = self.audits(c, "API_AUTH_DENIED")
        wdenied = self.audits(c, "WEATHER_AUTH_DENIED")
        self.assertEqual((len(denied), len(wdenied)), (1, 1), "Rechazos repetidos limitados")
        self.assertIn("X-Forwarded-For no confiable: 203.0.113.50", denied[0]["description"])
        self.assertIn("HTTP 401", wdenied[0]["details"])
        for k in c._kafka_reject_audit:
            c._kafka_reject_audit[k][0] -= 31
        self.http(c, "POST", "/api/action", {"action": "block", "ws": "WS-01"}, fwd)
        self.assertIn("7 mensajes similares omitidos", self.audits(c, "API_AUTH_DENIED")[-1]["details"])

        with patch.object(c, "_waterings_view", side_effect=RuntimeError("fallo")):
            self.assertEqual(self.http(c, "GET", "/api/waterings")[0], 500)
        err = self.audits(c, "API_ERROR")
        self.assertEqual(len(err), 1)
        self.assertIn("GET /api/waterings", err[0]["description"])
        self.assertEqual(err[0]["source_ip"], "127.0.0.1")

    def test_10_no_secrets_in_audit_storage_or_api(self):
        c = self.central(web=True)
        key = self.connect(c, "WS-01")
        c.revoke_station_keys("WS-01")
        self.http(c, "POST", "/api/action", {"action": "block", "ws": "WS-02"}, {"X-Admin-Token": "intento-fallido"})
        self.http(c, "PUT", "/api/weather/WS-01", {"temp": -4, "alert": True}, {"X-Weather-Token": self.weather_token})
        with sqlite3.connect(self.db) as db:
            stored = json.dumps(db.execute("SELECT * FROM audit_log").fetchall())
        with open(c.audit_log_path, encoding="utf-8") as f:
            stored += f.read()
        stored += json.dumps(self.http(c, "GET", "/api/audit")[1])
        for secret in (key, key[:10], self.admin_token, self.weather_token, "intento-fallido"):
            self.assertNotIn(secret, stored)
        self.assertIsNone(re.search(r"\b[0-9a-f]{64}\b", stored), "Sin huellas de credenciales")


if __name__ == "__main__":
    unittest.main(verbosity=2)
