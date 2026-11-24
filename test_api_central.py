#!/usr/bin/env python3
"""
Pruebas del bloque 6 (Release 2): API_Central REST, autenticación de acciones y de WM_W,
estado de registro consultado a WM_Registry y notificaciones periódicas de WM_W.

Tipo de prueba: REAL para HTTP (servidor de API_Central real), SQLite, WM_Registry HTTPS real y
WM_W real (con su simulación manual del clima en lugar de OpenWeather). Kafka SIMULADO: se
capturan las órdenes publicadas. Los tokens se generan aleatoriamente en cada ejecución.

Comprueba:
  1. Recursos de consulta: estaciones, operarios con su estado, riegos en curso (404 si no existen).
  2. Acciones de control: protegidas con WM_ADMIN_TOKEN (también a través de un proxy), validación
     (400), recursos inexistentes (404) y resultado real (409 si no se puede realizar).
  3. Notificaciones de WM_W: protegidas con WM_WEATHER_TOKEN (503 sin configurar, 401 sin token
     válido), PUT/DELETE/POST, validación y sin eventos ni órdenes duplicados al repetirse.
  4. Estado de registro de cada WS consultado a WM_Registry por HTTPS, sin datos sensibles y con
     el Registry caído.
  5. WM_W envía el estado cada ciclo: Central recupera las alertas tras reiniciarse.
  6. Cancelar una alerta solo devuelve a DISPONIBLE una WS con Monitor conectado, clave vigente y sin
     otro bloqueo (sin Monitor: DESCONECTADA; sin Engine operativo: FUGA; bloqueo manual o claves
     revocadas: FUERA DE SERVICIO), también tras reiniciar Central, y nunca inicia un riego.
"""

import json
import os
import secrets
import shutil
import socket
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
from common import TOPIC_COMMANDS, STATUS_AVAILABLE, STATUS_OUT_OF_SERVICE, STATUS_WATERING
from wm_crypto import generate_symmetric_key, open_kafka_message, KAFKA_KIND_COMMAND
from WM_Registry.WM_Registry import WMRegistry
from WM_W.WM_W import WeatherControlOffice


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


class ApiTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_api_")
        self.sent = []
        self.admin_token = secrets.token_urlsafe(24)
        self.weather_token = secrets.token_urlsafe(24)
        self.env = patch.dict(os.environ, {"WM_ADMIN_TOKEN": self.admin_token,
                                           "WM_WEATHER_TOKEN": self.weather_token})
        self.env.start()
        self.patches = [
            patch.object(WM_Central, "ensure_topics", lambda *a, **k: None),
            patch.object(WM_Central, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_Central, "send_kafka", lambda prod, topic, msg, **k: self.sent.append((topic, msg)) or True),
        ]
        for p in self.patches:
            p.start()
        self.centrals = []
        self.central, self.base = self._start_central()

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

    def _start_central(self):
        port = free_port()
        c = WM_Central.WMCentral(socket_port=0, kafka_bootstrap="fake:9092", web_port=port,
                                 db_path=os.path.join(self.tmp, "central.db"))
        self.centrals.append(c)
        threading.Thread(target=c.run_web_server, daemon=True).start()
        base = f"http://127.0.0.1:{port}"
        self.assertTrue(wait_until(lambda: self._try(base + "/api/status"), 5))
        return c, base

    @staticmethod
    def _try(url) -> bool:
        try:
            with urllib.request.urlopen(url, timeout=1):
                return True
        except Exception:
            return False

    def http(self, method, path, body=None, headers=None, raw=None, base=None):
        data = raw if raw is not None else (json.dumps(body).encode("utf-8") if body is not None else None)
        req = urllib.request.Request((base or self.base) + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                code, text = r.status, r.read().decode("utf-8")
        except urllib.error.HTTPError as he:
            code, text = he.code, he.read().decode("utf-8")
        try:
            return code, json.loads(text)
        except ValueError:
            return code, text

    # Peticiones como las reenvía un proxy (no locales): requieren el token de administración
    def admin(self, body, token=None):
        headers = {"X-Forwarded-For": "203.0.113.7"}
        if token is not None:
            headers["X-Admin-Token"] = token
        return self.http("POST", "/api/action", body, headers)

    def weather(self, method, path, body=None, token=None):
        return self.http(method, path, body, {"X-Weather-Token": self.weather_token if token is None else token})

    def commands(self, key, ws="WS-01"):
        out = []
        for t, m in self.sent:
            if t == TOPIC_COMMANDS:
                inner, _ = open_kafka_message(key, m, KAFKA_KIND_COMMAND, ws)
                if inner:
                    out.append(inner)
        return out

    def audits(self, action, ws=None):
        return [a for a in self.central.recent_audits if a["action"] == action and (ws is None or a["entity"] == ws)]

    def make_available(self, ws="WS-01"):
        key = generate_symmetric_key()
        with self.central.lock:
            self.central.station_keys[ws] = key
            self.central.station_sockets[ws] = object()  # Monitor conectado (simulado)
            st = self.central.stations[ws]
            st.update(status=STATUS_AVAILABLE, engine_ok=True, engine_reason="OK")
        return key


class ApiResourcesTests(ApiTestBase):
    def test_01_query_resources_and_404(self):
        code, stations = self.http("GET", "/api/stations")
        self.assertEqual(code, 200)
        self.assertEqual({s["id"] for s in stations}, {"WS-01", "WS-02", "WS-03", "WS-04", "WS-05"})
        for field in ("status", "has_key", "keys_revoked", "registry_registered", "weather_alert", "monitor_connected"):
            self.assertIn(field, stations[0])
        self.assertEqual(self.http("GET", "/api/stations/ws-01")[0], 200)
        self.assertEqual(self.http("GET", "/api/stations/WS-99")[0], 404)
        code, ops = self.http("GET", "/api/operators")
        self.assertEqual(code, 200)
        self.assertTrue(all(o["status"] == "EN_ESPERA" and o["waterings"] == [] for o in ops))
        self.assertEqual(self.http("GET", "/api/operators/FO-01")[1]["id"], "FO-01")
        self.assertEqual(self.http("GET", "/api/operators/FO-77")[0], 404)
        self.assertEqual(self.http("GET", "/api/waterings"), (200, []))
        self.assertEqual(self.http("GET", "/api/no-existe")[0], 404)
        for path in ("/api/status", "/api/history", "/api/audit", "/api/weather", "/api/stations/?x=1"):
            self.assertEqual(self.http("GET", path)[0], 200, path)
        self.assertIn("stations", self.http("GET", "/api/status")[1], "Compatibilidad del Front y la Release 1")
        self.assertIn(self.http("PATCH", "/api/action")[0], (405, 501), "Método no soportado")

    def test_02_waterings_and_operator_state(self):
        key = self.make_available()
        code, res = self.admin({"action": "start", "ws": "WS-01", "duration": 30, "op": "FO-01"}, self.admin_token)
        self.assertEqual(code, 200, res)
        self.assertEqual([c["cmd"] for c in self.commands(key)], ["START"])
        code, w = self.http("GET", "/api/waterings")
        self.assertEqual((w[0]["ws_id"], w[0]["operator"], w[0]["state"]), ("WS-01", "FO-01", "PENDIENTE_CONFIRMACION"))
        tx = w[0]["transaction_id"]
        self.central.handle_ws_event({"type": "STARTED", "ws": "WS-01", "op": "FO-01", "transaction_id": tx})
        w = self.http("GET", "/api/waterings")[1]
        self.assertEqual(w[0]["state"], "REGANDO")
        op = self.http("GET", "/api/operators/FO-01")[1]
        self.assertEqual((op["status"], op["waterings"][0]["ws_id"]), ("REGANDO", "WS-01"))
        self.assertEqual(self.http("GET", "/api/operators/FO-02")[1]["status"], "EN_ESPERA")
        self.assertEqual(self.http("GET", "/api/stations/WS-01")[1]["status"], STATUS_WATERING)


class ApiActionAuthTests(ApiTestBase):
    def test_03_admin_token_required_unless_direct_local(self):
        self.make_available("WS-03")
        for token in (None, "incorrecto"):
            code, res = self.admin({"action": "revoke_keys", "ws": "WS-03"}, token)
            self.assertEqual(code, 403, token)
        self.assertFalse(self.central.stations["WS-03"].get("keys_revoked"), "Sin token no debe tener efecto")
        code, _ = self.admin({"action": "revoke_keys", "ws": "WS-03"}, self.admin_token)
        self.assertEqual(code, 200)
        self.assertTrue(self.central.stations["WS-03"].get("keys_revoked"))
        # Petición directa desde la máquina de Central (sin proxy): permitida como en la Release 1
        self.assertEqual(self.http("POST", "/api/action", {"action": "block", "ws": "WS-04"})[0], 409)  # desconectada
        self.assertEqual(self.http("POST", "/api/action", {"action": "register", "ws": "WS-07"})[0], 200)
        # Sin WM_ADMIN_TOKEN configurado, ninguna petición reenviada está autorizada
        with patch.dict(os.environ, {"WM_ADMIN_TOKEN": ""}):
            self.assertEqual(self.admin({"action": "block", "ws": "WS-01"}, "")[0], 403)

    def test_04_action_validation_and_real_results(self):
        self.make_available("WS-01")
        t = self.admin_token
        bad = [
            ({"action": "volar", "ws": "WS-01"}, 400),
            ({"action": "block"}, 400),
            ({"action": "start", "ws": "WS-01", "duration": "x"}, 400),
            ({"action": "start", "ws": "WS-01", "duration": 0}, 400),
            ({"action": "start", "ws": "WS-01", "duration": 5, "op": "<script>"}, 400),
            ({"action": "revoke_keys", "ws": "ALL"}, 400),
            ({"action": "revoke_keys", "ws": "WS-77"}, 404),
            ({"action": "activate", "ws": "WS-77"}, 404),
        ]
        for body, expected in bad:
            self.assertEqual(self.admin(body, t)[0], expected, body)
        req_bad_json = self.http("POST", "/api/action", raw=b"{no es json",
                                 headers={"X-Forwarded-For": "203.0.113.7", "X-Admin-Token": t})
        self.assertEqual(req_bad_json[0], 400)
        self.assertEqual(self.admin({"action": "block", "ws": "WS-02"}, t)[0], 409, "Desconectada: no se bloquea")
        code, res = self.admin({"action": "start", "ws": "WS-02", "duration": 5}, t)
        self.assertEqual(code, 409)
        self.assertIn("desconectada", res["msg"])
        self.assertEqual(self.admin({"action": "block", "ws": "WS-01"}, t)[1]["station_status"], STATUS_OUT_OF_SERVICE)
        self.assertEqual(self.admin({"action": "activate", "ws": "WS-01"}, t)[1]["station_status"], STATUS_AVAILABLE)
        self.central.revoke_station_keys("WS-01")
        self.assertEqual(self.admin({"action": "activate", "ws": "WS-01"}, t)[0], 409, "Revocada: no se activa")


class ApiWeatherTests(ApiTestBase):
    ALERT = {"city": "Oslo", "temp": -4.0, "condition": "Snow", "alert": True, "reason": "Helada"}

    def test_05_weather_token_mandatory(self):
        with patch.dict(os.environ, {"WM_WEATHER_TOKEN": ""}):
            for method, path, body in (("PUT", "/api/weather/WS-01", self.ALERT),
                                       ("POST", "/api/weather/alert", dict(self.ALERT, ws_id="WS-01")),
                                       ("DELETE", "/api/weather/WS-01/alert", None)):
                self.assertEqual(self.weather(method, path, body, token="cualquiera")[0], 503, method)
        self.assertEqual(self.http("PUT", "/api/weather/WS-01", self.ALERT)[0], 401)
        self.assertEqual(self.weather("PUT", "/api/weather/WS-01", self.ALERT, token="incorrecto")[0], 401)
        self.assertNotIn("WS-01", self.central.weather_alerts, "Ninguna notificación anónima tiene efecto")
        self.assertEqual(self.weather("PUT", "/api/weather/WS-01", self.ALERT)[0], 200)
        self.assertIn("WS-01", self.central.weather_alerts)

    def test_06_weather_rest_validation_and_no_duplicates(self):
        key = self.make_available("WS-01")
        cases = [
            ("PUT", "/api/weather/WS-99", self.ALERT, 404),
            ("PUT", "/api/weather/XX", self.ALERT, 400),
            ("PUT", "/api/weather/WS-01", dict(self.ALERT, temp="abc"), 400),
            ("PUT", "/api/weather/WS-01", dict(self.ALERT, alert="yes"), 400),
            ("PUT", "/api/weather/WS-01", dict(self.ALERT, ws_id="WS-02"), 400),
            ("DELETE", "/api/weather/WS-01/alert", None, 404),  # aún no hay alerta
            ("PATCH", "/api/weather/WS-01", self.ALERT, 501),
        ]
        for method, path, body, expected in cases:
            code = self.weather(method, path, body)[0] if method != "PATCH" else self.http("PATCH", path)[0]
            if method == "PATCH":
                self.assertIn(code, (405, 501))
            else:
                self.assertEqual(code, expected, (method, path, body))
        self.assertEqual(self.http("PUT", "/api/weather/WS-01", raw=b"[1,2]",
                                   headers={"X-Weather-Token": self.weather_token})[0], 400)

        for _ in range(10):  # WM_W repite el estado cada 4 s
            self.assertEqual(self.weather("PUT", "/api/weather/WS-01", self.ALERT)[0], 200)
        self.assertEqual(self.central.stations["WS-01"]["status"], STATUS_OUT_OF_SERVICE)
        self.assertEqual(len(self.audits("WEATHER_ALERT", "WS-01")), 1, "Sin eventos duplicados")
        self.assertEqual([c["cmd"] for c in self.commands(key)], ["BLOCK"], "Una sola orden de parada")

        self.assertEqual(self.weather("DELETE", "/api/weather/WS-01/alert")[0], 200)
        self.assertEqual(self.central.stations["WS-01"]["status"], STATUS_AVAILABLE)
        self.assertEqual(self.weather("DELETE", "/api/weather/WS-01/alert")[0], 404)
        for _ in range(5):
            self.weather("PUT", "/api/weather/WS-01", dict(self.ALERT, temp=12.0, alert=False, condition="Clear"))
        self.assertEqual(len(self.audits("WEATHER_NORMAL", "WS-01")), 1)
        self.assertEqual([c["cmd"] for c in self.commands(key)], ["BLOCK", "ACTIVATE"])
        # Compatibilidad: POST /api/weather/alert con token
        self.assertEqual(self.weather("POST", "/api/weather/alert", dict(self.ALERT, ws_id="WS-01"))[0], 200)
        self.assertIn("WS-01", self.http("GET", "/api/weather")[1]["weather_alerts"])

    def test_07_repeated_alerts_during_watering_finish_normally_once(self):
        key = self.make_available("WS-01")
        with self.central.lock:
            self.central.stations["WS-01"].update(status=STATUS_WATERING, operator="FO-01", transaction_id="TX-W")
        for _ in range(6):
            self.weather("PUT", "/api/weather/WS-01", self.ALERT)
        st = self.central.stations["WS-01"]
        self.assertEqual(st["status"], STATUS_WATERING, "El riego en curso finaliza con normalidad")
        self.assertTrue(st.get("pending_weather_stop"))
        stops = [c for c in self.commands(key) if c["cmd"] == "STOP"]
        self.assertEqual(len(stops), 1)
        self.assertEqual(stops[0]["reason"], "WEATHER_ALERT")
        self.assertEqual(len(self.audits("WEATHER_ALERT", "WS-01")), 1)


class RegistryStatusTests(ApiTestBase):
    def test_08_registration_state_from_registry_https(self):
        reg_port = free_port()
        cert, keyf = os.path.join(self.tmp, "reg_cert.pem"), os.path.join(self.tmp, "reg_key.pem")
        reg = WMRegistry(port=reg_port, db_path=os.path.join(self.tmp, "registry.db"), cert_file=cert, key_file=keyf)
        threading.Thread(target=reg.run_server, daemon=True).start()
        self.assertTrue(wait_until(lambda: os.path.isfile(cert), 10))
        credential = reg.db.register_station("WS-01", "Parque")
        self.central.registry_url = f"https://127.0.0.1:{reg_port}"
        self.central.registry_ca = cert

        self.assertIsNone(self.http("GET", "/api/stations/WS-01")[1]["registry_registered"], "Aún sin consultar")
        self.assertTrue(wait_until(self.central.refresh_registry_status, 10))
        st1, st2 = self.http("GET", "/api/stations/WS-01")[1], self.http("GET", "/api/stations/WS-02")[1]
        self.assertEqual((st1["registry_registered"], st2["registry_registered"]), (True, False))
        self.assertTrue(st1["registry_available"])
        status = self.http("GET", "/api/status")[1]
        self.assertTrue(next(s for s in status["stations"] if s["id"] == "WS-01")["registry_registered"])
        cred = credential["credential"]
        self.assertTrue(cred.startswith("CRED-"))
        for path in ("/api/status", "/api/stations"):
            self.assertNotIn("CRED-", json.dumps(self.http("GET", path)[1]), "Sin credenciales en la API")
        self.assertNotIn(cred, json.dumps(self.http("GET", "/api/status")[1]))

        # Registry no accesible: estado desconocido, la API sigue respondiendo
        self.central.registry_url = f"https://127.0.0.1:{free_port()}"
        self.assertFalse(self.central.refresh_registry_status())
        code, st1 = self.http("GET", "/api/stations/WS-01")
        self.assertEqual(code, 200)
        self.assertEqual((st1["registry_registered"], st1["registry_available"]), (None, False))

        # Certificado no válido (CA distinta): tampoco se acepta
        self.central.registry_url = f"https://127.0.0.1:{reg_port}"
        other = os.path.join(self.tmp, "otro")
        os.makedirs(other)
        from wm_crypto import ensure_tls_certificate
        ensure_tls_certificate(os.path.join(other, "c.pem"), os.path.join(other, "k.pem"), "X", ["127.0.0.1"])
        self.central.registry_ca = os.path.join(other, "c.pem")
        self.assertFalse(self.central.refresh_registry_status())


class WeatherOfficeTests(ApiTestBase):
    def _wco(self, base, token):
        cities = os.path.join(self.tmp, "ciudades.json")
        with open(cities, "w") as f:
            json.dump({"WS-02": "Oslo"}, f)
        w = WeatherControlOffice(base, cities_file=cities, interval=0.3, weather_token=token)
        w.log = lambda msg: None
        return w

    def test_09_wm_w_sends_state_every_cycle_and_recovers_after_central_restart(self):
        self.make_available("WS-02")
        wco = self._wco(self.base, self.weather_token)
        wco.manual_overrides["WS-02"] = {"override": True, "temp": -5.0, "rain": False}
        threading.Thread(target=wco.run_monitoring_loop, daemon=True).start()
        try:
            self.assertTrue(wait_until(lambda: "WS-02" in self.central.weather_alerts, 3))
            self.assertEqual(self.central.stations["WS-02"]["status"], STATUS_OUT_OF_SERVICE)
            time.sleep(1.2)  # varios ciclos con el mismo estado
            self.assertEqual(len(self.audits("WEATHER_ALERT", "WS-02")), 1)

            # Reinicio de Central (misma BD): la alerta se recupera en el siguiente ciclo
            self.central.running = False
            central2, base2 = self._start_central()
            self.assertNotIn("WS-02", central2.weather_alerts)
            wco.central_api_url = base2
            self.assertTrue(wait_until(lambda: "WS-02" in central2.weather_alerts, 3),
                            "WM_W debe reenviar el estado aunque no cambie")

            # Clima favorable: DELETE de la alerta + estado actual
            wco.manual_overrides["WS-02"] = {"override": True, "temp": 15.0, "rain": False}
            self.assertTrue(wait_until(lambda: "WS-02" not in central2.weather_alerts, 3))
            self.assertEqual(len([a for a in central2.recent_audits if a["action"] == "WEATHER_NORMAL"]), 1)
        finally:
            wco.running = False

    def test_10_wm_w_without_valid_token_is_rejected(self):
        wco = self._wco(self.base, "incorrecto")
        wco.manual_overrides["WS-02"] = {"override": True, "temp": -5.0, "rain": False}
        self.assertFalse(wco.notify_central(wco.analyze_weather("WS-02", "Oslo")))
        self.assertFalse(wco.central_ok)
        self.assertNotIn("WS-02", self.central.weather_alerts)



class WeatherCancellationTests(ApiTestBase):
    ALERT = {"city": "Oslo", "temp": -4.0, "condition": "Snow", "alert": True, "reason": "Helada"}

    def _alert_then_cancel(self, ws):
        self.assertEqual(self.weather("PUT", f"/api/weather/{ws}", self.ALERT)[0], 200)
        self.assertEqual(self.http("GET", f"/api/stations/{ws}")[1]["status"], STATUS_OUT_OF_SERVICE)
        self.assertEqual(self.weather("DELETE", f"/api/weather/{ws}/alert")[0], 200)
        return self.http("GET", f"/api/stations/{ws}")[1]   # lo que ve el Front a través de la API

    def test_11_cancellation_only_restores_connected_operational_stations(self):
        keys = {ws: self.make_available(ws) for ws in ("WS-01", "WS-02", "WS-03", "WS-04")}
        with self.central.lock:
            self.central.stations["WS-02"]["engine_ok"] = False                # Engine no operativo
            self.central.station_sockets.pop("WS-03", None)                     # Monitor desconectado
            self.central.station_keys.pop("WS-04", None)                        # sin clave vigente
        self.assertEqual(self._alert_then_cancel("WS-01")["status"], STATUS_AVAILABLE)
        self.assertEqual(self._alert_then_cancel("WS-02")["status"], "FUGA")
        self.assertEqual(self._alert_then_cancel("WS-03")["status"], "DESCONECTADA")
        self.assertEqual(self._alert_then_cancel("WS-04")["status"], STATUS_OUT_OF_SERVICE)
        for ws, key in keys.items():
            cmds = [c["cmd"] for c in self.commands(key, ws)]
            self.assertNotIn("START", cmds, "La cancelación nunca inicia un riego")
            self.assertEqual(cmds.count("ACTIVATE"), 1 if ws in ("WS-01", "WS-02") else 0, ws)
        self.assertEqual(self.http("GET", "/api/waterings")[1], [])

    def test_12_manual_block_and_revocation_survive_cancellation(self):
        self.make_available("WS-01")
        self.make_available("WS-02")
        self.central.block_station("WS-01")
        self.central.revoke_station_keys("WS-02")
        for ws in ("WS-01", "WS-02"):
            self.assertEqual(self._alert_then_cancel(ws)["status"], STATUS_OUT_OF_SERVICE, ws)
        self.assertTrue(self.http("GET", "/api/stations/WS-02")[1]["keys_revoked"])

    def test_13_coherent_after_central_restart(self):
        self.make_available("WS-01")
        self.weather("PUT", "/api/weather/WS-01", self.ALERT)
        self.central.running = False
        central2, base2 = self._start_central()        # reinicio: todas las WS empiezan DESCONECTADAS
        self.central = central2
        self.assertEqual(self.http("GET", "/api/stations/WS-01", base=base2)[1]["status"], "DESCONECTADA")
        hdr = {"X-Weather-Token": self.weather_token}
        self.assertEqual(self.http("PUT", "/api/weather/WS-01", self.ALERT, hdr, base=base2)[0], 200)  # WM_W la reenvía
        self.assertEqual(self.http("GET", "/api/stations/WS-01", base=base2)[1]["status"], STATUS_OUT_OF_SERVICE)
        self.assertEqual(self.http("DELETE", "/api/weather/WS-01/alert", headers=hdr, base=base2)[0], 200)
        self.assertEqual(self.http("GET", "/api/stations/WS-01", base=base2)[1]["status"], "DESCONECTADA",
                         "Sin Monitor conectado tras el reinicio no vuelve a DISPONIBLE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
