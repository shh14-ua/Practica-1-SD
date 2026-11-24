#!/usr/bin/env python3
"""
Pruebas del bloque 8 (Release 2): WM_W, OpenWeather e integración con API_Central.

Tipo de prueba:
  - SIMULADO OpenWeather: servidor HTTP local que responde como 'Current weather data'
    (OPENWEATHER_URL apunta a él). WM_W, API_Central (HTTP) y SQLite son reales; Kafka simulado.
  - REAL OpenWeather (clase OpenWeatherRealTests): solo si OPENWEATHER_API_KEY está definida en el
    entorno; hace una única consulta por ciudad y nunca muestra la clave. Si no, se omite.

Comprueba:
  1. Detección: lluvia (Rain, Drizzle, Thunderstorm, Squall o campo 'rain') y heladas (< 0 ºC).
  2. Fallos de OpenWeather (401, 404, 429, 500, sin respuesta, respuesta no válida): nunca se
     inventan datos ni se cancelan alertas; Central conserva el último estado; un aviso por cambio.
  3. Una consulta por ciudad distinta en cada ciclo, compartida entre sus WS; consultas concurrentes:
     una ciudad lenta no retrasa el ciclo.
  4. Cambios en ciudades.json aplicados sin reiniciar; menú de ciudades y simulación.
  5. Estado de entrega a Central independiente por WS (sin mensajes alternos en el log).
"""

import http.server
import io
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
import urllib.parse
import urllib.request
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT_DIR, os.path.join(ROOT_DIR, "WM_Central")):
    if p not in sys.path:
        sys.path.insert(0, p)

import WM_Central
from WM_W.WM_W import WeatherControlOffice
from common import STATUS_OUT_OF_SERVICE


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def ow_body(temp, main="Clear", description="", rain=False):
    body = {"weather": [{"main": main, "description": description}], "main": {"temp": temp}, "name": "X"}
    if rain:
        body["rain"] = {"1h": 0.4}
    return body


class FakeOpenWeather(http.server.BaseHTTPRequestHandler):
    """Servidor que imita a OpenWeather. responses[city] = (código, cuerpo, retardo)."""
    responses = {}
    requests = []
    keys = []

    def log_message(self, *a):
        pass

    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        city = q.get("q", [""])[0]
        FakeOpenWeather.requests.append(city)
        FakeOpenWeather.keys.append(q.get("appid", [""])[0])
        code, body, delay = FakeOpenWeather.responses.get(city, (404, {"cod": "404"}, 0))
        if delay:
            time.sleep(delay)
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        except OSError:
            pass


class WeatherTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wm_weather_")
        FakeOpenWeather.responses, FakeOpenWeather.requests, FakeOpenWeather.keys = {}, [], []
        self.ow = http.server.ThreadingHTTPServer(("127.0.0.1", free_port()), FakeOpenWeather)
        threading.Thread(target=self.ow.serve_forever, daemon=True).start()
        self.api_key = secrets.token_hex(16)
        self.weather_token = secrets.token_urlsafe(24)
        self.env = patch.dict(os.environ, {
            "OPENWEATHER_URL": f"http://127.0.0.1:{self.ow.server_address[1]}/data/2.5/weather",
            "WM_WEATHER_TOKEN": self.weather_token, "WM_AUDIT_LOG": os.path.join(self.tmp, "audit.log")})
        self.env.start()
        self.patches = [
            patch.object(WM_Central, "ensure_topics", lambda *a, **k: None),
            patch.object(WM_Central, "make_producer", lambda *a, **k: "FAKE"),
            patch.object(WM_Central, "send_kafka", lambda *a, **k: True),
        ]
        for p in self.patches:
            p.start()
        port = free_port()
        self.central = WM_Central.WMCentral(socket_port=0, kafka_bootstrap="fake:9092", web_port=port,
                                            db_path=os.path.join(self.tmp, "central.db"))
        threading.Thread(target=self.central.run_web_server, daemon=True).start()
        self.base = f"http://127.0.0.1:{port}"
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                urllib.request.urlopen(self.base + "/api/status", timeout=1)
                break
            except Exception:
                time.sleep(0.05)
        self.logs = []

    def tearDown(self):
        self.central.running = False
        self.central._web_server.shutdown()
        self.central._web_server.server_close()
        self.ow.shutdown()
        self.ow.server_close()
        for p in reversed(self.patches):
            p.stop()
        self.env.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def wco(self, cities, interval=4.0, api_key=None):
        path = os.path.join(self.tmp, "ciudades.json")
        with open(path, "w") as f:
            json.dump(cities, f)
        w = WeatherControlOffice(self.base, cities_file=path, interval=interval,
                                 api_key=self.api_key if api_key is None else api_key,
                                 weather_token=self.weather_token)
        w.log = self.logs.append
        w.ow_timeout = 0.8
        return w

    def status(self, ws):
        with urllib.request.urlopen(f"{self.base}/api/stations/{ws}") as r:
            return json.loads(r.read())


class DetectionTests(WeatherTestBase):
    def test_01_rain_and_freezing_detection_reaches_central(self):
        cases = {
            "Lluvia": (ow_body(12, "Rain", "light rain"), True),
            "Llovizna": (ow_body(12, "Drizzle"), True),
            "Tormenta": (ow_body(14, "Thunderstorm"), True),
            "Turbonada": (ow_body(14, "Squall"), True),
            "Nubes con lluvia": (ow_body(10, "Clouds", rain=True), True),
            "Helada": (ow_body(-0.1, "Clear"), True),
            "Nieve": (ow_body(-2, "Snow"), True),
            "Cero": (ow_body(0.0, "Clear"), False),
            "Despejado": (ow_body(18, "Clear"), False),
        }
        stations = {f"WS-{i + 1:02d}": city for i, city in enumerate(cases)}
        for ws in stations:
            if ws not in self.central.stations:
                self.central.register_station(ws, "x")
        for city, (body, _) in cases.items():
            FakeOpenWeather.responses[city] = (200, body, 0)
        self.wco(stations).run_cycle()
        for ws, city in stations.items():
            st = self.status(ws)
            self.assertEqual(bool(st["weather_alert"]), cases[city][1], city)
            self.assertEqual(st["weather_info"]["city"], city)
        self.assertNotIn(self.api_key, " ".join(self.logs))


class FailureTests(WeatherTestBase):
    def test_02_openweather_failures_never_invent_or_cancel(self):
        FakeOpenWeather.responses["Oslo"] = (200, ow_body(-5, "Snow"), 0)
        w = self.wco({"WS-01": "Oslo"})
        w.run_cycle()
        self.assertTrue(self.status("WS-01")["weather_alert"])
        self.assertEqual(self.central.stations["WS-01"]["status"], STATUS_OUT_OF_SERVICE)
        before = dict(self.central.weather_status["WS-01"])

        failures = [(401, {"cod": 401}, 0), (404, {"cod": "404"}, 0), (429, {"cod": 429}, 0),
                    (500, {"cod": 500}, 0), (200, b"no es json", 0),
                    (200, ow_body(15), 2.0),          # tarda más que el límite de espera
                    (200, {"weather": []}, 0)]        # sin temperatura
        for resp in failures:
            FakeOpenWeather.responses["Oslo"] = resp
            n = len(self.logs)
            w.run_cycle()
            w.run_cycle()   # el mismo fallo repetido no vuelve a avisar
            self.assertTrue(self.status("WS-01")["weather_alert"], f"Alerta cancelada por un fallo: {resp[0]}")
            self.assertEqual(self.central.weather_status["WS-01"]["temp"], before["temp"], "Sin datos inventados")
            self.assertEqual(len([l for l in self.logs[n:] if "sin dato válido" in l]), 1, resp[:1])
        self.assertTrue(any("límite de peticiones" in l for l in self.logs), "429 registrado como tal")
        self.assertTrue(any("no válida o ausente" in l for l in self.logs))
        self.assertNotIn(self.api_key, " ".join(self.logs), "La clave nunca aparece en los logs")

        FakeOpenWeather.responses["Oslo"] = (200, ow_body(15), 0)  # recuperación: clima real favorable
        w.run_cycle()
        self.assertFalse(self.status("WS-01")["weather_alert"])
        self.assertTrue(any("vuelve a responder" in l for l in self.logs))

    def test_03_without_api_key_nothing_is_sent(self):
        FakeOpenWeather.responses["Helsinki"] = (401, {"cod": 401}, 0)
        w = self.wco({"WS-05": "Helsinki"}, api_key="")
        w.run_cycle()
        w.run_cycle()
        self.assertNotIn("WS-05", self.central.weather_status, "Antes se enviaba una helada inventada (-2 ºC)")
        self.assertEqual(self.central.stations["WS-05"]["status"], "DESCONECTADA")
        # La simulación del menú sigue funcionando sin clave
        w.manual_overrides["WS-05"] = {"override": True, "temp": -3.0, "rain": False}
        w.run_cycle()
        self.assertTrue(self.status("WS-05")["weather_alert"])


class CycleTests(WeatherTestBase):
    def test_04_one_request_per_distinct_city_shared_by_its_stations(self):
        FakeOpenWeather.responses.update({"Madrid": (200, ow_body(20), 0), "Oslo": (200, ow_body(-1), 0)})
        w = self.wco({"WS-01": "Madrid", "WS-02": "madrid", "WS-03": "Oslo"})
        w.run_cycle()
        self.assertEqual(sorted(c.lower() for c in FakeOpenWeather.requests), ["madrid", "oslo"])
        self.assertEqual(self.status("WS-02")["weather_info"]["temp"], 20.0)
        self.assertEqual(self.status("WS-02")["weather_info"]["city"], "madrid")
        self.assertTrue(self.status("WS-03")["weather_alert"])

    def test_05_slow_city_does_not_block_the_cycle(self):
        FakeOpenWeather.responses.update({"Rapida": (200, ow_body(20), 0), "Lenta": (200, ow_body(-5), 3.0),
                                          "Otra": (200, ow_body(15, "Rain"), 0)})
        w = self.wco({"WS-01": "Rapida", "WS-02": "Lenta", "WS-03": "Otra"})
        t0 = time.monotonic()
        w.run_cycle()
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 2.0, f"Ciclo retenido por una ciudad lenta ({elapsed:.1f} s)")
        self.assertIn("WS-01", self.central.weather_status)
        self.assertTrue(self.status("WS-03")["weather_alert"])
        self.assertNotIn("WS-02", self.central.weather_status, "Sin respuesta a tiempo: no se envía nada")
        self.assertTrue(any("no respondió a tiempo" in l for l in self.logs))

    def test_06_four_second_loop_queries_every_cycle(self):
        FakeOpenWeather.responses["Madrid"] = (200, ow_body(20), 0)
        w = self.wco({"WS-01": "Madrid"}, interval=4.0)
        threading.Thread(target=w.run_monitoring_loop, daemon=True).start()
        try:
            time.sleep(9.0)
        finally:
            w.running = False
        self.assertIn(len(FakeOpenWeather.requests), (3, 4), "Una consulta cada 4 s")

    def test_07_cities_file_changes_applied_without_restart_and_menu(self):
        FakeOpenWeather.responses.update({"Madrid": (200, ow_body(20), 0), "Oslo": (200, ow_body(-3), 0),
                                          "Paris": (200, ow_body(11, "Rain"), 0)})
        w = self.wco({"WS-01": "Madrid"})
        w.run_cycle()
        self.assertFalse(self.status("WS-01")["weather_alert"])
        time.sleep(0.05)
        with open(w._cities_path(), "w") as f:
            json.dump({"WS-01": "Oslo", "WS-02": "Madrid"}, f)
        os.utime(w._cities_path(), (time.time() + 1, time.time() + 1))
        w.run_cycle()
        self.assertTrue(self.status("WS-01")["weather_alert"], "Nueva ciudad aplicada sin reiniciar")
        self.assertIn("WS-02", self.central.weather_status)
        with open(w._cities_path(), "w") as f:
            f.write("{ roto")
        os.utime(w._cities_path(), (time.time() + 2, time.time() + 2))
        w.run_cycle()
        w.run_cycle()
        self.assertEqual(w.cities, {"WS-01": "Oslo", "WS-02": "Madrid"}, "Fichero no válido: se mantiene")
        self.assertEqual(len([l for l in self.logs if "no es válido" in l]), 1)

        # Menú (lo que usará el profesor): cambio de ciudad y simulación
        with patch("sys.stdin", io.StringIO("c WS-01 Paris\nf WS-02 -4\n")), patch("builtins.print"):
            w.run_cli_menu()
        self.assertEqual(w.cities["WS-01"], "Paris")
        with open(w._cities_path()) as f:
            self.assertEqual(json.load(f)["WS-01"], "Paris")
        w.run_cycle()
        self.assertEqual(self.status("WS-01")["weather_info"]["city"], "Paris")
        self.assertTrue(self.status("WS-01")["weather_alert"])
        self.assertTrue(self.status("WS-02")["weather_alert"], "Helada simulada desde el menú")

    def test_08_delivery_state_per_station(self):
        FakeOpenWeather.responses["Madrid"] = (200, ow_body(20), 0)
        w = self.wco({"WS-01": "Madrid", "WS-99": "Madrid"})  # WS-99 no existe en Central (404)
        for _ in range(4):
            w.run_cycle()
        self.assertEqual(len([l for l in self.logs if "correcta para WS-01" in l]), 1)
        self.assertEqual(len([l for l in self.logs if "WS-99 no existe en WM_Central" in l]), 1,
                         "Sin mensajes alternos en cada ciclo")


@unittest.skipUnless(os.getenv("OPENWEATHER_API_KEY"), "OPENWEATHER_API_KEY no definida: prueba real omitida")
class OpenWeatherRealTests(unittest.TestCase):
    """REAL: una única consulta por ciudad a OpenWeather (no se fuerza ningún 429)."""

    def test_real_current_weather(self):
        with patch.dict(os.environ, {"OPENWEATHER_URL": ""}):
            w = WeatherControlOffice("http://127.0.0.1:1", cities_file=os.path.join(tempfile.mkdtemp(), "c.json"),
                                     weather_token="x")
        logs = []
        w.log = logs.append
        data, error = w.fetch_openweather("Madrid")
        self.assertIsNone(error, error)
        weather = w.interpret_openweather("WS-01", "Madrid", data)
        self.assertIsInstance(weather["temp"], float)
        self.assertEqual(weather["alert"], weather["temp"] < 0 or weather["condition"] in
                         ("Rain", "Drizzle", "Thunderstorm", "Squall") or "rain" in data)
        self.assertEqual(w.fetch_openweather("CiudadQueNoExiste-WM-12345")[1], "HTTP 404")
        self.assertNotIn(os.environ["OPENWEATHER_API_KEY"], " ".join(logs))


if __name__ == "__main__":
    unittest.main(verbosity=2)
