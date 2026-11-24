#!/usr/bin/env python3
"""
WM_W: Weather Control Office (Oficina de Control Meteorológico - Release 2).
Funcionalidades:
  - Consulta del clima a la API de OpenWeather para cada estación de riego (WS) cada 4 segundos.
  - Análisis de condiciones meteorológicas adversas: temperatura < 0 ºC o lluvia/precipitaciones.
  - Notificación de situaciones de alerta climática a WM_Central a través de API_Central.
  - Notificación de cancelación de alertas cuando el clima vuelve a ser favorable.
  - API_Central (REST): PUT /api/weather/<WS> con el estado climático actual en cada ciclo (permite a
    Central recuperar las alertas tras reiniciarse) y DELETE /api/weather/<WS>/alert al cancelarse una
    alerta. Cada petición se autentica con el secreto compartido WM_WEATHER_TOKEN (variable de entorno).
  - Carga y modificación dinámica de ciudades asociadas a las WS (menú o ciudades.json, que se relee
    automáticamente al modificarse, sin reiniciar).
  - Una consulta concurrente por ciudad distinta en cada ciclo; si OpenWeather falla no se inventan
    datos: Central conserva el último estado conocido.
  - Menú interactivo en tiempo real para modificar ciudades o simular climas adversos/favorables.
"""

import argparse
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Dict, Any, Optional

# Endpoint 'Current weather data' de OpenWeather (configurable solo para pruebas con un servidor simulado)
DEFAULT_OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"


class WeatherControlOffice:
    def __init__(self, central_api_url: str, cities_file: str = "ciudades.json",
                 api_key: Optional[str] = None, interval: float = 4.0, weather_token: Optional[str] = None):
        self.central_api_url = central_api_url.rstrip("/")
        # Secreto compartido con Central para autenticar las notificaciones (nunca en el repositorio)
        self.weather_token = (weather_token if weather_token is not None
                              else os.getenv("WM_WEATHER_TOKEN", "")).strip()
        self.central_ok: Optional[bool] = None
        self.central_ok_by_ws: Dict[str, bool] = {}
        self.cities_file = cities_file
        self.api_key = api_key or os.getenv("OPENWEATHER_API_KEY", "")
        self.openweather_url = os.getenv("OPENWEATHER_URL", DEFAULT_OPENWEATHER_URL).strip() or DEFAULT_OPENWEATHER_URL
        self.ow_timeout = 3.0              # espera máxima por consulta (dentro del ciclo de 4 s)
        self.ow_errors: Dict[str, str] = {}  # último fallo de OpenWeather por ciudad
        self._cities_mtime: Optional[float] = None
        self.interval = interval
        self.running = True
        self.lock = threading.RLock()

        # Mapeo {ws_id: ciudad}
        self.cities: Dict[str, str] = {}
        # Estado actual del clima por estación {ws_id: {temp, cond, alert, last_check}}
        self.weather_state: Dict[str, Dict[str, Any]] = {}
        # Modos de simulación manual por estación {ws_id: {"override": True, "temp": float, "rain": bool}}
        self.manual_overrides: Dict[str, Dict[str, Any]] = {}

        self.load_cities()

    def log(self, msg: str):
        print(f"[{time.strftime('%H:%M:%S')}] [WM_W] {msg}", flush=True)

    # --------------------------------------------------------------------------
    # GESTIÓN DEL FICHERO DE CIUDADES
    # --------------------------------------------------------------------------
    def _cities_path(self) -> str:
        file_path = self.cities_file
        if not os.path.isabs(file_path):
            file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.cities_file)
        return file_path

    @staticmethod
    def _valid_cities(data) -> Optional[Dict[str, str]]:
        """{"WS-01": "Madrid", ...} con identificadores y ciudades no vacíos; None si no es válido."""
        if not isinstance(data, dict):
            return None
        cities = {}
        for ws_id, city in data.items():
            if not isinstance(ws_id, str) or not isinstance(city, str) or not ws_id.strip() or not city.strip():
                return None
            cities[ws_id.strip().upper()] = city.strip()
        return cities

    def load_cities(self):
        """Carga las ciudades asociadas a cada estación desde el archivo JSON."""
        with self.lock:
            file_path = self._cities_path()
            if os.path.isfile(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        cities = self._valid_cities(json.load(f))
                    self._cities_mtime = os.path.getmtime(file_path)
                    if cities is not None:
                        self.cities = cities
                        self.log(f"Ciudades cargadas correctamente desde {file_path}: {self.cities}")
                        return
                    self.log(f"Error: {file_path} no tiene el formato {{\"WS-01\": \"Ciudad\", ...}}.")
                except Exception as e:
                    self.log(f"Error al leer {file_path}: {e.__class__.__name__}")
                if self.cities:
                    return  # se mantiene la configuración anterior

            # Valores por defecto si el archivo no existe
            self.cities = {
                "WS-01": "Madrid",
                "WS-02": "Seville",
                "WS-03": "Valencia",
                "WS-04": "London",
                "WS-05": "Helsinki"
            }
            self.save_cities()

    def reload_cities_if_changed(self):
        """Aplica sin reiniciar los cambios hechos a mano en el fichero de ciudades. Si el fichero no
        es válido se mantiene la configuración actual (avisando una sola vez por modificación)."""
        file_path = self._cities_path()
        try:
            mtime = os.path.getmtime(file_path)
        except OSError:
            return
        with self.lock:
            if mtime == self._cities_mtime:
                return
            self._cities_mtime = mtime
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    cities = self._valid_cities(json.load(f))
            except Exception:
                cities = None
            if cities is None:
                self.log(f"Aviso: {file_path} modificado pero no es válido; se mantienen las ciudades actuales.")
                return
            if cities == self.cities:
                return
            for ws_id, city in cities.items():
                if self.cities.get(ws_id) != city:
                    self.manual_overrides.pop(ws_id, None)  # nueva ciudad: datos reales
            self.cities = cities
            self.log(f"Fichero de ciudades modificado: nueva configuración {self.cities}")

    def save_cities(self):
        """Guarda la configuración actual de ciudades en el archivo JSON."""
        with self.lock:
            file_path = self._cities_path()
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(self.cities, f, indent=2, ensure_ascii=False)
                self._cities_mtime = os.path.getmtime(file_path)
            except Exception as e:
                self.log(f"Aviso: no se pudo guardar {file_path}: {e.__class__.__name__}")

    def set_station_city(self, ws_id: str, city: str):
        """Modifica la ciudad de una estación en tiempo real sin reiniciar el sistema."""
        clean_id = ws_id.strip().upper()
        clean_city = city.strip()
        with self.lock:
            self.cities[clean_id] = clean_city
            # Limpiamos override manual si existía
            self.manual_overrides.pop(clean_id, None)
            self.save_cities()
            self.log(f"Ubicación de {clean_id} actualizada a: '{clean_city}'")

    # --------------------------------------------------------------------------
    # CONSULTA A OPENWEATHER Y ANÁLISIS CLIMÁTICO
    # --------------------------------------------------------------------------
    # Causas de fallo de una consulta (nunca se sustituyen por datos inventados)
    OW_ERROR_TEXT = {
        "HTTP 401": "clave de OpenWeather no válida o ausente (OPENWEATHER_API_KEY)",
        "HTTP 404": "ciudad no encontrada en OpenWeather",
        "HTTP 429": "límite de peticiones del plan de OpenWeather superado",
        "SIN_RED": "OpenWeather no accesible (sin red o servicio caído)",
        "TIMEOUT": "OpenWeather no respondió a tiempo",
        "RESPUESTA_NO_VALIDA": "respuesta de OpenWeather no válida",
    }

    def fetch_openweather(self, city: str) -> tuple:
        """Consulta 'Current weather data' de OpenWeather para una ciudad.
        Devuelve (datos, None) o (None, causa del fallo). Nunca registra la URL (contiene la clave)."""
        url = (f"{self.openweather_url}?q={urllib.parse.quote(city)}"
               f"&appid={urllib.parse.quote(self.api_key)}&units=metric")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "WaterManagement-WM_W/2.0"})
            with urllib.request.urlopen(req, timeout=self.ow_timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return (data, None) if isinstance(data, dict) else (None, "RESPUESTA_NO_VALIDA")
        except urllib.error.HTTPError as he:
            return None, f"HTTP {he.code}"
        except (socket.timeout, TimeoutError):
            return None, "TIMEOUT"
        except urllib.error.URLError as ue:
            return None, "TIMEOUT" if isinstance(ue.reason, (socket.timeout, TimeoutError)) else "SIN_RED"
        except ValueError:
            return None, "RESPUESTA_NO_VALIDA"
        except Exception:
            return None, "SIN_RED"

    @staticmethod
    def interpret_openweather(ws_id: str, city: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clima a partir de la respuesta de OpenWeather (main.temp y weather[0].main / campo 'rain').
        Criterio del enunciado: alerta si la temperatura baja de 0 ºC o llueve. None si faltan datos."""
        try:
            temp = data["main"]["temp"]
            if isinstance(temp, bool) or not isinstance(temp, (int, float)):
                return None
            temp = float(temp)
            weather_list = data.get("weather") or [{}]
            cond = str(weather_list[0].get("main", "Clear"))
            desc = str(weather_list[0].get("description", ""))
        except (KeyError, TypeError, AttributeError, IndexError):
            return None
        has_rain = cond in ("Rain", "Drizzle", "Thunderstorm", "Squall") or "rain" in data
        is_freezing = temp < 0.0
        reasons = []
        if is_freezing:
            reasons.append(f"Temperatura bajo cero ({temp:.1f}ºC)")
        if has_rain:
            reasons.append(f"Precipitaciones ({desc or cond})")
        return {
            "ws_id": ws_id, "city": city, "temp": temp, "condition": cond,
            "alert": is_freezing or has_rain,
            "reason": " y ".join(reasons) if reasons else "Clima favorable",
            "source": "OPENWEATHER API",
        }

    def simulated_weather(self, ws_id: str, city: str) -> Optional[Dict[str, Any]]:
        """Clima simulado desde el menú (f / r / ok) para esa WS, si lo hay."""
        with self.lock:
            override = self.manual_overrides.get(ws_id)
        if override is None:
            return None
        temp = float(override.get("temp", 20.0))
        is_raining = bool(override.get("rain", False))
        cond = "Rain" if is_raining else ("Clear" if temp >= 0 else "Snow")
        reason = []
        if temp < 0.0:
            reason.append(f"Temperatura de congelación ({temp:.1f}ºC)")
        if is_raining:
            reason.append("Lluvia detectada (precipitación activa)")
        return {
            "ws_id": ws_id, "city": city, "temp": temp, "condition": cond,
            "alert": (temp < 0.0) or is_raining,
            "reason": " y ".join(reason) if reason else "Clima favorable",
            "source": "SIMULACIÓN MANUAL",
        }

    def analyze_weather(self, ws_id: str, city: str) -> Optional[Dict[str, Any]]:
        """Clima actual de una WS (simulado o de OpenWeather). None si no hay dato válido."""
        simulated = self.simulated_weather(ws_id, city)
        if simulated is not None:
            return simulated
        data, error = self.fetch_openweather(city)
        weather = self.interpret_openweather(ws_id, city, data) if data is not None else None
        self._report_openweather(city, error or (None if weather else "RESPUESTA_NO_VALIDA"))
        return weather

    def _report_openweather(self, city: str, error: Optional[str]):
        """Avisa de los fallos de OpenWeather por ciudad solo cuando cambian (sin saturar el log)."""
        key = city.strip().lower()
        with self.lock:
            previous = self.ow_errors.get(key)
            if error:
                self.ow_errors[key] = error
            else:
                self.ow_errors.pop(key, None)
        if error and error != previous:
            text = self.OW_ERROR_TEXT.get(error, f"error de OpenWeather ({error})")
            self.log(f"Aviso: sin dato válido de OpenWeather para '{city}': {text}. "
                     f"Central conserva el último estado conocido.")
        elif not error and previous:
            self.log(f"OpenWeather vuelve a responder para '{city}'.")

    # --------------------------------------------------------------------------
    # NOTIFICACIÓN A CENTRAL VÍA API_CENTRAL
    # --------------------------------------------------------------------------
    def _central_request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> int:
        """Petición autenticada a API_Central. Devuelve el código HTTP (0 si no hay conexión)."""
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(
            f"{self.central_api_url}{path}", data=data, method=method,
            headers={"Content-Type": "application/json", "X-Weather-Token": self.weather_token}
        )
        try:
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                return resp.status
        except urllib.error.HTTPError as he:
            return he.code
        except Exception:
            return 0

    def _report_delivery(self, ws_id: str, code: int):
        """Informa del estado de entrega de cada WS solo cuando cambia (sin repetir el aviso cada 4 s)."""
        ok = code in (200, 201, 204)
        with self.lock:
            previous = self.central_ok_by_ws.get(ws_id)
            self.central_ok_by_ws[ws_id] = ok
            self.central_ok = ok
        if ok == previous:
            return
        if ok:
            self.log(f"Comunicación con API_Central correcta para {ws_id} ({self.central_api_url}).")
        elif code in (401, 503):
            self.log(f"API_Central RECHAZA las notificaciones de {ws_id}: revise WM_WEATHER_TOKEN en WM_W y en Central.")
        elif code == 404:
            self.log(f"Aviso: {ws_id} no existe en WM_Central (HTTP 404). Revise el fichero de ciudades.")
        elif code == 0:
            self.log(f"Aviso: API_Central no accesible ({self.central_api_url}) para {ws_id}. Se reintenta en cada ciclo.")
        else:
            self.log(f"Aviso: API_Central respondió HTTP {code} a la notificación climática de {ws_id}.")

    def notify_central(self, weather_data: Dict[str, Any]) -> bool:
        """Comunica a WM_Central el estado climático actual de una WS (PUT /api/weather/<WS>)."""
        ws_id = str(weather_data.get("ws_id", "")).strip().upper()
        payload = {
            "ws_id": ws_id,
            "city": str(weather_data.get("city", ""))[:80],
            "temp": float(weather_data.get("temp", 0.0)),
            "condition": str(weather_data.get("condition", "Clear"))[:40],
            "alert": bool(weather_data.get("alert", False)),
            "reason": str(weather_data.get("reason", ""))[:200],
        }
        code = self._central_request("PUT", f"/api/weather/{urllib.parse.quote(ws_id)}", payload)
        self._report_delivery(ws_id, code)
        return code in (200, 201, 204)

    def cancel_alert(self, ws_id: str) -> bool:
        """Cancela la alerta climática de una WS (DELETE /api/weather/<WS>/alert).
        404 significa que Central ya no tenía la alerta activa: se considera cancelada."""
        code = self._central_request("DELETE", f"/api/weather/{urllib.parse.quote(ws_id)}/alert")
        if code == 404:
            return True
        self._report_delivery(ws_id, code)
        return code in (200, 204)

    # --------------------------------------------------------------------------
    # BUCLE PRINCIPAL DE MONITORIZACIÓN METEOROLÓGICA (CADA 4 SEGUNDOS)
    # --------------------------------------------------------------------------
    def _process_station(self, ws_id: str, city: str, weather: Optional[Dict[str, Any]]):
        """Aplica el clima de una WS: sin dato válido no se envía nada (Central conserva el último
        estado real); con dato, cancelación explícita si procede y estado actual a Central."""
        if weather is None:
            return
        with self.lock:
            prev_state = self.weather_state.get(ws_id)
            self.weather_state[ws_id] = dict(weather, observed_at=time.strftime("%H:%M:%S"))
        prev_alert = prev_state.get("alert") if prev_state else None
        current_alert = weather["alert"]
        if current_alert and prev_alert is not True:
            self.log(f"¡ALERTA CLIMÁTICA DETECTADA! [{ws_id} - {city}]: {weather['reason']}. Notificando a Central...")
        elif prev_alert is True and not current_alert:
            self.log(f"Clima normalizado [{ws_id} - {city}]: {weather['temp']}ºC, {weather['condition']}. Notificando cancelación a Central...")
        if prev_alert is True and not current_alert:
            self.cancel_alert(ws_id)
        # Estado climático actual en cada ciclo (aunque no cambie): Central recupera las alertas
        # tras reiniciarse; las notificaciones repetidas no generan eventos duplicados
        self.notify_central(weather)

    def run_cycle(self):
        """Un ciclo: una consulta concurrente por ciudad distinta (con límite de espera, una ciudad
        lenta no retiene a las demás); su resultado se comparte entre las WS de esa ciudad."""
        self.reload_cities_if_changed()
        with self.lock:
            stations = dict(self.cities)
            simulated = {ws for ws in stations if ws in self.manual_overrides}
        real_cities = {}
        for ws_id, city in stations.items():
            if ws_id not in simulated:
                real_cities.setdefault(city.strip().lower(), city)
        if not stations:
            return
        pool = ThreadPoolExecutor(max_workers=max(1, len(real_cities), len(stations)))
        try:
            futures = {key: pool.submit(self.fetch_openweather, city) for key, city in real_cities.items()}
            wait(list(futures.values()), timeout=self.ow_timeout + 0.5)
            results = {}
            for key, fut in futures.items():
                data, error = fut.result() if fut.done() else (None, "TIMEOUT")
                weather_any = self.interpret_openweather("", real_cities[key], data) if data is not None else None
                self._report_openweather(real_cities[key], error or (None if weather_any else "RESPUESTA_NO_VALIDA"))
                results[key] = weather_any
            jobs = []
            for ws_id, city in stations.items():
                if ws_id in simulated:
                    weather = self.simulated_weather(ws_id, city)
                else:
                    shared = results.get(city.strip().lower())
                    weather = dict(shared, ws_id=ws_id, city=city) if shared else None
                jobs.append(pool.submit(self._process_station, ws_id, city, weather))
            wait(jobs, timeout=max(1.0, self.interval))
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def run_monitoring_loop(self):
        self.log(f"Iniciando ciclo meteorológico. Sondeo a OpenWeather cada {self.interval}s.")
        self.log(f"API Central configurada en: {self.central_api_url}")
        while self.running:
            start_time = time.time()
            try:
                self.run_cycle()
            except Exception as exc:
                self.log(f"Aviso: error en el ciclo meteorológico ({exc.__class__.__name__}).")
            # Dormir hasta completar el intervalo de 4 segundos
            time.sleep(max(0.2, self.interval - (time.time() - start_time)))

    # --------------------------------------------------------------------------
    # MENÚ INTERACTIVO EN TIEMPO REAL (SEGÚN NOTA DE CORRECCIÓN DEL ENUNCIADO)
    # --------------------------------------------------------------------------
    def run_cli_menu(self):
        time.sleep(0.5)
        print("\n" + "=" * 65)
        print("  WM_W - Weather Control Office (Control Meteorológico)")
        print("  Comandos disponibles:")
        print("    s               -> Mostrar estado del clima actual de todas las WS")
        print("    c <WS> <ciudad> -> Modificar ciudad de una WS (ej: c WS-01 London)")
        print("    f <WS> [temp]   -> Simular helada/congelación (ej: f WS-01 -3)")
        print("    r <WS>          -> Simular lluvia (ej: r WS-01)")
        print("    ok <WS> [temp]  -> Simular clima favorable (ej: ok WS-01 22)")
        print("    k <api_key>     -> Actualizar API Key de OpenWeather")
        print("    q               -> Salir")
        print("=" * 65 + "\n")

        while self.running:
            try:
                line = sys.stdin.readline()
                if not line:
                    break
                parts = line.strip().split()
                if not parts:
                    continue
                cmd = parts[0].lower()

                if cmd == "s":
                    print(f"\n--- ESTADO DEL CLIMA EN TIEMPO REAL (Sondeo cada 4s) ---")
                    with self.lock:
                        for ws_id, city in self.cities.items():
                            st = self.weather_state.get(ws_id, {})
                            temp = st.get("temp", "--")
                            cond = st.get("condition", "--")
                            alert = st.get("alert", False)
                            reason = st.get("reason", "Sin datos")
                            src = st.get("source", "")
                            status_badge = "[ALERTA ADVERSA]" if alert else "[CLIMA OK]"
                            error = self.ow_errors.get(city.strip().lower())
                            extra = f" | SIN DATO ACTUAL: {self.OW_ERROR_TEXT.get(error, error)}" if error and ws_id not in self.manual_overrides else ""
                            print(f"  • {ws_id:<7} | Ciudad: {city:<12} | Temp: {temp}ºC | Clima: {cond:<10} | {status_badge} ({reason}) [{src} {st.get('observed_at', '')}]{extra}")
                    print()

                elif cmd == "c" and len(parts) >= 3:
                    ws_id = parts[1].upper()
                    new_city = " ".join(parts[2:])
                    self.set_station_city(ws_id, new_city)
                    print(f"[OK] Ciudad de {ws_id} modificada a '{new_city}'. Se consultará en el siguiente ciclo.")

                elif cmd == "f" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    freezing_temp = float(parts[2]) if len(parts) >= 3 else -3.0
                    with self.lock:
                        self.manual_overrides[ws_id] = {"override": True, "temp": freezing_temp, "rain": False}
                    print(f"[SIMULACIÓN] Helada activada para {ws_id} ({freezing_temp}ºC). Generando alerta...")

                elif cmd == "r" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    with self.lock:
                        self.manual_overrides[ws_id] = {"override": True, "temp": 12.0, "rain": True}
                    print(f"[SIMULACIÓN] Lluvia activada para {ws_id}. Generando alerta...")

                elif cmd == "ok" and len(parts) >= 2:
                    ws_id = parts[1].upper()
                    ok_temp = float(parts[2]) if len(parts) >= 3 else 20.0
                    with self.lock:
                        self.manual_overrides[ws_id] = {"override": True, "temp": ok_temp, "rain": False}
                    print(f"[SIMULACIÓN] Clima favorable restaurado para {ws_id} ({ok_temp}ºC, sin lluvia).")

                elif cmd == "k" and len(parts) >= 2:
                    self.api_key = parts[1].strip()
                    print(f"[OK] API Key de OpenWeather actualizada.")

                elif cmd == "q":
                    print("Cerrando WM_W...")
                    self.running = False
                    os._exit(0)
                else:
                    print("Comando no reconocido. Use 's', 'c <WS> <ciudad>', 'f <WS>', 'r <WS>', 'ok <WS>', 'q'.")
            except Exception as e:
                print(f"Error procesando comando: {e}")


def main():
    parser = argparse.ArgumentParser(description="WM_W: Weather Control Office (Release 2)")
    parser.add_argument("central_api", nargs="?", default="http://localhost:8080",
                        help="URL de API_Central (por defecto http://localhost:8080)")
    parser.add_argument("--cities", default="ciudades.json", help="Fichero JSON de ciudades")
    parser.add_argument("--key", default=None, help="API Key de OpenWeather")
    parser.add_argument("--interval", type=float, default=4.0, help="Intervalo de consulta en segundos (por defecto 4.0)")
    args = parser.parse_args()

    if not os.getenv("WM_WEATHER_TOKEN", "").strip():
        print("[WM_W] ERROR: falta la variable de entorno WM_WEATHER_TOKEN (secreto compartido con WM_Central). "
              "Sin ella Central rechaza todas las notificaciones climáticas.", file=sys.stderr)
        sys.exit(1)

    wco = WeatherControlOffice(
        central_api_url=args.central_api,
        cities_file=args.cities,
        api_key=args.key,
        interval=args.interval
    )

    t = threading.Thread(target=wco.run_monitoring_loop, daemon=True)
    t.start()

    if sys.stdin.isatty():
        wco.run_cli_menu()
    else:
        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()
