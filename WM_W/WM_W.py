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
  - Carga y modificación dinámica de ciudades asociadas a las WS (mediante ciudades.json o menú).
  - Menú interactivo en tiempo real para modificar ciudades o simular climas adversos/favorables.
"""

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Any, Optional

DEFAULT_API_KEY = "demo_key"  # El alumno o profesor puede introducir su clave gratuita de OpenWeather


class WeatherControlOffice:
    def __init__(self, central_api_url: str, cities_file: str = "ciudades.json",
                 api_key: Optional[str] = None, interval: float = 4.0, weather_token: Optional[str] = None):
        self.central_api_url = central_api_url.rstrip("/")
        # Secreto compartido con Central para autenticar las notificaciones (nunca en el repositorio)
        self.weather_token = (weather_token if weather_token is not None
                              else os.getenv("WM_WEATHER_TOKEN", "")).strip()
        self.central_ok: Optional[bool] = None
        self.cities_file = cities_file
        self.api_key = api_key or os.getenv("OPENWEATHER_API_KEY", "")
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
    def load_cities(self):
        """Carga las ciudades asociadas a cada estación desde el archivo JSON."""
        with self.lock:
            # Buscar en directorio local o en ruta relativa
            file_path = self.cities_file
            if not os.path.isabs(file_path):
                base_dir = os.path.dirname(os.path.abspath(__file__))
                file_path = os.path.join(base_dir, self.cities_file)

            if os.path.isfile(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        self.cities = json.load(f)
                    self.log(f"Ciudades cargadas correctamente desde {file_path}: {self.cities}")
                    return
                except Exception as e:
                    self.log(f"Error al leer {file_path}: {e}")

            # Valores por defecto si el archivo no existe
            self.cities = {
                "WS-01": "Madrid",
                "WS-02": "Seville",
                "WS-03": "Valencia",
                "WS-04": "London",
                "WS-05": "Helsinki"
            }
            self.save_cities()

    def save_cities(self):
        """Guarda la configuración actual de ciudades en el archivo JSON."""
        with self.lock:
            file_path = self.cities_file
            if not os.path.isabs(file_path):
                base_dir = os.path.dirname(os.path.abspath(__file__))
                file_path = os.path.join(base_dir, self.cities_file)
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(self.cities, f, indent=2, ensure_ascii=False)
            except Exception as e:
                self.log(f"Aviso: no se pudo guardar {file_path}: {e}")

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
    def fetch_openweather(self, city: str) -> Optional[Dict[str, Any]]:
        """Consulta el endpoint Current weather data de OpenWeather para una ciudad."""
        encoded_city = urllib.parse.quote(city)
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?q={encoded_city}&appid={self.api_key}&units=metric"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "WaterManagement-WM_W/2.0"})
            with urllib.request.urlopen(req, timeout=3.5) as resp:
                if resp.status == 200:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw)
        except urllib.error.HTTPError as he:
            # 401 si la API key no está activa o 404 si la ciudad no existe
            return {"error": f"HTTP {he.code}", "detail": str(he)}
        except Exception as e:
            return {"error": "NetworkError", "detail": str(e)}
        return None

    def analyze_weather(self, ws_id: str, city: str) -> Dict[str, Any]:
        """
        Determina temperatura y si hay condiciones adversas (temp < 0 ºC o lluvia).
        Soporta simulación manual y consulta a OpenWeather.
        """
        with self.lock:
            override = self.manual_overrides.get(ws_id)
            if override is not None:
                temp = float(override.get("temp", 20.0))
                is_raining = bool(override.get("rain", False))
                cond = "Rain" if is_raining else ("Clear" if temp >= 0 else "Snow")
                is_adverse = (temp < 0.0) or is_raining
                reason = []
                if temp < 0.0:
                    reason.append(f"Temperatura de congelación ({temp:.1f}ºC)")
                if is_raining:
                    reason.append("Lluvia detectada (precipitación activa)")
                reason_str = " y ".join(reason) if reason else "Clima favorable"
                return {
                    "ws_id": ws_id,
                    "city": city,
                    "temp": temp,
                    "condition": cond,
                    "alert": is_adverse,
                    "reason": reason_str,
                    "source": "SIMULACIÓN MANUAL"
                }

        # Consulta real a OpenWeather
        res = self.fetch_openweather(city)
        if not res or "error" in res:
            # Si la API falla por límite o clave de prueba, aplicamos estimación según ciudad
            # para no interrumpir la ejecución del alumno
            default_temps = {"madrid": 15.0, "seville": 22.0, "valencia": 18.0, "london": 10.0, "helsinki": -2.0}
            est_temp = default_temps.get(city.lower(), 12.0)
            is_adverse = (est_temp < 0.0)
            cond = "Snow" if est_temp < 0 else "Clear"
            reason = f"Temperatura de congelación ({est_temp:.1f}ºC)" if is_adverse else "Clima favorable"
            return {
                "ws_id": ws_id,
                "city": city,
                "temp": est_temp,
                "condition": cond,
                "alert": is_adverse,
                "reason": reason,
                "source": f"FALLBACK ({res.get('error', 'Sin conexión') if res else 'Timeout'})"
            }

        try:
            main_data = res.get("main", {})
            temp = float(main_data.get("temp", 20.0))
            weather_list = res.get("weather", [{}])
            cond = weather_list[0].get("main", "Clear")
            desc = weather_list[0].get("description", "")

            # Criterio del enunciado: temperatura bajo cero (temp < 0ºC) o lluvia
            rain_conditions = ("Rain", "Drizzle", "Thunderstorm", "Squall")
            has_rain = (cond in rain_conditions) or ("rain" in res)
            is_freezing = (temp < 0.0)
            is_adverse = is_freezing or has_rain

            reasons = []
            if is_freezing:
                reasons.append(f"Temperatura bajo cero ({temp:.1f}ºC)")
            if has_rain:
                reasons.append(f"Precipitaciones ({desc or cond})")

            return {
                "ws_id": ws_id,
                "city": city,
                "temp": temp,
                "condition": cond,
                "alert": is_adverse,
                "reason": " y ".join(reasons) if reasons else "Clima favorable",
                "source": "OPENWEATHER API"
            }
        except Exception as e:
            return {
                "ws_id": ws_id,
                "city": city,
                "temp": 15.0,
                "condition": "Clear",
                "alert": False,
                "reason": f"Error parseando datos: {e}",
                "source": "ERROR"
            }

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

    def _report_delivery(self, code: int):
        """Informa del estado de entrega solo cuando cambia (sin repetir el aviso cada 4 s)."""
        ok = code in (200, 201, 204)
        if ok != self.central_ok:
            if ok:
                self.log(f"Comunicación con API_Central correcta ({self.central_api_url}).")
            elif code in (401, 503):
                self.log("API_Central RECHAZA las notificaciones: revise WM_WEATHER_TOKEN en WM_W y en Central.")
            elif code == 0:
                self.log(f"Aviso: API_Central no accesible ({self.central_api_url}). Se reintenta en cada ciclo.")
            else:
                self.log(f"Aviso: API_Central respondió HTTP {code} a la notificación climática.")
        self.central_ok = ok

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
        self._report_delivery(code)
        return code in (200, 201, 204)

    def cancel_alert(self, ws_id: str) -> bool:
        """Cancela la alerta climática de una WS (DELETE /api/weather/<WS>/alert).
        404 significa que Central ya no tenía la alerta activa: se considera cancelada."""
        code = self._central_request("DELETE", f"/api/weather/{urllib.parse.quote(ws_id)}/alert")
        if code == 404:
            return True
        self._report_delivery(code)
        return code in (200, 204)

    # --------------------------------------------------------------------------
    # BUCLE PRINCIPAL DE MONITORIZACIÓN METEOROLÓGICA (CADA 4 SEGUNDOS)
    # --------------------------------------------------------------------------
    def run_monitoring_loop(self):
        self.log(f"Iniciando ciclo meteorológico. Sondeo a OpenWeather cada {self.interval}s.")
        self.log(f"API Central configurada en: {self.central_api_url}")

        while self.running:
            start_time = time.time()
            with self.lock:
                current_cities = dict(self.cities)

            for ws_id, city in current_cities.items():
                if not self.running:
                    break
                w_info = self.analyze_weather(ws_id, city)
                prev_state = self.weather_state.get(ws_id)
                self.weather_state[ws_id] = w_info

                # Cambios del estado de alerta (para el log y la cancelación explícita)
                prev_alert = prev_state.get("alert") if prev_state else None
                current_alert = w_info["alert"]
                alert_changed = (prev_alert != current_alert)

                if alert_changed:
                    if current_alert:
                        self.log(f"¡ALERTA CLIMÁTICA DETECTADA! [{ws_id} - {city}]: {w_info['reason']}. Notificando a Central...")
                    else:
                        self.log(f"Clima normalizado [{ws_id} - {city}]: {w_info['temp']}ºC, {w_info['condition']}. Notificando cancelación a Central...")

                # Cancelación explícita de la alerta al volver el clima favorable
                if prev_alert is True and not current_alert:
                    self.cancel_alert(ws_id)

                # Estado climático actual en cada ciclo (aunque no cambie): Central recupera las alertas
                # tras reiniciarse; las notificaciones repetidas no generan eventos duplicados
                self.notify_central(w_info)

            # Dormir hasta completar el intervalo de 4 segundos
            elapsed = time.time() - start_time
            sleep_time = max(0.2, self.interval - elapsed)
            time.sleep(sleep_time)

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
                            print(f"  • {ws_id:<7} | Ciudad: {city:<12} | Temp: {temp}ºC | Clima: {cond:<10} | {status_badge} ({reason}) [{src}]")
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
