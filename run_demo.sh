#!/usr/bin/env bash
# Script interactivo de ayuda para arrancar módulos de prueba (Release 1 & Release 2)

export WM_ADMIN_TOKEN="${WM_ADMIN_TOKEN:-admin-2627}"
export WM_WEATHER_TOKEN="${WM_WEATHER_TOKEN:-weather-2627}"
PYTHON_BIN=".venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python3"
fi

echo "=================================================================="
echo " WaterManagement — Lanzador Rápido de Módulos (Release 2)"
echo " Tokens activos: WM_ADMIN_TOKEN=$WM_ADMIN_TOKEN | WM_WEATHER_TOKEN=$WM_WEATHER_TOKEN"
echo "=================================================================="
echo " 1) Levantar broker Apache Kafka (Docker)"
echo " 2) Lanzar WM_Registry (HTTPS REST API :5500)"
echo " 3) Lanzar WM_Central (Sockets TLS :5000, API_Central :8080)"
echo " 4) Lanzar Front Web Node.js (Dashboard :3000 -> API_Central :8080)"
echo " 5) Lanzar WM_W - Oficina Meteorológica (OpenWeather -> :8080)"
echo " 6) Lanzar Monitor WS-01 (Engine :6001, Central :5000, Registry :5500)"
echo " 7) Lanzar Engine WS-01 (Kafka :9092, Monitor :6001)"
echo " 8) Lanzar Field Operator FO-01 (Modo interactivo)"
echo " 9) Lanzar Field Operator FO-01 (Modo lotes con operaciones.txt)"
echo "10) Levantar stack Cloud completo con Docker Compose (Kafka+Registry+Central+Front)"
echo "11) Parar contenedores Docker"
echo "=================================================================="
read -p "Selecciona una opción [1-11]: " opcion

case $opcion in
  1)
    docker compose up -d kafka
    echo "Kafka en marcha en localhost:9092"
    ;;
  2)
    $PYTHON_BIN WM_Registry/WM_Registry.py 5500
    ;;
  3)
    $PYTHON_BIN WM_Central/WM_Central.py 5000 localhost:9092 --web-port 8080
    ;;
  4)
    node front/server.js 3000 http://localhost:8080
    ;;
  5)
    $PYTHON_BIN WM_W/WM_W.py http://localhost:8080
    ;;
  6)
    $PYTHON_BIN WM_WS/WM_WS_M.py 6001 localhost:5000 WS-01 --loc "River Park Norte" --registry https://localhost:5500
    ;;
  7)
    $PYTHON_BIN WM_WS/WM_WS_E.py localhost:9092 localhost:6001
    ;;
  8)
    $PYTHON_BIN WM_FO/WM_FO.py localhost:9092 FO-01
    ;;
  9)
    $PYTHON_BIN WM_FO/WM_FO.py localhost:9092 FO-01 --file WM_FO/operaciones.txt
    ;;
  10)
    docker compose up -d
    echo "Servicios Cloud activos. Dashboard Front en http://localhost:3000 (Token Admin: $WM_ADMIN_TOKEN)"
    ;;
  11)
    docker compose down
    echo "Contenedores detenidos."
    ;;
  *)
    echo "Opción no válida."
    ;;
esac
