#!/usr/bin/env bash
# Script para ejecutar cualquiera de los módulos dentro de un contenedor Docker
# (Cumpliendo el requisito del enunciado: "El uso de la tecnología Docker es obligatorio para el despliegue de cualquier módulo")

IMAGE_NAME="wm_app"

# Comprobar si la imagen base existe, o construirla
if [[ "$(docker images -q $IMAGE_NAME 2> /dev/null)" == "" ]]; then
  echo "Construyendo imagen Docker '$IMAGE_NAME'..."
  docker build -t $IMAGE_NAME .
fi

MODO=$1
shift

TTY_FLAG="-i"
if [ -t 0 ]; then
  TTY_FLAG="-it"
fi

case $MODO in
  central)
    echo "Iniciando WM_Central en contenedor Docker..."
    docker run --rm $TTY_FLAG --network=host \
      -v "$(pwd)/data:/data" \
      -e WM_ADMIN_TOKEN="${WM_ADMIN_TOKEN:-admin-2627}" \
      -e WM_WEATHER_TOKEN="${WM_WEATHER_TOKEN:-weather-2627}" \
      -e WM_REGISTRY_CA="/data/certs/registry_cert.pem" \
      $IMAGE_NAME python WM_Central/WM_Central.py 5000 localhost:9092 --web-port 8080 --db /data/central.db \
      --cert /data/certs/central_cert.pem --key /data/certs/central_key.pem "$@"
    ;;
  registry)
    echo "Iniciando WM_Registry (HTTPS) en contenedor Docker..."
    docker run --rm $TTY_FLAG --network=host \
      -v "$(pwd)/data:/data" \
      $IMAGE_NAME python WM_Registry/WM_Registry.py 5500 --db /data/registry.db \
      --cert /data/certs/registry_cert.pem --key /data/certs/registry_key.pem "$@"
    ;;
  weather|wm_w)
    echo "Iniciando WM_W (Weather Control Office) en contenedor Docker..."
    docker run --rm $TTY_FLAG --network=host \
      -v "$(pwd)/WM_W:/app/WM_W" \
      -e WM_WEATHER_TOKEN="${WM_WEATHER_TOKEN:-weather-2627}" \
      -e OPENWEATHER_API_KEY="${OPENWEATHER_API_KEY:-}" \
      $IMAGE_NAME python WM_W/WM_W.py "$@"
    ;;
  front)
    echo "Iniciando Front (Node.js Dashboard) en contenedor Docker..."
    docker run --rm $TTY_FLAG --network=host \
      -v "$(pwd)/front:/app" -w /app \
      node:20-alpine node server.js "${1:-3000}" "${2:-http://localhost:8080}"
    ;;
  monitor)
    echo "Iniciando WM_WS_M (Monitor) en contenedor Docker..."
    # data/certs (solo lectura) contiene el certificado de Central copiado a esta máquina (--central-ca /data/certs/central_cert.pem)
    docker run --rm $TTY_FLAG --network=host -v "$(pwd)/data/certs:/data/certs:ro" $IMAGE_NAME python WM_WS/WM_WS_M.py "$@"
    ;;
  engine)
    echo "Iniciando WM_WS_E (Engine) en contenedor Docker..."
    # data/engine_state conserva entre reinicios el último número de secuencia de órdenes aceptado
    docker run --rm $TTY_FLAG --network=host -v "$(pwd)/data/engine_state:/data/engine_state" \
      -e WM_ENGINE_STATE_DIR=/data/engine_state $IMAGE_NAME python WM_WS/WM_WS_E.py "$@"
    ;;
  fo)
    echo "Iniciando WM_FO (Field Operator) en contenedor Docker..."
    docker run --rm $TTY_FLAG --network=host $IMAGE_NAME python WM_FO/WM_FO.py "$@"
    ;;
  *)
    echo "Uso: ./docker_run.sh [central | registry | weather | front | monitor | engine | fo] [argumentos...]"
    echo ""
    echo "Ejemplos:"
    echo "  ./docker_run.sh registry"
    echo "  ./docker_run.sh central"
    echo "  ./docker_run.sh front 3000 http://localhost:8080"
    echo "  ./docker_run.sh weather http://localhost:8080"
    echo "  ./docker_run.sh monitor 6001 localhost:5000 WS-01 --loc 'River Park' --central-ca /data/certs/central_cert.pem --registry-ca /data/certs/registry_cert.pem"
    echo "  ./docker_run.sh engine localhost:9092 localhost:6001"
    echo "  ./docker_run.sh fo localhost:9092 FO-01"
    echo "  ./docker_run.sh fo localhost:9092 FO-01 --file WM_FO/operaciones.txt"
    exit 1
    ;;
esac
