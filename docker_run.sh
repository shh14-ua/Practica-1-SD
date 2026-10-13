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

case $MODO in
  central)
    echo "Iniciando WM_Central en contenedor Docker..."
    docker run --rm -it --network=host \
      -v "$(pwd)/data:/data" \
      $IMAGE_NAME python WM_Central.py 5000 localhost:9092 --web-port 8080 --db /data/central.db "$@"
    ;;
  monitor)
    echo "Iniciando WM_WS_M (Monitor) en contenedor Docker..."
    docker run --rm -it --network=host $IMAGE_NAME python WM_WS_M.py "$@"
    ;;
  engine)
    echo "Iniciando WM_WS_E (Engine) en contenedor Docker..."
    docker run --rm -it --network=host $IMAGE_NAME python WM_WS_E.py "$@"
    ;;
  fo)
    echo "Iniciando WM_FO (Field Operator) en contenedor Docker..."
    docker run --rm -it --network=host $IMAGE_NAME python WM_FO.py "$@"
    ;;
  *)
    echo "Uso: ./docker_run.sh [central | monitor | engine | fo] [argumentos...]"
    echo ""
    echo "Ejemplos:"
    echo "  ./docker_run.sh central"
    echo "  ./docker_run.sh monitor 6001 localhost:5000 WS-01 --loc 'River Park'"
    echo "  ./docker_run.sh engine localhost:9092 localhost:6001"
    echo "  ./docker_run.sh fo localhost:9092 FO-01"
    echo "  ./docker_run.sh fo localhost:9092 FO-01 --file operaciones.txt"
    exit 1
    ;;
esac
