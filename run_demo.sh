#!/usr/bin/env bash
# Script interactivo de ayuda para arrancar módulos de prueba

echo "=========================================================="
echo " WaterManagement — Lanzador Rápido de Módulos"
echo "=========================================================="
echo "1) Levantar broker Apache Kafka (Docker)"
echo "2) Lanzar WM_Central (Sockets :5000, Web :8080)"
echo "3) Lanzar Monitor WS-01 (Engine :6001, Central :5000)"
echo "4) Lanzar Engine WS-01 (Kafka :9092, Monitor :6001)"
echo "5) Lanzar Field Operator FO-01 (Modo interactivo)"
echo "6) Lanzar Field Operator FO-01 (Modo lotes con operaciones.txt)"
echo "7) Parar contenedores Docker"
echo "=========================================================="
read -p "Selecciona una opción [1-7]: " opcion

case $opcion in
  1)
    docker compose up -d kafka
    echo "Kafka en marcha en localhost:9092"
    ;;
  2)
    .venv/bin/python WM_Central.py 5000 localhost:9092 --web-port 8080
    ;;
  3)
    .venv/bin/python WM_WS_M.py 6001 localhost:5000 WS-01 --loc "River Park Norte"
    ;;
  4)
    .venv/bin/python WM_WS_E.py localhost:9092 localhost:6001
    ;;
  5)
    .venv/bin/python WM_FO.py localhost:9092 FO-01
    ;;
  6)
    .venv/bin/python WM_FO.py localhost:9092 FO-01 --file operaciones.txt
    ;;
  7)
    docker compose down
    echo "Contenedores detenidos."
    ;;
  *)
    echo "Opción no válida."
    ;;
esac
