FROM python:3.12-slim

WORKDIR /app

# Instalación de dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia de ficheros del proyecto respetando la estructura de carpetas
COPY common.py ./
COPY wm_crypto.py ./
COPY WM_Central/ ./WM_Central/
COPY WM_Registry/ ./WM_Registry/
COPY WM_W/ ./WM_W/
COPY WM_WS/ ./WM_WS/
COPY WM_FO/ ./WM_FO/

# Por defecto arranca WM_Central en el puerto de sockets 5000 y web en 8080 (o $PORT)
ENV PYTHONPATH=/app
ENV DB_PATH=/data/central.db
VOLUME ["/data"]

EXPOSE 5000 8080

# El certificado TLS de Central se guarda en el volumen /data para que no cambie entre reinicios
CMD ["python", "WM_Central/WM_Central.py", "5000", "kafka:19092", "--web-port", "8080", "--db", "/data/central.db", "--cert", "/data/certs/central_cert.pem", "--key", "/data/certs/central_key.pem"]
