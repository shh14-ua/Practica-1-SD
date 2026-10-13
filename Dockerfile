FROM python:3.12-slim

WORKDIR /app

# Instalación de dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia de ficheros del proyecto
COPY *.py operaciones.txt dashboard.html ./

# Por defecto arranca WM_Central en el puerto de sockets 5000 y web en 8080 (o $PORT)
ENV DB_PATH=/data/central.db
VOLUME ["/data"]

EXPOSE 5000 8080

CMD ["python", "WM_Central.py", "5000", "kafka:19092", "--web-port", "8080", "--db", "/data/central.db"]
