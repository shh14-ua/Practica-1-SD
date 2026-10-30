# Práctica WaterManagement — Sistemas Distribuidos (Curso 26/27)

Sistema distribuido para la gestión y supervisión centralizada del riego inteligente en parques y jardines urbanos, desarrollado en **Python** utilizando **Sockets TCP** y **Apache Kafka**.

---

## 1. Arquitectura del Sistema

El sistema implementa la arquitectura técnica definida en la especificación (Figura 3):

```
                        ┌────────────────────────────────────────────────────────┐
                        │                      CORE SYSTEM                       │
                        │                                                        │
                        │  ┌──────────────┐             ┌─────────────────────┐  │
                        │  │  central.db  │◄──(SQLite)──┤     WM_Central      │  │
                        │  └──────────────┘             │ (Panel + Web + API) │  │
                        │                               └──────────┬──────────┘  │
                        └──────────────────────────────────────────┼─────────────┘
                                                                   │
                                     Sockets (<STX><DATA><ETX><LRC>)│  Kafka (Topics)
                                                                   │
                ┌──────────────────────────────────────────────────┴───────────────────────────────────────┐
                │                                                                                          │
                ▼                                                                                          ▼
┌───────────────────────────────┐                                                          ┌───────────────────────────────┐
│     WATERING STATION (WS)     │                                                          │     FIELD OPERATOR (FO)       │
│                               │                                                          │                               │
│  ┌─────────────────────────┐  │                                                          │  ┌─────────────────────────┐  │
│  │         WM_WS_M         │  │                                                          │  │          WM_FO          │  │
│  │        (Monitor)        │  │                                                          │  │   (App de Operario)     │  │
│  └────────────┬────────────┘  │                                                          │  └────────────┬────────────┘  │
│               │ Sockets       │                                                          │               │ Kafka         │
│  ┌────────────┴────────────┐  │                                                          │               │ (fo_requests, │
│  │         WM_WS_E         │◄─┼───────────────────────(Kafka: ws_commands / ws_events)─┼───────────────┘  fo_replies,  │
│  │     (Engine / Actuador) │  │                                                          │                  ws_status)   │
│  └─────────────────────────┘  │                                                          └───────────────────────────────┘
└───────────────────────────────┘
```

### Estructura del Repositorio

```text
Practica-1-SD/
├── WM_Central/
│   ├── WM_Central.py          # Módulo central (Sockets TCP + Kafka + SQLite + Web)
│   └── dashboard.html         # Cuadro de mandos web embebido
├── WM_WS/
│   ├── WM_WS_M.py             # Monitor de la estación de riego (Sockets TCP)
│   └── WM_WS_E.py             # Motor / actuador de la estación (Sockets TCP + Kafka)
├── WM_FO/
│   ├── WM_FO.py               # Aplicación del operario de campo (Kafka + CLI)
│   └── operaciones.txt        # Fichero de peticiones por lotes (--file)
├── prompts/
│   └── PROMPTS_IA.md          # Registro detallado de prompts utilizados con IA
├── common.py                  # Protocolo <STX><DATA><ETX><LRC>, helpers Kafka y colores ANSI
├── Dockerfile                 # Construcción de imagen Docker para todos los módulos
├── docker-compose.yml         # Despliegue de Apache Kafka y WM_Central
├── docker_run.sh              # Script de ejecución en contenedores Docker sin IDE
├── run_demo.sh                # Lanzador interactivo rápido desde terminal
├── test_watering_flow.py      # Batería de 6 pruebas automatizadas de flujo y resiliencia
├── requirements.txt           # Dependencias Python (kafka-python-ng)
├── Nombres.txt                # Autoras de la práctica
└── README.md                  # Documentación y guía de despliegue
```

### Componentes Software

1. **`WM_Central` (`WM_Central/WM_Central.py`)**:
   - Centraliza el gobierno y monitorización de toda la red.
   - Servidor de sockets TCP para autenticación y supervisión de los monitores de estación (`WM_WS_M`).
   - Productor y consumidor de Kafka para gestionar peticiones de operarios y órdenes a motores.
   - Base de datos relacional **SQLite** (`central.db`) para registro de operarios, estaciones e historial de riegos.
   - **Cuadro de mandos**: Panel con códigos de color ANSI en consola y servidor web embebido en tiempo real (`WM_Central/dashboard.html`) accesible vía navegador (según Figuras 1 y 2).

2. **`WM_WS_M` (`WM_WS/WM_WS_M.py`) — Monitor de la Estación**:
   - Conexión cliente por socket a `WM_Central` para registrar y autenticar la estación (`REGISTRO#<ID>#<UBICACION>`).
   - Servidor de socket para conectar con su motor (`WM_WS_E`) y asignarle su ID (`ASSIGN#<ID>`).
   - Envío de comprobación de salud cada segundo (`HEALTH#OK` / `HEALTH#NO_CENTRAL`).
   - Si detecta avería o el motor no responde, envía `LEAK#<ID>` a Central. Cuando se resuelve, envía `FIXED#<ID>`.
   - Envía latidos periódicos (`PING#<ID>`) para mantener activa la conexión con Central.

3. **`WM_WS_E` (`WM_WS/WM_WS_E.py`) — Motor y Actuador de la Estación**:
   - Conexión por socket al Monitor para recibir su ID asignado.
   - Conexión a Kafka para recibir órdenes (`ws_commands`) y publicar telemetría y eventos (`ws_events`).
   - Simula la electroválvula y caudalímetro (~8.5 a 11.5 L/min).
   - Genera eventos periódicos de telemetría cada segundo durante el riego.
   - **Simulación de avería**: La tecla `k` conmuta entre funcionamiento normal y KO (fuga simulada).
   - Permite iniciar riego manual (`s`) y detenerlo (`f`) desde la propia estación.

4. **`WM_FO` (`WM_FO/WM_FO.py`) — Aplicación del Operario de Campo**:
   - Conexión a Kafka para solicitar servicios de riego (`fo_requests`).
   - Recibe notificaciones de estado paso a paso (`STEP`), autorización (`AUTH`), denegación (`DENY`), telemetría en tiempo real y resumen final (`SUMMARY`).
   - Consulta el estado de todas las estaciones registradas (`ws_status`).
   - **Modo por lotes (`--file WM_FO/operaciones.txt`)**: Procesa las peticiones secuencialmente, esperando a la conclusión de cada servicio y **aguardando exactamente 4 segundos** antes de lanzar el siguiente.

5. **`common.py`**:
   - Protocolo de tramas por socket `<STX><DATA><ETX><LRC>` con cálculo byte a byte por XOR.
   - Handshake `ENQ` / `ACK`.
   - Conexiones y tópicos de Apache Kafka.
   - Constantes de estado y códigos ANSI de color.

---

## 2. Requisitos y Preparación del Entorno

### Dependencias
- Python 3.10 o superior (compatible con Python 3.12).
- Docker y Docker Compose.

### Instalación de librerías Python
```bash
# Crear entorno virtual (opcional pero recomendado)
python3 -m venv .venv
source .venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

---

## 3. Despliegue y Ejecución Local (Terminal o Docker sin IDE)

Todos los módulos pueden ejecutarse directamente desde la raíz del proyecto (o desde su propia carpeta) o bien mediante contenedores Docker con `./docker_run.sh`.

### Paso 1: Levantar Apache Kafka
En la raíz del proyecto, ejecuta:
```bash
docker compose up -d kafka
```
Para comprobar que Kafka está funcionando:
```bash
docker logs wm_kafka
```

### Paso 2: Iniciar la Central (`WM_Central`)
Abre una terminal y ejecuta:
```bash
python WM_Central/WM_Central.py 5000 localhost:9092 --web-port 8080
# O mediante Docker:
./docker_run.sh central
```
- Parámetros: `<puerto_sockets> <kafka_host:puerto> [--web-port 8080] [--db central.db]`
- Cuadro de mandos web disponible en `http://localhost:8080/`.

### Paso 3: Iniciar una Estación de Riego (`WM_WS_M` y `WM_WS_E`)

**Terminal 2 — Monitor de la Estación 1:**
```bash
python WM_WS/WM_WS_M.py 6001 localhost:5000 WS-01 --loc "River Park Norte"
# O mediante Docker:
./docker_run.sh monitor 6001 localhost:5000 WS-01 --loc "River Park Norte"
```
- Parámetros: `<puerto_engine> <central_host:puerto> <ID_WS> [--loc UBICACION]`

**Terminal 3 — Motor de la Estación 1:**
```bash
python WM_WS/WM_WS_E.py localhost:9092 localhost:6001
# O mediante Docker:
./docker_run.sh engine localhost:9092 localhost:6001
```
- Parámetros: `<kafka_host:puerto> <monitor_host:puerto>`

*(Puedes levantar tantas estaciones como desees cambiando los puertos y el ID, ej: `WS-02` con puerto 6002).*

### Paso 4: Iniciar la Aplicación del Operario (`WM_FO`)

**Terminal 4 — Operario en Modo Interactivo:**
```bash
python WM_FO/WM_FO.py localhost:9092 FO-01
# O mediante Docker:
./docker_run.sh fo localhost:9092 FO-01
```
- Parámetros: `<kafka_host:puerto> <ID_operario>`

**Terminal 4 (Alternativa) — Operario en Modo por Lotes:**
```bash
python WM_FO/WM_FO.py localhost:9092 FO-01 --file WM_FO/operaciones.txt
# O mediante Docker:
./docker_run.sh fo localhost:9092 FO-01 --file WM_FO/operaciones.txt
```

### Ejecución de las Pruebas Automatizadas Locales
Para verificar el recorrido completo de las solicitudes y los escenarios de tolerancia a fallos sin depender de servidores externos:
```bash
python test_watering_flow.py
```

---

## 4. Guía de Pruebas y Comprobación de Casos de Uso

Durante la evaluación de la práctica se comprobarán los siguientes escenarios:

### Caso 1: Riego normal autorizado
1. En la consola del operario (`FO-01`), escribe:
   ```
   r WS-01 10
   ```
2. Observarás cómo:
   - Central recibe la petición y notifica los pasos al operario.
   - El motor de `WS-01` abre la electroválvula y emite telemetría cada segundo (caudal y volumen acumulado).
   - Al cumplirse los 10 segundos, se cierra la electroválvula y se envía un resumen final (`SUMMARY`) al operario con el volumen total y duración.
   - La estación vuelve a estado `DISPONIBLE` y se registra en la base de datos SQLite.

### Caso 2: Denegación de riego (operario no registrado o estación no disponible)
1. Inicia un operario no dado de alta: `python WM_FO/WM_FO.py localhost:9092 FO-99`.
2. Solicita riego: `r WS-01 10`.
3. Central deniega la solicitud inmediatamente indicando que el operario no está registrado.
4. Si una estación ya está regando o en fuga, cualquier solicitud posterior sobre ella será denegada explicando el motivo.

### Caso 3: Simulación de fuga / avería (tecla `k`)
1. Mientras `WS-01` está regando (o en reposo), pulsa la tecla `k` en la terminal de `WM_WS_E`.
2. El sensor pasa a estado de avería (KO):
   - El monitor `WM_WS_M` detecta la avería y envía `LEAK#WS-01` a Central por socket.
   - El riego se interrumpe de inmediato conservando el volumen regado hasta ese instante.
   - Central y el cuadro de mandos cambian el estado a **FUGA (ROJO)**.
   - El operario recibe notificación del corte por avería y el resumen del agua suministrada.
3. Vuelve a pulsar `k` en `WM_WS_E`:
   - El sensor vuelve a estado OK.
   - El monitor envía `FIXED#WS-01` a Central.
   - La estación informa del estado final del riego previo y pasa automáticamente a **DISPONIBLE (VERDE)**.

### Caso 4: Control arbitrario desde Central
Desde la terminal de `WM_Central`:
- `p` -> Imprime el panel con colores ANSI.
- `b WS-01` -> Bloquea la estación (pasa a **FUERA DE SERVICIO / NARANJA** y cierra electroválvula si estaba regando).
- `a WS-01` -> Reactiva la estación (vuelve a **DISPONIBLE / VERDE**).
- `s WS-01 15` -> Inicia un riego ordenado directamente desde la central.
- `h` -> Muestra los últimos riegos archivados en la base de datos.

### Caso 5: Ejecución automatizada con fichero (`--file WM_FO/operaciones.txt`)
Al ejecutar:
```bash
python WM_FO/WM_FO.py localhost:9092 FO-01 --file WM_FO/operaciones.txt
```
El sistema ejecutará cada orden del fichero, esperará a su conclusión y aguardará **exactamente 4 segundos** entre servicios.

---

## 5. Despliegue en Cloud (AWS / Railway) y Equipos del Laboratorio

Según el escenario de evaluación de la práctica:
- **Cloud (AWS)**:
  - **Instancia AWS 1 (`WM_Central` + SQLite)**: expone el puerto TCP `5000` (sockets para monitores) y el puerto HTTP `8080` (dashboard web).
  - **Instancia AWS 2 (`Apache Kafka`)**: expone el puerto `9092` (externo) y `19092` (interno), con factor de replicación `1` para `__consumer_offsets`.
- **Laboratorio (PCs)**:
  - **`LLIBRE1-12` (Estaciones de riego `WS-01` y `WS-02`)**: ejecuta `WM_WS_M` y `WM_WS_E` para cada estación.
  - **`LLIBRE1-11` (Operadores de campo `FO-01` y `FO-02`)**: ejecuta dos instancias de `WM_FO`.

### Comandos de ejecución en el escenario AWS + Laboratorio:

1. **En la instancia AWS de `WM_Central`**:
   ```bash
   # Ejecución directa o mediante contenedor Docker:
   docker build -t wm_app .
   docker run -d --name wm_central --network=host -v "$(pwd)/data:/data" \
     wm_app python WM_Central/WM_Central.py 5000 <IP_AWS_KAFKA>:9092 --web-port 8080 --db /data/central.db
   ```
   - Panel web disponible en: `http://<IP_AWS_CENTRAL>:8080/`

2. **En `LLIBRE1-12` (Estaciones `WS-01` y `WS-02`)**:
   ```bash
   # Estación WS-01 (Monitor en puerto local 6001 + Engine):
   python WM_WS/WM_WS_M.py 6001 <IP_AWS_CENTRAL>:5000 WS-01 --loc "River Park Norte"
   python WM_WS/WM_WS_E.py <IP_AWS_KAFKA>:9092 localhost:6001

   # Estación WS-02 (Monitor en puerto local 6002 + Engine):
   python WM_WS/WM_WS_M.py 6002 <IP_AWS_CENTRAL>:5000 WS-02 --loc "River Park Sur"
   python WM_WS/WM_WS_E.py <IP_AWS_KAFKA>:9092 localhost:6002
   ```

3. **En `LLIBRE1-11` (Operadores `FO-01` y `FO-02`)**:
   ```bash
   # Operador FO-01:
   python WM_FO/WM_FO.py <IP_AWS_KAFKA>:9092 FO-01

   # Operador FO-02:
   python WM_FO/WM_FO.py <IP_AWS_KAFKA>:9092 FO-02
   ```
