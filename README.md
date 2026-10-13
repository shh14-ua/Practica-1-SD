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

### Componentes Software

1. **`WM_Central` (`WM_Central.py`)**:
   - Centraliza el gobierno y monitorización de toda la red.
   - Servidor de sockets TCP para autenticación y supervisión de los monitores de estación (`WM_WS_M`).
   - Productor y consumidor de Kafka para gestionar peticiones de operarios y órdenes a motores.
   - Base de datos relacional **SQLite** (`central.db`) para registro de operarios, estaciones e historial de riegos.
   - **Cuadro de mandos**: Panel con códigos de color ANSI en consola y servidor web embebido en tiempo real accesible vía navegador (según Figuras 1 y 2).

2. **`WM_WS_M` (`WM_WS_M.py`) — Monitor de la Estación**:
   - Conexión cliente por socket a `WM_Central` para registrar y autenticar la estación (`REGISTRO#<ID>#<UBICACION>`).
   - Servidor de socket para conectar con su motor (`WM_WS_E`) y asignarle su ID (`ASSIGN#<ID>`).
   - Envío de comprobación de salud cada segundo (`HEALTH`).
   - Si detecta avería o el motor no responde, envía `LEAK#<ID>` a Central. Cuando se resuelve, envía `FIXED#<ID>`.
   - Envía latidos periódicos (`PING#<ID>`) para mantener activa la conexión con Central.

3. **`WM_WS_E` (`WM_WS_E.py`) — Motor y Actuador de la Estación**:
   - Conexión por socket al Monitor para recibir su ID asignado.
   - Conexión a Kafka para recibir órdenes (`ws_commands`) y publicar telemetría y eventos (`ws_events`).
   - Simula la electroválvula y caudalímetro (~8.5 a 11.5 L/min).
   - Genera eventos periódicos de telemetría cada segundo durante el riego.
   - **Simulación de avería**: La tecla `k` conmuta entre funcionamiento normal y KO (fuga simulada).
   - Permite iniciar riego manual (`s`) y detenerlo (`f`) desde la propia estación.

4. **`WM_FO` (`WM_FO.py`) — Aplicación del Operario de Campo**:
   - Conexión a Kafka para solicitar servicios de riego (`fo_requests`).
   - Recibe notificaciones de estado paso a paso (`STEP`), autorización (`AUTH`), denegación (`DENY`), telemetría en tiempo real y resumen final (`SUMMARY`).
   - Consulta el estado de todas las estaciones registradas (`ws_status`).
   - **Modo por lotes (`--file operaciones.txt`)**: Procesa las peticiones secuencialmente, esperando a la conclusión de cada servicio y **aguardando exactamente 4 segundos** antes de lanzar el siguiente.

5. **`common.py`**:
   - Protocolo de tramas por socket `<STX><DATA><ETX><LRC>` con cálculo byte a byte por XOR.
   - Handshake `ENQ` / `ACK`.
   - Conexiones y tópicos de Apache Kafka.
   - Constantes de estado y códigos ANSI de color.

---

## 2. Requisitos y Preparación del Entorno

### Dependencias
- Python 3.10 o superior (compatible con Python 3.12).
- Docker y Docker Compose (para levantar Kafka localmente).

### Instalación de librerías Python
```bash
# Crear entorno virtual (opcional pero recomendado)
python3 -m venv .venv
source .venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

---

## 3. Despliegue y Ejecución Local

### Paso 1: Levantar Apache Kafka
En la carpeta del proyecto, ejecuta:
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
python WM_Central.py 5000 localhost:9092
```
- Parámetros: `<puerto_sockets> <kafka_host:puerto>`
- Opcional: añade `--web-port 8080` para abrir el cuadro de mandos web en `http://localhost:8080/`.

### Paso 3: Iniciar una Estación de Riego (`WM_WS_M` y `WM_WS_E`)

**Terminal 2 — Monitor de la Estación 1:**
```bash
python WM_WS_M.py 6001 localhost:5000 WS-01 --loc "River Park Norte"
```
- Parámetros: `<puerto_engine> <central_host:puerto> <ID_WS> [--loc UBICACION]`

**Terminal 3 — Motor de la Estación 1:**
```bash
python WM_WS_E.py localhost:9092 localhost:6001
```
- Parámetros: `<kafka_host:puerto> <monitor_host:puerto>`

*(Puedes levantar tantas estaciones como desees cambiando los puertos y el ID, ej: `WS-02` con puerto 6002).*

### Paso 4: Iniciar la Aplicación del Operario (`WM_FO`)

**Terminal 4 — Operario en Modo Interactivo:**
```bash
python WM_FO.py localhost:9092 FO-01
```
- Parámetros: `<kafka_host:puerto> <ID_operario>`

**Terminal 4 (Alternativa) — Operario en Modo por Lotes:**
```bash
python WM_FO.py localhost:9092 FO-01 --file operaciones.txt
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
1. Inicia un operario no dado de alta: `python WM_FO.py localhost:9092 FO-99`.
2. Solicita riego: `r WS-01 10`.
3. Central deniega la solicitud inmediatamente indicando que el operario no está registrado.
4. Si una estación ya está regando o en fuga, cualquier solicitud posterior sobre ella será denegada explicando el motivo.

### Caso 3: Simulación de fuga / avería (tecla `k`)
1. Mientras `WS-01` está regando (o en reposo), pulsa la tecla `k` en la terminal de `WM_WS_E`.
2. El sensor pasa a estado de avería (KO):
   - El monitor `WM_WS_M` detecta la avería y envía `LEAK#WS-01` a Central por socket.
   - El riego se interrumpe de inmediato.
   - Central y el cuadro de mandos cambian el estado a **FUGA (ROJO)**.
   - El operario recibe notificación del corte por avería.
3. Vuelve a pulsar `k` en `WM_WS_E`:
   - El sensor vuelve a estado OK.
   - El monitor envía `FIXED#WS-01` a Central.
   - La estación pasa automáticamente a **DISPONIBLE (VERDE)**.

### Caso 4: Control arbitrario desde Central
Desde la terminal de `WM_Central`:
- `p` -> Imprime el panel con colores ANSI.
- `b WS-01` -> Bloquea la estación (pasa a **FUERA DE SERVICIO / NARANJA** y cierra electroválvula si estaba regando).
- `a WS-01` -> Reactiva la estación (vuelve a **DISPONIBLE / VERDE**).
- `s WS-01 15` -> Inicia un riego ordenado directamente desde la central.
- `h` -> Muestra los últimos riegos archivados en la base de datos.

### Caso 5: Ejecución automatizada con fichero (`--file operaciones.txt`)
Al ejecutar:
```bash
python WM_FO.py localhost:9092 FO-01 --file operaciones.txt
```
El sistema ejecutará cada orden del fichero, esperará a su conclusión y aguardará **exactamente 4 segundos** entre servicios.

---

## 5. Despliegue en Railway (Escenario de Evaluación Cloud)

Según la **Figura 4** de la práctica:
- **Cloud (Railway)**: Aloja `WM_Central`, la base de datos `central.db` y el broker de `Kafka`.
- **Laboratorio (PCs)**: Alojan `WM_FO` y las estaciones `WM_WS`.

### Pasos en Railway:
1. **Crear servicio Kafka**:
   - En Railway: `+ New` -> `Template` -> `Kafka`.
   - En *Settings* de Kafka -> *Networking* -> *TCP Proxy* -> Añadir proxy (apuntando al puerto 9092).
   - Railway generará un endpoint público, por ejemplo: `monorail.proxy.rlwy.net:12345`.
2. **Crear servicio WM_Central**:
   - `+ New` -> `GitHub Repo` (conectar tu repositorio con este código).
   - Railway detectará automáticamente el `Dockerfile`.
   - En *Settings* -> *Networking* -> Generar dominio público para acceder al cuadro de mandos Web.
   - Añadir TCP Proxy en el puerto 5000 para que los monitores de laboratorio (`WM_WS_M`) puedan conectar por socket.
   - En variables de entorno (*Variables*):
     - `KAFKA_BOOTSTRAP`: host y puerto interno o público de Kafka.
3. **Ejecutar en los PCs de laboratorio**:
   - En PC 1 (Operarios):
     ```bash
     python WM_FO.py monorail.proxy.rlwy.net:12345 FO-01
     ```
   - En PC 2 (Estaciones de riego):
     ```bash
     python WM_WS_M.py 6001 central.proxy.rlwy.net:5000 WS-01 --loc "River Park"
     python WM_WS_E.py monorail.proxy.rlwy.net:12345 localhost:6001
     ```
