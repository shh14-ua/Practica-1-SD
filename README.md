# WaterManagement Network — Release 2

**Práctica no guiada: Seguridad y API Rest — Sistemas Distribuidos · Curso 2026/2027**
**Autoras:** Sofía Herrmann Herrero y Katia Hanis

Sistema distribuido de gestión, supervisión y seguridad del riego en parques y zonas verdes urbanas (SOA). Esta segunda fase (Release 2) amplía el sistema base incorporando:
1. **API REST propia (API_Central)** expuesta desde el backend y consumida por el módulo Web Front y la oficina meteorológica.
2. **Nuevo módulo meteorológico (WM_W - Weather Control Office)** que consulta la API externa de OpenWeather cada 4 segundos para suspender riegos en caso de lluvia o heladas (< 0 ºC).
3. **Nuevo módulo de registro seguro (WM_Registry)** que gestiona altas, bajas y credenciales de acceso para las estaciones de riego a través de un canal seguro HTTPS/SSL.
4. **Módulo Web Front (Node.js)** que visualiza el cuadro de monitorización interactivo en tiempo real desde cualquier navegador.
5. **Tres pilares de seguridad:**
   - **Autenticación segura:** Canal cifrado HTTPS/SSL para la comunicación con `WM_Registry` y verificación obligatoria de credenciales en `WM_Central`.
   - **Cifrado simétrico de datos:** Clave simétrica única por estación otorgada por Central para evitar ataques Man-In-The-Middle (MITM), con soporte de revocación y restauración de claves ante supuesta vulnerabilidad.
   - **Auditoría estructurada:** Registro detallado de eventos de seguridad (fecha/hora, IP de origen, entidad, acción y descripción).

---

## Arquitectura del Sistema (Release 2)

```text
                             ┌───────────────────────────────────────┐
                             │             Browser (Web)             │
                             └───────────────────┬───────────────────┘
                                                 │ HTTP Request
                                     ┌───────────▼───────────┐
                                     │      Front (Node)     │
                                     └───────────┬───────────┘
                                                 │ REST API
┌─────────────────────────┐          ┌───────────▼────────────────────────────────────────┐
│  Weather Office (WM_W)  ├─────────►│                 API_CENTRAL                        │
│ (OpenWeather API cada 4s│ REST API │          WM_Central (Core System)                  │
└─────────────────────────┘          │          SQLite (central.db + audit)               │
                                     └───────────┬───────────────────────┬────────────────┘
                                                 │ Sockets (Cifrado AES) │ Kafka
                                                 │                       │
      ┌──────────────────────┐         ┌─────────▼──────┐                │     ┌─────────────────┐
      │     WM_Registry      │◄────────┤ WM_WS_M        │                ├────►│ WM_FO           │
      │  (HTTPS REST API)    │  HTTPS  │ Monitor        │                │     │ Operarios       │
      └──────────────────────┘         └─────────┬──────┘                │     └─────────────────┘
                                                 │ TCP Sockets           │
                                       ┌─────────▼──────┐                │
                                       │ WM_WS_E        │◄───────────────┘
                                       │ Engine         │   Kafka: ws_commands & ws_events
                                       └────────────────┘
```

---

## Componentes y Módulos

- **WM_Central & API_Central:**
  Coordina el sistema, valida credenciales de `WM_Registry`, expone la API REST para el Front y para `WM_W`, entrega la clave simétrica única por estación, ejecuta la revocación/restauración de claves y mantiene el registro estructurado de auditoría en SQLite (`audit_log`) y fichero `audit.log`.

- **WM_Registry (Registro Seguro HTTPS):**
  Servidor HTTPS sobre puerto `5500` con certificados SSL. Ofrece endpoints REST (`/api/stations`, `/api/verify`) para que las estaciones se den de alta y obtengan de forma segura su credencial de acceso (`CRED-...`). Sin este paso, Central deniega la autenticación.

- **Weather Control Office (WM_W):**
  Supervisa cada 4 segundos el clima de las ciudades de las estaciones conectándose a OpenWeather (`https://api.openweathermap.org/`). Si la temperatura baja de 0 ºC o hay lluvia, notifica a Central vía `API_Central` (`POST /api/weather/alert`). Central detiene el riego suavemente y la pasa a "FUERA DE SERVICIO". Cuando el clima vuelve a ser favorable, notifica la cancelación y la estación se restaura automáticamente. Incluye menú interactivo y fichero `ciudades.json` para cambiar ubicaciones en tiempo real sin reiniciar el sistema (conforme a los requisitos de evaluación).

- **Front (Node.js Web Dashboard):**
  Servidor Node.js ligero que consume `API_Central` y muestra el cuadro de mandos interactivo:
  - Estados de las WS con los colores oficiales de la especificación (Verde = Disponible, Verde parpadeante = Regando, Rojo = Fuga, Naranja = Fuera de Servicio, Gris = Desconectada).
  - Estado de las claves de seguridad simétricas y botón para **"Restaurar / Revocar Claves"**.
  - Alertas climáticas de `WM_W` y temperatura actual por ciudad.
  - Registro de auditoría en vivo.
  - Histórico de transacciones de riego en SQLite.

- **WM_WS_M (Monitor):**
  Se conecta a `WM_Registry` vía HTTPS para obtener sus credenciales, se autentica en `WM_Central` y recibe su clave simétrica única. Cifra simétricamente todas sus comunicaciones con Central y dispone de menú para re-autenticarse si Central revoca sus claves.

- **WM_WS_E (Engine):**
  Simula electroválvula y caudalímetro, recibe la clave simétrica del monitor, ejecuta riegos y publica telemetría. Si recibe orden de parada meteorológica mientras riega, el riego finaliza con normalidad y posteriormente pasa a modo "fuera de servicio".

- **WM_FO (Field Operator):**
  Mantiene la funcionalidad de la entrega anterior para solicitar riegos de forma interactiva o por lotes (`operaciones.txt`).

---

## Estructura del Repositorio

```text
Practica-1-SD/
├── WM_Central/
│   ├── WM_Central.py           # Núcleo Central, servidor de sockets y API_Central REST
│   └── dashboard.html          # Panel web integrado de respaldo
├── WM_Registry/
│   ├── WM_Registry.py          # API REST HTTPS de registro y credenciales
│   └── certs/                  # Certificado X.509 y clave privada RSA 2048 para TLS
├── WM_W/
│   ├── WM_W.py                 # Weather Control Office (OpenWeather + Simulación)
│   └── ciudades.json           # Ubicaciones de las WS editables en caliente
├── front/
│   ├── package.json
│   ├── server.js               # Servidor Web Node.js
│   └── public/
│       └── index.html          # Dashboard Web moderno y responsivo
├── WM_WS/
│   ├── WM_WS_M.py              # Monitor (HTTPS con Registry, sockets cifrados con Central)
│   └── WM_WS_E.py              # Engine (actuador, sensor, telemetría Kafka)
├── WM_FO/
│   ├── WM_FO.py                # Operarios de campo
│   └── operaciones.txt
├── common.py                   # Protocolos de sockets, Kafka y utilidades
├── wm_crypto.py                # Módulo criptográfico (clave simétrica, cifrado HMAC/AES, tokens)
├── test_release2.py            # Suite de pruebas automatizadas de integración
├── docker-compose.yml          # Despliegue completo de servicios Cloud
├── Dockerfile
└── requirements.txt
```

---

## Guía de Despliegue (Distribución según Enunciado)

El enunciado establece el siguiente esquema mixto de distribución:

### 1. Sistemas en Cloud (Railway / AWS Labs / Docker)
Incluye `WM_Central`, `API_Central`, `BD SQLite`, `KAFKA`, `Front (Web)` y `WM_Registry`.

Para desplegar todo el núcleo con Docker Compose:
```bash
docker compose up -d
```
O de forma manual por terminales:

**Paso 1.1 — Kafka:**
```bash
docker compose up -d kafka
```

**Paso 1.2 — WM_Registry (HTTPS REST API en puerto 5500):**
```bash
python WM_Registry/WM_Registry.py 5500
```

**Paso 1.3 — WM_Central (Sockets en 5000, API REST en 8080):**
```bash
python WM_Central/WM_Central.py 5000 localhost:9092 --web-port 8080
```

**Paso 1.4 — Front (Web Dashboard en Node.js en puerto 3000):**
```bash
node front/server.js 3000 http://localhost:8080
```
El panel de control queda disponible en el navegador en `http://localhost:3000/`.

---

### 2. PC de Laboratorio 1 (Operarios y Clima)

**Paso 2.1 — Oficina Meteorológica (WM_W):**
```bash
python WM_W/WM_W.py http://<IP_CENTRAL>:8080
```
- Realiza consultas cada 4 segundos a OpenWeather.
- Dispone de menú para cambiar ciudades en caliente (`c WS-01 Londres`) o simular heladas/lluvia instantáneas (`f WS-01 -4`, `r WS-01`, `ok WS-01`).

**Paso 2.2 — Operarios de Campo (WM_FO):**
```bash
python WM_FO/WM_FO.py <IP_KAFKA>:9092 FO-01
```

---

### 3. PC de Laboratorio 2 (Estaciones de Riego)

**Paso 3.1 — Monitor de la Estación (WM_WS_M):**
```bash
python WM_WS/WM_WS_M.py 6001 <IP_CENTRAL>:5000 WS-01 --loc "River Park" --registry https://<IP_REGISTRY>:5500
```
- Se registra automáticamente en `WM_Registry` vía HTTPS para recibir su credencial.
- Se autentica en `WM_Central` y recibe su clave de cifrado simétrico única.
- Abre socket local en el puerto `6001` para el Engine.

**Paso 3.2 — Motor de la Estación (WM_WS_E):**
```bash
python WM_WS/WM_WS_E.py <IP_KAFKA>:9092 localhost:6001
```
- Recibe del Monitor el identificador `WS-01` y la clave simétrica.
- Ejecuta las órdenes de riego y publica telemetría.

---

## Verificación y Pruebas Automatizadas

El proyecto incluye una suite de pruebas completa en `test_release2.py`:

```bash
python test_release2.py
```

La suite comprueba automáticamente los 7 aspectos críticos de la Release 2:
1. **TEST 1:** Cifrado y descifrado simétrico con verificación de integridad.
2. **TEST 2:** Alta segura en `WM_Registry` por HTTPS y emisión de credencial.
3. **TEST 3:** Autenticación en `WM_Central` con credenciales de Registry y entrega de clave simétrica única. Rechazo ante credencial no registrada.
4. **TEST 4:** Transmisión de tramas cifradas simétricamente por sockets sin exposición en claro.
5. **TEST 5:** Revocación de claves por parte de Central ante supuesta vulnerabilidad, puesta en "FUERA DE SERVICIO" y rechazo de tramas con clave revocada.
6. **TEST 6:** Alerta meteorológica de congelación (< 0 ºC) de `WM_W` vía `API_Central`, parada y posterior restauración al volver el clima favorable.
7. **TEST 7:** Comprobación del registro estructurado de auditoría en la base de datos (`audit_log`).
