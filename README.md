# WaterManagement Network — Release 2

**Práctica no guiada: Seguridad y API Rest — Sistemas Distribuidos · Curso 2026/2027**
**Autoras:** Sofía Herrmann Herrero y Katia Hanis

Sistema distribuido de gestión, supervisión y seguridad del riego en parques y zonas verdes urbanas (SOA). Esta segunda fase (Release 2) amplía el sistema base incorporando:
1. **API REST propia (API_Central)** expuesta desde el backend y consumida por el módulo Web Front y la oficina meteorológica.
2. **Nuevo módulo meteorológico (WM_W - Weather Control Office)** que consulta la API externa de OpenWeather cada 4 segundos para suspender riegos en caso de lluvia o heladas (< 0 ºC).
3. **Nuevo módulo de registro seguro (WM_Registry)** que gestiona altas, bajas y credenciales de acceso para las estaciones de riego a través de un canal seguro HTTPS/SSL.
4. **Módulo Web Front (Node.js)** que visualiza el cuadro de monitorización interactivo en tiempo real desde cualquier navegador.
5. **Tres pilares de seguridad:**
   - **Autenticación segura:** Canal cifrado HTTPS/SSL para la comunicación con `WM_Registry` y verificación obligatoria de credenciales en `WM_Central`. La autenticación de las WS en Central y la entrega de la clave simétrica viajan por un canal TLS (certificado propio de Central validado por el Monitor).
   - **Cifrado simétrico de datos:** Clave simétrica única por estación otorgada por Central para evitar ataques Man-In-The-Middle (MITM), con soporte de revocación y restauración de claves ante supuesta vulnerabilidad. Todos los mensajes entre Central y la estación van cifrados y autenticados con Fernet (AES-128-CBC + HMAC-SHA256): por socket con el Monitor y por Kafka (`ws_commands` y `ws_events`) con el Engine.
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
  Se conecta a `WM_Registry` vía HTTPS para obtener sus credenciales, se autentica en `WM_Central` por un canal TLS (valida el certificado de Central y su nombre de host) y recibe su clave simétrica única. Cifra simétricamente todas sus comunicaciones con Central. Si Central revoca sus claves, la estación queda FUERA DE SERVICIO y el Monitor no vuelve a autenticarse hasta que se usa la opción de menú `k` (o `a`).

- **WM_WS_E (Engine):**
  Simula electroválvula y caudalímetro, recibe la clave simétrica del monitor, ejecuta riegos y publica telemetría. Si recibe orden de parada meteorológica mientras riega, el riego finaliza con normalidad y posteriormente pasa a modo "fuera de servicio".

- **WM_FO (Field Operator):**
  Mantiene la funcionalidad de la entrega anterior para solicitar riegos de forma interactiva o por lotes (`operaciones.txt`).

---

## Estructura del Repositorio

```text
Practica-1-SD/
├── WM_Central/
│   ├── WM_Central.py           # Núcleo Central, servidor de sockets TLS y API_Central REST
│   ├── dashboard.html          # Panel web integrado de respaldo
│   └── certs/                  # Certificado TLS de Central (se genera al arrancar; la clave no se versiona)
├── WM_Registry/
│   ├── WM_Registry.py          # API REST HTTPS de registro y credenciales
│   └── certs/                  # Certificado X.509 y clave RSA-2048 (se generan al arrancar; no se versionan)
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
├── wm_crypto.py                # Módulo criptográfico (clave simétrica, cifrado Fernet, mensajes Kafka, certificados)
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
python WM_Registry/WM_Registry.py 5500 --san <HOST_O_IP_PUBLICA_DEL_REGISTRY>
```
- En el primer arranque genera `WM_Registry/certs/registry_cert.pem` y su clave privada (0600) y en los siguientes los reutiliza. Solo se regeneran si falta alguno, si no forman pareja o si se pide un nombre nuevo con `--san` (o `$WM_REGISTRY_SAN`).
- El certificado no se versiona: copie `registry_cert.pem` (nunca la clave) a los Monitores y a Central (`--registry-ca` / `$WM_REGISTRY_CA`).

**Paso 1.3 — WM_Central (Sockets TLS en 5000, API REST en 8080):**
```bash
python WM_Central/WM_Central.py 5000 localhost:9092 --web-port 8080 --san <HOST_O_IP_PUBLICA_DE_CENTRAL>
```
- El canal de sockets con los Monitores es TLS obligatorio: una conexión sin TLS se rechaza y queda en la auditoría como `TLS_FAILED`.
- Al arrancar se genera (si no existe) un certificado autofirmado RSA-2048 en `WM_Central/certs/central_cert.pem` y su clave privada `central_key.pem` (permisos 0600, excluida de Git). Rutas configurables con `--cert` y `--key`.
- `--san` (o `$WM_CENTRAL_SAN`) añade, separados por comas, los nombres DNS/IP con los que los Monitores contactan con Central (p. ej. el host público del despliegue cloud). Siempre se incluyen `localhost`, `central` y `127.0.0.1`. Si se añade un nombre nuevo, el certificado se regenera y hay que volver a copiarlo a los Monitores.
- Copie `central_cert.pem` (nunca la clave) a las máquinas de los Monitores.
- Secretos de API_Central (variables de entorno del despliegue, p. ej. en Railway o en `docker compose`; nunca en el repositorio):
  - `WM_ADMIN_TOKEN`: token de administración para las acciones de control por la API (regar, bloquear, activar, revocar claves, alta de WS). Solo las peticiones directas desde la propia máquina de Central están exentas; las que llegan a través del Front siempre lo requieren.
  - `WM_WEATHER_TOKEN`: secreto compartido con WM_W. Sin él configurado, Central rechaza todas las notificaciones climáticas.
- Central consulta cada 10 s a WM_Registry (`GET /api/stations` por HTTPS, validando su certificado con `$WM_REGISTRY_CA`) qué WS están dadas de alta, para mostrar su estado de registro.

**API_Central (REST):**

| Método y ruta | Acceso | Descripción |
|---|---|---|
| `GET /api/stations`, `GET /api/stations/{ws}` | Público | Estado de las WS: estado, activación, clave/token, revocación, registro en WM_Registry, clima |
| `GET /api/operators`, `GET /api/operators/{id}` | Público | Operarios FO con su estado (`REGANDO` / `EN_ESPERA`) y sus riegos en curso |
| `GET /api/waterings` | Público | Riegos en curso (regando o pendientes de confirmación del Engine) |
| `GET /api/status`, `/api/history`, `/api/audit`, `/api/weather` | Público | Panel completo (Front), histórico, auditoría y clima |
| `PUT /api/weather/{ws}` | WM_W (`X-Weather-Token`) | Estado climático actual de una WS (activa o cancela la alerta) |
| `DELETE /api/weather/{ws}/alert` | WM_W (`X-Weather-Token`) | Cancelación de la alerta climática |
| `POST /api/weather/alert` | WM_W (`X-Weather-Token`) | Compatibilidad: igual que el PUT, con `ws_id` en el cuerpo |
| `POST /api/action` | Administración (`X-Admin-Token`) | `start`, `block`, `activate`, `revoke_keys`, `register` |

Las respuestas son JSON con códigos coherentes: 400 (datos no válidos), 401/403 (sin autorización), 404 (recurso inexistente), 409 (acción no realizable en el estado actual) y 503 (notificaciones climáticas sin `WM_WEATHER_TOKEN` configurado).

**Paso 1.4 — Front (Web Dashboard en Node.js en puerto 3000):**
```bash
node front/server.js 3000 http://localhost:8080
```
El panel de control queda disponible en el navegador en `http://localhost:3000/`.
- El Front solo reenvía a API_Central las consultas y las acciones de control; nunca las notificaciones climáticas. Al ejecutar una acción pide el token de administración, que solo se guarda en la sesión del navegador.
- Si API_Central usa HTTPS con un certificado propio, indique su CA con `CENTRAL_API_CA=<ruta>`; el certificado siempre se verifica.

---

### 2. PC de Laboratorio 1 (Operarios y Clima)

**Paso 2.1 — Oficina Meteorológica (WM_W):**
```bash
WM_WEATHER_TOKEN=<secreto compartido con Central> python WM_W/WM_W.py http://<IP_CENTRAL>:8080
```
- Realiza consultas cada 4 segundos a OpenWeather y en cada ciclo envía a Central el estado climático actual de cada WS (`PUT /api/weather/{ws}`), aunque no cambie: así Central recupera las alertas si se reinicia. Al volver el clima favorable cancela la alerta (`DELETE /api/weather/{ws}/alert`).
- Sin `WM_WEATHER_TOKEN` no arranca. Como el secreto viaja en una cabecera, en el despliegue use una URL `https://` de API_Central cuando esté disponible (p. ej. la que proporciona Railway).
- Dispone de menú para cambiar ciudades en caliente (`c WS-01 Londres`) o simular heladas/lluvia instantáneas (`f WS-01 -4`, `r WS-01`, `ok WS-01`).

**Paso 2.2 — Operarios de Campo (WM_FO):**
```bash
python WM_FO/WM_FO.py <IP_KAFKA>:9092 FO-01
```

---

### 3. PC de Laboratorio 2 (Estaciones de Riego)

**Paso 3.1 — Monitor de la Estación (WM_WS_M):**
```bash
python WM_WS/WM_WS_M.py 6001 <IP_CENTRAL>:5000 WS-01 --loc "River Park" --registry https://<IP_REGISTRY>:5500 --central-ca <ruta>/central_cert.pem
```
- Se registra automáticamente en `WM_Registry` vía HTTPS para recibir su credencial.
- Se conecta a `WM_Central` por TLS validando su certificado (`--central-ca` o `$WM_CENTRAL_CA`; por defecto `WM_Central/certs/central_cert.pem`). `<IP_CENTRAL>` debe figurar en el SAN del certificado de Central.
- Se autentica en `WM_Central` y recibe su clave de cifrado simétrico única.
- Abre el socket del Engine en el puerto `6001` **solo en la interfaz local (127.0.0.1)**: por él viaja la clave simétrica, así que el Engine debe ejecutarse en la misma máquina que su Monitor y conectarse a `localhost:6001` (con Docker, ambos contenedores con `--network=host`, como en `docker_run.sh`).

**Paso 3.2 — Motor de la Estación (WM_WS_E):**
```bash
python WM_WS/WM_WS_E.py <IP_KAFKA>:9092 localhost:6001
```
- Recibe del Monitor el identificador `WS-01` y la clave simétrica.
- Ejecuta las órdenes de riego y publica telemetría.

**Revocación de claves y recuperación de la estación:**
- Desde Central (CLI `k <WS>`, botón "Restaurar Claves" del Front o `POST /api/action` con `revoke_keys`) se borran las claves de la WS, que queda FUERA DE SERVICIO. Justo antes de borrarla, Central envía por Kafka una última orden `BLOCK` cifrada con esa clave, que detiene el Engine aunque su Monitor no pueda recibir el aviso (p. ej. corte de red con Central). El Monitor lo recibe como respuesta a su siguiente mensaje, ordena al Engine borrar su clave (`CLEAR_KEY`) y bloquearse, y deja de re-autenticarse automáticamente. Tras autenticarse, el Monitor envía al Engine el estado (bloqueado o activo) que indica Central.
- La credencial que la WS usaba al revocarse queda invalidada en Central: solo la recupera la opción `k` del Monitor, que renueva la credencial en WM_Registry y obtiene claves nuevas. Una reconexión automática, un reinicio del Monitor o la opción `a` con la credencial anterior se rechazan (`STATUS#REVOKED`), también si la revocación se hizo con el Monitor desconectado.
- La revocación se guarda en SQLite (tabla `station_security`, solo huellas SHA-256 de credenciales, junto con el bloqueo manual de CENTRAL si lo hay) y sobrevive a reinicios de Central. Tras re-autenticarse, la WS sigue FUERA DE SERVICIO si tiene un bloqueo manual de CENTRAL o una alerta climática vigente; si no, vuelve a servicio.
- Un bloqueo manual de CENTRAL (`b <WS>`) solo se levanta con una activación explícita: la cancelación de una alerta climática restaura únicamente las estaciones bloqueadas por el clima. Como en la Release 1, el bloqueo manual de una estación no revocada no se conserva tras reiniciar Central.
- Un riego en curso se interrumpe (el Monitor bloquea el Engine) y Central lo consolida de inmediato en el historial con el volumen conocido y el motivo `KEYS_REVOKED`, avisando al operario; el `FINISHED` tardío del Engine solo actualiza el volumen de esa transacción. Si el riego estaba pendiente de confirmar, el operario recibe una denegación.
- API_Central expone `keys_revoked` por estación y el Front muestra con él el estado real del token.

**Canal Kafka cifrado Central ↔ Engine (`ws_commands` / `ws_events`):**
- Cada mensaje viaja como `{"ws": "<WS>", "enc": "ENC:<token Fernet>"}`: solo el identificador de la estación es visible (para elegir la clave) y se rechaza cualquier otro campo en claro (`CAMPOS_EXTRA`). Dentro del cifrado van la orden o el evento, el tipo (`cmd`/`evt`), la WS y un número de secuencia creciente.
- Se rechazan los mensajes en claro, manipulados, cifrados con otra clave (anterior o revocada), de otra WS, del sentido contrario o repetidos. Central registra los rechazos en la auditoría (`KAFKA_REJECTED`, como máximo uno cada 30 s por estación y motivo).
- Sin clave vigente Central no envía órdenes a esa estación y el Engine no acepta órdenes ni nuevos riegos (tampoco manuales), no publica nada en claro y conserva los `FINISHED` pendientes, que reenvía cifrados al recibir una clave nueva. Al revocarse las claves o darse de baja, el Monitor ordena al Engine borrar la suya (`CLEAR_KEY`); si estaba regando, el Engine cierra la válvula en ese momento (motivo `KEYS_CLEARED`), de modo que la baja del Monitor interrumpe el riego como en la Release 1.
- Central confirma cada `FINISHED` registrado con la orden cifrada `ACK_FINISHED` (también los duplicados). El Engine conserva los `FINISHED` no confirmados y, si su clave cambia antes de la confirmación (re-autenticación, reinicio de Central o revocación), los reenvía cifrados con la clave nueva; Central descarta los duplicados por `transaction_id` y un `FINISHED` de una transacción anterior nunca altera el estado de un riego en curso.
- El Engine guarda en `WM_WS/.engine_state/<WS>.json` (o `$WM_ENGINE_STATE_DIR`; con Docker, `data/engine_state`) la huella de su clave y el último número de secuencia aceptado, para no aceptar órdenes repetidas tras reiniciarse con la misma clave. No contiene la clave.
- Es obligatoria la librería `cryptography` (`pip install -r requirements.txt`) en todas las máquinas: no existe cifrado alternativo y Central, Monitor y Engine no arrancan sin ella.

---

## Verificación y Pruebas Automatizadas

Pruebas de API_Central, WM_W y el Front (HTTP, Registry HTTPS y Node.js reales; Kafka simulado):

```bash
python test_api_central.py
python test_front_proxy.py
```

Las pruebas del canal TLS entre Central y los Monitores están en `test_tls_central.py` (sockets, TLS y Registry HTTPS reales):

```bash
python test_tls_central.py
```

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
