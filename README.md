# WaterManagement Network

**Práctica 1 — Sistemas Distribuidos · Curso 2026/2027**  
**Autoras:** Sofía Herrmann Herrero y Katia Hanis

Sistema distribuido de gestión y supervisión del riego en parques y zonas verdes urbanas, desarrollado en **Python 3.12**. Combina **sockets TCP** para el registro y la supervisión de estaciones, **Apache Kafka** para las solicitudes, órdenes y telemetría, **SQLite** para persistencia y **Docker** para el despliegue.

## Arquitectura

```text
                   ┌──────────────────────────────┐
                   │          WM_Central          │
                   │     Control + panel web      │
                   │           SQLite             │
                   └───────┬───────────┬──────────┘
                           │ TCP       │ Kafka
                           │           │
                 ┌─────────▼──────┐    │     ┌─────────────────┐
                 │ WM_WS_M        │    ├────►│ WM_FO           │
                 │ Monitor        │    │     │ Operarios       │
                 └─────────┬──────┘    │     └─────────────────┘
                           │ TCP       │
                 ┌─────────▼──────┐    │
                 │ WM_WS_E        │◄───┘
                 │ Engine         │  Kafka: órdenes y eventos
                 └────────────────┘
```

- **WM_Central:** autentica Monitores, gestiona estados y autorizaciones, coordina riegos, almacena el historial y ofrece una interfaz web.
- **WM_WS_M (Monitor):** establece la conexión TCP con Central, comprueba la salud del Engine y notifica averías y recuperaciones.
- **WM_WS_E (Engine):** simula la electroválvula y el caudalímetro, ejecuta los riegos y publica telemetría.
- **WM_FO:** permite a los operarios consultar estaciones, solicitar riegos y recibir sus resultados.
- **Kafka:** intercambia mensajes asíncronos mediante los tópicos `fo_requests`, `fo_replies`, `ws_commands`, `ws_events` y `ws_status`.
- **SQLite:** guarda operarios, estaciones y el historial de servicios en `central.db`.

Los enlaces TCP emplean un protocolo de aplicación con tramas `STX–DATA–ETX–LRC`, comprobación XOR y confirmaciones `ACK/NACK`.

## Estructura principal del repositorio

```text
Practica-1-SD/
├── WM_Central/
│   ├── WM_Central.py
│   └── dashboard.html
├── WM_WS/
│   ├── WM_WS_M.py
│   └── WM_WS_E.py
├── WM_FO/
│   └── WM_FO.py
├── common.py
├── Dockerfile
├── docker-compose.yml
├── docker_run.sh
├── requirements.txt
├── test_transaction_duplicates.py
└── test_watering_flow.py
```

## Requisitos

- Python 3.12 (versión utilizada en el proyecto).
- Docker y Docker Compose.
- Acceso de red entre Central, Kafka, Monitores, Engines y operarios en un despliegue distribuido.

Para ejecutar componentes directamente con Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecución local

En terminales separadas, desde la raíz del repositorio:

**1. Iniciar Kafka**

```bash
docker compose up -d kafka
```

**2. Iniciar Central**

```bash
python3 WM_Central/WM_Central.py 5000 localhost:9092 --web-port 8080 --db data/central.db
```

Panel web: <http://localhost:8080>.

**3. Iniciar una estación ya registrada**

```bash
# Terminal del Monitor
python3 WM_WS/WM_WS_M.py 6001 localhost:5000 WS-01 --loc "River Park Norte"

# Terminal del Engine
python3 WM_WS/WM_WS_E.py localhost:9092 localhost:6001
```

**4. Iniciar un operario**

```bash
python3 WM_FO/WM_FO.py localhost:9092 FO-01
```

En su consola:

```text
l             # Consultar estaciones
r WS-01 10    # Solicitar un riego de 10 segundos
q             # Salir
```

> **Importante:** las estaciones nuevas deben **darse de alta primero en WM_Central**. Iniciar un Monitor con un identificador no autorizado provoca el rechazo del registro. Los identificadores de estación admitidos siguen el formato `WS-07`, `WS-10`, `WS-100`, etc.

### Ejecución con Docker de los módulos

El script `docker_run.sh` permite ejecutar los módulos sin abrir un IDE, pasando sus parámetros por terminal. Ejemplo con Central y Kafka ya accesibles:

```bash
./docker_run.sh monitor 6007 <IP_CENTRAL>:5000 WS-07 --loc "Central Park Oeste"
./docker_run.sh engine <IP_KAFKA>:9092 localhost:6007
./docker_run.sh fo <IP_KAFKA>:9092 FO-03
```

Iniciar cada componente en una terminal diferente. En estaciones nuevas, ejecutar el alta administrativa antes de arrancar el Monitor.

## Alta dinámica de estaciones

WM_Central permite **registrar estaciones nuevas durante la ejecución**, sin reiniciar Central ni las instancias existentes. El alta guarda la estación en SQLite y en memoria; una estación ya registrada no se sobrescribe.

Ejemplo para una Central desplegada en el contenedor `wm_central` (petición realizada **dentro** del contenedor, desde la máquina que lo administra):

```bash
sudo docker exec -i wm_central python3 - <<'PY'
import json
import urllib.request

payload = {"action": "register", "ws": "WS-07", "location": "Central Park Oeste"}
request = urllib.request.Request(
    "http://127.0.0.1:8080/api/action",
    data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request) as response:
    print(response.status, response.read().decode())
PY
```

La acción HTTP `register` está restringida a solicitudes administrativas autorizadas. **No se debe exponer el alta de estaciones públicamente ni compartir tokens administrativos.** Tras el alta, conectar primero el Monitor y después el Engine. Sin Engine, la estación debe aparecer averiada; con ambos componentes sanos, disponible.

## Estados de las estaciones

| Estado | Visualización | Significado |
|---|---|---|
| `DISPONIBLE` | Verde | Monitor y Engine operativos; acepta riegos. |
| `REGANDO` | Verde parpadeante | Riego activo y envío de telemetría. |
| `FUGA` | Rojo | Monitor conectado, pero Engine ausente o averiado. |
| `FUERA DE SERVICIO` | Naranja | Bloqueo ordenado por Central. |
| `DESCONECTADA` | Gris | Central no recibe mensajes del Monitor. |

Si el Monitor está desconectado, la estación aparece como `DESCONECTADA` independientemente del estado del Engine.

## Operaciones y pruebas manuales

- **Riego desde FO:** `r WS-01 10`.
- **Riego manual desde Engine:** `s`; detener con `f`.
- **Simular avería del Engine:** `k` (volver a pulsar para recuperarlo).
- **Consultar estaciones desde FO:** `l`.
- **Control desde Central:** permite iniciar, bloquear y activar estaciones individualmente o mediante `ALL`, respetando sus estados reales.
- **Caídas de componentes:** se pueden simular deteniendo Monitor, Engine, FO o Central durante un riego; consultar la memoria de prácticas para el procedimiento y los resultados observados.

### Pruebas automatizadas

Desde la raíz del proyecto:

```bash
python3 -m unittest -v test_transaction_duplicates test_watering_flow
```

En la revisión del **commit `6b256d7`**, la batería registrada ejecutó **18 pruebas correctamente**: 6 de transacciones y 12 de flujo de riego, incluyendo el alta dinámica de WS y controles de acceso. Este resultado corresponde a esa revisión y no sustituye las pruebas de integración en el despliegue real.

## Despliegue distribuido utilizado (AWS + equipos locales)

En las pruebas se utilizaron dos instancias AWS: una para **WM_Central y SQLite** y otra para **Kafka**. Los Monitores, Engines y operarios se ejecutaron en distintos equipos mediante Docker.

La imagen de Central se construye desde la raíz:

```bash
sudo docker build -f Dockerfile -t wm_central_aws:latest .
```

Ejemplo de inicio de Central, ajustando la dirección de Kafka y la ruta persistente al entorno:

```bash
sudo docker run -d \
  --name wm_central \
  --restart unless-stopped \
  -p 5000:5000 \
  -p 8080:8080 \
  -v "$(pwd)/data:/data" \
  -e DB_PATH=/data/central.db \
  wm_central_aws:latest \
  python WM_Central/WM_Central.py \
  5000 <IP_PRIVADA_KAFKA>:19092 \
  --web-port 8080 \
  --db /data/central.db
```

El puerto `5000` recibe conexiones TCP de los Monitores y el `8080` ofrece el panel web. La carpeta `data` está montada para conservar SQLite al sustituir el contenedor.

> **Seguridad:** el panel y sus acciones administrativas deben protegerse mediante las reglas de red y los controles de acceso apropiados. No se recomienda publicar el puerto `8080` sin restricciones en un despliegue fuera del laboratorio.

## Resiliencia y limitaciones conocidas

Se probaron caídas y recuperaciones de Monitor, Engine, FO, WM_Central y Kafka, incluyendo riegos en curso, notificaciones y conservación de resultados. Las operaciones se identifican con `transaction_id` para evitar duplicados de historial.

Durante las pruebas se observaron dos aspectos pendientes de revisión: la interferencia de una caída de Kafka en la comunicación entre Monitor y Engine, y un primer listado de estaciones desactualizado al iniciar un nuevo FO que se corrigió en la consulta posterior. Por ello, las pruebas de resiliencia deben interpretarse junto con las observaciones y evidencias documentadas en la memoria.

## Documentación

La memoria de prácticas recoge la arquitectura, los protocolos, las decisiones de diseño, el despliegue y las pruebas de validación realizadas siguiendo la guía de corrección de la asignatura.
