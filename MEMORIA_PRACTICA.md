# MEMORIA DE PRÁCTICAS: WATERMANAGEMENT
## Sistemas Distribuidos — Curso Académico 2026/2027

---

### DATOS DEL PROYECTO Y ESTUDIANTES

| Campo | Información |
| :--- | :--- |
| **Título del Proyecto** | Sistema Distribuido de Gestión y Supervisión de Riego Urbano (WaterManagement - Release 1) |
| **Asignatura** | Sistemas Distribuidos (SD) |
| **Titulación** | Grado en Ingeniería Informática |
| **Estudiante(s)** | Sofía [Apellidos del estudiante] |
| **DNI / NIF** | [DNI del estudiante] |
| **Repositorio GitHub** | [Enlace al repositorio privado compartido con el profesor] |
| **Entorno Cloud** | Railway ([URL del proyecto / Dashboard]) |

---

## 1. Introducción y Objetivos

La gestión eficiente del agua potable y regenerada representa uno de los desafíos más críticos en las ciudades modernas. El riego no supervisado genera fugas no detectadas, sobrecostes y un uso ineficiente de un recurso escaso.

El objetivo de esta práctica es el diseño, implementación y despliegue de un **sistema distribuido resiliente, escalable y seguro** denominado **WaterManagement**, que permite la monitorización en tiempo real y la activación remota del riego en parques y jardines urbanos.

Los objetivos pedagógicos y técnicos principales alcanzados son:
1. **Comunicaciones síncronas de bajo nivel mediante Sockets TCP**, definiendo un protocolo de tramas riguroso con control de errores mediante redundancia longitudinal (**LRC**).
2. **Streaming asíncrono desacoplado mediante Apache Kafka**, implementando el patrón Publicador-Suscriptor (*Pub/Sub*) para telemetría continua y comandos entre entidades desacopladas en el tiempo y el espacio.
3. **Persistencia relacional mediante SQLite**, garantizando la coherencia del estado del sistema ante reinicios.
4. **Despliegue distribuido real** utilizando **Docker** y **Railway** en un entorno mixto nube/laboratorio.

---

## 2. Arquitectura del Sistema

El sistema implementa el modelo de arquitectura distribuida establecido en la Figura 3 del enunciado:

```
                           ┌────────────────────────────────────────┐
                           │              CORE SYSTEM               │
                           │                                        │
                           │  ┌──────────────┐   ┌───────────────┐  │
                           │  │  central.db  │◄──┤  WM_Central   │  │
                           │  │   (SQLite)   │   │(Lógica Central│  │
                           │  └──────────────┘   └───────┬───────┘  │
                           └─────────────────────────────┼──────────┘
                                                         │
                                        Sockets TCP / LRC│  Apache Kafka
                                                         │
                ┌────────────────────────────────────────┴─────────────────────────────────────────┐
                │                                                                                  │
                ▼                                                                                  ▼
┌───────────────────────────────┐                                                  ┌───────────────────────────────┐
│     WATERING STATION (WS)     │                                                  │     FIELD OPERATOR (FO)       │
│                               │                                                  │                               │
│  ┌─────────────────────────┐  │                                                  │  ┌─────────────────────────┐  │
│  │         WM_WS_M         │  │                                                  │  │          WM_FO          │  │
│  │        (Monitor)        │  │                                                  │  │   (App de Operario)     │  │
│  └────────────┬────────────┘  │                                                  │  └────────────┬────────────┘  │
│               │ Sockets / LRC │                                                  │               │ Kafka         │
│  ┌────────────┴────────────┐  │                                                  │               │               │
│  │         WM_WS_E         │◄─┼───────────────────────(Kafka: ws_commands / ws_events)──────────┘               │
│  │     (Engine / Actuador) │  │                                                                                  │
│  └─────────────────────────┘  │                                                                                  │
└───────────────────────────────┘                                                  └───────────────────────────────┘
```

El sistema se divide en tres subsistemas funcionales:
1. **Core System (`WM_Central`)**: El nodo coordinador que gobierna el estado global, valida los permisos y almacena las auditorías.
2. **Watering Stations (`WM_WS_M` + `WM_WS_E`)**: Entidades locales divididas en dos procesos: el **Monitor** (supervisión y enlace de sockets) y el **Motor** (actuador de electroválvula, caudalímetro y streaming Kafka).
3. **Field Operators (`WM_FO`)**: Clientes móviles para el personal de parques y jardines.

---

## 3. Protocolos de Comunicación

### 3.1. Sockets TCP con Empaquetado `<STX><DATA><ETX><LRC>`

Para las comunicaciones directas de supervisión de salud y autenticación (`Monitor <-> Central` y `Monitor <-> Motor`), se ha implementado un protocolo a nivel de aplicación sobre TCP:

1. **Handshake inicial**: Intercambio de bytes de control `ENQ` (`0x05`) y respuesta `ACK` (`0x06`).
2. **Formato de trama**:
   $$\text{Trama} = \langle\text{STX}\rangle + \text{DATA} + \langle\text{ETX}\rangle + \langle\text{LRC}\rangle$$
   - `STX`: Start of Text (`0x02`).
   - `DATA`: Mensaje codificado en UTF-8 con campos delimitados por `#` (ej: `REGISTRO#WS-01#River Park`).
   - `ETX`: End of Text (`0x03`).
   - `LRC`: Byte de verificación calculado como el XOR acumulativo de todos los bytes de `DATA`:
     $$\text{LRC} = \bigoplus_{i=1}^{n} \text{byte}_i$$
3. **Flujo de confirmación**:
   - Si el receptor valida que $\text{LRC}_{\text{calculado}} == \text{LRC}_{\text{recibido}}$, responde inmediatamente con `ACK` (`0x06`).
   - Si no coincide, responde con `NACK` (`0x15`) y el emisor reintenta la transmisión hasta 3 veces.

### 3.2. Streaming de Eventos con Apache Kafka

Para desacoplar a la Central de los múltiples operarios y motores, se utiliza Apache Kafka en modo KRaft estructurado en 5 tópicos JSON:

| Tópico | Emisor | Receptor | Propósito |
| :--- | :--- | :--- | :--- |
| `fo_requests` | `WM_FO` | `WM_Central` | Envío de peticiones de riego (`op`, `ws`, `duration`). |
| `fo_replies` | `WM_Central` | `WM_FO` | Notificaciones de progreso (`STEP`), autorización (`AUTH`), denegación (`DENY`), telemetría y resumen final (`SUMMARY`). |
| `ws_commands` | `WM_Central` | `WM_WS_E` | Órdenes centrales hacia motores (`START`, `STOP`, `BLOCK`, `ACTIVATE`). |
| `ws_events` | `WM_WS_E` | `WM_Central` | Eventos de estado (`STARTED`, `FINISHED`) y telemetría por segundo (`TELEMETRY`). |
| `ws_status` | `WM_Central` | Broadcast (`WM_FO`) | Snapshot global con el estado actualizado de todas las estaciones. |

**Solución al problema de rebalanceo:** Cada cliente genera un grupo de consumo con offset `earliest` y filtra internamente los mensajes anteriores a su marca de tiempo de inicio (`timestamp`), evitando la pérdida de respuestas en arranques simultáneos.

---

## 4. Base de Datos Relacional SQLite

La base de datos relacional `central.db` gestionada por `WM_Central` garantiza la persistencia con las siguientes tablas:

```sql
-- Operarios autorizados
CREATE TABLE operators (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- Estaciones registradas y última ubicación
CREATE TABLE stations (
    id TEXT PRIMARY KEY,
    location TEXT NOT NULL,
    status TEXT NOT NULL
);

-- Historial y auditoría de consumos
CREATE TABLE watering_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ws_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    total_vol REAL NOT NULL,
    duration_sec INTEGER NOT NULL,
    reason TEXT NOT NULL
);
```

* **Regla de negocio ante reinicios (Requisito 1):** Al arrancar la Central, todas las estaciones existentes en la base de datos se marcan automáticamente en estado `DESCONECTADA` hasta que sus monitores restablecen la conexión por socket y se autentican.

---

## 5. Descripción de Componentes Software

### 5.1. `WM_Central.py`
- Servidor concurrente de sockets TCP con hilos dedicados por monitor.
- Consumidores y productores Kafka independientes ejecutados en hilos demonio.
- Panel en consola con secuencias de escape ANSI según los colores exigidos por la práctica (Verde, Verde parpadeante, Rojo, Naranja, Gris).
- Servidor HTTP multihilo embebido en el puerto 8080 (o variable `$PORT` en Railway) que sirve la interfaz web de tiempo real ([`dashboard.html`](file:///home/sofia/Escritorio/6to/SD/Practica%20waterManagment/dashboard.html)).

### 5.2. `WM_WS_M.py` (Monitor)
- Cliente socket hacia Central con reconexión automática y latidos periódicos (`PING`) cada 3 segundos.
- Servidor socket local hacia el motor (`WM_WS_E`) para asignación de ID (`ASSIGN`) y sondeo de salud cada 1 segundo (`HEALTH`).
- Notificación inmediata de incidencias: `LEAK` ante fallos o pérdida de señal del motor, y `FIXED` tras la resolución.

### 5.3. `WM_WS_E.py` (Motor / Engine)
- Simulación física de caudalímetro (~8.5 a 11.5 L/min) y electroválvula.
- Emisión continua de telemetría cada segundo durante el riego.
- Inyección interactiva de averías mediante la tecla **`k`**, forzando la respuesta `KO` al monitor y cerrando la electroválvula de inmediato.
- Riego manual (`s`) y detención manual (`f`) desde la estación.

### 5.4. `WM_FO.py` (Field Operator)
- Interfaz de comandos para solicitar riegos puntuales y consultar estaciones disponibles (`l`).
- Modo por lotes (`--file operaciones.txt`): Lee secuencialmente las solicitudes y, tras la conclusión de cada servicio (éxito o denegación), **espera estrictamente 4 segundos** antes de procesar la siguiente orden.

---

## 6. Cuadro de Mandos y Visualización

La interfaz de usuario implementa las dos representaciones de la especificación:

1. **Mapa Urbano Interactivo (Figura 2)**:
   - Representación vectorial SVG de los 4 distritos urbanos (*River Park*, *Riverside Gardens*, *Central Park*, *North Roundabout*).
   - Estaciones con códigos de color dinámicos y animación de ondas concéntricas (*ripples*) durante el suministro de agua.
   - Panel de detalle con medición de caudal, volumen y estado de subsensores.
2. **Cuadro de Mandos de Escritorio (Figura 1)**:
   - Tabla completa con filtros, leyendas y resumen de contadores del sistema.
   - Terminal inferior con el volcado de trazas de eventos del sistema central.
3. **Simulador de Operario integrado**:
   - Pestaña web para probar y demostrar el ciclo completo sin necesidad de abrir múltiples terminales.

---

## 7. Guía de Despliegue Paso a Paso

### 7.1. Despliegue Local con Docker

```bash
# 1. Situarse en el directorio del proyecto
cd "Practica waterManagment"

# 2. Levantar el broker Apache Kafka
docker compose up -d kafka

# 3. Iniciar la Central (Sockets en puerto 5000, Web en 8080)
.venv/bin/python WM_Central.py 5000 localhost:9092 --web-port 8080

# 4. Iniciar una Estación de Riego (Monitor en una terminal, Engine en otra)
.venv/bin/python WM_WS_M.py 6001 localhost:5000 WS-01 --loc "River Park Norte"
.venv/bin/python WM_WS_E.py localhost:9092 localhost:6001

# 5. Iniciar la Aplicación del Operario
.venv/bin/python WM_FO.py localhost:9092 FO-01
```

Alternativamente, se puede utilizar el script interactivo [`run_demo.sh`](file:///home/sofia/Escritorio/6to/SD/Practica%20waterManagment/run_demo.sh) o ejecutar cualquier módulo en contenedor Docker mediante [`docker_run.sh`](file:///home/sofia/Escritorio/6to/SD/Practica%20waterManagment/docker_run.sh).

### 7.2. Despliegue en Cloud (Railway - Escenario Figura 4)

1. **Servicio Kafka**: Desplegado en Railway mediante plantilla oficial `apache/kafka` en modo KRaft, habilitando un **TCP Proxy** en el puerto 9092 (ej: `monorail.proxy.rlwy.net:19092`).
2. **Servicio WM_Central**: Desplegado conectando el repositorio de GitHub con el `Dockerfile`. Se expone un dominio público HTTP para la web y un TCP Proxy en el puerto 5000 para los sockets.
3. **Clientes en el Laboratorio**: Los PCs de laboratorio apuntan al endpoint del TCP Proxy de Railway tanto para sockets como para Kafka.

---

## 8. Guía de Pruebas y Casos de Evaluación

| Caso de Prueba | Acción Realizada | Comportamiento Esperado y Verificado |
| :--- | :--- | :--- |
| **1. Autenticación y Registro** | Arrancar `WM_WS_M` con ID `WS-01`. | Central recibe `REGISTRO#WS-01`, responde `STATUS#OK` y pasa el estado de `DESCONECTADA` a `DISPONIBLE` (Verde). |
| **2. Riego Normal** | En `WM_FO`, ejecutar `r WS-01 10`. | Central valida disponibilidad, autoriza el riego, `WM_WS_E` abre la válvula y emite telemetría por segundo. Al concluir, emite `SUMMARY` y guarda en SQLite. |
| **3. Denegación de Servicio** | Solicitar riego con operario no registrado (`FO-99`) o estación ocupada. | Central deniega la solicitud inmediatamente indicando el motivo por Kafka (`DENY`). |
| **4. Detección de Fuga** | Pulsar tecla `k` en `WM_WS_E` durante un riego. | Motor responde `KO` a la comprobación `HEALTH`. Monitor envía `LEAK` a Central. El riego se aborta de inmediato y el estado pasa a **FUGA (Rojo)**. |
| **5. Recuperación de Avería** | Pulsar tecla `k` de nuevo en `WM_WS_E`. | Motor vuelve a responder `OK`. Monitor envía `FIXED`. Central restituye el estado a **DISPONIBLE (Verde)**. |
| **6. Control Central Arbitrario** | En Central, ejecutar `b WS-01` y luego `a WS-01`. | La estación se bloquea pasando a **FUERA DE SERVICIO (Naranja)** y posteriormente se reactiva a Verde. |
| **7. Ejecución por Lotes** | En `WM_FO`, ejecutar con `--file operaciones.txt`. | Las órdenes se ejecutan secuencialmente, respetando un retardo estricto de **4 segundos** entre servicios. |

---

## 9. Conclusiones

La solución desarrollada cumple íntegramente con todos los requisitos funcionales, técnicos y de arquitectura fijados en la especificación de la práctica WaterManagement (Release 1). La combinación de sockets TCP para control determinista y Apache Kafka para streaming masivo de eventos proporciona un sistema distribuido robusto, resiliente ante fallos de nodos y desacoplado, preparado para su despliegue real en entornos de producción.
