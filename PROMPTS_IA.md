# Registro Detallado de Consultas y Prompts a la IA
## Práctica WaterManagement — Sistemas Distribuidos (Curso 26/27)

> **Nota para el profesorado:** En cumplimiento con la normativa de la Universidad sobre el uso transparente de asistentes de Inteligencia Artificial, se recoge aquí la secuencia cronológica y granular de consultas (prompts) realizadas durante la concepción, depuración e implementación de la práctica.

---

### Fase 1: Análisis de Requisitos y Arquitectura General
1. `Léete el PDF de la práctica WaterManagement de Sistemas Distribuidos y hazme un resumen estructurado.`
2. `¿Cuáles son los 4 componentes software principales que tenemos que programar según el enunciado?`
3. `¿Qué tecnología de comunicación une cada componente según el diagrama de la Figura 3?`
4. `¿Por qué el enunciado pide sockets TCP para el monitor y Apache Kafka para el operario y el motor?`
5. `¿Qué argumentos por línea de comandos debe recibir obligatoriamente cada programa?`
6. `¿Qué nombres exactos de archivo y ejecutables exige la guía de la práctica para cada módulo?`
7. `¿Cómo debe ser el escenario de despliegue físico entre Railway en la nube y los PCs del laboratorio?`

---

### Fase 2: Protocolo de Sockets y Verificación de Tramas (LRC)
8. `¿Cómo se define el formato de empaquetado de sockets <STX><DATA><ETX><LRC> en Python?`
9. `¿Qué códigos ASCII/hexadecimales corresponden a STX, ETX, ACK, NACK y ENQ?`
10. `Escribe una función en Python para calcular el LRC como XOR byte a byte de un mensaje.`
11. `¿Cómo hago la lectura de un socket para que lea byte a byte hasta encontrar ETX y luego verifique el LRC?`
12. `Si el LRC recibido no coincide con el calculado, ¿cómo respondo NACK y hago que el emisor reintente 3 veces?`
13. `¿Cómo implemento el handshake inicial cliente/servidor enviando ENQ y esperando ACK antes de transmitir?`
14. `¿Cómo formateamos las tramas de datos separadas por almohadillas (#) según el anexo de sockets?`
15. `Pon todo este protocolo de sockets modularizado en un archivo common.py reutilizable.`

---

### Fase 3: Monitor de la Estación de Riego (WM_WS_M)
16. `Crea la estructura de WM_WS_M.py para que reciba puerto del engine, central_host:puerto e ID de estación.`
17. `¿Cómo hace el monitor para conectarse por socket a Central y enviar REGISTRO#WS-01#Ubicacion?`
18. `¿Cómo mantengo un socket servidor en el monitor esperando a que el Engine de la estación se conecte?`
19. `Cuando el Engine conecta por socket local, ¿cómo le asigna el monitor su ID mediante ASSIGN#WS-01?`
20. `Programa un bucle en el monitor que envíe cada segundo la trama HEALTH al Engine para comprobar salud.`
21. `Si el Engine responde KO o se desconecta, ¿cómo manda el monitor LEAK#WS-01 a Central por socket?`
22. `Si el Engine vuelve a responder OK, ¿cómo avisa el monitor a Central enviando FIXED#WS-01?`
23. `Añade un latido periódico PING#WS-01 cada 3 segundos a Central para que no considere la estación caída.`
24. `¿Cómo hago para que si la Central se reinicia, el monitor reintente la conexión automáticamente?`

---

### Fase 4: Motor y Actuador de la Estación (WM_WS_E)
25. `Crea la estructura de WM_WS_E.py recibiendo el broker Kafka y la dirección del socket del monitor.`
26. `¿Cómo hago para que el Engine espere a recibir su ID por socket del monitor antes de arrancar Kafka?`
27. `¿Cómo simulo un caudalímetro físico para que genere un flujo aleatorio de ~8.5 a 11.5 L/min cada segundo?`
28. `¿Cómo calculo el volumen acumulado en litros en cada segundo de riego y lo publico en Kafka?`
29. `¿Cómo capturo la pulsación de la tecla 'k' en tiempo de ejecución para alternar entre salud OK y KO?`
30. `Si se pulsa 'k' y la estación está regando, ¿cómo corto la electroválvula inmediatamente por avería?`
31. `Añade comandos para iniciar riego manual con 's' y finalizar suministro con 'f' en la propia estación.`
32. `¿Cómo consume el Engine las órdenes START, STOP, BLOCK y ACTIVATE procedentes de Central por Kafka?`

---

### Fase 5: Integración con Apache Kafka y Resiliencia
33. `¿Qué librería de Kafka funciona bien en Python 3.12? kafka-python estándar da fallo con el módulo imp.`
34. `Configura requirements.txt con kafka-python-ng.`
35. `¿Qué tópicos de Kafka necesitamos definir y qué esquema de JSON debe llevar cada uno?`
36. `Crea una función ensure_topics para crear fo_requests, fo_replies, ws_commands, ws_events y ws_status.`
37. `Tengo un problema: a veces el operario no recibe la confirmación de riego si Central responde muy rápido.`
38. `¿Por qué con auto_offset_reset='latest' se pierden mensajes durante el rebalanceo del grupo en Kafka?`
39. `¿Cómo evito esa pérdida usando auto_offset_reset='earliest' y filtrando por timestamp del mensaje?`
40. `Crea funciones helper en common.py para crear productores y consumidores de Kafka de forma limpia.`

---

### Fase 6: Aplicación del Operario de Campo (WM_FO)
41. `Crea WM_FO.py recibiendo el broker de Kafka y el identificador de operario (ej. FO-01).`
42. `¿Cómo publica el operario una petición de riego en fo_requests con op, ws y duración?`
43. `¿Cómo filtra el operario en fo_replies para mostrar solo los mensajes dirigidos a su ID?`
44. `Muestra en tiempo real los mensajes de progreso (STEP), autorización (AUTH), denegación y telemetría.`
45. `¿Cómo implemento el modo por lotes con el argumento --file operaciones.txt?`
46. `El enunciado pide esperar exactamente 4 segundos entre órdenes en --file tras cada servicio, ¿cómo lo programo?`
47. `Añade un comando 'l' para listar todas las estaciones de la red consumiendo el tópico ws_status.`
48. `Asegúrate de que al terminar de procesar el archivo en modo no interactivo el proceso no se quede colgado.`

---

### Fase 7: Lógica Central y Persistencia (WM_Central)
49. `Crea WM_Central.py para escuchar en un puerto de sockets y conectarse al broker de Kafka.`
50. `Diseña el esquema SQLite en central.db con tablas para operarios, estaciones e historial de riegos.`
51. `El requisito 1 dice que al arrancar Central las estaciones de la BD deben salir DESCONECTADAS, ¿cómo lo hago?`
52. `¿Cómo compruebo en SQLite si el operario que solicita riego existe antes de autorizarlo?`
53. `Si la estación pedida está regando, en fuga o fuera de servicio, deniega el riego explicando el motivo.`
54. `Al terminar un riego, guarda en watering_history la fecha, operario, litros consumidos y causa de fin.`
55. `Añade comandos interactivos en Central: 'b' para bloquear, 'a' para activar y 's' para ordenar riego.`
56. `¿Cómo hago para que el comando 's ALL' ordene regar a todas las estaciones disponibles de una vez?`
57. `¿Cómo imprimo el panel en la terminal con los colores ANSI requeridos (verde, parpadeante, rojo, naranja, gris)?`

---

### Fase 8: Cuadro de Mandos Web e Interfaz Gráfica (Figuras 1 y 2)
58. `El enunciado muestra la Figura 1 (cuadro de mandos de escritorio) y Figura 2 (mapa web con parques).`
59. `Dado que Railway corre en contenedores sin entorno gráfico X11, ¿cómo monto un servidor web en WM_Central?`
60. `Diseña un dashboard.html con un mapa SVG oscuro de los 4 parques (River Park, Riverside Gardens, etc.).`
61. `Añade nodos interactivos en el mapa que tengan ondas de agua concéntricas parpadeantes cuando estén regando.`
62. `Añade un panel lateral que muestre telemetría en directo y botones para regar, bloquear o activar.`
63. `Crea una pestaña para ver la tabla completa con el estilo de la Figura 1 y el log de consola.`
64. `Añade un simulador de operario en la web para poder probar riegos fácilmente sin abrir muchas terminales.`
65. `Haz que el frontend consulte /api/status cada 1 segundo para actualizar el estado automáticamente.`

---

### Fase 9: Despliegue con Docker y Railway
66. `Escribe el Dockerfile para empaquetar la aplicación con Python 3.12 y las dependencias.`
67. `Escribe el docker-compose.yml para levantar Apache Kafka 3.8 en modo KRaft y WM_Central en local.`
68. `El enunciado exige que cualquier módulo pueda correrse en Docker, crea un script docker_run.sh.`
69. `Explícame paso a paso cómo configurar Kafka en Railway usando plantillas y TCP Proxy.`
70. `¿Cómo configuramos los puertos en Railway para que los PCs del laboratorio puedan conectar por socket y Kafka?`
