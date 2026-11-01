# Registro de Consultas y Prompts a la IA
## Práctica WaterManagement — Sistemas Distribuidos (Curso 26/27)

    1. Tengo que hacer una práctica de Sistemas Distribuidos con sockets y Kafka. Te pego el enunciado, ¿me haces un resumen de lo que hay que entregar?
    2. ¿Qué módulos me piden exactamente? Me lío con Central, Monitor, Engine y FO
    3. Explícame con un esquema sencillo cómo se comunican entre sí los módulos WM_Central, WM_WS y WM_FO
    4. ¿Qué parte va por sockets y qué parte va por Kafka según el enunciado?
    5. No entiendo bien la diferencia entre el Monitor y el Engine de una estación, ¿por qué hacen falta los dos?
    6. ¿Qué estados puede tener una estación de riego y cuándo pasa de uno a otro?
    7. Hazme una tabla con los estados (disponible, regando, fuga, fuera de servicio, desconectada) y qué color tiene cada uno en el panel
    8. ¿Qué pasa si el Monitor está conectado pero el Engine no? ¿Es fuga o desconectada?
    9. Dime en qué orden me conviene implementar los módulos para no atascarme
    10. Somos dos en el equipo, ¿cómo nos repartimos el trabajo para no pisarnos el código?
    11. ¿Qué estructura de carpetas me recomiendas para el repositorio?
    12. Quiero hacerlo en Python, ¿qué librerías voy a necesitar para sockets, Kafka y base de datos?
    13. ¿Qué versión de Python es mejor usar para que funcione igual en el laboratorio y en mi portátil?
    14. ¿Cómo creo un entorno virtual y un requirements.txt para que mi compañera pueda instalar lo mismo?
    15. Explícame qué es un broker de Kafka y qué es un tópico, que nunca lo he usado
    16. ¿Cuántos tópicos necesito para las peticiones de los operarios, las respuestas, las órdenes a las estaciones y la telemetría?
    17. Propón un diseño de los mensajes que se intercambian, en forma de lista, antes de escribir código
    18. ¿Qué debería guardar en la base de datos? ¿Solo el historial de riegos o también las estaciones?
    19. Hazme un plan de trabajo por semanas teniendo en cuenta que la entrega es en noviembre
    20. Resúmeme los criterios de la guía de corrección para ir comprobándolos según avance
    21. El enunciado dice que las tramas llevan STX, datos, ETX y LRC. ¿Qué es el LRC y cómo se calcula?
    22. Escríbeme una función en Python que construya una trama con STX, el mensaje, ETX y el LRC
    23. Ahora la función contraria: que lea una trama, compruebe el LRC y devuelva el mensaje
    24. ¿El LRC se calcula incluyendo el STX o no? Me da dudas con lo que pone el enunciado
    25. Si el LRC no coincide, ¿qué se supone que debo contestar? ¿Un NACK?
    26. Haz una prueba rápida que construya una trama, le cambie un byte y compruebe que se detecta el error
    27. A veces recv me devuelve medio mensaje, ¿cómo lo gestiono?
    28. ¿Y si me llegan dos tramas pegadas en la misma lectura?
    29. Hazme una clase pequeña que acumule bytes y vaya sacando tramas completas
    30. El enunciado habla de un handshake inicial con ENQ y ACK, ¿cómo lo implemento en cliente y en servidor?
    31. ¿Qué hago si el otro lado no contesta nunca? Quiero poner un timeout pero no sé dónde
    32. ¿Cuál es la forma correcta de cerrar un socket para que no queden conexiones colgadas?
    33. Me sale un ResourceWarning de unclosed socket al terminar, ¿cómo lo evito?
    34. ¿Se puede hacer sendall desde dos hilos sobre el mismo socket sin problemas?
    35. Quiero una función genérica que abra una conexión, envíe una trama y espere la respuesta
    36. Necesito leer direcciones tipo host:puerto de los argumentos, ¿cómo lo parseo bien y con valores por defecto?
    37. Explícame cómo funciona KafkaProducer en Python y cómo compruebo que el mensaje se ha enviado de verdad
    38. ¿Qué diferencia hay entre enviar y esperar el future.get y no esperar?
    39. Hazme una función que cree el productor y reintente si Kafka todavía no ha arrancado
    40. Lo mismo para el consumidor, quiero que no confirme los mensajes automáticamente
    41. ¿Qué pasa con el group_id? ¿Todos los módulos pueden compartir el mismo?
    42. Si un mensaje de Kafka no es un JSON válido, mi programa se cae. ¿Cómo lo protejo?
    43. Quiero poner los nombres de los tópicos como constantes en un único archivo para no equivocarme escribiéndolos
    44. Añade unos colores ANSI para los logs, pero que no rompan la terminal si no los soporta
    45. Revisa este common.py y dime si ves algo mal o que se pueda mejorar [código pegado]
    46. Tengo un error: BrokenPipeError al enviar una trama cuando el otro lado cierra, ¿cómo lo trato?
    47. Vamos con WM_Central. ¿Cómo monto un servidor TCP que acepte varios monitores a la vez?
    48. ¿Mejor un hilo por conexión o asyncio? Son pocas estaciones pero quiero que sea sencillo
    49. Explícame cómo debería registrar Central a un Monitor cuando se conecta
    50. ¿Qué verifica Central al recibir el registro de una estación? ¿Que exista en la base de datos?
    51. Al conectarse un Monitor sin Engine la estación debería quedar en fuga, ¿cómo lo organizo?
    52. ¿Cuándo pasa a disponible? Cuando el Monitor avisa de que el Engine ya está bien
    53. Crea las tablas de SQLite para estaciones y para el historial de riegos
    54. ¿Qué campos debería tener el historial para poder comprobar luego todo en la memoria?
    55. ¿Cómo guardo la ubicación de cada estación y la recupero al reiniciar Central?
    56. Una vez reiniciado Central quiero que todas las estaciones salgan como desconectadas hasta que se conecten, ¿cómo lo hago?
    57. Necesito acceder a SQLite desde varios hilos, me da database is locked. ¿Qué hago?
    58. ¿Una conexión por hilo o una global con un lock?
    59. Escribe la lógica para validar una petición de riego, que solo se acepte si la estación está disponible
    60. Si la estación está ocupada, ¿qué mensaje le devuelvo al operario?
    61. Quiero que si el Engine no confirma el inicio en 10 segundos la petición se cancele
    62. Escribe el consumidor de Kafka que recibe la telemetría y actualiza el caudal y el volumen de cada estación
    63. Cuando termina un riego, ¿cómo guardo el resumen en la base de datos?
    64. ¿Cómo evito guardar dos veces el mismo riego si Kafka me repite el mensaje?
    65. Dame una idea para identificar de forma única cada riego, algún tipo de identificador de transacción
    66. Quiero confirmar los mensajes de Kafka solo después de haberlos guardado en la base de datos
    67. ¿Qué pasa si falla la escritura en SQLite? No quiero decirle al operario que todo fue bien
    68. Ya tenía una base de datos de pruebas sin la columna del identificador, ¿cómo la actualizo sin perder datos?
    69. Cuando se cierra el socket de un Monitor, la estación debe pasar a desconectada. ¿Dónde lo detecto?
    70. Si el Monitor se cae durante un riego, ¿qué hago con el volumen acumulado hasta ese momento?
    71. Implementa los comandos de bloquear y activar una estación desde Central
    72. ¿Cómo evito que una estación bloqueada se reactive sola si su Engine falla y se recupera?
    73. Quiero un bloquear todas y activar todas. En activar todas, que no active las que no tienen Engine
    74. Una estación sin Engine que se activa debe volver a fuga, no a disponible, ¿cómo lo controlo?
    75. Haz que el log de Central ponga la hora y el identificador de la estación en cada línea
    76. Si un Monitor se reconecta rápido, el socket antiguo sigue ahí. ¿Cómo lo sustituyo?
    77. Revisa estas consultas SQL y dime si alguna es vulnerable a inyección [código pegado]
    78. Cierra Central de forma limpia con Ctrl+C: hilos, sockets y base de datos
    79. No entiendo por qué mi hilo consumidor de Kafka se queda parado, mira este código [código pegado]
    80. Pasamos al Monitor. Tiene que hablar con Central como cliente y con el Engine como servidor, ¿cómo lo estructuro?
    81. Quiero que el Monitor pregunte al Engine por su estado cada segundo
    82. ¿Qué respuesta debería dar el Engine? ¿OK y KO?
    83. Si el Engine responde KO el Monitor debe avisar a Central de la fuga
    84. Cuando el Engine vuelve a responder OK, avisar de que la avería está resuelta
    85. Si el socket del Engine se cierra, tratarlo como que no hay Engine
    86. Añade un mensaje de latido hacia Central para que sepa que sigue vivo
    87. Si Central se cae, el Monitor debe intentar reconectar solo cada pocos segundos
    88. ¿Cómo evito un bucle de reintentos que me ponga la CPU al 100 por cien?
    89. Quiero que el Monitor le asigne su identificador al Engine al conectarse
    90. Los parámetros del Monitor deben venir por línea de comandos: puerto, dirección de Central, id y ubicación
    91. Si Central reinicia, el Monitor tiene que volver a registrarse automáticamente
    92. Pasa también el estado de bloqueo en el mensaje de salud para que el Engine lo sepa
    93. ¿Se puede capturar Ctrl+C en el Monitor para avisar a Central antes de cerrar?
    94. Ahora el Engine. ¿Cómo simulo un caudal que varíe un poco entre 8 y 12 litros por minuto?
    95. ¿Cómo calculo el volumen acumulado cada segundo a partir del caudal?
    96. Quiero que el riego se pare solo cuando pase el tiempo pedido, sin depender de Kafka
    97. Crea un hilo temporizador que cierre la electroválvula al terminar la duración
    98. El Engine tiene que publicar un evento de inicio, luego telemetría cada segundo y al final un evento de fin
    99. ¿Cómo garantizo ese orden aunque Kafka vaya lento?
    100. Si falla el envío del evento de inicio, no debería mandar telemetría sin sentido
    101. Haz un menú por consola con opciones para regar a mano, parar, simular avería y salir
    102. Con la tecla k quiero alternar avería y funcionamiento normal
    103. Si hay avería durante un riego, se cierra la válvula y se termina con motivo LEAK
    104. Si el Monitor se desconecta en mitad de un riego, cerrar inmediatamente la válvula
    105. Si el estado es bloqueado, que no deje iniciar ningún riego
    106. Me sale un deadlock entre el temporizador y el hilo de inicio, mira este código [código pegado]
    107. ¿Puedes revisar los locks del Engine? Creo que estoy bloqueando demasiado tiempo
    108. Si Kafka está caído cuando termina un riego, ¿cómo guardo el resumen para enviarlo después?
    109. ¿Cómo vacío esa cola de pendientes cuando Kafka vuelva, sin enviar dos veces lo mismo?
    110. Si me llegan dos intentos de vaciado a la vez, ¿cómo evito que se pisen?
    111. El Engine debe consumir las órdenes de Kafka solo de su estación. ¿Cómo las filtro?
    112. Implementa las órdenes de iniciar, parar, bloquear y activar desde Central
    113. Un riego de duración cero lo quiero tratar como manual, hasta que pulse f
    114. Redondea la telemetría para que el JSON no lleve decenas de decimales
    115. Ahora WM_FO, el operario. ¿Cómo hago que envíe una petición de riego por Kafka?
    116. Quiero que solo muestre los mensajes que van dirigidos a su identificador
    117. ¿Cómo espero la autorización de Central sin bloquear toda la aplicación?
    118. Haz que espere el resumen final antes de dejarme pedir otro riego
    119. Necesito un comando para listar las estaciones y su estado
    120. El comando de riego tiene la forma r WS-01 15, ¿cómo lo parseo bien con espacios de más?
    121. Si escribo mal el identificador de estación quiero que me avise antes de enviar nada
    122. Añade el modo por fichero: leer operaciones.txt y lanzar los riegos uno tras otro
    123. Entre un riego y el siguiente tiene que esperar cuatro segundos, ¿cómo lo meto?
    124. Si Central deniega un riego en modo fichero, que lo anote y siga con el siguiente
    125. Si Central tarda más de cinco segundos en responder, que avise al usuario
    126. Si cierro el operario a mitad de un riego, el riego debe seguir, ¿lo puedes comprobar?
    127. Cuando lo vuelvo a abrir con el mismo identificador, que reciba el resumen que se perdió
    128. ¿Cómo uso los offsets de Kafka para eso?
    129. Muestra el resumen final con el volumen, la duración y el motivo de fin
    130. Comprueba al arrancar que el broker de Kafka responde antes de enseñar el menú
    131. Argumentos por línea de comandos: dirección de Kafka, id del operario y opción de fichero
    132. Dos operarios a la vez no deberían verse los mensajes, ¿cómo lo verifico?
    133. Haz un script bash para lanzar tres operarios pidiendo riegos en distintas estaciones
    134. Necesito un panel web en Central, ¿cómo lo sirvo desde el propio programa en Python?
    135. Quiero un endpoint que devuelva el estado de todas las estaciones en JSON
    136. Diseña un HTML sencillo con una tabla de estaciones: id, ubicación, estado, caudal, volumen y operario
    137. Los colores tienen que ser verde, verde parpadeante, rojo, naranja y gris según el estado
    138. Añade unos contadores arriba con cuántas estaciones hay en cada estado
    139. Haz que se refresque cada segundo sin recargar la página entera
    140. Quiero botones para regar y bloquear cada estación, y otros generales
    141. Añade una consola de mensajes en la parte inferior con los eventos de Central
    142. Quiero también una vista de mapa con las estaciones agrupadas por zonas
    143. Crea una pestaña de simulador de operario para pedir riegos desde la web sin abrir terminal
    144. Y otra pestaña con el historial de riegos de la base de datos
    145. El dashboard no debe bloquear los hilos de Kafka y sockets, ¿cómo lo separo?
    146. Quiero poder dar de alta una estación nueva sin reiniciar Central
    147. Ese alta solo debería aceptarse desde la propia máquina, ¿cómo lo restrinjo?
    148. Valida que el identificador de la estación tenga el formato WS- y al menos dos dígitos
    149. Los botones de bloquear parecen no hacer nada, revisa este JavaScript [código pegado]
    150. Mejora los estilos para que se vea bien en pantallas pequeñas
    151. Quiero un tema oscuro con tipografía legible
    152. Necesito un Dockerfile para ejecutar cualquiera de los módulos con Python
    153. ¿Qué imagen base uso para que pese poco?
    154. Copia primero los requisitos para aprovechar la caché de capas
    155. Hazme el docker-compose con Kafka, Zookeeper y Central
    156. Kafka tarda en arrancar y Central falla porque no lo encuentra, ¿cómo ordeno el arranque?
    157. Añade un healthcheck a Kafka para que Central espere a que esté listo
    158. Quiero un script docker_run.sh que reciba el módulo y sus parámetros y lo ejecute en un contenedor
    159. Que el script admita monitor, engine y fo con los argumentos que ya tienen
    160. Necesito que la base de datos se conserve cuando reinicie el contenedor de Central
    161. Estoy en el laboratorio de AWS y no puedo conectar con Kafka desde fuera, ¿qué configuro en los listeners?
    162. ¿Qué puertos tengo que abrir en el grupo de seguridad?
    163. ¿Cómo despliego Central y Kafka en una instancia y las estaciones en los portátiles?
    164. Me da PermissionError al escribir la base de datos en el volumen, ¿qué pasa?
    165. ¿Cómo veo los logs de todos los contenedores a la vez?
    166. El contenedor de Central parece activo pero no responde en el 8080, ¿cómo lo diagnostico?
    167. Si paro un contenedor con docker stop quiero que el Engine cierre la válvula antes
    168. Escribe un run_demo.sh que levante todo para la demostración
    169. ¿Cómo limpio contenedores y volúmenes antiguos antes de empezar de cero?
    170. Comprueba en el script que docker y docker compose están instalados antes de empezar
    171. ¿Funcionará igual en un ordenador con Apple Silicon que en uno con Intel?
    172. Con docker ps no sé cuál es el contenedor de cada estación, ¿cómo los nombro?
    173. Quiero comprobar desde un contenedor que puedo llegar al puerto 5000 de Central
    174. Quiero tests automáticos sin necesidad de arrancar Kafka real, ¿cómo lo simulo?
    175. Haz un broker falso en memoria con tópicos y consumidores
    176. Y unos sockets falsos para conectar Central y Monitor dentro del test
    177. Escribe un test del flujo completo: operario pide riego, Central autoriza, Engine riega y el resumen llega a SQLite
    178. Prueba que si el reloj de Central va adelantado dos minutos la duración sigue saliendo bien
    179. Test: el Monitor se desconecta durante un riego y la válvula se cierra por MONITOR_DOWN
    180. Test: avería en el Engine, la estación pasa a fuga y vuelve a disponible al recuperarse
    181. Test: Central se detiene en mitad de un riego, el riego termina y no se aceptan nuevos
    182. Test: Kafka va lento, ¿la válvula se cierra a la hora justa?
    183. Test de que el evento de fin nunca se envía antes que el de inicio
    184. Test de activar todas con estaciones en distintos estados
    185. Test de que un bloqueo se mantiene aunque el Engine se desconecte y vuelva
    186. Test del alta dinámica de una estación nueva sin alterar las demás
    187. Test del endpoint de alta: que rechace peticiones externas e identificadores como WS-7
    188. Test de que un timeout de Kafka no hace que el Monitor detecte una falsa desconexión
    189. Test de riego de 2 segundos con Kafka bloqueado 4 segundos: ¿se cierra a los 2?
    190. Test de Kafka caído durante un riego: el resumen debe guardarse cuando vuelva
    191. Test de que el mismo evento de fin duplicado solo se cuenta una vez
    192. Test con la base de datos bloqueada: Central no debe confirmar el mensaje
    193. Test de migración desde una base de datos antigua sin perder filas
    194. Este test falla de forma intermitente, ¿puede ser una condición de carrera? [salida pegada]
    195. Me sale AssertionError en test_watering_flow, mira la salida [salida pegada]
    196. ¿Cuánto debería tardar la batería completa? A mí me tarda más de un minuto
    197. Explica en pocas líneas qué prueba cada test para ponerlo en la memoria
    198. Vuelve a ejecutar mentalmente la guía de corrección y dime qué puntos podrían fallar
    199. Hemos detectado que a veces los riegos duran un segundo de más, ¿dónde puede estar el fallo?
    200. Corrige el control de duración del Engine y añade una prueba que lo cubra
    201. Al caerse Kafka el Monitor dejaba de contestar, ¿qué lo bloquea?
    202. Después de la caída de Kafka quedan estaciones en regando cuando ya no riegan, ¿cómo lo evito?
    203. Revisa todo el código buscando sockets sin cerrar y excepciones sin capturar
    204. Ayúdame a estructurar la memoria: introducción, arquitectura, componentes, protocolos, decisiones, despliegue y pruebas
    205. Redacta la introducción y los objetivos de la práctica a partir de este enunciado
    206. Explica la arquitectura general para la memoria con un tono formal pero claro
    207. Describe cada módulo en un apartado propio, con sus responsabilidades
    208. Explica el protocolo de tramas con STX, ETX y LRC como si fuera para alguien que no conoce la práctica
    209. Describe los tópicos de Kafka y para qué sirve cada uno
    210. Explica el esquema de la base de datos y por qué guardamos esos campos
    211. Justifica nuestras decisiones de diseño, sobre todo la separación entre Monitor y Engine
    212. Escribe el apartado de resiliencia: qué pasa cuando cae cada componente
    213. Redacta la guía de despliegue paso a paso con los comandos que usamos
    214. Convierte mis notas de cada prueba en un apartado con objetivo, procedimiento, resultado y conclusión
    215. Revisa la redacción de la prueba 3 para que quede más clara
    216. Estas pruebas están numeradas distinto que en la guía, ¿cómo lo ordeno?
    217. Sugiere un pie de figura para cada captura que te paso
    218. Hazme una lista de los defectos que encontramos y cómo los corregimos
    219. Escribe unas conclusiones breves de lo aprendido
    220. ¿Qué archivos tengo que entregar y cuáles no?
    221. Revisa la memoria completa y dime qué falta o qué está repetido
    222. Corrige faltas de ortografía y de estilo sin cambiar el contenido
    223. Haz un README con los pasos para ejecutar la práctica desde cero
    224. Prepárame unas notas para explicar el sistema en la defensa
