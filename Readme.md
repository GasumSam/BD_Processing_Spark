##Proyecto Big Data Processing

### José Manuel Guzmán Gutiérrez



Siguiendo la plantilla del ejercicio final en clase, he procurado simplificar el proceso, ya que los jobs requeridos son de menor complejidad.

- Selección de columnas requeridas en base a los datos obtenidos a través de Kafka.

- Agrupación por cada uno de los parámetros requeridos y suma de los valores de las fila "bytes"

- En al Batch, complejidad a la hora de valorar los datos que debían enriquecerse con los metadatos, ya que el join solo es necesario para calcular el total de bytes por email de usuario.



### Observaciones

El proyecto final ha estado condicionado por los errores en la configuración de Kafka en el Dataproc, después de solucionar aspectos como la recepción de mensajes a través de un consumidor (desde la propia shell de GCP), ha sido imposible corregir los errores para la conexión en local con el Dataproc.

Asímismo, Kafka se mostraba muy inestable en la instancia de GCP, manteniéndose unos dos minutos. Cada vez que se caía era necesario parar las aplicaciones y hacer una limpieza de los archivos temporales de Kafka y Zookeper para poder volver a levantarlos. El proceso de error se repetía, lo que unido a la imposibilidad de conectar con local, hacía imposible el testeo.

Intenté en la tarde del domingo rehacer el proyecto levantando Kafka desde Docker, pero el tiempo era insuficiente. He preferidop entregar algo, antes que no entregar nada. Gracias por la molestia de preparar el proyecto.

