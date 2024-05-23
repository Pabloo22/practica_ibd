# Práctica de Infraestructuras de Big Data

En esta práctica, se busca realizar un procesamiento batch de datos sobre la situación actual de la comunidad de Madrid. En concreto, se pretende extraer datos sobre la meteorología, la calidad del aire, la contaminación acústica y noticias recientes de periódicos locales. La infraestructura permitirá la extracción, procesamiento y almacenamiento de estos datos; con la finalidad de realizar una visualización de los mismos a través de un dashboard interactivo creado mediante Streamlit. Este dashboard, permitirá visualizar la situación en Madrid que hubo en un día concreto.

Para ello, con una frecuencia diaria, se extraerán los datos mediante Apache Airflow (que será la herramienta que orquestará todo el flujo de datos desde la extracción hasta la propuesta de valor). Estos incrementales diarios se almacenarán en la carpeta `/raw`, cuya finalidad es que sea utilizada a modo de data lake (como AWS S3). Tras ello, se realizará semanalmente (todos los lunes) un procesamiento paralelo de los datos en crudo con Apache Spark (PySpark) para, no solamente enriquecerlos; sino también, combinarlos con el histórico generado guardando los datos estructurados en una base de datos relacional (PostgreSQL) y los no estructurados, en una base de datos no relacional (MongoDB). De estas dos soluciones de almacenamiento, beberá Streamlit para la visualización interactiva de los datos (dashboard)

<div align="center">
<image src="images/Architecture_Overview.png" width="80%">
</div>

## Despliegue :rocket:
> [!WARNING]  
> Es posible que sea necesario otorgar los permisos correspondientes a las carpetas `./logs` y `./raw` para que Apache Airflow pueda escribir en ellas desde el interior del contenedor. 
> 
> En Linux, se puede hacer mediante el comando `sudo chmod -R 777 ./logs ./dags` antes de realizar el despliegue. Este comando cambiará los permisos de los directorios logs y dags, así como de todos los archivos y subdirectorios contenidos dentro de ellos, otorgando permisos completos (lectura, escritura y ejecución) a todos los usuarios.

Para desplegar el entorno, debemos ejecutar el siguiente comando:

```bash
docker compose up -d --build
```

Al desplegar el entorno por primera vez, **es posible que debamos inicializar manualmente el contenedor asociado al servidor web de Apache Airflow** (`webserver`). No obstante, tras la adición de "restart: on-failure" en la nueva versión, esto ya no es necesario.

Tras ello, podremos acceder a las interfaces de Airflow y Apache Spark:
- Apache Airflow UI: http://localhost:8080
- Apache Spark UI: http://localhost:9090

Las credenciales de acceso a Apache Airflow son las siguientes:
- Username:`admin`
- Password: `admin`

Finalmente, para establecer la conexión de Apache Airflow con Apache Spark, debemos especificar manualmente los siguientes parámetros en el panel `/Admin/Connections` de Airflow:
 - Connection Id: `spark-conn`
 - Connection Type: `Spark`
 - Host: `spark://spark-master`
 - Port: `7077` 

 También, debemos especificar la conexión con Postgres en el panel `/Admin/Connections` de Airflow:
 - Connection Id: `postgres_default`
 - Connection Type: `Postgres`
 - Host: `postgres-datawarehouse`
 - Schema: `postgres`
 - Login: `postgres`
 - Password: `password`
 - Port: `5432`
  
  Por último, debemos añadir la conexión a MongoDB en este mismo panel:
  - Connection Id: `mongo-conn`
  - Connection Type: `MongoDB`
  - Host: `mongodb`
  - Port: `27017`

<div align="center">
<image src="images/mongo-conection.png" width="80%">
</div>

> [!NOTE]  
> En el [Dockerfile](Dockerfile) se han especificado todas las versiones de todas las dependencias del proyecto meticulosamente para evitar conflictos de versiones en el futuro.
> 
> El uso de [Poetry](https://python-poetry.org/) se utiliza únicamente desde el punto de vista de desarrollo, para gestionar las dependencias del proyecto. De esta forma, se permite la realización de pruebas en local sin necesidad de desarrollar desde el interior del contenedor. Estas pruebas se realizan principalmente en los notebooks contenidos en la carpeta `./notebooks`. No obstante, ninguno de los contenedores desplegados en producción hace uso de Poetry o de los notebooks.



## Componentes de la Infraestructura :gear:

Para desplegar la infraestructura de Big Data compuesta por Apache Airflow y Apache Spark de manera eficiente y coherente, se ha utilizado Docker Compose debido a su capacidad para simplificar el proceso de configuración y lanzamiento de múltiples contenedores que necesitan trabajar juntos de forma integrada. 

Docker Compose permite definir en un solo archivo `docker-compose.yml` todas las configuraciones de los servicios, incluidos los volúmenes, redes y dependencias, asegurando que cada componente de la infraestructura sea desplegado en un entorno aislado y reproducible. Gracias a esta virtualización, se garantiza que las mismas versiones de software y configuraciones exactas sean utilizadas en cada despliegue, eliminando los problemas comunes relacionados con las discrepancias de "funciona en mi máquina". Además, la gestión de múltiples servicios que deben iniciarse en un orden específico y configurarse para interactuar entre ellos se maneja automáticamente, reduciendo la complejidad y aumentando la eficiencia del proceso de despliegue.

Esta infraestructura está compuesta por los siguientes componentes:

### Red
La infraestrctura utiliza una red de tipo *bridge* que permite la comunicación entre los distintos contenedores. 

### Apache Spark

El clúster de Spark está compuesto por tres servicios:
- **`spark-master`**: Este servicio actúa como el nodo maestro en un clúster de Spark. Es responsable de administrar y distribuir las tareas entre los nodos trabajadores (workers).
- **`spark-worker-1`**, **`spark-worker-2`**, **`spark-worker-3`** y **`spark-worker-4`**: Estos cuatro servicios actúan como nodos trabajadores en el clúster de Spark. Se conectan al nodo maestro y son los encargados de ejecutar las tareas asignadas. Cada trabajador está configurado con 2 núcleos y 1GB de memoria RAM.

> [!NOTE]
> **Apache Spark** es un motor de procesamiento de datos que permite análisis complejos y computación distribuida. Se justifica su uso por las siguientes razones:
>
> - **Procesamiento en memoria**: Spark realiza el procesamiento de datos en memoria, lo que lo hace mucho más rápido que otros sistemas que acceden al disco, como Hadoop MapReduce.
>
> - **Facilidad para procesamiento de datos a gran escala**: Spark se diseñó para manejar petabytes de datos distribuidos a través de múltiples nodos de manera eficiente. Esto lo hace ideal para el escenario descrito, donde se manejan datos incrementales diarios de múltiples fuentes.


### Apache Airflow
El entorno de Airflow está compuesto por dos servicios:
- **`webserver`**: Este servicio corre el servidor web de Airflow, que proporciona la interfaz de usuario para administrar y visualizar flujos de trabajo (DAGs). Está configurado para ejecutarse en el puerto 8080, haciendo la interfaz accesible a través de `http://localhost:8080`.
- **`scheduler`**: Este servicio corre el programador (scheduler) de Airflow, que es responsable de programar las tareas definidas en los DAGs y ejecutarlas según sus dependencias y programación. Antes de iniciar el scheduler, realiza migraciones de base de datos y crea un usuario admin para acceder a la interfaz de Airflow.
- **`postgres`**: Este servicio funciona como la base de datos para Airflow. Guarda la información sobre el estado de las tareas, los DAGs y otros metadatos necesarios para que Airflow funcione correctamente. La base de datos está configurada con las credenciales `airflow` tanto para el usuario como para la contraseña.


> [!NOTE]
> **Apache Airflow** es una plataforma open-source diseñada para programar y orquestar flujos de trabajo complejos mediante programación en Python. Algunas de las razones por las cuales hemos utilizado Apache Airflow son las siguientes:
>
> - **Programación y orquestación flexibles**: Airflow permite definir flujos de trabajo, conocidos como DAGs (Directed Acyclic Graphs), que son visualizaciones de las tareas y sus dependencias. Esto es fundamental para procesos donde las tareas deben ejecutarse todos los días a una hora específica.
>
> - **Reintentos y manejo de errores**: Airflow ofrece mecanismos robustos para el manejo de fallos, incluyendo reintentos automáticos y alertas. Esto asegura que los flujos de datos no se detengan por fallos puntuales, lo cual es esencial en entornos de producción donde la continuidad y la fiabilidad son cruciales.
>
> - **Interfaz de usuario intuitiva**: La interfaz de usuario de Airflow nos permite visualizar la ejecución de los flujos de trabajo, monitorear el progreso y revisar registros de las tareas ejecutadas.
> 
> - **Escalabilidad y Integración**: Airflow se integra fácilmente con otras herramientas y plataformas, como Apache Spark para el procesamiento de datos y sistemas de bases de datos para el almacenamiento de datos. Esto permite escalar las operaciones de procesamiento conforme al volumen de datos crece, y facilita la implementación de mejoras sin grandes sobrecostos de adaptación.

### PostgreSQL


- **`pgAdmin4`**: Este servicio proporciona una interfaz web para administrar y visualizar la base de datos PostgreSQL. Se puede acceder a la interfaz en http://localhost:16543. Las credenciales de acceso son:
    - Correo electrónico: `teste@teste.com`
    - Contraseña: `teste`
- **`postgres-datawarehouse`**: Este servicio es la base de datos relacional para nuestro datawarehouse (OLAP). En él, se guardará todos los datos estructurados en un modelo dimensional de Kimball para optimizar la lectura de los mismos (visualización desde Streamlit). 
      
Al ingresar a la interfaz, se solicitará una contraseña para acceder al servidor de la base de datos, la cual es `password`.

### Volúmenes
Se configuran varios volúmenes para mantener la persistencia de los datos y el código entre reinicios de los contenedores. En concreto, varios volúmenes se mapean a las carpetas locales para que Spark y Airflow puedan acceder a scripts, DAGs, registros de ejecución, datos crudos y enriquecidos (`./jobs`, `./dags`, `./logs`, `./raw`, `./rich`).

## DAGs de Extracción de Apache Airflow :arrows_counterclockwise:

Para cargar los datos anteriormente mencionados, se realizan una serie de DAGs de Airflow para extraer y cargar la información cruda en la carpeta `/raw`. Cada DAG está compuesto de dos tareas: la extracción de la información y la carga de la misma en dicha carpeta.

A continuación se detalla la lista de DAGs:

- `gob_meteor` DAG: extrae el CSV incremental de datos 'Datos meteorológicos. Datos en tiempo real' 
    que proporciona el Ayuntamiento de Madrid. La URL correspondiente es:

        https://datos.gob.es/es/catalogo/l01280796-datos-meteorologicos-datos-en-tiempo-real1

    La información sobre las distintas variables medidas es la siguiente:

        81 - VELOCIDAD VIENTO
        82 - DIR. DE VIENTO
        83 - TEMPERATURA
        86 - HUMEDAD RELATIVA
        87 - PRESION BARIOMETRICA
        88 - RADIACION SOLAR
        89 - PRECIPITACIÓN
    
    Para más información, se puede consultar la siguiente web: 
        https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=2ac5be53b4d2b610VgnVCM2000001f4a900aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD&vgnextfmt=default

- `mambiente_hourly_data` DAG: extrae el contenido del fichero [horario.txt](https://www.mambiente.madrid.es/opendata/horario.txt) de la web del Ayuntamiento de
    Madrid, el cual contiene información sobre la calidad del aire en la
    ciudad de Madrid y se actualiza cada hora. Los datos son accesibles en el siguiente [enlace](https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=41e01e007c9db410VgnVCM2000000c205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD).

    A continuación se muestra un extracto del contenido de la web que proporciona los datos:

    > El Sistema Integral de la Calidad del Aire del Ayuntamiento de Madrid
    > permite conocer en cada momento los niveles de contaminación atmosférica
    > en el municipio.
    >
    > En este conjunto de datos puede obtener la información actualizada en
    > tiempo real, actualizándose estos datos cada hora, y esta actualización
    > se realizará entre los minutos 20 y 30.
    >
    > **Importante:** estos datos en tiempo real son los que salen
    > automáticamente de las estaciones de medición y están pendientes de
    > revisión y validación.


    Para más información sobre el contenido del fichero, consultar el
    documento [Interprete_ficheros_calidad_del_aire_global.pdf](https://shorturl.at/ahmSZ).

- `noticias` DAG: está diseñado para automatizar la extracción de noticias recientes de los periódicos *El País* y *ABC*, específicamente de sus secciones de Madrid. Utiliza técnicas de web scraping para obtener el contenido de las páginas web de estos medios.

    Estos datos se almacenan en un archivo JSON, cuyo nombre incorpora la fecha y hora de la extracción, para su posterior análisis o uso. Los datos siguen el siguiente formato:

    ```python
    [
        {
            "title": "El secretario general del PP de Ayuso se citó con la pareja de la líder en plena polémica por su caso de fraude fiscal",
            "link": "https://elpais.com/espana/madrid/2024-04-04/el-secretario-general-del-pp-de-ayuso-se-cito-con-la-pareja-de-la-lider-en-plena-polemica-por-su-caso-de-fraude-fiscal.html",
            "description": "Aunque los conservadores reducen la denuncia de la Fiscalía a un asunto que afecta a un particular, altos cargos del partido y del Gobierno se han implicado en la gestión de la crisis política y reputacional",
            "journal": "EL_PAIS"
        },
        ...
    ]
    ```

    El DAG se ejecuta diariamente a las 22:35 UTC, garantizando la recopilación de noticias a lo largo del día. Las noticias son filtradas para solo incluir las noticias del día de ejecución del DAG.

    Para más información sobre *El País* y *ABC*, sus secciones de noticias de Madrid pueden ser accedidas a través de los siguientes enlaces:
    - El País: [Sección Madrid de El País](https://elpais.com/espana/madrid/)
    - ABC: [Sección Madrid de ABC](https://www.abc.es/espana/madrid/)

- `contaminacion_acustica` DAG: extrae los datos de contaminación acústica de la web del Ayuntamiento de Madrid. Los datos son accesibles en el siguiente [enlace](https://datos.madrid.es/egob/catalogo/215885-10749127-contaminacion-ruido.csv).

    A continuación se muestra un extracto del contenido del fichero que proporciona los datos:

    > Este conjunto de datos ofrece la información desde el 1 de enero de 2014, y se va actualizacion diariamente. Todos los días laborables se publican los datos correspondientes al día anterior. En el caso de los días laborables inmediatamente posteriores a días no laborables, se publican los datos correspondientes a los días pendientes no laborables. La unidad en la que se proporcionan todos los datos (LAeq, LAS01, LAS10, LAS50, LAS90 y LAS99) es el decibelio ponderado en base A (dBA). En este portal tambien están disponibles otros conjuntos de datos relacionados con la contaminación acústica: Contaminación acústica. Datos históricos mensuales Contaminación acústica: Estaciones de medida Asímismo, puedes encontrar más información sobre estos datos en el Portal de transparencia > Ruido.

    El DAG se ejecuta diariamente a las 22:35 UTC, descargándose el fichero de datos completo. Este fichero contiene todos los datos de contaminación acústica desde el 1 de enero de 2014 hasta el día anterior a la ejecución del DAG. En el caso de los días festivos, los datos correspondientes a esos días se publican en el siguiente día laborable, por lo que el DAG se encarga de descargar los datos correspondientes a esos días festivos en el día laborable siguiente.

    La  información sobre las estaciones de medida se puede encontrar en el siguiente [enlace](https://datos.madrid.es/egob/catalogo/211346-1-estaciones-acusticas.csv).

    Para más información sobre el contenido del fichero, consultar el siguiente [documento](https://datos.madrid.es/FWProjects/egob/Catalogo/MedioAmbiente/Ruido/Ficheros/INTERPRETE%20DE%20ARCHIVO%20DE%20DATOS%20DIARIOS%20RUIDOS.pdf)

- `meteor` DAG: se encarga de extraer datos meteorológicos de varios distritos de Madrid y cargarlos en un archivo CSV. La ejecución del DAG se programa diariamente a las 22:35 UTC.

  A continuación, se detallan las tareas del DAG:

    >1. **extract_data**: Esta tarea se encarga de extraer los datos meteorológicos de los distritos especificados utilizando la API de open-meteo.com. Los distritos y sus coordenadas están definidos dentro de la tarea. Los datos extraídos corresponden a la información disponible hace 2 días anteriores y se procesan y almacenan   
    >en un DataFrame de Pandas.
    >
    >2. **export_raw_df**: Una vez que los datos se han extraído y procesado, se exportan a un archivo CSV en la carpeta "/opt/airflow/raw". El nombre del archivo se genera con la fecha y hora actual.
    >
    >3. **end**: Esta tarea final simplemente imprime un mensaje indicando que las tareas han sido completadas con éxito.

   Los datos meteorológicos se obtienen de open-meteo.com, una plataforma que proporciona información detallada sobre varias variables meteorológicas. El DataFrame generado incluye información como la temperatura, la humedad relativa, la presión barométrica, la velocidad del viento y la radiación solar, entre otras variables.
   Para más información sobre la fuente de datos, visita [open-meteo.com](https://open-meteo.com).

## Explicación de Flujo Completo de ETL
Tras realizar esta extracción incremental diaria (volcando los correspondientes datos en la carpeta /raw), se ejecuta un DAG de Airflow con PySpark semanalmente para cargar los nuevos incrementales en el datawarehouse. Este datawarehouse se ha desplegado en PostgreSQL puesto que no solamente soporta funcionalidades de OLTP; sino también de OLAP (nuestro caso de uso).

En el datawarehouse, se ha optado por implementar un modelo Kimball. Todos los detalles de este modelo (las tablas a implementar, los mappeos correspondientes de antiguos IDs con los nuevos, la normalización de algunas estaciones repetidas, etc) se pueden encontrar en el Excel “Final_Kimball_Model”. A modo general, el modelo posee las siguientes tablas:

<div align="center">
<image src="images/Kimbal_Model.svg" width="80%">
</div>

- **Measures (Fact Table)**: actualizada cada semana con los nuevos datos de los incrementales. Posee los siguientes campos: “measure_id”, una clave primaria autoincremental (que también actúa como clave surrogada); “metric_id”, una clave foránea que conecta con la tabla dimensional de Metrics; “station_id”, una clave foránea que conecta con la tabla dimensional de Stations; “measure”, valor cuantitativo de la medida; y “date”, fecha en la que se ha obtenido la medida (muy útil para los posteriores filtros en la parte de visualización)
- **Stations (Dimensional Table)**: se ingesta una única vez al desplegar la infraestructura de datos en el script de inicialización de PostgreSQL. Posee los siguientes campos: “station_id”, clave primaria; “station_name”, nombre de la estación; “district_name”, nombre del distrito; “longitude”, longitud (coordenada); y “latitude”, latitud (coordenada)
- **Metrics (Dimensional Table)**: se ingesta una única vez al desplegar la infraestructura de datos en el script de inicialización de PostgreSQL. Posee los siguientes campos: “metric_id”, clave primaria; y “metric_name”, nombre de la métrica.


Respecto al procesamiento semanal, todos los lunes por la mañana (a las 9:00 en UTC) se ejecuta el DAG **“weekly_spark_incremental”**. Este DAG se compone principalmente de dos tasks:
- **“transform_task”**: se ejecuta un job de PySpark donde se realiza el respectivo procesamiento para  transformar los datos en /raw de la semana pasada al formato correspondiente a la FACT table de Measures. Este job de PySpark con 4 worker nodes se realiza en el script “enrich_spark.py”. En él, se realizan las transformaciones necesarias para todas las tablas (mappeos de station_id y metric_id, cálculo agregado de la medida “measure” realizando la media aritmética, concatenación de strings, procesamiento de fechas, filtros específicos, etc). Finalmente, todos los datos enriquecidos con el nuevo formato se guardan en el volumen /rich. Como se puede apreciar, los volúmenes /raw y /rich están funcionando como data lakes (AWS S3, ADLS, etc) en nuestra arquitectura delta.
- **“load_rich_task”**: tras transformar los nuevos incrementales al formato de la FACT table en la task de PySpark, se deben insertar en la base de datos de PostgreSQL con la sentencia de COPY leyendo los datos enriquecidos del volumen /rich.



## Las 5 V's del Big Data :bar_chart:
El proyecto aborda las 5 Vs del Big Data de la siguiente manera:

- **Volumen**: Esta dimensión se sacrifica en este proyecto. No obstante, el proyecto podría manejar grandes volúmenes de datos potencialmente, ya que se extraen datos de múltiples fuentes y se procesan diariamente. Los datos meteorológicos, de calidad del aire, de contaminación acústica y de noticias se recopilan y almacenan en bruto en la carpeta `/raw`, lo que podría resultar en un volumen significativo de datos a lo largo del tiempo.
- **Velocidad**: El proyecto se centra en la velocidad de procesamiento de los datos, ya que se extraen y procesan diariamente. Los DAGs de Airflow se programan para ejecutarse diariamente a una hora específica, lo que garantiza la actualización regular de los datos. Además, se consigue una gran rapidez en la extracción y procesamiento de datos gracias a la elección de las herramientas anteriormente explicadas (gracias al uso de Apache Spark y Apache Airflow)
- **Variedad**: El proyecto maneja una variedad de datos de diferentes fuentes, así como datos de tipo estructurado y no estructurado. Los datos meteorológicos, de calidad del aire y de contaminación acústica son estructurados, mientras que los datos de noticias son no estructurados.
- **Veracidad**: El proyecto se centra en garantizar la veracidad de los datos, ya que se extraen de fuentes oficiales, como el Ayuntamiento de Madrid. Con respecto a los datos de noticias, se obtienen datos de dos de los principales periódicos de España, *El País* y *ABC*, cada uno con diferentes enfoques y perspectivas.
- **Valor**: El proyecto busca proporcionar valor a través de la visualización de los datos en un dashboard interactivo creado con Streamlit. Una de las principales utilidades de este dashboard es permitir a los usuarios conocer como fue la situación en Madrid en el pasado. Conocer los datos relacionados con la contaminación es esencial para tomar decisiones informadas sobre la salud y el bienestar. Los datos relacionados con la meteorología y las noticias nos permiten comprender mejor el contexto en el que se producen los eventos relacionados con la contaminación.
  
## Miembros del Equipo :busts_in_silhouette:

**Grupo 2:**
- Pablo Ariño
- Álvaro Laguna
- Ismail Merabet
- Roberto Mulas

*Nombres ordenados alfabéticamente por apellido.*

## Referencias
- https://hub.docker.com/r/bitnami/spark/tags
- https://medium.com/swlh/using-airflow-to-schedule-spark-jobs-811becf3a960
- https://github.com/pyjaime/docker-airflow-spark
- https://github.com/mk-hasan/Dockerized-Airflow-Spark?tab=readme-ov-file
- https://github.com/airscholar/SparkingFlow/tree/main
- https://github.com/soumilshah1995/Learn-How-to-Integerate-Hudi-Spark-job-with-Airflow-and-MinIO/tree/main
