# Práctica de Infraestructura de Big Data

## 1. Despliegue :rocket:
> [!WARNING]  
> Es posible que sea necesario otorgar los permisos correspondientes a las carpetas `./logs` y `./raw` para que Apache Airflow pueda escribir en ellas desde el interior del contenedor. 
> 
> En Linux, se puede hacer mediante el comando `sudo chmod -R 777 ./logs ./dags` antes de realizar el despliegue. Este comando cambiará los permisos de los directorios logs y dags, así como de todos los archivos y subdirectorios contenidos dentro de ellos, otorgando permisos completos (lectura, escritura y ejecución) a todos los usuarios.

Para desplegar el entorno, debemos ejecutar el siguiente comando:

```bash
docker compose up -d --build
```

Al desplegar el entorno por primera vez, es posible que debamos inicializar manualmente el contenedor asociado al servidor web de Apache Airflow (`webserver`). 

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

> [!NOTE]  
> En el [Dockerfile](Dockerfile) se han especificado todas las versiones de todas las dependencias del proyecto meticulosamente para evitar conflictos de versiones en el futuro.
> 
> El uso de [Poetry](https://python-poetry.org/) se utiliza únicamente desde el punto de vista de desarrollo, para gestionar las dependencias del proyecto. De esta forma, se permite la realización de pruebas en local sin necesidad de desarrollar desde el interior del contenedor. Estas pruebas se realizan principalmente en los notebooks contenidos en la carpeta `./notebooks`. No obstante, ninguno de los contenedores desplegados en producción hace uso de Poetry o de los notebooks.


## 2. Descripción de la Infraestructura Digital :page_facing_up:
En esta práctica, se busca realizar un procesamiento batch de datos sobre la meteorología de la ciudad de Madrid. Para ello, primero se extraerán, a nivel diario, datos muy variados y heterogéneos de una gran diversidad de fuentes mediante Apache Airflow (que será la herramienta que orquestará todo el flujo de datos desde la extracción hasta la propuesta de valor). Estos incrementales diarios se almacenarán en la carpeta `/raw` (información cruda). Tras ello, se realizaría un procesamiento paralelo de los datos en crudo con Apache Spark (PySpark) para, no solamente enriquecerlos; sino también, combinarlos con el histórico generado. Finalmente, los almacenaríamos en una base de datos NoSQL de la cual bebería Streamlit, para dashboards interactivos, o MLFlow, para crear modelos de Machine Learning (nuestras propuestas de valor).

Respecto a la primera parte de la arquitectura, se realizan una serie de DAGs de Airflow para extraer y cargar la información cruda en la carpeta `/raw`. Los datos que se extraen diariamente sobre la meteorología de Madrid son muy variados (indicadores básicos de lluvia o viento, métricas sobre calidad de aire, contaminación acústica, etc). Cada DAG está compuesto de dos tareas: la extracción de la información y la carga de la misma en la carpeta `/raw`. Respecto a la información extraída, se detalla la lista de DAGs a continuación:
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
