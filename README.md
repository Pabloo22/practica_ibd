# Práctica de Infraestructura de Big Data



## Descripción de la Infraestructura Digital :page_facing_up:
**TODO:** descripción detallada de la infraestructura, donde se justifique su diseño.

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


## 2. Desarrollo de Solución ETL

## 3. Propuesta de Valor

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
