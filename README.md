# practica_ibd
Práctica de la asignatura de Infraestructura de Big Data

## 1. Inicialización de Entorno
Para desplegar el entorno, debemos ejecutar el siguiente comando: \
docker compose up -d --build

Al desplegar el entorno por primera vez, debemos inicializar manualmente el contenedor asociado al servidor web de Apache Airflow ("webserver"). Tras ello, podremos acceder a las interfaces de Airflow y Apache Spark:
- Apache Airflow UI: http://localhost:8080
- Apache Spark UI: http://localhost:9090

Finalmente, para establecer la conexión de Apache Airflow con Apache Spark, debemos especificar manualmente los siguientes parámetros en el panel "/Admin/Connections" de Airflow:
 - Connection Id: spark-conn
 - Connection Type: Spark
 - Host: spark://spark-master
 - Port: 7077 

Además, es relevante mencionar que se han especificado todas las versiones de todas las dependencias del proyecto meticulosamente para evitar conflictos de versiones en el futuro.

## 2. Desarrollo de Solución ETL

## 3. Propuesta de Valor

## Referencias
- https://hub.docker.com/r/bitnami/spark/tags
- https://medium.com/swlh/using-airflow-to-schedule-spark-jobs-811becf3a960
- https://github.com/pyjaime/docker-airflow-spark
- https://github.com/mk-hasan/Dockerized-Airflow-Spark?tab=readme-ov-file
- https://github.com/airscholar/SparkingFlow/tree/main
- https://github.com/soumilshah1995/Learn-How-to-Integerate-Hudi-Spark-job-with-Airflow-and-MinIO/tree/main