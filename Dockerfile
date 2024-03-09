FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean 

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

RUN pip install apache-airflow==2.8.2 apache-airflow-providers-apache-spark==4.7.1 pyspark==3.4.0