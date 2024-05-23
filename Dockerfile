FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean 

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    apache-airflow==2.8.2 \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyspark==3.4.0 \
    beautifulsoup4==4.12.3 \
    lxml==5.2.0 \
    pymongo==4.7.2 \
    apache-airflow-providers-mongo==4.1.0 \
    torch==2.3.0 \
    transformers==4.41.0

# Download and cache the model
RUN python -c "from transformers import pipeline; pipeline('sentiment-analysis', model='mrm8488/electricidad-small-finetuned-sst2-es')"
