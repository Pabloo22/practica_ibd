from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="sentiment_analysis_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    sentiment_analysis_task = SparkSubmitOperator(
        task_id="run_sentiment_analysis",
        application="/opt/bitnami/spark/jobs/sentiment_analysis.py",
        conn_id="spark-conn",
        verbose=True,
    )

    sentiment_analysis_task  # pylint: disable=pointless-statement
