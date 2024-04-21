import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}

with DAG(
    dag_id="weekly_spark_incremental",
    schedule_interval="0 9 * * 1",
    tags=["Procesamiento en Spark"],
    default_args=default_args,
) as dag:

    transform_task = SparkSubmitOperator(
        task_id="transform_to_fact_rows",
        conn_id="spark-conn",
        application="jobs/enrich_spark.py",
        verbose=True,
        files="raw/*.csv"

    )

    # load_rich_task = load_rich(transform_task)

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    # transform_task >> load_rich_task >> end_task
    transform_task >> end_task
