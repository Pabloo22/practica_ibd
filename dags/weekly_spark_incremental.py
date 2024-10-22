import os
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from airflow.operators.postgres_operator import PostgresOperator
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
    )

    @task(task_id="last_Sunday")
    def last_Sunday():
        today = datetime.today()
        # Monday is 0 and Sunday is 6, so we need to add 1 to the weekday and mod by 7
        offset = (today.weekday() + 1) % 7
        last_sunday = today - timedelta(days=offset)
        formatted_date = last_sunday.strftime("%Y_%m_%d")

        return formatted_date

    @task(task_id="filename_rich")
    def get_csv_filename(folder_path):
        # folder_path = '/opt/airflow/rich/air_quality_2024_04_20'
        csv_files = [
            file
            for file in os.listdir(folder_path)
            if file.endswith(".csv")
        ]
        print(csv_files[0])
        return csv_files[0]

    last_Sunday_task = last_Sunday()

    filename_task = get_csv_filename(
        f"/opt/airflow/rich/final_df_{last_Sunday_task}"
    )

    load_rich_task = PostgresOperator(
        task_id="load_data_to_postgres",
        # Connection ID configured in Airflow for PostgreSQL
        postgres_conn_id="postgres_default",
        sql=f"""COPY fact_measure (metric_id, station_id, measure, date)
            FROM '/rich/final_df_{last_Sunday_task}/{filename_task}'
            WITH (FORMAT CSV, HEADER TRUE)""",
    )
    # Note: we don't use parameters in the SQL query because there is no
    # risk for SQL injection in this case. Since this is a prototype, we
    # prefer to keep the code simple and readable.

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    # transform_task >> load_rich_task >> end_task
    # load_rich_task >> end_task
    # pylint: disable=pointless-statement
    (
        transform_task
        >> last_Sunday_task
        >> filename_task
        >> load_rich_task
        >> end_task
    )
