import json
import os
from datetime import timedelta, datetime

from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import DAG
from airflow.decorators import task
import pendulum


RICH_DIR = "/opt/airflow/rich"
OUTPUT_DIR = f"{RICH_DIR}/noticias_with_sentiment"

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}


@task(task_id="last_sunday_task")
def get_last_sunday():
    today = datetime.today()
    # Monday is 0 and Sunday is 6, so we need to add 1 to
    # the weekday and mod by 7
    offset = (today.weekday() + 1) % 7
    last_sunday = today - timedelta(days=offset)
    formatted_date = last_sunday.strftime("%Y_%m_%d")

    return formatted_date


@task(task_id="load_to_mongodb")
def load_to_mongodb(folder_path: str):
    mongo_hook = MongoHook("mongo-conn")
    client = mongo_hook.get_conn()
    db = client["noticias_db"]
    collection = db["noticias"]

    json_file_paths = [
        file for file in os.listdir(folder_path) if file.endswith(".json")
    ]
    for json_file in json_file_paths:
        json_file_path = os.path.join(folder_path, json_file)
        with open(json_file_path, "r", encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                existing_news = collection.find_one({"title": item["title"]})
                if existing_news:
                    collection.update_one(
                        {"title": item["title"]}, {"$set": item}
                    )
                else:
                    collection.insert_one(item)


with DAG(
    dag_id="load_noticias_to_mongodb_dag",
    schedule_interval="0 9 * * 1",
    default_args=default_args,
    catchup=False,
) as dag:
    sentiment_analysis_task = SparkSubmitOperator(
        task_id="run_sentiment_analysis",
        application="jobs/sentiment_analysis.py",
        conn_id="spark-conn",
        verbose=True,
    )

    last_sunday_task = get_last_sunday()
    # pylint: disable=invalid-name
    folder_path_ = f"{OUTPUT_DIR}_{last_sunday_task}"
    load_to_mongodb_task = load_to_mongodb(folder_path_)

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    # pylint: disable=pointless-statement
    (
        sentiment_analysis_task
        >> last_sunday_task
        >> load_to_mongodb_task
        >> end_task
    )
