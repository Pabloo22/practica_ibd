from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
import json
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


def load_to_mongodb():
    client = MongoClient(host="mongodb", port=27017)
    db = client.noticias_db
    collection = db.noticias

    # Read the transformed JSON files from the rich folder
    with open(
        "/opt/airflow/rich/noticias_with_sentiment/*.json",
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)
        collection.insert_many(data)


with DAG(
    dag_id="load_noticias_to_mongodb_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    load_to_mongodb_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
    )

    load_to_mongodb_task  # pylint: disable=pointless-statement
