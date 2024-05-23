import json

from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow import DAG
from airflow.decorators import task
import pendulum

RICH_DIR = "/opt/airflow/rich"
OUTPUT_PATH = f"{RICH_DIR}/noticias_with_sentiment.json"

default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}


@task(task_id="load_to_mongodb")
def load_to_mongodb():
    mongo_hook = MongoHook("mongo-conn")
    client = mongo_hook.get_conn()
    db = client["noticias_db"]
    collection = db["noticias"]

    with open(
        OUTPUT_PATH,
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)
        for item in data:
            existing_news = collection.find_one({"title": item["title"]})
            if existing_news:
                collection.update_one({"title": item["title"]}, {"$set": item})
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
    # pylint: disable=invalid-name
    load_to_mongodb_task = load_to_mongodb()
    # pylint: disable=pointless-statement
    sentiment_analysis_task >> load_to_mongodb_task
