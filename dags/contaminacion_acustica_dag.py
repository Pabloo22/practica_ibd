import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


def extract_contaminacion_acustica(url):
    """
    Extrae el CSV incremental de datos 'Contaminación acústica. Datos diarios (acumulado)'
    que proporciona el Ayuntamiento de Madrid. La URL correspondiente es:

        https://datos.gob.es/es/catalogo/l01280796-contaminacion-acustica-datos-diarios1

    La información sobre las distintas variables medidas se encuentra en el siguiente enlace:

        https://datos.madrid.es/FWProjects/egob/Catalogo/MedioAmbiente/Ruido/Ficheros/INTERPRETE%20DE%20ARCHIVO%20DE%20DATOS%20DIARIOS%20RUIDOS.pdf

    Las estaciones de medición de ruido se encuentran en el siguiente enlace:

        https://datos.madrid.es/egob/catalogo/211346-1-estaciones-acusticas.csv

    """

    # https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
    # Best Practices - Section: Top level Python Code
    import numpy as np
    import pandas as pd

    df = pd.read_csv(url, sep=";", decimal=",")

    print("Show 5 first rows: ")
    print(df.head())

    return df


def load_contaminacion_acustica(df, folder_path, filename):
    """
    Carga el CSV incremental correspondiente en la carpeta de información cruda "/raw"
    """

    # Best Practices - Section: Top level Python Code
    import os

    # For Debugging Purposes
    print(os.getcwd())

    # Construct the full file path
    file_path = os.path.join(folder_path, filename)

    # Filter DataFrame by date
    year = pendulum.now().year
    month = pendulum.now().month
    day = pendulum.now().day

    df = df[(df["anio"] == year) & (df["mes"] == month) & (df["dia"] == day)]

    # Write DataFrame to CSV
    df.to_csv(file_path, index=False)
    print(f"DataFrame written to {file_path}")


default_args = {
    "start_date": pendulum.datetime(2024, 3, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}

with DAG(
    dag_id="contaminacion_acustica",
    schedule_interval="35 23 * * *",
    tags=["Ayuntamiento_Madrid"],
    default_args=default_args,
) as dag:

    @task(task_id="extract_from_url")
    def extract():

        url = "https://datos.madrid.es/egob/catalogo/215885-10749127-contaminacion-ruido.csv"

        scraped_data = extract_contaminacion_acustica(url)

        return scraped_data

    @task(task_id="load_df_to_raw")
    def load_raw(df):
        # Retrieve the Pandas DataFrame
        print(df.head())

        # Define the folder path
        folder_path = "/opt/airflow/raw"

        # Generate a unique filename based on the current timestamp
        timestamp_str = (str(pendulum.now(tz="UTC"))[:10]).replace("-", "_")
        print(timestamp_str)
        filename = f"contaminacion_acustica_{timestamp_str}.csv"
        print(filename)

        load_contaminacion_acustica(df, folder_path, filename)

    extract_task = extract()

    load_raw_task = load_raw(extract_task)

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    extract_task >> load_raw_task >> end_task
