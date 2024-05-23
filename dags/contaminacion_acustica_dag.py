import os

import pendulum
import pandas as pd
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

    df = pd.read_csv(url, sep=";", decimal=",", encoding="ISO-8859-1")

    reverse_column_mapping = {
        "NMT": "NMT",
        "Año": "anio",
        "Mes": "mes",
        "Dia": "dia",
        "Tipo": "tipo",
        "LAeq": "LAEQ",
        "L1": "LAS01",
        "L10": "LAS10",
        "L50": "LAS50",
        "L90": "LAS90",
        "L99": "LAS99",
    }

    df = df.rename(columns=reverse_column_mapping)

    print("Show 5 first rows: ")
    print(df.head())

    return df


def load_contaminacion_acustica(df, folder_path):
    """
    Carga el CSV incremental correspondiente en la carpeta de información cruda "/raw"
    """

    start_date = pd.to_datetime(
        str(default_args["start_date"])[:10], format="%Y-%m-%d"
    )

    # Filter DataFrame by date
    run_date = pd.to_datetime(
        os.environ.get("AIRFLOW_CTX_EXECUTION_DATE")[:10]
    )

    files = os.listdir(folder_path)
    files = [
        file for file in files if file.startswith("contaminacion_acustica")
    ]

    dates = [
        "".join(file.split("_")[2:]).split(".")[0].replace("_", "-")
        for file in files
    ]

    dates = pd.to_datetime(dates)
    print(f"Dates: {dates}")

    last_date = dates.max()
    if last_date is pd.NaT:
        last_date = start_date

    print(f"Last date: {last_date}")
    print(f"Run date: {run_date}")

    for date in pd.date_range(start=last_date, end=run_date, freq="D"):
        print(f"Processing date: {date}")
        print(f"Year: {date.year}")
        print(f"Month: {date.month}")
        print(f"Day: {date.day}")
        # Construct the full file path
        assert date.month != 1
        timestamp_str = (str(date)[:10]).replace("-", "_")
        filename = f"contaminacion_acustica_{timestamp_str}.csv"
        file_path = os.path.join(folder_path, filename)
        # Write DataFrame to CSV
        df_filtered = df[
            (df.anio == date.year)
            & (df.mes == date.month)
            & (df.dia == date.day)
        ]
        if not df_filtered.empty:
            df_filtered.to_csv(file_path, index=False)
            print(f"DataFrame written to {file_path}")


default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}

with DAG(
    dag_id="contaminacion_acustica",
    schedule_interval="35 22 * * *",
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
        print(f"Start_date {dag.__dict__}")
        print(str(default_args["start_date"])[:10])
        print(str(default_args["start_date"]))

        load_contaminacion_acustica(df, folder_path)

    extract_task = extract()

    load_raw_task = load_raw(extract_task)

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    # pylint: disable=pointless-statement
    extract_task >> load_raw_task >> end_task
