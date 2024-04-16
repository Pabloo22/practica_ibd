import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from utils import extract_from_madrid_url, load_df_to_raw


def extract_mambiente_data(url):
    """
    Extrae el contenido del fichero horario.txt de la web del Ayuntamiento de
    Madrid, el cual contiene información sobre la calidad del aire en la
    ciudad de Madrid y se actualiza cada hora. La URL del fichero es la
    siguiente:

        https://www.mambiente.madrid.es/opendata/horario.csv

    > El Sistema Integral de la Calidad del Aire del Ayuntamiento de Madrid
    > permite conocer en cada momento los niveles de contaminación atmosférica
    > en el municipio.
    >
    > En este conjunto de datos puede obtener la información actualizada en
    > tiempo real, actualizándose estos datos cada hora, y esta actualización
    > se realizará entre los minutos 20 y 30.
    >
    > **Importante:** estos datos en tiempo real son los que salen
    > automáticamente de las estaciones de medición y están pendientes de
    > revisión y validación.


    Para más información sobre el contenido del fichero, consultar el
    documento [Interprete_ficheros_calidad_del_aire_global.pdf](https://shorturl.at/ahmSZ).
    """
    return extract_from_madrid_url(url)


def load_air_quality(df, folder_path, filename):
    """
    Guarda el DataFrame `df` en un fichero CSV en la carpeta `folder_path` con
    el nombre `filename`.

    El dataframe debería contener la informaciçon del CSV incremental
    correspondiente en la carpeta de información cruda "/raw".
    """
    return load_df_to_raw(df, folder_path, filename)


default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}

with DAG(
    dag_id="mambiente_hourly_data",
    schedule_interval="35 22 * * *",
    tags=["Ayuntamiento_Madrid", "Environmental_Data"],
    default_args=default_args,
) as dag:

    @task(task_id="extract_mambiente_hourly")
    def extract():
        url = "https://www.mambiente.madrid.es/opendata/horario.csv"

        extracted_data = extract_mambiente_data(url)

        return extracted_data

    @task(task_id="load_df_to_raw")
    def load_raw(df):
        # Retrieve the Pandas DataFrame
        print(df.head())

        # Define the folder path
        folder_path = "/opt/airflow/raw"

        # Generate a unique filename based on the current timestamp
        timestamp_str = (str(pendulum.now(tz="UTC"))[:10]).replace("-", "_")
        print(timestamp_str)
        filename = f"air_quality_{timestamp_str}.csv"
        print(filename)

        load_air_quality(df, folder_path, filename)

    extract_task = extract()

    load_raw_task = load_raw(extract_task)

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print(
            "Data extraction completed successfully"
        ),
    )

    # pylint: disable=pointless-statement
    extract_task >> load_raw_task >> end_task
