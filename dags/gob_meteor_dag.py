import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from utils import extract_from_madrid_url, load_df_to_raw


def extract_gob_meteor(url):
    """
    Extrae el CSV incremental de datos 'Datos meteorológicos. Datos en tiempo real'
    que proporciona el Ayuntamiento de Madrid. La URL correspondiente es:

        https://datos.gob.es/es/catalogo/l01280796-datos-meteorologicos-datos-en-tiempo-real1

    La información sobre las distintas variables medidas es la siguiente:
        81 - VELOCIDAD VIENTO
        82 - DIR. DE VIENTO
        83 - TEMPERATURA
        86 - HUMEDAD RELATIVA
        87 - PRESION BARIOMETRICA
        88 - RADIACION SOLAR
        89 - PRECIPITACIÓN

    Para más información, se puede consultar la siguiente web:
        https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=2ac5be53b4d2b610VgnVCM2000001f4a900aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD&vgnextfmt=default

    """
    return extract_from_madrid_url(url)


def load_gob_meteor(df, folder_path, filename):
    """
    Carga el CSV incremental correspondiente en la carpeta de información
    cruda "/raw"
    """
    return load_df_to_raw(df, folder_path, filename)


default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}

with DAG(
    dag_id="gob_meteor",
    schedule_interval="35 22 * * *",
    tags=["Ayuntamiento_Madrid"],
    default_args=default_args,
) as dag:

    @task(task_id="extract_from_url")
    def extract():

        url = "https://datos.madrid.es/egob/catalogo/300392-11041819-meteorologia-tiempo-real.csv"
        scraped_data = extract_gob_meteor(url)

        return scraped_data

    @task(task_id="load_df_to_raw")
    def load_raw(df):
        print(df.head())
        folder_path = "/opt/airflow/raw"

        # Generate a unique filename based on the current timestamp
        timestamp_str = (str(pendulum.now(tz="UTC"))[:10]).replace("-", "_")
        print(timestamp_str)
        filename = f"gob_meteor_{timestamp_str}.csv"
        print(filename)

        load_gob_meteor(df, folder_path, filename)

    extract_task = extract()

    load_raw_task = load_raw(extract_task)

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Jobs completed successfully"),
    )

    # pylint: disable=pointless-statement
    extract_task >> load_raw_task >> end_task
