import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


def extract_mambiente_data(url):
    """
    Extrae el contenido del fichero horario.txt de la web del Ayuntamiento de
    Madrid, el cual contiene información sobre la calidad del aire en la
    ciudad de Madrid y se actualiza cada hora. La URL del fichero es la
    siguiente:

        https://www.mambiente.madrid.es/opendata/horario.txt

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
    import pandas as pd

    columns = [
        "provincia",
        "municipio",
        "estacion",
        "magnitud",
        "punto_muestreo",
        "ano",
        "mes",
        "dia",
    ]

    for i in range(1, 25):
        columns.append(f"H{i:02d}")
        columns.append(f"V{i:02d}")

    df = pd.read_csv(url, names=columns, sep=",")

    print("Show 5 first rows: ")
    print(df.head())

    return df


default_args = {
    "start_date": pendulum.datetime(2024, 3, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}

with DAG(
    dag_id="mambiente_hourly_data",
    schedule_interval="35 23 * * *",
    tags=["Ayuntamiento_Madrid", "Environmental_Data"],
    default_args=default_args,
) as dag:

    @task(task_id="extract_mambiente_hourly")
    def extract():
        url = "https://www.mambiente.madrid.es/opendata/horario.txt"

        extracted_data = extract_mambiente_data(url)

        return extracted_data

    extract_task = extract()

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print(
            "Data extraction completed successfully"
        ),
    )

    extract_task >> end_task
