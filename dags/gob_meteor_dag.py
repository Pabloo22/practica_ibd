import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


def extract_gob_meteor(url, hour_booleans):
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

    # https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
    # Best Practices - Section: Top level Python Code
    import numpy as np
    import pandas as pd

    df = pd.read_csv(url, sep=';')

    for hour_bool in hour_booleans:
        print(f"Check: {hour_bool}")
        assert any(np.array(df[hour_bool]) == 'V')

    df = df[['ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO',
        'ANO', 'MES', 'DIA', 'H01', 'H02', 'H03', 'H04',
        'H05', 'H06', 'H07', 'H08', 'H09', 'H10', 'H11', 
        'H12', 'H13', 'H14','H15', 'H16', 'H17', 'H18', 
        'H19', 'H20', 'H21', 'H22', 'H23', 'H24']]
    
    print("Show 5 first rows: ")
    print(df.head())


    return df



default_args = {
    "start_date": pendulum.datetime(2024, 3, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False
}

with DAG(
    dag_id="gob_meteor",
    schedule_interval="35 23 * * *",
    tags=["Ayuntamiento_Madrid"],
    default_args=default_args
) as dag:
    
    @task(task_id='extract_from_url')
    def extract():

        url = "https://datos.madrid.es/egob/catalogo/300392-11041819-meteorologia-tiempo-real.csv"
        hour_booleans = ['V01', 'V02', 'V03', 'V04', 'V05', 'V06', 'V07', 'V08', \
                        'V09', 'V10', 'V11', 'V12','V13', 'V14', 'V15', 'V16', \
                        'V17', 'V18', 'V19', 'V20', 'V21', 'V22', 'V23', 'V24']
        
        scraped_data = extract_gob_meteor(url, hour_booleans)

        return scraped_data
    

    extract_task = extract()

    end_task = PythonOperator(
        task_id="end",
        python_callable = lambda: print("Jobs completed successfully"),
    )

    extract_task >> end_task