import pendulum
import os
import subprocess
import json
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


def extract_meteor_data(dictionary):

    date = pendulum.today().subtract(days=2).strftime('%Y-%m-%d')
    start_date = date
    end_date = date
    
    # Lista para almacenar los DataFrames de cada distrito
    dfs = []
    
    for district, coordinates in dictionary.items():
        latitud, longitud = coordinates['lat'], coordinates['long']
        
        curl_command = [
            'curl',
            f"https://archive-api.open-meteo.com/v1/era5?latitude={latitud}&longitude={longitud}&start_date={start_date}&end_date={end_date}&hourly=snowfall,snow_depth,cloud_cover,soil_temperature_0_to_7cm,soil_moisture_0_to_7cm,terrestrial_radiation"
        ]
        
        try:
            output = subprocess.check_output(curl_command, text=True)
            data = json.loads(output)

            # Intentar crear el DataFrame con los datos 'hourly', si no existe, llenar todas las columnas con None
            try:
                df = pd.DataFrame(data['hourly'])
            except KeyError:
                df = pd.DataFrame(columns=data.get('headers', []))
                df[:] = None
            
            # Si la columna 'time' está presente en el DataFrame
            if 'time' in df.columns:
                # Convertir la columna de tiempo a formato datetime
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
            
            # Añadir una columna de nombre de distrito
            df['district'] = district
            
            # Añadir el DataFrame a la lista
            dfs.append(df)
        except subprocess.CalledProcessError:
            print(f"No se pudieron recopilar datos para el distrito: {district}")
    
    if dfs:
        # Concatenar todos los DataFrames en uno solo
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Si la columna 'district' está presente en el DataFrame
        if 'district' in combined_df.columns:
            # Establecer 'district' como índice
            combined_df.set_index(['district', 'time'], inplace=True)
    else:
        # Si no se recopilaron datos para ningún distrito, devuelve un DataFrame vacío
        combined_df = pd.DataFrame()
    
    return combined_df


def export_gob_meteor(df, folder_path, filename):
    """
    Carga el CSV incremental correspondiente en la carpeta de información cruda "/raw"
    """
    
    # Best Practices - Section: Top level Python Code
    import os

    # For Debugging Purposes
    print(os.getcwd())
    
    # Verificar si el directorio existe, si no, crearlo
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    # Construct the full file path
    file_path = os.path.join(folder_path, filename)
    
    # Write DataFrame to CSV
    df.to_csv(file_path, index=False)
    print(f"DataFrame written to {file_path}")


default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False
}

with DAG(
    dag_id="meteo_isma",
    schedule_interval="35 22 * * *",
    tags=["gob-meteo"],
    default_args=default_args
) as dag:
    
    @task(task_id='extract_data')
    def extract():
        districts = {'Centro': {'lat': 40.415277777777774, 'long': 3.7075},
                'Arganzuela': {'lat': 40.40277777777778, 'long': 3.695277777777778},
                'Retiro (Madrid)': {'lat': 40.408055555555556, 'long': 3.6766666666666663},
                'Salamanca (Madrid)': {'lat': 40.43, 'long': 3.6777777777777776},
                'Chamartín': {'lat': 40.45333333333333, 'long': 3.6774999999999998},
                'Tetuán (Madrid)': {'lat': 40.46055555555556, 'long': 3.7},
                'Chamberí': {'lat': 40.43277777777777, 'long': 3.6972222222222224},
                'Fuencarral-El Pardo': {'lat': 40.478611111111114,
                'long': 3.7097222222222226},
                'Moncloa-Aravaca': {'lat': 40.43527777777778, 'long': 3.718888888888889},
                'Latina (Madrid)': {'lat': 40.402499999999996, 'long': 3.741388888888889},
                'Carabanchel': {'lat': 40.38361111111111, 'long': 3.7280555555555557},
                'Usera': {'lat': 40.38138888888889, 'long': 3.706944444444445},
                'Puente de Vallecas': {'lat': 40.39833333333333, 'long': 3.6691666666666665},
                'Moratalaz': {'lat': 40.41, 'long': 3.6444444444444444},
                'Ciudad Lineal': {'lat': 40.45, 'long': 3.65},
                'Hortaleza': {'lat': 40.46944444444445, 'long': 3.6405555555555553},
                'Villaverde': {'lat': 40.34583333333334, 'long': 3.7094444444444448},
                'Villa de Vallecas': {'lat': 40.37972222222222, 'long': 3.6213888888888888},
                'Vicálvaro': {'lat': 40.40416666666667, 'long': 3.6080555555555556},
                'San Blas-Canillejas': {'lat': 40.42611111111111, 'long': 3.612777777777778},
                'Barajas': {'lat': 40.47027777777778, 'long': 3.585}}
        
        data = extract_meteor_data(districts)

        return data
    
    @task(task_id='export_raw_df')
    
    def load_raw_data(df):
        # Retrieve the Pandas DataFrame
        print(df.head())

        # Define the folder path
        folder_path = "/opt/airflow/raw"
        
        # Generate a unique filename based on the current timestamp
        timestamp_str = (str(pendulum.now(tz="UTC"))[:10]).replace("-", "_")
        print(timestamp_str)
        filename = f"open_meteo_{timestamp_str}.csv"
        print(filename)

        export_gob_meteor(df, folder_path, filename)

    extract_task = extract()
    export_task = load_raw_data(extract_task)
    end_task = PythonOperator(
        task_id="end",
        python_callable = lambda: print("Jobs completed successfully"),
    )

    extract_task >> export_task >> end_task
