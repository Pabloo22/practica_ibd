import pendulum
import os
import subprocess
import json
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


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



def meteor_data():
    date = pendulum.today().subtract(days=2).strftime('%Y-%m-%d')
    start_date = date
    end_date = date
    
    # Lista para almacenar los DataFrames de cada distrito
    dfs = []
    
    for district, coordinates in districts.items():
        latitud, longitud = coordinates['lat'], coordinates['long']
        
        curl_command = [
            'curl',
            f"https://archive-api.open-meteo.com/v1/era5?latitude={latitud}&longitude={longitud}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,snowfall,snow_depth,weather_code,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,et0_fao_evapotranspiration,vapour_pressure_deficit,wind_speed_10m,wind_speed_100m,wind_direction_10m,wind_direction_100m,wind_gusts_10m,soil_temperature_0_to_7cm,soil_temperature_7_to_28cm,soil_temperature_28_to_100cm,soil_temperature_100_to_255cm,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,soil_moisture_28_to_100cm,soil_moisture_100_to_255cm,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,global_tilted_irradiance,terrestrial_radiation,shortwave_radiation_instant,direct_radiation_instant,diffuse_radiation_instant,direct_normal_irradiance_instant,global_tilted_irradiance_instant,terrestrial_radiation_instant"
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



# Define los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),  # Fecha de inicio del DAG
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define el DAG
dag = DAG(
    'meteorological_data_collection',  # Nombre del DAG
    default_args=default_args,
    description='Recopilación de datos meteorológicos de distritos',
    schedule_interval='@daily',  # Frecuencia de ejecución del DAG (diaria)
)

# Define la tarea para ejecutar la función meteor_data
collect_meteor_data = PythonOperator(
    task_id='meteorological_data_collection',
    python_callable=meteor_data,
    dag=dag,
)