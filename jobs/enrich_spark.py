from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, concat, create_map, round

from datetime import datetime, timedelta
from itertools import chain


# Function to generate the last 7 days dates in the format YYYY_MM_DD
def last_seven_days():
    today = datetime.today()
    dates = [today - timedelta(days=i) for i in range(1, 8)]
    formatted_dates = [date.strftime("%Y_%m_%d") for date in dates]

    return formatted_dates

def merge_csv_files(csv_files):

    # List of CSV files
    # csv_files = ["/opt/***/raw/air_quality_2024_04_09.csv", "/opt/***/raw/air_quality_2024_04_10.csv"]

    # Read each CSV file and add a new column for the date
    dfs = []
    for file in csv_files:
        df = spark.read.option("header", "true").csv(file)
        dfs.append(df)

    # Merge all DataFrames into a single DataFrame
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.union(df)

    # Show the schema of the merged DataFrame
    merged_df.printSchema()

    # Show the first few rows of the merged DataFrame
    merged_df.show()

    return merged_df



if __name__ == "__main__":

    # Initialize SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Enrich Spark") \
        .getOrCreate()

    # Generate the list of filenames
    last_week_dates = last_seven_days()
    last_Sunday = last_week_dates[0]

    air_quality_list = [f"/opt/***/raw/air_quality_{date}.csv" for date in last_week_dates]
    # contaminacion_acustica_list = [f"/opt/***/raw/contaminacion_acustica_{date}.csv" for date in last_week_dates]
    # gob_meteor_list = [f"/opt/***/raw/gob_meteor_{date}.csv" for date in last_week_dates]
    # open_meteo_list = [f"/opt/***/raw/open_meteo_{date}.csv" for date in last_week_dates]

    air_quality_df = merge_csv_files(air_quality_list)
    # contaminacion_acustica_df = merge_csv_files(contaminacion_acustica_list)
    # gob_meteor_df = merge_csv_files(gob_meteor_list)
    # open_meteo_df = merge_csv_files(open_meteo_list)
    
    # Create a mapping dictionary for ESTACION column
    air_quality_estacion_mapping = {
        "4":  "1",
        "8":  "2",
        "11": "3",
        "16": "4",
        "17": "5",
        "18": "6",
        "24": "7",
        "27": "8",
        "35": "9",
        "36": "10",
        "38": "11",
        "39": "12",
        "40": "13",
        "47": "14",
        "48": "15",
        "49": "16",
        "50": "17",
        "54": "18",
        "55": "19",
        "56": "20",
        "57": "21",
        "58": "22",
        "59": "23",
        "60": "24"
    }

    air_quality_estacion_mapping = create_map([lit(x) for x in chain(*air_quality_estacion_mapping.items())])

    # Create a mapping dictionary for MAGNITUD column
    air_quality_metric_mapping = {
        "1": "1",
        "6": "2",
        "7": "3",
        "8": "4",
        "9": "5",
        "10": "6",
        "12": "7",
        "14": "8",
        "20": "9",
        "30": "10",
        "35": "11",
        "37": "12",
        "38": "13",
        "39": "14",
        "42": "15",
        "43": "16",
        "44": "17"
    }

    air_quality_metric_mapping = create_map([lit(x) for x in chain(*air_quality_metric_mapping.items())])

    # Calculate the average for each hour
    hour_columns = ["H{:02d}".format(i) for i in range(1, 25)]
    avg_hour_expr = sum(col(hour) for hour in hour_columns) / 24

    air_quality_output = air_quality_df.withColumn("measure", round(avg_hour_expr, 2)) \
                .withColumn("date", concat(col("ANO"), lit("-"), col("MES"), lit("-"), col("DIA"))) \
                .withColumn("station_id", air_quality_estacion_mapping.getItem(col("ESTACION"))) \
                .withColumn("metric_id", air_quality_metric_mapping.getItem(col("MAGNITUD"))) \
                .select("metric_id", "station_id", "measure", "date")
    
    air_quality_output.show()

    air_quality_output.coalesce(1).write.csv(f"/opt/airflow/rich/air_quality_{last_Sunday}", header=True) #bitnami/spark

    # Stop SparkSession
    spark.stop()