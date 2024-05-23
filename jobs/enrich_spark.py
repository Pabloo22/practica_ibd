from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    col,
    concat,
    create_map,
    round,
    date_format,
)

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
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Enrich Spark")
        .getOrCreate()
    )

    # Generate the list of filenames
    last_week_dates = last_seven_days()
    last_Sunday = last_week_dates[0]

    air_quality_list = [
        f"/opt/***/raw/air_quality_{date}.csv" for date in last_week_dates
    ]
    gob_meteor_list = [
        f"/opt/***/raw/gob_meteor_{date}.csv" for date in last_week_dates
    ]
    contaminacion_acustica_list = [
        f"/opt/***/raw/contaminacion_acustica_{date}.csv"
        for date in last_week_dates
    ]
    open_meteo_list = [
        f"/opt/***/raw/open_meteo_{date}.csv" for date in last_week_dates
    ]

    air_quality_df = merge_csv_files(air_quality_list)
    gob_meteor_df = merge_csv_files(gob_meteor_list)
    contaminacion_acustica_df = merge_csv_files(contaminacion_acustica_list)
    open_meteo_df = merge_csv_files(open_meteo_list)

    # 1. air_quality

    # Create a mapping dictionary for ESTACION column
    air_quality_estacion_mapping = {
        "4": "1",
        "8": "2",
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
        "60": "24",
    }

    air_quality_estacion_mapping = create_map(
        [lit(x) for x in chain(*air_quality_estacion_mapping.items())]
    )

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
        "44": "17",
    }

    air_quality_metric_mapping = create_map(
        [lit(x) for x in chain(*air_quality_metric_mapping.items())]
    )

    # Calculate the average for each hour
    hour_columns = ["H{:02d}".format(i) for i in range(1, 25)]
    avg_hour_expr = sum(col(hour) for hour in hour_columns) / 24

    air_quality_output = (
        air_quality_df.withColumn("measure", round(avg_hour_expr, 2))
        .withColumn(
            "date",
            concat(col("ANO"), lit("-"), col("MES"), lit("-"), col("DIA")),
        )
        .withColumn(
            "station_id", air_quality_estacion_mapping.getItem(col("ESTACION"))
        )
        .withColumn(
            "metric_id", air_quality_metric_mapping.getItem(col("MAGNITUD"))
        )
        .select("metric_id", "station_id", "measure", "date")
    )

    air_quality_output.show()

    # 2. gob_meteor
    # Create a mapping dictionary for ESTACION column

    gob_meteor_estacion_mapping = {
        "102": "25",
        "103": "5",
        "104": "26",
        "106": "27",
        "107": "28",
        "108": "29",
        "109": "30",
        "110": "31",
        "111": "32",
        "112": "33",
        "113": "34",
        "114": "35",
        "115": "36",
        "4": "1",
        "8": "2",
        "16": "4",
        "18": "6",
        "24": "7",
        "35": "9",
        "36": "10",
        "38": "11",
        "39": "12",
        "54": "18",
        "56": "20",
        "58": "22",
        "59": "23",
    }

    gob_meteor_estacion_mapping = create_map(
        [lit(x) for x in chain(*gob_meteor_estacion_mapping.items())]
    )

    # Create a mapping dictionary for MAGNITUD column
    gob_meteor_metric_mapping = {
        "81": "18",
        "82": "19",
        "83": "20",
        "86": "21",
        "87": "22",
        "88": "23",
        "89": "24",
    }

    gob_meteor_metric_mapping = create_map(
        [lit(x) for x in chain(*gob_meteor_metric_mapping.items())]
    )

    # Calculate the average for each hour
    hour_columns = ["H{:02d}".format(i) for i in range(1, 25)]
    avg_hour_expr = sum(col(hour) for hour in hour_columns) / 24

    gob_meteor_output = (
        gob_meteor_df.withColumn("measure", round(avg_hour_expr, 2))
        .withColumn(
            "date",
            concat(col("ANO"), lit("-"), col("MES"), lit("-"), col("DIA")),
        )
        .withColumn(
            "station_id", gob_meteor_estacion_mapping.getItem(col("ESTACION"))
        )
        .withColumn(
            "metric_id", gob_meteor_metric_mapping.getItem(col("MAGNITUD"))
        )
        .select("metric_id", "station_id", "measure", "date")
    )

    gob_meteor_output.show()

    # 3. contaminacion_acustica

    contaminacion_acustica_estacion_mapping = {
        "1": "37",
        "2": "38",
        "3": "9",
        "4": "1",
        "5": "12",
        "6": "39",
        "8": "2",
        "10": "11",
        "11": "3",
        "12": "40",
        "13": "13",
        "14": "20",
        "16": "4",
        "17": "5",
        "18": "6",
        "19": "41",
        "20": "42",
        "24": "7",
        "25": "43",
        "26": "44",
        "27": "8",
        "28": "45",
        "29": "22",
        "30": "23",
        "31": "21",
        "47": "14",
        "48": "15",
        "50": "17",
        "54": "18",
        "55": "19",
        "86": "24",
    }

    contaminacion_acustica_estacion_mapping = create_map(
        [
            lit(x)
            for x in chain(*contaminacion_acustica_estacion_mapping.items())
        ]
    )

    contaminacion_acustica_metric_mapping = {
        "LAEQ": "25",
        "LAS01": "26",
        "LAS10": "27",
        "LAS50": "28",
        "LAS90": "29",
        "LAS99": "30",
    }

    contaminacion_acustica_metric_mapping = create_map(
        [lit(x) for x in chain(*contaminacion_acustica_metric_mapping.items())]
    )

    # filter tipo=T
    # unpivot metric columns
    # final dataframe

    contaminacion_acustica_output = (
        contaminacion_acustica_df.filter(col("tipo") == "T")
        .selectExpr(
            "NMT",
            "anio",
            "mes",
            "dia",
            "tipo",
            "stack(6, 'LAEQ', LAEQ, 'LAS01', LAS01, 'LAS10', LAS10, 'LAS50', LAS50, 'LAS90', LAS90, 'LAS99', LAS99) as (metric, measure)",
        )
        .select("NMT", "anio", "mes", "dia", "metric", "measure")
    )

    contaminacion_acustica_output = (
        contaminacion_acustica_output.withColumn(
            "date",
            concat(col("anio"), lit("-"), col("mes"), lit("-"), col("dia")),
        )
        .withColumn(
            "station_id",
            contaminacion_acustica_estacion_mapping.getItem(col("NMT")),
        )
        .withColumn(
            "metric_id",
            contaminacion_acustica_metric_mapping.getItem(col("metric")),
        )
        .select("metric_id", "station_id", "measure", "date")
    )

    contaminacion_acustica_output.show()

    # 4. open_meteo
    open_meteo_estacion_mapping = {
        "Centro": 31,
        "Arganzuela": 46,
        "Retiro (Madrid)": 16,
        "Salamanca (Madrid)": 2,
        "Chamartín": 32,
        "Tetuán (Madrid)": 47,
        "Chamberí": 30,
        "Fuencarral-El Pardo": 12,
        "Moncloa-Aravaca": 1,
        "Latina (Madrid)": 39,
        "Carabanchel": 6,
        "Usera": 48,
        "Puente de Vallecas": 13,
        "Moratalaz": 10,
        "Ciudad Lineal": 49,
        "Hortaleza": 28,
        "Villaverde": 5,
        "Villa de Vallecas": 50,
        "Vicálvaro": 51,
        "San Blas-Canillejas": 52,
        "Barajas": 53,
    }

    open_meteo_estacion_mapping = create_map(
        [lit(x) for x in chain(*open_meteo_estacion_mapping.items())]
    )

    open_meteo_metric_mapping = {
        "snowfall": 31,
        "snow_depth": 32,
        "cloud_cover": 33,
        "soil_temperature_0_to_7cm": 34,
        "soil_moisture_0_to_7cm": 35,
        "terrestrial_radiation": 36,
    }

    open_meteo_metric_mapping = create_map(
        [lit(x) for x in chain(*open_meteo_metric_mapping.items())]
    )

    # Convert time column to timestamp
    open_meteo_df = open_meteo_df.withColumn(
        "time", col("time").cast("timestamp")
    )

    # Group by date and aggregate columns
    open_meteo_output = (
        open_meteo_df.withColumn(
            "date", date_format(col("time"), "yyyy-MM-dd")
        )
        .groupBy("district", "date")
        .agg(
            {
                "snowfall": "avg",
                "snow_depth": "avg",
                "cloud_cover": "avg",
                "soil_temperature_0_to_7cm": "avg",
                "soil_moisture_0_to_7cm": "avg",
                "terrestrial_radiation": "avg",
            }
        )
        .select(
            col("district"),
            col("date"),
            col("avg(snowfall)").alias("snowfall"),
            col("avg(snow_depth)").alias("snow_depth"),
            col("avg(cloud_cover)").alias("cloud_cover"),
            col("avg(soil_temperature_0_to_7cm)").alias(
                "soil_temperature_0_to_7cm"
            ),
            col("avg(soil_moisture_0_to_7cm)").alias("soil_moisture_0_to_7cm"),
            col("avg(terrestrial_radiation)").alias("terrestrial_radiation"),
        )
    )

    open_meteo_output.show()

    open_meteo_output = open_meteo_output.selectExpr(
        "district",
        "date",
        "stack(6, 'snowfall', snowfall, 'snow_depth', snow_depth, 'cloud_cover', cloud_cover, 'soil_temperature_0_to_7cm', soil_temperature_0_to_7cm, 'soil_moisture_0_to_7cm', soil_moisture_0_to_7cm, 'terrestrial_radiation', terrestrial_radiation) as (metric, measure)",
    ).select("district", "date", "metric", "measure")

    open_meteo_output = (
        open_meteo_output.withColumn("measure", round(col("measure"), 2))
        .withColumn(
            "station_id", open_meteo_estacion_mapping.getItem(col("district"))
        )
        .withColumn(
            "metric_id", open_meteo_metric_mapping.getItem(col("metric"))
        )
        .select("metric_id", "station_id", "measure", "date")
    )

    open_meteo_output = open_meteo_output.fillna(0, subset=["measure"])

    open_meteo_output.show()

    # FINAL WRITE

    final_df = (
        air_quality_output.unionByName(gob_meteor_output)
        .unionByName(contaminacion_acustica_output)
        .unionByName(open_meteo_output)
    )
    """
    final_df = open_meteo_output.unionByName(open_meteo_output) \
                    .unionByName(open_meteo_output) \
                    .unionByName(open_meteo_output)
    """

    final_df.coalesce(1).write.csv(
        f"/opt/airflow/rich/final_df_{last_Sunday}", header=True
    )  # bitnami/spark

    # Stop SparkSession
    spark.stop()
