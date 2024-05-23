import os
from datetime import datetime, timedelta
from itertools import chain

from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.functions import (
    lit,
    col,
    concat,
    create_map,
    date_format,
)

from mappings import (
    CONTAMINACION_ACUSTICA_METRIC_MAPPING,
    CONTAMINACION_ACUSTICA_ESTACION_MAPPING,
    GOB_METEOR_METRIC_MAPPING,
    GOB_METEOR_ESTACION_MAPPING,
    AIR_QUALITY_METRIC_MAPPING,
    AIR_QUALITY_ESTACION_MAPPING,
    OPEN_METEO_METRIC_MAPPING,
    OPEN_METEO_ESTACION_MAPPING,
)


RAW_DIR = "/opt/***/raw"
RICH_DIR = "/opt/***/rich"

# For local execution uncomment the following lines:
# RAW_DIR = "raw"
# RICH_DIR = "rich"


# Function to generate the last 7 days dates in the format YYYY_MM_DD
def last_seven_days():
    today = datetime.today()
    dates = [today - timedelta(days=i) for i in range(1, 8)]
    formatted_dates = [date.strftime("%Y_%m_%d") for date in dates]

    return formatted_dates


def merge_csv_files(csv_files):
    # List of CSV files
    # csv_files = ["/opt/***/raw/air_quality_2024_04_09.csv",
    # "/opt/***/raw/air_quality_2024_04_10.csv"]

    # Read each CSV file and add a new column for the date
    dfs = []
    for file in csv_files:
        if not os.path.exists(file):
            print(f"File not found: {file}")
            continue
        # Spark is used from global scope
        # pylint: disable=used-before-assignment
        df = spark.read.option("header", "true").csv(file)
        dfs.append(df)

    if not dfs:
        raise ValueError(
            f"No files found for the last seven days. files: {csv_files}"
        )

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
        f"{RAW_DIR}/air_quality_{date}.csv" for date in last_week_dates
    ]
    gob_meteor_list = [
        f"{RAW_DIR}/gob_meteor_{date}.csv" for date in last_week_dates
    ]
    contaminacion_acustica_list = [
        f"{RAW_DIR}/contaminacion_acustica_{date}.csv"
        for date in last_week_dates
    ]
    open_meteo_list = [
        f"{RAW_DIR}/open_meteo_{date}.csv" for date in last_week_dates
    ]

    air_quality_df = merge_csv_files(air_quality_list)
    gob_meteor_df = merge_csv_files(gob_meteor_list)
    contaminacion_acustica_df = merge_csv_files(contaminacion_acustica_list)
    open_meteo_df = merge_csv_files(open_meteo_list)

    # 1. air_quality
    air_quality_estacion_mapping = create_map(
        [lit(x) for x in chain(*AIR_QUALITY_ESTACION_MAPPING.items())]
    )

    air_quality_metric_mapping = create_map(
        [lit(x) for x in chain(*AIR_QUALITY_METRIC_MAPPING.items())]
    )

    # Calculate the average for each hour
    hour_columns = ["H{:02d}".format(i) for i in range(1, 25)]
    avg_hour_expr = sum(col(hour) for hour in hour_columns) / 24

    air_quality_output = (
        air_quality_df.withColumn("measure", F.round(avg_hour_expr, 2))
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

    gob_meteor_estacion_mapping = create_map(
        [lit(x) for x in chain(*GOB_METEOR_ESTACION_MAPPING.items())]
    )

    gob_meteor_metric_mapping = create_map(
        [lit(x) for x in chain(*GOB_METEOR_METRIC_MAPPING.items())]
    )

    # Calculate the average for each hour
    hour_columns = ["H{:02d}".format(i) for i in range(1, 25)]
    avg_hour_expr = sum(col(hour) for hour in hour_columns) / 24

    gob_meteor_output = (
        gob_meteor_df.withColumn("measure", F.round(avg_hour_expr, 2))
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
    contaminacion_acustica_estacion_mapping = create_map(
        [
            lit(x)
            for x in chain(*CONTAMINACION_ACUSTICA_ESTACION_MAPPING.items())
        ]
    )

    contaminacion_acustica_metric_mapping = create_map(
        [lit(x) for x in chain(*CONTAMINACION_ACUSTICA_METRIC_MAPPING.items())]
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
    open_meteo_estacion_mapping = create_map(
        [lit(x) for x in chain(*OPEN_METEO_ESTACION_MAPPING.items())]
    )

    open_meteo_metric_mapping = create_map(
        [lit(x) for x in chain(*OPEN_METEO_METRIC_MAPPING.items())]
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
        open_meteo_output.withColumn("measure", F.round(col("measure"), 2))
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

    final_df.coalesce(1).write.csv(
        f"{RICH_DIR}/final_df_{last_Sunday}", header=True
    )  # bitnami/spark

    # Stop SparkSession
    spark.stop()
