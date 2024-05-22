import os
import json
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import FloatType
from transformers import pipeline


from enrich_spark import last_seven_days


RAW_DIR = "/opt/airflow/raw"
RICH_DIR = "/opt/airflow/rich"
OUTPUT_PATH = f"{RICH_DIR}/noticias_with_sentiment.json"

# Local dirs
RAW_DIR = "raw"
RICH_DIR = "rich"
OUTPUT_PATH = f"{RICH_DIR}/noticias_with_sentiment.json"

DATE_LEN = len("yyyy_mm_dd")
EXTENSION_LEN = len(".json")


def get_classifier():
    model_ckpt = "mrm8488/electricidad-small-finetuned-sst2-es"
    return pipeline("sentiment-analysis", model=model_ckpt)


def predict(classifier, text):
    label, score = classifier(text)[0].values()

    # Change to the range [-1, 1] (prob_pos - prob_neg)
    if label == "LABEL_0":
        negative_probability = score
        positive_probability = 1 - score
    else:
        positive_probability = score
        negative_probability = 1 - score

    return positive_probability - negative_probability


def main():
    # Initialize SparkSession
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("SentimentAnalysis")
        .getOrCreate()
    )

    # Register UDF
    classifier = get_classifier()
    calculate_sentiment = partial(predict, classifier)
    sentiment_udf = udf(calculate_sentiment, FloatType())

    # Generate the list of filenames from the last seven days
    last_week_dates = last_seven_days()
    file_paths = [
        f"{RAW_DIR}/noticias_{date}.json" for date in last_week_dates
    ]

    # Check which files exist
    existing_files = [file for file in file_paths if os.path.exists(file)]
    if not existing_files:
        print("No files found for the last seven days.")
        print(f"files: {file_paths}")
        spark.stop()
        return

    # Process each file individually to add the date column
    dfs = []
    for file_path in existing_files:
        date_str = os.path.basename(file_path)[
            -(DATE_LEN + EXTENSION_LEN) : -EXTENSION_LEN
        ]
        df = spark.read.option("multiline", "true").json(file_path)
        df = df.withColumn("date", lit(date_str))
        dfs.append(df)

    # Combine all DataFrames
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.union(df)

    # Add sentiment column
    combined_df_with_sentiment = combined_df.withColumn(
        "sentiment", sentiment_udf(combined_df["title"])
    )
    data = combined_df_with_sentiment.toJSON().collect()
    json_data = [json.loads(row) for row in data]

    # Load existing data if the file exists and combine
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            existing_data = json.load(f)

        # Filter news with the same title
        existing_titles = [news["title"] for news in existing_data]
        filtered_data = [
            news for news in json_data if news["title"] not in existing_titles
        ]
        # Combine the existing data with the new data
        json_data = existing_data + filtered_data

    # Save the final combined data to a single JSON file
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e
