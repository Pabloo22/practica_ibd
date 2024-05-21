from pyspark.sql import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


# Define UDF to calculate sentiment
def calculate_sentiment(text):
    return TextBlob(text).sentiment.polarity


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Sentiment Analysis").getOrCreate()

    # Register UDF
    sentiment_udf = udf(calculate_sentiment, FloatType())

    # Load noticias JSON files from the raw folder
    df = spark.read.json("/opt/bitnami/spark/raw/noticias_*.json")

    # Add sentiment column
    df_with_sentiment = df.withColumn(
        "sentiment", sentiment_udf(df.description)
    )

    # Save the transformed JSON to the rich folder
    df_with_sentiment.write.json(
        "/opt/bitnami/spark/rich/noticias_with_sentiment"
    )

    spark.stop()
