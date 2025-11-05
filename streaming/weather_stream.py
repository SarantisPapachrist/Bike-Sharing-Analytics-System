from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    spark = (
        SparkSession.builder
        .appName("WeatherStream")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )

    weather_schema = StructType([
        StructField("timestamp", IntegerType()),
        StructField("temperature", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("cloudiness", IntegerType()),
        StructField("precipitation", DoubleType()),
        StructField("city", StringType()),
        StructField("event_timestamp", IntegerType())
    ])
 
    df_weather_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "weather")
        .load()
    )

    df_weather_raw = df_weather_stream.selectExpr("CAST(value AS STRING) AS value")
    df_weather_parsed = df_weather_raw.withColumn("data", from_json(col("value"), weather_schema)).select("data.*")

    df_formatted = df_weather_parsed.withColumn("event_time", to_timestamp(col("event_timestamp")))

    query = (
        df_formatted.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()