import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, from_unixtime,
    window, avg, max as smax, min as smin, stddev_samp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, DoubleType
)


output_path = "csv/usage_summary.csv"
header_written = os.path.exists(output_path)


def main():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "env", ".env"))
    kafka_bootstrap = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic_info     = os.getenv("KAFKA_STATION_INFO", "station_info")
    topic_status   = os.getenv("KAFKA_STATION_STATUS", "station_status")
    topic_weather  = os.getenv("KAFKA_WEATHER", "weather")

    spark = (
        SparkSession.builder
        .appName("Analytics-Separate-Streams")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    status_schema = StructType([
        StructField("station_id", StringType()),
        StructField("num_bikes_available", IntegerType()),
        StructField("num_docks_available", IntegerType()),
        StructField("is_installed", BooleanType()),
        StructField("is_renting", BooleanType()),
        StructField("is_returning", BooleanType()),
        StructField("last_reported", IntegerType()),
        StructField("city", StringType()),
        StructField("event_timestamp", IntegerType())
    ])

    info_schema = StructType([
        StructField("station_id", StringType()),
        StructField("name", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("capacity", IntegerType())
    ])

    weather_schema = StructType([
        StructField("timestamp", IntegerType()),
        StructField("temperature", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("cloudiness", IntegerType()),
        StructField("precipitation", DoubleType()),
        StructField("city", StringType()),
        StructField("event_timestamp", IntegerType())
    ])

    df_info = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic_info)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .withColumn("data", from_json(col("value"), info_schema))
        .select("data.*")
        .cache()
    )

    df_status = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic_status)
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .withColumn("data", from_json(col("value"), status_schema))
        .select("data.*")
    )

    df_status = df_status.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("event_timestamp")))
    )

    df_joined = df_status.join(df_info, on="station_id", how="left")

    df_joined = df_joined.withColumn(
        "utilization",
        col("num_bikes_available") /
        (col("num_bikes_available") + col("num_docks_available"))
    )
    df_joined = df_joined.withWatermark("event_time", "30 seconds")

    df_hourly_util = (
        df_joined
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("city")
        )
        .agg(
            avg("utilization").alias("avg_util"),
            smax("utilization").alias("max_util"),
            smin("utilization").alias("min_util"),
            stddev_samp("utilization").alias("std_util")
        )
    )

    df_weather = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic_weather)
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .withColumn("data", from_json(col("value"), weather_schema))
        .select("data.*")
    )

    df_weather = df_weather.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("event_timestamp")))
    ).withWatermark("event_time", "30 seconds")

    df_hourly_weather = (
        df_weather
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("city")
        )
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("wind_speed").alias("avg_wind"),
            avg("cloudiness").alias("avg_clouds"),
            avg("precipitation").alias("avg_rain")
        )
    )

    def handle_util_batch(df, batch_id):
        if not df.head(1):
            return
        print(f"\n🚲 Utilization Batch: {batch_id}")
        df.orderBy("window.start").show(truncate=False)


    query_util = (
        df_hourly_util      
        .writeStream
        .foreachBatch(handle_util_batch)
        .outputMode("append")
        .start()
    )


    def handle_weather_batch(df, batch_id):
        if not df.head(1): 
            return
        print(f"\n🌦️ Weather Batch: {batch_id}")
        df.orderBy("window.start").show(truncate=False)


    query_weather = (
        df_hourly_weather    
        .writeStream
        .foreachBatch(handle_weather_batch)
        .outputMode("append")
        .start()
    )

    df_final = (
        df_hourly_util.alias("u")
        .join(
            df_hourly_weather.alias("w"),
            on=[
                col("u.city") == col("w.city"),
                col("u.window.start") == col("w.window.start"),
                col("u.window.end") == col("w.window.end")
            ],
            how="inner"
        )
        .select(
            col("u.window.start").alias("window_start"),
            col("u.window.end").alias("window_end"),
            col("u.city").alias("city"),
            "avg_util", "max_util", "min_util", "std_util",
            "avg_temp", "avg_wind", "avg_clouds", "avg_rain"
        )
    )

    def print_final(df, batch_id):
        if df.head(1):
            print(f"\n📊 Final Combined Batch {batch_id}")
            df.orderBy("window_start").show(truncate=False)

    query_final = (
        df_final
        .writeStream
        .foreachBatch(print_final)
        .outputMode("append").
        start()
    )

    def write_csv_single(df, batch_id):
        global header_written
        pdf = df.toPandas()
        if pdf.empty:
            return
        
        pdf.to_csv(
            output_path,
            mode="a",
            header=not header_written,
            index=False
        )
        
        header_written = True

    query_csv = (
        df_final
        .writeStream
        .foreachBatch(write_csv_single)
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()