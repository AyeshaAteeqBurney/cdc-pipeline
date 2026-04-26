#!/usr/bin/env python3
"""
Taxi medallion pipeline (Bronze -> Silver -> Gold).

Notebook-first friendly: this script is runnable from inside the `jupyter` container:
  docker exec jupyter python /home/jovyan/project/jobs/taxi_pipeline.py --mode bronze
  docker exec jupyter python /home/jovyan/project/jobs/taxi_pipeline.py --mode silver
  docker exec jupyter python /home/jovyan/project/jobs/taxi_pipeline.py --mode gold

Bronze + Silver run as Structured Streaming with checkpoints under `.checkpoints/`.
Gold is a batch aggregation overwriting partitions.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Ensure `/home/jovyan/project` is on sys.path when run by absolute path.
PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.spark_session import get_spark


def _ensure_namespaces_and_tables(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze (
            VendorID DOUBLE,
            tpep_pickup_datetime STRING,
            tpep_dropoff_datetime STRING,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            RatecodeID DOUBLE,
            store_and_fwd_flag STRING,
            PULocationID DOUBLE,
            DOLocationID DOUBLE,
            payment_type DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            airport_fee DOUBLE,
            cbd_congestion_fee DOUBLE,
            kafka_key STRING,
            kafka_timestamp TIMESTAMP,
            kafka_partition INT,
            kafka_offset LONG,
            ingested_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.silver (
            vendor_id            INT,
            pickup_datetime      TIMESTAMP,
            dropoff_datetime     TIMESTAMP,
            passenger_count      INT,
            trip_distance        DOUBLE,
            ratecode_id          INT,
            store_and_fwd_flag   STRING,
            pu_location_id       INT,
            do_location_id       INT,
            payment_type         INT,
            fare_amount          DOUBLE,
            extra                DOUBLE,
            mta_tax              DOUBLE,
            tip_amount           DOUBLE,
            tolls_amount         DOUBLE,
            total_amount         DOUBLE,
            congestion_surcharge DOUBLE,
            airport_fee          DOUBLE,
            cbd_congestion_fee   DOUBLE,
            pickup_zone          STRING,
            dropoff_zone         STRING
        )
        USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold (
            pickup_hour    TIMESTAMP,
            pickup_zone    STRING,
            trip_count     LONG,
            avg_fare       DOUBLE,
            avg_distance   DOUBLE,
            total_revenue  DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(pickup_hour))
        """
    )


def _taxi_schema() -> StructType:
    # Mirrors the Project 2 schema (relaxed types + nullable `airport_fee`).
    return StructType(
        [
            StructField("VendorID", DoubleType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", DoubleType(), True),
            StructField("DOLocationID", DoubleType(), True),
            StructField("payment_type", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
            StructField("cbd_congestion_fee", DoubleType(), True),
        ]
    )


def run_bronze_stream(spark, bootstrap: str, topic: str, checkpoint: str, trigger: str, once: bool = False) -> None:
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )

    parsed_stream = (
        raw_stream.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_value"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select(
            F.from_json(F.col("json_value"), _taxi_schema()).alias("trip_data"),
            "kafka_key",
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
        )
        .select(
            F.col("trip_data.*"),
            "kafka_key",
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
            F.current_timestamp().alias("ingested_at"),
        )
    )

    def _write_bronze_batch(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        (
            batch_df.dropDuplicates(["kafka_partition", "kafka_offset"])
            .writeTo("lakehouse.taxi.bronze")
            .append()
        )

    os.makedirs(checkpoint, exist_ok=True)
    writer = (
        parsed_stream.writeStream.foreachBatch(_write_bronze_batch)
        .option("checkpointLocation", checkpoint)
    )
    if once:
        q = writer.trigger(availableNow=True).start()
        q.awaitTermination(300)
    else:
        q = writer.trigger(processingTime=trigger).start()
        q.awaitTermination()


def run_silver_stream(spark, zone_lookup_path: str, checkpoint: str, trigger: str, once: bool = False) -> None:
    zone_lookup = (
        spark.read.parquet(zone_lookup_path)
        .select(F.col("LocationID").cast("int").alias("location_id"), F.col("Zone").alias("zone_name"))
        .cache()
    )

    pu_lookup = zone_lookup.withColumnRenamed("location_id", "_pu_id").withColumnRenamed("zone_name", "pickup_zone")
    do_lookup = zone_lookup.withColumnRenamed("location_id", "_do_id").withColumnRenamed("zone_name", "dropoff_zone")

    def _process_silver_batch(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        typed = (
            batch_df.withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
            .withColumn("pickup_datetime", F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd'T'HH:mm:ss"))
            .withColumn("dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd'T'HH:mm:ss"))
            .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
            .withColumn("trip_distance", F.col("trip_distance").cast("double"))
            .withColumn("ratecode_id", F.col("RatecodeID").cast(IntegerType()))
            .withColumn("pu_location_id", F.col("PULocationID").cast(IntegerType()))
            .withColumn("do_location_id", F.col("DOLocationID").cast(IntegerType()))
            .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
            .withColumn("fare_amount", F.col("fare_amount").cast("double"))
            .withColumn("extra", F.col("extra").cast("double"))
            .withColumn("mta_tax", F.col("mta_tax").cast("double"))
            .withColumn("tip_amount", F.col("tip_amount").cast("double"))
            .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
            .withColumn("total_amount", F.col("total_amount").cast("double"))
            .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast("double"))
            .withColumn("airport_fee", F.col("airport_fee").cast("double"))
            .withColumn("cbd_congestion_fee", F.col("cbd_congestion_fee").cast("double"))
        )

        cleaned = (
            typed.filter(F.col("pickup_datetime").isNotNull())
            .filter(F.col("dropoff_datetime").isNotNull())
            .filter(F.col("fare_amount") >= 0)
            .filter(F.col("trip_distance") > 0)
            .filter(
                F.col("passenger_count").isNotNull()
                & (F.col("passenger_count") >= 1)
                & (F.col("passenger_count") <= 8)
            )
            .dropDuplicates(["vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"])
        )

        enriched = (
            cleaned.join(F.broadcast(pu_lookup), cleaned.pu_location_id == pu_lookup._pu_id, "left")
            .drop("_pu_id")
            .join(F.broadcast(do_lookup), cleaned.do_location_id == do_lookup._do_id, "left")
            .drop("_do_id")
            .select(
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "ratecode_id",
                "store_and_fwd_flag",
                "pu_location_id",
                "do_location_id",
                "payment_type",
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "total_amount",
                "congestion_surcharge",
                "airport_fee",
                "cbd_congestion_fee",
                "pickup_zone",
                "dropoff_zone",
            )
        )

        enriched.writeTo("lakehouse.taxi.silver").append()

    # When once=True (Airflow mode), a plain batch read covers everything available.
    # Skipping the streaming query avoids a Spark 4.x BlockManagerId NPE with
    # trigger(availableNow=True) on Iceberg sources.
    _process_silver_batch(spark.read.table("lakehouse.taxi.bronze"), 0)
    if once:
        return

    os.makedirs(checkpoint, exist_ok=True)
    (
        spark.readStream.format("iceberg")
        .load("lakehouse.taxi.bronze")
        .writeStream.foreachBatch(_process_silver_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger)
        .start()
        .awaitTermination()
    )


def run_gold_batch(spark) -> None:
    silver_df = spark.read.table("lakehouse.taxi.silver")
    gold_df = (
        silver_df.withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_datetime")))
        .groupBy("pickup_hour", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue"),
        )
    )
    gold_df.writeTo("lakehouse.taxi.gold").overwritePartitions()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["bronze", "silver", "gold"], required=True)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default="taxi-trips")
    p.add_argument("--trigger", default="5 seconds")
    p.add_argument("--bronze-checkpoint", default="/home/jovyan/project/.checkpoints/bronze")
    p.add_argument("--silver-checkpoint", default="/home/jovyan/project/.checkpoints/silver")
    p.add_argument("--zone-lookup", default="/home/jovyan/project/data/taxi_zone_lookup.parquet")
    p.add_argument("--once", action="store_true", help="Process available data and exit (for Airflow)")
    args = p.parse_args()

    spark = get_spark(f"project3-taxi-{args.mode}")
    _ensure_namespaces_and_tables(spark)

    if args.mode == "bronze":
        run_bronze_stream(spark, args.bootstrap, args.topic, args.bronze_checkpoint, args.trigger, once=args.once)
    elif args.mode == "silver":
        run_silver_stream(spark, args.zone_lookup, args.silver_checkpoint, args.trigger, once=args.once)
    else:
        run_gold_batch(spark)


if __name__ == "__main__":
    main()

