#!/usr/bin/env python3
"""
Taxi medallion pipeline (Bronze -> Silver -> Gold).

Notebook-first friendly: this script is runnable from inside the `jupyter` container:
  docker exec jupyter python /home/jovyan/project/jobs/taxi_pipeline.py --mode bronze
  docker exec jupyter python /home/jovyan/project/jobs/taxi_pipeline.py --mode silver
  docker exec jupyter python /home/jovyan/project/jobs/taxi_pipeline.py --mode gold

Bronze uses Structured Streaming from Kafka with checkpoints under `.checkpoints/bronze`.

Silver reads **incrementally** from the Iceberg bronze table using a per-partition Kafka
offset watermark (`lakehouse.taxi.silver_bronze_watermark`) and **MERGE** upserts into
silver on the natural trip key — idempotent under repeated Airflow `--once` runs.

Gold is a batch aggregation overwriting partitions.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from functools import reduce
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import Window
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

    _ensure_silver_kafka_columns(spark)


def _silver_column_names(spark) -> set[str]:
    return set(spark.table("lakehouse.taxi.silver").columns)


def _ensure_silver_kafka_columns(spark) -> None:
    """Ensure silver has kafka_partition / kafka_offset for watermark MERGE idempotency."""
    existing = _silver_column_names(spark)
    if "kafka_partition" not in existing:
        spark.sql("ALTER TABLE lakehouse.taxi.silver ADD COLUMN kafka_partition INT")
    if "kafka_offset" not in existing:
        spark.sql("ALTER TABLE lakehouse.taxi.silver ADD COLUMN kafka_offset LONG")


def _get_bronze_watermark(spark) -> dict[int, int]:
    """Returns {partition: max_kafka_offset} already processed by taxi silver."""
    wm_tbl = "lakehouse.taxi.silver_bronze_watermark"
    try:
        rows = spark.read.table(wm_tbl).collect()
        return {int(r["partition"]): int(r["max_offset"]) for r in rows}
    except Exception:
        return {}


def _save_bronze_watermark(spark, processed_df) -> None:
    wm_tbl = "lakehouse.taxi.silver_bronze_watermark"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {wm_tbl} (
            partition INT,
            max_offset LONG
        )
        USING iceberg
        """
    )
    new_max = (
        processed_df.groupBy("kafka_partition")
        .agg(F.max("kafka_offset").alias("max_offset"))
        .withColumnRenamed("kafka_partition", "partition")
    )
    # Break Iceberg scan lineage before MERGE (same pattern as jobs/cdc_pipeline.py).
    new_max_local = spark.createDataFrame(new_max.collect(), new_max.schema)
    new_max_local.createOrReplaceTempView("new_taxi_bronze_wm")
    spark.sql(
        f"""
        MERGE INTO {wm_tbl} AS target
        USING new_taxi_bronze_wm AS source
        ON target.partition = source.partition
        WHEN MATCHED THEN UPDATE SET target.max_offset = source.max_offset
        WHEN NOT MATCHED THEN INSERT (partition, max_offset) VALUES (source.partition, source.max_offset)
        """
    )


def _filter_new_bronze(df, wm: dict[int, int]):
    if not wm:
        return df
    conditions = [
        (F.col("kafka_partition") == part) & (F.col("kafka_offset") > off) for part, off in wm.items()
    ]
    seen = list(wm.keys())
    cond = reduce(lambda a, b: a | b, conditions)
    cond = cond | ~F.col("kafka_partition").isin(seen)
    return df.filter(cond)


def _parse_trigger_seconds(trigger: str) -> float:
    m = re.match(r"^\s*(\d+)\s*seconds?\s*$", trigger.strip(), re.I)
    if m:
        return float(m.group(1))
    return 5.0


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
    # `--silver-checkpoint` retained for CLI compatibility; silver uses Kafka offset watermark, not Spark checkpoints.
    _ = checkpoint
    _ensure_silver_kafka_columns(spark)

    zone_lookup = (
        spark.read.parquet(zone_lookup_path)
        .select(F.col("LocationID").cast("int").alias("location_id"), F.col("Zone").alias("zone_name"))
        .cache()
    )

    pu_lookup = zone_lookup.withColumnRenamed("location_id", "_pu_id").withColumnRenamed("zone_name", "pickup_zone")
    do_lookup = zone_lookup.withColumnRenamed("location_id", "_do_id").withColumnRenamed("zone_name", "dropoff_zone")

    def _silver_incremental_pass() -> bool:
        bronze_df = spark.read.table("lakehouse.taxi.bronze")
        wm = _get_bronze_watermark(spark)
        new_bronze = _filter_new_bronze(bronze_df, wm)
        if not new_bronze.take(1):
            print("No new bronze rows for taxi silver (watermark up to date).")
            return False

        typed = (
            new_bronze.withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
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
        )

        # Latest Kafka offset wins per natural trip key within this micro-batch.
        trip_keys = ["vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"]

        w = Window.partitionBy(*trip_keys).orderBy(
            F.col("kafka_partition"),
            F.col("kafka_offset").desc(),
        )
        deduped = (
            cleaned.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        enriched = (
            deduped.join(F.broadcast(pu_lookup), deduped.pu_location_id == pu_lookup._pu_id, "left")
            .drop("_pu_id")
            .join(F.broadcast(do_lookup), deduped.do_location_id == do_lookup._do_id, "left")
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
                "kafka_partition",
                "kafka_offset",
            )
        )

        if not enriched.take(1):
            _save_bronze_watermark(spark, new_bronze)
            print("New bronze rows existed but all were filtered as invalid; watermark advanced.")
            return True

        cols = enriched.columns
        merge_cols = [c for c in cols if c not in trip_keys]
        set_clause = ", ".join(f"target.{c} = source.{c}" for c in merge_cols)
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join(f"source.{c}" for c in cols)

        # Recreate from collected rows to break Iceberg lineage before MERGE (Spark 4 + Iceberg
        # can assert "No plan for TableReference" when the MERGE source view still scans bronze).
        upsert_local = spark.createDataFrame(enriched.collect(), enriched.schema)
        upsert_local.createOrReplaceTempView("taxi_silver_upserts")

        spark.sql(
            f"""
            MERGE INTO lakehouse.taxi.silver AS target
            USING taxi_silver_upserts AS source
            ON target.vendor_id = source.vendor_id
               AND target.pickup_datetime = source.pickup_datetime
               AND target.dropoff_datetime = source.dropoff_datetime
               AND target.pu_location_id = source.pu_location_id
               AND target.do_location_id = source.do_location_id
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
        )

        _save_bronze_watermark(spark, new_bronze)
        print("Taxi silver MERGE complete.")
        return True

    if once:
        _silver_incremental_pass()
        return

    interval = _parse_trigger_seconds(trigger)
    while True:
        _silver_incremental_pass()
        time.sleep(interval)


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

