#!/usr/bin/env python3
"""
CDC path: one entrypoint for the group, aligned with Airflow task names (bronze_cdc, silver_cdc, …).

Stages:
  bronze  — append-only raw Debezium events from Kafka → Iceberg (schema-flexible JSON columns).
  silver  — add a ``--stage silver`` branch here when implementing the DAG’s ``silver_cdc`` task (MERGE).

Run inside the `jupyter` container after the connector is registered (use spark-submit if needed):
  docker exec jupyter spark-submit /home/jovyan/project/jobs/cdc_pipeline.py --stage bronze --table customers
  docker exec jupyter spark-submit /home/jovyan/project/jobs/cdc_pipeline.py --stage bronze --table drivers
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.types import LongType

# Ensure `/home/jovyan/project` is on sys.path when run by absolute path.
PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.spark_session import get_spark


def _ensure_bronze_table(spark, table: str) -> str:
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.cdc")
    tgt = f"lakehouse.cdc.bronze_{table}_flex"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {tgt} (
            op STRING,
            ts_ms LONG,
            source_lsn LONG,
            before_json STRING,
            after_json STRING,
            value_json STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset LONG,
            kafka_timestamp TIMESTAMP,
            ingested_at TIMESTAMP
        )
        USING iceberg
        """
    )
    return tgt


def run_bronze(
    spark,
    *,
    table: str,
    bootstrap: str,
    topic_prefix: str,
    starting_offsets: str,
    ending_offsets: str,
) -> None:
    topic = f"{topic_prefix}{table}"
    target = _ensure_bronze_table(spark, table)

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", ending_offsets)
        .load()
        .select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("value_json"),
        )
    )

    non_tombstone = raw.filter(F.col("value_json").isNotNull())

    out = non_tombstone.select(
        F.get_json_object("value_json", "$.payload.op").alias("op"),
        F.get_json_object("value_json", "$.payload.ts_ms").cast(LongType()).alias("ts_ms"),
        F.get_json_object("value_json", "$.payload.source.lsn").cast(LongType()).alias("source_lsn"),
        F.get_json_object("value_json", "$.payload.before").alias("before_json"),
        F.get_json_object("value_json", "$.payload.after").alias("after_json"),
        F.col("value_json").alias("value_json"),
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("ingested_at"),
    )

    out.writeTo(target).append()
    print(f"Wrote {out.count()} rows to {target} from topic {topic}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--stage",
        choices=["bronze"],
        default="bronze",
        help="Pipeline stage (use bronze for the bronze_cdc Airflow task; extend with silver for silver_cdc).",
    )
    p.add_argument("--table", choices=["customers", "drivers"], required=True)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic-prefix", default="dbserver1.public.")
    p.add_argument("--startingOffsets", default="earliest")
    p.add_argument("--endingOffsets", default="latest")
    args = p.parse_args()

    spark = get_spark(f"project3-cdc-{args.stage}-{args.table}")
    try:
        run_bronze(
            spark,
            table=args.table,
            bootstrap=args.bootstrap,
            topic_prefix=args.topic_prefix,
            starting_offsets=args.startingOffsets,
            ending_offsets=args.endingOffsets,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
