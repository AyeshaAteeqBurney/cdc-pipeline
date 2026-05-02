#!/usr/bin/env python3
"""
CDC entrypoint aligned with Airflow task names (bronze_cdc, silver_cdc, …).

Stages:
  bronze  — append-only raw Debezium events from Kafka → Iceberg (schema-flexible JSON columns).
  silver  — MERGE into silver Iceberg tables (dynamic column discovery; see ``run_silver``).

Run inside the `jupyter` container after the connector is registered (use spark-submit if needed):
  docker exec jupyter spark-submit /home/jovyan/project/jobs/cdc_pipeline.py --stage bronze --table customers
  docker exec jupyter spark-submit /home/jovyan/project/jobs/cdc_pipeline.py --stage bronze --table drivers
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import LongType, IntegerType, BooleanType

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


# Bootstrap DDL for brand-new clusters (CREATE TABLE IF NOT EXISTS). Silver still evolves:
# any top-level key present in Debezium ``after`` JSON but missing from Iceberg gets
# ``ALTER TABLE ... ADD COLUMN <name> STRING`` at silver runtime (see ``_evolve_silver_schema``).
SILVER_SCHEMAS = {
    "customers": """
        id         INT,
        name       STRING,
        email      STRING,
        country    STRING,
        created_at STRING
    """,
    "drivers": """
        id             INT,
        name           STRING,
        license_number STRING,
        rating         DOUBLE,
        city           STRING,
        active         BOOLEAN,
        created_at     STRING
    """,
}

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_sql_identifier(name: str) -> bool:
    return bool(_SAFE_IDENT.match(name))


def _discover_top_level_keys_from_upserts(upserts_df, *, max_samples: int = 8000) -> set[str]:
    """Union of top-level keys observed in ``after_json`` for this micro-batch."""
    keys: set[str] = set()
    for row in upserts_df.select("after_json").limit(max_samples).collect():
        raw = row.after_json
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            keys.update(obj.keys())
    return keys


def _silver_column_names(spark, silver_tgt: str) -> set[str]:
    return set(spark.table(silver_tgt).columns)


def _ordered_silver_columns(keys_from_json: set[str], existing_table_cols: set[str]) -> list[str]:
    """Merge JSON keys with existing Iceberg columns; stable order with ``id`` first."""
    return sorted(
        existing_table_cols | keys_from_json,
        key=lambda c: (0 if c == "id" else 1, c),
    )


def _evolve_silver_schema(spark, silver_tgt: str, keys_from_json: set[str]) -> None:
    """ADD COLUMN for every JSON key not yet present on the Iceberg silver table (new cols as STRING)."""
    existing = _silver_column_names(spark, silver_tgt)
    missing = keys_from_json - existing
    for col in sorted(missing):
        if not _is_safe_sql_identifier(col):
            print(f"Schema evolution: skip unsafe column name {col!r}")
            continue
        spark.sql(f"ALTER TABLE {silver_tgt} ADD COLUMN `{col}` STRING")
        print(f"Schema evolution: ALTER TABLE {silver_tgt} ADD COLUMN `{col}` STRING")


def _projection_column(col: str, table: str):
    """One Spark column expr from ``after_json``; unknown/new columns are STRING."""
    path = f"$.{col}"
    if col == "id":
        return F.get_json_object("after_json", path).cast(IntegerType()).alias(col)
    if table == "drivers" and col == "rating":
        return F.expr("try_cast(get_json_object(after_json, '$.rating') as DOUBLE)").alias(col)
    if table == "drivers" and col == "active":
        return F.get_json_object("after_json", path).cast(BooleanType()).alias(col)
    return F.get_json_object("after_json", path).alias(col)


def _build_upsert_projection(upserts_df, table: str, columns: list[str]):
    exprs = [_projection_column(c, table) for c in columns]
    return upserts_df.select(*exprs)


def _ensure_silver_table(spark, table: str) -> str:
    tgt = f"lakehouse.cdc.silver_{table}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt} (
            {SILVER_SCHEMAS[table]}
        )
        USING iceberg
    """)
    return tgt


def _get_silver_watermark(spark, table: str) -> dict:
    """Returns {partition: max_offset} already processed by silver. Empty dict on first run."""
    watermark_tbl = f"lakehouse.cdc.silver_{table}_watermark"
    try:
        rows = spark.read.table(watermark_tbl).collect()
        return {r["partition"]: r["max_offset"] for r in rows}
    except Exception:
        return {}


def _save_silver_watermark(spark, table: str, processed_df) -> None:
    """Save max kafka_offset per partition from the rows just processed."""
    watermark_tbl = f"lakehouse.cdc.silver_{table}_watermark"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {watermark_tbl} (
            partition INT,
            max_offset LONG
        ) USING iceberg
    """)
    new_max = (
        processed_df
        .groupBy("kafka_partition")
        .agg(F.max("kafka_offset").alias("max_offset"))
        .withColumnRenamed("kafka_partition", "partition")
    )
    new_max_local = spark.createDataFrame(new_max.collect(), new_max.schema)
    new_max_local.createOrReplaceTempView("new_wm")
    spark.sql(f"""
        MERGE INTO {watermark_tbl} AS target
        USING new_wm AS source
        ON target.partition = source.partition
        WHEN MATCHED THEN UPDATE SET target.max_offset = source.max_offset
        WHEN NOT MATCHED THEN INSERT (partition, max_offset) VALUES (source.partition, source.max_offset)
    """)


def run_silver(spark, *, table: str) -> None:
    bronze_tgt = f"lakehouse.cdc.bronze_{table}_flex"
    silver_tgt = _ensure_silver_table(spark, table)

    bronze_df = spark.read.table(bronze_tgt)

    # Filter bronze to only rows with kafka_offset > what silver already processed.
    wm = _get_silver_watermark(spark, table)
    if wm:
        from functools import reduce
        conditions = [
            (F.col("kafka_partition") == part) & (F.col("kafka_offset") > offset)
            for part, offset in wm.items()
        ]
        seen_parts = list(wm.keys())
        cond = reduce(lambda a, b: a | b, conditions)
        cond = cond | ~F.col("kafka_partition").isin(seen_parts)
        new_bronze = bronze_df.filter(cond)
    else:
        new_bronze = bronze_df

    if new_bronze.count() == 0:
        print(f"No new bronze events for {table}, silver is up to date.")
        return

    # Get record id from after_json for inserts/updates, before_json for deletes.
    with_id = new_bronze.withColumn(
        "record_id",
        F.coalesce(
            F.get_json_object("after_json", "$.id").cast(IntegerType()),
            F.get_json_object("before_json", "$.id").cast(IntegerType()),
        ),
    )

    # Keep only the latest event per record id.
    window = Window.partitionBy("record_id").orderBy(F.col("ts_ms").desc())
    latest = (
        with_id.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    upserts = latest.filter(F.col("op").isin("c", "u", "r"))
    deletes = latest.filter(F.col("op") == "d")

    # Apply upserts to silver (dynamic schema: JSON keys → Iceberg columns).
    if upserts.count() > 0:
        keys_from_json = _discover_top_level_keys_from_upserts(upserts)
        safe_keys = {k for k in keys_from_json if _is_safe_sql_identifier(k)}
        if "id" not in safe_keys:
            raise ValueError("Silver upsert batch has no `id` in after_json — cannot MERGE.")
        _evolve_silver_schema(spark, silver_tgt, safe_keys)
        existing_cols = _silver_column_names(spark, silver_tgt)
        merge_cols = _ordered_silver_columns(safe_keys, existing_cols)
        print(f"Silver MERGE columns ({len(merge_cols)}): {merge_cols}")
        upsert_df = _build_upsert_projection(upserts, table, merge_cols)
        # Recreate from collected rows to break Iceberg lineage before MERGE.
        upsert_local = spark.createDataFrame(upsert_df.collect(), upsert_df.schema)
        upsert_local.createOrReplaceTempView("silver_upserts")
        cols = ", ".join(upsert_df.columns)
        set_clause = ", ".join(f"target.{c} = source.{c}" for c in upsert_df.columns)
        src_cols = ", ".join(f"source.{c}" for c in upsert_df.columns)
        spark.sql(f"""
            MERGE INTO {silver_tgt} AS target
            USING silver_upserts AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({src_cols})
        """)

    # Apply deletes to silver.
    if deletes.count() > 0:
        delete_df = deletes.select(F.col("record_id").alias("id"))
        delete_local = spark.createDataFrame(delete_df.collect(), delete_df.schema)
        delete_local.createOrReplaceTempView("silver_deletes")
        spark.sql(f"""
            MERGE INTO {silver_tgt} AS target
            USING silver_deletes AS source
            ON target.id = source.id
            WHEN MATCHED THEN DELETE
        """)

    # Save watermark using kafka_offset so re-runs of bronze don't cause duplicates.
    _save_silver_watermark(spark, table, new_bronze)

    silver_count = spark.read.table(silver_tgt).count()
    print(f"Silver {silver_tgt} now has {silver_count} rows")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--stage",
        choices=["bronze", "silver"],
        default="bronze",
        help="Pipeline stage.",
    )
    p.add_argument("--table", choices=["customers", "drivers"], required=True)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic-prefix", default="dbserver1.public.")
    p.add_argument("--startingOffsets", default="earliest")
    p.add_argument("--endingOffsets", default="latest")
    args = p.parse_args()

    spark = get_spark(f"project3-cdc-{args.stage}-{args.table}")
    try:
        if args.stage == "bronze":
            run_bronze(
                spark,
                table=args.table,
                bootstrap=args.bootstrap,
                topic_prefix=args.topic_prefix,
                starting_offsets=args.startingOffsets,
                ending_offsets=args.endingOffsets,
            )
        elif args.stage == "silver":
            run_silver(spark, table=args.table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
