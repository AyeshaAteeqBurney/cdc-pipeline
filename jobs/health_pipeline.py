#!/usr/bin/env python3
"""
Computes CDC pipeline health metrics and appends one row to gold_pipeline_health.
Run after silver CDC jobs complete.

Usage:
    docker exec jupyter spark-submit --packages "..." /home/jovyan/project/jobs/health_pipeline.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pyspark.sql.functions as F

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.spark_session import get_spark


def _ensure_health_table(spark) -> str:
    tgt = "lakehouse.cdc.gold_pipeline_health"
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.gold_pipeline_health (
            run_timestamp          TIMESTAMP,
            events_c               LONG,
            events_u               LONG,
            events_d               LONG,
            events_r               LONG,
            bronze_row_count       LONG,
            silver_row_count       LONG,
            bronze_silver_delta    LONG,
            silver_pg_delta        LONG,
            new_customers          LONG,
            updated_customers      LONG,
            deleted_customers      LONG,
            processing_lag_seconds DOUBLE
        )
        USING iceberg
    """)
    return tgt


def _pg_counts() -> tuple[int, int]:
    # get live row counts from postgres
    import subprocess
    import json
    result = subprocess.run(
        [
            "python3", "-c",
            """
import psycopg2, os, json
conn = psycopg2.connect(
    host=os.environ.get('PG_HOST','postgres'),
    port=int(os.environ.get('PG_PORT',5432)),
    dbname=os.environ.get('PG_DB','sourcedb'),
    user=os.environ.get('PG_USER','cdc_user'),
    password=os.environ.get('PG_PASSWORD','admin')
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM customers')
c = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM drivers')
d = cur.fetchone()[0]
conn.close()
print(json.dumps({'customers': c, 'drivers': d}))
"""
        ],
        capture_output=True, text=True
    )
    data = json.loads(result.stdout.strip())
    return data["customers"], data["drivers"]


def run_health(spark) -> None:
    _ensure_health_table(spark)

    run_ts = F.current_timestamp()

    bronze_customers = spark.read.table("lakehouse.cdc.bronze_customers_flex")
    bronze_drivers   = spark.read.table("lakehouse.cdc.bronze_drivers_flex")
    silver_customers = spark.read.table("lakehouse.cdc.silver_customers")
    silver_drivers   = spark.read.table("lakehouse.cdc.silver_drivers")

    bronze_row_count = bronze_customers.count() + bronze_drivers.count()
    silver_row_count = silver_customers.count() + silver_drivers.count()

    # get postgres counts
    pg_customers, pg_drivers = _pg_counts()
    pg_total = pg_customers + pg_drivers

    bronze_silver_delta = bronze_row_count - silver_row_count
    silver_pg_delta     = silver_row_count - pg_total

    # get ts_ms watermark from last health run to count only new events
    try:
        last_ts_ms = (
            spark.read.table("lakehouse.cdc.gold_pipeline_health")
            .agg(F.max(F.col("run_timestamp").cast("long") * 1000).alias("max_ts"))
            .first()["max_ts"]
        )
    except Exception:
        last_ts_ms = None

    all_bronze = bronze_customers.unionByName(bronze_drivers)

    if last_ts_ms is not None:
        new_events = all_bronze.filter(F.col("ts_ms") > last_ts_ms)
        new_customer_events = bronze_customers.filter(F.col("ts_ms") > last_ts_ms)
    else:
        new_events = all_bronze
        new_customer_events = bronze_customers

    # count events by op type
    op_counts = (
        new_events.groupBy("op")
        .count()
        .collect()
    )
    op_map = {row["op"]: row["count"] for row in op_counts}

    events_c = op_map.get("c", 0)
    events_u = op_map.get("u", 0)
    events_d = op_map.get("d", 0)
    events_r = op_map.get("r", 0)

    # customer-level breakdown
    cust_ops = (
        new_customer_events.groupBy("op")
        .count()
        .collect()
    )
    cust_map = {row["op"]: row["count"] for row in cust_ops}
    new_customers     = cust_map.get("c", 0)
    updated_customers = cust_map.get("u", 0)
    deleted_customers = cust_map.get("d", 0)

    # processing lag: gap between newest DB change and now
    max_ts_ms_row = all_bronze.agg(F.max("ts_ms").alias("max_ts_ms")).first()
    max_ts_ms = max_ts_ms_row["max_ts_ms"] if max_ts_ms_row["max_ts_ms"] else 0

    lag_df = spark.sql(f"""
        SELECT (unix_timestamp(current_timestamp()) - {max_ts_ms} / 1000.0) AS lag_seconds
    """)
    processing_lag_seconds = lag_df.first()["lag_seconds"]

    # build one-row dataframe and append
    from pyspark.sql.types import (
        StructType, StructField, TimestampType, LongType, DoubleType
    )
    from datetime import datetime, timezone

    schema = StructType([
        StructField("run_timestamp",          TimestampType(), True),
        StructField("events_c",               LongType(),      True),
        StructField("events_u",               LongType(),      True),
        StructField("events_d",               LongType(),      True),
        StructField("events_r",               LongType(),      True),
        StructField("bronze_row_count",       LongType(),      True),
        StructField("silver_row_count",       LongType(),      True),
        StructField("bronze_silver_delta",    LongType(),      True),
        StructField("silver_pg_delta",        LongType(),      True),
        StructField("new_customers",          LongType(),      True),
        StructField("updated_customers",      LongType(),      True),
        StructField("deleted_customers",      LongType(),      True),
        StructField("processing_lag_seconds", DoubleType(),    True),
    ])

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    row = [(
        now,
        int(events_c),
        int(events_u),
        int(events_d),
        int(events_r),
        int(bronze_row_count),
        int(silver_row_count),
        int(bronze_silver_delta),
        int(silver_pg_delta),
        int(new_customers),
        int(updated_customers),
        int(deleted_customers),
        float(processing_lag_seconds),
    )]

    health_df = spark.createDataFrame(row, schema)
    health_df.writeTo("lakehouse.cdc.gold_pipeline_health").append()

    print(f"\n=== Pipeline Health ===")
    print(f"Run timestamp:       {now}")
    print(f"Events (c/u/d/r):    {events_c}/{events_u}/{events_d}/{events_r}")
    print(f"Bronze rows:         {bronze_row_count}")
    print(f"Silver rows:         {silver_row_count}")
    print(f"Bronze-Silver delta: {bronze_silver_delta}")
    print(f"Silver-PG delta:     {silver_pg_delta}  {'OK' if silver_pg_delta == 0 else 'DRIFT DETECTED'}")
    print(f"New customers:       {new_customers}")
    print(f"Updated customers:   {updated_customers}")
    print(f"Deleted customers:   {deleted_customers}")
    print(f"Processing lag:      {processing_lag_seconds:.1f}s")

    # print alerts
    print(f"\n=== Alerts ===")
    spark.read.table("lakehouse.cdc.gold_pipeline_health").createOrReplaceTempView("health")
    spark.sql("""
        SELECT run_timestamp,
               silver_pg_delta,
               (events_c + events_u + events_d + events_r) AS total_events,
               processing_lag_seconds,
               CASE WHEN silver_pg_delta != 0                              THEN 'DATA DRIFT'  ELSE NULL END AS drift_alert,
               CASE WHEN (events_c + events_u + events_d + events_r) = 0  THEN 'SOURCE DOWN' ELSE NULL END AS silence_alert,
               CASE WHEN processing_lag_seconds > 300                      THEN 'HIGH LAG'    ELSE NULL END AS lag_alert
        FROM health
        WHERE silver_pg_delta != 0
           OR (events_c + events_u + events_d + events_r) = 0
           OR processing_lag_seconds > 300
        ORDER BY run_timestamp DESC
    """).show(truncate=False)

    # average lag over last 10 runs
    print("\n=== Avg processing lag over last 10 runs ===")
    spark.sql("""
        SELECT ROUND(AVG(processing_lag_seconds), 2) AS avg_lag_seconds
        FROM (
            SELECT processing_lag_seconds
            FROM health
            ORDER BY run_timestamp DESC
            LIMIT 10
        )
    """).show()

    # runs with data drift
    print("=== Runs with data drift (silver != postgres) ===")
    spark.sql("""
        SELECT run_timestamp, silver_row_count, silver_pg_delta
        FROM health
        WHERE silver_pg_delta != 0
        ORDER BY run_timestamp DESC
    """).show(truncate=False)

    return int(silver_pg_delta)


def main() -> None:
    spark = get_spark("project3-cdc-health")
    try:
        drift = run_health(spark)
    finally:
        spark.stop()

    if drift != 0:
        print(f"VALIDATE FAILED: silver_pg_delta={drift}. Silver does not mirror PostgreSQL.")
        sys.exit(1)


if __name__ == "__main__":
    main()
