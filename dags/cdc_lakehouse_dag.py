"""
CDC & Orchestrated Lakehouse Pipeline DAG
==========================================
Orchestrates both data paths end-to-end on a 15-minute schedule:

  Path A (CDC):  PostgreSQL → Debezium → Kafka → Bronze → Silver (MERGE)
  Path B (Taxi): Kafka → Bronze → Silver → Gold

Schedule: every 15 minutes  → supports a 15-minute freshness SLA.
Phased layers (course README): all bronze tasks must finish before any silver
task starts; each silver task depends on all three bronze tasks.

Idempotent: Silver CDC uses Kafka-offset watermarks; re-running with no new
bronze events is a no-op. Bronze taxi uses Spark streaming checkpoints.
Gold overwrites partitions — same input always produces same output.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor


def _on_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    logging.error(
        "ALERT: Task failed! DAG=%s  Task=%s  Run=%s", dag_id, task_id, run_id
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT = "/home/jovyan/project"
PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.1"
)

# spark-submit inside the jupyter container (where Iceberg + Kafka jars live)
SPARK_SUBMIT = f"docker exec jupyter spark-submit --packages {PACKAGES}"

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": False,
    "on_failure_callback": _on_failure,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="cdc_lakehouse_pipeline",
    description="CDC (Debezium→Iceberg) + Taxi streaming pipeline, both paths orchestrated.",
    schedule_interval="*/15 * * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["cdc", "iceberg", "kafka", "taxi"],
) as dag:

    # ── 1. Connector health ─────────────────────────────────────────────────
    # HttpSensor polls the Debezium Connect REST API.
    # If the connector is not RUNNING (or not registered), the sensor times
    # out after 2 minutes and marks the task as failed — all downstream tasks
    # are skipped automatically because they depend on this with ALL_SUCCESS.
    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="debezium_connect",          # configured in Airflow UI
        endpoint="/connectors/pg-cdc/status",
        request_params={},
        response_check=lambda response: (
            response.json().get("connector", {}).get("state") == "RUNNING"
            and all(
                t.get("state") == "RUNNING"
                for t in response.json().get("tasks", [])
            )
        ),
        poke_interval=15,
        timeout=120,
        mode="reschedule",
        retries=2,
    )

    # ── 2a. Bronze CDC — customers ──────────────────────────────────────────
    bronze_cdc_customers = BashOperator(
        task_id="bronze_cdc_customers",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/cdc_pipeline.py "
            "--stage bronze --table customers "
            "--bootstrap kafka:9092 "
            "--startingOffsets earliest --endingOffsets latest"
        ),
    )

    # ── 2b. Bronze CDC — drivers ────────────────────────────────────────────
    bronze_cdc_drivers = BashOperator(
        task_id="bronze_cdc_drivers",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/cdc_pipeline.py "
            "--stage bronze --table drivers "
            "--bootstrap kafka:9092 "
            "--startingOffsets earliest --endingOffsets latest"
        ),
    )

    # ── 2c. Bronze Taxi ─────────────────────────────────────────────────────
    # --once: uses trigger(availableNow=True) so the stream processes all
    # currently available Kafka messages and exits within 300 s.
    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/taxi_pipeline.py "
            "--mode bronze --bootstrap kafka:9092 --once"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── 3a. Silver CDC — customers (MERGE) ──────────────────────────────────
    silver_cdc_customers = BashOperator(
        task_id="silver_cdc_customers",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/cdc_pipeline.py "
            "--stage silver --table customers"
        ),
    )

    # ── 3b. Silver CDC — drivers (MERGE) ────────────────────────────────────
    silver_cdc_drivers = BashOperator(
        task_id="silver_cdc_drivers",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/cdc_pipeline.py "
            "--stage silver --table drivers"
        ),
    )

    # ── 3c. Silver Taxi ─────────────────────────────────────────────────────
    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/taxi_pipeline.py "
            "--mode silver --once"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── 4. Gold Taxi ────────────────────────────────────────────────────────
    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/taxi_pipeline.py "
            "--mode gold"
        ),
    )

    # ── 5. Validate ─────────────────────────────────────────────────────────
    # Runs health_pipeline.py which compares silver CDC row counts against
    # live PostgreSQL counts. Exits with code 1 if silver_pg_delta != 0,
    # causing this task to fail and trigger its retries / alert.
    # Runs after gold_taxi only; gold_taxi waits on all silvers (CDC + taxi).
    validate = BashOperator(
        task_id="validate",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"{PROJECT}/jobs/health_pipeline.py"
        ),
    )

    # ── Dependency chain (phased bronze → silver) ───────────────────────────
    #
    #  connector_health
    #       │
    #       ├── bronze_cdc_customers ──┐
    #       ├── bronze_cdc_drivers   ──┼── ALL bronze done ──► [silver_cdc_* , silver_taxi]
    #       └── bronze_taxi ───────────┘                              │
    #                                           silver_cdc_* + silver_taxi ──► gold_taxi ──► validate
    #
    _bronze = [bronze_cdc_customers, bronze_cdc_drivers, bronze_taxi]

    connector_health >> _bronze
    _bronze >> silver_cdc_customers
    _bronze >> silver_cdc_drivers
    _bronze >> silver_taxi

    silver_taxi >> gold_taxi
    [silver_cdc_customers, silver_cdc_drivers] >> gold_taxi >> validate
