"""dags/nammamart_elt_chandrika.py

NammaMart ELT Pipeline — SQL-Only Transformation DAG

Flow:
  Phase 1  EXTRACT + LOAD RAW   Read CSVs → insert into raw_* tables (all VARCHAR)
  Phase 2  STAGING SQL           Cast, clean, dedupe, mask PII, DQ flags
  Phase 3  MARTS SQL             Build 6 business-ready tables from clean staging data
  Phase 4  INCREMENTAL           MERGE increment orders into staging + rebuild marts
  Phase 5  AUDIT                 Write pipeline_runs row

Key Difference from ETL:
  - ETL transforms in Python (pandas) BEFORE loading
  - ELT loads raw data first, then transforms using SQL INSIDE DuckDB

Schedule: 9:00 AM IST (03:30 UTC), Monday-Saturday
Warehouse: include/warehouse_chandrika.duckdb
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
INCLUDE_DIR = Path("/usr/local/airflow/include")
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE_PATH = INCLUDE_DIR / "warehouse_chandrika.duckdb"

DATASET_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

INCREMENT_FILE = "orders_2026-10-01_increment.csv"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _run_sql_file(connection, sql_path):
    """Read a .sql file and execute each statement (split on ';')."""
    sql_text = Path(sql_path).read_text(encoding="utf-8")
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        # Skip pure comments
        lines = [l for l in stmt.split("\n") if not l.strip().startswith("--")]
        if not any(l.strip() for l in lines):
            continue
        connection.execute(stmt)
    return len(statements)


def _warehouse_has_raw_orders():
    """Check whether raw_orders table exists and has data."""
    if not WAREHOUSE_PATH.exists():
        return False
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        count = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'raw_orders'"
        ).fetchone()[0]
        if not count:
            return False
        rows = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        return rows > 0
    except Exception:
        return False
    finally:
        con.close()


def _resolve_run_mode(params):
    requested = str(params.get("run_mode", "auto")).lower()
    if requested == "initial":
        return "initial"
    if requested == "incremental":
        if not _warehouse_has_raw_orders():
            raise AirflowFailException(
                "run_mode='incremental' but raw_orders is empty."
            )
        return "incremental"
    if requested == "auto":
        return "incremental" if _warehouse_has_raw_orders() else "initial"
    raise AirflowFailException(f"Unsupported run_mode: {requested}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nammamart_elt_chandrika",
    description=(
        "NammaMart ELT: load raw CSVs, SQL-only transform in DuckDB "
        "(staging + marts), incremental MERGE, audit"
    ),
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    schedule="0 9 * * 1-6",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "chandrika",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "run_mode": Param(
            "auto",
            type="string",
            enum=["auto", "initial", "incremental"],
            description=(
                "auto = initial when warehouse is empty, "
                "otherwise incremental."
            ),
        ),
    },
    tags=["nammamart", "elt", "chandrika"],
) as dag:

    # ===================================================================
    # PHASE 1: EXTRACT + LOAD RAW (Part B.1 + B.2)
    # ===================================================================

    @task
    def load_raw_data():
        """Read CSVs and INSERT into raw_* tables (all VARCHAR)."""
        context = get_current_context()
        mode = _resolve_run_mode(context["params"])
        run_id = context["run_id"]

        con = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            # Create raw tables if they don't exist
            _run_sql_file(con, SQL_DIR / "01_create_raw.sql")

            rows_loaded = {}

            if mode == "initial":
                # Load all 5 base datasets
                for dataset_name, filename in DATASET_FILES.items():
                    csv_path = DATA_DIR / filename
                    if not csv_path.exists():
                        raise AirflowFailException(
                            f"CSV not found: {csv_path}"
                        )
                    df = pd.read_csv(str(csv_path), dtype=str)
                    df["_loaded_at"] = pd.Timestamp.now()
                    df["_source_file"] = filename
                    df["_run_id"] = run_id

                    table_name = f"raw_{dataset_name}"
                    con.register("_csv_data", df)
                    con.execute(f"DROP TABLE IF EXISTS {table_name}")  # Clear old data
                    con.execute(
                        f"CREATE TABLE {table_name} AS "
                        "SELECT * FROM _csv_data"
                    )
                    con.unregister("_csv_data")
                    rows_loaded[dataset_name] = len(df)
                    logger.info(
                        "Loaded %s: %d rows into %s",
                        filename, len(df), table_name,
                    )
            else:
                # Incremental: load only the increment file into raw_orders
                csv_path = DATA_DIR / INCREMENT_FILE
                if not csv_path.exists():
                    raise AirflowFailException(
                        f"Increment CSV not found: {csv_path}"
                    )
                df = pd.read_csv(str(csv_path), dtype=str)
                df["_loaded_at"] = pd.Timestamp.now()
                df["_source_file"] = INCREMENT_FILE
                df["_run_id"] = run_id

                con.register("_csv_data", df)
                con.execute(
                    "INSERT INTO raw_orders "
                    "SELECT * FROM _csv_data"
                )
                con.unregister("_csv_data")
                rows_loaded["orders_increment"] = len(df)
                logger.info(
                    "Loaded increment: %d rows into raw_orders",
                    len(df),
                )
        finally:
            con.close()

        return {
            "run_id": run_id,
            "run_mode": mode,
            "rows_loaded": rows_loaded,
            "total_loaded": sum(rows_loaded.values()),
        }

    # ===================================================================
    # PHASE 2: STAGING SQL (Part B.3)
    # ===================================================================

    @task
    def run_staging_sql(load_result):
        """Execute 02_create_staging.sql — SQL-only transforms."""
        import time
        time.sleep(5)  # Let raw load release DuckDB lock

        con = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            stmt_count = _run_sql_file(
                con, SQL_DIR / "02_create_staging.sql"
            )
            # Count staging rows
            counts = {}
            for table in [
                "stg_orders", "stg_payments", "stg_customers",
                "stg_products", "stg_stores",
            ]:
                try:
                    n = con.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                    counts[table] = n
                except Exception:
                    counts[table] = 0

            # Count DQ pass/fail
            pass_count = con.execute(
                "SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'PASS'"
            ).fetchone()[0]
            fail_count = con.execute(
                "SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'FAIL'"
            ).fetchone()[0]

            logger.info(
                "Staging complete: %d statements, orders pass=%d fail=%d",
                stmt_count, pass_count, fail_count,
            )
        finally:
            con.close()

        return {
            "run_id": load_result["run_id"],
            "run_mode": load_result["run_mode"],
            "staging_counts": counts,
            "orders_pass": pass_count,
            "orders_fail": fail_count,
        }

    # ===================================================================
    # PHASE 3: MARTS SQL (Part B.4)
    # ===================================================================

    @task
    def run_marts_sql(staging_result):
        """Execute 03_create_marts.sql — business-ready tables."""
        import time
        time.sleep(5)

        con = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            stmt_count = _run_sql_file(
                con, SQL_DIR / "03_create_marts.sql"
            )
            # Count mart rows
            counts = {}
            for table in [
                "mart_daily_zone_revenue", "mart_category_margin",
                "mart_delivery_sla", "mart_payment_reconciliation",
                "mart_top_customers", "mart_elt_dq_summary",
            ]:
                try:
                    n = con.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                    counts[table] = n
                except Exception:
                    counts[table] = 0

            logger.info("Marts complete: %s", counts)
        finally:
            con.close()

        return {
            "run_id": staging_result["run_id"],
            "run_mode": staging_result["run_mode"],
            "mart_counts": counts,
        }

    # ===================================================================
    # PHASE 4: INCREMENTAL MERGE (Part B.5) — only for incremental runs
    # ===================================================================

    @task
    def run_incremental_merge(staging_result):
        """Execute 04_incremental_orders.sql if mode is incremental."""
        if staging_result["run_mode"] != "incremental":
            logger.info("Initial run — skipping incremental merge.")
            return {"skipped": True, "run_mode": staging_result["run_mode"]}

        import time
        time.sleep(5)  # Let staging release DuckDB lock

        con = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            _run_sql_file(con, SQL_DIR / "04_incremental_orders.sql")

            stg_count = con.execute(
                "SELECT COUNT(*) FROM stg_orders"
            ).fetchone()[0]

            logger.info(
                "Incremental merge done. stg_orders now has %d rows.",
                stg_count,
            )
        finally:
            con.close()

        return {
            "skipped": False,
            "run_mode": "incremental",
            "stg_orders_after_merge": stg_count,
        }

    # ===================================================================
    # PHASE 4b: REBUILD MARTS after incremental merge
    # ===================================================================

    @task
    def rebuild_marts_after_merge(merge_result):
        """Re-run marts SQL after incremental merge to reflect changes."""
        if merge_result.get("skipped"):
            logger.info("Skipped — no merge happened.")
            return {"skipped": True}

        import time
        time.sleep(5)

        con = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            _run_sql_file(con, SQL_DIR / "03_create_marts.sql")
            logger.info("Marts rebuilt after incremental merge.")
        finally:
            con.close()

        return {"skipped": False, "status": "REBUILT"}

    # ===================================================================
    # PHASE 5: AUDIT (Part F)
    # ===================================================================

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def write_elt_audit():
        """Write one row per run to pipeline_runs."""
        context = get_current_context()
        run_id = context["run_id"]
        ti = context["ti"]

        load_result = ti.xcom_pull(task_ids="load_raw_data") or {}
        staging_result = ti.xcom_pull(task_ids="run_staging_sql") or {}
        mart_result = ti.xcom_pull(task_ids="run_marts_sql") or {}

        dag_run = context["dag_run"]
        try:
            task_instances = dag_run.get_task_instances()
            failed_tasks = [
                inst.task_id
                for inst in task_instances
                if inst.state in {"failed", "upstream_failed"}
                and inst.task_id != "write_elt_audit"
            ]
        except Exception as exc:
            logger.warning("Could not fetch task states: %s", exc)
            failed_tasks = []

        status = (
            "FAILED" if failed_tasks
            else "COMPLETED_WITH_DQ_FLAGS"
            if staging_result.get("orders_fail", 0) > 0
            else "SUCCESS"
        )

        import time
        time.sleep(5)

        con = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    run_mode VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    rows_extracted BIGINT,
                    rows_loaded BIGINT,
                    rows_quarantined BIGINT,
                    status VARCHAR,
                    failed_tasks VARCHAR,
                    reconciliation_rows BIGINT,
                    audited_at TIMESTAMP
                )
            """)
            con.execute(
                "DELETE FROM pipeline_runs WHERE run_id = ?",
                [run_id],
            )
            con.execute(
                "INSERT INTO pipeline_runs VALUES "
                "(?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, "
                "CURRENT_TIMESTAMP)",
                [
                    run_id,
                    "nammamart_elt_chandrika",
                    load_result.get("run_mode", "unknown"),
                    context["dag_run"].start_date,
                    load_result.get("total_loaded", 0),
                    staging_result.get("orders_pass", 0),
                    staging_result.get("orders_fail", 0),
                    status,
                    ", ".join(failed_tasks),
                    sum(
                        mart_result.get("mart_counts", {}).values()
                    ) if mart_result.get("mart_counts") else 0,
                ],
            )
        finally:
            con.close()

        logger.info(
            "ELT Audit: run_id=%s, status=%s, loaded=%s",
            run_id, status, load_result.get("total_loaded", 0),
        )

    # ===================================================================
    # WIRE UP DEPENDENCIES
    # ===================================================================
    raw = load_raw_data()
    staging = run_staging_sql(raw)
    marts = run_marts_sql(staging)

    # Incremental path (runs after staging, parallel with initial marts)
    merge = run_incremental_merge(staging)
    rebuild = rebuild_marts_after_merge(merge)

    # Audit runs after everything
    [marts, rebuild] >> write_elt_audit()
