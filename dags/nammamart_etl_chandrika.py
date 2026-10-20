"""dags/nammamart_etl_chandrika.py

NammaMart ETL Pipeline — 8-Phase Airflow DAG

Flow (matching ASSIGNMENT.md Part A steps 1-7 + Part D + Part F + Bonus):

  Phase 1  EXTRACT   5 parallel tasks read CSVs
  Phase 2  VALIDATE  DQ checks (technical + business rules) + write dq_results
  Phase 3  QUARANTINE  write failed rows + AI explain + Slack alert
  Phase 4  TRANSFORM   mask PII, derive measures, build star schema
  Phase 5  LOAD        upsert fact + create/replace 3 dimension tables
  Phase 6  RECONCILE   compare payments vs orders
  Phase 7  REPORT      build zone revenue + category margin tables
  Phase 8  AUDIT       write pipeline_runs row (trigger=ALL_DONE)

Schedule: 9:00 AM IST (03:30 UTC), Monday-Saturday
Warehouse: include/warehouse_chandrika.duckdb
Quarantine: include/quarantine/nammamart/<run_id>/
Staging: include/staging/nammamart/<run_id>/
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
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from include.extract_nammamart import extract_dataset
from include.quality_checks_nammamart import run_technical_checks
from include.business_rules_check_nammamart import run_business_rules
from include.transform_nammamart import run_transformations
try:
    from include.ai_explainer_nammamart import explain_failures
except ImportError:
    explain_failures = None
    logging.getLogger(__name__).info(
        "[AI] langgraph/langchain not installed — AI explainer disabled."
    )
from include.slack_notifier_nammamart import send_slack_alert

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
INCLUDE_DIR = Path("/usr/local/airflow/include")
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
WAREHOUSE_PATH = INCLUDE_DIR / "warehouse_chandrika.duckdb"
STAGING_DIR = INCLUDE_DIR / "staging" / "nammamart"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "nammamart"

DATASET_NAMES = ("orders", "payments", "customers", "products", "stores")
BLOCKING_SEVERITIES = {"HIGH", "CRITICAL", "ERROR", "FAIL"}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _normalise_findings(findings):
    """Add consistent aliases to each finding dict."""
    normalised = []
    for finding in findings or []:
        item = dict(finding)
        item["dataset"] = (
            item.get("dataset") or item.get("file")
            or item.get("dataset_name") or item.get("table_name")
            or item.get("source_file")
        )
        item["reason"] = (
            item.get("reason") or item.get("problem")
            or item.get("message") or item.get("issue")
            or item.get("error_message") or "Data quality check failed"
        )
        item["severity"] = str(item.get("severity", "HIGH")).upper()
        source_row = item.get(
            "source_row", item.get("row_number", item.get("row_num"))
        )
        try:
            item["source_row"] = int(source_row) if source_row is not None else None
        except (TypeError, ValueError):
            item["source_row"] = None
        item["dataset"] = (
            str(item["dataset"]).strip().lower()
            if item.get("dataset") is not None else None
        )
        normalised.append(item)
    return normalised


def _payload_to_df(payload):
    """Convert an extracted JSON payload to a DataFrame."""
    if isinstance(payload, pd.DataFrame):
        return payload.copy()
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, str):
        try:
            records = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise AirflowFailException(
                f"Could not parse extracted JSON: {exc}"
            ) from exc
        return pd.DataFrame(records if isinstance(records, list) else [])
    raise AirflowFailException(
        f"Unsupported extracted payload type: {type(payload).__name__}"
    )


def _warehouse_has_orders():
    """Check whether the warehouse already contains orders."""
    if not WAREHOUSE_PATH.exists():
        return False
    connection = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        tables = connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'etl_fact_orders'"
        ).fetchone()[0]
        if not tables:
            return False
        count = connection.execute(
            "SELECT COUNT(*) FROM etl_fact_orders"
        ).fetchone()[0]
        return count > 0
    finally:
        connection.close()


def _resolve_run_mode(params):
    """auto = initial when warehouse empty; auto = incremental otherwise."""
    requested = str(params.get("run_mode", "auto")).lower()
    if requested == "initial":
        return "initial"
    if requested == "incremental":
        if not _warehouse_has_orders():
            raise AirflowFailException(
                "run_mode='incremental' requested but etl_fact_orders "
                "is not populated."
            )
        return "incremental"
    if requested == "auto":
        return "incremental" if _warehouse_has_orders() else "initial"
    raise AirflowFailException(f"Unsupported run_mode: {requested}")


def _is_payment_order_ref_finding(finding):
    dataset = str(finding.get("file") or finding.get("dataset") or "").lower()
    column = str(finding.get("column") or "").lower()
    return dataset == "payments" and column == "order_id"


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(value, str):
        # Already a JSON string — write directly (avoid double-serialization)
        path.write_text(value, encoding="utf-8")
    else:
        path.write_text(
            json.dumps(value, default=str, ensure_ascii=False),
            encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nammamart_etl_chandrika",
    description=(
        "NammaMart ETL: extract, validate, quarantine, transform, "
        "load star schema, reconcile, report, audit"
    ),
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    schedule="0 9 * * 1-6",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "chandrika",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "run_mode": Param(
            "auto",
            type="string",
            enum=["auto", "initial", "incremental"],
            description=(
                "auto = initial when warehouse is empty, "
                "otherwise incremental upsert."
            ),
        ),
    },
    tags=["nammamart", "etl", "chandrika"],
) as dag:

    # ===================================================================
    # PHASE 1: EXTRACT (Part A.1) — 5 parallel tasks
    # ===================================================================

    @task
    def extract_orders():
        context = get_current_context()
        mode = _resolve_run_mode(context["params"])
        dataset_name = (
            "orders" if mode == "initial" else "orders_incremental"
        )
        payload = extract_dataset(dataset_name)
        return {
            "payload": payload,
            "run_mode": mode,
            "source_dataset": dataset_name,
        }

    @task
    def extract_payments():
        return extract_dataset("payments")

    @task
    def extract_customers():
        return extract_dataset("customers")

    @task
    def extract_products():
        return extract_dataset("products")

    @task
    def extract_stores():
        return extract_dataset("stores")

    # ===================================================================
    # PHASE 2: VALIDATE (Part A.2 + Part D)
    # ===================================================================

    @task
    def validate_data_quality(
        orders_result, payments_json, customers_json,
        products_json, stores_json,
    ):
        context = get_current_context()
        mode = orders_result["run_mode"]
        run_id = context["run_id"]

        dataset_json = {
            "orders": orders_result["payload"],
            "payments": payments_json,
            "customers": customers_json,
            "products": products_json,
            "stores": stores_json,
        }

        # Persist staging for quarantine/transform
        staging_dir = STAGING_DIR / run_id
        staging_dir.mkdir(parents=True, exist_ok=True)
        for name, payload in dataset_json.items():
            _write_json(staging_dir / f"{name}.json", payload)

        datasets = {
            name: _payload_to_df(payload)
            for name, payload in dataset_json.items()
        }

        # Run checks
        technical_findings = run_technical_checks(datasets)

        # For incremental: validate payment refs against full orders
        if mode == "incremental":
            full_orders_payload = extract_dataset("orders")
            ref_datasets = dict(datasets)
            ref_datasets["orders"] = _payload_to_df(full_orders_payload)
            historical_findings = run_technical_checks(ref_datasets)
            technical_findings = [
                f for f in technical_findings
                if not _is_payment_order_ref_finding(f)
            ]
            technical_findings.extend(
                f for f in historical_findings
                if _is_payment_order_ref_finding(f)
            )

        findings = run_business_rules(
            datasets, existing_issues=technical_findings
        )
        findings = _normalise_findings(findings)

        # Classify findings
        pipeline_blockers = [
            item for item in findings
            if (
                item.get("severity") in BLOCKING_SEVERITIES
                and (
                    item.get("dataset") not in DATASET_NAMES
                    or item.get("source_row") is None
                )
            )
        ]

        blocked_source_rows = {name: [] for name in DATASET_NAMES}
        for item in findings:
            if (
                item.get("severity") in BLOCKING_SEVERITIES
                and item.get("dataset") in DATASET_NAMES
                and item.get("source_row") is not None
            ):
                blocked_source_rows[item["dataset"]].append(
                    item["source_row"]
                )
        blocked_source_rows = {
            name: sorted(set(rows))
            for name, rows in blocked_source_rows.items()
        }

        warning_count = sum(
            1 for item in findings if item["severity"] == "WARNING"
        )
        blocking_count = sum(
            1 for item in findings
            if item["severity"] in BLOCKING_SEVERITIES
        )
        total_rows = sum(len(datasets[n]) for n in DATASET_NAMES)

        result = {
            "run_id": run_id,
            "run_mode": mode,
            "staging_dir": str(staging_dir),
            "findings": findings,
            "blocked_source_rows": blocked_source_rows,
            "pipeline_blockers": pipeline_blockers,
            "warning_count": warning_count,
            "blocking_count": blocking_count,
            "total_rows": total_rows,
            "status": "FAIL" if pipeline_blockers else "CHECKED",
        }

        _write_json(staging_dir / "dq_result.json", result)
        logger.info(
            "DQ completed: mode=%s, blocking=%s, warnings=%s",
            mode, blocking_count, warning_count,
        )
        return result

    @task
    def write_dq_results(dq_result):
        """Write aggregated DQ findings to dq_results table (Part D)."""
        run_id = dq_result["run_id"]
        findings = dq_result["findings"]

        # Aggregate by dimension + severity
        aggregated = {}
        for f in findings:
            dim = f.get("quality_dimension", f.get("dimension", "Unknown"))
            sev = f.get("severity", "HIGH")
            key = (dim, sev)
            if key not in aggregated:
                aggregated[key] = {"rows_checked": 0, "rows_failed": 0}
            aggregated[key]["rows_failed"] += 1

        connection = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    check_name VARCHAR,
                    dimension VARCHAR,
                    severity VARCHAR,
                    rows_checked BIGINT,
                    rows_failed BIGINT,
                    status VARCHAR,
                    checked_at TIMESTAMP
                )
            """)
            connection.execute(
                "DELETE FROM dq_results WHERE run_id = ?", [run_id]
            )

            for (dim, sev), counts in aggregated.items():
                status = "FAIL" if counts["rows_failed"] > 0 else "PASS"
                connection.execute(
                    "INSERT INTO dq_results VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                    [
                        run_id,
                        "nammamart_etl_chandrika",
                        f"{dim}_{sev}",
                        dim,
                        sev,
                        dq_result.get("total_rows", 0),
                        counts["rows_failed"],
                        status,
                    ],
                )
        finally:
            connection.close()

        logger.info(
            "dq_results: wrote %d check entries for run %s",
            len(aggregated), run_id,
        )
        return {"dq_entries": len(aggregated)}

    # ===================================================================
    # PHASE 3: QUARANTINE + AI + SLACK (Part A.3 + Bonus)
    # ===================================================================

    @task
    def quarantine_failed_records(dq_result):
        """Write failed rows to CSV files with failure_reason column."""
        findings = dq_result["findings"]
        pipeline_blockers = dq_result["pipeline_blockers"]
        staging_dir = Path(dq_result["staging_dir"])
        quarantine_dir = QUARANTINE_DIR / dq_result["run_id"]
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        quarantine_summary = {}

        if pipeline_blockers:
            _write_json(
                quarantine_dir / "pipeline_blockers.json",
                pipeline_blockers,
            )

        for dataset_name in DATASET_NAMES:
            source_path = staging_dir / f"{dataset_name}.json"
            records = json.loads(source_path.read_text(encoding="utf-8"))
            if not isinstance(records, list):
                records = []

            failed_rows = set(
                dq_result["blocked_source_rows"].get(dataset_name, [])
            )
            failed_records = []

            for source_row in sorted(failed_rows):
                row_index = source_row - 2
                if row_index < 0 or row_index >= len(records):
                    continue
                reasons = [
                    item["reason"]
                    for item in findings
                    if item.get("dataset") == dataset_name
                    and item.get("source_row") == source_row
                    and item.get("severity") in BLOCKING_SEVERITIES
                ]
                failed_record = dict(records[row_index])
                failed_record["_source_dataset"] = dataset_name
                failed_record["_source_row"] = source_row
                failed_record["_failure_reasons"] = " | ".join(
                    sorted(set(reasons))
                )
                failed_records.append(failed_record)

            output_path = (
                quarantine_dir / f"{dataset_name}_quarantine.csv"
            )
            pd.DataFrame(failed_records).to_csv(output_path, index=False)

            quarantine_summary[dataset_name] = {
                "failed_rows": len(failed_records),
                "file": str(output_path),
            }

        _write_json(
            quarantine_dir / "quarantine_summary.json",
            quarantine_summary,
        )
        return {
            "run_id": dq_result["run_id"],
            "directory": str(quarantine_dir),
            "summary": quarantine_summary,
            "pipeline_blockers": pipeline_blockers,
        }

    @task
    def ai_explain_failures(quarantine_result, dq_result):
        """Use LangGraph AI agent to explain DQ failures (Bonus)."""
        if explain_failures is None:
            logger.warning("[AI] Skipped — langgraph/langchain not installed.")
            return ""
        try:
            message = explain_failures(quarantine_result, dq_result)
            if message:
                logger.info("[AI] Generated alert:\n%s", message)
            else:
                logger.info("[AI] All rows passed. No alert needed.")
            return message or ""
        except Exception as exc:
            logger.warning("[AI] Explainer failed (non-blocking): %s", exc)
            return ""

    @task
    def slack_alert_on_failures(ai_message):
        """Send Slack alert if there were quarantined rows (Bonus)."""
        if not ai_message:
            logger.info("[Slack] No failures to report.")
            return
        try:
            webhook_url = Variable.get(
                "SLACK_WEBHOOK_URL", default_var=None
            )
            send_slack_alert(ai_message, webhook_url=webhook_url)
        except Exception as exc:
            logger.warning("[Slack] Alert failed (non-blocking): %s", exc)

    # ===================================================================
    # PHASE 4: TRANSFORM (Part A.4)
    # ===================================================================

    @task
    def transform_passed_records(dq_result):
        """Transform passing rows into star schema components."""
        if dq_result["pipeline_blockers"]:
            raise AirflowFailException(
                "Pipeline-level DQ blockers detected. "
                "Review quarantine/.../pipeline_blockers.json."
            )
        staging_dir = Path(dq_result["staging_dir"])
        dataset_json = {
            name: (staging_dir / f"{name}.json").read_text(
                encoding="utf-8"
            )
            for name in DATASET_NAMES
        }

        transformed = run_transformations(
            orders_json=dataset_json["orders"],
            payments_json=dataset_json["payments"],
            customers_json=dataset_json["customers"],
            products_json=dataset_json["products"],
            stores_json=dataset_json["stores"],
            dq_result=dq_result,
        )
        transformed["run_id"] = dq_result["run_id"]
        transformed["run_mode"] = dq_result["run_mode"]
        transformed["warning_count"] = dq_result["warning_count"]
        return transformed

    # ===================================================================
    # PHASE 5: LOAD (Part A.5) — fact + 3 dimensions
    # ===================================================================

    @task
    def load_to_warehouse(transformed):
        """Load fact table and dimension tables into DuckDB."""
        run_id = transformed["run_id"]
        mode = transformed["run_mode"]

        fact_df = _payload_to_df(transformed["fact_json"])
        dim_cust = _payload_to_df(transformed["dim_customers_json"])
        dim_prod = _payload_to_df(transformed["dim_products_json"])
        dim_store = _payload_to_df(transformed["dim_stores_json"])

        connection = duckdb.connect(str(WAREHOUSE_PATH))
        inserted_count = 0
        updated_count = 0

        try:
            connection.execute("BEGIN TRANSACTION")

            # --- Fact table ---
            table_exists = connection.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'etl_fact_orders'"
            ).fetchone()[0] > 0

            if not table_exists:
                if mode == "incremental":
                    raise AirflowFailException(
                        "Incremental load but etl_fact_orders doesn't exist."
                    )
                connection.register("init_fact", fact_df)
                connection.execute(
                    "CREATE TABLE etl_fact_orders AS "
                    "SELECT * FROM init_fact LIMIT 0"
                )
                connection.unregister("init_fact")

            existing_count = connection.execute(
                "SELECT COUNT(*) FROM etl_fact_orders"
            ).fetchone()[0]

            if mode == "initial" and existing_count > 0:
                raise AirflowFailException(
                    "Initial load cannot overwrite a populated warehouse. "
                    "Use run_mode='incremental' or 'auto'."
                )

            if not fact_df.empty:
                if "order_id" not in fact_df.columns:
                    raise AirflowFailException(
                        "Fact data has no order_id column."
                    )
                if fact_df["order_id"].duplicated().any():
                    dupes = (
                        fact_df.loc[
                            fact_df["order_id"].duplicated(keep=False),
                            "order_id",
                        ].astype(str).unique().tolist()
                    )
                    raise AirflowFailException(
                        f"Duplicate order IDs in fact: {dupes[:10]}"
                    )

                # Align columns with existing table
                target_columns = [
                    row[0] for row in connection.execute(
                        "DESCRIBE etl_fact_orders"
                    ).fetchall()
                ]
                missing = set(target_columns) - set(fact_df.columns)
                extra = set(fact_df.columns) - set(target_columns)
                if missing or extra:
                    raise AirflowFailException(
                        f"Schema mismatch. Missing={sorted(missing)}; "
                        f"Extra={sorted(extra)}."
                    )
                fact_df = fact_df[target_columns]
                connection.register("incoming_fact", fact_df)

                if mode == "initial":
                    inserted_count = len(fact_df)
                    connection.execute(
                        "INSERT INTO etl_fact_orders "
                        "SELECT * FROM incoming_fact"
                    )
                else:
                    # Upsert: delete-then-insert
                    incoming_count = len(fact_df)
                    updated_count = connection.execute(
                        "SELECT COUNT(*) FROM etl_fact_orders t "
                        "INNER JOIN incoming_fact s "
                        "ON t.order_id = s.order_id"
                    ).fetchone()[0]
                    inserted_count = incoming_count - updated_count
                    connection.execute(
                        "DELETE FROM etl_fact_orders USING incoming_fact "
                        "WHERE etl_fact_orders.order_id = incoming_fact.order_id"
                    )
                    connection.execute(
                        "INSERT INTO etl_fact_orders "
                        "SELECT * FROM incoming_fact"
                    )
                connection.unregister("incoming_fact")

            # --- Dimension tables (CREATE OR REPLACE) ---
            for dim_name, dim_df in [
                ("etl_dim_customer", dim_cust),
                ("etl_dim_product", dim_prod),
                ("etl_dim_store", dim_store),
            ]:
                if not dim_df.empty:
                    connection.register("dim_data", dim_df)
                    connection.execute(
                        f"CREATE OR REPLACE TABLE {dim_name} AS "
                        "SELECT * FROM dim_data"
                    )
                    connection.unregister("dim_data")
                    logger.info(
                        "Loaded %s: %d rows", dim_name, len(dim_df)
                    )

            final_count = connection.execute(
                "SELECT COUNT(*) FROM etl_fact_orders"
            ).fetchone()[0]

            connection.execute("COMMIT")

            return {
                "run_id": run_id,
                "run_mode": mode,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
                "warehouse_count": final_count,
                "dim_customers": len(dim_cust),
                "dim_products": len(dim_prod),
                "dim_stores": len(dim_store),
                "status": "SUCCESS",
            }
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            connection.close()

    # ===================================================================
    # PHASE 6: RECONCILE (Part A.6)
    # ===================================================================

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def reconcile_payments(transformed, load_result):
        """Compare payments with orders, write etl_payment_reconciliation."""
        import time
        run_id = load_result["run_id"]
        payments_df = _payload_to_df(transformed["payments_json"])

        if payments_df.empty:
            logger.info("No payments to reconcile.")
            return {"reconciliation_rows": 0}

        # Small delay to let the load task fully release the DuckDB lock
        time.sleep(2)
        connection = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (
                    run_id VARCHAR,
                    payment_id VARCHAR,
                    order_id VARCHAR,
                    amount_paid DOUBLE,
                    order_amount DOUBLE,
                    payment_status VARCHAR,
                    reconciliation_status VARCHAR,
                    checked_at TIMESTAMP
                )
            """)
            connection.execute(
                "DELETE FROM etl_payment_reconciliation "
                "WHERE run_id = ?", [run_id]
            )

            # Determine order amount column name
            fact_cols = [
                r[0] for r in connection.execute(
                    "DESCRIBE etl_fact_orders"
                ).fetchall()
            ]
            amt_col = (
                "calculated_order_amount"
                if "calculated_order_amount" in fact_cols
                else "order_amount"
            )

            connection.register("incoming_payments", payments_df)
            recon_sql = f"""
                INSERT INTO etl_payment_reconciliation
                SELECT
                    ? AS run_id,
                    CAST(p.payment_id AS VARCHAR),
                    CAST(p.order_id AS VARCHAR),
                    TRY_CAST(p.amount_paid AS DOUBLE),
                    TRY_CAST(o.{amt_col} AS DOUBLE),
                    CAST(p.payment_status AS VARCHAR),
                    CASE
                        WHEN o.order_id IS NULL
                            THEN 'ORDER_NOT_FOUND'
                        WHEN UPPER(CAST(p.payment_status AS VARCHAR)) = 'SUCCESS'
                             AND ABS(
                                TRY_CAST(p.amount_paid AS DOUBLE)
                                - TRY_CAST(o.{amt_col} AS DOUBLE)
                             ) > 0.01
                            THEN 'AMOUNT_MISMATCH'
                        WHEN UPPER(CAST(p.payment_status AS VARCHAR)) = 'SUCCESS'
                            THEN 'MATCHED'
                        ELSE 'NOT_SUCCESS'
                    END,
                    CURRENT_TIMESTAMP
                FROM incoming_payments p
                LEFT JOIN etl_fact_orders o
                    ON CAST(p.order_id AS VARCHAR) = CAST(o.order_id AS VARCHAR)
            """
            connection.execute(recon_sql, [run_id])
            connection.unregister("incoming_payments")

            recon_count = connection.execute(
                "SELECT COUNT(*) FROM etl_payment_reconciliation "
                "WHERE run_id = ?", [run_id]
            ).fetchone()[0]
        finally:
            connection.close()

        logger.info("Reconciliation: %d rows written", recon_count)
        return {"reconciliation_rows": recon_count}

    # ===================================================================
    # PHASE 7: REPORT (Part A.7)
    # ===================================================================

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def build_reports(load_result):
        """Build etl_daily_zone_revenue and etl_category_margin."""
        import time
        time.sleep(2)
        connection = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            # --- etl_daily_zone_revenue ---
            connection.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT
                    f.order_date,
                    s.zone,
                    s.store_name,
                    COUNT(*) AS order_count,
                    SUM(f.net_revenue) AS total_net_revenue
                FROM etl_fact_orders f
                LEFT JOIN etl_dim_store s
                    ON CAST(f.store_id AS VARCHAR) = CAST(s.store_id AS VARCHAR)
                WHERE f.order_status = 'DELIVERED'
                GROUP BY f.order_date, s.zone, s.store_name
                ORDER BY f.order_date, s.zone, s.store_name
            """)
            zone_revenue = connection.execute(
                "SELECT * FROM etl_daily_zone_revenue LIMIT 10"
            ).fetchdf()
            logger.info(
                "etl_daily_zone_revenue (sample):\n%s",
                zone_revenue.to_string(),
            )

            # --- etl_category_margin ---
            connection.execute("""
                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT
                    p.category,
                    COUNT(*) AS order_count,
                    SUM(f.net_revenue) AS total_revenue,
                    SUM(f.gross_margin) AS total_gross_margin
                FROM etl_fact_orders f
                LEFT JOIN etl_dim_product p
                    ON CAST(f.product_id AS VARCHAR) = CAST(p.product_id AS VARCHAR)
                WHERE f.order_status = 'DELIVERED'
                GROUP BY p.category
                ORDER BY total_revenue DESC
            """)
            cat_margin = connection.execute(
                "SELECT * FROM etl_category_margin"
            ).fetchdf()
            logger.info(
                "etl_category_margin:\n%s",
                cat_margin.to_string(),
            )

            return {
                "zone_revenue_rows": len(zone_revenue),
                "category_margin_rows": len(cat_margin),
                "status": "SUCCESS",
            }
        finally:
            connection.close()

    # ===================================================================
    # PHASE 8: AUDIT (Part F)
    # ===================================================================

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def write_run_audit():
        """Write one row per run to pipeline_runs."""
        context = get_current_context()
        run_id = context["run_id"]
        ti = context["ti"]

        dq_result = ti.xcom_pull(task_ids="validate_data_quality") or {}
        load_result = ti.xcom_pull(task_ids="load_to_warehouse") or {}
        quarantine_result = ti.xcom_pull(
            task_ids="quarantine_failed_records"
        ) or {}
        recon_result = ti.xcom_pull(
            task_ids="reconcile_payments"
        ) or {}

        dag_run = context["dag_run"]
        try:
            task_instances = dag_run.get_task_instances()
            failed_tasks = [
                inst.task_id
                for inst in task_instances
                if inst.state in {"failed", "upstream_failed"}
                and inst.task_id != "write_run_audit"
            ]
        except Exception as exc:
            logger.warning("Could not fetch task states: %s", exc)
            failed_tasks = []

        if failed_tasks:
            status = "FAILED"
        elif dq_result.get("pipeline_blockers"):
            status = "FAILED"
        elif dq_result.get("blocking_count", 0):
            status = "COMPLETED_WITH_QUARANTINE"
        else:
            status = "SUCCESS"

        # Count quarantined rows
        q_summary = quarantine_result.get("summary", {})
        rows_quarantined = sum(
            (v.get("failed_rows", 0) if isinstance(v, dict) else 0)
            for v in q_summary.values()
        )

        connection = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            connection.execute("""
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
            connection.execute(
                "DELETE FROM pipeline_runs WHERE run_id = ?",
                [run_id],
            )
            connection.execute(
                "INSERT INTO pipeline_runs VALUES "
                "(?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, "
                "CURRENT_TIMESTAMP)",
                [
                    run_id,
                    context["dag"].dag_id,
                    dq_result.get("run_mode", "unknown"),
                    context["dag_run"].start_date,
                    dq_result.get("total_rows", 0),
                    load_result.get("warehouse_count", 0),
                    rows_quarantined,
                    status,
                    ", ".join(failed_tasks),
                    recon_result.get("reconciliation_rows", 0),
                ],
            )
        finally:
            connection.close()

        logger.info(
            "Audit written: run_id=%s, status=%s, "
            "loaded=%s, quarantined=%s",
            run_id, status,
            load_result.get("warehouse_count", 0),
            rows_quarantined,
        )

    # ===================================================================
    # WIRE UP DEPENDENCIES
    # ===================================================================
    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()

    dq_result = validate_data_quality(
        orders, payments, customers, products, stores,
    )

    # Phase 2b: DQ results table
    dq_log = write_dq_results(dq_result)

    # Phase 3: Quarantine path
    quarantine = quarantine_failed_records(dq_result)
    ai_message = ai_explain_failures(quarantine, dq_result)
    slack_alert_on_failures(ai_message)

    # Phase 4-7: Transform -> Load -> Reconcile -> Report
    transformed = transform_passed_records(dq_result)
    load_result = load_to_warehouse(transformed)
    recon = reconcile_payments(transformed, load_result)
    reports = build_reports(load_result)

    # Phase 8: Audit (ALL_DONE)
    [load_result, quarantine, dq_log, recon, reports] >> write_run_audit()
