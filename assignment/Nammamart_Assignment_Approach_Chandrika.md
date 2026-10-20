# NammaMart Quick Commerce — ETL & ELT Data Pipeline

**Assignment:** IIT Roorkee — Forward Deployed AI Engineering (FDAIE Module 2)
**Prepared by:** Ravi Chandrika
**Date:** 2026-10-20
**Status:** Both pipelines complete and tested locally (all green).

---

## 1. Objective

Develop ETL and ELT data pipelines for NammaMart Quick Commerce using Apache Airflow (Astro CLI) and DuckDB. Both pipelines process September 2026 source data and an October 1 incremental file, apply data quality controls across 6 dimensions, and produce analytical tables answering 6 business questions.

---

## 2. Project Structure

### ETL Pipeline (`nammamart_etl_updated/`)

| File | Purpose | Status |
|---|---|---|
| `dags/nammamart_etl_chandrika.py` | Main ETL DAG — 18 tasks, 8 phases | ✅ Tested |
| `include/transform_nammamart.py` | Star schema transforms (pandas) | ✅ Tested |
| `include/ai_explainer_nammamart.py` | AI failure explanation (optional) | ✅ Tested (graceful fallback) |
| `include/extract_nammamart.py` | CSV extraction for 5+1 datasets | ✅ Unchanged |
| `include/quality_checks_nammamart.py` | Technical DQ checks | ✅ Unchanged |
| `include/business_rules_check_nammamart.py` | Business rule checks | ✅ Unchanged |
| `include/slack_notifier_nammamart.py` | Slack alerts | ✅ Unchanged |

### ELT Pipeline (`nammamart_elt_updated/`)

| File | Purpose | Status |
|---|---|---|
| `dags/nammamart_elt_chandrika.py` | Main ELT DAG — 6 tasks, 5 phases | ✅ Tested |
| `include/sql/01_create_raw.sql` | Raw table creation | ✅ Tested |
| `include/sql/02_create_staging.sql` | Staging transforms + DQ flags | ✅ Tested |
| `include/sql/03_create_marts.sql` | Mart tables (6 business questions) | ✅ Tested |
| `include/sql/04_incremental_orders.sql` | Incremental merge | ✅ Tested |

### Source Data (`include/data/nammamart/`)

| File | Rows | Columns | Type |
|---|---:|---:|---|
| orders.csv | 2,412 | 13 | Fact |
| payments.csv | 2,228 | 7 | Fact |
| customers.csv | 405 | 10 | Dimension |
| products.csv | 152 | 10 | Dimension |
| stores.csv | 15 | 8 | Dimension |
| orders_2026-10-01_increment.csv | 160 | 13 | Incremental |

---

## 3. Data Profiling (Part 0)

Profiling performed with `include/profile_nammamart.py`. Key findings:

- **orders.csv:** 1 null order_ts, 18 null customer_id, 164 null delivery_minutes, 12 duplicate order_ids, negative quantities, discount up to 250%, future dates (2027), 5 unparseable timestamps, invalid statuses ('Pending??'), invalid payment modes ('Cashh', 'cod')
- **payments.csv:** 301 null gateway_ref, 4 duplicate payment_ids, future dates
- **customers.csv:** 6 null city, 5 duplicate customer_ids, future signup_dates, mixed loyalty_tier casing
- **products.csv:** 3 null category, 2 duplicate product_ids, negative MRP (-203)
- **stores.csv:** 1 null zone, 0-capacity store
- **increment:** CLEAN (160 rows, no issues)

Referential integrity: 9 unmatched product refs, 5 unmatched store refs, 8 payment refs to missing orders, 15 order amount formula mismatches, 14 payment amount mismatches.

---

## 4. ETL Pipeline Design (Part A)

### Architecture: Extract → Validate → Transform → Load

Bad rows are removed **before** loading into the warehouse.

### DAG Flow (18 Tasks, 8 Phases)

```
Phase 1 EXTRACT → Phase 2 VALIDATE → Phase 3 QUARANTINE+AI+ALERT → Phase 4 TRANSFORM
    → Phase 5 LOAD → Phase 6 RECONCILE → Phase 7 REPORT → Phase 8 AUDIT
```

- **Phase 1:** 5 parallel extract tasks (one per CSV)
- **Phase 2:** `validate_data_quality` (Python checks across 6 dimensions) + `write_dq_results`
- **Phase 3:** `quarantine_failed_records` (CSV with `_failure_reasons`) → `ai_explain_failures` (optional LangGraph) → `slack_alert_on_failures`
- **Phase 4:** `transform_passed_records` (star schema: fact + 3 dimensions, PII masked, values standardised)
- **Phase 5:** `load_to_warehouse` (DuckDB: `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`)
- **Phase 6:** `reconcile_payments` (order vs payment matching)
- **Phase 7:** `build_reports` (`etl_daily_zone_revenue`, `etl_category_margin`)
- **Phase 8:** `write_run_audit` (trigger_rule=ALL_DONE, writes to `pipeline_runs`)

### Key Decisions

- **Idempotent:** Fact/dimension tables use CREATE OR REPLACE; reconciliation uses DELETE WHERE run_id then INSERT
- **PII masking (Python):** Email → `p***@domain.com`, Phone → `******4321`
- **Star schema:** Deduplicated dimensions with business-key grain
- **DuckDB lock handling:** `time.sleep(5)` + `retries=3, retry_delay=15s`
- **AI explainer:** `try/except ImportError` — works without LangGraph installed

---

## 5. ELT Pipeline Design (Part B)

### Architecture: Load Raw → Transform in SQL → Aggregate to Marts

All rows (including bad ones) are loaded raw. Filtering happens in SQL.

### Three-Layer Design

| Layer | Tables | Rule |
|---|---|---|
| Raw | raw_orders, raw_payments, raw_customers, raw_products, raw_stores | All VARCHAR, no cleaning, `_loaded_at`, `_source_file`, `_run_id` |
| Staging | stg_orders, stg_payments, stg_customers, stg_products, stg_stores | TRY_CAST, dedupe, PII mask, standardise, `dq_status` + `failure_reason` |
| Mart | mart_daily_zone_revenue, mart_category_margin, mart_delivery_sla, mart_payment_reconciliation, mart_top_customers, mart_elt_dq_summary | Aggregates from PASS rows only |

### DAG Flow (6 Tasks, 5 Phases)

```
load_raw_data → run_staging_sql → run_marts_sql → run_incremental_merge
    → rebuild_marts_after_merge → write_elt_audit
```

### Key Decisions

- **Raw tables dynamically created** from CSV headers (DROP + CREATE TABLE AS SELECT) — avoids column mismatch
- **PII masking in SQL:** `SUBSTR(email,1,1)||'***'||SUBSTR(email,POSITION('@' IN email))`
- **DQ in SQL:** Each staging table has `dq_status` (PASS/FAIL/WARN) and `failure_reason`
- **Incremental merge:** DELETE + INSERT on `order_id` in `stg_orders`
- **Marts rebuilt** after merge to reflect updated data

---

## 6. Data Quality (Part D)

| Dimension | ETL (Python) | ELT (SQL) |
|---|---|---|
| Completeness | Null checks on required fields | CASE WHEN IS NULL THEN 'FAIL' |
| Uniqueness | Duplicate key detection | ROW_NUMBER() deduplication |
| Validity | Allowed-value lists, format checks | CASE checks on status, payment_mode |
| Accuracy | Formula verification, range checks | calculated_order_amount, quantity > 0 |
| Consistency | Referential integrity, casing | loyalty_tier standardised, payment_mode mapped |
| Timeliness | Future-date detection | order_ts/signup_date not > CURRENT_DATE |

Severity: **Critical** = quarantined/FAIL (excluded from marts). **Warning** = loaded but logged.

`dq_results` table populated every run with run_id, check_name, dimension, severity, rows_checked, rows_failed.

---

## 7. Scheduling (Part C)

- **Cron:** `0 9 * * 1-6` (9:00 AM Monday–Saturday)
- **Timezone:** `pendulum.datetime(2026,9,1,tz="Asia/Kolkata")` for IST
- **catchup=False:** Skip missed runs; manual re-trigger if needed
- **default_args:** retries=3, retry_delay=15s, execution_timeout=30min

---

## 8. Bugs Found & Fixed During Testing

### ETL Bugs

| Bug | Root Cause | Fix |
|---|---|---|
| Double-serialized JSON in XCom | `_write_json()` serialized already-serialized strings | Detect if value is already a string; write directly |
| DuckDB lock — reconcile_payments fails | Single-writer DuckDB; previous task's lock not released | `time.sleep(5)` + retries |
| `write_run_audit` AttributeError | Airflow 2.x API difference for `get_task_instances()` | Use `context["dag_run"].get_task_instances()` with try/except |
| AI packages slow Docker build | langgraph/langchain pull 200+ dependencies | Made import conditional; task returns empty when disabled |
| Secrets backend 403 | Astro CLI tries managed secrets backend | Added `AIRFLOW__SECRETS__BACKEND=` to `.env` |

### ELT Bugs

| Bug | Root Cause | Fix |
|---|---|---|
| raw_customers column mismatch | Raw DDL had 7 columns; CSV has 10 | Changed to DROP + CREATE TABLE AS SELECT (dynamic) |
| INITCAP does not exist | DuckDB lacks INITCAP function | `CONCAT(UPPER(SUBSTR(...,1,1)), LOWER(SUBSTR(...,2)))` |
| customer_name not found | CSV has `full_name` not `customer_name` | Aliased `full_name AS customer_name` in staging |
| city not found in stores | stores.csv has no `city` column | Rewrote stg_stores with correct columns |
| GROUP BY error in marts | `area` in SELECT but not GROUP BY | Added to GROUP BY |
| DuckDB lock | Same single-writer issue | `time.sleep(5)` + retries |

---

## 9. Actual CSV Column Names

| CSV | Columns |
|---|---|
| orders | order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes |
| payments | payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref |
| customers | customer_id, **full_name**, email, phone, city, **area**, **pincode**, signup_date, loyalty_tier, **is_active** |
| products | product_id, product_name, category, **sub_category**, **brand**, mrp, cost_price, **unit**, **is_perishable**, **launch_date** |
| stores | store_id, store_name, zone, **area**, **pincode**, **open_date**, **manager_name**, capacity_orders_per_day |

**Bold** = columns I initially missed (discovered during ELT testing).

---

## 10. Warehouse Tables Summary

### ETL Output (warehouse_chandrika.duckdb)

| Table | Type | Rows (single clean run) |
|---|---|---|
| etl_fact_orders | Fact | ~2,100–2,200 |
| etl_dim_customer | Dimension | ~400 |
| etl_dim_product | Dimension | ~150 |
| etl_dim_store | Dimension | 15 |
| etl_payment_reconciliation | Audit (accumulates) | ~2,228 |
| etl_daily_zone_revenue | Report | ~470 |
| etl_category_margin | Report | 6 |
| dq_results | Audit (accumulates) | ~47 |
| pipeline_runs | Audit (accumulates) | 1 per run |

### ELT Output (warehouse_chandrika.duckdb)

| Table | Type | Rows (single clean run) |
|---|---|---|
| raw_orders | Raw | 2,412 |
| raw_payments | Raw | 2,228 |
| raw_customers | Raw | 405 |
| raw_products | Raw | 152 |
| raw_stores | Raw | 15 |
| stg_orders | Staging | ~2,400 |
| stg_payments | Staging | ~2,224 |
| stg_customers | Staging | ~400 |
| stg_products | Staging | ~150 |
| stg_stores | Staging | 15 |
| mart_daily_zone_revenue | Mart | ~470 |
| mart_category_margin | Mart | 6 |
| mart_delivery_sla | Mart | ~15 |
| mart_payment_reconciliation | Mart | ~2,200 |
| mart_top_customers | Mart | 10 |
| mart_elt_dq_summary | Mart | ~10 |
| pipeline_runs | Audit (shared with ETL) | 1 per run |

**Note on accumulating tables:** `etl_payment_reconciliation`, `dq_results`, `pipeline_runs` use `DELETE WHERE run_id` then INSERT, so they accumulate across runs. For clean single-run counts: delete `warehouse_chandrika.duckdb` and run one `initial` trigger.

---

## 11. Tools and Environment

| Tool | Role |
|---|---|
| Apache Airflow (TaskFlow API) | Orchestration and scheduling |
| Astro CLI (Astronomer) | Local Airflow in Docker |
| Docker Desktop | Runs Airflow containers |
| pandas | ETL transformations |
| DuckDB | Local warehouse + ELT SQL transforms |
| VS Code | Development |
| Git + GitHub | Version control |

### Environment Fixes Applied

- `requirements.txt`: Commented out `langgraph`, `langchain`, `langchain-openai` (speeds Docker build)
- `.env`: Added `AIRFLOW__SECRETS__BACKEND=` (prevents 403 on secrets backend)
- DuckDB lock: `time.sleep(5)` before each DuckDB connection + `retries=3, retry_delay=15s`

---

## 12. Remaining Work

| # | Task | Status |
|---|---|---|
| 1 | Test ETL incremental run | TODO |
| 2 | Test ELT incremental run | TODO |
| 3 | Run 10 observability experiments (Part F) | TODO |
| 4 | Screenshots for submission | TODO |
| 5 | Final SUBMISSION.md review | TODO |
