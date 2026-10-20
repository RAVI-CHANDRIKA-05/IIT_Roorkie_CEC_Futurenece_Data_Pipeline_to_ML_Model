-- ==========================================================================
-- 04_incremental_orders.sql
-- NammaMart ELT Pipeline — Incremental Order Processing
--
-- Purpose : MERGE incremental order data into staging.
--           - New order_ids → INSERT
--           - Existing order_ids → UPDATE (corrections from increment file)
--           Then re-build mart tables to reflect changes.
--
-- Called by the ELT DAG when run_mode = 'incremental'.
-- The DAG loads increment CSV into raw_orders BEFORE executing this script.
-- ==========================================================================

-- -------------------------------------------------------------------------
-- Step 1: Build a temporary staging view of ONLY the new increment rows
--         (identified by the latest _run_id in raw_orders)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _increment_staged AS
WITH latest_run AS (
    SELECT MAX(_run_id) AS run_id FROM raw_orders
),
casted AS (
    SELECT
        r.order_id,
        TRY_CAST(r.order_ts AS TIMESTAMP)                   AS order_ts,
        TRY_CAST(r.order_ts AS TIMESTAMP)::DATE             AS order_date,
        r.customer_id,
        r.store_id,
        r.product_id,
        TRY_CAST(r.quantity AS INTEGER)                      AS quantity,
        TRY_CAST(r.unit_price AS DOUBLE)                     AS unit_price,
        TRY_CAST(r.discount_pct AS DOUBLE)                   AS discount_pct,
        TRY_CAST(r.order_amount AS DOUBLE)                   AS order_amount,
        CASE UPPER(TRIM(r.payment_mode))
            WHEN 'CASHH'       THEN 'CASH'
            WHEN 'COD'         THEN 'CASH'
            WHEN 'CARD'        THEN 'CARD'
            WHEN 'CREDIT CARD' THEN 'CARD'
            WHEN 'DEBIT CARD'  THEN 'CARD'
            WHEN 'UPI'         THEN 'UPI'
            WHEN 'WALLET'      THEN 'WALLET'
            ELSE UPPER(TRIM(r.payment_mode))
        END                                                  AS payment_mode,
        CASE
            WHEN UPPER(TRIM(r.order_status)) IN ('DELIVERED','DELIVER') THEN 'DELIVERED'
            WHEN UPPER(TRIM(r.order_status)) IN ('CANCELLED','CANCELED') THEN 'CANCELLED'
            WHEN UPPER(TRIM(r.order_status)) LIKE 'PENDING%'  THEN 'PENDING'
            WHEN UPPER(TRIM(r.order_status)) IN ('SHIPPED','SHIP')    THEN 'SHIPPED'
            WHEN UPPER(TRIM(r.order_status)) IN ('RETURNED','RETURN') THEN 'RETURNED'
            ELSE UPPER(TRIM(r.order_status))
        END                                                  AS order_status,
        TRIM(r.channel)                                      AS channel,
        TRY_CAST(r.delivery_minutes AS DOUBLE)               AS delivery_minutes,
        r._loaded_at,
        r._source_file,
        r._run_id
    FROM raw_orders r
    INNER JOIN latest_run lr ON r._run_id = lr.run_id
),
with_derived AS (
    SELECT
        *,
        ROUND(quantity * unit_price * (1.0 - COALESCE(discount_pct, 0) / 100.0), 2)
            AS calculated_order_amount,
        CASE WHEN order_status = 'DELIVERED'
             THEN ROUND(quantity * unit_price * (1.0 - COALESCE(discount_pct, 0) / 100.0), 2)
             ELSE 0.0
        END AS net_revenue,
        CASE WHEN delivery_minutes IS NOT NULL AND delivery_minutes <= 15
             THEN TRUE ELSE FALSE
        END AS within_15_min
    FROM casted
)
SELECT
    *,
    CASE
        WHEN order_id IS NULL THEN 'FAIL'
        WHEN order_ts IS NULL THEN 'FAIL'
        WHEN quantity IS NULL OR quantity <= 0 THEN 'FAIL'
        WHEN unit_price IS NULL OR unit_price <= 0 THEN 'FAIL'
        WHEN discount_pct IS NOT NULL AND (discount_pct < 0 OR discount_pct > 100) THEN 'FAIL'
        WHEN order_amount IS NOT NULL AND order_amount < 0 THEN 'FAIL'
        WHEN order_date IS NOT NULL AND order_date > CURRENT_DATE THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN order_id IS NULL THEN 'Missing order_id'
        WHEN order_ts IS NULL THEN 'Unparseable or missing order_ts'
        WHEN quantity IS NULL OR quantity <= 0 THEN 'Invalid quantity'
        WHEN unit_price IS NULL OR unit_price <= 0 THEN 'Invalid unit_price'
        WHEN discount_pct IS NOT NULL AND (discount_pct < 0 OR discount_pct > 100) THEN 'discount_pct out of range'
        WHEN order_amount IS NOT NULL AND order_amount < 0 THEN 'Negative order_amount'
        WHEN order_date IS NOT NULL AND order_date > CURRENT_DATE THEN 'Future order_date'
        ELSE NULL
    END AS failure_reason
FROM with_derived;


-- -------------------------------------------------------------------------
-- Step 2: MERGE incremental rows into stg_orders
--         - Existing order_id → UPDATE (correction)
--         - New order_id → INSERT
-- -------------------------------------------------------------------------
DELETE FROM stg_orders
WHERE order_id IN (SELECT order_id FROM _increment_staged);

INSERT INTO stg_orders
SELECT
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    calculated_order_amount, net_revenue, within_15_min,
    dq_status, failure_reason,
    _loaded_at, _source_file, _run_id
FROM _increment_staged;


-- -------------------------------------------------------------------------
-- Step 3: Drop temporary table
-- -------------------------------------------------------------------------
DROP TABLE IF EXISTS _increment_staged;


-- -------------------------------------------------------------------------
-- Step 4: Re-build mart tables to reflect updated staging data
--         (The mart SQL uses CREATE OR REPLACE, so this is safe)
-- -------------------------------------------------------------------------
-- Note: The DAG will re-execute 03_create_marts.sql after this script.
--       No mart rebuild needed here — avoids duplicating mart logic.
