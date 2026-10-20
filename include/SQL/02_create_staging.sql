-- ==========================================================================
-- 02_create_staging.sql
-- NammaMart ELT Pipeline — Staging Layer
--
-- Purpose : SQL-only transformations on raw data:
--           1. Cast VARCHAR → proper types (DOUBLE, TIMESTAMP, DATE, etc.)
--           2. Standardise values (UPPER statuses, alias mapping)
--           3. Deduplicate (ROW_NUMBER over primary key)
--           4. Mask PII (email, phone)
--           5. Apply DQ rules, flag rows with dq_status + failure_reason
-- Idempotent: CREATE OR REPLACE TABLE.
-- ==========================================================================

-- -------------------------------------------------------------------------
-- STG_ORDERS
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_orders AS
WITH casted AS (
    SELECT
        order_id,
        TRY_CAST(order_ts AS TIMESTAMP)                     AS order_ts,
        TRY_CAST(order_ts AS TIMESTAMP)::DATE               AS order_date,
        customer_id,
        store_id,
        product_id,
        TRY_CAST(quantity AS INTEGER)                        AS quantity,
        TRY_CAST(unit_price AS DOUBLE)                       AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE)                     AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE)                     AS order_amount,
        -- Standardise payment_mode aliases
        CASE UPPER(TRIM(payment_mode))
            WHEN 'CASHH'       THEN 'CASH'
            WHEN 'COD'         THEN 'CASH'
            WHEN 'CARD'        THEN 'CARD'
            WHEN 'CREDIT CARD' THEN 'CARD'
            WHEN 'DEBIT CARD'  THEN 'CARD'
            WHEN 'UPI'         THEN 'UPI'
            WHEN 'WALLET'      THEN 'WALLET'
            ELSE UPPER(TRIM(payment_mode))
        END                                                  AS payment_mode,
        -- Standardise order_status
        CASE
            WHEN UPPER(TRIM(order_status)) IN ('DELIVERED','DELIVER') THEN 'DELIVERED'
            WHEN UPPER(TRIM(order_status)) IN ('CANCELLED','CANCELED') THEN 'CANCELLED'
            WHEN UPPER(TRIM(order_status)) LIKE 'PENDING%'  THEN 'PENDING'
            WHEN UPPER(TRIM(order_status)) IN ('SHIPPED','SHIP')    THEN 'SHIPPED'
            WHEN UPPER(TRIM(order_status)) IN ('RETURNED','RETURN') THEN 'RETURNED'
            ELSE UPPER(TRIM(order_status))
        END                                                  AS order_status,
        TRIM(channel)                                        AS channel,
        TRY_CAST(delivery_minutes AS DOUBLE)                 AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _loaded_at DESC
        )                                                    AS _rn
    FROM raw_orders
),
deduped AS (
    SELECT * FROM casted WHERE _rn = 1
),
with_derived AS (
    SELECT
        *,
        -- Calculated order amount
        ROUND(quantity * unit_price * (1.0 - COALESCE(discount_pct, 0) / 100.0), 2)
            AS calculated_order_amount,
        -- Net revenue: DELIVERED only
        CASE WHEN order_status = 'DELIVERED'
             THEN ROUND(quantity * unit_price * (1.0 - COALESCE(discount_pct, 0) / 100.0), 2)
             ELSE 0.0
        END AS net_revenue,
        -- Within 15 min SLA
        CASE WHEN delivery_minutes IS NOT NULL AND delivery_minutes <= 15
             THEN TRUE ELSE FALSE
        END AS within_15_min
    FROM deduped
),
with_dq AS (
    SELECT
        *,
        -- DQ flag: collect all failure reasons
        CASE
            WHEN order_id IS NULL THEN 'FAIL'
            WHEN order_ts IS NULL THEN 'FAIL'
            WHEN quantity IS NULL OR quantity <= 0 THEN 'FAIL'
            WHEN unit_price IS NULL OR unit_price <= 0 THEN 'FAIL'
            WHEN discount_pct IS NOT NULL AND (discount_pct < 0 OR discount_pct > 100) THEN 'FAIL'
            WHEN order_amount IS NOT NULL AND order_amount < 0 THEN 'FAIL'
            WHEN order_date IS NOT NULL AND order_date > CURRENT_DATE THEN 'FAIL'
            WHEN delivery_minutes IS NOT NULL AND delivery_minutes > 1440 THEN 'FAIL'
            WHEN order_status IN ('PENDING??') THEN 'FAIL'
            ELSE 'PASS'
        END AS dq_status,
        CASE
            WHEN order_id IS NULL THEN 'Missing order_id'
            WHEN order_ts IS NULL THEN 'Unparseable or missing order_ts'
            WHEN quantity IS NULL OR quantity <= 0 THEN 'Invalid quantity: ' || CAST(quantity AS VARCHAR)
            WHEN unit_price IS NULL OR unit_price <= 0 THEN 'Invalid unit_price'
            WHEN discount_pct IS NOT NULL AND (discount_pct < 0 OR discount_pct > 100) THEN 'discount_pct out of range: ' || CAST(discount_pct AS VARCHAR)
            WHEN order_amount IS NOT NULL AND order_amount < 0 THEN 'Negative order_amount'
            WHEN order_date IS NOT NULL AND order_date > CURRENT_DATE THEN 'Future order_date'
            WHEN delivery_minutes IS NOT NULL AND delivery_minutes > 1440 THEN 'delivery_minutes > 24hrs'
            WHEN order_status IN ('PENDING??') THEN 'Invalid order_status'
            ELSE NULL
        END AS failure_reason
    FROM with_derived
)
SELECT
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount,
    payment_mode, order_status, channel, delivery_minutes,
    calculated_order_amount, net_revenue, within_15_min,
    dq_status, failure_reason,
    _loaded_at, _source_file, _run_id
FROM with_dq;


-- -------------------------------------------------------------------------
-- STG_PAYMENTS
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_payments AS
WITH casted AS (
    SELECT
        payment_id,
        order_id,
        TRY_CAST(payment_ts AS TIMESTAMP)                   AS payment_ts,
        CASE UPPER(TRIM(payment_mode))
            WHEN 'CASHH'       THEN 'CASH'
            WHEN 'COD'         THEN 'CASH'
            WHEN 'CARD'        THEN 'CARD'
            WHEN 'CREDIT CARD' THEN 'CARD'
            WHEN 'DEBIT CARD'  THEN 'CARD'
            WHEN 'UPI'         THEN 'UPI'
            WHEN 'WALLET'      THEN 'WALLET'
            ELSE UPPER(TRIM(payment_mode))
        END                                                  AS payment_mode,
        TRY_CAST(amount_paid AS DOUBLE)                      AS amount_paid,
        UPPER(TRIM(payment_status))                          AS payment_status,
        gateway_ref,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY _loaded_at DESC
        )                                                    AS _rn
    FROM raw_payments
),
deduped AS (
    SELECT * FROM casted WHERE _rn = 1
)
SELECT
    payment_id, order_id, payment_ts, payment_mode,
    amount_paid, payment_status, gateway_ref,
    -- DQ flags
    CASE
        WHEN payment_id IS NULL THEN 'FAIL'
        WHEN order_id IS NULL THEN 'FAIL'
        WHEN amount_paid IS NULL OR amount_paid <= 0 THEN 'FAIL'
        WHEN payment_ts IS NOT NULL AND payment_ts::DATE > CURRENT_DATE THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN payment_id IS NULL THEN 'Missing payment_id'
        WHEN order_id IS NULL THEN 'Missing order_id'
        WHEN amount_paid IS NULL OR amount_paid <= 0 THEN 'Invalid amount_paid'
        WHEN payment_ts IS NOT NULL AND payment_ts::DATE > CURRENT_DATE THEN 'Future payment_ts'
        ELSE NULL
    END AS failure_reason,
    _loaded_at, _source_file, _run_id
FROM deduped;


-- -------------------------------------------------------------------------
-- STG_CUSTOMERS (PII masked)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_customers AS
WITH casted AS (
    SELECT
        customer_id,
        full_name                                            AS customer_name,
        -- Mask email: first char + *** + @domain
        CASE
            WHEN email IS NULL THEN NULL
            WHEN POSITION('@' IN email) = 0 THEN '***'
            ELSE SUBSTR(email, 1, 1) || '***' || SUBSTR(email, POSITION('@' IN email))
        END                                                  AS masked_email,
        -- Mask phone: replace all but last 4 digits with *
        CASE
            WHEN phone IS NULL THEN NULL
            WHEN LENGTH(phone) <= 4 THEN REPEAT('*', LENGTH(phone))
            ELSE REPEAT('*', LENGTH(phone) - 4) || SUBSTR(phone, LENGTH(phone) - 3)
        END                                                  AS masked_phone,
        TRIM(city)                                           AS city,
        TRIM(area)                                           AS area,
        pincode,
        TRY_CAST(signup_date AS DATE)                        AS signup_date,
        -- Standardise loyalty_tier casing: Title Case
        CONCAT(
            UPPER(SUBSTR(TRIM(loyalty_tier), 1 , 1)),
            LOWER(SUBSTR(TRIM(loyalty_tier), 2))
            )                                                AS loyalty_tier,
        is_active,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _loaded_at DESC
        )                                                    AS _rn
    FROM raw_customers
),
deduped AS (
    SELECT * FROM casted WHERE _rn = 1
)
SELECT
    customer_id, customer_name, masked_email, masked_phone,
    city, area, pincode, signup_date, loyalty_tier, is_active,
    -- DQ flags
    CASE
        WHEN customer_id IS NULL THEN 'FAIL'
        WHEN signup_date IS NOT NULL AND signup_date > CURRENT_DATE THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN customer_id IS NULL THEN 'Missing customer_id'
        WHEN signup_date IS NOT NULL AND signup_date > CURRENT_DATE THEN 'Future signup_date'
        ELSE NULL
    END AS failure_reason,
    _loaded_at, _source_file, _run_id
FROM deduped;


-- -------------------------------------------------------------------------
-- STG_PRODUCTS
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_products AS
WITH casted AS (
    SELECT
        product_id,
        product_name,
        TRIM(category)                                       AS category,
        TRIM(sub_category)                                   AS sub_category,
        TRIM(brand)                                          AS brand,
        TRY_CAST(mrp AS DOUBLE)                              AS mrp,
        TRY_CAST(cost_price AS DOUBLE)                       AS cost_price,
        TRY_CAST(unit AS VARCHAR)                            AS unit,
        is_perishable,
        -- Standardise is_perishable to TRUE/FALSE
        TRY_CAST(launch_date AS DATE)                        AS launch_date,
        CASE WHEN LOWER(product_name) LIKE '%(old)%' THEN 1 ELSE 0 END AS _is_old,
        -- Prefer non-(old) records
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE WHEN LOWER(product_name) LIKE '%(old)%' THEN 1 ELSE 0 END ASC,
                _loaded_at DESC
        )                                                    AS _rn
    FROM raw_products
),
deduped AS (
    SELECT * FROM casted WHERE _rn = 1
)
SELECT
    product_id, product_name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date,
    -- DQ flags
    CASE
        WHEN product_id IS NULL THEN 'FAIL'
        WHEN mrp IS NOT NULL AND mrp < 0 THEN 'FAIL'
        WHEN cost_price IS NOT NULL AND cost_price < 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN product_id IS NULL THEN 'Missing product_id'
        WHEN mrp IS NOT NULL AND mrp < 0 THEN 'Negative MRP: ' || CAST(mrp AS VARCHAR)
        WHEN cost_price IS NOT NULL AND cost_price < 0 THEN 'Negative cost_price'
        ELSE NULL
    END AS failure_reason,
    _loaded_at, _source_file, _run_id
FROM deduped;


-- -------------------------------------------------------------------------
-- STG_STORES
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_stores AS
WITH casted AS (
    SELECT
        store_id,
        TRIM(store_name)                                     AS store_name,
        TRIM(area)                                           AS area,
        TRIM(zone)                                           AS zone,
        pincode,
        TRIM(manager_name)                                   AS manager_name,
        TRY_CAST(capacity_orders_per_day AS INTEGER)         AS capacity_orders_per_day,
        TRY_CAST(open_date AS DATE)                          AS open_date,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY _loaded_at DESC
        )                                                    AS _rn
    FROM raw_stores
),
deduped AS (
    SELECT * FROM casted WHERE _rn = 1
)
SELECT
    store_id, store_name, area, zone, pincode, open_date, manager_name, capacity_orders_per_day,
    -- DQ flags
    CASE
        WHEN store_id IS NULL THEN 'FAIL'
        WHEN zone IS NULL THEN 'WARN'
        WHEN capacity_orders_per_day IS NOT NULL AND capacity_orders_per_day <= 0 THEN 'WARN'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN store_id IS NULL THEN 'Missing store_id'
        WHEN zone IS NULL THEN 'Missing zone'
        WHEN capacity_orders_per_day IS NOT NULL AND capacity_orders_per_day <= 0 THEN 'Zero or negative capacity'
        ELSE NULL
    END AS failure_reason,
    _loaded_at, _source_file, _run_id
FROM deduped;
