-- ==========================================================================
-- 03_create_marts.sql
-- NammaMart ELT Pipeline — Mart Layer (Business-Ready Tables)
--
-- Purpose : Build analytical tables from staging data.
--           Only uses rows where dq_status = 'PASS' (clean data).
--           Answers the 6 business questions from the assignment.
-- Idempotent: CREATE OR REPLACE TABLE.
-- ==========================================================================

-- -------------------------------------------------------------------------
-- MART 1: Daily Zone Revenue  (Q1: Revenue by zone and store)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_daily_zone_revenue AS
SELECT
    o.order_date,
    s.zone,
    s.store_name,
    COUNT(*)                                AS order_count,
    SUM(o.net_revenue)                      AS total_net_revenue,
    SUM(o.calculated_order_amount)          AS total_gross_revenue,
    ROUND(AVG(o.net_revenue), 2)            AS avg_order_value
FROM stg_orders o
LEFT JOIN stg_stores s
    ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, s.store_name
ORDER BY o.order_date, s.zone, s.store_name;


-- -------------------------------------------------------------------------
-- MART 2: Category Margin  (Q2: Revenue and gross margin by category)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(*)                                AS order_count,
    SUM(o.net_revenue)                      AS total_revenue,
    SUM(
        o.net_revenue - (o.quantity * COALESCE(p.cost_price, 0))
    )                                       AS total_gross_margin,
    ROUND(
        CASE WHEN SUM(o.net_revenue) > 0
             THEN 100.0 * SUM(o.net_revenue - (o.quantity * COALESCE(p.cost_price, 0)))
                       / SUM(o.net_revenue)
             ELSE 0
        END, 2
    )                                       AS margin_pct
FROM stg_orders o
LEFT JOIN stg_products p
    ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY total_revenue DESC;


-- -------------------------------------------------------------------------
-- MART 3: Delivery SLA  (Q3: Avg delivery time, % within 15 min)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.zone,
    s.store_name,
    COUNT(*)                                AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 2)       AS avg_delivery_minutes,
    ROUND(MIN(o.delivery_minutes), 2)       AS min_delivery_minutes,
    ROUND(MAX(o.delivery_minutes), 2)       AS max_delivery_minutes,
    SUM(CASE WHEN o.within_15_min THEN 1 ELSE 0 END)
                                            AS orders_within_15_min,
    ROUND(
        100.0 * SUM(CASE WHEN o.within_15_min THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 2
    )                                       AS pct_within_15_min
FROM stg_orders o
LEFT JOIN stg_stores s
    ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
  AND o.delivery_minutes IS NOT NULL
GROUP BY s.zone, s.store_name
ORDER BY s.zone, avg_delivery_minutes;


-- -------------------------------------------------------------------------
-- MART 4: Payment Reconciliation  (Q4: Revenue by payment mode + mismatches)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT
    p.payment_id,
    p.order_id,
    p.amount_paid,
    o.calculated_order_amount               AS order_amount,
    p.payment_status,
    CASE
        WHEN o.order_id IS NULL
            THEN 'ORDER_NOT_FOUND'
        WHEN UPPER(p.payment_status) = 'SUCCESS'
             AND ABS(p.amount_paid - o.calculated_order_amount) > 0.01
            THEN 'AMOUNT_MISMATCH'
        WHEN UPPER(p.payment_status) = 'SUCCESS'
            THEN 'MATCHED'
        ELSE 'NOT_SUCCESS'
    END                                     AS reconciliation_status
FROM stg_payments p
LEFT JOIN stg_orders o
    ON p.order_id = o.order_id
WHERE p.dq_status = 'PASS';


-- -------------------------------------------------------------------------
-- MART 5: Top Customers by Spend  (Q5: Top 10 customers, masked PII)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.customer_name,
    c.masked_email,
    c.masked_phone,
    c.city,
    c.area,
    c.pincode,
    c.loyalty_tier,
    COUNT(*)                                AS total_orders,
    SUM(o.net_revenue)                      AS total_spend,
    ROUND(AVG(o.net_revenue), 2)            AS avg_order_value,
    MIN(o.order_date)                       AS first_order_date,
    MAX(o.order_date)                       AS last_order_date
FROM stg_orders o
INNER JOIN stg_customers c
    ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASS'
  AND c.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY
    c.customer_id, c.customer_name, c.masked_email,
    c.masked_phone, c.city, c.area, c.pincode, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;


-- -------------------------------------------------------------------------
-- MART 6: ELT DQ Summary  (aggregated DQ status across staging tables)
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE mart_elt_dq_summary AS
SELECT 'orders'    AS dataset, dq_status, COUNT(*) AS row_count, failure_reason FROM stg_orders GROUP BY dq_status, failure_reason
UNION ALL
SELECT 'payments'  AS dataset, dq_status, COUNT(*) AS row_count, failure_reason FROM stg_payments GROUP BY dq_status, failure_reason
UNION ALL
SELECT 'customers' AS dataset, dq_status, COUNT(*) AS row_count, failure_reason FROM stg_customers GROUP BY dq_status, failure_reason
UNION ALL
SELECT 'products'  AS dataset, dq_status, COUNT(*) AS row_count, failure_reason FROM stg_products GROUP BY dq_status, failure_reason
UNION ALL
SELECT 'stores'    AS dataset, dq_status, COUNT(*) AS row_count, failure_reason FROM stg_stores GROUP BY dq_status, failure_reason
ORDER BY dataset, dq_status;
