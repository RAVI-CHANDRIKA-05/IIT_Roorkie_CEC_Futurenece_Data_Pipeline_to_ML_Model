-- ==========================================================================
-- 01_create_raw.sql
-- NammaMart ELT Pipeline — Raw Layer (Landing Zone)
--
-- Purpose : Create raw tables that mirror CSV structure exactly.
--           ALL columns are VARCHAR (no type casting at this stage).
--           Metadata columns track lineage: _loaded_at, _source_file, _run_id.
-- Idempotent: CREATE TABLE IF NOT EXISTS — safe to re-run.
-- ==========================================================================

-- -------------------------------------------------------------------------
-- RAW ORDERS
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id          VARCHAR,
    order_ts          VARCHAR,
    customer_id       VARCHAR,
    store_id          VARCHAR,
    product_id        VARCHAR,
    quantity          VARCHAR,
    unit_price        VARCHAR,
    discount_pct      VARCHAR,
    order_amount      VARCHAR,
    payment_mode      VARCHAR,
    order_status      VARCHAR,
    channel           VARCHAR,
    delivery_minutes  VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _run_id           VARCHAR
);

-- -------------------------------------------------------------------------
-- RAW PAYMENTS
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id        VARCHAR,
    order_id          VARCHAR,
    payment_ts        VARCHAR,
    payment_mode      VARCHAR,
    amount_paid       VARCHAR,
    payment_status    VARCHAR,
    gateway_ref       VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _run_id           VARCHAR
);

-- -------------------------------------------------------------------------
-- RAW CUSTOMERS
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id       VARCHAR,
    customer_name     VARCHAR,
    email             VARCHAR,
    phone             VARCHAR,
    city              VARCHAR,
    signup_date       VARCHAR,
    loyalty_tier      VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _run_id           VARCHAR
);

-- -------------------------------------------------------------------------
-- RAW PRODUCTS
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_products (
    product_id        VARCHAR,
    product_name      VARCHAR,
    category          VARCHAR,
    mrp               VARCHAR,
    cost_price        VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR,
    _run_id           VARCHAR
);

-- -------------------------------------------------------------------------
-- RAW STORES
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_stores (
    store_id              VARCHAR,
    store_name            VARCHAR,
    city                  VARCHAR,
    zone                  VARCHAR,
    capacity_orders_per_day VARCHAR,
    _loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file          VARCHAR,
    _run_id               VARCHAR
);
