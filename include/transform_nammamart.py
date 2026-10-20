"""include/transform_nammamart.py

Purpose:
    Transform passing records into a star schema for the ETL pipeline.

Outputs:
    - fact_json:          etl_fact_orders  (FKs + derived measures)
    - dim_customers_json: etl_dim_customer (PII masked)
    - dim_products_json:  etl_dim_product  (deduplicated)
    - dim_stores_json:    etl_dim_store
    - payments_json:      passing payment records for reconciliation
    - loaded_count:       number of fact rows ready to load

Derived columns on the fact table:
    - order_date:              DATE from order_ts
    - calculated_order_amount: qty * unit_price * (1 - discount_pct/100)
    - net_revenue:             calculated_order_amount for DELIVERED, else 0
    - gross_margin:            net_revenue - (quantity * cost_price)
    - within_15_min:           True if delivery_minutes <= 15

This module does NOT:
    - Read CSV files (extract_nammamart handles that)
    - Write quarantine files (the DAG handles that)
    - Load the warehouse (the DAG handles that)
"""
from __future__ import annotations

import json
import logging

import pandas as pd
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

BLOCKING_SEVERITIES = {"HIGH", "CRITICAL", "ERROR", "FAIL"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_dataframe(payload, dataset_name):
    """Convert JSON, list or DataFrame input into a DataFrame."""
    if isinstance(payload, pd.DataFrame):
        return payload.copy()
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise AirflowFailException(
                f"Invalid JSON for dataset '{dataset_name}': {exc}"
            ) from exc
        if not isinstance(parsed, list):
            raise AirflowFailException(
                f"Expected a JSON list for dataset '{dataset_name}'."
            )
        return pd.DataFrame(parsed)
    raise AirflowFailException(
        f"Unsupported payload type for '{dataset_name}': "
        f"{type(payload).__name__}"
    )


def _normalise_dataset_name(finding):
    value = (
        finding.get("dataset")
        or finding.get("file")
        or finding.get("dataset_name")
        or finding.get("table_name")
        or finding.get("source_file")
    )
    return str(value).strip().lower() if value is not None else None


def _get_blocked_indexes(dq_result, dataset_name):
    """Return zero-based DataFrame indexes for blocking findings only."""
    blocked = set()
    for finding in dq_result.get("findings", []):
        if _normalise_dataset_name(finding) != dataset_name:
            continue
        severity = str(finding.get("severity", "HIGH")).upper()
        if severity not in BLOCKING_SEVERITIES:
            continue
        source_row = finding.get(
            "source_row",
            finding.get("row_number", finding.get("row_num")),
        )
        try:
            source_row = int(source_row)
        except (TypeError, ValueError):
            continue
        index = source_row - 2
        if index >= 0:
            blocked.add(index)
    return blocked


def _exclude_blocked_rows(df, dataset_name, dq_result):
    blocked_indexes = _get_blocked_indexes(dq_result, dataset_name)
    if not blocked_indexes:
        return df.copy()
    keep_mask = ~df.index.isin(blocked_indexes)
    result = df.loc[keep_mask].copy()
    logger.info(
        "Dataset %s: excluded %d failed rows; %d rows remain",
        dataset_name,
        len(df) - len(result),
        len(result),
    )
    return result


def _require_columns(df, required, dataset_name):
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise AirflowFailException(
            f"Cannot transform '{dataset_name}'; missing columns: {missing}"
        )


def _mask_email(value):
    if pd.isna(value):
        return None
    email = str(value).strip()
    if "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    if not local:
        return f"***@{domain}"
    return f"{local[0]}***@{domain}"


def _mask_phone(value):
    if pd.isna(value):
        return None
    digits = "".join(c for c in str(value) if c.isdigit())
    if len(digits) <= 4:
        return "*" * len(digits)
    return "*" * (len(digits) - 4) + digits[-4:]


def _to_numeric(df, columns):
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _json_records(df):
    """Serialize a DataFrame as JSON records with null-safe values."""
    safe_df = df.copy()
    safe_df = safe_df.astype(object).where(pd.notna(safe_df), None)
    return safe_df.to_json(orient="records", date_format="iso", date_unit="s")


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def _build_dim_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate customers and mask PII (email, phone)."""
    dim = customers.copy()
    dim["customer_id"] = dim["customer_id"].astype(str)

    # Keep first occurrence of each customer_id (DQ already quarantined conflicts)
    dim = dim.drop_duplicates(subset=["customer_id"], keep="first")

    # Mask PII
    if "email" in dim.columns:
        dim["masked_email"] = dim["email"].map(_mask_email)
        dim = dim.drop(columns=["email"])
    if "phone" in dim.columns:
        dim["masked_phone"] = dim["phone"].map(_mask_phone)
        dim = dim.drop(columns=["phone"])

    # Standardise loyalty_tier casing
    if "loyalty_tier" in dim.columns:
        dim["loyalty_tier"] = (
            dim["loyalty_tier"].astype(str).str.strip().str.title()
        )

    dim = dim.reset_index(drop=True)
    return dim


def _build_dim_products(products: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate products, keeping the current (non-old) record."""
    dim = products.copy()
    dim["product_id"] = dim["product_id"].astype(str)

    # Prefer records without '(old)' in the name
    if "product_name" in dim.columns:
        dim["_is_old"] = (
            dim["product_name"]
            .astype(str)
            .str.lower()
            .str.contains("(old)", regex=False)
            .fillna(False)
        )
        dim = dim.sort_values("_is_old", ascending=True)
        dim = dim.drop_duplicates(subset=["product_id"], keep="first")
        dim = dim.drop(columns=["_is_old"])
    else:
        dim = dim.drop_duplicates(subset=["product_id"], keep="first")

    dim = _to_numeric(dim, ["mrp", "cost_price"])
    dim = dim.reset_index(drop=True)
    return dim


def _build_dim_stores(stores: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate stores."""
    dim = stores.copy()
    dim["store_id"] = dim["store_id"].astype(str)
    dim = dim.drop_duplicates(subset=["store_id"], keep="first")
    dim = dim.reset_index(drop=True)
    return dim


# ---------------------------------------------------------------------------
# Main transformation
# ---------------------------------------------------------------------------

def run_transformations(
    orders_json,
    payments_json,
    customers_json,
    products_json,
    stores_json,
    dq_result,
):
    """
    Transform passing rows into a star schema.

    Returns:
        dict with fact_json, dim_customers_json, dim_products_json,
        dim_stores_json, payments_json, loaded_count.
    """
    if dq_result.get("pipeline_blockers"):
        raise AirflowFailException(
            "Transformation stopped because pipeline-level DQ blockers exist."
        )

    # --- Parse inputs ---
    orders = _to_dataframe(orders_json, "orders")
    payments = _to_dataframe(payments_json, "payments")
    customers = _to_dataframe(customers_json, "customers")
    products = _to_dataframe(products_json, "products")
    stores = _to_dataframe(stores_json, "stores")

    # --- Exclude blocked rows ---
    orders = _exclude_blocked_rows(orders, "orders", dq_result)
    payments = _exclude_blocked_rows(payments, "payments", dq_result)
    customers = _exclude_blocked_rows(customers, "customers", dq_result)
    products = _exclude_blocked_rows(products, "products", dq_result)
    stores = _exclude_blocked_rows(stores, "stores", dq_result)

    # --- Validate minimum columns ---
    _require_columns(
        orders,
        [
            "order_id", "order_ts", "customer_id", "store_id",
            "product_id", "quantity", "unit_price", "discount_pct",
            "order_amount", "order_status", "payment_mode",
            "delivery_minutes",
        ],
        "orders",
    )
    _require_columns(products, ["product_id", "cost_price"], "products")
    _require_columns(customers, ["customer_id"], "customers")
    _require_columns(stores, ["store_id"], "stores")

    # --- Build dimension tables ---
    dim_customers = _build_dim_customers(customers)
    dim_products = _build_dim_products(products)
    dim_stores = _build_dim_stores(stores)

    # --- Build fact table ---
    fact = orders.copy()
    fact["order_id"] = fact["order_id"].astype(str)
    fact["customer_id"] = fact["customer_id"].astype(str)
    fact["product_id"] = fact["product_id"].astype(str)
    fact["store_id"] = fact["store_id"].astype(str)

    fact = _to_numeric(
        fact,
        ["quantity", "unit_price", "discount_pct",
         "order_amount", "delivery_minutes"],
    )

    fact["order_status"] = (
        fact["order_status"].astype(str).str.strip().str.upper()
    )
    fact["payment_mode"] = fact["payment_mode"].astype(str).str.strip()

    # Calculated order amount (business formula)
    fact["calculated_order_amount"] = (
        fact["quantity"]
        * fact["unit_price"]
        * (1 - fact["discount_pct"] / 100)
    ).round(2)

    # Order date from timestamp
    fact["order_ts"] = pd.to_datetime(
        fact["order_ts"], errors="coerce", utc=True
    )
    fact["order_date"] = fact["order_ts"].dt.date.astype("string")

    # Net revenue: DELIVERED orders only
    fact["net_revenue"] = fact["calculated_order_amount"].where(
        fact["order_status"].eq("DELIVERED"), 0.0
    )

    # Gross margin: net_revenue - (quantity * cost_price)
    # Join with dim_products temporarily to get cost_price
    cost_lookup = dim_products[["product_id", "cost_price"]].copy()
    fact = fact.merge(
        cost_lookup, on="product_id", how="left", suffixes=("", "_dim")
    )
    fact["gross_margin"] = (
        fact["net_revenue"]
        - (fact["quantity"] * fact["cost_price"].fillna(0))
    ).round(2)
    fact = fact.drop(columns=["cost_price"], errors="ignore")

    # Within 15 min delivery SLA
    if "delivery_minutes" in fact.columns:
        fact["within_15_min"] = (
            fact["delivery_minutes"].notna()
            & fact["delivery_minutes"].le(15)
        )
    else:
        fact["within_15_min"] = False

    # --- Deduplicate fact on order_id ---
    if fact["order_id"].duplicated().any():
        raise AirflowFailException(
            "Duplicate order_id values remain in passing orders."
        )

    # --- Prepare payments for reconciliation ---
    if not payments.empty:
        if "amount_paid" not in payments.columns:
            if "payment_amount" in payments.columns:
                payments = payments.rename(
                    columns={"payment_amount": "amount_paid"}
                )
            else:
                raise AirflowFailException(
                    "Payments need amount_paid or payment_amount."
                )
        _require_columns(
            payments,
            ["payment_id", "order_id", "amount_paid", "payment_status"],
            "payments",
        )
        payments["amount_paid"] = pd.to_numeric(
            payments["amount_paid"], errors="coerce"
        )
        payments["order_id"] = payments["order_id"].astype(str)

    # --- Final fact columns (star schema: FKs + measures only) ---
    fact_columns = [
        "order_id", "order_ts", "order_date",
        "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct",
        "order_amount", "calculated_order_amount",
        "net_revenue", "gross_margin", "within_15_min",
        "payment_mode", "order_status", "channel",
        "delivery_minutes",
    ]
    # Keep only columns that exist (channel may be absent)
    fact_columns = [c for c in fact_columns if c in fact.columns]
    fact = fact[fact_columns]

    logger.info(
        "Transform complete: %d fact rows, %d customers, "
        "%d products, %d stores",
        len(fact), len(dim_customers),
        len(dim_products), len(dim_stores),
    )

    return {
        "fact_json": _json_records(fact),
        "dim_customers_json": _json_records(dim_customers),
        "dim_products_json": _json_records(dim_products),
        "dim_stores_json": _json_records(dim_stores),
        "payments_json": _json_records(payments),
        "loaded_count": len(fact),
    }
