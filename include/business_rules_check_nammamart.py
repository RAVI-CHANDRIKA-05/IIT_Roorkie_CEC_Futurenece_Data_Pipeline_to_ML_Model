
"""
File: include/business_rules_check_nammamart.py

Purpose:
    Performs NammaMart-specific business-rule validation on the five
    DataFrames extracted and parsed by the ETL pipeline.

Business rules:
    1. Order status must be DELIVERED, CANCELLED, or RETURNED.
    2. Quantity must be positive.
    3. Unit price must be non-negative.
    4. Discount percentage must be between 0 and 100.
    5. Order amount must match the agreed calculation.
    6. DELIVERED orders must have a positive delivery duration.
    7. Delivery durations outside 8-35 minutes are warnings only.
    8. Payment mode must be UPI, Card, Cash on Delivery, or Wallet.
    9. Payment amounts must match their associated order amounts,
       when the required fields are available.
    10. Product prices and costs must be non-negative.
    11. Product cost should not exceed MRP; this is a warning.

Responsibilities:
    - Accept already-parsed DataFrames.
    - Append findings to the technical DQ findings list.
    - Return a consistent issue structure for downstream classification.

This module does not:
    - Read source CSV files.
    - Process the incremental orders file.
    - Write quarantine files.
    - Load the warehouse.
    - Modify source DataFrames.
"""

import logging

import pandas as pd


logger = logging.getLogger(__name__)

VALID_ORDER_STATUSES = {
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
}

VALID_PAYMENT_MODES = {
    "UPI",
    "CARD",
    "CASH ON DELIVERY",
    "WALLET",
}

MIN_DELIVERY_MINUTES = 8
MAX_DELIVERY_MINUTES = 35


def normalized_text(value):
    """Normalize a text value for case-insensitive comparisons."""
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def add_issue(
    issues: list,
    dataset_name: str,
    df: pd.DataFrame,
    row_index: int,
    column: str,
    problem: str,
    dimension: str,
    severity: str,
    proposed_rule: str,
):
    """Append a standardized business-rule finding."""
    key_columns = {
        "orders": "order_id",
        "payments": "payment_id",
        "customers": "customer_id",
        "products": "product_id",
        "stores": "store_id",
    }

    key_column = key_columns.get(dataset_name)

    record_id = None
    if key_column and key_column in df.columns:
        record_id = df.at[row_index, key_column]

    issues.append({
        "file": dataset_name,
        "source_row": int(row_index) + 2,
        "record_id": record_id,
        "column": column,
        "problem": problem,
        "quality_dimension": dimension,
        "severity": severity,
        "proposed_rule": proposed_rule,
    })


def require_columns(
    dataset_name: str,
    df: pd.DataFrame,
    required_columns: list,
    issues: list,
):
    """Report missing columns needed to evaluate a business rule."""
    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    for column in missing_columns:
        issues.append({
            "file": dataset_name,
            "source_row": None,
            "record_id": None,
            "column": column,
            "problem": (
                f"Cannot evaluate business rules because "
                f"column '{column}' is missing"
            ),
            "quality_dimension": "Validity",
            "severity": "Critical",
            "proposed_rule": (
                "Confirm the source schema and provide the required "
                "column before enabling this business-rule check."
            ),
        })

    return not missing_columns


def numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    """Convert a column to numeric values; invalid values become NaN."""
    return pd.to_numeric(df[column], errors="coerce")


def check_order_values(datasets: dict, issues: list):
    """Validate order status, quantity, prices, discounts and amounts."""
    dataset_name = "orders"
    df = datasets["orders"]

    required_columns = [
        "order_status",
        "quantity",
        "unit_price",
        "discount_pct",
        "order_amount",
    ]

    if not require_columns(
        dataset_name, df, required_columns, issues
    ):
        return

    statuses = df["order_status"].map(normalized_text)
    quantity = numeric_series(df, "quantity")
    unit_price = numeric_series(df, "unit_price")
    discount = numeric_series(df, "discount_pct")
    order_amount = numeric_series(df, "order_amount")

    for row_index in df.index:
        status = statuses.loc[row_index]

        if status not in VALID_ORDER_STATUSES:
            add_issue(
                issues, dataset_name, df, row_index,
                "order_status",
                f"Invalid order status: {df.at[row_index, 'order_status']}",
                "Validity", "High",
                "Allow only DELIVERED, CANCELLED, or RETURNED.",
            )

        if pd.isna(quantity.loc[row_index]):
            add_issue(
                issues, dataset_name, df, row_index,
                "quantity",
                "Quantity is missing or is not numeric",
                "Validity", "High",
                "Require a numeric quantity greater than zero.",
            )
        elif quantity.loc[row_index] <= 0:
            add_issue(
                issues, dataset_name, df, row_index,
                "quantity",
                "Quantity must be greater than zero",
                "Validity", "High",
                "Require quantity > 0.",
            )

        if pd.isna(unit_price.loc[row_index]):
            add_issue(
                issues, dataset_name, df, row_index,
                "unit_price",
                "Unit price is missing or is not numeric",
                "Validity", "High",
                "Require a numeric unit price greater than or equal to zero.",
            )
        elif unit_price.loc[row_index] < 0:
            add_issue(
                issues, dataset_name, df, row_index,
                "unit_price",
                "Unit price cannot be negative",
                "Validity", "High",
                "Require unit_price >= 0.",
            )

        if pd.isna(discount.loc[row_index]):
            add_issue(
                issues, dataset_name, df, row_index,
                "discount_pct",
                "Discount is missing or is not numeric",
                "Validity", "High",
                "Require a numeric discount between 0 and 100.",
            )
        elif not 0 <= discount.loc[row_index] <= 100:
            add_issue(
                issues, dataset_name, df, row_index,
                "discount_pct",
                "Discount percentage must be between 0 and 100",
                "Validity", "High",
                "Require 0 <= discount_pct <= 100.",
            )

        if pd.isna(order_amount.loc[row_index]):
            add_issue(
                issues, dataset_name, df, row_index,
                "order_amount",
                "Order amount is missing or is not numeric",
                "Consistency", "High",
                "Calculate order_amount from quantity, unit price and discount.",
            )
            continue

        if order_amount.loc[row_index] < 0:
            add_issue(
                issues, dataset_name, df, row_index,
                "order_amount",
                "Order amount cannot be negative",
                "Validity", "High",
                "Require order_amount >= 0.",
            )

        # Only calculate the expected amount when all inputs are valid.
        q = quantity.loc[row_index]
        price = unit_price.loc[row_index]
        pct = discount.loc[row_index]

        if (
            pd.notna(q)
            and pd.notna(price)
            and pd.notna(pct)
            and q > 0
            and price >= 0
            and 0 <= pct <= 100
        ):
            expected_amount = round(
                float(q) * float(price) * (1 - float(pct) / 100),
                2,
            )

            if round(float(order_amount.loc[row_index]), 2) != expected_amount:
                add_issue(
                    issues, dataset_name, df, row_index,
                    "order_amount",
                    (
                        f"Order amount mismatch: source="
                        f"{order_amount.loc[row_index]}, "
                        f"expected={expected_amount}"
                    ),
                    "Consistency", "High",
                    (
                        "Recalculate as round(quantity * unit_price * "
                        "(1 - discount_pct / 100), 2)."
                    ),
                )


def check_delivery_rules(datasets: dict, issues: list):
    """Validate delivery duration; out-of-range durations are warnings."""
    dataset_name = "orders"
    df = datasets["orders"]

    required_columns = ["order_status", "delivery_minutes"]

    if not require_columns(
        dataset_name, df, required_columns, issues
    ):
        return

    statuses = df["order_status"].map(normalized_text)
    delivery = numeric_series(df, "delivery_minutes")

    for row_index in df.index:
        status = statuses.loc[row_index]
        minutes = delivery.loc[row_index]

        if status == "DELIVERED":
            if pd.isna(minutes) or minutes <= 0:
                add_issue(
                    issues, dataset_name, df, row_index,
                    "delivery_minutes",
                    "DELIVERED order must have a positive delivery duration",
                    "Completeness", "High",
                    "Require delivery_minutes > 0 for DELIVERED orders.",
                )
                continue

        if pd.notna(minutes) and minutes > 0:
            if (
                minutes < MIN_DELIVERY_MINUTES
                or minutes > MAX_DELIVERY_MINUTES
            ):
                add_issue(
                    issues, dataset_name, df, row_index,
                    "delivery_minutes",
                    (
                        f"Delivery duration of {minutes:g} minutes "
                        "is outside the usual 8-35 minute range"
                    ),
                    "Validity", "Warning",
                    (
                        "Review unusual delivery durations. This is a "
                        "warning and must not automatically quarantine "
                        "the record."
                    ),
                )


def check_revenue_rules(datasets: dict, issues: list):
    """
    Check revenue values only when a revenue column already exists.

    Revenue derivation itself belongs in the transformation stage.
    """
    dataset_name = "orders"
    df = datasets["orders"]

    if "revenue" not in df.columns:
        logger.info(
            "[Business Rules] No revenue column in source orders; "
            "revenue will be derived during transformation."
        )
        return

    if not require_columns(
        dataset_name, df, ["order_status", "revenue"], issues
    ):
        return

    statuses = df["order_status"].map(normalized_text)
    revenue = numeric_series(df, "revenue")

    for row_index in df.index:
        status = statuses.loc[row_index]
        value = revenue.loc[row_index]

        if pd.isna(value):
            add_issue(
                issues, dataset_name, df, row_index,
                "revenue",
                "Revenue is missing or is not numeric",
                "Consistency", "High",
                "Derive revenue from order status and order amount.",
            )
            continue

        if status in {"CANCELLED", "RETURNED"} and value != 0:
            add_issue(
                issues, dataset_name, df, row_index,
                "revenue",
                f"{status} orders must have zero revenue",
                "Consistency", "High",
                "Revenue must be zero for CANCELLED and RETURNED orders.",
            )


def check_payment_modes(datasets: dict, issues: list):
    """Validate payment modes against the allowed values."""
    dataset_name = "payments"
    df = datasets["payments"]

    if not require_columns(
        dataset_name, df, ["payment_mode"], issues
    ):
        return

    for row_index in df.index:
        payment_mode = normalized_text(df.at[row_index, "payment_mode"])

        if payment_mode not in VALID_PAYMENT_MODES:
            add_issue(
                issues, dataset_name, df, row_index,
                "payment_mode",
                f"Invalid payment mode: {df.at[row_index, 'payment_mode']}",
                "Validity", "High",
                (
                    "Allow only UPI, Card, Cash on Delivery, or Wallet; "
                    "normalize case and whitespace before comparison."
                ),
            )


def check_payment_amounts(datasets: dict, issues: list):
    """
    Compare payment amount with the associated order amount when both
    datasets contain the required columns.

    This check does not assume one payment per order. If multiple payment
    rows exist for one order, each payment is checked individually
    against the order amount, so review the business interpretation
    before treating mismatches as blocking.
    """
    payments = datasets["payments"]
    orders = datasets["orders"]

    if not require_columns(
        "payments", payments, ["order_id", "amount_paid"], issues
    ):
        return

    if not require_columns(
        "orders", orders, ["order_id", "order_amount"], issues
    ):
        return

    order_amounts = orders[["order_id", "order_amount"]].copy()
    order_amounts["_join_key"] = (
        order_amounts["order_id"].astype("string").str.strip()
    )

    # Duplicate order IDs are handled by technical uniqueness checks.
    # Avoid a many-to-many merge that could multiply records.
    order_amounts = order_amounts.drop_duplicates(
        subset=["_join_key"], keep=False
    )
    amount_by_order = order_amounts.set_index("_join_key")["order_amount"]

    for row_index in payments.index:
        order_id = payments.at[row_index, "order_id"]

        if pd.isna(order_id) or str(order_id).strip() == "":
            continue

        join_key = str(order_id).strip()

        if join_key not in amount_by_order.index:
            # Referential integrity reports missing order references.
            continue

        payment_amount = pd.to_numeric(
            pd.Series([payments.at[row_index, "amount_paid"]]),
            errors="coerce",
        ).iloc[0]

        order_amount = pd.to_numeric(
            pd.Series([amount_by_order.loc[join_key]]),
            errors="coerce",
        ).iloc[0]

        if pd.isna(payment_amount) or pd.isna(order_amount):
            add_issue(
                issues, "payments", payments, row_index,
                "amount_paid",
                "Payment or associated order amount is not numeric",
                "Consistency", "High",
                "Require numeric payment and order amounts.",
            )
        elif round(float(payment_amount), 2) != round(float(order_amount), 2):
            add_issue(
                issues, "payments", payments, row_index,
                "amount_paid",
                (
                    f"Payment amount {payment_amount} does not match "
                    f"order amount {order_amount}"
                ),
                "Consistency", "High",
                (
                    "Verify payment allocation, refunds, split payments, "
                    "and the expected payment-to-order relationship."
                ),
            )


def check_product_values(datasets: dict, issues: list):
    """Validate product prices and flag cost exceeding MRP as a warning."""
    dataset_name = "products"
    df = datasets["products"]

    required_columns = ["mrp", "cost_price"]

    if not require_columns(
        dataset_name, df, required_columns, issues
    ):
        return

    mrp = numeric_series(df, "mrp")
    cost_price = numeric_series(df, "cost_price")

    for row_index in df.index:
        for column, values in [
            ("mrp", mrp),
            ("cost_price", cost_price),
        ]:
            value = values.loc[row_index]

            if pd.isna(value):
                add_issue(
                    issues, dataset_name, df, row_index,
                    column,
                    f"{column} is missing or is not numeric",
                    "Validity", "High",
                    f"Require a numeric, non-negative {column}.",
                )
            elif value < 0:
                add_issue(
                    issues, dataset_name, df, row_index,
                    column,
                    f"{column} cannot be negative",
                    "Validity", "High",
                    f"Require {column} >= 0.",
                )

        if (
            pd.notna(mrp.loc[row_index])
            and pd.notna(cost_price.loc[row_index])
            and mrp.loc[row_index] >= 0
            and cost_price.loc[row_index] >= 0
            and cost_price.loc[row_index] > mrp.loc[row_index]
        ):
            add_issue(
                issues, dataset_name, df, row_index,
                "cost_price",
                "Product cost price exceeds MRP",
                "Consistency", "Warning",
                (
                    "Review the product pricing. Treat this as a warning "
                    "unless the business owner confirms it must block loading."
                ),
            )


def run_business_rules(
    datasets: dict,
    existing_issues: list | None = None,
) -> list:
    """
    Run business-rule checks against the same five DataFrames used by
    technical DQ validation.

    Existing technical findings are retained and business findings
    are appended to the same list.
    """
    expected_datasets = {
        "orders", "payments", "customers", "products", "stores"
    }

    if set(datasets) != expected_datasets:
        raise ValueError(
            "Business rules require exactly these datasets: "
            f"{sorted(expected_datasets)}"
        )

    issues = existing_issues if existing_issues is not None else []

    check_order_values(datasets, issues)
    check_delivery_rules(datasets, issues)
    check_revenue_rules(datasets, issues)
    check_payment_modes(datasets, issues)
    check_payment_amounts(datasets, issues)
    check_product_values(datasets, issues)

    logger.info(
        "[Business Rules] Validation completed. Total findings: %d",
        len(issues),
    )

    return issues
