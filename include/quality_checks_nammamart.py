
"""
File: include/quality_checks_nammamart.py

Purpose:
    Technical data-quality checks for the five NammaMart datasets.

Checks:
    1. Completeness
    2. Uniqueness and duplicate resolution
    3. Referential integrity

Duplicate-handling rules:
    - Exact duplicate orders/payments: flag only extra copies.
    - Conflicting duplicate orders/payments: flag all records.
    - Customers:
        * Compare full_name and email first.
        * If either differs, flag all records as an identity conflict.
        * If both match, identify every other column that differs.
        * Flag all conflicting customer records for confirmation.
    - Products: if exactly one current product and one or more '(old)'
      records exist, flag only the '(old)' records.
    - Stores: flag all records sharing a duplicated store_id.

This module does not write quarantine files or load the warehouse.
The Airflow DAG handles quarantine and downstream orchestration.
"""

import json
import logging

import pandas as pd


logger = logging.getLogger(__name__)


PRIMARY_KEYS = {
    "orders": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
}

FOREIGN_KEYS = {
    "orders": {
        "customer_id": ("customers", "customer_id"),
        "product_id": ("products", "product_id"),
        "store_id": ("stores", "store_id"),
    },
    "payments": {
        "order_id": ("orders", "order_id"),
    },
}


def normalize_values(series: pd.Series) -> pd.Series:
    """Normalize values for comparison without modifying source data."""
    return series.astype("string").str.strip()


def values_match(value_a, value_b) -> bool:
    """Compare two field values, treating missing values consistently."""
    a_missing = pd.isna(value_a)
    b_missing = pd.isna(value_b)

    if a_missing and b_missing:
        return True

    if a_missing or b_missing:
        return False

    return str(value_a).strip() == str(value_b).strip()


def record_identifier(
    df: pd.DataFrame,
    row_index: int,
    dataset_name: str,
):
    """Return the primary identifier for a record, when available."""
    key_column = PRIMARY_KEYS.get(dataset_name)

    if key_column and key_column in df.columns:
        return df.at[row_index, key_column]

    return None


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
    """Append a standardized technical DQ finding."""
    issues.append({
        "file": dataset_name,
        "source_row": int(row_index) + 2,
        "record_id": record_identifier(
            df, row_index, dataset_name
        ),
        "column": column,
        "problem": problem,
        "quality_dimension": dimension,
        "severity": severity,
        "proposed_rule": proposed_rule,
    })


def parse_json_datasets(
    orders_json: str,
    payments_json: str,
    customers_json: str,
    products_json: str,
    stores_json: str,
) -> dict:
    """Convert the five JSON strings from Airflow XCom into DataFrames."""
    json_inputs = {
        "orders": orders_json,
        "payments": payments_json,
        "customers": customers_json,
        "products": products_json,
        "stores": stores_json,
    }

    datasets = {}

    for dataset_name, json_text in json_inputs.items():
        if not isinstance(json_text, str):
            raise TypeError(
                f"Expected JSON string for '{dataset_name}', "
                f"received {type(json_text).__name__}"
            )

        try:
            records = json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSON received for '{dataset_name}': {exc}"
            ) from exc

        if not isinstance(records, list):
            raise ValueError(
                f"Expected a JSON array of records for '{dataset_name}'"
            )

        datasets[dataset_name] = pd.DataFrame(records)

        logger.info(
            "[DQ] Received dataset '%s': %d records",
            dataset_name,
            len(datasets[dataset_name]),
        )

    return datasets


def check_missing_values(
    dataset_name: str,
    df: pd.DataFrame,
    issues: list,
):
    """Check missing values in mandatory fields."""
    required_columns = {
        "orders": [
            "order_ts",
            "customer_id",
            "store_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_pct",
            "order_amount",
            "order_status",
        ],
        "payments": [
            "order_id",
            "payment_ts",
            "payment_mode",
            "amount_paid",
            "payment_status",
        ],
        "customers": ["full_name"],
        "products": ["product_name", "mrp", "cost_price"],
        "stores": ["store_name"],
    }

    for column in required_columns.get(dataset_name, []):
        if column not in df.columns:
            issues.append({
                "file": dataset_name,
                "source_row": None,
                "record_id": None,
                "column": column,
                "problem": (
                    f"Required column '{column}' is missing "
                    "from the source dataset"
                ),
                "quality_dimension": "Completeness",
                "severity": "Critical",
                "proposed_rule": (
                    "Confirm the source schema and provide the "
                    "required column before processing records."
                ),
            })
            continue

        missing_mask = df[column].isna()

        if (
            pd.api.types.is_object_dtype(df[column])
            or pd.api.types.is_string_dtype(df[column])
        ):
            missing_mask = missing_mask | (
                df[column]
                .astype("string")
                .str.strip()
                .eq("")
                .fillna(False)
            )

        for row_index in df.index[missing_mask]:
            add_issue(
                issues,
                dataset_name,
                df,
                row_index,
                column,
                f"Required value missing in column '{column}'",
                "Completeness",
                "High",
                f"Provide a valid value for required field '{column}'.",
            )


def check_customer_duplicates(
    df: pd.DataFrame,
    duplicate_keys,
    identifiers: pd.Series,
    issues: list,
):
    """
    Identify why records sharing a customer_id conflict.

    If full_name or email differs, flag an identity conflict.
    If both match, compare every other column and list the fields
    with different values.

    All records in a conflicting customer group are flagged for
    confirmation; this function does not choose a winning record.
    """
    key_column = "customer_id"

    for key_value in duplicate_keys:
        group_mask = identifiers.eq(key_value).fillna(False)
        group_indices = list(df.index[group_mask])

        if len(group_indices) < 2:
            continue

        group_df = df.loc[group_indices]

        name_matches = (
            "full_name" in group_df.columns
            and all(
                values_match(
                    group_df.at[group_indices[0], "full_name"],
                    group_df.at[idx, "full_name"],
                )
                for idx in group_indices[1:]
            )
        )

        email_matches = (
            "email" in group_df.columns
            and all(
                values_match(
                    group_df.at[group_indices[0], "email"],
                    group_df.at[idx, "email"],
                )
                for idx in group_indices[1:]
            )
        )

        if not name_matches or not email_matches:
            reason = (
                f"Duplicate customer ID {key_value}: "
                "full_name and/or email differs across records. "
                "Identity confirmation is required before deciding "
                "which customer record to retain."
            )

            for row_index in group_indices:
                add_issue(
                    issues,
                    "customers",
                    df,
                    row_index,
                    key_column,
                    reason,
                    "Uniqueness",
                    "Critical",
                    (
                        "Quarantine the conflicting customer records. "
                        "Confirm the customer's identity and correct "
                        "details before loading."
                    ),
                )

            continue

        # Name and email match. Compare every other available column.
        identity_columns = {"customer_id", "full_name", "email"}
        conflicting_columns = []

        for column in df.columns:
            if column in identity_columns:
                continue

            values = [
                df.at[idx, column]
                for idx in group_indices
            ]

            first_value = values[0]

            if any(
                not values_match(first_value, value)
                for value in values[1:]
            ):
                conflicting_columns.append(column)

        if not conflicting_columns:
            # The customer records match across all available fields.
            # Keep the first row and flag only additional copies.
            for row_index in group_indices[1:]:
                add_issue(
                    issues,
                    "customers",
                    df,
                    row_index,
                    key_column,
                    (
                        f"Exact duplicate customer record; additional "
                        f"copy of customer ID {key_value}"
                    ),
                    "Uniqueness",
                    "Critical",
                    (
                        "Keep one copy of the identical customer record "
                        "and quarantine additional copies."
                    ),
                )

            continue

        field_summary = ", ".join(conflicting_columns)

        reason = (
            f"Duplicate customer ID {key_value}: full_name and email "
            f"match, but conflicting values exist in: {field_summary}. "
            "Confirm the latest/correct details before deciding which "
            "record to retain."
        )

        for row_index in group_indices:
            add_issue(
                issues,
                "customers",
                df,
                row_index,
                key_column,
                reason,
                "Uniqueness",
                "Critical",
                (
                    "Quarantine the conflicting customer records. "
                    "Use a trusted update timestamp if available; "
                    "otherwise request customer or data-steward "
                    "confirmation before loading."
                ),
            )


def check_primary_keys(
    dataset_name: str,
    df: pd.DataFrame,
    issues: list,
):
    """Check missing and duplicated primary-key values."""
    key_column = PRIMARY_KEYS[dataset_name]

    if key_column not in df.columns:
        issues.append({
            "file": dataset_name,
            "source_row": None,
            "record_id": None,
            "column": key_column,
            "problem": (
                f"Required primary-key column '{key_column}' is missing"
            ),
            "quality_dimension": "Completeness",
            "severity": "Critical",
            "proposed_rule": (
                f"Require the '{key_column}' column in the "
                "source dataset before processing records."
            ),
        })
        return

    identifiers = normalize_values(df[key_column])

    missing_mask = (
        identifiers.isna()
        | identifiers.eq("").fillna(False)
    )

    for row_index in df.index[missing_mask]:
        add_issue(
            issues,
            dataset_name,
            df,
            row_index,
            key_column,
            f"Primary key '{key_column}' is missing",
            "Completeness",
            "Critical",
            "Every record must have a non-empty primary identifier.",
        )

    valid_key_mask = (
        identifiers.notna()
        & identifiers.ne("").fillna(False)
    )

    duplicate_mask = (
        valid_key_mask
        & identifiers.duplicated(keep=False)
    )

    if not duplicate_mask.any():
        return

    duplicate_keys = identifiers[duplicate_mask].unique()

    # Customers: compare identity fields and all other columns.
    if dataset_name == "customers":
        check_customer_duplicates(
            df, duplicate_keys, identifiers, issues
        )
        return

    # Products: retain the current record and flag '(old)' rows
    # only when exactly one non-'(old)' record exists.
    if dataset_name == "products" and "product_name" in df.columns:
        product_names = (
            df["product_name"].astype("string").str.strip()
        )

        for key_value in duplicate_keys:
            group_mask = identifiers.eq(key_value).fillna(False)
            group_indices = list(df.index[group_mask])

            old_indices = [
                idx for idx in group_indices
                if pd.notna(product_names.at[idx])
                and "(old)" in product_names.at[idx].lower()
            ]

            current_indices = [
                idx for idx in group_indices
                if pd.notna(product_names.at[idx])
                and "(old)" not in product_names.at[idx].lower()
            ]

            if len(current_indices) == 1 and len(old_indices) >= 1:
                for row_index in old_indices:
                    add_issue(
                        issues,
                        dataset_name,
                        df,
                        row_index,
                        key_column,
                        (
                            "Historical product duplicate marked '(old)' "
                            f"for product ID {key_value}"
                        ),
                        "Uniqueness",
                        "Critical",
                        (
                            "Retain the single product record without "
                            "'(old)' and quarantine the historical record, "
                            "subject to the documented assignment assumption."
                        ),
                    )
            else:
                for row_index in group_indices:
                    add_issue(
                        issues,
                        dataset_name,
                        df,
                        row_index,
                        key_column,
                        f"Duplicate primary-key value: {key_value}",
                        "Uniqueness",
                        "Critical",
                        (
                            "Investigate duplicate records and define "
                            "a deterministic resolution rule before loading."
                        ),
                    )

        return

    # Orders and payments: keep the first copy of an exact duplicate.
    # If any row differs within the same key group, flag every row.
    if dataset_name in {"orders", "payments"}:
        for key_value in duplicate_keys:
            group_mask = identifiers.eq(key_value).fillna(False)
            group_indices = list(df.index[group_mask])
            group_df = df.loc[group_indices]

            exact_copies = (
                group_df.drop_duplicates().shape[0] == 1
            )

            if exact_copies:
                for row_index in group_indices[1:]:
                    add_issue(
                        issues,
                        dataset_name,
                        df,
                        row_index,
                        key_column,
                        (
                            f"Exact duplicate record; additional copy "
                            f"of primary key {key_value}"
                        ),
                        "Uniqueness",
                        "Critical",
                        (
                            "Keep the first exact copy and quarantine "
                            "additional identical records."
                        ),
                    )
            else:
                for row_index in group_indices:
                    add_issue(
                        issues,
                        dataset_name,
                        df,
                        row_index,
                        key_column,
                        (
                            f"Conflicting duplicate primary-key value: "
                            f"{key_value}"
                        ),
                        "Uniqueness",
                        "Critical",
                        (
                            "Investigate conflicting records and define "
                            "a deterministic correction rule before loading."
                        ),
                    )

        return

    # Stores: preserve the existing rule of flagging all duplicates.
    for row_index in df.index[duplicate_mask]:
        add_issue(
            issues,
            dataset_name,
            df,
            row_index,
            key_column,
            (
                "Duplicate primary-key value: "
                f"{df.at[row_index, key_column]}"
            ),
            "Uniqueness",
            "Critical",
            (
                "Investigate duplicate records and define a "
                "deterministic deduplication or correction rule "
                "before loading curated data."
            ),
        )


def check_referential_integrity(
    datasets: dict,
    issues: list,
):
    """Check that foreign-key references resolve to source records."""
    for child_dataset, relationships in FOREIGN_KEYS.items():
        child_df = datasets[child_dataset]

        for (
            child_column,
            (parent_dataset, parent_column),
        ) in relationships.items():

            if child_column not in child_df.columns:
                issues.append({
                    "file": child_dataset,
                    "source_row": None,
                    "record_id": None,
                    "column": child_column,
                    "problem": (
                        f"Required relationship column '{child_column}' "
                        "is missing"
                    ),
                    "quality_dimension": "Referential Integrity",
                    "severity": "High",
                    "proposed_rule": (
                        "Confirm the expected source schema and retain "
                        "the relationship column required for validation."
                    ),
                })
                continue

            parent_df = datasets[parent_dataset]

            if parent_column not in parent_df.columns:
                issues.append({
                    "file": parent_dataset,
                    "source_row": None,
                    "record_id": None,
                    "column": parent_column,
                    "problem": (
                        f"Referenced key column '{parent_column}' "
                        "is missing"
                    ),
                    "quality_dimension": "Referential Integrity",
                    "severity": "Critical",
                    "proposed_rule": (
                        "Ensure the parent dataset contains its expected "
                        "key column before validating relationships."
                    ),
                })
                continue

            parent_keys = set(
                normalize_values(parent_df[parent_column])
                .dropna()
                .loc[lambda values: values.ne("")]
                .tolist()
            )

            child_keys = normalize_values(child_df[child_column])

            invalid_mask = (
                child_keys.notna()
                & child_keys.ne("").fillna(False)
                & ~child_keys.isin(parent_keys)
            )

            for row_index in child_df.index[invalid_mask]:
                add_issue(
                    issues,
                    child_dataset,
                    child_df,
                    row_index,
                    child_column,
                    (
                        f"Referenced {parent_dataset} key not found: "
                        f"{child_df.at[row_index, child_column]}"
                    ),
                    "Referential Integrity",
                    "High",
                    (
                        f"Ensure '{child_column}' matches an existing "
                        f"'{parent_dataset}.{parent_column}' value, or "
                        "document an approved exception."
                    ),
                )


def run_technical_checks(datasets: dict) -> list:
    """Run technical DQ checks against the five extracted DataFrames."""
    expected_datasets = set(PRIMARY_KEYS)
    actual_datasets = set(datasets)

    if actual_datasets != expected_datasets:
        raise ValueError(
            "Unexpected dataset names. "
            f"Missing: {sorted(expected_datasets - actual_datasets)}; "
            f"extra: {sorted(actual_datasets - expected_datasets)}"
        )

    issues = []

    for dataset_name in PRIMARY_KEYS:
        df = datasets[dataset_name]

        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"Expected a DataFrame for '{dataset_name}'"
            )

        check_missing_values(dataset_name, df, issues)
        check_primary_keys(dataset_name, df, issues)

    check_referential_integrity(datasets, issues)

    logger.info(
        "[DQ] Technical validation completed: %d findings",
        len(issues),
    )

    return issues


def validate_extracted_json(
    orders_json: str,
    payments_json: str,
    customers_json: str,
    products_json: str,
    stores_json: str,
) -> dict:
    """Parse all five inputs and run technical data-quality checks."""
    datasets = parse_json_datasets(
        orders_json,
        payments_json,
        customers_json,
        products_json,
        stores_json,
    )

    issues = run_technical_checks(datasets)

    return {
        "datasets": datasets,
        "technical_issues": issues,
        "dataset_counts": {
            name: int(len(df))
            for name, df in datasets.items()
        },
    }
