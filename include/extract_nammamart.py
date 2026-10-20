
"""
File: include/extract_nammamart.py

Purpose:
    Extracts data from the five NammaMart source CSV files:
    orders, payments, customers, products, and stores.

How it works:
    1. Identifies the source CSV using the dataset name.
    2. Reads the CSV file into a Pandas DataFrame.
    3. Trims whitespace from text fields.
    4. Standardises approved values and known aliases.
    5. Converts the extracted and standardised records into JSON.
    6. Returns the JSON string for downstream Airflow tasks.

Standardisation examples:
    - " upi "             -> "UPI"
    - "card"              -> "Card"
    - "cod"               -> "Cash on Delivery"
    - "cash on delivery"  -> "Cash on Delivery"
    - " delivered "       -> "DELIVERED"
    - " returned "        -> "RETURNED"

Important:
    Only known aliases are standardised. Unknown values such as
    "Cashh" remain unchanged so that data-quality validation can
    identify and quarantine them.

    The Airflow DAG controls task scheduling and parallel execution.
    Data quality validation, business transformations, and warehouse
    loading are handled by separate modules.
"""

import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)

DATA_DIR = Path("/usr/local/airflow/include/data/nammamart")

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders_incremental": "orders_2026-10-01_increment.csv",
}

# Approved canonical values and aliases for payment modes.
PAYMENT_MODE_ALIASES = {
    "UPI": "UPI",
    "CARD": "Card",
    "COD": "Cash on Delivery",
    "CASH ON DELIVERY": "Cash on Delivery",
    "CASH ON DELIVERY (COD)": "Cash on Delivery",
    "WALLET": "Wallet",
}

# Allowed order statuses.
ORDER_STATUS_ALIASES = {
    "DELIVERED": "DELIVERED",
    "CANCELLED": "CANCELLED",
    "CANCELED": "CANCELLED",
    "RETURNED": "RETURNED",
}

# Allowed payment statuses and their common formatting variations.
PAYMENT_STATUS_ALIASES = {
    "SUCCESS": "SUCCESS",
    "SUCCESSFUL": "SUCCESS",
    "COMPLETED": "SUCCESS",
    "FAILED": "FAILED",
    "PENDING": "PENDING",
    "REFUNDED": "REFUNDED",
}


def _trim_text_values(df: pd.DataFrame) -> pd.DataFrame:
    """Trim leading/trailing whitespace from text columns."""
    df = df.copy()

    for column in df.select_dtypes(include=["object", "string"]).columns:
        df[column] = df[column].map(
            lambda value: value.strip()
            if isinstance(value, str)
            else value
        )

    return df


def _standardise_alias(
    value,
    aliases: dict,
    uppercase_lookup: bool = True,
):
    """Map a known alias to its canonical value; preserve unknown values."""
    if not isinstance(value, str):
        return value

    lookup_value = value.upper() if uppercase_lookup else value

    return aliases.get(lookup_value, value)


def standardise_dataset(
    dataset_name: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply safe text cleanup and approved alias mapping to one dataset.

    Unknown values are deliberately preserved for DQ validation.
    """
    df = _trim_text_values(df)

    if dataset_name == "orders":
        if "payment_mode" in df.columns:
            df["payment_mode"] = df["payment_mode"].map(
                lambda value: _standardise_alias(
                    value,
                    PAYMENT_MODE_ALIASES,
                )
            )

        if "order_status" in df.columns:
            df["order_status"] = df["order_status"].map(
                lambda value: _standardise_alias(
                    value,
                    ORDER_STATUS_ALIASES,
                )
            )

    elif dataset_name == "payments":
        if "payment_mode" in df.columns:
            df["payment_mode"] = df["payment_mode"].map(
                lambda value: _standardise_alias(
                    value,
                    PAYMENT_MODE_ALIASES,
                )
            )

        if "payment_status" in df.columns:
            df["payment_status"] = df["payment_status"].map(
                lambda value: _standardise_alias(
                    value,
                    PAYMENT_STATUS_ALIASES,
                )
            )

    return df


def extract_dataset(dataset_name: str) -> str:
    """
    Read one NammaMart CSV, standardise approved values,
    and return the records as a JSON string for Airflow XCom.
    """
    if dataset_name not in SOURCE_FILES:
        raise ValueError(
            f"Unknown dataset: {dataset_name}. "
            f"Expected one of {list(SOURCE_FILES.keys())}"
        )

    file_path = DATA_DIR / SOURCE_FILES[dataset_name]

    if not file_path.is_file():
        raise FileNotFoundError(
            f"CSV file for {dataset_name} was not found: {file_path}"
        )

    df = pd.read_csv(file_path)

    original_count = len(df)

    # Standardise before the payload reaches DQ validation.
    standardisation_name = (
    "orders" if dataset_name == "orders_incremental"
    else dataset_name
    )
    df = standardise_dataset(standardisation_name, df)

    logger.info(
        "[Extract] %s: %d rows extracted and standardised from %s",
        dataset_name,
        original_count,
        file_path.name,
    )

    return df.to_json(
        orient="records",
        date_format="iso",
    )
