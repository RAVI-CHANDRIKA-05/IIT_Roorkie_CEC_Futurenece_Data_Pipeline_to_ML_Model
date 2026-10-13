
from pathlib import Path
import pandas as pd

# Project paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "include" / "data" / "nammamart"
OUTPUT_FILE = PROJECT_DIR / "assignment" / "data_issues_log.csv"

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "increment": "orders_2026-10-01_increment.csv",
}

data = {}
issues = []


def add_issue(file_name, row_index, record_id, column, problem,
              dimension, severity, proposed_rule):
    """Add a finding to the separate issue log."""
    issues.append({
        "file": file_name,
        "source_row": row_index + 2 if row_index is not None else None,
        "record_id": record_id,
        "column": column,
        "problem": problem,
        "quality_dimension": dimension,
        "severity": severity,
        "proposed_rule": proposed_rule,
    })


def normalized(series):
    """Normalize values for comparisons without changing source data."""
    return series.astype("string").str.strip()


def load_csvs():
    for key, filename in FILES.items():
        path = DATA_DIR / filename

        if not path.exists():
            print(f"WARNING: File not found: {path}")
            continue

        df = pd.read_csv(path)
        data[key] = df
        print(f"Loaded {filename}: {len(df)} rows, {len(df.columns)} columns")


def check_duplicate_ids():
    print("\n--- DUPLICATE IDENTIFIERS ---")

    key_columns = {
        "orders": "order_id",
        "payments": "payment_id",
        "customers": "customer_id",
        "products": "product_id",
        "stores": "store_id",
        "increment": "order_id",
    }

    for name, key_col in key_columns.items():
        if name not in data or key_col not in data[name].columns:
            continue

        df = data[name]
        ids = normalized(df[key_col])
        duplicate_mask = ids.notna() & ids.ne("") & ids.duplicated(
            keep=False
        )

        duplicate_rows = df[duplicate_mask]
        print(
            f"{name}: {duplicate_mask.sum()} rows with duplicate "
            f"{key_col} values; "
            f"{ids[duplicate_mask].nunique()} distinct duplicated IDs"
        )

        for idx in duplicate_rows.index:
            add_issue(
                name,
                idx,
                df.at[idx, key_col],
                key_col,
                "Identifier appears more than once",
                "Uniqueness",
                "High",
                "Investigate duplicate records and define a deterministic "
                "deduplication rule before loading curated data.",
            )


def check_missing_values():
    print("\n--- MISSING VALUES ---")

    for name, df in data.items():
        for column in df.columns:
            missing_mask = df[column].isna()

            if df[column].dtype == "object" or pd.api.types.is_string_dtype(
                df[column]
            ):
                missing_mask = missing_mask | (
                    df[column].astype("string").str.strip().eq("")
                )

            count = int(missing_mask.sum())

            if count:
                print(f"{name}.{column}: {count} missing values")

            for idx in df.index[missing_mask]:
                record_id = None

                for possible_key in [
                    "order_id", "payment_id", "customer_id",
                    "product_id", "store_id"
                ]:
                    if possible_key in df.columns:
                        record_id = df.at[idx, possible_key]
                        break

                add_issue(
                    name,
                    idx,
                    record_id,
                    column,
                    "Required or potentially important value is missing",
                    "Completeness",
                    "Medium",
                    "Define whether the field is mandatory for this record "
                    "type; quarantine or retain with a documented exception.",
                )


def check_order_values():
    print("\n--- ORDER VALIDATION ---")

    for name in ["orders", "increment"]:
        if name not in data:
            continue

        df = data[name]

        if "quantity" in df.columns:
            quantity = pd.to_numeric(df["quantity"], errors="coerce")

            for idx in df.index[quantity.isna() | (quantity <= 0)]:
                add_issue(
                    name, idx,
                    df.at[idx, "order_id"] if "order_id" in df.columns else None,
                    "quantity",
                    f"Invalid quantity: {df.at[idx, 'quantity']}",
                    "Validity",
                    "High",
                    "Require a positive numeric quantity; investigate returns "
                    "or corrections before applying exceptions.",
                )

        if "unit_price" in df.columns:
            price = pd.to_numeric(df["unit_price"], errors="coerce")

            for idx in df.index[price.isna() | (price < 0)]:
                add_issue(
                    name, idx,
                    df.at[idx, "order_id"] if "order_id" in df.columns else None,
                    "unit_price",
                    f"Invalid unit price: {df.at[idx, 'unit_price']}",
                    "Validity",
                    "High",
                    "Require a valid non-negative numeric unit price.",
                )

        if "discount_pct" in df.columns:
            discount = pd.to_numeric(df["discount_pct"], errors="coerce")

            for idx in df.index[
                discount.isna() | (discount < 0) | (discount > 100)
            ]:
                add_issue(
                    name, idx,
                    df.at[idx, "order_id"] if "order_id" in df.columns else None,
                    "discount_pct",
                    f"Discount outside 0–100: {df.at[idx, 'discount_pct']}",
                    "Validity",
                    "High",
                    "Confirm the discount unit and require a percentage "
                    "between 0 and 100 unless a documented exception applies.",
                )

        if "order_amount" in df.columns:
            amount = pd.to_numeric(df["order_amount"], errors="coerce")

            for idx in df.index[amount.isna() | (amount < 0)]:
                add_issue(
                    name, idx,
                    df.at[idx, "order_id"] if "order_id" in df.columns else None,
                    "order_amount",
                    f"Invalid or negative order amount: {df.at[idx, 'order_amount']}",
                    "Validity",
                    "High",
                    "Reconcile the amount against the source transaction and "
                    "documented return/cancellation rules.",
                )

        if "delivery_minutes" in df.columns and "status" in df.columns:
            status = normalized(df["status"]).str.upper()
            delivery = pd.to_numeric(
                df["delivery_minutes"], errors="coerce"
            )

            mask = status.eq("DELIVERED") & (
                delivery.isna() | (delivery <= 0)
            )

            for idx in df.index[mask]:
                add_issue(
                    name, idx,
                    df.at[idx, "order_id"] if "order_id" in df.columns else None,
                    "delivery_minutes",
                    "Delivered order has missing or non-positive delivery time",
                    "Completeness / Validity",
                    "Medium",
                    "Require a valid delivery time for delivered orders; "
                    "do not apply this rule to undelivered orders.",
                )

        # Check the arithmetic only where inputs are valid.
        required = {
            "quantity", "unit_price", "discount_pct", "order_amount"
        }

        if required.issubset(df.columns):
            quantity = pd.to_numeric(df["quantity"], errors="coerce")
            price = pd.to_numeric(df["unit_price"], errors="coerce")
            discount = pd.to_numeric(df["discount_pct"], errors="coerce")
            amount = pd.to_numeric(df["order_amount"], errors="coerce")

            valid = (
                quantity.gt(0)
                & price.ge(0)
                & discount.between(0, 100)
                & amount.notna()
            )

            expected = quantity * price * (1 - discount / 100)
            mismatch = valid & ((amount - expected).abs() > 0.02)

            print(
                f"{name}: {int(mismatch.sum())} rows with order amounts "
                "that differ from quantity × unit price × (1 − discount)"
            )

            for idx in df.index[mismatch]:
                add_issue(
                    name, idx,
                    df.at[idx, "order_id"] if "order_id" in df.columns else None,
                    "order_amount",
                    f"Actual={amount.at[idx]:.2f}; "
                    f"calculated={expected.at[idx]:.2f}",
                    "Consistency",
                    "Medium",
                    "Confirm the business formula, tax/fee treatment, and "
                    "rounding policy before rejecting the record.",
                )


def check_product_values():
    if "products" not in data:
        return

    print("\n--- PRODUCT VALIDATION ---")
    df = data["products"]

    for column in ["mrp", "cost_price"]:
        if column not in df.columns:
            continue

        values = pd.to_numeric(df[column], errors="coerce")
        mask = values.isna() | (values < 0)

        for idx in df.index[mask]:
            add_issue(
                "products", idx,
                df.at[idx, "product_id"] if "product_id" in df.columns else None,
                column,
                f"Invalid or negative value: {df.at[idx, column]}",
                "Validity",
                "High",
                "Require a valid non-negative numeric value and verify it "
                "against the product master.",
            )

    if {"mrp", "cost_price"}.issubset(df.columns):
        mrp = pd.to_numeric(df["mrp"], errors="coerce")
        cost = pd.to_numeric(df["cost_price"], errors="coerce")
        mask = mrp.notna() & cost.notna() & (cost > mrp)

        print(f"Products with cost_price greater than MRP: {int(mask.sum())}")

        for idx in df.index[mask]:
            add_issue(
                "products", idx,
                df.at[idx, "product_id"] if "product_id" in df.columns else None,
                "cost_price / mrp",
                f"Cost price {cost.at[idx]} exceeds MRP {mrp.at[idx]}",
                "Consistency",
                "Medium",
                "Verify the values and business rule; do not assume this is "
                "invalid until the pricing policy is confirmed.",
            )


def check_referential_integrity():
    print("\n--- REFERENTIAL INTEGRITY ---")

    relationships = [
        ("orders", "customer_id", "customers", "customer_id"),
        ("orders", "product_id", "products", "product_id"),
        ("orders", "store_id", "stores", "store_id"),
        ("increment", "customer_id", "customers", "customer_id"),
        ("increment", "product_id", "products", "product_id"),
        ("increment", "store_id", "stores", "store_id"),
        ("payments", "order_id", "orders", "order_id"),
    ]

    for child_name, child_col, parent_name, parent_col in relationships:
        if child_name not in data or parent_name not in data:
            continue

        child = data[child_name]
        parent = data[parent_name]

        if child_col not in child.columns or parent_col not in parent.columns:
            print(
                f"Skipped {child_name}.{child_col} -> "
                f"{parent_name}.{parent_col}: column not found"
            )
            continue

        parent_ids = set(normalized(parent[parent_col]).dropna())
        child_ids = normalized(child[child_col])

        # Blank foreign keys are reported by the missing-value check.
        orphan_mask = (
            child_ids.notna()
            & child_ids.ne("")
            & ~child_ids.isin(parent_ids)
        )

        print(
            f"{child_name}.{child_col} -> {parent_name}.{parent_col}: "
            f"{int(orphan_mask.sum())} unmatched references"
        )

        for idx in child.index[orphan_mask]:
            add_issue(
                child_name, idx,
                child.at[idx, child_col],
                child_col,
                f"Value {child.at[idx, child_col]} not found in "
                f"{parent_name}.{parent_col}",
                "Referential integrity",
                "High",
                "Investigate the missing parent record or incorrect key; "
                "quarantine unresolved records from curated outputs.",
            )


def main():
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")

    load_csvs()

    check_duplicate_ids()
    check_missing_values()
    check_order_values()
    check_product_values()
    check_referential_integrity()

    issues_df = pd.DataFrame(issues)

    if not issues_df.empty:
        issues_df = issues_df.sort_values(
            ["severity", "file", "source_row"],
            na_position="last"
        )

    issues_df.to_csv(OUTPUT_FILE, index=False)

    print("\n--- ISSUE LOG SUMMARY ---")
    print(f"Total findings: {len(issues_df)}")

    if not issues_df.empty:
        print("\nFindings by file:")
        print(issues_df.groupby("file").size().to_string())

        print("\nFindings by severity:")
        print(issues_df.groupby("severity").size().to_string())

        print("\nFindings by quality dimension:")
        print(issues_df.groupby("quality_dimension").size().to_string())

    print(f"\nIssue log saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
