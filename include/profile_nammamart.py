
from pathlib import Path
import pandas as pd

# Project paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "include" / "data" / "nammamart"
OUTPUT_FILE = PROJECT_DIR / "assignment" / "profile_output.txt"

FILES = [
    "orders.csv",
    "payments.csv",
    "customers.csv",
    "products.csv",
    "stores.csv",
    "orders_2026-10-01_increment.csv",
]

NUMERIC_COLUMNS = {
    "orders.csv": [
        "quantity", "unit_price", "discount_pct",
        "order_amount", "delivery_minutes",
    ],
    "payments.csv": ["amount_paid"],
    "customers.csv": [],
    "products.csv": ["mrp", "cost_price"],
    "stores.csv": ["capacity_orders_per_day"],
    "orders_2026-10-01_increment.csv": [
        "quantity", "unit_price", "discount_pct",
        "order_amount", "delivery_minutes",
    ],
}

DATE_COLUMNS = {
    "orders.csv": ["order_ts"],
    "payments.csv": ["payment_ts"],
    "customers.csv": ["signup_date"],
    "products.csv": ["launch_date"],
    "stores.csv": ["open_date"],
    "orders_2026-10-01_increment.csv": ["order_ts"],
}

KEY_COLUMNS = {
    "orders.csv": ["order_id"],
    "payments.csv": ["payment_id"],
    "customers.csv": ["customer_id"],
    "products.csv": ["product_id"],
    "stores.csv": ["store_id"],
    "orders_2026-10-01_increment.csv": ["order_id"],
}

CATEGORY_COLUMNS = {
    "orders.csv": ["order_status", "payment_mode", "channel"],
    "payments.csv": ["payment_mode", "payment_status"],
    "customers.csv": ["loyalty_tier", "is_active"],
    "products.csv": ["category", "sub_category", "brand", "unit",
                     "is_perishable"],
    "stores.csv": ["zone"],
    "orders_2026-10-01_increment.csv": [
        "order_status", "payment_mode", "channel"
    ],
}


def profile_file(filename):
    path = DATA_DIR / filename

    print("\n" + "=" * 80)
    print(f"FILE: {filename}")
    print("=" * 80)

    if not path.exists():
        print(f"ERROR: File not found: {path}")
        return

    # Read source values as supplied; do not overwrite or clean the CSV.
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Column names: {df.columns.tolist()}")

    # Empty strings and whitespace-only values count as missing.
    print("\nMissing / blank values:")
    for col in df.columns:
        missing = df[col].str.strip().eq("").sum()
        if missing:
            print(f"  {col}: {missing}")

    print("\nDuplicate key values:")
    for col in KEY_COLUMNS[filename]:
        if col in df.columns:
            key = df[col].str.strip()
            duplicate_rows = key.duplicated(keep=False) & key.ne("")
            print(
                f"  {col}: {duplicate_rows.sum()} rows with duplicate keys; "
                f"{key[duplicate_rows].nunique()} distinct duplicate keys"
            )

    print("\nNumeric ranges:")
    for col in NUMERIC_COLUMNS[filename]:
        if col not in df.columns:
            print(f"  {col}: COLUMN MISSING")
            continue

        values = pd.to_numeric(df[col].str.strip(), errors="coerce")
        invalid = values.isna() & df[col].str.strip().ne("")
        print(
            f"  {col}: min={values.min()}, max={values.max()}, "
            f"unparseable_nonblank={invalid.sum()}"
        )

    print("\nDate ranges:")
    for col in DATE_COLUMNS[filename]:
        if col not in df.columns:
            print(f"  {col}: COLUMN MISSING")
            continue

        values = pd.to_datetime(
            df[col].str.strip(), errors="coerce", utc=True
        )
        invalid = values.isna() & df[col].str.strip().ne("")
        print(
            f"  {col}: min={values.min()}, max={values.max()}, "
            f"unparseable_nonblank={invalid.sum()}"
        )

    print("\nDistinct category values:")
    for col in CATEGORY_COLUMNS[filename]:
        if col in df.columns:
            values = sorted(df[col].str.strip().unique().tolist())
            print(f"  {col}: {values}")

    return df



def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    import contextlib

    with open(OUTPUT_FILE, "w", encoding="utf-8") as output:
        with contextlib.redirect_stdout(output):
            print("NAMMAMART DATA PROFILING")
            print(f"Source directory: {DATA_DIR}")

            for filename in FILES:
                profile_file(filename)

            print("\nProfiling complete. Source files were not modified.")

    print(f"Profiling complete. Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
