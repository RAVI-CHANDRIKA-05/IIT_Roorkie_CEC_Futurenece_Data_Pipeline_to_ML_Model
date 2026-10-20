
from pathlib import Path
import pandas as pd

# Project paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "include" / "data" / "nammamart"

BASE_FILE = DATA_DIR / "orders.csv"
INCREMENT_FILE = DATA_DIR / "orders_2026-10-01_increment.csv"

UPSERT_NOTE = (
    "Use Upsert - inserts new records and updates existing records "
    "in the target table."
)


def main():
    # 1. Check whether source files exist
    for path in [BASE_FILE, INCREMENT_FILE]:
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

    # 2. Load files as strings for consistent comparisons
    original = pd.read_csv(BASE_FILE, dtype=str).fillna("")
    increment = pd.read_csv(INCREMENT_FILE, dtype=str).fillna("")

    # 3. Validate required key column
    for name, df in [
        ("orders.csv", original),
        ("increment file", increment),
    ]:
        if "order_id" not in df.columns:
            raise ValueError(f"'order_id' column missing in {name}")

    original_ids = original["order_id"].str.strip()
    increment_ids = increment["order_id"].str.strip()

    print("\n--- INCREMENTAL ORDER ANALYSIS ---")
    print(f"Increment rows: {len(increment)}")

    # 4. Check blank order IDs
    blank_ids = increment_ids.eq("").sum()
    print(f"Increment rows with blank order_id: {int(blank_ids)}")

    if blank_ids:
        print(
            "CAUTION: Rows with blank order_id cannot be reliably "
            "matched for an upsert."
        )

    # 5. Check duplicate order IDs within increment file

    increment_duplicates = (
        increment_ids.ne("")
        & increment_ids.duplicated(keep=False)
    )

    print(
        "Duplicate order ID rows within increment file: "
        f"{int(increment_duplicates.sum())}"
    )

    # 6. Identify existing and new order IDs
    original_id_set = set(original_ids[original_ids.ne("")])
    valid_ids = increment_ids.ne("")

    existing_mask = (
        valid_ids & increment_ids.isin(original_id_set)
    )
    new_mask = (
        valid_ids & ~increment_ids.isin(original_id_set)
    )

    print("\n--- INCREMENT SUMMARY ---")
    print(f"Rows with existing order IDs: {int(existing_mask.sum())}")
    print(
        "Distinct existing order IDs: "
        f"{increment_ids[existing_mask].nunique()}"
    )
    print(f"Rows with new order IDs: {int(new_mask.sum())}")
    print(
        "Distinct new order IDs: "
        f"{increment_ids[new_mask].nunique()}"
    )

    # 7. Compare existing orders and report changed fields

    shared_columns = [
        col for col in increment.columns
        if col in original.columns and col != "order_id"
    ]

    # Group original records by order ID for comparison
    original_groups = {
        key: group
        for key, group in original.assign(
            _normalized_id=original_ids
        ).groupby("_normalized_id", sort=False)
        if key != ""
    }

    orders_compared = 0
    orders_changed = 0
    orders_unchanged = 0
    ambiguous_orders = 0
    total_changed_fields = 0

    for idx, new_row in increment.loc[existing_mask].iterrows():
        order_id = increment_ids.loc[idx]
        matches = original_groups.get(order_id)

        # Avoid choosing arbitrarily if an ID has multiple original matches
        if matches is None or len(matches) != 1:
            print(
                f"\nOrder ID: {order_id} - comparison skipped "
                "because the original match is ambiguous."
            )
            ambiguous_orders += 1
            continue

        old_row = matches.iloc[0]

        changed_columns = [
            col for col in shared_columns
            if str(old_row[col]).strip()
            != str(new_row[col]).strip()
        ]

        orders_compared += 1

        if changed_columns:
            orders_changed += 1
            total_changed_fields += len(changed_columns)

        else:
            orders_unchanged += 1

    # 8. Final comparison summary
    print("\n--- COMPARISON RESULTS ---")
    print(f"Orders compared (excluding ambiguous matches): {orders_compared}")
    print(f"Orders with changed fields: {orders_changed}")
    print(f"Orders with no field changes: {orders_unchanged}")
    print(f"Total changed fields across orders: {total_changed_fields}")
    print(f"Orders skipped due to ambiguous matches: {ambiguous_orders}")

    print(f"\nNOTE: {UPSERT_NOTE}")
    print("Analysis complete.")

if __name__ == "__main__":
    main()