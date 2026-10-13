from pathlib import Path

import polars as pl


INPUT = Path("data/customers_messy.csv")
REJECTED = Path("data/rejected/customers_rejected.csv")


df = pl.read_csv(INPUT)

print(f"Input rows: {len(df):,}")

condition = (
    pl.col("customer_id").is_not_null()
    & pl.col("name").is_not_null()
    & pl.col("age").is_between(18, 100)
    & pl.col("city").is_not_null()
)

valid = df.filter(condition)
invalid = df.filter(~condition)

print(f"Valid rows: {len(valid):,}")
print(f"Invalid rows: {len(invalid):,}")

# Save invalid records
REJECTED.parent.mkdir(parents=True, exist_ok=True)

invalid.write_csv(REJECTED)

print(f"Rejected records saved to: {REJECTED}")