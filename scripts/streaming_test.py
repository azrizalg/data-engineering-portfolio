import time
import polars as pl

INPUT = "data/customers_1m.csv"


start = time.perf_counter()

df = (
    pl.scan_csv(INPUT)
    # .filter(pl.col("age") >= 25)
    .collect(engine="streaming")
)

elapsed = time.perf_counter() - start

print(f"Rows after filter: {len(df):,}")
print(f"Read + filter time: {elapsed:.2f}s")
print(f"Estimated size: {df.estimated_size() / 1024 / 1024:.2f} MB")