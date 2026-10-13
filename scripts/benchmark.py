import time

import polars as pl
import psycopg


start = time.perf_counter()

df = pl.read_csv("data/customers_1m.csv")

print(f"Loaded {len(df):,} rows from CSV")

connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="de_lab",
    user="de_user",
    password="de_password",
)

with connection.cursor() as cursor:
    cursor.execute("TRUNCATE TABLE customers")

    for row in df.iter_rows():
        cursor.execute(
            """
            INSERT INTO customers (customer_id, name, age, city)
            VALUES (%s, %s, %s, %s)
            """,
            row,
        )

connection.commit()
connection.close()

elapsed = time.perf_counter() - start

print(f"Finished in {elapsed:.2f} seconds")