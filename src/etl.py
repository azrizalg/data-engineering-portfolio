import polars as pl

df = pl.read_csv("data/customers.csv")

print(df)

df = df.filter(pl.col("age") >= 25)

print("\nAfter transformation:")
print(df)
import polars as pl
import psycopg

# Extract
df = pl.read_csv("data/customers.csv")

# Transform
df = df.filter(pl.col("age") >= 25)

print("Data after transformation:")
print(df)

# Load
connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="de_lab",
    user="de_user",
    password="de_password",
)

with connection.cursor() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            city TEXT
        )
    """)

    for row in df.iter_rows():
        cursor.execute(
            """
            INSERT INTO customers (customer_id, name, age, city)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            row,
        )

connection.commit()
connection.close()

print("Data loaded into PostgreSQL!")
