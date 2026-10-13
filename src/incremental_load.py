import polars as pl
import psycopg


df = pl.read_csv("data/customers_new.csv")

connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="de_lab",
    user="de_user",
    password="de_password",
)

with connection.cursor() as cursor:
    # Clear previous staging data
    cursor.execute("TRUNCATE TABLE customers_staging")

    # Load new data into staging
    with cursor.copy(
        """
        COPY customers_staging (customer_id, name, age, city)
        FROM STDIN
        """
    ) as copy:
        for row in df.iter_rows():
            copy.write_row(row)

    # Move only new customers into the final table
    cursor.execute(
        """
        INSERT INTO customers (customer_id, name, age, city)
        SELECT customer_id, name, age, city
        FROM customers_staging
        ON CONFLICT (customer_id) DO NOTHING
        """
    )

connection.commit()
connection.close()

print(f"Processed {len(df):,} rows")