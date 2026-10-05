from pathlib import Path

import polars as pl
import psycopg
import os
import logging
from dotenv import load_dotenv

# db needs
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


# logging needs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ],
)
logger = logging.getLogger(__name__)

INPUT = Path("data/customers_messy.csv")
REJECTED = Path("data/rejected/customers_rejected.csv")




def extract() -> pl.DataFrame:
    df = pl.read_csv(INPUT)
    logger.info("Extracted %s rows", len(df))
    return df


def validate(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    condition = (
        pl.col("customer_id").is_not_null()
        & pl.col("name").is_not_null()
        & pl.col("age").is_between(18, 100)
        & pl.col("city").is_not_null()
    )

    valid = df.filter(condition)
    invalid = df.filter(~condition)

    logger.info("Valid: %s rows", len(valid))
    logger.warning("Invalid: %s rows", len(invalid))

    return valid, invalid


def save_rejected(invalid: pl.DataFrame) -> None:
    REJECTED.parent.mkdir(parents=True, exist_ok=True)
    invalid.write_csv(REJECTED)
    logger.info("Rejected records: %s", len(invalid))


def load(valid: pl.DataFrame) -> None:
    connection = psycopg.connect(**DB_CONFIG)

    try:
        with connection.cursor() as cursor:

            # Start with an empty staging table
            cursor.execute("TRUNCATE TABLE customers_staging")

            # Bulk load valid records
            with cursor.copy(
                """
                COPY customers_staging
                    (customer_id, name, age, city)
                FROM STDIN
                """
            ) as copy:
                for row in valid.iter_rows():
                    copy.write_row(row)

            # Move staging → target
            cursor.execute(
                """
                INSERT INTO customers
                    (customer_id, name, age, city)
                SELECT
                    customer_id,
                    name,
                    age,
                    city
                FROM customers_staging
                ON CONFLICT (customer_id) DO NOTHING
                """
            )

        connection.commit()

    except Exception:
        connection.rollback()
        logger.exception("Database load failed")
        raise

    finally:
        connection.close()

    print(f"Loaded: {len(valid):,} valid rows")


def main():
    print("=== Incremental ETL ===")

    df = extract()

    valid, invalid = validate(df)

    save_rejected(invalid)

    load(valid)

    print("=== ETL completed ===")


if __name__ == "__main__":
    main()