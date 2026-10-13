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




def process_batches(
    input_path: Path,
    batch_size: int = 100_000,
) -> None:

    lf = pl.scan_csv(input_path)

    condition = (
        pl.col("customer_id").is_not_null()
        & pl.col("name").is_not_null()
        & pl.col("age").is_between(18, 100)
        & pl.col("city").is_not_null()
    )

    valid_lf = lf.filter(condition)
    invalid_lf = lf.filter(~condition)

    total_valid = 0
    total_invalid = 0

    for batch in valid_lf.collect_batches(
        chunk_size=batch_size
    ):
        total_valid += len(batch)

        load_batch(batch)

        logger.info(
            "Processed valid batch: %s rows",
            len(batch),
        )

    invalid = invalid_lf.collect(engine="streaming")

    total_invalid = len(invalid)

    save_rejected(invalid)

    logger.info("Total valid: %s rows", total_valid)
    logger.info("Total invalid: %s rows", total_invalid)


def save_rejected(invalid: pl.DataFrame) -> None:
    REJECTED.parent.mkdir(parents=True, exist_ok=True)
    invalid.write_csv(REJECTED)
    logger.info("Rejected records: %s", len(invalid))


def load_batch(batch: pl.DataFrame) -> None:
    connection = psycopg.connect(**DB_CONFIG)

    try:
        with connection.transaction():
            with connection.cursor() as cursor:

                cursor.execute(
                    "TRUNCATE TABLE customers_staging"
                )

                with cursor.copy(
                    """
                    COPY customers_staging
                        (customer_id, name, age, city)
                    FROM STDIN
                    """
                ) as copy:

                    for row in batch.iter_rows():
                        copy.write_row(row)

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
                    ON CONFLICT (customer_id)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        age = EXCLUDED.age,
                        city = EXCLUDED.city
                    """
                )

    except Exception:
        logger.exception("Batch load failed")
        raise

    finally:
        connection.close()

def main():
    logger.info("=== Incremental ETL started ===")

    process_batches(INPUT)

    logger.info("=== Incremental ETL completed ===")


if __name__ == "__main__":
    main()