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




def extract_and_validate(
    input_path: Path,
) -> tuple[pl.DataFrame, pl.DataFrame]:

    lf = pl.scan_csv(input_path)

    condition = (
        pl.col("customer_id").is_not_null()
        & pl.col("name").is_not_null()
        & pl.col("age").is_between(18, 100)
        & pl.col("city").is_not_null()
    )

    df = (
        lf
        .with_columns(
            condition.alias("_is_valid")
        )
        .collect(engine="streaming")
    )

    valid = df.filter(pl.col("_is_valid")).drop("_is_valid")
    invalid = df.filter(~pl.col("_is_valid")).drop("_is_valid")

    logger.info("Input: %s rows", len(df))
    logger.info("Valid: %s rows", len(valid))
    logger.warning("Invalid: %s rows", len(invalid))

    return valid, invalid


def save_rejected(invalid: pl.DataFrame) -> None:
    REJECTED.parent.mkdir(parents=True, exist_ok=True)
    invalid.write_csv(REJECTED)
    logger.info("Rejected records: %s", len(invalid))


def load(valid: pl.DataFrame, batch_size: int = 100_000) -> None:
    connection = psycopg.connect(**DB_CONFIG)

    try:
        for batch in valid.iter_slices(n_rows=batch_size):
            
            logger.info(
                "Processing batch: %s rows",
                len(batch),
            )

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

            logger.info(
                "Batch committed: %s rows",
                len(batch),
            )

    except Exception:
        logger.exception("Database load failed")
        raise

    finally:
        connection.close()

def main():
    print("=== Incremental ETL ===")

    valid, invalid = extract_and_validate(INPUT)

    save_rejected(invalid)

    load(valid)

    print("=== ETL completed ===")


if __name__ == "__main__":
    main()