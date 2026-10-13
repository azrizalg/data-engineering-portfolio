from pathlib import Path
from datetime import datetime, timezone
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



def start_etl_run(conn, pipeline_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_runs (
                pipeline_name,
                started_at,
                status
            )
            VALUES (%s, %s, %s)
            RETURNING run_id
            """,
            (
                pipeline_name,
                datetime.now(timezone.utc),
                "RUNNING",
            ),
        )

        run_id = cur.fetchone()[0]

    conn.commit()

    return run_id

def finish_etl_run(
    conn,
    run_id: int,
    status: str,
    rows_read: int,
    rows_loaded: int,
    rows_rejected: int,
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_runs
            SET
                finished_at = %s,
                status = %s,
                rows_read = %s,
                rows_loaded = %s,
                rows_rejected = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (
                datetime.now(timezone.utc),
                status,
                rows_read,
                rows_loaded,
                rows_rejected,
                error_message,
                run_id,
            ),
        )

    conn.commit()
    
    
def process_batches(
    input_path: Path,
    batch_size: int = 100,
) -> None:

    lf = (
        pl.scan_csv(input_path)
        .with_columns(
            (
                pl.col("customer_id").is_not_null()
                & pl.col("name").is_not_null()
                & pl.col("age").is_between(18, 100)
                & pl.col("city").is_not_null()
            ).alias("_is_valid")
        )
    )

    total_input = 0
    total_valid = 0
    total_invalid = 0
    first_rejected_batch = True

    connection = psycopg.connect(**DB_CONFIG)
    run_id = None
    
    
    try:
        run_id = start_etl_run(connection, "customers_csv_to_postgres")
        for batch in lf.collect_batches(
            chunk_size=batch_size
        ):

            total_input += len(batch)

            valid = (
                batch
                .filter(pl.col("_is_valid"))
                .drop("_is_valid")
            )

            invalid = (
                batch
                .filter(~pl.col("_is_valid"))
                .drop("_is_valid")
            )

            if len(valid) > 0:
                load_batch(connection, valid)
                total_valid += len(valid)
                
            if len(invalid) > 0:
                save_rejected(
                    invalid,
                    append=not first_rejected_batch,
                )
                first_rejected_batch = False
                total_invalid += len(invalid)

            logger.info(
                "Batch processed | input=%s | valid=%s | invalid=%s",
                len(batch),
                len(valid),
                len(invalid),
            )
            
            
    
        finish_etl_run(
                connection,
                run_id,
                "SUCCESS",
                total_input,
                total_valid,
                total_invalid,
            )
    except Exception as e:
        if run_id is not None:
            finish_etl_run(
                connection,
                run_id,
                "FAILED",
                total_input,
                total_valid,
                total_invalid,
                str(e),
            )

        
        raise
    finally:
        connection.close()

    logger.info("Total input: %s", total_input)
    logger.info("Total valid: %s", total_valid)
    logger.info("Total invalid: %s", total_invalid)

def save_rejected(
    invalid: pl.DataFrame,
    append: bool = False,
) -> None:

    REJECTED.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    mode = "ab" if append else "wb"

    with open(REJECTED, mode) as f:
        invalid.write_csv(
            f,
            include_header=not append,
        )

    logger.info(
        "Saved %s rejected records",
        len(invalid),
    )


def load_batch(
    connection: psycopg.Connection,
    batch: pl.DataFrame,
) -> None:

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
    
    
def main():
    logger.info("=== Incremental ETL started ===")

    process_batches(INPUT)

    logger.info("=== Incremental ETL completed ===")


if __name__ == "__main__":
    main()