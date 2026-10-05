from pathlib import Path
from datetime import datetime, timezone
import polars as pl
import psycopg
import os
import logging
from dotenv import load_dotenv
import time
import random
from dq_checks import (
    check_not_null,
    check_unique,
    dq_passed,
)

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

def get_last_successful_batch(
    conn,
    run_id: int,
) -> int:

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(batch_number), 0)
            FROM etl_batch_runs
            WHERE run_id = %s
              AND status = 'SUCCESS'
            """,
            (run_id,),
        )

        return cur.fetchone()[0]
    
def get_latest_run(
    conn,
    pipeline_name: str,
) -> tuple[int, str] | None:

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, status
            FROM etl_runs
            WHERE pipeline_name = %s
            ORDER BY run_id DESC
            LIMIT 1
            """,
            (pipeline_name,),
        )

        result = cur.fetchone()

    return result

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

def start_batch_run(
    conn,
    run_id: int,
    batch_number: int,
) -> int:

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_batch_runs (
                run_id,
                batch_number,
                started_at,
                status
            )
            VALUES (%s, %s, %s, %s)
            RETURNING batch_run_id
            """,
            (
                run_id,
                batch_number,
                datetime.now(timezone.utc),
                "RUNNING",
            ),
        )

        batch_run_id = cur.fetchone()[0]

    conn.commit()

    return batch_run_id

def finish_batch_run(
    conn,
    batch_run_id: int,
    status: str,
    rows_read: int,
    rows_loaded: int,
    rows_rejected: int,
    error_message: str | None = None,
) -> None:

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_batch_runs
            SET
                finished_at = %s,
                status = %s,
                rows_read = %s,
                rows_loaded = %s,
                rows_rejected = %s,
                error_message = %s
            WHERE batch_run_id = %s
            """,
            (
                datetime.now(timezone.utc),
                status,
                rows_read,
                rows_loaded,
                rows_rejected,
                error_message,
                batch_run_id,
            ),
        )

    conn.commit()
    
def process_batches(
    input_path: Path,
    batch_size: int = 100,
) -> None:
    
    pipeline_name = "customers_csv_to_postgres"

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
        # --------------------------------------------------
        # Check previous run
        # --------------------------------------------------
        latest_run = get_latest_run(
            connection,
            pipeline_name,
        )
        last_successful_batch = 0

        if latest_run is not None:
            previous_run_id, previous_status = latest_run

            if previous_status == "FAILED":
                last_successful_batch = get_last_successful_batch(
                    connection,
                    previous_run_id,
                )

            elif previous_status == "RUNNING":
                raise RuntimeError(
                    f"Pipeline already has a RUNNING execution: "
                    f"{previous_run_id}"
                )


        # --------------------------------------------------
        # Start new ETL run
        # --------------------------------------------------
        run_id = start_etl_run(connection, pipeline_name)
        logger.info(
            "ETL run started | run_id=%s | resume_from_batch=%s",
            run_id,
            last_successful_batch + 1,
        )
         
        # --------------------------------------------------
        # Process batches
        # --------------------------------------------------
        for batch_number, batch in enumerate(
            lf.collect_batches(chunk_size=batch_size),
            start=1,
        ):
            # Skip batches that were already successful
            # in the previous failed run.
            if batch_number <= last_successful_batch:
                logger.info(
                    "Skipping batch %s | already successful",
                    batch_number,
                )
                continue
            
            
            batch_run_id = start_batch_run(
                connection,
                run_id,
                batch_number,
            )
            
            
            rows_read = 0
            rows_loaded = 0
            rows_rejected = 0

            try:
                rows_read = len(batch)

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
                    run_with_retry( lambda: load_batch(connection, valid) )
                    # load_batch(connection, valid)
                    rows_loaded += len(valid)
                    
                
                if len(invalid) > 0:
                    save_rejected(
                        invalid,
                        append=not first_rejected_batch,
                    )
                    first_rejected_batch = False
                    rows_rejected += len(invalid)

                
                finish_batch_run(
                        connection,
                        batch_run_id,
                        "SUCCESS",
                        rows_read,
                        rows_loaded,
                        rows_rejected,
                    )

                total_input += rows_read
                total_valid += rows_loaded
                total_invalid += rows_rejected

                logger.info(
                    "Batch processed | batch=%s | input=%s | valid=%s | invalid=%s",
                    batch_number,
                    rows_read,
                    rows_loaded,
                    rows_rejected,
                )
            
            except Exception as e:

                finish_batch_run(
                    connection,
                    batch_run_id,
                    "FAILED",
                    rows_read,
                    rows_loaded,
                    rows_rejected,
                    str(e),
                )

                logger.exception(
                    "Batch failed | batch=%s",
                    batch_number,
                )

                raise
            
            
        # --------------------------------------------------
        # Entire ETL succeeded
        # --------------------------------------------------
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
        dq_results = [
            check_not_null(
                connection,
                "customers",
                "customer_id"
            ),
            check_unique(
                connection,
                "customers",
                "customer_id"
            ),
        ]

        if dq_passed(dq_results):
            print("Data quality checks passed")
        else:
            print("Data quality checks failed")

            for result in dq_results:
                if not result["passed"]:
                    logger.info(
                        f"DQ FAILED: {result['check_name']} "
                        f"({result['failed_rows']} failed rows)"
                    )

        logger.info("Data quality checks passed")

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
    

# untuk retry call operation
def run_with_retry(
    operation,
    max_attempts: int = 3,
    base_delay: int = 1,
) -> None:

    for attempt in range(1, max_attempts + 1):

        try:
            operation()
            return

        except psycopg.OperationalError as e:

            logger.warning(
                "Transient database error | attempt=%s/%s | error=%s",
                attempt,
                max_attempts,
                e,
            )

            if attempt == max_attempts:
                raise

            delay = base_delay * (2 ** (attempt - 1))
            jitter = random.uniform(0, delay)

            logger.warning(
                "Transient database error | "
                "attempt=%s/%s | retrying in %ss | error=%s",
                attempt,
                max_attempts,
                jitter,
                e,
            )

            time.sleep(jitter)

        
def main():
    
    logger.info("=== Incremental ETL started ===")

    process_batches(INPUT)

    logger.info("=== Incremental ETL completed ===")


if __name__ == "__main__":
    main()