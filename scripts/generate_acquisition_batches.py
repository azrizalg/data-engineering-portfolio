import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg
from dotenv import load_dotenv
from src.database.postgres_connection import get_postgres_connection

# ==================================================
# CONFIGURATION
# ==================================================

load_dotenv()



TIMEZONE = ZoneInfo("Asia/Jakarta")

START_DATE = datetime(2026, 1, 1, 0, 0, 0, tzinfo=TIMEZONE)
END_DATE = datetime(2026, 11, 1, 0, 0, 0, tzinfo=TIMEZONE)

BATCH_INTERVAL = timedelta(hours=1)


# ==================================================
# HELPER FUNCTIONS
# ==================================================

def to_unix_ms(dt: datetime) -> int:
    """Convert timezone-aware datetime to Unix milliseconds."""
    return int(dt.timestamp() * 1000)


# ==================================================
# BATCH GENERATOR
# ==================================================

def generate_batches():
    batches = []

    current_time = START_DATE

    while current_time < END_DATE:
        next_time = current_time + BATCH_INTERVAL

        batches.append((
            to_unix_ms(current_time),
            to_unix_ms(next_time),
        ))

        current_time = next_time

    return batches


# ==================================================
# DATABASE INSERTION
# ==================================================

def insert_batches(batches):
    query = """
        INSERT INTO t_acquisition_batch_hourly (
            awb_date_from,
            awb_date_to
        )
        VALUES (%s, %s)
        ON CONFLICT (awb_date_from, awb_date_to)
        DO NOTHING;
    """

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:

            cur.executemany(query, batches)

            inserted_count = cur.rowcount

            # Verify the actual hourly boundaries.
            start_ms = to_unix_ms(START_DATE)
            end_ms = to_unix_ms(END_DATE)

            cur.execute("""
                SELECT awb_date_from, awb_date_to
                FROM t_acquisition_batch_hourly
                WHERE awb_date_from >= %s
                  AND awb_date_to <= %s
            """, (start_ms, end_ms))

            existing_batches = set(cur.fetchall())
            expected_batches = set(batches)

            if existing_batches != expected_batches:
                raise RuntimeError(
                    "Batch verification failed. "
                    f"Expected: {len(expected_batches)}, "
                    f"Found: {len(existing_batches)}"
                )

    return inserted_count


# ==================================================
# MAIN
# ==================================================

def main():
    print("Generating hourly acquisition batches...")

    batches = generate_batches()

    print(f"Generated batches: {len(batches)}")

    assert len(batches) == 7296

    print("Inserting batches into PostgreSQL...")

    inserted_count = insert_batches(batches)

    print(f"Newly inserted: {inserted_count}")
    print(f"Total verified: {len(batches)}")
    print("Batch generation completed successfully.")


if __name__ == "__main__":
    main()