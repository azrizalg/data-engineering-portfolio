from datetime import datetime, timezone
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

conn = psycopg.connect(**DB_CONFIG)
start_etl_run(conn, "Cihuy")