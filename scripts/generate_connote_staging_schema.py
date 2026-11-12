#!/usr/bin/env python3
"""Generate the PostgreSQL staging-table DDL from connote field metadata."""

import json
import re
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
METADATA_FILE = PROJECT_ROOT / "data" /  "metadata" /"column_t_connote_data.json"
print(METADATA_FILE)
OUTPUT_FILE = PROJECT_ROOT / "src" / "connote_report" / "schema.sql"

TABLE_NAME = "t_connote_staging"

TYPE_MAPPING = {
    "varchar": "TEXT",
    "timestamp": "TIMESTAMPTZ",
    "big_integer": "BIGINT",
    "decimal": "NUMERIC",
    "float": "DOUBLE PRECISION",
    "integer": "INTEGER",
}

PIPELINE_COLUMNS = [
    ("staging_id", "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"),
    ("report_param_id", "BIGINT NOT NULL"),
    ("batch_run_id", "BIGINT NOT NULL"),
    ("csv_batch_number", "INTEGER"),
    ("staged_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
]

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(name: str) -> str:
    """Quote a validated PostgreSQL identifier."""
    if not IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def load_metadata(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as file:
        columns = json.load(file)

    if not isinstance(columns, list):
        raise ValueError("Metadata must be a JSON array of field objects.")

    seen_names = set()
    for index, column in enumerate(columns):
        if not isinstance(column, dict):
            raise ValueError(f"Metadata item {index} is not an object.")

        name = column.get("name")
        source_type = column.get("type")

        if not isinstance(name, str) or not IDENTIFIER_PATTERN.fullmatch(name):
            raise ValueError(f"Invalid or missing field name at item {index}: {name!r}")

        if name.lower() in seen_names:
            raise ValueError(f"Duplicate field name (case-insensitive): {name!r}")
        seen_names.add(name.lower())

        if not isinstance(source_type, str) or source_type not in TYPE_MAPPING:
            raise ValueError(
                f"Unsupported or missing type for field {name!r}: {source_type!r}"
            )

    pipeline_names = {name.lower() for name, _ in PIPELINE_COLUMNS}
    collisions = seen_names & pipeline_names
    if collisions:
        raise ValueError(
            "Source fields collide with pipeline metadata columns: "
            + ", ".join(sorted(collisions))
        )

    if "id_connote_data" not in seen_names:
        raise ValueError("Required canonical identifier 'id_connote_data' is missing.")

    return columns


def build_schema_sql(columns: list[dict]) -> str:
    definitions = []

    for column in columns:
        name = quote_identifier(column["name"])
        postgres_type = TYPE_MAPPING[column["type"]]
        definitions.append(f"    {name} {postgres_type}")

    for name, definition in PIPELINE_COLUMNS:
        definitions.append(f"    {quote_identifier(name)} {definition}")

    constraint_name = quote_identifier("uq_connote_staging_report_connote")
    definitions.append(
        f"    CONSTRAINT {constraint_name} UNIQUE "
        f"({quote_identifier('report_param_id')}, "
        f"{quote_identifier('id_connote_data')})"
    )

    table_sql = (
        f"CREATE TABLE {quote_identifier(TABLE_NAME)} (\n"
        + ",\n".join(definitions)
        + "\n);\n"
    )

    index_sql = "CREATE INDEX idx_connote_staging_csv_batch ON t_connote_staging (report_param_id, csv_batch_number);"

    return table_sql + "\n" + index_sql


def main() -> None:
    if not METADATA_FILE.is_file():
        raise FileNotFoundError(
            f"Metadata file not found: {METADATA_FILE}\n"
            "Place column_t_connote_data.json in the project's data/ directory."
        )

    columns = load_metadata(METADATA_FILE)
    sql = build_schema_sql(columns)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(sql, encoding="utf-8")

    type_counts = Counter(column["type"] for column in columns)
    print(f"Source fields: {len(columns)}")
    print(f"Pipeline metadata fields: {len(PIPELINE_COLUMNS)}")
    print(f"Total table columns: {len(columns) + len(PIPELINE_COLUMNS)}")
    print("Source type counts:")
    for source_type, count in sorted(type_counts.items()):
        print(f"  {source_type}: {count}")
    print(f"Generated SQL: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
