"""
Export dbt feature tables from DuckDB to local Parquet (for Feast) and MinIO (for demo).

After `dbt run` populates user_daily_features and user_rolling_features
in DuckDB, this script:
1. Writes local Parquet files under data/feast/ for Feast offline store
2. Copies to MinIO so they're visible in the MinIO console

Usage:
    python scripts/export_dbt_to_minio.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "data" / "dbt_dev.duckdb"
FEAST_DATA_DIR = PROJECT_ROOT / "data" / "feast"

MINIO_SETTINGS = {
    "s3_endpoint": "localhost:9000",
    "s3_access_key_id": "minioadmin",
    "s3_secret_access_key": "minioadmin",
    "s3_use_ssl": "false",
    "s3_url_style": "path",
}

EXPORTS = [
    ("user_daily_features", "user_daily_features.parquet", "s3://ml-feature-platform/delta/user_daily_features/data.parquet"),
    ("user_rolling_features", "user_rolling_features.parquet", "s3://ml-feature-platform/delta/user_rolling_features/data.parquet"),
]


def main():
    if not DUCKDB_PATH.exists():
        print(f"DuckDB database not found at {DUCKDB_PATH}")
        print("Run dbt first:  make dbt-run")
        return

    FEAST_DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    conn.execute("SET TimeZone='UTC';")  # ensure CAST(date AS TIMESTAMPTZ) = midnight UTC
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    for key, value in MINIO_SETTINGS.items():
        conn.execute(f"SET {key} = '{value}';")

    for table_name, local_filename, s3_path in EXPORTS:
        row_count = conn.execute(f"SELECT COUNT(*) FROM main.{table_name}").fetchone()[0]
        local_path = FEAST_DATA_DIR / local_filename

        # Write local copy for Feast — cast event_date to TIMESTAMPTZ so Feast's
        # dask offline store gets datetime.datetime objects, not datetime.date.
        print(f"Exporting {table_name} ({row_count} rows)")
        conn.execute(
            f"COPY (SELECT * REPLACE (CAST(event_date AS TIMESTAMPTZ) AS event_date) "
            f"FROM main.{table_name}) TO '{local_path}' (FORMAT PARQUET);"
        )
        print(f"  Local: {local_path}")

        # Write to MinIO for demo visibility
        try:
            conn.execute(
                f"COPY (SELECT * REPLACE (CAST(event_date AS TIMESTAMPTZ) AS event_date) "
                f"FROM main.{table_name}) TO '{s3_path}' (FORMAT PARQUET);"
            )
            print(f"  MinIO: {s3_path}")
        except Exception as e:
            print(f"  MinIO: skipped ({e})")

    conn.close()
    print(f"\nExport complete. Feast data at: {FEAST_DATA_DIR}/")


if __name__ == "__main__":
    main()
