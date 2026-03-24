"""
Export dbt feature tables from DuckDB to MinIO as Parquet.

After `dbt run` populates user_daily_features and user_rolling_features
in DuckDB, this script copies them to MinIO so Feast can read them as
offline feature sources.

Usage:
    python scripts/export_dbt_to_minio.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

DBT_DUCKDB_PATH = Path("src/dbt_features/../../data/dbt_dev.duckdb")
# Resolve relative to project root
DUCKDB_PATH = Path(__file__).resolve().parent.parent / "data" / "dbt_dev.duckdb"

MINIO_SETTINGS = {
    "s3_endpoint": "localhost:9000",
    "s3_access_key_id": "minioadmin",
    "s3_secret_access_key": "minioadmin",
    "s3_use_ssl": "false",
    "s3_url_style": "path",
}

EXPORTS = [
    ("user_daily_features", "s3://ml-feature-platform/delta/user_daily_features/data.parquet"),
    ("user_rolling_features", "s3://ml-feature-platform/delta/user_rolling_features/data.parquet"),
]


def main():
    if not DUCKDB_PATH.exists():
        print(f"DuckDB database not found at {DUCKDB_PATH}")
        print("Run dbt first:  cd src/dbt_features && dbt run --profiles-dir .")
        return

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    for key, value in MINIO_SETTINGS.items():
        conn.execute(f"SET {key} = '{value}';")

    for table_name, s3_path in EXPORTS:
        row_count = conn.execute(f"SELECT COUNT(*) FROM main.{table_name}").fetchone()[0]
        print(f"Exporting {table_name} ({row_count} rows) → {s3_path}")
        conn.execute(f"COPY main.{table_name} TO '{s3_path}' (FORMAT PARQUET);")
        print(f"  Done.")

    conn.close()
    print(f"\nAll exports complete. Feast can now read from MinIO.")


if __name__ == "__main__":
    main()
