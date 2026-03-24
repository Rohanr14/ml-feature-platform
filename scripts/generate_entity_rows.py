"""
Generate entity rows for model training from dbt feature tables.

Reads user_daily_features from the dbt DuckDB database and produces
a Parquet file with (user_id, event_timestamp, has_anomaly) that the
training script uses for point-in-time Feast feature retrieval.

Usage:
    python scripts/generate_entity_rows.py
    python scripts/generate_entity_rows.py --output data/entity_rows.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "data" / "dbt_dev.duckdb"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "entity_rows.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate entity rows for training")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main():
    args = parse_args()

    if not DUCKDB_PATH.exists():
        print(f"DuckDB database not found at {DUCKDB_PATH}")
        print("Run dbt first:  make dbt-run")
        return

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # Read daily features and extract entity rows with labels
    df = conn.execute("""
        SELECT
            user_id,
            CAST(event_date AS TIMESTAMP) AS event_timestamp,
            has_anomaly
        FROM main.user_daily_features
        ORDER BY event_date, user_id
    """).fetchdf()

    conn.close()

    # Ensure correct types.
    # Use end-of-day UTC so entity timestamps are always after the feature
    # timestamps (which land at midnight UTC after export_dbt_to_minio).
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True) + pd.Timedelta(hours=23, minutes=59, seconds=59)
    df["has_anomaly"] = df["has_anomaly"].astype(int)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)

    anomaly_count = df["has_anomaly"].sum()
    total = len(df)
    anomaly_pct = 100 * anomaly_count / total if total > 0 else 0

    print(f"Generated {total} entity rows → {args.output}")
    print(f"  Users: {df['user_id'].nunique()}")
    print(f"  Dates: {df['event_timestamp'].dt.date.nunique()}")
    print(f"  Anomalies: {anomaly_count} ({anomaly_pct:.1f}%)")


if __name__ == "__main__":
    main()
