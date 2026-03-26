"""Simple smoke test for the FastAPI serving API.

Checks:
1. `/health`
2. `/model-info`
3. `/predict`
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib import error, request

import pandas as pd

DEFAULT_PREDICT_PAYLOAD = {
    "user_id": "user_1",
    "transaction_id": "txn_smoke_test",
    "amount": 125.0,
    "category": "electronics",
    "merchant_id": "merchant_42",
    "device_type": "ios",
    "ms_since_last_txn": 45000,
}


def resolve_predict_payload() -> dict:
    payload = dict(DEFAULT_PREDICT_PAYLOAD)
    candidate_files = [
        (Path("data/entity_rows.parquet"), ["user_id", "event_timestamp"]),
        (Path("data/feast/user_daily_features.parquet"), ["user_id", "event_date"]),
    ]

    for parquet_path, columns in candidate_files:
        if not parquet_path.exists():
            continue
        try:
            frame = pd.read_parquet(parquet_path, columns=columns)
            if frame.empty:
                continue
            row = frame.iloc[-1].to_dict()
            payload["user_id"] = str(row["user_id"])

            if "event_timestamp" in row and pd.notna(row["event_timestamp"]):
                payload["event_timestamp"] = pd.to_datetime(row["event_timestamp"], utc=True).isoformat()
            elif "event_date" in row and pd.notna(row["event_date"]):
                payload["event_timestamp"] = (
                    pd.to_datetime(row["event_date"], utc=True) + pd.Timedelta(hours=23, minutes=59, seconds=59)
                ).isoformat()
            return payload
        except Exception as exc:
            print(f"Warning: unable to read {parquet_path}: {exc}")

    return payload


def fetch_json(url: str, method: str = "GET", payload: dict | None = None) -> tuple[int, dict]:
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=body, headers=headers, method=method)
    with request.urlopen(req, timeout=10) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8001", help="Serving API base URL.")
    args = parser.parse_args()

    health_status, health_body = fetch_json(f"{args.base_url}/health")
    print(f"/health [{health_status}] -> {health_body}")

    model_info_status, model_info_body = fetch_json(f"{args.base_url}/model-info")
    print(f"/model-info [{model_info_status}] -> {model_info_body}")

    predict_payload = resolve_predict_payload()
    print(f"/predict payload -> user_id={predict_payload['user_id']}")

    predict_status, predict_body = fetch_json(
        f"{args.base_url}/predict",
        method="POST",
        payload=predict_payload,
    )
    print(f"/predict [{predict_status}] -> {predict_body}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        print(f"Smoke test failed with HTTP {exc.code}: {response_body}", file=sys.stderr)
        raise SystemExit(1) from exc
    except error.URLError as exc:
        print(f"Smoke test could not reach the serving API: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
