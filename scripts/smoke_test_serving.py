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
from urllib import error, request

DEFAULT_PREDICT_PAYLOAD = {
    "user_id": "user_1",
    "transaction_id": "txn_smoke_test",
    "amount": 125.0,
    "category": "electronics",
    "merchant_id": "merchant_42",
    "device_type": "ios",
    "ms_since_last_txn": 45000,
}


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
    parser.add_argument("--base-url", default="http://localhost:8000", help="Serving API base URL.")
    args = parser.parse_args()

    health_status, health_body = fetch_json(f"{args.base_url}/health")
    print(f"/health [{health_status}] -> {health_body}")

    model_info_status, model_info_body = fetch_json(f"{args.base_url}/model-info")
    print(f"/model-info [{model_info_status}] -> {model_info_body}")

    predict_status, predict_body = fetch_json(
        f"{args.base_url}/predict",
        method="POST",
        payload=DEFAULT_PREDICT_PAYLOAD,
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
