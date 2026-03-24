"""
Promote the latest trained model version to Production in MLflow.

Usage:
    python scripts/promote_model.py
    python scripts/promote_model.py --tracking-uri http://localhost:5001
"""

from __future__ import annotations

import argparse

import mlflow
from mlflow.tracking import MlflowClient


MODEL_NAME = "TransactionAnomalyDetector"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote latest model to Production")
    parser.add_argument("--tracking-uri", default="http://localhost:5001")
    parser.add_argument("--model-name", default=MODEL_NAME)
    return parser.parse_args()


def main():
    args = parse_args()
    mlflow.set_tracking_uri(args.tracking_uri)
    client = MlflowClient()

    # Get latest version
    versions = client.search_model_versions(f"name='{args.model_name}'")
    if not versions:
        print(f"No versions found for model '{args.model_name}'")
        return

    latest = max(versions, key=lambda v: int(v.version))
    print(f"Model: {args.model_name}")
    print(f"Latest version: {latest.version} (run_id: {latest.run_id})")

    # Try alias-based promotion first (MLflow 2.9+), fall back to legacy stages
    try:
        client.set_registered_model_alias(args.model_name, "Production", latest.version)
        print(f"Set alias 'Production' → version {latest.version}")
    except Exception:
        pass

    try:
        client.transition_model_version_stage(
            name=args.model_name,
            version=latest.version,
            stage="Production",
            archive_existing_versions=True,
        )
        print(f"Transitioned version {latest.version} → Production stage")
    except Exception as e:
        print(f"Stage transition skipped: {e}")

    print(f"\nModel ready for serving at: models:/{args.model_name}/Production")


if __name__ == "__main__":
    main()
