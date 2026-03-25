"""Dagster assets and jobs: orchestrate the batch feature pipeline.

DAG:
  1. Run dbt models (staging -> features)
  2. Run dbt data-quality tests
  3. Export dbt features to Parquet for Feast
  4. Apply Feast feature definitions
  5. Materialize Feast offline -> online store
  6. Generate entity rows for training
  7. Train anomaly-detection model and log to MLflow
  8. Promote latest model version to Production
"""

import subprocess
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = REPO_ROOT / "src" / "dbt_features"
FEATURE_STORE_DIR = REPO_ROOT / "src" / "feature_store"


def _run(cmd: list[str], *, cwd: Path | None = None) -> str:
    """Run a shell command and return stdout. Raises on non-zero exit."""
    result = subprocess.run(
        cmd,
        cwd=cwd or REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


# ── Assets ──────────────────────────────────────────────────────────


@asset(group_name="batch_features", description="Run dbt models to compute daily and rolling features from Delta Lake.")
def dbt_models(context: AssetExecutionContext) -> str:
    output = _run(["dbt", "run", "--profiles-dir", "."], cwd=DBT_PROJECT_DIR)
    context.log.info("dbt run completed:\n%s", output)
    return output


@asset(
    group_name="batch_features",
    deps=[dbt_models],
    description="Run dbt data-quality tests (not-null, unique, accepted-values, custom SQL).",
)
def dbt_tests(context: AssetExecutionContext) -> str:
    output = _run(["dbt", "test", "--profiles-dir", "."], cwd=DBT_PROJECT_DIR)
    context.log.info("dbt test completed:\n%s", output)
    return output


@asset(
    group_name="batch_features",
    deps=[dbt_tests],
    description="Export dbt feature tables and Kafka features-5m to Parquet for Feast.",
)
def feast_parquet_export(context: AssetExecutionContext) -> str:
    out1 = _run(["python", "scripts/export_dbt_to_minio.py"])
    out2 = _run(["python", "scripts/export_features_5m_to_parquet.py"])
    context.log.info("Export completed.")
    return out1 + "\n" + out2


@asset(
    group_name="feature_store",
    deps=[feast_parquet_export],
    description="Register / update Feast feature view definitions.",
)
def feast_apply(context: AssetExecutionContext) -> str:
    output = _run(["feast", "apply"], cwd=FEATURE_STORE_DIR)
    context.log.info("feast apply completed:\n%s", output)
    return output


@asset(
    group_name="feature_store",
    deps=[feast_apply],
    description="Materialize features from offline to online store for low-latency serving.",
)
def feast_materialize(context: AssetExecutionContext) -> str:
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")
    output = _run(["feast", "materialize-incremental", now], cwd=FEATURE_STORE_DIR)
    context.log.info("feast materialize-incremental completed:\n%s", output)
    return output


@asset(
    group_name="training",
    deps=[feast_materialize],
    description="Generate entity rows (user_id, event_timestamp, has_anomaly) for model training.",
)
def entity_rows(context: AssetExecutionContext) -> str:
    output = _run(["python", "scripts/generate_entity_rows.py"])
    context.log.info("Entity rows generated:\n%s", output)
    return output


@asset(
    group_name="training",
    deps=[entity_rows],
    description="Train TransactionAnomalyDetector and log metrics / model to MLflow.",
)
def trained_model(context: AssetExecutionContext) -> str:
    output = _run(
        [
            "python",
            "-m",
            "src.training.scripts.train",
            "--entity-rows-path",
            "data/entity_rows.parquet",
            "--feature-repo",
            "src/feature_store",
            "--tracking-uri",
            "http://localhost:5001",
            "--experiment-name",
            "transaction-anomaly-detector",
            "--run-name",
            "dagster-run",
        ]
    )
    context.log.info("Training completed:\n%s", output)
    return output


@asset(
    group_name="training",
    deps=[trained_model],
    description="Promote latest model version to Production alias in MLflow Model Registry.",
)
def promoted_model(context: AssetExecutionContext) -> str:
    output = _run(["python", "scripts/promote_model.py"])
    context.log.info("Model promoted:\n%s", output)
    return output


# ── Jobs & Schedules ────────────────────────────────────────────────

batch_feature_job = define_asset_job(
    name="batch_feature_pipeline",
    selection=AssetSelection.groups("batch_features", "feature_store"),
    description="Runs dbt models, tests, exports, and materializes features to Feast.",
)

training_job = define_asset_job(
    name="training_pipeline",
    selection=AssetSelection.all(),
    description="End-to-end: dbt -> Feast -> train -> promote.",
)

nightly_training_schedule = ScheduleDefinition(
    job=training_job,
    cron_schedule="0 2 * * *",  # 02:00 UTC daily
    name="nightly_training",
    description="Retrain the anomaly detector nightly from fresh batch features.",
)


# ── Definitions ─────────────────────────────────────────────────────

defs = Definitions(
    assets=[
        dbt_models,
        dbt_tests,
        feast_parquet_export,
        feast_apply,
        feast_materialize,
        entity_rows,
        trained_model,
        promoted_model,
    ],
    jobs=[batch_feature_job, training_job],
    schedules=[nightly_training_schedule],
)
