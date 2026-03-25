"""Training entrypoint for the Phase 2 anomaly detector pipeline.

This script:
1. Loads point-in-time-correct historical features from Feast.
2. Splits the resulting dataset into train/validation/test sets.
3. Trains the ``TransactionAnomalyDetector`` PyTorch model.
4. Logs parameters, metrics, and the trained model to MLflow.
5. Optionally registers the best model in the MLflow Model Registry.

Example:
    python -m src.training.scripts.train \
        --entity-rows-path data/training/entity_rows.parquet \
        --tracking-uri http://localhost:5000
"""

from __future__ import annotations

import argparse
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

import mlflow
import mlflow.pytorch
import pandas as pd
import torch
import yaml
from feast import FeatureStore
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from torch import nn
from torch.optim import AdamW
from torch.utils.data import DataLoader, TensorDataset

from src.training.models.anomaly_detector import TransactionAnomalyDetector

LABEL_COLUMN = "has_anomaly"
TIMESTAMP_COLUMN = "event_timestamp"
ENTITY_ID_COLUMN = "user_id"
DAILY_FEATURE_COLUMNS = [
    "daily_txn_count",
    "daily_txn_sum",
    "daily_txn_avg",
    "daily_txn_max",
    "daily_unique_categories",
    "daily_unique_merchants",
    "daily_unique_devices",
    "first_txn_hour",
    "last_txn_hour",
]
ROLLING_FEATURE_COLUMNS = [
    "rolling_7d_txn_count",
    "rolling_7d_avg_amount",
    "rolling_30d_txn_count",
    "rolling_30d_avg_amount",
]
FEATURE_COLUMNS = DAILY_FEATURE_COLUMNS + ROLLING_FEATURE_COLUMNS
FEATURE_REFERENCES = [
    *(f"user_daily_features:{feature_name}" for feature_name in DAILY_FEATURE_COLUMNS),
    *(f"user_rolling_features:{feature_name}" for feature_name in ROLLING_FEATURE_COLUMNS),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("src/training/configs/default.yaml"),
        help="Path to the YAML training config.",
    )
    parser.add_argument(
        "--feature-repo",
        type=Path,
        default=Path("src/feature_store"),
        help="Path to the Feast feature repository.",
    )
    parser.add_argument(
        "--entity-rows-path",
        type=Path,
        required=True,
        help="CSV or Parquet file with user_id, event_timestamp/event_date, and has_anomaly columns.",
    )
    parser.add_argument(
        "--tracking-uri",
        type=str,
        default=None,
        help="Optional MLflow tracking URI override.",
    )
    parser.add_argument(
        "--experiment-name",
        type=str,
        default="transaction-anomaly-detector",
        help="MLflow experiment name.",
    )
    parser.add_argument(
        "--registered-model-name",
        type=str,
        default="TransactionAnomalyDetector",
        help="Registered model name for MLflow Model Registry.",
    )
    parser.add_argument(
        "--run-name",
        type=str,
        default=None,
        help="Optional MLflow run name.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=42,
        help="Random seed used for train/val/test splitting and Torch initialization.",
    )
    parser.add_argument("--epochs", type=int, default=None, help="Override training.epochs from config.")
    parser.add_argument("--lr", type=float, default=None, help="Override training.learning_rate from config.")
    parser.add_argument("--batch-size", type=int, default=None, help="Override training.batch_size from config.")
    return parser.parse_args()


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def apply_overrides(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated_config = deepcopy(config)
    training_config = updated_config.setdefault("training", {})

    if args.epochs is not None:
        training_config["epochs"] = args.epochs
    if args.lr is not None:
        training_config["learning_rate"] = args.lr
    if args.batch_size is not None:
        training_config["batch_size"] = args.batch_size

    return updated_config


def load_entity_rows(entity_rows_path: Path) -> pd.DataFrame:
    if entity_rows_path.suffix == ".csv":
        entity_rows = pd.read_csv(entity_rows_path)
    else:
        entity_rows = pd.read_parquet(entity_rows_path)

    if TIMESTAMP_COLUMN not in entity_rows.columns:
        if "event_date" not in entity_rows.columns:
            raise ValueError(
                f"Entity rows must include either '{TIMESTAMP_COLUMN}' or 'event_date'."
            )
        entity_rows[TIMESTAMP_COLUMN] = pd.to_datetime(entity_rows["event_date"])
    else:
        entity_rows[TIMESTAMP_COLUMN] = pd.to_datetime(entity_rows[TIMESTAMP_COLUMN])

    required_columns = {ENTITY_ID_COLUMN, TIMESTAMP_COLUMN, LABEL_COLUMN}
    missing_columns = required_columns - set(entity_rows.columns)
    if missing_columns:
        missing_str = ", ".join(sorted(missing_columns))
        raise ValueError(f"Entity rows file is missing required columns: {missing_str}")

    entity_rows[LABEL_COLUMN] = entity_rows[LABEL_COLUMN].astype(int)
    return entity_rows[[ENTITY_ID_COLUMN, TIMESTAMP_COLUMN, LABEL_COLUMN]].copy()


def load_training_dataframe(feature_repo_path: Path, entity_rows_path: Path) -> pd.DataFrame:
    feature_store = FeatureStore(repo_path=str(feature_repo_path))
    entity_rows = load_entity_rows(entity_rows_path)

    training_df = feature_store.get_historical_features(
        entity_df=entity_rows,
        features=FEATURE_REFERENCES,
    ).to_df()

    expected_columns = {ENTITY_ID_COLUMN, TIMESTAMP_COLUMN, LABEL_COLUMN, *FEATURE_COLUMNS}
    missing_columns = expected_columns - set(training_df.columns)
    if missing_columns:
        missing_str = ", ".join(sorted(missing_columns))
        raise ValueError(f"Historical feature retrieval is missing columns: {missing_str}")

    training_df = training_df.dropna(subset=FEATURE_COLUMNS).copy()
    training_df[LABEL_COLUMN] = training_df[LABEL_COLUMN].astype(int)
    return training_df


def split_dataset(training_df: pd.DataFrame, config: dict[str, Any], random_seed: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    split_config = config["data"]
    train_split = float(split_config["train_split"])
    val_split = float(split_config["val_split"])
    test_split = float(split_config["test_split"])
    total = train_split + val_split + test_split
    if abs(total - 1.0) > 1e-9:
        raise ValueError(f"Train/val/test splits must sum to 1.0, got {total}")

    stratify_labels = training_df[LABEL_COLUMN] if training_df[LABEL_COLUMN].nunique() > 1 else None
    train_df, temp_df = train_test_split(
        training_df,
        train_size=train_split,
        random_state=random_seed,
        stratify=stratify_labels,
    )

    remaining_fraction = val_split + test_split
    val_size_within_temp = val_split / remaining_fraction
    temp_stratify = temp_df[LABEL_COLUMN] if temp_df[LABEL_COLUMN].nunique() > 1 else None
    val_df, test_df = train_test_split(
        temp_df,
        train_size=val_size_within_temp,
        random_state=random_seed,
        stratify=temp_stratify,
    )
    return train_df, val_df, test_df


def to_tensor_dataset(dataframe: pd.DataFrame) -> TensorDataset:
    features = torch.tensor(dataframe[FEATURE_COLUMNS].to_numpy(dtype="float32"), dtype=torch.float32)
    labels = torch.tensor(dataframe[LABEL_COLUMN].to_numpy(dtype="float32"), dtype=torch.float32)
    return TensorDataset(features, labels)


def build_dataloader(dataframe: pd.DataFrame, batch_size: int, shuffle: bool) -> DataLoader:
    return DataLoader(to_tensor_dataset(dataframe), batch_size=batch_size, shuffle=shuffle)


def evaluate_model(model: nn.Module, dataframe: pd.DataFrame, device: torch.device) -> dict[str, float]:
    if dataframe.empty:
        raise ValueError("Evaluation dataframe is empty.")

    model.eval()
    with torch.no_grad():
        features = torch.tensor(dataframe[FEATURE_COLUMNS].to_numpy(dtype="float32"), dtype=torch.float32, device=device)
        labels = dataframe[LABEL_COLUMN].to_numpy(dtype="int64")
        probabilities = model(features).cpu().numpy()

    predictions = (probabilities >= 0.5).astype(int)
    metrics = {
        "accuracy": float(accuracy_score(labels, predictions)),
        "precision": float(precision_score(labels, predictions, zero_division=0)),
        "recall": float(recall_score(labels, predictions, zero_division=0)),
        "f1": float(f1_score(labels, predictions, zero_division=0)),
    }
    if len(set(labels)) > 1:
        metrics["roc_auc"] = float(roc_auc_score(labels, probabilities))
    return metrics


def train_model(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    config: dict[str, Any],
    random_seed: int,
) -> tuple[TransactionAnomalyDetector, dict[str, float]]:
    torch.manual_seed(random_seed)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model_config = config["model"]
    training_config = config["training"]
    model = TransactionAnomalyDetector(
        input_dim=len(FEATURE_COLUMNS),
        hidden_dim=int(model_config["hidden_dim"]),
        num_heads=int(model_config["num_heads"]),
        dropout=float(model_config["dropout"]),
    ).to(device)

    optimizer = AdamW(
        model.parameters(),
        lr=float(training_config["learning_rate"]),
        weight_decay=float(training_config["weight_decay"]),
    )
    criterion = nn.BCELoss()
    train_loader = build_dataloader(train_df, batch_size=int(training_config["batch_size"]), shuffle=True)

    best_state_dict: dict[str, torch.Tensor] | None = None
    best_metrics: dict[str, float] = {}
    best_val_loss = float("inf")
    patience = int(training_config["early_stopping_patience"])
    patience_counter = 0

    for epoch in range(int(training_config["epochs"])):
        model.train()
        total_loss = 0.0
        total_examples = 0

        for batch_features, batch_labels in train_loader:
            batch_features = batch_features.to(device)
            batch_labels = batch_labels.to(device)

            optimizer.zero_grad()
            predictions = model(batch_features)
            loss = criterion(predictions, batch_labels)
            loss.backward()
            optimizer.step()

            batch_size = batch_features.size(0)
            total_loss += loss.item() * batch_size
            total_examples += batch_size

        avg_train_loss = total_loss / max(total_examples, 1)
        val_metrics = evaluate_model(model, val_df, device)

        with torch.no_grad():
            val_features = torch.tensor(val_df[FEATURE_COLUMNS].to_numpy(dtype="float32"), dtype=torch.float32, device=device)
            val_labels = torch.tensor(val_df[LABEL_COLUMN].to_numpy(dtype="float32"), dtype=torch.float32, device=device)
            val_loss = criterion(model(val_features), val_labels).item()

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_metrics = {
                "train_loss": float(avg_train_loss),
                "val_loss": float(val_loss),
                **{f"val_{name}": value for name, value in val_metrics.items()},
                "best_epoch": float(epoch + 1),
            }
            best_state_dict = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= patience:
                break

    if best_state_dict is None:
        raise RuntimeError("Training did not produce a valid model state.")

    model.load_state_dict(best_state_dict)
    return model, best_metrics


def log_to_mlflow(
    model: TransactionAnomalyDetector,
    config: dict[str, Any],
    test_metrics: dict[str, float],
    args: argparse.Namespace,
) -> None:
    import os
    if args.tracking_uri:
        mlflow.set_tracking_uri(args.tracking_uri)
    # Point boto3 at local MinIO so artifact uploads don't go to real AWS
    os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
    mlflow.set_experiment(args.experiment_name)

    flattened_params = {
        "feature_repo": str(args.feature_repo),
        "entity_rows_path": str(args.entity_rows_path),
        "feature_count": len(FEATURE_COLUMNS),
        "configured_input_dim": int(config["model"].get("input_dim", len(FEATURE_COLUMNS))),
        "effective_input_dim": len(FEATURE_COLUMNS),
        "label_column": LABEL_COLUMN,
        **{f"model.{key}": value for key, value in config["model"].items()},
        **{f"training.{key}": value for key, value in config["training"].items()},
        **{f"data.{key}": value for key, value in config["data"].items() if key != "feast_feature_views"},
        "data.feast_feature_views": ",".join(config["data"].get("feast_feature_views", [])),
    }

    with mlflow.start_run(run_name=args.run_name):
        mlflow.log_params(flattened_params)
        mlflow.log_metrics(test_metrics)
        mlflow.log_dict(
            {
                "features": FEATURE_COLUMNS,
                "feature_references": FEATURE_REFERENCES,
                "label_column": LABEL_COLUMN,
            },
            artifact_file="feature_metadata.json",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = Path(temp_dir) / "model.pt"
            torch.save(model.state_dict(), model_path)
            mlflow.log_artifact(str(model_path), artifact_path="artifacts")

        mlflow.pytorch.log_model(
            pytorch_model=model,
            artifact_path="model",
            registered_model_name=args.registered_model_name,
        )


def main() -> None:
    args = parse_args()
    config = apply_overrides(load_config(args.config), args)

    training_df = load_training_dataframe(args.feature_repo, args.entity_rows_path)
    train_df, val_df, test_df = split_dataset(training_df, config, args.random_seed)

    model, validation_metrics = train_model(train_df, val_df, config, args.random_seed)
    test_metrics = evaluate_model(model, test_df, torch.device("cuda" if torch.cuda.is_available() else "cpu"))
    combined_metrics = {
        **validation_metrics,
        **{f"test_{metric_name}": metric_value for metric_name, metric_value in test_metrics.items()},
    }
    try:
        log_to_mlflow(model, config, combined_metrics, args)
    except Exception as mlflow_err:
        print(f"Warning: MLflow logging failed ({mlflow_err})")
        print("Training succeeded — re-run 'make train' once MLflow is up to log results.")

    print("Training complete.")
    for metric_name, metric_value in sorted(combined_metrics.items()):
        print(f"{metric_name}: {metric_value:.4f}")


if __name__ == "__main__":
    main()
