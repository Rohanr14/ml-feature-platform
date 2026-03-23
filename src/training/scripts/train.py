"""
Training script: pulls features from Feast, trains model, logs to MLflow.

Usage:
    python -m src.training.scripts.train --epochs 50 --lr 0.001
"""

# TODO Phase 2: Implement
# 1. Load training dataset from Feast offline store (point-in-time join)
# 2. Split train/val/test
# 3. Train TransactionAnomalyDetector
# 4. Log params, metrics, model artifact to MLflow
# 5. Register best model in MLflow Model Registry
