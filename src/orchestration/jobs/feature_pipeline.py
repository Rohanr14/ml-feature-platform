"""
Dagster job: orchestrates the batch feature pipeline.

DAG:
  1. Run dbt models (staging → features)
  2. Materialize Feast offline → online store
  3. Trigger model retraining (if drift detected)
  4. Update model registry
"""

# TODO Phase 2/3: Implement with Dagster assets
# from dagster import asset, job, schedule, define_asset_job
