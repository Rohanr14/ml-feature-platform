"""Unit tests for the Dagster feature pipeline definitions."""

from __future__ import annotations

import unittest


class DagsterDefinitionsTests(unittest.TestCase):
    def test_definitions_load_without_error(self):
        """Importing the module should produce valid Dagster Definitions."""
        from src.orchestration.jobs.feature_pipeline import defs

        self.assertIsNotNone(defs)

    def test_all_assets_registered(self):
        from src.orchestration.jobs.feature_pipeline import defs

        repo = defs.get_repository_def()
        # Extract asset keys from the assets_defs_by_key mapping
        asset_keys = {key.to_user_string() for key in repo.assets_defs_by_key}
        expected = {
            "dbt_models",
            "dbt_tests",
            "feast_parquet_export",
            "feast_apply",
            "feast_materialize",
            "entity_rows",
            "trained_model",
            "promoted_model",
        }
        self.assertEqual(expected, asset_keys)

    def test_jobs_registered(self):
        from src.orchestration.jobs.feature_pipeline import defs

        repo = defs.get_repository_def()
        job_names = {job.name for job in repo.get_all_jobs()}
        self.assertIn("batch_feature_pipeline", job_names)
        self.assertIn("training_pipeline", job_names)

    def test_schedule_registered(self):
        from src.orchestration.jobs.feature_pipeline import defs

        repo = defs.get_repository_def()
        schedule_names = {s.name for s in repo.schedule_defs}
        self.assertIn("nightly_training", schedule_names)


if __name__ == "__main__":
    unittest.main()
