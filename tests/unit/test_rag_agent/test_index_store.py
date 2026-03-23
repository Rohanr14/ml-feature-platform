"""Unit tests for the pgvector-ready metadata index layer."""

from __future__ import annotations

import unittest

from src.rag_agent.index_store import PgvectorMetadataIndex, format_vector, hash_embedding


class PgvectorMetadataIndexTests(unittest.TestCase):
    def test_hash_embedding_returns_normalized_vector(self):
        embedding = hash_embedding("feature store mlflow delta", dimensions=8)

        self.assertEqual(len(embedding), 8)
        self.assertAlmostEqual(sum(value * value for value in embedding), 1.0, places=5)

    def test_format_vector_uses_pgvector_literal_format(self):
        formatted = format_vector([0.5, -0.25, 0.0])
        self.assertEqual(formatted, "[0.50000000, -0.25000000, 0.00000000]")

    def test_index_builds_records_and_sql(self):
        index = PgvectorMetadataIndex.from_repo(dimensions=8)

        self.assertTrue(index.records)
        create_sql = index.create_table_sql()
        self.assertIn("CREATE EXTENSION IF NOT EXISTS vector", create_sql)
        self.assertIn("vector(8)", create_sql)


if __name__ == "__main__":
    unittest.main()
