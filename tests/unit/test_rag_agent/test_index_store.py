"""Unit tests for the pgvector metadata index layer."""

from __future__ import annotations

import unittest

from src.rag_agent.index_store import (
    PgvectorMetadataIndex,
    format_vector,
    hash_embedding,
)


class HashEmbeddingTests(unittest.TestCase):
    def test_hash_embedding_returns_normalized_vector(self):
        embedding = hash_embedding("feature store mlflow delta", dimensions=8)

        self.assertEqual(len(embedding), 8)
        self.assertAlmostEqual(sum(value * value for value in embedding), 1.0, places=5)

    def test_hash_embedding_empty_text(self):
        embedding = hash_embedding("", dimensions=8)
        self.assertEqual(embedding, [0.0] * 8)

    def test_hash_embedding_is_deterministic(self):
        a = hash_embedding("feast feature store", dimensions=16)
        b = hash_embedding("feast feature store", dimensions=16)
        self.assertEqual(a, b)


class FormatVectorTests(unittest.TestCase):
    def test_format_vector_uses_pgvector_literal_format(self):
        formatted = format_vector([0.5, -0.25, 0.0])
        self.assertEqual(formatted, "[0.50000000, -0.25000000, 0.00000000]")


class PgvectorMetadataIndexTests(unittest.TestCase):
    def test_index_builds_records_with_hash_fallback(self):
        index = PgvectorMetadataIndex.from_repo(use_hash_fallback=True)

        self.assertTrue(index.records)
        self.assertEqual(index.dimensions, 64)
        for record in index.records:
            self.assertEqual(len(record.embedding), 64)

    def test_create_table_sql_contains_vector_type(self):
        index = PgvectorMetadataIndex.from_repo(use_hash_fallback=True)
        create_sql = index.create_table_sql()
        self.assertIn("CREATE EXTENSION IF NOT EXISTS vector", create_sql)
        self.assertIn("vector(64)", create_sql)


if __name__ == "__main__":
    unittest.main()
