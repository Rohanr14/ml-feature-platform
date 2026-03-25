"""Unit tests for the platform query agent (in-memory + pgvector retriever)."""

from __future__ import annotations

import unittest

from src.rag_agent.agent import (
    KnowledgeDocument,
    PgvectorRetriever,
    PlatformQueryAgent,
    SimpleMetadataRetriever,
    build_platform_documents,
    run_context_tools,
)


class SimpleMetadataRetrieverTests(unittest.TestCase):
    def test_tokenize_strips_stopwords_and_short_tokens(self):
        tokens = SimpleMetadataRetriever.tokenize("the Feast feature store is great")
        self.assertNotIn("the", tokens)
        self.assertNotIn("is", tokens)
        self.assertIn("feast", tokens)
        self.assertIn("feature", tokens)

    def test_search_returns_top_k_by_overlap(self):
        docs = [
            KnowledgeDocument(
                source_path="a.md",
                section="A",
                content="feast feature store setup",
                title="a.md :: A",
                tokens=SimpleMetadataRetriever.tokenize("feast feature store setup"),
            ),
            KnowledgeDocument(
                source_path="b.md",
                section="B",
                content="unrelated content about weather",
                title="b.md :: B",
                tokens=SimpleMetadataRetriever.tokenize("unrelated content about weather"),
            ),
        ]
        retriever = SimpleMetadataRetriever(docs)
        results = retriever.search("feast feature store", top_k=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].source_path, "a.md")


class PlatformQueryAgentTests(unittest.TestCase):
    def test_build_platform_documents_indexes_expected_sources(self):
        documents = build_platform_documents()
        indexed_sources = {document.source_path for document in documents}

        self.assertIn("README.md", indexed_sources)
        self.assertIn("src/serving/app.py", indexed_sources)

    def test_agent_answers_from_phase3_docs(self):
        agent = PlatformQueryAgent.from_repo()
        answer = agent.answer("How do I run serve-smoke for the serving API?", top_k=2)

        self.assertTrue(answer.citations)
        self.assertIn("docs/PHASE_3_GUIDE.md", answer.citations)
        self.assertIn("smoke-test", answer.answer)

    def test_context_tools_report_feature_and_mlflow_details(self):
        observations = run_context_tools("Explain the Feast feature views and MLflow setup")
        tool_names = {item.tool_name for item in observations}

        self.assertIn("feature_store_tool", tool_names)
        self.assertIn("mlflow_tool", tool_names)

    def test_agent_returns_fallback_when_no_match_exists(self):
        agent = PlatformQueryAgent.from_repo()
        answer = agent.answer("zzqv qxj9 plmokn", top_k=2)

        self.assertEqual(answer.citations, [])
        self.assertIn("could not find", answer.answer.lower())

    def test_agent_accepts_custom_retriever(self):
        """PlatformQueryAgent respects an injected retriever."""
        fake_doc = KnowledgeDocument(
            source_path="fake.md",
            section="Fake",
            content="injected result",
            title="fake.md :: Fake",
            tokens=set(),
        )

        class FakeRetriever:
            def search(self, query: str, top_k: int = 3) -> list[KnowledgeDocument]:
                return [fake_doc]

        agent = PlatformQueryAgent(documents=[], retriever=FakeRetriever())
        answer = agent.answer("anything")
        self.assertIn("injected result", answer.answer)


class PgvectorRetrieverTests(unittest.TestCase):
    def test_pgvector_retriever_instantiates(self):
        retriever = PgvectorRetriever(connection_uri="postgresql://user:pass@localhost/db")
        self.assertEqual(retriever.table_name, "rag_metadata_index")


if __name__ == "__main__":
    unittest.main()
