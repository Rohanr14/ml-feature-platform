"""Unit tests for the Phase 4 platform query agent foundation."""

from __future__ import annotations

import unittest

from src.rag_agent.agent import PlatformQueryAgent, build_platform_documents, run_context_tools


class PlatformQueryAgentTests(unittest.TestCase):
    def test_build_platform_documents_indexes_expected_sources(self):
        documents = build_platform_documents()
        indexed_sources = {document.source_path for document in documents}

        self.assertIn("README.md", indexed_sources)
        self.assertIn("docs/PHASE_3_GUIDE.md", indexed_sources)
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


if __name__ == "__main__":
    unittest.main()
