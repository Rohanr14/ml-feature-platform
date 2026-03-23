"""Unit tests for the LangChain-compatible query chain."""

from __future__ import annotations

import sys
import types
import unittest

from src.rag_agent.agent import PlatformQueryAgent
from src.rag_agent.langchain_chain import build_langchain_query_chain


class FakeRunnableLambda:
    def __init__(self, func):
        self.func = func

    def invoke(self, payload):
        return self.func(payload)


class LangChainQueryChainTests(unittest.TestCase):
    def test_chain_invokes_query_agent(self):
        langchain_core = types.ModuleType("langchain_core")
        runnables = types.ModuleType("langchain_core.runnables")
        runnables.RunnableLambda = FakeRunnableLambda
        sys.modules["langchain_core"] = langchain_core
        sys.modules["langchain_core.runnables"] = runnables

        chain = build_langchain_query_chain(PlatformQueryAgent.from_repo())
        result = chain.invoke({"question": "How do I smoke test the serving API?", "top_k": 2})

        self.assertIn("answer", result)
        self.assertIn("citations", result)
        self.assertTrue(result["citations"])


if __name__ == "__main__":
    unittest.main()
