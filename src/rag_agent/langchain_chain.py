"""LangChain-compatible query chain for the Phase 4 platform agent."""

from __future__ import annotations

from pathlib import Path

from src.rag_agent.agent import PlatformQueryAgent


def build_langchain_query_chain(query_agent: PlatformQueryAgent):
    from langchain_core.runnables import RunnableLambda

    def run_query(payload: str | dict) -> dict:
        if isinstance(payload, str):
            question = payload
            top_k = 3
        else:
            question = payload["question"]
            top_k = int(payload.get("top_k", 3))

        answer = query_agent.answer(question, top_k=top_k)
        return {
            "question": question,
            "answer": answer.answer,
            "citations": answer.citations,
            "matched_sections": answer.matched_sections,
        }

    return RunnableLambda(run_query)


def run_langchain_query(question: str, repo_root: str | Path | None = None, top_k: int = 3) -> dict:
    query_agent = PlatformQueryAgent.from_repo(repo_root)
    chain = build_langchain_query_chain(query_agent)
    return chain.invoke({"question": question, "top_k": top_k})
