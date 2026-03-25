"""LangChain RAG chain for the ML feature platform metadata agent.

Provides two modes:

1. **Full RAG** (``build_rag_chain``): Uses a pgvector-backed
   ``VectorStoreRetriever`` with a ``ChatPromptTemplate`` to produce
   grounded answers.  Requires ``langchain-community`` and an LLM
   (defaults to ``ChatOpenAI`` but any LangChain-compatible chat model
   works).

2. **Lightweight** (``build_langchain_query_chain``): Wraps the
   ``PlatformQueryAgent`` in a ``RunnableLambda`` so it can participate
   in LangChain pipelines without an external LLM dependency.
"""

from __future__ import annotations

import os
from pathlib import Path

from src.rag_agent.agent import PlatformQueryAgent

# ── Lightweight chain (no LLM required) ────────────────────────────


def build_langchain_query_chain(query_agent: PlatformQueryAgent):
    """Wrap the agent's ``answer()`` method as a LangChain Runnable."""
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
    pgvector_uri = os.getenv("RAG_PGVECTOR_DSN") or os.getenv("DATABASE_URL")
    query_agent = PlatformQueryAgent.from_repo(repo_root, pgvector_uri=pgvector_uri)
    chain = build_langchain_query_chain(query_agent)
    return chain.invoke({"question": question, "top_k": top_k})


# ── Full RAG chain (requires LLM + pgvector) ───────────────────────

_RAG_SYSTEM_PROMPT = """\
You are an expert assistant for the ML Feature Platform, a production-grade
real-time anomaly detection system.  Answer the user's question using ONLY
the retrieved context below.  If the context does not contain enough
information, say so — do not make things up.

When referencing specific files or components, cite them.

Retrieved context:
{context}
"""


def build_rag_chain(
    connection_uri: str,
    table_name: str = "rag_metadata_index",
    llm=None,
):
    """Build a full retrieval-augmented generation chain.

    Parameters
    ----------
    connection_uri:
        PostgreSQL connection string for the pgvector database.
    table_name:
        Table containing the indexed embeddings.
    llm:
        A LangChain chat model instance.  When *None* the chain uses
        ``ChatOpenAI(model="gpt-4o-mini", temperature=0)`` which requires
        the ``OPENAI_API_KEY`` env-var.  Any LangChain-compatible chat
        model (Anthropic, local Ollama, etc.) can be passed instead.
    """
    from langchain_core.documents import Document
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.runnables import RunnableLambda, RunnablePassthrough

    from src.rag_agent.index_store import embed_text, format_vector

    # -- Retriever ---------------------------------------------------

    def _retrieve(question: str) -> list[Document]:
        import psycopg

        query_vec = format_vector(embed_text(question))
        sql = (
            f"SELECT source_path, section, content, "
            f"1 - (embedding <=> %s::vector) AS similarity "
            f"FROM {table_name} "
            "ORDER BY embedding <=> %s::vector "
            "LIMIT 5;"
        )
        with psycopg.connect(connection_uri) as conn, conn.cursor() as cur:
            cur.execute(sql, (query_vec, query_vec))
            rows = cur.fetchall()

        return [
            Document(
                page_content=content,
                metadata={"source": source_path, "section": section, "similarity": float(sim)},
            )
            for source_path, section, content, sim in rows
        ]

    retriever = RunnableLambda(_retrieve)

    # -- Prompt ------------------------------------------------------

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", _RAG_SYSTEM_PROMPT),
            ("human", "{question}"),
        ]
    )

    # -- LLM ---------------------------------------------------------

    if llm is None:
        from langchain_openai import ChatOpenAI

        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # -- Chain -------------------------------------------------------

    def _format_docs(docs: list[Document]) -> str:
        parts: list[str] = []
        for i, doc in enumerate(docs, 1):
            source = doc.metadata.get("source", "unknown")
            section = doc.metadata.get("section", "")
            parts.append(f"[{i}] {source} :: {section}\n{doc.page_content}")
        return "\n\n".join(parts)

    chain = (
        {
            "context": retriever | RunnableLambda(_format_docs),
            "question": RunnablePassthrough(),
        }
        | prompt
        | llm
        | StrOutputParser()
    )
    return chain
