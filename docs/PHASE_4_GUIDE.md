# Phase 4: RAG Agent, Query Workflows, and Platform Polish

## What This Phase Builds

Phase 4 adds a queryable operator/developer assistant on top of the platform metadata.

Current Phase 4 capabilities in the repo include:

- searchable metadata indexing from repo docs/code
- direct context tools for Feast / MLflow / Delta-storage questions
- pgvector-ready persistence for the metadata index
- a FastAPI endpoint at `/agent/query`
- a LangChain-compatible query chain and CLI workflow

## Core Workflows

### 1. Ask the agent from Python / FastAPI

The serving API exposes:

```bash
POST /agent/query
```

This returns:

- `answer`
- `citations`
- `matched_sections`

### 2. Build the pgvector metadata index

```bash
make rag-index
```

This uses `scripts/build_rag_pgvector_index.py` and expects either:

- `RAG_PGVECTOR_DSN`, or
- `DATABASE_URL`

## 3. Run a direct metadata query from the CLI

```bash
python scripts/run_rag_query.py "How is MLflow configured?"
```

Or with an explicit retrieval depth:

```bash
python scripts/run_rag_query.py "What feature views exist?" --top-k 5
```

## LangChain-Compatible Chain

`src/rag_agent/langchain_chain.py` provides a thin LangChain-compatible runnable around the platform query agent. This keeps the current implementation lightweight while giving the project a clean path toward richer chain composition later.

## What “Done” Looks Like For Phase 4

You should be able to:

- query indexed platform metadata from Python or FastAPI
- get direct structured observations for Feast / MLflow / storage questions
- persist metadata chunks into pgvector
- run a CLI query flow for operator/developer questions

## Remaining Polish Opportunities

The repo now has the major building blocks for Phase 4. Remaining work is mostly optional polish such as:

- stronger LLM-backed answer generation on top of the current chain
- richer live-data tools beyond repo/config inspection
- extra observability/dashboard polish around the agent itself
