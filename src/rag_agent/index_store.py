"""pgvector persistence layer with sentence-transformer embeddings.

Uses the ``all-MiniLM-L6-v2`` model (384-dim) for semantic search over
platform metadata.  Falls back to deterministic hash embeddings when
``sentence-transformers`` is not installed so that CI tests still pass.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from math import sqrt
from pathlib import Path

from src.rag_agent.agent import KnowledgeDocument, build_platform_documents

EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIMENSIONS = 384  # output size of all-MiniLM-L6-v2
HASH_FALLBACK_DIMENSIONS = 64


@dataclass(slots=True)
class PgvectorDocumentRecord:
    source_path: str
    section: str
    content: str
    embedding: list[float]


# ── Embedding helpers ───────────────────────────────────────────────

_model_cache: dict[str, object] = {}


def _get_embedding_model():
    """Lazily load the sentence-transformer model (cached)."""
    if "model" not in _model_cache:
        from sentence_transformers import SentenceTransformer

        _model_cache["model"] = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _model_cache["model"]


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Encode a batch of texts into dense vectors using sentence-transformers."""
    model = _get_embedding_model()
    embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    return [vec.tolist() for vec in embeddings]


def embed_text(text: str) -> list[float]:
    """Encode a single text string."""
    return embed_texts([text])[0]


def hash_embedding(text: str, dimensions: int = HASH_FALLBACK_DIMENSIONS) -> list[float]:
    """Deterministic hash-based embedding fallback (for environments without GPU / large deps)."""
    vector = [0.0] * dimensions
    tokens = re.findall(r"[a-zA-Z0-9_]+", text.lower())
    if not tokens:
        return vector

    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % dimensions
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vector[index] += sign

    norm = sqrt(sum(value * value for value in vector)) or 1.0
    return [value / norm for value in vector]


def format_vector(values: list[float]) -> str:
    return "[" + ", ".join(f"{value:.8f}" for value in values) + "]"


def resolve_connection_uri() -> str:
    uri = os.getenv("RAG_PGVECTOR_DSN") or os.getenv("DATABASE_URL")
    if not uri:
        raise ValueError("Set RAG_PGVECTOR_DSN or DATABASE_URL to persist/query the pgvector metadata index.")
    return uri


# ── Index class ─────────────────────────────────────────────────────


class PgvectorMetadataIndex:
    """Manages pgvector table creation, upsert, and semantic search."""

    def __init__(self, records: list[PgvectorDocumentRecord], dimensions: int = EMBEDDING_DIMENSIONS):
        self.records = records
        self.dimensions = dimensions

    @classmethod
    def from_documents(
        cls,
        documents: list[KnowledgeDocument],
        *,
        use_hash_fallback: bool = False,
    ) -> PgvectorMetadataIndex:
        texts = [f"{doc.title}\n{doc.content}" for doc in documents]

        if use_hash_fallback:
            embeddings = [hash_embedding(t, dimensions=HASH_FALLBACK_DIMENSIONS) for t in texts]
            dims = HASH_FALLBACK_DIMENSIONS
        else:
            embeddings = embed_texts(texts)
            dims = EMBEDDING_DIMENSIONS

        records = [
            PgvectorDocumentRecord(
                source_path=doc.source_path,
                section=doc.section,
                content=doc.content,
                embedding=emb,
            )
            for doc, emb in zip(documents, embeddings, strict=True)
        ]
        return cls(records=records, dimensions=dims)

    @classmethod
    def from_repo(
        cls,
        repo_root: str | Path | None = None,
        *,
        use_hash_fallback: bool = False,
    ) -> PgvectorMetadataIndex:
        return cls.from_documents(
            build_platform_documents(repo_root),
            use_hash_fallback=use_hash_fallback,
        )

    # ── SQL helpers ─────────────────────────────────────────────────

    def create_table_sql(self, table_name: str = "rag_metadata_index") -> str:
        return (
            "CREATE EXTENSION IF NOT EXISTS vector;\n"
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            "    id BIGSERIAL PRIMARY KEY,\n"
            "    source_path TEXT NOT NULL,\n"
            "    section TEXT NOT NULL,\n"
            "    content TEXT NOT NULL,\n"
            f"    embedding vector({self.dimensions}) NOT NULL\n"
            ");"
        )

    def upsert_records(self, connection_uri: str, table_name: str = "rag_metadata_index") -> int:
        import psycopg

        with psycopg.connect(connection_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(self.create_table_sql(table_name))
                cur.execute(f"TRUNCATE TABLE {table_name};")
                insert_sql = (
                    f"INSERT INTO {table_name} (source_path, section, content, embedding) VALUES (%s, %s, %s, %s);"
                )
                rows = [
                    (rec.source_path, rec.section, rec.content, format_vector(rec.embedding)) for rec in self.records
                ]
                cur.executemany(insert_sql, rows)
            conn.commit()
        return len(self.records)

    def search(
        self,
        connection_uri: str,
        question: str,
        top_k: int = 3,
        table_name: str = "rag_metadata_index",
        *,
        use_hash_fallback: bool = False,
    ) -> list[dict[str, str]]:
        """Semantic search over the indexed metadata using pgvector cosine distance."""
        import psycopg

        query_vec = hash_embedding(question, dimensions=self.dimensions) if use_hash_fallback else embed_text(question)

        query_vector_str = format_vector(query_vec)
        sql = (
            f"SELECT source_path, section, content, "
            f"1 - (embedding <=> %s::vector) AS similarity "
            f"FROM {table_name} "
            "ORDER BY embedding <=> %s::vector "
            "LIMIT %s;"
        )
        with psycopg.connect(connection_uri) as conn, conn.cursor() as cur:
            cur.execute(sql, (query_vector_str, query_vector_str, top_k))
            rows = cur.fetchall()
        return [
            {
                "source_path": source_path,
                "section": section,
                "content": content,
                "similarity": float(similarity),
            }
            for source_path, section, content, similarity in rows
        ]
