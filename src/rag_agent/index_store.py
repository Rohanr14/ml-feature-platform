"""pgvector-ready persistence utilities for Phase 4 metadata indexing."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from pathlib import Path
import hashlib
import os
import re

from src.rag_agent.agent import KnowledgeDocument, build_platform_documents

DEFAULT_VECTOR_DIMENSIONS = 64


@dataclass(slots=True)
class PgvectorDocumentRecord:
    source_path: str
    section: str
    content: str
    embedding: list[float]


class PgvectorMetadataIndex:
    def __init__(self, records: list[PgvectorDocumentRecord], dimensions: int = DEFAULT_VECTOR_DIMENSIONS):
        self.records = records
        self.dimensions = dimensions

    @classmethod
    def from_documents(
        cls,
        documents: list[KnowledgeDocument],
        dimensions: int = DEFAULT_VECTOR_DIMENSIONS,
    ) -> "PgvectorMetadataIndex":
        records = [
            PgvectorDocumentRecord(
                source_path=document.source_path,
                section=document.section,
                content=document.content,
                embedding=hash_embedding(f"{document.title}\n{document.content}", dimensions=dimensions),
            )
            for document in documents
        ]
        return cls(records=records, dimensions=dimensions)

    @classmethod
    def from_repo(
        cls,
        repo_root: str | Path | None = None,
        dimensions: int = DEFAULT_VECTOR_DIMENSIONS,
    ) -> "PgvectorMetadataIndex":
        return cls.from_documents(build_platform_documents(repo_root), dimensions=dimensions)

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
                    f"INSERT INTO {table_name} (source_path, section, content, embedding) "
                    "VALUES (%s, %s, %s, %s);"
                )
                rows = [
                    (record.source_path, record.section, record.content, format_vector(record.embedding))
                    for record in self.records
                ]
                cur.executemany(insert_sql, rows)
            conn.commit()
        return len(self.records)

    def search(self, connection_uri: str, question: str, top_k: int = 3, table_name: str = "rag_metadata_index") -> list[dict[str, str]]:
        import psycopg

        query_vector = format_vector(hash_embedding(question, dimensions=self.dimensions))
        sql = (
            f"SELECT source_path, section, content FROM {table_name} "
            "ORDER BY embedding <=> %s::vector "
            "LIMIT %s;"
        )
        with psycopg.connect(connection_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (query_vector, top_k))
                rows = cur.fetchall()
        return [
            {"source_path": source_path, "section": section, "content": content}
            for source_path, section, content in rows
        ]


def hash_embedding(text: str, dimensions: int = DEFAULT_VECTOR_DIMENSIONS) -> list[float]:
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
