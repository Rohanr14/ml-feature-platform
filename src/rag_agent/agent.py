"""RAG platform query agent for the ML feature platform.

Supports two retrieval modes:
- **In-memory** (SimpleMetadataRetriever): token-overlap search over local files,
  no external dependencies.  Used as a fallback and in tests.
- **pgvector** (PgvectorRetriever): semantic search over sentence-transformer
  embeddings stored in PostgreSQL via pgvector.  Activated when a database
  connection URI is provided.

Both retrievers implement the same ``search(query, top_k) -> list[KnowledgeDocument]``
interface so the agent is backend-agnostic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

DEFAULT_SOURCE_FILES = [
    "README.md",
    "docs/PHASE_1_GUIDE.md",
    "docs/PHASE_3_GUIDE.md",
    "src/feature_store/feature_repo.py",
    "src/training/scripts/train.py",
    "src/serving/app.py",
    "src/dbt_features/models/features/schema.yml",
    "docker-compose.yml",
]
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "for",
    "from",
    "how",
    "in",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "what",
    "with",
}


# ── Data classes ────────────────────────────────────────────────────


@dataclass(slots=True)
class KnowledgeDocument:
    source_path: str
    section: str
    content: str
    title: str
    tokens: set[str] = field(default_factory=set)


@dataclass(slots=True)
class ToolObservation:
    tool_name: str
    summary: str
    citations: list[str]


@dataclass(slots=True)
class AgentAnswer:
    question: str
    answer: str
    citations: list[str]
    matched_sections: list[str]


# ── Retriever protocol + implementations ────────────────────────────


class Retriever(Protocol):
    def search(self, query: str, top_k: int = 3) -> list[KnowledgeDocument]: ...


class SimpleMetadataRetriever:
    """Token-overlap retriever that works entirely in-memory."""

    def __init__(self, documents: list[KnowledgeDocument]):
        self.documents = documents

    @staticmethod
    def tokenize(text: str) -> set[str]:
        tokens = {token for token in re.findall(r"[a-zA-Z0-9_]+", text.lower()) if token not in STOPWORDS}
        return {token for token in tokens if len(token) > 1}

    def search(self, query: str, top_k: int = 3) -> list[KnowledgeDocument]:
        query_tokens = self.tokenize(query)
        scored_documents: list[tuple[int, int, KnowledgeDocument]] = []
        for document in self.documents:
            overlap = len(query_tokens & document.tokens)
            if overlap == 0:
                continue
            scored_documents.append((overlap, len(document.content), document))

        scored_documents.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return [document for _, _, document in scored_documents[:top_k]]


class PgvectorRetriever:
    """Semantic retriever backed by pgvector + sentence-transformers."""

    def __init__(self, connection_uri: str, table_name: str = "rag_metadata_index"):
        self.connection_uri = connection_uri
        self.table_name = table_name

    def search(self, query: str, top_k: int = 3) -> list[KnowledgeDocument]:
        import psycopg

        from src.rag_agent.index_store import embed_text, format_vector

        query_vec = format_vector(embed_text(query))
        sql = (
            f"SELECT source_path, section, content, "
            f"1 - (embedding <=> %s::vector) AS similarity "
            f"FROM {self.table_name} "
            "ORDER BY embedding <=> %s::vector "
            "LIMIT %s;"
        )
        with psycopg.connect(self.connection_uri) as conn, conn.cursor() as cur:
            cur.execute(sql, (query_vec, query_vec, top_k))
            rows = cur.fetchall()

        return [
            KnowledgeDocument(
                source_path=source_path,
                section=section,
                content=content,
                title=f"{source_path} :: {section}",
                tokens=set(),
            )
            for source_path, section, content, _similarity in rows
        ]


# ── Main agent ──────────────────────────────────────────────────────


class PlatformQueryAgent:
    def __init__(
        self,
        documents: list[KnowledgeDocument],
        repo_root: str | Path | None = None,
        retriever: Retriever | None = None,
    ):
        self.documents = documents
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[2])
        self.retriever: Retriever = retriever or SimpleMetadataRetriever(documents)

    @classmethod
    def from_repo(
        cls,
        repo_root: str | Path | None = None,
        *,
        pgvector_uri: str | None = None,
    ) -> PlatformQueryAgent:
        root = Path(repo_root or Path(__file__).resolve().parents[2])
        documents = build_platform_documents(root)
        retriever: Retriever | None = None
        if pgvector_uri:
            retriever = PgvectorRetriever(connection_uri=pgvector_uri)
        return cls(documents, repo_root=root, retriever=retriever)

    def answer(self, question: str, top_k: int = 3) -> AgentAnswer:
        tool_observations = run_context_tools(question, self.repo_root)
        matches = self.retriever.search(question, top_k=top_k)
        if not matches and not tool_observations:
            return AgentAnswer(
                question=question,
                answer="I could not find a strong match in the indexed platform metadata.",
                citations=[],
                matched_sections=[],
            )

        answer_sections: list[str] = []
        citations: list[str] = []
        matched_sections: list[str] = []

        if tool_observations:
            tool_lines = [f"- {item.tool_name}: {item.summary}" for item in tool_observations]
            answer_sections.append("Tool observations:\n" + "\n".join(tool_lines))
            for item in tool_observations:
                citations.extend(item.citations)
                matched_sections.append(item.tool_name)

        if matches:
            summary_lines = [f"- {document.section}: {document.content.strip()}" for document in matches]
            answer_sections.append("Relevant platform context:\n" + "\n".join(summary_lines))
            citations.extend(document.source_path for document in matches)
            matched_sections.extend(document.section for document in matches)

        return AgentAnswer(
            question=question,
            answer="\n\n".join(answer_sections),
            citations=dedupe_preserving_order(citations),
            matched_sections=matched_sections,
        )


# ── Document building ──────────────────────────────────────────────


def build_platform_documents(repo_root: str | Path | None = None) -> list[KnowledgeDocument]:
    root = Path(repo_root or Path(__file__).resolve().parents[2])
    documents: list[KnowledgeDocument] = []

    for relative_path in DEFAULT_SOURCE_FILES:
        full_path = root / relative_path
        if not full_path.exists():
            continue

        text = full_path.read_text(encoding="utf-8")
        sections = split_into_sections(text)
        for section_title, section_content in sections:
            cleaned_content = section_content.strip()
            if not cleaned_content:
                continue
            documents.append(
                KnowledgeDocument(
                    source_path=relative_path,
                    section=section_title,
                    title=f"{relative_path} :: {section_title}",
                    content=cleaned_content,
                    tokens=SimpleMetadataRetriever.tokenize(f"{relative_path} {section_title} {cleaned_content}"),
                )
            )

    return documents


def split_into_sections(text: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_title = "Introduction"
    current_lines: list[str] = []

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or stripped.endswith(":"):
            if current_lines:
                sections.append((current_title, "\n".join(current_lines).strip()))
            current_title = stripped.lstrip("#").strip() or "Untitled"
            current_lines = []
            continue
        current_lines.append(line)

    if current_lines:
        sections.append((current_title, "\n".join(current_lines).strip()))

    return sections


# ── Context tools ──────────────────────────────────────────────────


def run_context_tools(question: str, repo_root: str | Path | None = None) -> list[ToolObservation]:
    root = Path(repo_root or Path(__file__).resolve().parents[2])
    lowered = question.lower()
    observations: list[ToolObservation] = []

    if any(keyword in lowered for keyword in ("feast", "feature", "feature view", "feature store")):
        observations.append(inspect_feature_store(root))
    if any(keyword in lowered for keyword in ("mlflow", "model registry", "registry", "training")):
        observations.append(inspect_mlflow_setup(root))
    if any(keyword in lowered for keyword in ("delta", "minio", "s3", "storage", "lake")):
        observations.append(inspect_storage_paths(root))

    return observations


def inspect_feature_store(repo_root: Path) -> ToolObservation:
    feature_repo_text = (repo_root / "src/feature_store/feature_repo.py").read_text(encoding="utf-8")
    feature_views = re.findall(r'name="([^"]+)"', feature_repo_text)
    interesting_views = [name for name in feature_views if "features" in name or name == "transaction_request"]
    summary = "Indexed Feast-related definitions: " + ", ".join(interesting_views[:8])
    return ToolObservation(
        tool_name="feature_store_tool",
        summary=summary,
        citations=["src/feature_store/feature_repo.py"],
    )


def inspect_mlflow_setup(repo_root: Path) -> ToolObservation:
    train_script = (repo_root / "src/training/scripts/train.py").read_text(encoding="utf-8")
    compose_text = (repo_root / "docker-compose.yml").read_text(encoding="utf-8")
    model_names = re.findall(r'default="([^"]*TransactionAnomalyDetector[^"]*)"', train_script)
    tracking_present = "MLFLOW" in compose_text
    summary = (
        f"Training CLI defaults reference model(s): {', '.join(model_names) or 'none found'}; "
        f"docker-compose MLflow service configured: {tracking_present}."
    )
    return ToolObservation(
        tool_name="mlflow_tool",
        summary=summary,
        citations=["src/training/scripts/train.py", "docker-compose.yml"],
    )


def inspect_storage_paths(repo_root: Path) -> ToolObservation:
    feature_repo_text = (repo_root / "src/feature_store/feature_repo.py").read_text(encoding="utf-8")
    s3_paths = re.findall(r'path="([^"]+)"', feature_repo_text)
    summary = "Configured storage paths: " + ", ".join(s3_paths[:5])
    return ToolObservation(
        tool_name="storage_tool",
        summary=summary,
        citations=["src/feature_store/feature_repo.py"],
    )


def dedupe_preserving_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
