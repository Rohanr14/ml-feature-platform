"""Build and persist the repository metadata index into pgvector.

Uses sentence-transformer embeddings (all-MiniLM-L6-v2, 384-dim) for
semantic search.  Pass ``--hash-fallback`` to use deterministic hash
embeddings instead (useful for CI or environments without torch).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.rag_agent.index_store import PgvectorMetadataIndex, resolve_connection_uri


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="rag_metadata_index", help="Destination pgvector table name.")
    parser.add_argument(
        "--hash-fallback",
        action="store_true",
        help="Use deterministic hash embeddings instead of sentence-transformers.",
    )
    args = parser.parse_args()

    print("Building metadata index...")
    index = PgvectorMetadataIndex.from_repo(use_hash_fallback=args.hash_fallback)
    print(f"Embedded {len(index.records)} chunks ({index.dimensions}-dim vectors).")

    uri = resolve_connection_uri()
    inserted = index.upsert_records(uri, table_name=args.table)
    print(f"Persisted {inserted} metadata chunks into {args.table}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
