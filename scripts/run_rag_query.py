"""Run a Phase 4 metadata query from the command line."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.rag_agent.langchain_chain import run_langchain_query


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question", help="Question to ask the metadata agent.")
    parser.add_argument("--top-k", type=int, default=3, help="How many chunks to retrieve.")
    args = parser.parse_args()

    result = run_langchain_query(args.question, top_k=args.top_k)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
