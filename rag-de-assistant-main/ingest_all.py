"""
ingest_all.py  –  Bootstrap script: creates sample data and indexes
                  everything into ChromaDB.

Run ONCE before starting the API:
    cd rag-de-assistant
    python ingest_all.py
"""

import sys
from pathlib import Path

# Make sure project root is on PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
from dotenv import load_dotenv

load_dotenv()

from rag.chroma_client import reset_collections
from ingestion.docs_loader import DocsLoader
from ingestion.code_parser import CodeParser
from ingestion.metadata_ingest import MetadataIngestor


def main():
    logger.info("=" * 60)
    logger.info("  RAG-Powered DE Assistant — Ingestion Bootstrap")
    logger.info("=" * 60)

    logger.info("Step 1/4 — Resetting ChromaDB collections…")
    reset_collections()

    logger.info("Step 2/4 — Ingesting pipeline documentation…")
    docs_count = DocsLoader().ingest()
    logger.success(f"  ✓ {docs_count} doc chunks indexed")

    logger.info("Step 3/4 — Ingesting pipeline source code…")
    code_count = CodeParser().ingest()
    logger.success(f"  ✓ {code_count} code chunks indexed")

    logger.info("Step 4/4 — Ingesting data catalog metadata…")
    meta_count = MetadataIngestor().ingest()
    logger.success(f"  ✓ {meta_count} catalog chunks indexed")

    total = docs_count + code_count + meta_count
    logger.success(f"\n🎉 Ingestion complete — {total} total chunks indexed into ChromaDB.")
    logger.info("\nNext steps:")
    logger.info("  1. Start API:       uvicorn app.api:app --port 8502 --reload")
    logger.info("  2. Start Streamlit: streamlit run app/streamlit_app.py")
    logger.info("  3. Open browser:    http://localhost:8501")


if __name__ == "__main__":
    main()
