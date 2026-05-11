"""
chroma_client.py  –  ChromaDB client factory.
Tries HTTP client first (for a running Chroma server),
falls back to persistent local client for development.
"""

from __future__ import annotations
from functools import lru_cache
from typing import Optional

import chromadb
from chromadb.config import Settings as ChromaSettings
from loguru import logger

from app.config import get_settings


@lru_cache(maxsize=1)
def get_chroma_client() -> chromadb.ClientAPI:
    cfg = get_settings()

    # Try HTTP client (production / Docker setup)
    try:
        client = chromadb.HttpClient(
            host=cfg.chroma_host,
            port=cfg.chroma_port,
            settings=ChromaSettings(anonymized_telemetry=False),
        )
        client.heartbeat()          # raises if not reachable
        logger.info(f"[ChromaDB] Connected to HTTP server at {cfg.chroma_host}:{cfg.chroma_port}")
        return client
    except Exception:
        logger.info("[ChromaDB] HTTP server not available – using persistent local client.")

    # Fallback: local persistent client
    persist_dir = str(cfg.chroma_persist_dir)
    import os; os.makedirs(persist_dir, exist_ok=True)
    client = chromadb.PersistentClient(
        path=persist_dir,
        settings=ChromaSettings(anonymized_telemetry=False),
    )
    logger.info(f"[ChromaDB] Using PersistentClient at {persist_dir}")
    return client


def reset_collections():
    """Drop and recreate all collections (useful for re-ingestion)."""
    cfg = get_settings()
    client = get_chroma_client()
    for name in [cfg.collection_code, cfg.collection_docs, cfg.collection_metadata]:
        try:
            client.delete_collection(name)
            logger.info(f"[ChromaDB] Deleted collection '{name}'")
        except Exception:
            pass
