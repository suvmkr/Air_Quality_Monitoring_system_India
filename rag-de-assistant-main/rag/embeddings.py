"""
embeddings.py  –  Wraps sentence-transformers for local embedding generation.
Uses all-MiniLM-L6-v2 (384-dim) by default – fast, good quality.
Switch to all-mpnet-base-v2 for higher accuracy at 2× cost.
"""

from __future__ import annotations
from functools import lru_cache
from typing import List

from loguru import logger


class Embedder:
    """
    Lazy-loads the sentence-transformer model on first call.
    Thread-safe; reuses a single model instance per process.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._model = None
        return cls._instance

    def _load_model(self):
        if self._model is None:
            from app.config import get_settings
            from sentence_transformers import SentenceTransformer
            cfg = get_settings()
            logger.info(f"[Embedder] Loading {cfg.embedding_model} on {cfg.embedding_device}…")
            self._model = SentenceTransformer(cfg.embedding_model, device=cfg.embedding_device)
            logger.info("[Embedder] Model loaded ✓")
        return self._model

    def embed(self, texts: List[str], batch_size: int = 64) -> List[List[float]]:
        """
        Embed a list of strings.
        Returns a list of float vectors.
        Handles empty-string edge cases gracefully.
        """
        if not texts:
            return []
        # Replace empty strings to avoid zero-vectors
        cleaned = [t if t.strip() else "[empty]" for t in texts]
        model = self._load_model()
        vectors = model.encode(
            cleaned,
            batch_size=batch_size,
            show_progress_bar=len(cleaned) > 200,
            normalize_embeddings=True,     # cosine similarity works better
        )
        return vectors.tolist()

    def embed_query(self, query: str) -> List[float]:
        """Embed a single query string."""
        return self.embed([query])[0]

    @property
    def dimension(self) -> int:
        return self._load_model().get_sentence_embedding_dimension()
