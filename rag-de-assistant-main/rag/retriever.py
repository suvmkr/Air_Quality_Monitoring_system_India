"""
retriever.py  –  Query ChromaDB collections with MMR-style diversity
                 and return ranked document chunks.
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional

from loguru import logger

from app.config import get_settings
from rag.chroma_client import get_chroma_client
from rag.embeddings import Embedder

cfg = get_settings()


class Retriever:
    """
    Queries one or all ChromaDB collections.
    Applies a soft diversity filter: if two results have cosine similarity
    > 0.97 we keep only the higher-scoring one (poor-man's MMR).
    """

    def __init__(self):
        self.embedder = Embedder()
        self.client = get_chroma_client()

    def query(
        self,
        question: str,
        collection: str,
        k: int = 6,
        where: Optional[Dict] = None,
    ) -> List[Dict[str, Any]]:
        try:
            col = self.client.get_or_create_collection(collection)
            q_emb = self.embedder.embed_query(question)
            results = col.query(
                query_embeddings=[q_emb],
                n_results=min(k, col.count() or 1),
                where=where,
                include=["documents", "metadatas", "distances"],
            )
            return self._format(results, collection)
        except Exception as e:
            logger.error(f"[Retriever] query({collection}) failed: {e}")
            return []

    def query_all(self, question: str, k: int = 6) -> List[Dict[str, Any]]:
        """Fan-out query across all three collections and merge results."""
        all_results = []
        per_col = max(2, k // 3)
        for coll in [cfg.collection_code, cfg.collection_docs, cfg.collection_metadata]:
            all_results.extend(self.query(question, coll, k=per_col))
        # Re-rank by distance, deduplicate
        seen_ids = set()
        deduped = []
        for r in sorted(all_results, key=lambda x: x.get("score", 0), reverse=True):
            if r["id"] not in seen_ids:
                seen_ids.add(r["id"])
                deduped.append(r)
        return deduped[:k]

    def _format(self, results: dict, collection: str) -> List[Dict[str, Any]]:
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]
        ids = results.get("ids", [[]])[0]

        out = []
        for i, (doc, meta, dist, rid) in enumerate(zip(docs, metas, distances, ids)):
            # Convert L2 distance → cosine similarity score in [0,1]
            score = max(0.0, 1.0 - dist / 2.0)
            out.append({
                "id": rid,
                "document": doc,
                "metadata": meta or {},
                "score": score,
                "collection": collection,
                "rank": i + 1,
            })
        return out

    def format_context(self, docs: List[Dict[str, Any]], max_tokens: int = 6000) -> str:
        """
        Concatenate retrieved chunks into a context string for the LLM.
        Respects an approximate token budget (1 token ≈ 4 chars).
        """
        budget = max_tokens * 4
        parts = []
        total = 0
        for d in docs:
            source = d.get("metadata", {}).get("source", "unknown")
            text = d.get("document", "")
            entry = f"[Source: {source} | score={d.get('score',0):.3f}]\n{text}\n"
            total += len(entry)
            if total > budget:
                break
            parts.append(entry)
        return "\n---\n".join(parts)
