"""
catalog_agent.py  –  Data-catalog query agent.
Provides structured access to table metadata, lineage, and PII info.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

from loguru import logger

from app.config import get_settings
from ingestion.metadata_ingest import MetadataIngestor

cfg = get_settings()


class CatalogAgent:
    def __init__(self):
        self._ingestor = MetadataIngestor()
        self._cache: Optional[List[Dict]] = None

    def _get_tables(self) -> List[Dict]:
        if self._cache is None:
            self._cache = self._ingestor.get_all_tables()
        return self._cache

    async def list_tables(self, search: Optional[str] = None) -> Dict[str, Any]:
        tables = self._get_tables()
        if search:
            q = search.lower()
            tables = [
                t for t in tables
                if q in t.get("table_name", "").lower()
                or q in t.get("description", "").lower()
                or any(q in tag.lower() for tag in t.get("tags", []))
            ]
        # Return summary (no column details to keep response small)
        summary = [
            {
                "table_name": t.get("table_name"),
                "database": t.get("database"),
                "schema": t.get("schema"),
                "description": t.get("description", "")[:120],
                "owner": t.get("owner"),
                "row_count": t.get("row_count"),
                "has_pii": t.get("has_pii", False),
                "last_updated": t.get("last_updated"),
                "tags": t.get("tags", []),
            }
            for t in tables
        ]
        return {"total": len(summary), "tables": summary}

    async def get_table_details(self, table_name: str) -> Dict[str, Any]:
        tables = self._get_tables()
        for t in tables:
            if t.get("table_name", "").lower() == table_name.lower():
                return t
        return {"error": f"Table '{table_name}' not found in catalog."}

    async def get_pii_tagged_tables(self) -> Dict[str, Any]:
        tables = self._get_tables()
        pii_tables = []
        for t in tables:
            if t.get("has_pii"):
                pii_cols = [
                    c for c in t.get("columns", [])
                    if c.get("is_pii")
                ]
                pii_tables.append({
                    "table_name": t.get("table_name"),
                    "database": t.get("database"),
                    "schema": t.get("schema"),
                    "pii_columns": pii_cols,
                    "owner": t.get("owner"),
                })
        return {"total_pii_tables": len(pii_tables), "pii_tables": pii_tables}
