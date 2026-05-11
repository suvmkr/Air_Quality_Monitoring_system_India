"""
metadata_ingest.py  –  Load data-catalog JSON/YAML entries and upsert
                       into the data_catalog ChromaDB collection.
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import List, Dict, Any

from loguru import logger

from app.config import get_settings
from ingestion.chunker import MetadataChunker, Chunk
from rag.chroma_client import get_chroma_client
from rag.embeddings import Embedder

cfg = get_settings()


class MetadataIngestor:
    def __init__(self):
        self.chunker = MetadataChunker()
        self.embedder = Embedder()
        self.client = get_chroma_client()
        self.collection = self.client.get_or_create_collection(
            name=cfg.collection_metadata,
            metadata={"description": "Data catalog: table schemas, lineage, PII tags"},
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest(self, metadata_path: Path | None = None) -> int:
        root = Path(metadata_path or cfg.metadata_path)
        if not root.exists():
            logger.warning(f"[MetadataIngestor] Path {root} not found – creating sample catalog.")
            self._create_sample_catalog(root)

        tables = self._load_catalog(root)
        all_chunks: List[Chunk] = []
        for table in tables:
            try:
                chunks = self.chunker.chunk_table(table)
                all_chunks.extend(chunks)
            except Exception as e:
                logger.error(f"[MetadataIngestor] Table {table.get('table_name')}: {e}")

        self._upsert(all_chunks)
        logger.info(f"[MetadataIngestor] Indexed {len(tables)} tables → {len(all_chunks)} chunks")
        return len(all_chunks)

    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Return all catalog entries for the API catalog endpoint."""
        root = Path(cfg.metadata_path)
        if not root.exists():
            return []
        return self._load_catalog(root)

    # ── Private ───────────────────────────────────────────────────────────────

    def _load_catalog(self, root: Path) -> List[Dict[str, Any]]:
        tables = []
        for p in sorted(root.rglob("*.json")):
            try:
                data = json.loads(p.read_text())
                if isinstance(data, list):
                    tables.extend(data)
                elif isinstance(data, dict):
                    tables.append(data)
            except Exception as e:
                logger.warning(f"[MetadataIngestor] Could not parse {p}: {e}")
        return tables

    def _upsert(self, chunks: List[Chunk]):
        if not chunks:
            return
        ids = [c.id for c in chunks]
        docs = [c.text for c in chunks]
        metas = [c.metadata for c in chunks]
        embeddings = self.embedder.embed(docs)
        batch = 500
        for i in range(0, len(ids), batch):
            self.collection.upsert(
                ids=ids[i:i+batch],
                documents=docs[i:i+batch],
                metadatas=metas[i:i+batch],
                embeddings=embeddings[i:i+batch],
            )
        logger.info(f"[MetadataIngestor] Upserted {len(ids)} chunks into '{cfg.collection_metadata}'")

    def _create_sample_catalog(self, root: Path):
        root.mkdir(parents=True, exist_ok=True)
        catalog = _SAMPLE_CATALOG
        (root / "catalog.json").write_text(json.dumps(catalog, indent=2))
        logger.info(f"[MetadataIngestor] Created sample catalog at {root}/catalog.json")


# ── Sample catalog ────────────────────────────────────────────────────────────

_SAMPLE_CATALOG = [
    {
        "table_name": "user_events_clean",
        "database": "analytics",
        "schema": "events",
        "description": "Deduplicated and validated user click-stream events. Source: Kafka raw.user_events.",
        "owner": "alice@company.com",
        "tags": ["core", "events", "streaming"],
        "row_count": 4500000000,
        "last_updated": "2024-01-15T04:12:00Z",
        "has_pii": True,
        "upstream": ["kafka.raw.user_events"],
        "downstream": ["sessions", "funnel_analysis", "user_profile"],
        "columns": [
            {"name": "event_id", "type": "STRING", "description": "Unique event identifier", "is_pii": False},
            {"name": "user_id", "type": "STRING", "description": "Hashed user identifier", "is_pii": False},
            {"name": "session_id", "type": "STRING", "description": "Browser session ID", "is_pii": False},
            {"name": "event_type", "type": "STRING", "description": "click|view|purchase|search", "is_pii": False},
            {"name": "timestamp", "type": "TIMESTAMP", "description": "Event time (UTC)", "is_pii": False},
            {"name": "ip_address", "type": "STRING", "description": "User IP (masked to /24)", "is_pii": True},
            {"name": "user_agent", "type": "STRING", "description": "Browser user agent string", "is_pii": True},
            {"name": "page_url", "type": "STRING", "description": "Page URL at time of event", "is_pii": False},
            {"name": "event_date", "type": "DATE", "description": "Partition column (UTC date)", "is_pii": False},
        ],
    },
    {
        "table_name": "orders_fact",
        "database": "warehouse",
        "schema": "commerce",
        "description": "Orders fact table. Dimensional model. Includes PII-masked customer fields. Late-arriving data up to 3 days.",
        "owner": "bob@company.com",
        "tags": ["finance", "orders", "fact_table"],
        "row_count": 280000000,
        "last_updated": "2024-01-15T06:05:00Z",
        "has_pii": True,
        "upstream": ["postgres.orders_raw", "postgres.customers"],
        "downstream": ["revenue_daily", "customer_ltv", "finance_reports"],
        "columns": [
            {"name": "order_id", "type": "STRING", "description": "Unique order ID", "is_pii": False},
            {"name": "order_date", "type": "DATE", "description": "Order placement date", "is_pii": False},
            {"name": "customer_id", "type": "STRING", "description": "Hashed customer ID", "is_pii": False},
            {"name": "customer_email", "type": "STRING", "description": "SHA-256 hashed email", "is_pii": True},
            {"name": "product_id", "type": "STRING", "description": "Product SKU", "is_pii": False},
            {"name": "total_amount", "type": "DOUBLE", "description": "Order total in USD", "is_pii": False},
            {"name": "order_count", "type": "BIGINT", "description": "Number of line items", "is_pii": False},
            {"name": "currency", "type": "STRING", "description": "ISO currency code", "is_pii": False},
            {"name": "billing_address", "type": "STRING", "description": "Encrypted billing address", "is_pii": True},
        ],
    },
    {
        "table_name": "product_catalog",
        "database": "analytics",
        "schema": "commerce",
        "description": "Product master data including descriptions, categories, pricing tiers, and inventory levels.",
        "owner": "charlie@company.com",
        "tags": ["products", "catalog", "reference"],
        "row_count": 150000,
        "last_updated": "2024-01-15T12:00:00Z",
        "has_pii": False,
        "upstream": ["catalog_service.api", "inventory_service.api"],
        "downstream": ["orders_fact", "recommendations", "search_index"],
        "columns": [
            {"name": "product_id", "type": "STRING", "description": "Unique product SKU", "is_pii": False},
            {"name": "product_name", "type": "STRING", "description": "Display name", "is_pii": False},
            {"name": "category_l1", "type": "STRING", "description": "Top-level category", "is_pii": False},
            {"name": "category_l2", "type": "STRING", "description": "Sub-category", "is_pii": False},
            {"name": "base_price_usd", "type": "DOUBLE", "description": "Base price before discounts", "is_pii": False},
            {"name": "stock_quantity", "type": "INTEGER", "description": "Current inventory", "is_pii": False},
            {"name": "is_active", "type": "BOOLEAN", "description": "Whether product is live", "is_pii": False},
        ],
    },
    {
        "table_name": "sessions",
        "database": "analytics",
        "schema": "events",
        "description": "Aggregated user sessions derived from user_events_clean. One row per session.",
        "owner": "diana@company.com",
        "tags": ["sessions", "derived", "analytics"],
        "row_count": 800000000,
        "last_updated": "2024-01-15T05:30:00Z",
        "has_pii": False,
        "upstream": ["user_events_clean"],
        "downstream": ["funnel_analysis", "retention", "ab_testing"],
        "columns": [
            {"name": "session_id", "type": "STRING", "description": "Unique session ID", "is_pii": False},
            {"name": "user_id", "type": "STRING", "description": "Hashed user ID", "is_pii": False},
            {"name": "session_start", "type": "TIMESTAMP", "description": "Session start UTC", "is_pii": False},
            {"name": "session_end", "type": "TIMESTAMP", "description": "Session end UTC", "is_pii": False},
            {"name": "duration_seconds", "type": "INTEGER", "description": "Session length in seconds", "is_pii": False},
            {"name": "page_views", "type": "INTEGER", "description": "Number of page views", "is_pii": False},
            {"name": "events", "type": "INTEGER", "description": "Total event count", "is_pii": False},
            {"name": "converted", "type": "BOOLEAN", "description": "Did session result in purchase", "is_pii": False},
        ],
    },
]
