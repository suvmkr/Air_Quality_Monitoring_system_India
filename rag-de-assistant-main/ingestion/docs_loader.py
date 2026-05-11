"""
docs_loader.py  –  Load pipeline documentation (Markdown, TXT, RST)
                   and upsert into the pipeline_docs ChromaDB collection.
"""

from __future__ import annotations
from pathlib import Path
from typing import List

from loguru import logger

from app.config import get_settings
from ingestion.chunker import DocChunker, Chunk
from rag.chroma_client import get_chroma_client
from rag.embeddings import Embedder

cfg = get_settings()
DOC_EXTS = {".md", ".txt", ".rst", ".html"}


class DocsLoader:
    def __init__(self):
        self.chunker = DocChunker()
        self.embedder = Embedder()
        self.client = get_chroma_client()
        self.collection = self.client.get_or_create_collection(
            name=cfg.collection_docs,
            metadata={"description": "Pipeline design docs, runbooks, and READMEs"},
        )

    def ingest(self, docs_root: Path | None = None) -> int:
        root = Path(docs_root or cfg.pipeline_repo_path)
        if not root.exists():
            logger.warning(f"[DocsLoader] Path {root} missing – creating sample docs.")
            self._create_sample_docs(root)

        all_chunks: List[Chunk] = []
        for p in sorted(root.rglob("*")):
            if p.is_file() and p.suffix in DOC_EXTS:
                try:
                    text = p.read_text(errors="replace")
                    rel = str(p.relative_to(root))
                    chunks = self.chunker.chunk(text, {
                        "source": rel,
                        "filename": p.name,
                        "type": "documentation",
                        "extension": p.suffix,
                    })
                    all_chunks.extend(chunks)
                except Exception as e:
                    logger.error(f"[DocsLoader] {p}: {e}")

        self._upsert(all_chunks)
        logger.info(f"[DocsLoader] Indexed {len(all_chunks)} doc chunks.")
        return len(all_chunks)

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
        logger.info(f"[DocsLoader] Upserted {len(ids)} chunks into '{cfg.collection_docs}'")

    def _create_sample_docs(self, root: Path):
        root.mkdir(parents=True, exist_ok=True)
        docs_dir = root / "docs"
        docs_dir.mkdir(exist_ok=True)
        (docs_dir / "pipeline_overview.md").write_text(_SAMPLE_OVERVIEW_MD)
        (docs_dir / "runbook.md").write_text(_SAMPLE_RUNBOOK_MD)
        (docs_dir / "sla_policy.md").write_text(_SAMPLE_SLA_MD)
        logger.info(f"[DocsLoader] Created sample docs in {docs_dir}")


_SAMPLE_OVERVIEW_MD = """# Data Pipeline Overview

## Architecture

Our data platform processes ~500M events/day across three major pipelines:

### user_events pipeline
- **Source**: Kafka topic `raw.user_events`
- **Transform**: Deduplication (24h window), schema validation
- **Sink**: BigQuery `dataset.user_events_clean`
- **Owner**: @alice
- **SLA**: Available by 04:00 UTC daily

### orders_fact pipeline
- **Source**: Postgres CDC via Debezium
- **Transform**: PII masking (SHA-256), late-arriving data (3-day watermark)
- **Sink**: Delta Lake `warehouse.orders_fact`
- **Owner**: @bob
- **SLA**: Available by 06:00 UTC daily

### product_catalog pipeline
- **Source**: REST API (catalog service)
- **Transform**: Denormalization, price history tracking
- **Sink**: BigQuery `dataset.product_catalog`
- **Owner**: @charlie
- **SLA**: Refreshed every 6 hours

## Data Quality Framework

Each pipeline runs the following quality checks:
1. **Row count validation** – compare against previous day ± 20%
2. **Null rate checks** – critical columns must have < 0.1% nulls
3. **Freshness check** – data must not be older than SLA window
4. **Schema drift detection** – alert on new/dropped columns
"""

_SAMPLE_RUNBOOK_MD = """# Pipeline Runbook

## Incident Response

### P1: Pipeline Failure (data not available by SLA)
1. Check Airflow UI for failed tasks
2. Review logs: `gcloud logging read "resource.labels.dag_id=daily_warehouse_refresh"`
3. Common causes:
   - Kafka consumer lag > 1M messages → restart consumer group
   - BigQuery quota exceeded → check quota dashboard
   - Schema mismatch → run `python tools/schema_check.py --pipeline <name>`
4. Escalate to #data-engineering-oncall if unresolved in 30min

### P2: Data Quality Failure
1. Identify failing check from monitoring dashboard
2. Run: `python tools/quality_check.py --table <table> --date <date>`
3. If row count anomaly: check upstream for missing data
4. If null rate spike: check schema changes in source system

## Rollback Procedure
```bash
# Reprocess a specific date partition
python tools/reprocess.py --pipeline user_events --date 2024-01-15

# Roll back Delta table to previous version
python tools/delta_rollback.py --table orders_fact --version 42
```
"""

_SAMPLE_SLA_MD = """# SLA Policy

| Pipeline | SLA Window | Owner | P1 Threshold |
|---|---|---|---|
| user_events | 04:00 UTC | @alice | 30 min breach |
| orders_fact | 06:00 UTC | @bob | 60 min breach |
| product_catalog | Every 6h | @charlie | 2h breach |
| sessions | 05:00 UTC | @diana | 45 min breach |

## Penalties
- **SLA breach < 30min**: Log only
- **SLA breach 30-60min**: Slack alert to #data-ops
- **SLA breach > 60min**: PagerDuty + email to stakeholders
- **Data quality failure**: Immediate halt + stakeholder notification
"""
