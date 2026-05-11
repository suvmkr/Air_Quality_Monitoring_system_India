"""
code_parser.py  –  Walk the pipeline repository, extract Python/SQL files,
                   chunk them, and upsert into the ChromaDB code collection.
"""

from __future__ import annotations
import ast
import re
from pathlib import Path
from typing import Dict, Any, List

from loguru import logger

from app.config import get_settings
from ingestion.chunker import CodeChunker, Chunk
from rag.chroma_client import get_chroma_client
from rag.embeddings import Embedder

cfg = get_settings()
SKIP_DIRS = {".git", "__pycache__", ".venv", "node_modules", "dist", "build"}
SUPPORTED_EXTS = {".py", ".sql", ".yaml", ".yml", ".json"}


class CodeParser:
    """
    Scans PIPELINE_REPO_PATH for source files, extracts rich metadata
    (docstrings, function signatures, imports), chunks, embeds, and
    upserts into the `pipeline_code` ChromaDB collection.
    """

    def __init__(self):
        self.chunker = CodeChunker()
        self.embedder = Embedder()
        self.client = get_chroma_client()
        self.collection = self.client.get_or_create_collection(
            name=cfg.collection_code,
            metadata={"description": "Pipeline source code and configs"},
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest(self, repo_path: Path | None = None) -> int:
        root = Path(repo_path or cfg.pipeline_repo_path)
        if not root.exists():
            logger.warning(f"[CodeParser] Repo path {root} does not exist. Creating sample data.")
            self._create_sample_data(root)

        all_chunks: List[Chunk] = []
        for file_path in self._walk(root):
            try:
                chunks = self._process_file(file_path, root)
                all_chunks.extend(chunks)
            except Exception as e:
                logger.error(f"[CodeParser] Failed {file_path}: {e}")

        self._upsert(all_chunks)
        logger.info(f"[CodeParser] Indexed {len(all_chunks)} chunks from {root}")
        return len(all_chunks)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _walk(self, root: Path):
        for p in sorted(root.rglob("*")):
            if p.is_file() and p.suffix in SUPPORTED_EXTS:
                if not any(skip in p.parts for skip in SKIP_DIRS):
                    yield p

    def _process_file(self, file_path: Path, root: Path) -> List[Chunk]:
        rel = str(file_path.relative_to(root))
        text = file_path.read_text(errors="replace")
        meta = self._extract_metadata(file_path, text, rel)
        return self.chunker.chunk(text, meta)

    def _extract_metadata(self, path: Path, text: str, rel: str) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "source": rel,
            "extension": path.suffix,
            "filename": path.name,
            "size_bytes": len(text),
            "type": "code",
        }

        if path.suffix == ".py":
            meta.update(self._parse_python(text))
        elif path.suffix == ".sql":
            meta.update(self._parse_sql(text))

        return meta

    def _parse_python(self, code: str) -> Dict[str, Any]:
        info: Dict[str, Any] = {"language": "python", "functions": [], "classes": [], "imports": []}
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    info["functions"].append(node.name)
                elif isinstance(node, ast.ClassDef):
                    info["classes"].append(node.name)
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    mod = getattr(node, "module", None) or ""
                    info["imports"].append(mod)
        except SyntaxError:
            pass
        # Stringify lists so ChromaDB accepts them
        info["functions"] = ", ".join(info["functions"])
        info["classes"] = ", ".join(info["classes"])
        info["imports"] = ", ".join(filter(None, info["imports"]))
        return info

    def _parse_sql(self, sql: str) -> Dict[str, Any]:
        tables = re.findall(r"(?:FROM|JOIN|INTO|UPDATE)\s+([`\"]?\w+[`\"]?)", sql, re.IGNORECASE)
        return {"language": "sql", "referenced_tables": ", ".join(set(tables))}

    def _upsert(self, chunks: List[Chunk]):
        if not chunks:
            return
        ids = [c.id for c in chunks]
        docs = [c.text for c in chunks]
        metas = [c.metadata for c in chunks]
        embeddings = self.embedder.embed(docs)
        # ChromaDB upsert in batches of 500
        batch = 500
        for i in range(0, len(ids), batch):
            self.collection.upsert(
                ids=ids[i:i+batch],
                documents=docs[i:i+batch],
                metadatas=metas[i:i+batch],
                embeddings=embeddings[i:i+batch],
            )
        logger.info(f"[CodeParser] Upserted {len(ids)} chunks into '{cfg.collection_code}'")

    # ── Sample data creation ──────────────────────────────────────────────────

    def _create_sample_data(self, root: Path):
        root.mkdir(parents=True, exist_ok=True)
        samples = {
            "pipelines/user_events_pipeline.py": _SAMPLE_PIPELINE_PY,
            "pipelines/orders_fact_pipeline.py": _SAMPLE_ORDERS_PY,
            "sql/user_events_dedup.sql": _SAMPLE_SQL,
            "dags/daily_refresh_dag.py": _SAMPLE_DAG_PY,
        }
        for rel, content in samples.items():
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
        logger.info(f"[CodeParser] Created sample pipeline data in {root}")


# ── Sample source files ───────────────────────────────────────────────────────

_SAMPLE_PIPELINE_PY = '''"""
user_events_pipeline.py
Ingests raw click-stream events from Kafka, deduplicates, validates,
and writes to the user_events_clean table in BigQuery.
"""
import hashlib
from datetime import datetime, timedelta
from typing import Iterator, Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


DEDUP_WINDOW_HOURS = 24
REQUIRED_FIELDS = ["user_id", "event_type", "timestamp", "session_id"]


class DeduplicateEvents(beam.DoFn):
    """
    Removes duplicate events within a 24-hour window using a composite key
    of (user_id, event_type, session_id).
    Deduplication strategy: keep the FIRST occurrence (earliest timestamp).
    """

    def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        dedup_key = hashlib.md5(
            f"{element['user_id']}:{element['event_type']}:{element['session_id']}".encode()
        ).hexdigest()
        yield {**element, "_dedup_key": dedup_key}


class ValidateSchema(beam.DoFn):
    """Schema validation – emits valid records or dead-letter queue."""

    def process(self, element):
        missing = [f for f in REQUIRED_FIELDS if f not in element or element[f] is None]
        if missing:
            yield beam.pvalue.TaggedOutput("dead_letter", {**element, "_missing_fields": missing})
        else:
            yield element


def run(argv=None):
    options = PipelineOptions(argv)
    with beam.Pipeline(options=options) as p:
        raw = (
            p
            | "ReadKafka" >> beam.io.ReadFromKafka(
                consumer_config={"bootstrap.servers": "kafka:9092"},
                topics=["raw.user_events"],
            )
            | "ParseJSON" >> beam.Map(lambda x: __import__("json").loads(x[1]))
        )

        valid, dead = (
            raw
            | "ValidateSchema" >> beam.ParDo(ValidateSchema()).with_outputs("dead_letter", main="valid")
        )

        (
            valid
            | "Deduplicate" >> beam.ParDo(DeduplicateEvents())
            | "WriteBigQuery" >> beam.io.WriteToBigQuery(
                table="project:dataset.user_events_clean",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        dead | "WriteDeadLetter" >> beam.io.WriteToText("gs://bucket/dead_letter/user_events")
'''

_SAMPLE_ORDERS_PY = '''"""
orders_fact_pipeline.py
Builds the orders_fact dimensional table from raw order events.
Handles late-arriving data up to 3 days with a watermark strategy.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


WATERMARK_DAYS = 3
PII_COLUMNS = ["customer_email", "billing_address", "credit_card_last4"]


def build_orders_fact(spark: SparkSession, source_table: str, target_table: str):
    """
    Reads incremental orders from source_table, applies transformations,
    masks PII columns, and merges into target_table (Delta Lake / Iceberg).
    """

    raw_orders = spark.readStream.table(source_table)

    # Watermark for late data
    watermarked = raw_orders.withWatermark("order_timestamp", f"{WATERMARK_DAYS} days")

    # Mask PII
    masked = watermarked
    for col in PII_COLUMNS:
        if col in raw_orders.columns:
            masked = masked.withColumn(col, F.sha2(F.col(col), 256))

    # Aggregate
    orders_fact = (
        masked
        .withColumn("order_date", F.to_date("order_timestamp"))
        .groupBy("order_date", "product_id", "customer_id")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("order_id").alias("order_count"),
            F.first("currency").alias("currency"),
        )
    )

    return (
        orders_fact.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"s3://checkpoints/{target_table}")
        .table(target_table)
    )
'''

_SAMPLE_SQL = '''-- user_events_dedup.sql
-- Purpose: Final deduplication layer in BigQuery.
-- Runs daily at 02:00 UTC via Airflow.
-- Partition: event_date (DATE)
-- Clustering: user_id, event_type

CREATE OR REPLACE TABLE `project.dataset.user_events_clean_v2`
PARTITION BY event_date
CLUSTER BY user_id, event_type
AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, event_type, session_id, DATE(timestamp)
            ORDER BY timestamp ASC
        ) AS rn
    FROM `project.dataset.user_events_raw`
    WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)
SELECT
    * EXCEPT(rn, _dedup_key),
    DATE(timestamp) AS event_date
FROM ranked
WHERE rn = 1;
'''

_SAMPLE_DAG_PY = '''"""
daily_refresh_dag.py
Airflow DAG: orchestrates the daily data warehouse refresh.
Schedule: 02:00 UTC
SLA: all tasks complete within 90 minutes.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=90),
    "email_on_failure": True,
    "email": ["de-alerts@company.com"],
}

with DAG(
    dag_id="daily_warehouse_refresh",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["core", "warehouse"],
) as dag:

    ingest_user_events = BigQueryInsertJobOperator(
        task_id="ingest_user_events",
        configuration={"query": {"query": "{% include 'sql/user_events_dedup.sql' %}", "useLegacySql": False}},
    )

    build_orders_fact = PythonOperator(
        task_id="build_orders_fact",
        python_callable=lambda: print("Orders fact build triggered"),
    )

    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        http_conn_id="slack_webhook",
        message=":white_check_mark: Daily refresh completed successfully",
        trigger_rule="all_success",
    )

    ingest_user_events >> build_orders_fact >> notify_success
'''
