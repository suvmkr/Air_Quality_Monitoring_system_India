"""
chunker.py  –  Smart text splitting for code, docs, and metadata.
Uses recursive character splitting with language-aware separators.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any

from langchain_text_splitters import (
    RecursiveCharacterTextSplitter,
    Language,
)
from loguru import logger

from app.config import get_settings

cfg = get_settings()


@dataclass
class Chunk:
    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        import hashlib
        return hashlib.md5(self.text.encode()).hexdigest()[:16]


class CodeChunker:
    """Language-aware splitter for Python / SQL."""

    # We define SQL separators manually because LangChain's from_language 
    # surprisingly doesn't support "sql" yet.
    SQL_SEPARATORS = [
        "\nCREATE TABLE ",
        "\nINSERT INTO ",
        "\nSELECT ",
        "\nWITH ",
        "\n\n",
        "\n",
        " ",
        "",
    ]

    def __init__(self):
        self.python_splitter = RecursiveCharacterTextSplitter.from_language(
            language=Language.PYTHON,
            chunk_size=cfg.chunk_size,
            chunk_overlap=cfg.chunk_overlap,
        )
        
        self.sql_splitter = RecursiveCharacterTextSplitter(
            separators=self.SQL_SEPARATORS,
            chunk_size=cfg.chunk_size,
            chunk_overlap=cfg.chunk_overlap,
        )
        
        self._default_splitter = RecursiveCharacterTextSplitter(
            chunk_size=cfg.chunk_size, 
            chunk_overlap=cfg.chunk_overlap
        )

    def chunk(self, text: str, metadata: Dict[str, Any]) -> List[Chunk]:
        ext = metadata.get("extension", "").lower()
        
        if ext == ".py":
            splitter = self.python_splitter
        elif ext == ".sql":
            splitter = self.sql_splitter
        else:
            splitter = self._default_splitter
            
        pieces = splitter.split_text(text)
        return [
            Chunk(text=p, metadata={**metadata, "chunk_index": i, "total_chunks": len(pieces)})
            for i, p in enumerate(pieces)
        ]


class DocChunker:
    """Splitter for markdown / plain-text documentation."""

    def __init__(self):
        self._splitter = RecursiveCharacterTextSplitter(
            separators=["\n## ", "\n### ", "\n\n", "\n", " "],
            chunk_size=cfg.chunk_size,
            chunk_overlap=cfg.chunk_overlap,
        )

    def chunk(self, text: str, metadata: Dict[str, Any]) -> List[Chunk]:
        pieces = self._splitter.split_text(text)
        logger.debug(f"[DocChunker] {len(pieces)} chunks from {metadata.get('source','?')}")
        return [
            Chunk(text=p, metadata={**metadata, "chunk_index": i, "total_chunks": len(pieces)})
            for i, p in enumerate(pieces)
        ]


class MetadataChunker:
    """
    Converts structured metadata (dict / JSON) into human-readable
    text chunks that embed well.
    """

    def chunk_table(self, table: Dict[str, Any]) -> List[Chunk]:
        """Turn one catalog entry into 1-3 descriptive text chunks."""
        name = table.get("table_name", "unknown")
        db = table.get("database", "")
        schema = table.get("schema", "")
        description = table.get("description", "No description.")
        owner = table.get("owner", "?")
        tags = ", ".join(table.get("tags", []))
        columns = table.get("columns", [])

        # Chunk 1: Table summary
        summary = (
            f"Table: {db}.{schema}.{name}\n"
            f"Description: {description}\n"
            f"Owner: {owner}\n"
            f"Tags: {tags}\n"
            f"Row count: {table.get('row_count', 'unknown')}\n"
            f"Last updated: {table.get('last_updated', 'unknown')}\n"
            f"PII: {table.get('has_pii', False)}"
        )

        chunks = [Chunk(text=summary, metadata={
            "source": f"catalog/{name}",
            "type": "catalog_summary",
            "table_name": name,
            "has_pii": table.get("has_pii", False),
        })]

        # Chunk 2+: Column details (batched in groups of 10)
        if columns:
            batch = []
            for col in columns:
                pii_flag = " [PII]" if col.get("is_pii") else ""
                batch.append(
                    f"  - {col.get('name','?')} ({col.get('type','?')}){pii_flag}: {col.get('description','')}"
                )
            col_text = f"Columns for {name}:\n" + "\n".join(batch)
            chunks.append(Chunk(text=col_text, metadata={
                "source": f"catalog/{name}/columns",
                "type": "catalog_columns",
                "table_name": name,
            }))

        # Chunk 3: Lineage
        if table.get("upstream") or table.get("downstream"):
            lineage = (
                f"Lineage for {name}:\n"
                f"  Upstream: {', '.join(table.get('upstream', []))}\n"
                f"  Downstream: {', '.join(table.get('downstream', []))}"
            )
            chunks.append(Chunk(text=lineage, metadata={
                "source": f"catalog/{name}/lineage",
                "type": "catalog_lineage",
                "table_name": name,
            }))

        return chunks