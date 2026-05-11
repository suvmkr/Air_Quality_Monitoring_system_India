"""
config.py  –  Central configuration loaded from .env
All modules import Settings() from here; never read os.environ directly.
"""

from __future__ import annotations
import os
from functools import lru_cache
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings   # pip install pydantic-settings


ROOT = Path(__file__).resolve().parent.parent   # project root


class Settings(BaseSettings):
    # ── Anthropic ────────────────────────────────────────────
    anthropic_api_key: str = Field(..., env="ANTHROPIC_API_KEY")
    claude_model: str = Field("claude-sonnet-4-20250514", env="CLAUDE_MODEL")

    # ── ChromaDB ─────────────────────────────────────────────
    chroma_host: str = Field("localhost", env="CHROMA_HOST")
    chroma_port: int = Field(8000, env="CHROMA_PORT")
    chroma_persist_dir: Path = Field(ROOT / "data/chroma_db", env="CHROMA_PERSIST_DIR")

    # ── Collections ──────────────────────────────────────────
    collection_code: str = Field("pipeline_code", env="COLLECTION_CODE")
    collection_docs: str = Field("pipeline_docs", env="COLLECTION_DOCS")
    collection_metadata: str = Field("data_catalog", env="COLLECTION_METADATA")

    # ── Embeddings ───────────────────────────────────────────
    embedding_model: str = Field("all-MiniLM-L6-v2", env="EMBEDDING_MODEL")
    embedding_device: str = Field("cpu", env="EMBEDDING_DEVICE")

    # ── Paths ────────────────────────────────────────────────
    pipeline_repo_path: Path = Field(ROOT / "data/sample_pipelines", env="PIPELINE_REPO_PATH")
    metadata_path: Path = Field(ROOT / "data/sample_metadata", env="METADATA_PATH")

    # ── App ──────────────────────────────────────────────────
    app_host: str = Field("0.0.0.0", env="APP_HOST")
    app_port: int = Field(8501, env="APP_PORT")
    api_port: int = Field(8502, env="API_PORT")
    secret_key: str = Field("change-this-in-production", env="SECRET_KEY")

    # ── Monitoring ───────────────────────────────────────────
    sla_threshold_hours: int = Field(2, env="SLA_THRESHOLD_HOURS")
    failure_alert_email: str = Field("de-team@company.com", env="FAILURE_ALERT_EMAIL")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    # ── RAG Tuning ───────────────────────────────────────────
    top_k: int = 6
    chunk_size: int = 800
    chunk_overlap: int = 120
    max_context_tokens: int = 8000

    class Config:
        env_file = ROOT / ".env"
        env_file_encoding = "utf-8"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
