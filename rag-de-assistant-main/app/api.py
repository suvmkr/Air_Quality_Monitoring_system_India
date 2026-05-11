"""
api.py  –  FastAPI backend exposing REST endpoints consumed by the
           Streamlit frontend (and any other client).

Run:
    cd rag-de-assistant
    uvicorn app.api:app --host 0.0.0.0 --port 8502 --reload
"""

from __future__ import annotations
import asyncio, time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from loguru import logger

from app.config import get_settings
from app.auth import create_session, require_auth
from rag.retriever import Retriever
from agents.quality_agent import QualityAgent
from agents.pipeline_agent import PipelineAgent
from agents.catalog_agent import CatalogAgent
from monitoring.health_checker import HealthChecker
from monitoring.sla_tracker import SLATracker
from monitoring.failure_logs import FailureLogs

cfg = get_settings()

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="RAG-Powered DE Assistant API",
    description="Conversational assistant for Data Engineers – codebase Q&A, catalogue, health.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Lazy singletons ───────────────────────────────────────────────────────────
_retriever: Optional[Retriever] = None
_quality_agent: Optional[QualityAgent] = None
_pipeline_agent: Optional[PipelineAgent] = None
_catalog_agent: Optional[CatalogAgent] = None


def get_retriever() -> Retriever:
    global _retriever
    if _retriever is None:
        _retriever = Retriever()
    return _retriever


def get_quality_agent() -> QualityAgent:
    global _quality_agent
    if _quality_agent is None:
        _quality_agent = QualityAgent()
    return _quality_agent


def get_pipeline_agent() -> PipelineAgent:
    global _pipeline_agent
    if _pipeline_agent is None:
        _pipeline_agent = PipelineAgent()
    return _pipeline_agent


def get_catalog_agent() -> CatalogAgent:
    global _catalog_agent
    if _catalog_agent is None:
        _catalog_agent = CatalogAgent()
    return _catalog_agent


# ── Pydantic Schemas ──────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    mode: str = Field("auto", pattern="^(auto|code|catalog|health)$")
    history: List[Dict[str, str]] = Field(default_factory=list)


class ChatResponse(BaseModel):
    answer: str
    sources: List[Dict[str, Any]] = []
    mode_used: str
    latency_ms: float
    agent_actions: List[str] = []


class QualityCheckRequest(BaseModel):
    pipeline_name: str
    run_id: Optional[str] = None


class TokenResponse(BaseModel):
    token: str
    expires_in_seconds: int = 28800


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/auth/token", response_model=TokenResponse, tags=["Auth"])
async def get_token(user: str = "de-user"):
    """Issue a session token (no password needed in dev; add LDAP in prod)."""
    token = create_session(user)
    return TokenResponse(token=token)


# ── Core chat endpoint ────────────────────────────────────────────────────────

@app.post("/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(
    req: ChatRequest,
    session: dict = Depends(require_auth),
):
    t0 = time.time()
    retriever = get_retriever()
    question = req.question.strip()
    agent_actions: List[str] = []

    # ── Route to appropriate collection(s) ──────────────────────────────────
    mode = req.mode
    if mode == "auto":
        mode = _classify_question(question)

    logger.info(f"[chat] user={session['user']} mode={mode} q={question[:80]}")

    if mode == "code":
        docs = retriever.query(question, collection=cfg.collection_code, k=cfg.top_k)
    elif mode == "catalog":
        docs = retriever.query(question, collection=cfg.collection_metadata, k=cfg.top_k)
    elif mode == "health":
        docs = retriever.query(question, collection=cfg.collection_docs, k=cfg.top_k // 2)
        # Also pull live monitoring data
        hc = HealthChecker()
        health_summary = hc.get_summary()
        agent_actions.append(f"HealthChecker.get_summary() → {len(health_summary)} pipelines")
    else:
        docs = retriever.query_all(question, k=cfg.top_k)

    # ── Build answer via PipelineAgent ───────────────────────────────────────
    p_agent = get_pipeline_agent()
    answer = await p_agent.answer(
        question=question,
        retrieved_docs=docs,
        history=req.history,
        extra_context=health_summary if mode == "health" else None,
    )

    sources = [
        {
            "id": d.get("id", ""),
            "source": d.get("metadata", {}).get("source", "unknown"),
            "score": round(d.get("score", 0.0), 4),
            "preview": d.get("document", "")[:200],
        }
        for d in docs
    ]

    return ChatResponse(
        answer=answer,
        sources=sources,
        mode_used=mode,
        latency_ms=round((time.time() - t0) * 1000, 1),
        agent_actions=agent_actions,
    )


# ── Agentic quality-check endpoint ───────────────────────────────────────────

@app.post("/agents/quality-check", tags=["Agents"])
async def trigger_quality_check(
    req: QualityCheckRequest,
    background_tasks: BackgroundTasks,
    session: dict = Depends(require_auth),
):
    """Trigger an agentic quality check on a pipeline (runs in background)."""
    agent = get_quality_agent()
    background_tasks.add_task(
        agent.run_check, req.pipeline_name, req.run_id
    )
    logger.info(f"[quality-check] triggered for {req.pipeline_name} run={req.run_id}")
    return {
        "status": "queued",
        "pipeline": req.pipeline_name,
        "message": f"Quality check for '{req.pipeline_name}' queued. Poll /monitoring/health for results.",
    }


# ── Catalog endpoints ─────────────────────────────────────────────────────────

@app.get("/catalog/tables", tags=["Catalog"])
async def list_tables(
    search: Optional[str] = None,
    session: dict = Depends(require_auth),
):
    agent = get_catalog_agent()
    return await agent.list_tables(search=search)


@app.get("/catalog/tables/{table_name}", tags=["Catalog"])
async def get_table_info(
    table_name: str,
    session: dict = Depends(require_auth),
):
    agent = get_catalog_agent()
    return await agent.get_table_details(table_name)


@app.get("/catalog/pii", tags=["Catalog"])
async def get_pii_tables(session: dict = Depends(require_auth)):
    agent = get_catalog_agent()
    return await agent.get_pii_tagged_tables()


# ── Monitoring endpoints ──────────────────────────────────────────────────────

@app.get("/monitoring/health", tags=["Monitoring"])
async def pipeline_health(session: dict = Depends(require_auth)):
    hc = HealthChecker()
    return hc.get_full_report()


@app.get("/monitoring/sla", tags=["Monitoring"])
async def sla_report(session: dict = Depends(require_auth)):
    tracker = SLATracker()
    return tracker.get_report()


@app.get("/monitoring/failures", tags=["Monitoring"])
async def recent_failures(
    limit: int = 20,
    session: dict = Depends(require_auth),
):
    fl = FailureLogs()
    return fl.get_recent(limit=limit)


# ── Ingestion management ──────────────────────────────────────────────────────

@app.post("/ingest/trigger", tags=["Ingestion"])
async def trigger_ingestion(
    background_tasks: BackgroundTasks,
    session: dict = Depends(require_auth),
):
    """Re-index all documents into ChromaDB."""
    from ingestion.metadata_ingest import MetadataIngestor
    from ingestion.code_parser import CodeParser
    from ingestion.docs_loader import DocsLoader

    async def _run():
        DocsLoader().ingest()
        CodeParser().ingest()
        MetadataIngestor().ingest()

    background_tasks.add_task(_run)
    return {"status": "ingestion_started", "message": "Check /monitoring/health for progress."}


# ── Health ping ───────────────────────────────────────────────────────────────

@app.get("/ping", tags=["Infra"])
async def ping():
    return {"status": "ok", "model": cfg.claude_model}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _classify_question(q: str) -> str:
    """Heuristic router – replace with an LLM classifier if needed."""
    q_lower = q.lower()
    if any(k in q_lower for k in ["schema", "table", "column", "pii", "lineage", "catalog", "dataset"]):
        return "catalog"
    if any(k in q_lower for k in ["fail", "slo", "sla", "health", "status", "alert", "broken", "lag"]):
        return "health"
    if any(k in q_lower for k in ["code", "function", "class", "import", "pipeline", "dag", "task", "logic", "def "]):
        return "code"
    return "code"  # default to codebase Q&A
