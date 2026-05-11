"""
streamlit_app.py  –  Streamlit chat interface for the RAG-Powered DE Assistant.

Run:
    cd rag-de-assistant
    streamlit run app/streamlit_app.py
"""

from __future__ import annotations
import json, time, requests, os
from pathlib import Path
from typing import Any

import streamlit as st
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

API_URL = os.getenv("API_URL", "http://localhost:8502")

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DE Assistant",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Sidebar */
    section[data-testid="stSidebar"] { background: #0f1117; }
    section[data-testid="stSidebar"] * { color: #e2e8f0 !important; }

    /* Chat bubbles */
    .user-bubble {
        background: #1e3a5f; color: #e2e8f0; border-radius: 12px 12px 2px 12px;
        padding: 10px 16px; margin: 6px 0 6px 20%; text-align: right;
    }
    .assistant-bubble {
        background: #1a1f2e; border: 1px solid #2d3748; color: #e2e8f0;
        border-radius: 2px 12px 12px 12px; padding: 12px 16px; margin: 6px 20% 6px 0;
    }
    .source-card {
        background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
        padding: 8px 12px; font-size: 12px; color: #8b949e; margin: 4px 0;
    }
    .metric-card {
        background: #161b22; border: 1px solid #30363d; border-radius: 10px;
        padding: 14px; text-align: center;
    }
    .metric-value { font-size: 28px; font-weight: 700; color: #58a6ff; }
    .metric-label { font-size: 12px; color: #8b949e; margin-top: 4px; }
    .badge-healthy { background: #238636; color: white; border-radius: 4px; padding: 2px 8px; font-size: 11px; }
    .badge-failing { background: #da3633; color: white; border-radius: 4px; padding: 2px 8px; font-size: 11px; }
    .badge-warning { background: #9e6a03; color: white; border-radius: 4px; padding: 2px 8px; font-size: 11px; }
    .stButton > button { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Session state init
# ─────────────────────────────────────────────────────────────────────────────
def init_state():
    defaults = {
        "messages": [],
        "token": None,
        "mode": "auto",
        "show_sources": True,
        "active_page": "Chat",
        "quality_results": {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()


# ─────────────────────────────────────────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_token() -> str | None:
    if st.session_state.token:
        return st.session_state.token
    try:
        r = requests.post(f"{API_URL}/auth/token", timeout=5)
        if r.status_code == 200:
            st.session_state.token = r.json()["token"]
            return st.session_state.token
    except Exception as e:
        st.error(f"❌ Cannot reach API at {API_URL}. Is it running? ({e})")
    return None


def api_chat(question: str, history: list) -> dict:
    token = get_token()
    if not token:
        return {"answer": "⚠️ Not connected to API.", "sources": [], "mode_used": "error", "latency_ms": 0}
    try:
        r = requests.post(
            f"{API_URL}/chat",
            headers={"Authorization": f"Bearer {token}"},
            json={"question": question, "mode": st.session_state.mode, "history": history},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"answer": f"❌ API error: {e}", "sources": [], "mode_used": "error", "latency_ms": 0}


def api_get(path: str) -> Any:
    token = get_token()
    if not token:
        return {}
    try:
        r = requests.get(f"{API_URL}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def api_post(path: str, payload: dict = None) -> Any:
    token = get_token()
    if not token:
        return {}
    try:
        r = requests.post(
            f"{API_URL}{path}",
            headers={"Authorization": f"Bearer {token}"},
            json=payload or {},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🚀 DE Assistant")
    st.markdown("---")

    st.session_state.active_page = st.radio(
        "Navigation",
        ["💬 Chat", "🗂️ Data Catalog", "📊 Pipeline Health", "🔍 Quality Checks"],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown("### ⚙️ Chat Settings")

    st.session_state.mode = st.selectbox(
        "Query Mode",
        options=["auto", "code", "catalog", "health"],
        format_func=lambda m: {
            "auto": "🤖 Auto (smart routing)",
            "code": "💻 Code / Pipeline Q&A",
            "catalog": "🗂️ Data Catalog",
            "health": "❤️ Pipeline Health",
        }.get(m, m),
    )
    st.session_state.show_sources = st.toggle("Show Source Citations", value=True)

    st.markdown("---")
    st.markdown("### 🔌 API Status")
    if st.button("Check Connection"):
        try:
            r = requests.get(f"{API_URL}/ping", timeout=3)
            if r.status_code == 200:
                st.success(f"✅ Connected | Model: {r.json().get('model','?')}")
            else:
                st.warning(f"⚠️ API returned {r.status_code}")
        except:
            st.error("❌ API offline")

    st.markdown("---")
    if st.button("🗑️ Clear Chat History"):
        st.session_state.messages = []
        st.rerun()

    st.markdown("---")
    st.caption("RAG-Powered DE Assistant v1.0")


# ─────────────────────────────────────────────────────────────────────────────
# Page: Chat
# ─────────────────────────────────────────────────────────────────────────────
def page_chat():
    st.markdown("## 💬 Data Engineering Assistant")
    st.caption("Ask about pipeline code, data catalogue, or system health. I retrieve relevant context before answering.")

    # Example prompts
    examples = [
        "How does the user_events pipeline handle deduplication?",
        "Which tables contain PII data?",
        "Show me recent pipeline failures and SLA breaches",
        "What's the schema of the orders_fact table?",
    ]
    cols = st.columns(len(examples))
    for col, ex in zip(cols, examples):
        if col.button(ex, use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": ex})
            st.rerun()

    st.markdown("---")

    # Message history
    chat_container = st.container()
    with chat_container:
        for msg in st.session_state.messages:
            role = msg["role"]
            content = msg["content"]
            if role == "user":
                st.markdown(f'<div class="user-bubble">👤 {content}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="assistant-bubble">🤖 {content}</div>', unsafe_allow_html=True)
                if st.session_state.show_sources and msg.get("sources"):
                    with st.expander(f"📚 {len(msg['sources'])} sources  ·  {msg.get('mode_used','?')} mode  ·  {msg.get('latency_ms',0):.0f} ms"):
                        for s in msg["sources"]:
                            st.markdown(
                                f'<div class="source-card">📄 <b>{s.get("source","?")}</b>  '
                                f'score={s.get("score",0):.3f}<br>{s.get("preview","")}</div>',
                                unsafe_allow_html=True,
                            )

    # Input box
    question = st.chat_input("Ask a data engineering question…")
    if question:
        st.session_state.messages.append({"role": "user", "content": question})
        history_for_api = [
            {"role": m["role"], "content": m["content"]}
            for m in st.session_state.messages[-10:]
            if m["role"] in ("user", "assistant")
        ]
        with st.spinner("🔍 Retrieving context & generating answer…"):
            result = api_chat(question, history_for_api)
        st.session_state.messages.append({
            "role": "assistant",
            "content": result["answer"],
            "sources": result.get("sources", []),
            "mode_used": result.get("mode_used", "?"),
            "latency_ms": result.get("latency_ms", 0),
        })
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Page: Data Catalog
# ─────────────────────────────────────────────────────────────────────────────
def page_catalog():
    st.markdown("## 🗂️ Data Catalog")
    st.caption("Browse tables, understand lineage, and check PII tags.")

    tab1, tab2, tab3 = st.tabs(["All Tables", "PII Tables", "Search"])

    with tab1:
        data = api_get("/catalog/tables")
        if "error" in data:
            st.warning(data["error"])
        else:
            tables = data.get("tables", [])
            if tables:
                import pandas as pd
                df = pd.DataFrame(tables)
                st.dataframe(df, use_container_width=True, height=400)
            else:
                st.info("No tables indexed yet. Run ingestion first.")

    with tab2:
        data = api_get("/catalog/pii")
        pii_tables = data.get("pii_tables", [])
        if pii_tables:
            for t in pii_tables:
                with st.expander(f"🔒 {t.get('table_name', '?')} — {len(t.get('pii_columns', []))} PII column(s)"):
                    st.json(t)
        else:
            st.success("✅ No PII-tagged tables found (or catalog not yet loaded).")

    with tab3:
        search_term = st.text_input("Search tables by name or description")
        if search_term:
            data = api_get(f"/catalog/tables?search={search_term}")
            st.json(data)


# ─────────────────────────────────────────────────────────────────────────────
# Page: Pipeline Health
# ─────────────────────────────────────────────────────────────────────────────
def page_health():
    st.markdown("## 📊 Pipeline Health Dashboard")

    col_refresh, _ = st.columns([1, 5])
    if col_refresh.button("🔄 Refresh"):
        st.rerun()

    health = api_get("/monitoring/health")
    sla = api_get("/monitoring/sla")
    failures = api_get("/monitoring/failures")

    # Summary metrics
    pipelines = health.get("pipelines", [])
    total = len(pipelines)
    healthy = sum(1 for p in pipelines if p.get("status") == "healthy")
    failing = sum(1 for p in pipelines if p.get("status") == "failing")
    warning = total - healthy - failing

    mc1, mc2, mc3, mc4 = st.columns(4)
    for col, label, value, color in [
        (mc1, "Total Pipelines", total, "#58a6ff"),
        (mc2, "✅ Healthy", healthy, "#3fb950"),
        (mc3, "❌ Failing", failing, "#f85149"),
        (mc4, "⚠️ Warning", warning, "#d29922"),
    ]:
        col.markdown(
            f'<div class="metric-card"><div class="metric-value" style="color:{color}">{value}</div>'
            f'<div class="metric-label">{label}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # Pipeline table
    if pipelines:
        import pandas as pd
        df = pd.DataFrame(pipelines)
        st.dataframe(df, use_container_width=True, height=350)

    # SLA report
    st.markdown("### SLA Adherence")
    sla_pipelines = sla.get("sla_report", [])
    if sla_pipelines:
        import pandas as pd
        df_sla = pd.DataFrame(sla_pipelines)
        st.dataframe(df_sla, use_container_width=True)
    else:
        st.info("No SLA data available yet.")

    # Recent failures
    st.markdown("### Recent Failures")
    failure_list = failures.get("failures", [])
    if failure_list:
        for f in failure_list[:10]:
            status_badge = "🔴" if f.get("severity") == "critical" else "🟡"
            st.markdown(
                f"{status_badge} **{f.get('pipeline','?')}** — `{f.get('error_type','?')}` "
                f"at {f.get('timestamp','?')}  \n> {f.get('message','')}"
            )
    else:
        st.success("🎉 No recent failures!")


# ─────────────────────────────────────────────────────────────────────────────
# Page: Quality Checks
# ─────────────────────────────────────────────────────────────────────────────
def page_quality():
    st.markdown("## 🔍 Agentic Quality Checks")
    st.caption("Trigger on-demand data quality checks. The agent runs rule-based + LLM-powered validation.")

    health = api_get("/monitoring/health")
    pipeline_names = [p.get("name", "") for p in health.get("pipelines", [])]

    if not pipeline_names:
        pipeline_names = ["user_events", "orders_fact", "product_catalog", "sessions"]

    col1, col2 = st.columns([2, 1])
    with col1:
        selected_pipeline = st.selectbox("Select Pipeline", pipeline_names)
        run_id = st.text_input("Run ID (optional, leave blank for latest)")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🚀 Trigger Quality Check", use_container_width=True):
            result = api_post(
                "/agents/quality-check",
                {"pipeline_name": selected_pipeline, "run_id": run_id or None},
            )
            if "error" not in result:
                st.success(f"✅ {result.get('message', 'Check queued!')}")
            else:
                st.error(result["error"])

    st.markdown("---")
    st.info("💡 Quality checks run in the background. Results will appear in the Pipeline Health dashboard within ~30 seconds.")

    # Show cached results
    if st.session_state.quality_results:
        st.markdown("### Recent Check Results")
        for pipeline, result in st.session_state.quality_results.items():
            with st.expander(f"📋 {pipeline}"):
                st.json(result)


# ─────────────────────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────────────────────
page_key = st.session_state.active_page

if "Chat" in page_key:
    page_chat()
elif "Catalog" in page_key:
    page_catalog()
elif "Health" in page_key:
    page_health()
elif "Quality" in page_key:
    page_quality()
