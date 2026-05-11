# 🚀 RAG-Powered DE Assistant

A conversational assistant for Data Engineers — built with Claude API, ChromaDB, and Streamlit.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Streamlit UI  (port 8501)                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │
│  │   Chat   │ │ Catalog  │ │  Health  │ │ Quality  │  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘  │
└───────┼─────────────┼─────────────┼─────────────┼───────┘
        │             │             │             │
        └─────────────┴──────┬──────┴─────────────┘
                             │  HTTP (REST)
                    ┌────────▼────────┐
                    │  FastAPI (8502) │
                    │  app/api.py     │
                    └────────┬────────┘
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
      ┌───────────┐  ┌───────────┐  ┌──────────────┐
      │  Retriever│  │  Agents   │  │  Monitoring  │
      │  ChromaDB │  │  Claude   │  │  HealthCheck │
      └───────────┘  └───────────┘  └──────────────┘
```

## Setup

### Prerequisites
- Python 3.10+
- 4 GB RAM (for local embedding model)

### 1. Clone & install

```bash
git clone <repo>
cd rag-de-assistant
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env .env.local
# Edit .env.local and add your ANTHROPIC_API_KEY
```

### 3. Run ingestion (one-time)

```bash
python ingest_all.py
```

This creates sample pipeline code, docs, and catalog data, then indexes everything into ChromaDB (~2-3 min for model download on first run).

### 4. Start the API

```bash
uvicorn app.api:app --host 0.0.0.0 --port 8502 --reload
```

Verify: http://localhost:8502/docs

### 5. Start Streamlit

```bash
streamlit run app/streamlit_app.py
```

Open: http://localhost:8501

---

## Usage

### Chat Examples
| Question | Mode Auto-Detected |
|---|---|
| "How does user_events handle deduplication?" | 💻 Code |
| "Which tables contain PII?" | 🗂️ Catalog |
| "Are there any pipeline failures today?" | ❤️ Health |
| "What is the schema of orders_fact?" | 🗂️ Catalog |
| "Why is user_events_clean downstream of sessions?" | 🗂️ Catalog |

### Trigger a Quality Check (API)
```bash
TOKEN=$(curl -s -X POST http://localhost:8502/auth/token | jq -r .token)
curl -X POST http://localhost:8502/agents/quality-check \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "user_events_clean"}'
```

### Re-ingest after code changes
```bash
python ingest_all.py
# or via API:
curl -X POST http://localhost:8502/ingest/trigger -H "Authorization: Bearer $TOKEN"
```

---

## Project Structure

```
rag-de-assistant/
├── app/
│   ├── streamlit_app.py    # Streamlit chat UI
│   ├── api.py              # FastAPI REST backend
│   ├── config.py           # Centralised config
│   └── auth.py             # Token-based auth
├── ingestion/
│   ├── code_parser.py      # Python/SQL/YAML indexer
│   ├── docs_loader.py      # Markdown/text docs indexer
│   ├── metadata_ingest.py  # Data catalog JSON indexer
│   └── chunker.py          # Smart text splitting
├── rag/
│   ├── embeddings.py       # sentence-transformers wrapper
│   ├── chroma_client.py    # ChromaDB client factory
│   ├── retriever.py        # Vector search + MMR
│   └── prompt_templates.py # All LLM prompts
├── agents/
│   ├── pipeline_agent.py   # Main Q&A agent (Claude)
│   ├── quality_agent.py    # Agentic quality checker
│   └── catalog_agent.py    # Catalog query agent
├── monitoring/
│   ├── health_checker.py   # Pipeline status
│   ├── sla_tracker.py      # SLA adherence
│   └── failure_logs.py     # Failure log store
├── ingest_all.py           # Bootstrap ingestion script
├── requirements.txt
└── .env
```

## Extending with Real Data

1. **Real pipeline repo**: Set `PIPELINE_REPO_PATH` to your actual Git repo root
2. **Real catalog**: Replace `_SAMPLE_CATALOG` in `metadata_ingest.py` with API calls to DataHub, Amundsen, or your internal catalog
3. **Real monitoring**: Replace `_fetch_pipeline_status()` in `health_checker.py` with Airflow REST API calls
4. **Real auth**: Replace `app/auth.py` with OAuth2 + your LDAP/IdP
