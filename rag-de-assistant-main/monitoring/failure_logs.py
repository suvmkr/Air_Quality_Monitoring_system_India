"""
failure_logs.py  –  Simple JSON-backed failure log.
In production replace with a proper time-series store (InfluxDB, PG, etc.)
"""

from __future__ import annotations
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

from loguru import logger
from app.config import get_settings

cfg = get_settings()
LOG_FILE = Path(cfg.chroma_persist_dir).parent / "failure_logs.json"
_lock = Lock()


class FailureLogs:
    def __init__(self):
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not LOG_FILE.exists():
            LOG_FILE.write_text("[]")

    def _read(self) -> List[Dict]:
        try:
            return json.loads(LOG_FILE.read_text())
        except Exception:
            return []

    def _write(self, data: List[Dict]):
        LOG_FILE.write_text(json.dumps(data, indent=2))

    def log_failure(
        self,
        pipeline: str,
        error_type: str,
        message: str,
        severity: str = "warning",
        run_id: Optional[str] = None,
    ):
        entry = {
            "id": f"{pipeline}_{int(time.time())}",
            "pipeline": pipeline,
            "error_type": error_type,
            "message": message,
            "severity": severity,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with _lock:
            data = self._read()
            data.append(entry)
            # Keep last 1000 entries
            self._write(data[-1000:])
        logger.warning(f"[FailureLogs] Logged: {pipeline} | {error_type} | {severity}")

    def get_recent(self, limit: int = 20) -> Dict[str, Any]:
        data = self._read()
        recent = sorted(data, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]
        return {
            "total": len(data),
            "returned": len(recent),
            "failures": recent,
        }

    def get_by_pipeline(self, pipeline: str, limit: int = 10) -> List[Dict]:
        data = self._read()
        return [f for f in reversed(data) if f.get("pipeline") == pipeline][:limit]

    def clear_old(self, days: int = 30):
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with _lock:
            data = self._read()
            filtered = [f for f in data if f.get("timestamp", "") >= cutoff]
            self._write(filtered)
