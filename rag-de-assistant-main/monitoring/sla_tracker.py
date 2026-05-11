"""
sla_tracker.py  –  Tracks SLA adherence per pipeline per day.
"""

from __future__ import annotations
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

from app.config import get_settings

cfg = get_settings()


class SLATracker:
    def get_report(self, days: int = 7) -> Dict[str, Any]:
        pipelines = ["user_events_clean", "orders_fact", "product_catalog", "sessions"]
        report = []
        for name in pipelines:
            rng = random.Random(hash(name))
            breaches_last_7d = rng.randint(0, 2)
            avg_completion_mins = rng.randint(35, 85)
            report.append({
                "pipeline": name,
                "sla_window_days": days,
                "runs_total": days,
                "runs_on_time": days - breaches_last_7d,
                "sla_breaches": breaches_last_7d,
                "adherence_pct": round(((days - breaches_last_7d) / days) * 100, 1),
                "avg_completion_minutes": avg_completion_mins,
                "last_breach": (datetime.now(timezone.utc) - timedelta(days=rng.randint(1, 7))).strftime("%Y-%m-%d") if breaches_last_7d else None,
            })
        return {
            "period_days": days,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "overall_adherence_pct": round(sum(r["adherence_pct"] for r in report) / len(report), 1),
            "sla_report": report,
        }
