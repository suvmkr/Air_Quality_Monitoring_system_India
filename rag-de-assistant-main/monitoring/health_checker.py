"""
health_checker.py  –  Simulates pipeline health checks.
Replace _fetch_pipeline_status() with real DWH / Airflow API calls.
"""

from __future__ import annotations
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

from app.config import get_settings

cfg = get_settings()

PIPELINE_REGISTRY = [
    {"name": "user_events_clean",  "owner": "alice@company.com", "sla_hour": 4},
    {"name": "orders_fact",        "owner": "bob@company.com",   "sla_hour": 6},
    {"name": "product_catalog",    "owner": "charlie@company.com","sla_hour": 12},
    {"name": "sessions",           "owner": "diana@company.com", "sla_hour": 5},
]


class HealthChecker:
    def get_summary(self) -> List[Dict[str, Any]]:
        return [self._fetch_pipeline_status(p) for p in PIPELINE_REGISTRY]

    def get_full_report(self) -> Dict[str, Any]:
        pipelines = self.get_summary()
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_pipelines": len(pipelines),
            "healthy": sum(1 for p in pipelines if p["status"] == "healthy"),
            "failing": sum(1 for p in pipelines if p["status"] == "failing"),
            "warning": sum(1 for p in pipelines if p["status"] == "warning"),
            "pipelines": pipelines,
        }

    def _fetch_pipeline_status(self, pipeline_def: Dict) -> Dict[str, Any]:
        """
        In production: call Airflow REST API or query a metadata table.
        Here we simulate with seeded randomness so the demo looks realistic.
        """
        name = pipeline_def["name"]
        seed = hash(name + str(datetime.now().date()))
        rng = random.Random(seed)

        now = datetime.now(timezone.utc)
        last_run_minutes_ago = rng.randint(10, 180)
        last_run = (now - timedelta(minutes=last_run_minutes_ago)).isoformat()

        # 70% healthy, 20% warning, 10% failing
        roll = rng.random()
        if roll < 0.70:
            status = "healthy"
            last_error = None
            rows_processed = rng.randint(800_000, 1_200_000)
        elif roll < 0.90:
            status = "warning"
            last_error = rng.choice([
                "Row count 22% below 7-day average",
                "Null rate spike in user_id column (0.3%)",
                "Processing time exceeded P95 threshold",
            ])
            rows_processed = rng.randint(400_000, 800_000)
        else:
            status = "failing"
            last_error = rng.choice([
                "BigQuery quota exceeded",
                "Kafka consumer lag > 2M messages",
                "Schema mismatch: column 'session_id' not found",
                "OOM error in Spark executor",
            ])
            rows_processed = 0

        return {
            "name": name,
            "status": status,
            "last_run": last_run,
            "last_run_minutes_ago": last_run_minutes_ago,
            "rows_processed": rows_processed,
            "owner": pipeline_def["owner"],
            "sla_hour_utc": pipeline_def["sla_hour"],
            "sla_met": status == "healthy",
            "last_error": last_error,
        }
