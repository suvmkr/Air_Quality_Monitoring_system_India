"""
quality_agent.py  –  Agentic data-quality checker.
Runs rule-based checks first, then calls Claude to interpret results
and produce a structured quality report with remediation advice.
"""

from __future__ import annotations
import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import anthropic
from loguru import logger

from app.config import get_settings
from rag.prompt_templates import QUALITY_CHECK_SYSTEM, QUALITY_CHECK_USER
from monitoring.failure_logs import FailureLogs

cfg = get_settings()

# ── Quality rule definitions ──────────────────────────────────────────────────

QUALITY_RULES = {
    "row_count_check": {
        "description": "Row count must be within ±20% of 7-day moving average",
        "threshold_pct": 0.20,
    },
    "null_rate_check": {
        "description": "Critical columns must have null rate < 0.1%",
        "critical_columns": ["user_id", "event_type", "order_id", "customer_id"],
        "threshold_pct": 0.001,
    },
    "freshness_check": {
        "description": "Most recent record must be within SLA window",
        "max_lag_hours": cfg.sla_threshold_hours,
    },
    "schema_drift_check": {
        "description": "No unexpected schema changes since last run",
    },
    "duplicate_check": {
        "description": "Duplicate rate must be < 0.01% on primary key",
        "threshold_pct": 0.0001,
    },
}


class QualityAgent:
    """
    1. Collect run statistics (simulated here; replace with real DWH queries)
    2. Evaluate quality rules
    3. Call Claude to interpret violations and suggest remediations
    4. Persist result to FailureLogs
    """

    def __init__(self):
        self._client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
        self._failure_logs = FailureLogs()

    # ── Public API ─────────────────────────────────────────────────────────────

    def run_check(self, pipeline_name: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Synchronous entry-point (called from FastAPI BackgroundTasks).
        Returns the full quality report dict.
        """
        run_id = run_id or f"run_{int(time.time())}"
        logger.info(f"[QualityAgent] Starting check: pipeline={pipeline_name} run={run_id}")

        # Step 1: gather run statistics
        run_stats = self._collect_run_stats(pipeline_name)

        # Step 2: evaluate rules
        rule_results = self._evaluate_rules(run_stats, pipeline_name)

        # Step 3: ask Claude to interpret
        report = self._llm_assess(pipeline_name, run_id, run_stats, rule_results)

        # Step 4: persist failures
        if report.get("overall_status") != "pass":
            for check in report.get("checks", []):
                if check.get("status") != "pass":
                    self._failure_logs.log_failure(
                        pipeline=pipeline_name,
                        error_type=f"quality_{check['check_name']}",
                        message=check.get("detail", ""),
                        severity=check.get("severity", "warning"),
                        run_id=run_id,
                    )

        logger.info(f"[QualityAgent] Done: {pipeline_name} → {report.get('overall_status')}")
        return report

    # ── Private helpers ────────────────────────────────────────────────────────

    def _collect_run_stats(self, pipeline: str) -> Dict[str, Any]:
        """
        In production: query BigQuery / DuckDB for real stats.
        Here we simulate realistic (occasionally bad) numbers.
        """
        seed = hash(pipeline + str(datetime.now().date()))
        rng = random.Random(seed)

        base_rows = {"user_events_clean": 4_500_000, "orders_fact": 280_000,
                     "product_catalog": 1_500, "sessions": 8_000_000}.get(pipeline, 100_000)

        anomaly = rng.random() < 0.25  # 25% chance of an anomaly for demo

        return {
            "pipeline": pipeline,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "row_count": base_rows * (0.55 if anomaly else 1.02),   # ±2% normal, -45% anomaly
            "row_count_7d_avg": base_rows,
            "null_rates": {
                "user_id": 0.0 if not anomaly else 0.003,
                "event_type": 0.0,
                "timestamp": 0.0,
            },
            "duplicate_rate": 0.000005 if not anomaly else 0.0,
            "max_lag_hours": 0.8 if not anomaly else 3.5,
            "schema_changed": anomaly and rng.random() < 0.3,
            "new_columns": ["_temp_debug"] if (anomaly and rng.random() < 0.3) else [],
            "dropped_columns": [],
        }

    def _evaluate_rules(self, stats: Dict, pipeline: str) -> List[Dict[str, Any]]:
        results = []

        # Row count check
        base = stats["row_count_7d_avg"]
        actual = stats["row_count"]
        deviation = abs(actual - base) / max(base, 1)
        results.append({
            "check_name": "row_count_check",
            "status": "fail" if deviation > 0.20 else "pass",
            "deviation_pct": round(deviation * 100, 2),
            "actual": actual,
            "expected_approx": base,
        })

        # Null rate check
        for col, rate in stats.get("null_rates", {}).items():
            results.append({
                "check_name": f"null_rate_{col}",
                "status": "fail" if rate > 0.001 else "pass",
                "null_rate_pct": round(rate * 100, 4),
                "column": col,
            })

        # Freshness check
        lag = stats.get("max_lag_hours", 0)
        results.append({
            "check_name": "freshness_check",
            "status": "fail" if lag > cfg.sla_threshold_hours else "pass",
            "lag_hours": lag,
            "threshold_hours": cfg.sla_threshold_hours,
        })

        # Schema drift
        if stats.get("schema_changed"):
            results.append({
                "check_name": "schema_drift_check",
                "status": "warning",
                "new_columns": stats.get("new_columns", []),
                "dropped_columns": stats.get("dropped_columns", []),
            })
        else:
            results.append({"check_name": "schema_drift_check", "status": "pass"})

        # Duplicate check
        dup_rate = stats.get("duplicate_rate", 0)
        results.append({
            "check_name": "duplicate_check",
            "status": "fail" if dup_rate > 0.0001 else "pass",
            "duplicate_rate_pct": round(dup_rate * 100, 6),
        })

        return results

    def _llm_assess(
        self,
        pipeline: str,
        run_id: str,
        run_stats: Dict,
        rule_results: List[Dict],
    ) -> Dict[str, Any]:
        failures_exist = any(r.get("status") != "pass" for r in rule_results)
        if not failures_exist:
            return {
                "pipeline": pipeline,
                "run_id": run_id,
                "overall_status": "pass",
                "checks": [{"check_name": r["check_name"], "status": "pass",
                             "severity": "info", "detail": "All checks passed.",
                             "remediation": "None required."} for r in rule_results],
                "summary": "All quality checks passed. No action required.",
            }

        prompt = QUALITY_CHECK_USER.format(
            pipeline_name=pipeline,
            run_id=run_id,
            timestamp=run_stats.get("run_timestamp", ""),
            run_stats=json.dumps(run_stats, indent=2),
            rule_results=json.dumps(rule_results, indent=2),
        )

        try:
            response = self._client.messages.create(
                model=cfg.claude_model,
                max_tokens=1000,
                system=QUALITY_CHECK_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            # Strip markdown fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            report = json.loads(raw)
        except (json.JSONDecodeError, anthropic.APIError) as e:
            logger.error(f"[QualityAgent] LLM assessment failed: {e}")
            report = {
                "pipeline": pipeline,
                "run_id": run_id,
                "overall_status": "warning",
                "checks": rule_results,
                "summary": f"Rule-based checks found issues. LLM assessment unavailable: {e}",
            }

        return report
