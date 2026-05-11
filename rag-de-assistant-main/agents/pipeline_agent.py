"""
pipeline_agent.py  –  Core conversational agent.
Receives retrieved context, builds Claude messages, streams response.
"""

from __future__ import annotations
import asyncio
from typing import Any, Dict, List, Optional

import anthropic
from loguru import logger

from app.config import get_settings
from rag.prompt_templates import (
    DE_ASSISTANT_SYSTEM,
    RAG_USER_TEMPLATE,
    HEALTH_SUMMARY_SYSTEM,
    HEALTH_SUMMARY_USER,
    CATALOG_SYSTEM,
)
from rag.retriever import Retriever

cfg = get_settings()


class PipelineAgent:
    """
    Wraps the Anthropic Messages API with:
    - Context injection from retrieved docs
    - Multi-turn history
    - Mode-specific system prompts
    - Token budget management
    """

    MODE_SYSTEM_PROMPTS = {
        "code": DE_ASSISTANT_SYSTEM,
        "catalog": CATALOG_SYSTEM,
        "health": HEALTH_SUMMARY_SYSTEM,
        "auto": DE_ASSISTANT_SYSTEM,
    }

    def __init__(self):
        self._client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
        self._retriever = Retriever()

    async def answer(
        self,
        question: str,
        retrieved_docs: List[Dict[str, Any]],
        history: List[Dict[str, str]] | None = None,
        mode: str = "auto",
        extra_context: Optional[Any] = None,
    ) -> str:
        """
        Generate an answer using Claude with RAG context.
        Runs in asyncio via run_in_executor to keep the event loop free.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._sync_answer,
            question,
            retrieved_docs,
            history,
            mode,
            extra_context,
        )

    def _sync_answer(
        self,
        question: str,
        retrieved_docs: List[Dict[str, Any]],
        history: List[Dict[str, str]] | None,
        mode: str,
        extra_context: Optional[Any],
    ) -> str:
        system_prompt = self.MODE_SYSTEM_PROMPTS.get(mode, DE_ASSISTANT_SYSTEM)

        context = self._retriever.format_context(retrieved_docs, max_tokens=cfg.max_context_tokens // 2)

        if extra_context:
            import json
            context += f"\n\n## Live Monitoring Data\n{json.dumps(extra_context, indent=2)}"

        user_content = RAG_USER_TEMPLATE.format(context=context, question=question)

        messages = self._build_messages(history or [], user_content)

        logger.debug(f"[PipelineAgent] Calling Claude with {len(messages)} messages, mode={mode}")

        try:
            response = self._client.messages.create(
                model=cfg.claude_model,
                max_tokens=1500,
                system=system_prompt,
                messages=messages,
            )
            answer = response.content[0].text
            logger.debug(f"[PipelineAgent] Received {len(answer)} chars")
            return answer
        except anthropic.APIError as e:
            logger.error(f"[PipelineAgent] Anthropic API error: {e}")
            return f"⚠️ Sorry, I encountered an API error: {e}"

    def _build_messages(
        self,
        history: List[Dict[str, str]],
        current_user_message: str,
    ) -> List[Dict[str, str]]:
        """
        Build the messages array for Claude.
        Keep last 6 turns of history to stay within context window.
        """
        messages = []
        for turn in history[-6:]:
            role = turn.get("role", "user")
            content = turn.get("content", "")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": current_user_message})
        return messages
