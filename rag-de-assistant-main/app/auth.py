"""
auth.py  –  Lightweight token auth for the FastAPI backend.
In production replace with OAuth2 / JWT + LDAP.
"""

from __future__ import annotations
import hashlib, hmac, secrets, time
from typing import Optional
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import get_settings

cfg = get_settings()
bearer_scheme = HTTPBearer(auto_error=False)

# ── In-memory session store (replace with Redis in prod) ──────────────────────
_sessions: dict[str, dict] = {}


def _sign(token: str) -> str:
    return hmac.new(cfg.secret_key.encode(), token.encode(), hashlib.sha256).hexdigest()


def create_session(user: str = "de-user") -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = {"user": user, "created_at": time.time(), "hits": 0}
    return token


def verify_token(token: str) -> dict:
    session = _sessions.get(token)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token. Call POST /auth/token first.",
        )
    # 8-hour TTL
    if time.time() - session["created_at"] > 8 * 3600:
        _sessions.pop(token, None)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired.")
    session["hits"] += 1
    return session


async def require_auth(
    creds: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
) -> dict:
    if creds is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing.",
        )
    return verify_token(creds.credentials)
