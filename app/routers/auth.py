import uuid
from datetime import datetime, timedelta
from typing import Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..config import ADMIN_EMAIL, ADMIN_PASSWORD, RESET_BASE_URL

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


class ForgotRequest(BaseModel):
    email: str


class ResetRequest(BaseModel):
    token: str
    new_password: str


_RESET_TOKENS: Dict[str, datetime] = {}
_CURRENT_PASSWORD = ADMIN_PASSWORD


def _is_valid_token(token: str) -> bool:
    expires_at = _RESET_TOKENS.get(token)
    if not expires_at:
        return False
    if datetime.utcnow() > expires_at:
        _RESET_TOKENS.pop(token, None)
        return False
    return True


@router.post("/login")
def login(payload: LoginRequest):
    username = payload.username.strip()
    if username not in {"admin", ADMIN_EMAIL}:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if payload.password != _CURRENT_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"message": "ok"}


@router.post("/forgot")
def forgot(payload: ForgotRequest):
    email = payload.email.strip().lower()
    if email != ADMIN_EMAIL.lower():
        # For security, respond with success even if email does not match.
        return {"message": "If that email exists, a reset link was sent."}
    token = uuid.uuid4().hex
    _RESET_TOKENS[token] = datetime.utcnow() + timedelta(minutes=20)
    reset_link = f"{RESET_BASE_URL}?token={token}"
    return {"message": f"Reset link generated: {reset_link}"}


@router.post("/reset")
def reset(payload: ResetRequest):
    if not _is_valid_token(payload.token):
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    new_password = payload.new_password.strip()
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
    global _CURRENT_PASSWORD
    _CURRENT_PASSWORD = new_password
    _RESET_TOKENS.pop(payload.token, None)
    return {"message": "Password updated"}
