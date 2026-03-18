import os
import smtplib
import uuid
from datetime import datetime, timedelta
from email.message import EmailMessage
from typing import Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import requests

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


def _send_reset_email(to_email: str, reset_link: str) -> None:
    resend_key = os.getenv("RESEND_API_KEY", "").strip()
    if resend_key:
        from_email = os.getenv("RESET_EMAIL_FROM", "").strip() or os.getenv("SMTP_USER", "").strip() or ADMIN_EMAIL
        if not from_email:
            raise RuntimeError("RESET_EMAIL_FROM not configured")
        payload = {
            "from": from_email,
            "to": [to_email],
            "subject": "Reset your password",
            "text": (
                "Click the link below to reset your password:\n\n"
                f"{reset_link}\n\n"
                "This link expires in 20 minutes."
            ),
        }
        resp = requests.post(
            "https://api.resend.com/emails",
            json=payload,
            headers={"Authorization": f"Bearer {resend_key}"},
            timeout=10,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Resend error {resp.status_code}: {resp.text}")
        return

    host = os.getenv("SMTP_HOST", "").strip()
    if not host:
        raise RuntimeError("SMTP_HOST not configured")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASS", "")
    use_tls = os.getenv("SMTP_USE_TLS", "true").lower() in {"1", "true", "yes"}

    msg = EmailMessage()
    msg["Subject"] = "Reset your password"
    msg["From"] = user or ADMIN_EMAIL
    msg["To"] = to_email
    msg.set_content(
        "Click the link below to reset your password:\n\n"
        f"{reset_link}\n\n"
        "This link expires in 20 minutes."
    )

    with smtplib.SMTP(host, port, timeout=10) as server:
        server.ehlo()
        if use_tls:
            server.starttls()
            server.ehlo()
        if user and password:
            server.login(user, password)
        server.send_message(msg)


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
    allow_link_in_response = os.getenv("RESET_LINK_IN_RESPONSE", "").lower() in {"1", "true", "yes"}
    admin_email = ADMIN_EMAIL.strip().lower()
    if not admin_email:
        raise HTTPException(status_code=500, detail="ADMIN_EMAIL not configured")
    if email != admin_email:
        raise HTTPException(status_code=400, detail="Admin email is wrong.")
    token = uuid.uuid4().hex
    _RESET_TOKENS[token] = datetime.utcnow() + timedelta(minutes=20)
    reset_link = f"{RESET_BASE_URL}?token={token}"
    try:
        _send_reset_email(ADMIN_EMAIL, reset_link)
        if allow_link_in_response:
            return {"message": f"Reset email sent. Link: {reset_link}"}
        return {"message": "Reset email sent."}
    except Exception as exc:  # noqa: BLE001
        if allow_link_in_response:
            return {"message": f"Reset link generated: {reset_link}"}
        raise HTTPException(status_code=500, detail=f"Failed to send reset email: {exc}")


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
