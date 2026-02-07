from __future__ import annotations

import json
import os
from typing import Any, Tuple

import requests
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from packages.features.subscriptions.subscriptions import list_active_addons_for_user


router = APIRouter()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip().rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "30") or 30)


def _json(data: Any, status: int = 200) -> Response:
    return Response(content=json.dumps(data, ensure_ascii=False), status_code=status, media_type="application/json")


def _get_owner_and_access(request: Request, cu: dict | None) -> Tuple[str, bool]:
    if not cu:
        return "", False
    owner = request.app.state.get_billing_user(request, cu) or cu
    owner_id = str((owner or {}).get("id") or "")

    try:
        if request.app.state.is_super_admin(cu):
            return owner_id, True
        if request.app.state.is_company_admin(cu):
            active_owner = list_active_addons_for_user(owner_id, owner_user_id=owner_id)
            return owner_id, ("ai_assistant" in active_owner)
        if request.app.state.is_sub_user(cu):
            active = list_active_addons_for_user(str(cu.get("id")), owner_user_id=owner_id)
            return owner_id, ("ai_assistant" in active)
        active_owner = list_active_addons_for_user(owner_id, owner_user_id=owner_id)
        return owner_id, ("ai_assistant" in active_owner)
    except Exception:
        return owner_id, False


def _instructions_for_mode(mode: str) -> str:
    if mode == "ops":
        return (
            "You are an internal assistant for a travel booking platform. "
            "Explain how to use the platform's features, screens, and workflows. "
            "Be concise and step-by-step. If asked for data you cannot access, say so."
        )
    return (
        "You are a travel assistant. Help with airport codes, routes, timing, "
        "basic visa guidance, and general travel questions. Be concise. "
        "If unsure, ask for the city/country or more details. Do not invent facts."
    )


def _extract_output_text(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    if isinstance(payload.get("output_text"), str) and payload.get("output_text").strip():
        return payload.get("output_text").strip()
    output = payload.get("output") or []
    if isinstance(output, list):
        chunks = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content") or []
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                if part.get("type") in ("output_text", "text") and part.get("text"):
                    chunks.append(str(part.get("text")))
        if chunks:
            return "\n".join(chunks).strip()
    return ""


def _call_openai(message: str, mode: str) -> tuple[bool, str, dict | None]:
    if not OPENAI_API_KEY:
        return False, "OPENAI_API_KEY is not configured on the server.", None

    payload = {
        "model": OPENAI_MODEL,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": _instructions_for_mode(mode)}]},
            {"role": "user", "content": [{"type": "input_text", "text": message}]},
        ],
        "max_output_tokens": 400,
    }

    try:
        resp = requests.post(
            f"{OPENAI_BASE_URL}/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json=payload,
            timeout=OPENAI_TIMEOUT,
        )
    except Exception:
        return False, "Unable to reach AI service. Please try again.", None

    try:
        data = resp.json()
    except Exception:
        data = {}

    if resp.status_code >= 400:
        err = ""
        if isinstance(data, dict):
            err = (data.get("error") or {}).get("message") if isinstance(data.get("error"), dict) else ""
        return False, err or "AI request failed.", data if isinstance(data, dict) else None

    text = _extract_output_text(data if isinstance(data, dict) else {})
    if not text:
        return False, "No response text returned.", data if isinstance(data, dict) else None
    return True, text, data if isinstance(data, dict) else None


@router.get("/assistant", response_class=HTMLResponse, name="ai_assistant")
def assistant_page(request: Request):
    return request.app.state.render(
        request,
        "ai_assistant/index.html",
        {"ai_api_ready": bool(OPENAI_API_KEY)},
    )


@router.post("/assistant/api/message", name="ai_assistant_message")
async def assistant_message(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    _, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "AI Assistant add-on is not active."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    message = str(payload.get("message") or "").strip()
    mode = str(payload.get("mode") or "travel").strip().lower()
    if mode not in ("travel", "ops"):
        mode = "travel"

    if not message:
        return _json({"status": "error", "error": "Please enter a message."}, 400)
    if len(message) > 2000:
        return _json({"status": "error", "error": "Message is too long."}, 400)

    ok, reply, data = _call_openai(message, mode)
    if not ok:
        return _json({"status": "error", "error": reply}, 502)

    usage = {}
    if isinstance(data, dict) and isinstance(data.get("usage"), dict):
        usage = data.get("usage")

    return _json({"status": "ok", "reply": reply, "usage": usage})
