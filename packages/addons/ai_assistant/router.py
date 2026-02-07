from __future__ import annotations

import json
import os
import time
from typing import Any, Tuple

import requests
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from packages.features.subscriptions.subscriptions import list_active_addons_for_user


router = APIRouter()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip().rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4-turbo").strip()
OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "30") or 30)
BACKEND_BASE_URL = os.getenv("AVAILABILITY_BACKEND_URL", "").strip()
BACKEND_URLS_ENV = os.getenv("AVAILABILITY_BACKEND_URLS", "").strip()

_backend_cache: dict = {}


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


def _backend_candidates() -> list[str]:
    urls: list[str] = []
    if BACKEND_URLS_ENV:
        urls.extend([u.strip().rstrip("/") for u in BACKEND_URLS_ENV.split(",") if u.strip()])
    if BACKEND_BASE_URL:
        urls.insert(0, BACKEND_BASE_URL.rstrip("/"))
    defaults = [
        "http://localhost:5050",
        "http://127.0.0.1:5050",
        "http://localhost:8001",
        "http://127.0.0.1:8001",
    ]
    for d in defaults:
        if d not in urls:
            urls.append(d)
    seen = set()
    out: list[str] = []
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _probe_backend(base: str) -> bool:
    try:
        r = requests.get(f"{base}/health", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def _resolve_backend_base() -> str:
    cache = _backend_cache if isinstance(_backend_cache, dict) else {}
    now = time.time()
    if cache.get("url") and (now - float(cache.get("ts") or 0)) < 30:
        return cache["url"]
    for base in _backend_candidates():
        if _probe_backend(base):
            _backend_cache["url"] = base
            _backend_cache["ts"] = now
            return base
    fallback = _backend_candidates()[0]
    _backend_cache["url"] = fallback
    _backend_cache["ts"] = now
    return fallback


def _backend_url(path: str) -> str:
    base = _resolve_backend_base()
    return base.rstrip("/") + path


def _extract_provider_id(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    direct_keys = [
        "provider_id",
        "providerId",
        "providerID",
        "provider",
        "provider_code",
        "providerCode",
        "ota_id",
        "otaId",
        "ota",
        "api",
        "api_id",
        "apiId",
        "source",
        "supplier",
        "supplier_id",
        "supplierId",
        "channel",
        "gds",
    ]

    def _from_value(v) -> str:
        if isinstance(v, (str, int)):
            s = str(v).strip()
            return s.lower() if s else ""
        if isinstance(v, dict):
            for kk in ("id", "code", "provider_id", "providerId", "ota", "api", "name"):
                vv = v.get(kk)
                out = _from_value(vv)
                if out:
                    return out
        return ""

    for k in direct_keys:
        out = _from_value(payload.get(k))
        if out:
            return out

    for k in ("ticketing", "pricing", "selection", "meta", "result", "offer", "source_info", "provider"):
        sub = payload.get(k)
        if isinstance(sub, dict):
            pid = _extract_provider_id(sub)
            if pid:
                return pid

    for v in payload.values():
        if isinstance(v, dict):
            pid = _extract_provider_id(v)
            if pid:
                return pid
    return ""


def _allowed_provider_ids_for_request(request: Request, cu: dict | None) -> list[str] | None:
    if not cu:
        return []
    if request.app.state.is_super_admin(cu):
        return None
    users = request.app.state.load_users()
    billing = request.app.state.get_billing_user(request, cu) or cu
    apis = billing.get("apis")
    if not isinstance(apis, list):
        apis = []
    apis = [str(x).strip().lower() for x in apis if x is not None and str(x).strip() != ""]
    return apis


def _filter_by_allowed_providers(results: list, allowed_providers: list | None) -> list:
    if allowed_providers is None or not isinstance(allowed_providers, list):
        return results
    def _pid(res):
        if isinstance(res, dict):
            return _extract_provider_id(res)
        return ""
    return [r for r in results if (_pid(r) == "" or _pid(r) in allowed_providers)]


def _normalize_iata(code: str) -> str:
    code = (code or "").strip().upper()
    return code


def _search_flights(request: Request, args: dict) -> dict:
    from_code = _normalize_iata(str(args.get("from_code") or args.get("from") or ""))
    to_code = _normalize_iata(str(args.get("to_code") or args.get("to") or ""))
    date = str(args.get("date") or "").strip()
    trip_type = str(args.get("trip_type") or "oneway").strip().lower()
    return_date = str(args.get("return_date") or "").strip()
    cabin = str(args.get("cabin") or "economy").strip().lower()
    adults = int(args.get("adults") or 1)
    children = int(args.get("children") or 0)
    infants = int(args.get("infants") or 0)

    if not from_code or not to_code:
        return {"status": "error", "error": "Please provide origin and destination airport codes."}
    if len(from_code) != 3 or len(to_code) != 3:
        return {"status": "error", "error": "Please use 3-letter IATA airport codes (e.g., BGW, DXB)."}
    if not date:
        return {"status": "error", "error": "Please provide a departure date (YYYY-MM-DD)."}
    if trip_type not in ("oneway", "roundtrip"):
        trip_type = "oneway"
    if trip_type == "roundtrip" and not return_date:
        return {"status": "error", "error": "Please provide a return date for round-trip searches."}
    if cabin not in ("economy", "business"):
        cabin = "economy"

    payload = {
        "from": from_code,
        "to": to_code,
        "date": date,
        "trip_type": trip_type,
        "return_date": return_date,
        "cabin": cabin,
        "pax": {"adults": adults, "children": children, "infants": infants},
    }

    cu = request.app.state.get_current_user(request)
    allowed = _allowed_provider_ids_for_request(request, cu)
    if allowed is not None and len(allowed) == 0:
        return {"status": "error", "error": "No flight providers are enabled for this account."}

    try:
        r = requests.post(_backend_url("/api/availability"), json=payload, timeout=60)
        r.raise_for_status()
        data = r.json() if isinstance(r.json(), dict) else {}
    except Exception as exc:
        return {"status": "error", "error": f"Flight search failed: {exc}"}

    outbound = data.get("results") or []
    inbound = data.get("results_return") or []
    outbound = _filter_by_allowed_providers(outbound, allowed)
    inbound = _filter_by_allowed_providers(inbound, allowed)

    return {
        "status": "ok",
        "search": payload,
        "results": outbound[:5],
        "results_return": inbound[:5],
        "meta": data.get("meta") or {},
    }


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
        "When a user requests live flight options, call the search_flights tool. "
        "Ask for missing IATA airport codes or travel dates instead of guessing. "
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


def _history_to_input(history: list) -> list[dict]:
    items: list[dict] = []
    for msg in history:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        if role not in ("user", "assistant"):
            continue
        content = str(msg.get("content") or "").strip()
        if not content:
            continue
        # Prevent oversized history entries.
        if len(content) > 2000:
            content = content[:2000]
        items.append(
            {"role": role, "content": [{"type": "input_text", "text": content}]}
        )
    return items


def _call_openai(
    message: str,
    mode: str,
    request: Request | None = None,
    history: list | None = None,
) -> tuple[bool, str, dict | None]:
    if not OPENAI_API_KEY:
        return False, "OPENAI_API_KEY is not configured on the server.", None

    tools = [
        {
            "type": "function",
            "name": "search_flights",
            "description": "Search for flights using the internal availability API.",
            "parameters": {
                "type": "object",
                "properties": {
                    "from_code": {"type": "string", "description": "Origin IATA airport code, e.g. BGW"},
                    "to_code": {"type": "string", "description": "Destination IATA airport code, e.g. DXB"},
                    "date": {"type": "string", "description": "Departure date in YYYY-MM-DD"},
                    "trip_type": {"type": "string", "enum": ["oneway", "roundtrip"]},
                    "return_date": {"type": "string", "description": "Return date in YYYY-MM-DD (required for roundtrip)"},
                    "cabin": {"type": "string", "enum": ["economy", "business"]},
                    "adults": {"type": "integer", "minimum": 1},
                    "children": {"type": "integer", "minimum": 0},
                    "infants": {"type": "integer", "minimum": 0},
                },
                "required": ["from_code", "to_code", "date"],
            },
        }
    ]

    history_items = _history_to_input(history or [])
    # Keep only the most recent 12 items to limit context size.
    if len(history_items) > 12:
        history_items = history_items[-12:]

    input_items = [
        {"role": "system", "content": [{"type": "input_text", "text": _instructions_for_mode(mode)}]},
    ]
    input_items.extend(history_items)
    input_items.append({"role": "user", "content": [{"type": "input_text", "text": message}]})

    payload = {
        "model": OPENAI_MODEL,
        "input": input_items,
        "max_output_tokens": 400,
        "tools": tools,
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

    data = data if isinstance(data, dict) else {}
    output_items = data.get("output") if isinstance(data.get("output"), list) else []

    tool_calls = [
        item for item in output_items
        if isinstance(item, dict) and item.get("type") in ("function_call", "tool_call")
    ]
    if tool_calls and request is not None:
        context = list(input_items)
        context.extend(output_items)
        tool_outputs = []
        for call in tool_calls:
            name = call.get("name")
            call_id = call.get("call_id") or call.get("id") or ""
            args_raw = call.get("arguments") or "{}"
            try:
                args = json.loads(args_raw) if isinstance(args_raw, str) else (args_raw or {})
            except Exception:
                args = {}
            result = {"status": "error", "error": "Unknown tool."}
            if name == "search_flights":
                result = _search_flights(request, args if isinstance(args, dict) else {})
            tool_outputs.append(
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": json.dumps(result, ensure_ascii=False),
                }
            )
        context.extend(tool_outputs)

        try:
            resp2 = requests.post(
                f"{OPENAI_BASE_URL}/responses",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": OPENAI_MODEL, "input": context, "tools": tools, "max_output_tokens": 400},
                timeout=OPENAI_TIMEOUT,
            )
            data2 = resp2.json() if resp2.content else {}
        except Exception:
            return False, "AI follow-up failed after tool call.", data

        if resp2.status_code >= 400:
            err2 = ""
            if isinstance(data2, dict):
                err2 = (data2.get("error") or {}).get("message") if isinstance(data2.get("error"), dict) else ""
            return False, err2 or "AI follow-up failed.", data2 if isinstance(data2, dict) else None

        text2 = _extract_output_text(data2 if isinstance(data2, dict) else {})
        if not text2:
            return False, "No response text returned after tool call.", data2 if isinstance(data2, dict) else None
        return True, text2, data2 if isinstance(data2, dict) else None

    text = _extract_output_text(data)
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

    history = payload.get("history")
    history = history if isinstance(history, list) else []

    ok, reply, data = _call_openai(message, mode, request=request, history=history)
    if not ok:
        return _json({"status": "error", "error": reply}, 502)

    usage = {}
    if isinstance(data, dict) and isinstance(data.get("usage"), dict):
        usage = data.get("usage")

    return _json({"status": "ok", "reply": reply, "usage": usage})
