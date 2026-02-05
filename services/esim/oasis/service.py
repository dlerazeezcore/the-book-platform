from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import httpx


CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
DEFAULT_BASE_URL = "https://www.esimoasis.com/api/v1"


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def _split_token(raw: str) -> tuple[str, str]:
    raw = (raw or "").strip()
    if not raw or ":" not in raw:
        return raw, ""
    key_id, secret = raw.split(":", 1)
    return key_id.strip(), secret.strip()


def _normalize_account(raw: Dict[str, Any]) -> Dict[str, str]:
    key_id = str(raw.get("key_id") or "").strip()
    secret = str(raw.get("secret") or "").strip()

    if ":" in key_id and (not secret or secret == key_id):
        key_id, secret = _split_token(key_id)
    elif ":" in secret and (not key_id or key_id == secret):
        key_id, secret = _split_token(secret)

    return {
        "id": str(raw.get("id") or "").strip(),
        "label": str(raw.get("label") or ""),
        "key_id": key_id,
        "secret": secret,
        "base_url": str(raw.get("base_url") or ""),
    }


def _normalize_popular_destinations(raw: Any) -> list[dict]:
    raw_list = raw if isinstance(raw, list) else []
    out: list[dict] = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        iso = str(item.get("iso") or "").strip().upper()
        initials = str(item.get("initials") or "").strip().upper()
        out.append(
            {
                "name": name,
                "iso": iso,
                "initials": initials,
            }
        )
        if len(out) >= 16:
            break
    return out


def _normalize_settings(raw: Any) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    allowed = raw.get("allowed_countries")
    if not isinstance(allowed, list):
        allowed = []
    allowed = [str(x).strip().upper() for x in allowed if str(x).strip()]

    def _num(v: Any, default: float = 0.0) -> float:
        try:
            if v is None or v == "":
                return default
            return float(v)
        except Exception:
            return default

    popular = _normalize_popular_destinations(raw.get("popular_destinations"))

    return {
        "allowed_countries": allowed,
        "fx_rate": _num(raw.get("fx_rate"), 0.0),
        "markup_percent": _num(raw.get("markup_percent"), 0.0),
        "markup_fixed_iqd": _num(raw.get("markup_fixed_iqd"), 0.0),
        "popular_destinations": popular,
    }


def load_config() -> dict:
    try:
        if CONFIG_PATH.exists():
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8") or "{}")
            if isinstance(data, dict):
                accounts = data.get("accounts")
                if not isinstance(accounts, list):
                    accounts = []
                data["accounts"] = [_normalize_account(a) for a in accounts if isinstance(a, dict)]
                if "active_account_id" not in data:
                    data["active_account_id"] = ""
                data["settings"] = _normalize_settings(data.get("settings"))
                if not isinstance(data.get("fx_history"), list):
                    data["fx_history"] = []
                return data
    except Exception:
        pass
    return {"accounts": [], "active_account_id": "", "settings": _normalize_settings({}), "fx_history": []}


def save_config(cfg: dict) -> dict:
    if not isinstance(cfg, dict):
        cfg = {}
    existing = load_config()

    accounts = cfg.get("accounts")
    if not isinstance(accounts, list):
        accounts = []
    norm_accounts = []
    for a in accounts:
        if not isinstance(a, dict):
            continue
        normalized = _normalize_account(a)
        aid = normalized.get("id") or ""
        if not aid:
            continue
        norm_accounts.append(normalized)
    cfg["accounts"] = norm_accounts
    raw_active = cfg.get("active_account_id", None)
    if raw_active is None:
        active = norm_accounts[0].get("id") if norm_accounts else ""
    else:
        active = str(raw_active).strip()
        if active and not any(a.get("id") == active for a in norm_accounts):
            active = ""
    cfg["active_account_id"] = active

    incoming_settings = cfg.get("settings")
    if isinstance(incoming_settings, dict):
        settings = _normalize_settings(incoming_settings)
    else:
        settings = _normalize_settings(existing.get("settings") or {})
    cfg["settings"] = settings

    fx_history = existing.get("fx_history") if isinstance(existing.get("fx_history"), list) else []
    old_rate = (existing.get("settings") or {}).get("fx_rate")
    new_rate = settings.get("fx_rate")
    if new_rate and new_rate != old_rate:
        changed_by = str(cfg.get("fx_updated_by") or "").strip()
        changed_by_id = str(cfg.get("fx_updated_by_id") or "").strip()
        fx_history = list(fx_history)
        fx_history.append(
            {
                "rate": new_rate,
                "created_at": _now_iso(),
                "created_by": changed_by,
                "created_by_id": changed_by_id,
            }
        )
    cfg["fx_history"] = fx_history

    # drop transient fields
    if "fx_updated_by" in cfg:
        cfg.pop("fx_updated_by", None)
    if "fx_updated_by_id" in cfg:
        cfg.pop("fx_updated_by_id", None)

    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    return cfg


def _get_active_account() -> dict:
    cfg = load_config()
    accounts = cfg.get("accounts") or []
    active_id = str(cfg.get("active_account_id") or "").strip()
    account = next((a for a in accounts if str(a.get("id")) == active_id), None)

    env_key_id = os.getenv("ESIM_OASIS_KEY_ID") or ""
    env_secret = os.getenv("ESIM_OASIS_SECRET") or ""
    env_base = os.getenv("ESIM_OASIS_BASE_URL") or ""

    if not account and env_key_id and env_secret:
        account = {
            "id": "env",
            "label": "ENV",
            "key_id": env_key_id,
            "secret": env_secret,
            "base_url": env_base or DEFAULT_BASE_URL,
        }

    if not account:
        raise ValueError("No active eSIM Oasis account configured.")

    account = _normalize_account(account)

    if not (account.get("key_id") and account.get("secret")):
        raise ValueError("eSIM Oasis credentials are missing.")

    return account


def _request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    idempotency_key: str | None = None,
) -> dict:
    account = _get_active_account()
    base_url = (account.get("base_url") or DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
    url = base_url.rstrip("/") + "/" + path.lstrip("/")

    headers = {
        "Authorization": f"Bearer {account.get('key_id')}:{account.get('secret')}",
        "Accept": "application/json",
    }
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    resp = httpx.request(
        method.upper(),
        url,
        params=params or None,
        json=payload if payload is not None else None,
        headers=headers,
        timeout=30,
    )

    if resp.status_code >= 400:
        raise ValueError(f"eSIM Oasis request failed ({resp.status_code}): {resp.text}")

    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}


def ping() -> dict:
    return _request("GET", "/ping")


def list_bundles(params: Optional[Dict[str, Any]] = None) -> dict:
    return _request("GET", "/catalog", params=params)


def quote(payload: Dict[str, Any]) -> dict:
    return _request("POST", "/quote", payload=payload)


def create_order(payload: Dict[str, Any], idempotency_key: str | None = None) -> dict:
    return _request("POST", "/orders", payload=payload, idempotency_key=idempotency_key)


def get_order(order_id: str) -> dict:
    return _request("GET", f"/orders/{order_id}")


def list_orders(params: Optional[Dict[str, Any]] = None) -> dict:
    return _request("GET", "/orders", params=params)


def balance() -> dict:
    return _request("GET", "/balance")
