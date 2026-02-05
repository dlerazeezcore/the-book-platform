from __future__ import annotations

import json
import os
import time
from pathlib import Path

import httpx


CONFIG_PATH = Path(__file__).resolve().parent / "config.json"


def load_config() -> dict:
    try:
        if CONFIG_PATH.exists():
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8") or "{}")
            if isinstance(data, dict):
                if not isinstance(data.get("accounts"), list):
                    data["accounts"] = []
                if "active_account_id" not in data:
                    data["active_account_id"] = ""
                return data
    except Exception:
        pass
    return {"accounts": [], "active_account_id": ""}


def save_config(cfg: dict) -> dict:
    if not isinstance(cfg, dict):
        cfg = {}
    accounts = cfg.get("accounts")
    if not isinstance(accounts, list):
        accounts = []
    norm_accounts = []
    for a in accounts:
        if not isinstance(a, dict):
            continue
        aid = str(a.get("id") or "").strip()
        if not aid:
            continue
        norm_accounts.append(
            {
                "id": aid,
                "label": str(a.get("label") or ""),
                "client_id": str(a.get("client_id") or ""),
                "client_secret": str(a.get("client_secret") or ""),
                "base_url": str(a.get("base_url") or ""),
            }
        )
    cfg["accounts"] = norm_accounts
    raw_active = cfg.get("active_account_id", None)
    if raw_active is None:
        # If not provided, default to the first account.
        active = norm_accounts[0].get("id") if norm_accounts else ""
    else:
        active = str(raw_active).strip()
        if active and not any(a.get("id") == active for a in norm_accounts):
            active = ""
    cfg["active_account_id"] = active

    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    return cfg


def _get_active_account() -> dict:
    cfg = load_config()
    accounts = cfg.get("accounts") or []
    active_id = str(cfg.get("active_account_id") or "").strip()
    account = next((a for a in accounts if str(a.get("id")) == active_id), None)

    env_base = os.getenv("FIB_BASE_URL") or ""
    env_client_id = os.getenv("FIB_CLIENT_ID") or ""
    env_client_secret = os.getenv("FIB_CLIENT_SECRET") or ""

    if not account and env_base and env_client_id and env_client_secret:
        account = {
            "id": "env",
            "label": "ENV",
            "client_id": env_client_id,
            "client_secret": env_client_secret,
            "base_url": env_base,
        }

    if not account:
        raise ValueError("No active FIB account configured.")

    if not (account.get("client_id") and account.get("client_secret")):
        raise ValueError("FIB credentials are missing.")

    return account


def _get_access_token(account: dict) -> str:
    base_url = (account.get("base_url") or "").strip()
    if not base_url:
        raise ValueError("FIB base URL is missing.")

    token_url = base_url.rstrip("/") + "/auth/realms/fib-online-shop/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": account.get("client_id"),
        "client_secret": account.get("client_secret"),
    }

    resp = httpx.post(token_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=20)
    if resp.status_code != 200:
        raise ValueError(f"Token request failed ({resp.status_code}): {resp.text}")
    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise ValueError("Missing access_token in FIB response.")
    return token


def create_payment(amount_iqd: int, description: str | None = None) -> dict:
    account = _get_active_account()
    token = _get_access_token(account)

    base_url = (account.get("base_url") or "").strip()
    pay_url = base_url.rstrip("/") + "/protected/v1/payments"

    public_base = (os.getenv("PUBLIC_BASE_URL") or "http://127.0.0.1:8000").rstrip("/")
    redirect_uri = public_base + "/fib/return"
    status_callback = public_base + "/fib/webhook"
    desc = (description or "Payment").strip()

    payload = {
        "monetaryValue": {
            "amount": str(int(amount_iqd or 0)),
            "currency": "IQD",
        },
        "statusCallbackUrl": status_callback,
        "description": desc,
        "redirectUri": redirect_uri,
        "expiresIn": "PT1H",
        "category": "ECOMMERCE",
        "refundableFor": "PT48H",
    }

    resp = httpx.post(
        pay_url,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=20,
    )
    if resp.status_code not in (200, 201):
        raise ValueError(f"Payment request failed ({resp.status_code}): {resp.text}")

    data = resp.json()
    ref = data.get("readableCode") or data.get("paymentId") or f"FIB-{int(time.time())}"
    payment_link = (
        data.get("personalAppLink")
        or data.get("businessAppLink")
        or data.get("corporateAppLink")
        or ""
    )
    qr_code = data.get("qrCode") or ""

    return {
        "reference": ref,
        "amount": int(amount_iqd or 0),
        "currency": "IQD",
        "description": desc,
        "account_id": account.get("id"),
        "account_label": account.get("label") or "",
        "payment_link": payment_link,
        "qr_url": qr_code,
        "readable_code": data.get("readableCode"),
        "payment_id": data.get("paymentId"),
        "valid_until": data.get("validUntil"),
        "links": {
            "personal": data.get("personalAppLink"),
            "business": data.get("businessAppLink"),
            "corporate": data.get("corporateAppLink"),
        },
    }
