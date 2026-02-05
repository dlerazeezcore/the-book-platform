from __future__ import annotations

import json
import time

from fastapi import APIRouter, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from services.esim.oasis.service import (
    balance as esim_balance,
    create_order as esim_create_order,
    get_order as esim_get_order,
    list_bundles as esim_list_bundles,
    list_orders as esim_list_orders,
    load_config as load_esim_config,
    ping as esim_ping,
    quote as esim_quote,
    save_config as save_esim_config,
)

router = APIRouter()

_ESIM_BUNDLES_CACHE: dict[str, dict] = {}
_ESIM_BUNDLES_TTL_SEC = 300


def _esim_cache_key(params: dict | None, settings: dict) -> str:
    payload = {
        "params": params or {},
        "allowed": settings.get("allowed_countries") or [],
        "fx_rate": settings.get("fx_rate") or 0,
        "markup_percent": settings.get("markup_percent") or 0,
        "markup_fixed_iqd": settings.get("markup_fixed_iqd") or 0,
    }
    return json.dumps(payload, sort_keys=True)


def _esim_cache_get(key: str) -> dict | None:
    item = _ESIM_BUNDLES_CACHE.get(key)
    if not item:
        return None
    ts = float(item.get("ts") or 0)
    if (time.time() - ts) > _ESIM_BUNDLES_TTL_SEC:
        _ESIM_BUNDLES_CACHE.pop(key, None)
        return None
    return item.get("value")


def _esim_cache_set(key: str, value: dict) -> None:
    _ESIM_BUNDLES_CACHE[key] = {"ts": time.time(), "value": value}


@router.get("/api/other-apis/esim")
async def esim_config_get():
    return load_esim_config()


@router.post("/api/other-apis/esim")
async def esim_config_set(payload: dict):
    try:
        cfg = save_esim_config(payload or {})
        return cfg
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/other-apis/esim/ping")
async def esim_ping_endpoint():
    try:
        return esim_ping()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _esim_settings() -> dict:
    cfg = load_esim_config()
    settings = cfg.get("settings") if isinstance(cfg.get("settings"), dict) else {}
    allowed = settings.get("allowed_countries")
    if not isinstance(allowed, list):
        allowed = []
    settings["allowed_countries"] = [str(x).strip().upper() for x in allowed if str(x).strip()]
    popular = settings.get("popular_destinations")
    if not isinstance(popular, list):
        popular = []
    norm_popular = []
    for item in popular:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        norm_popular.append(
            {
                "name": name,
                "iso": str(item.get("iso") or "").strip().upper(),
                "initials": str(item.get("initials") or "").strip().upper(),
            }
        )
        if len(norm_popular) >= 16:
            break
    settings["popular_destinations"] = norm_popular
    return settings


def _esim_apply_country_filter(item: dict, allowed: list[str]) -> tuple[bool, dict]:
    if not allowed:
        return True, item
    countries = item.get("countries") or []
    if not isinstance(countries, list):
        countries = []
    filtered = []
    for c in countries:
        if not isinstance(c, dict):
            continue
        iso = str(c.get("iso") or "").strip().upper()
        if iso and iso in allowed:
            filtered.append(c)
    if not filtered:
        return False, item
    item["countries"] = filtered
    return True, item


def _esim_apply_pricing(item: dict, settings: dict) -> dict:
    price = item.get("price") or {}
    try:
        usd_minor = price.get("finalMinor")
        if usd_minor is None:
            return item
        usd_minor = float(usd_minor)
        fx = float(settings.get("fx_rate") or 0)
        if fx <= 0:
            return item
        iqd = (usd_minor / 100.0) * fx
        pct = float(settings.get("markup_percent") or 0)
        if pct:
            iqd = iqd * (1 + pct / 100.0)
        fixed = float(settings.get("markup_fixed_iqd") or 0)
        if fixed:
            iqd += fixed
        iqd_final = int(round(iqd))
        price["finalMinor"] = iqd_final
        price["currency"] = "IQD"
        item["price"] = price
        item["price_usd_minor"] = int(usd_minor)
        item["fx_rate"] = fx
        item["markup_percent"] = pct
        item["markup_fixed_iqd"] = fixed
        return item
    except Exception:
        return item


@router.get("/api/esim/bundles")
async def esim_bundles(request: Request):
    try:
        params = dict(request.query_params)
        settings = _esim_settings()
        cache_key = _esim_cache_key(params, settings)
        cached = _esim_cache_get(cache_key)
        if cached:
            return cached

        data = await run_in_threadpool(esim_list_bundles, params=params or None)
        allowed = settings.get("allowed_countries") or []
        items = data.get("items") or data.get("bundles") or []
        out_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            ok, item = _esim_apply_country_filter(item, allowed)
            if not ok:
                continue
            item = _esim_apply_pricing(item, settings)
            out_items.append(item)
        data["items"] = out_items
        if "bundles" in data:
            data["bundles"] = out_items
        _esim_cache_set(cache_key, data)
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/esim/quote")
async def esim_quote_endpoint(payload: dict):
    try:
        data = esim_quote(payload or {})
        settings = _esim_settings()
        allowed = settings.get("allowed_countries") or []
        if isinstance(data, dict):
            ok, data = _esim_apply_country_filter(data, allowed)
            if not ok:
                raise HTTPException(status_code=403, detail="Bundle not available for allowed countries.")
            data = _esim_apply_pricing(data, settings)
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/esim/orders")
async def esim_order_create(payload: dict):
    try:
        body = dict(payload or {})
        if "idempotencyKey" not in body and body.get("idempotency_key"):
            body["idempotencyKey"] = body.pop("idempotency_key")
        idempotency_key = str(body.get("idempotencyKey") or "").strip() or None
        data = esim_create_order(body, idempotency_key=idempotency_key)
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/esim/orders")
async def esim_orders_list(request: Request):
    try:
        params = dict(request.query_params)
        data = esim_list_orders(params=params or None)
        settings = _esim_settings()
        if isinstance(data, dict) and settings.get("fx_rate"):
            items = data.get("items") or []
            out = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    usd_minor = float(item.get("totalMinor"))
                except Exception:
                    usd_minor = None
                if usd_minor is not None:
                    fx = float(settings.get("fx_rate") or 0)
                    pct = float(settings.get("markup_percent") or 0)
                    fixed = float(settings.get("markup_fixed_iqd") or 0)
                    iqd = (usd_minor / 100.0) * fx
                    if pct:
                        iqd = iqd * (1 + pct / 100.0)
                    if fixed:
                        iqd += fixed
                    item["total_iqd"] = int(round(iqd))
                out.append(item)
            data["items"] = out
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/esim/orders/{order_id}")
async def esim_order_get(order_id: str):
    try:
        data = esim_get_order(order_id)
        settings = _esim_settings()
        if isinstance(data, dict) and settings.get("fx_rate"):
            try:
                usd_minor = float(data.get("totalMinor"))
            except Exception:
                usd_minor = None
            if usd_minor is not None:
                fx = float(settings.get("fx_rate") or 0)
                pct = float(settings.get("markup_percent") or 0)
                fixed = float(settings.get("markup_fixed_iqd") or 0)
                iqd = (usd_minor / 100.0) * fx
                if pct:
                    iqd = iqd * (1 + pct / 100.0)
                if fixed:
                    iqd += fixed
                data["total_iqd"] = int(round(iqd))
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/esim/balance")
async def esim_balance_get():
    try:
        data = esim_balance()
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/esim/settings")
async def esim_public_settings():
    settings = _esim_settings()
    return {
        "allowed_countries": settings.get("allowed_countries") or [],
        "fx_rate": settings.get("fx_rate") or 0,
        "markup_percent": settings.get("markup_percent") or 0,
        "markup_fixed_iqd": settings.get("markup_fixed_iqd") or 0,
        "popular_destinations": settings.get("popular_destinations") or [],
    }
