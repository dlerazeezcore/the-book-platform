from __future__ import annotations

from fastapi import APIRouter, HTTPException

from services.gateway.permissions_store import (
    _compute_schedule_windows,
    _load_permissions,
    _save_permissions,
    _ticketing_schedule_allows,
)

router = APIRouter()


@router.get("/api/permissions")
async def get_permissions():
    return _load_permissions()


@router.post("/api/permissions")
async def set_permissions(payload: dict):
    try:
        # Keep it simple: accept full object and write it.
        cfg = _save_permissions(payload or {})
        return cfg
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/permissions/status")
async def permissions_status():
    cfg = _load_permissions()
    providers = cfg.get("providers") or {}
    out = {}
    for code, p in providers.items():
        if not isinstance(p, dict):
            continue
        availability = bool(p.get("availability_enabled", True))
        blocked_suppliers = p.get("blocked_suppliers") or []
        if str(code) in [str(x).strip() for x in blocked_suppliers]:
            availability = False

        ticketing_mode = (p.get("ticketing_mode") or "full").strip().lower()
        schedule_cfg = p.get("ticketing_schedule") or {}
        schedule_ok = _ticketing_schedule_allows(schedule_cfg)
        ticketing_effective = availability and (ticketing_mode == "full") and schedule_ok
        schedule_info = _compute_schedule_windows(schedule_cfg)
        out[str(code)] = {
            "availability": availability,
            "ticketing_mode": "full" if ticketing_mode == "full" else "availability_only",
            "ticketing_schedule_ok": schedule_ok,
            "ticketing_effective": ticketing_effective,
            "schedule": schedule_info,
        }
    return {"providers": out}
