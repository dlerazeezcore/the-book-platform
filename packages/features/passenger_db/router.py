from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from .passenger_db import (
    compute_view_profile,
    create_member,
    create_profile,
    find_members_by_query,
    load_profiles,
    save_profiles,
    backfill_esim_history_for_member,
    history_for_member,
)
from packages.features.subscriptions.subscriptions import list_active_addons_for_user


router = APIRouter()


def _json(data: Any, status: int = 200) -> Response:
    return Response(content=json.dumps(data, ensure_ascii=False), status_code=status, media_type="application/json")


def _now_iso() -> str:
    from datetime import datetime
    return datetime.utcnow().isoformat() + "Z"


def _get_owner_and_access(request: Request, cu: dict | None) -> tuple[str, bool]:
    """
    Returns (owner_id, has_access) for Passenger DB.
    - owner_id is the company admin (billing) id for sub users, or the user id otherwise.
    - sub users must have passenger_database assigned from their company admin.
    """
    if not cu:
        return "", False
    users = request.app.state.load_users()
    owner = request.app.state.get_billing_user(request, cu) or cu
    owner_id = str((owner or {}).get("id") or "")

    try:
        if request.app.state.is_super_admin(cu):
            return owner_id, True
        if request.app.state.is_company_admin(cu):
            return owner_id, True
        if request.app.state.is_sub_user(cu):
            active = list_active_addons_for_user(str(cu.get("id")), owner_user_id=owner_id)
            return owner_id, ("passenger_database" in active)
        # company admin (owner) must have active subscription
        active_owner = list_active_addons_for_user(owner_id, owner_user_id=owner_id)
        return owner_id, ("passenger_database" in active_owner)
    except Exception:
        pass
    return owner_id, False


@router.get("/passenger-database", response_class=HTMLResponse, name="passenger_database")
def passenger_database_page(request: Request):
    # Page gated by subscription check handled in app.py (template has an empty state too).
    return request.app.state.render(request, "passenger_database/index.html", {})


@router.get("/passenger-database/api/search", name="passenger_database_search")
def passenger_database_search(request: Request, q: str = ""):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    profiles = load_profiles()
    # Scope to owner (company admin) database; optionally allow explicit sharing.
    user_id = str(cu.get("id"))
    accessible = []
    for p in profiles:
        if str(p.get("owner_user_id") or "") == owner_id:
            accessible.append(p)
            continue
        allowed = p.get("allowed_user_ids") or []
        allowed = [str(x) for x in allowed] if isinstance(allowed, list) else []
        if user_id in allowed:
            accessible.append(p)
    results = find_members_by_query(accessible, q) if (q or "").strip() else accessible

    return _json({"status": "ok", "results": [compute_view_profile(p) for p in results]})


@router.post("/passenger-database/api/profile", name="passenger_database_create_profile")
async def passenger_database_create_profile(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    label = str(payload.get("label") or "").strip()
    phone = str(payload.get("phone") or "").strip()

    profiles = load_profiles()
    prof = create_profile(owner_user_id=owner_id, label=label, phone=phone)
    profiles.append(prof)
    save_profiles(profiles)
    return _json({"status": "ok", "profile": compute_view_profile(prof)})


@router.put("/passenger-database/api/profile/{profile_id}", name="passenger_database_update_profile")
async def passenger_database_update_profile(request: Request, profile_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    profiles = load_profiles()
    prof = next((p for p in profiles if str(p.get("id")) == str(profile_id)), None)
    if not prof:
        return _json({"status": "error", "error": "Profile not found."}, 404)

    # Owner (company admin) only
    if str(prof.get("owner_user_id") or "") != owner_id:
        return _json({"status": "error", "error": "Forbidden."}, 403)

    prof["label"] = str(payload.get("label") or prof.get("label") or "").strip()
    prof["phone"] = str(payload.get("phone") or prof.get("phone") or "").strip()
    prof["updated_at"] = _now_iso()

    save_profiles(profiles)
    return _json({"status": "ok", "profile": compute_view_profile(prof)})


@router.delete("/passenger-database/api/profile/{profile_id}", name="passenger_database_delete_profile")
async def passenger_database_delete_profile(request: Request, profile_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    profiles = load_profiles()
    prof = next((p for p in profiles if str(p.get("id")) == str(profile_id)), None)
    if not prof:
        return _json({"status": "ok", "deleted": False})

    if str(prof.get("owner_user_id") or "") != owner_id:
        return _json({"status": "error", "error": "Forbidden."}, 403)

    profiles = [p for p in profiles if str(p.get("id")) != str(profile_id)]
    save_profiles(profiles)
    return _json({"status": "ok", "deleted": True})


@router.post("/passenger-database/api/profile/{profile_id}/member", name="passenger_database_add_member")
async def passenger_database_add_member(request: Request, profile_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    profiles = load_profiles()
    prof = next((p for p in profiles if str(p.get("id")) == str(profile_id)), None)
    if not prof:
        return _json({"status": "error", "error": "Profile not found."}, 404)

    if str(prof.get("owner_user_id") or "") != owner_id:
        return _json({"status": "error", "error": "Forbidden."}, 403)

    mem = create_member(payload if isinstance(payload, dict) else {})
    prof.setdefault("members", []).append(mem)
    prof["updated_at"] = _now_iso()
    save_profiles(profiles)
    return _json({"status": "ok", "profile": compute_view_profile(prof)})


@router.put("/passenger-database/api/profile/{profile_id}/member/{member_id}", name="passenger_database_update_member")
async def passenger_database_update_member(request: Request, profile_id: str, member_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    profiles = load_profiles()
    prof = next((p for p in profiles if str(p.get("id")) == str(profile_id)), None)
    if not prof:
        return _json({"status": "error", "error": "Profile not found."}, 404)

    if str(prof.get("owner_user_id") or "") != owner_id:
        return _json({"status": "error", "error": "Forbidden."}, 403)

    members = prof.get("members") or []
    mem = next((m for m in members if str(m.get("id")) == str(member_id)), None)
    if not mem:
        return _json({"status": "error", "error": "Member not found."}, 404)

    for k in ("title", "first_name", "last_name", "dob", "nationality", "national_id_number", "phone", "notes"):
        if k in payload:
            mem[k] = str(payload.get(k) or "").strip()

    # passports comes as list, replace after basic validation
    passports = payload.get("passports")
    if isinstance(passports, list):
        cleaned = []
        seen = set()
        for d in passports:
            if not isinstance(d, dict):
                continue
            num = str(d.get("number") or "").strip()
            if not num:
                continue
            key = num.strip().lower()
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(
                {
                    "number": num,
                    "issue_date": str(d.get("issue_date") or "").strip(),
                    "expiry_date": str(d.get("expiry_date") or "").strip(),
                    "issue_place": str(d.get("issue_place") or "").strip(),
                }
            )
        mem["passports"] = cleaned

    mem["updated_at"] = _now_iso()
    prof["updated_at"] = _now_iso()
    save_profiles(profiles)
    return _json({"status": "ok", "profile": compute_view_profile(prof)})


@router.delete("/passenger-database/api/profile/{profile_id}/member/{member_id}", name="passenger_database_delete_member")
async def passenger_database_delete_member(request: Request, profile_id: str, member_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    profiles = load_profiles()
    prof = next((p for p in profiles if str(p.get("id")) == str(profile_id)), None)
    if not prof:
        return _json({"status": "error", "error": "Profile not found."}, 404)

    if str(prof.get("owner_user_id") or "") != owner_id:
        return _json({"status": "error", "error": "Forbidden."}, 403)

    members = prof.get("members") or []
    prof["members"] = [m for m in members if str(m.get("id")) != str(member_id)]
    prof["updated_at"] = _now_iso()
    save_profiles(profiles)
    return _json({"status": "ok", "profile": compute_view_profile(prof)})


@router.get("/passenger-database/api/member/{member_id}/history", name="passenger_database_member_history")
def passenger_database_member_history(request: Request, member_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    owner_id, ok = _get_owner_and_access(request, cu)
    if not ok:
        return _json({"status": "error", "error": "Passenger Database access is not active for this user."}, 403)

    # Backfill eSIM history for this member (best-effort).
    try:
        profiles = load_profiles()
        prof = None
        mem = None
        for p in profiles:
            for m in (p.get("members") or []):
                if str(m.get("id") or "") == str(member_id):
                    prof = p
                    mem = m
                    break
            if prof and mem:
                break
        if prof and mem:
            from packages.features.esim.orders import list_orders_for_owner
            hist_owner_id = str(prof.get("owner_user_id") or owner_id)
            orders = list_orders_for_owner(hist_owner_id)
            backfill_esim_history_for_member(hist_owner_id, prof, mem, orders)
    except Exception:
        pass

    # Scope history by the profile owner if available (super admins can view any profile).
    try:
        if "prof" in locals() and prof:
            hist_owner_id = str(prof.get("owner_user_id") or owner_id)
        else:
            hist_owner_id = owner_id
    except Exception:
        hist_owner_id = owner_id
    return _json({"status": "ok", "history": history_for_member(hist_owner_id, member_id)})
