from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from .subscriptions import (
    ADDONS,
    admin_delete_subscription,
    admin_update_subscription,
    compute_period_dates,
    is_active,
    list_all_subscriptions,
    list_active_addons_for_user,
    list_subscriptions_for_owner,
    purchase_subscription,
    update_addon_prices,
    grant_subscription_free,
    now_iso,
)

router = APIRouter()


def _json(data: Any, status: int = 200) -> Response:
    return Response(content=json.dumps(data, ensure_ascii=False), status_code=status, media_type="application/json")


def _is_sub_user(u: dict | None) -> bool:
    if not isinstance(u, dict):
        return False
    if str(u.get("role") or "").strip().lower() == "sub_user":
        return True
    return bool(u.get("company_admin_id"))


@router.get("/subscriptions", response_class=HTMLResponse, name="subscriptions")
def subscriptions_page(request: Request):
    cu = request.app.state.get_current_user(request)
    if _is_sub_user(cu):
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/", status_code=303)
    return request.app.state.render(request, "subscriptions.html", {"addons": ADDONS})


@router.get("/subscriptions/api/check", name="subscriptions_check")
def subscriptions_check(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)
    if _is_sub_user(cu):
        return _json({"status": "error", "error": "Sub users cannot access subscriptions."}, 403)

    billing = request.app.state.get_billing_user(request, cu)
    owner_id = str((billing or cu).get("id"))

    subs = list_subscriptions_for_owner(owner_id)
    active = list_active_addons_for_user(str(cu.get("id")), owner_user_id=owner_id)
    try:
        if request.app.state.is_super_admin(cu):
            active = list(ADDONS.keys())
    except Exception:
        pass
    return _json({"status": "ok", "subscriptions": subs, "active_addons": active})


@router.post("/subscriptions/api/preview", name="subscriptions_preview")
async def subscriptions_preview(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)
    if _is_sub_user(cu):
        return _json({"status": "error", "error": "Sub users cannot purchase subscriptions."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    addon = str(payload.get("addon") or "").strip()
    period = str(payload.get("period") or "").strip().lower()

    if addon not in ADDONS:
        return _json({"status": "error", "error": "Unknown add-on."}, 400)
    if period not in ("monthly", "yearly"):
        return _json({"status": "error", "error": "Invalid period."}, 400)

    users = request.app.state.load_users()
    billing = request.app.state.get_billing_user(request, cu)
    billing_u = request.app.state.find_user(users, str((billing or cu).get("id")))
    if not billing_u:
        return _json({"status": "error", "error": "Billing user not found."}, 401)

    subs = list_subscriptions_for_owner(str(billing_u.get("id")))
    active_sub = next((s for s in subs if str(s.get("addon") or "") == addon and is_active(s)), None)

    start, end = compute_period_dates(period)
    price = ADDONS[addon]["monthly_price"] if period == "monthly" else ADDONS[addon]["yearly_price"]
    preview = {
        "addon": addon,
        "addon_name": ADDONS[addon]["name"],
        "period": period,
        "price": price,
        "currency": ADDONS[addon]["currency"],
        "start_at": start.isoformat() + "Z",
        "end_at": end.isoformat() + "Z",
    }
    return _json(
        {
            "status": "ok",
            "preview": preview,
            "already_active": bool(active_sub),
            "active_until": (active_sub or {}).get("end_at") if active_sub else "",
        }
    )


@router.post("/subscriptions/api/buy", name="subscriptions_buy")
async def subscriptions_buy(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)
    if _is_sub_user(cu):
        return _json({"status": "error", "error": "Sub users cannot purchase subscriptions."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    addon = str(payload.get("addon") or "").strip()
    period = str(payload.get("period") or "").strip().lower()
    payment_method = str(payload.get("payment_method") or "credit").strip().lower()
    recurring_raw = payload.get("recurring")
    recurring = True if str(recurring_raw).lower() in ("1", "true", "yes", "on") else False

    users = request.app.state.load_users()
    billing = request.app.state.get_billing_user(request, cu)
    billing_u = request.app.state.find_user(users, str((billing or cu).get("id")))
    if not billing_u:
        return _json({"status": "error", "error": "Billing user not found."}, 401)

    if addon not in ADDONS:
        return _json({"status": "error", "error": "Unknown add-on."}, 400)

    if period not in ("monthly", "yearly"):
        return _json({"status": "error", "error": "Invalid period."}, 400)

    if payment_method not in ("credit", "cash"):
        return _json({"status": "error", "error": "Invalid payment method."}, 400)

    price = ADDONS[addon]["monthly_price"] if period == "monthly" else ADDONS[addon]["yearly_price"]
    credit = float(request.app.state.to_number(billing_u.get("credit"), 0) or 0)
    cash = float(request.app.state.to_number(billing_u.get("cash"), 0) or 0)

    subs = list_subscriptions_for_owner(str(billing_u.get("id")))
    active_sub = next((s for s in subs if str(s.get("addon") or "") == addon and is_active(s)), None)
    if active_sub:
        return _json({"status": "error", "error": "You already have an active subscription for this add-on."}, 400)

    if payment_method == "credit":
        if credit < float(price):
            return _json({"status": "error", "error": "Insufficient credit."}, 400)
    else:
        if cash < float(price):
            return _json({"status": "error", "error": "Insufficient cash."}, 400)

    ok, msg, sub = purchase_subscription(
        owner_user_id=str(billing_u.get("id")),
        addon=addon,
        period=period,
        purchased_by_user_id=str(cu.get("id")),
        recurring=recurring,
    )
    if not ok or not sub:
        return _json({"status": "error", "error": msg or "Failed."}, 400)

    if payment_method == "credit":
        new_credit = credit - float(price)
        billing_u["credit"] = int(new_credit) if int(new_credit) == new_credit else new_credit
    else:
        new_cash = cash - float(price)
        billing_u["cash"] = int(new_cash) if int(new_cash) == new_cash else new_cash
    request.app.state.save_users(users)

    # Email notification (best-effort)
    to_email = str(billing_u.get("email") or cu.get("email") or "").strip()
    subject = f"Subscription activated: {sub.get('addon_name')}"
    body = (
        f"Your subscription has been activated.\n\n"
        f"Add-on: {sub.get('addon_name')}\n"
        f"Period: {sub.get('period')}\n"
        f"Start: {sub.get('start_at')}\n"
        f"Expires: {sub.get('end_at')}\n"
    )
    sent, send_msg = request.app.state.send_email(to_email, subject, body)

    return _json(
        {
            "status": "ok",
            "subscription": sub,
            "email_sent": bool(sent),
            "email_message": send_msg,
            "payment_method": payment_method,
            "balances": {
                "credit": billing_u.get("credit"),
                "cash": billing_u.get("cash"),
            },
        }
    )


@router.post("/subscriptions/api/stop-recurring", name="subscriptions_stop_recurring")
async def subscriptions_stop_recurring(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)
    if _is_sub_user(cu):
        return _json({"status": "error", "error": "Sub users cannot change subscriptions."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    sub_id = str(payload.get("sub_id") or "").strip()
    if not sub_id:
        return _json({"status": "error", "error": "Missing subscription id."}, 400)

    billing = request.app.state.get_billing_user(request, cu) or cu
    owner_id = str(billing.get("id"))

    subs = list_subscriptions_for_owner(owner_id)
    sub = next((s for s in subs if str(s.get("id")) == sub_id), None)
    if not sub:
        return _json({"status": "error", "error": "Subscription not found."}, 404)

    if str(sub.get("owner_user_id") or "") != owner_id:
        return _json({"status": "error", "error": "Forbidden."}, 403)

    ok, msg, updated = admin_update_subscription(
        sub_id,
        {"recurring": False, "recurring_stopped_at": now_iso()},
    )
    if not ok or not updated:
        return _json({"status": "error", "error": msg or "Failed."}, 400)
    return _json({"status": "ok", "subscription": updated})

@router.post("/subscriptions/api/assign", name="subscriptions_assign")
async def subscriptions_assign(request: Request):
    """
    Company admin assigns an active add-on subscription to a sub user (employee).
    For now, this toggles assignment on the most recent ACTIVE subscription of the requested add-on.
    """
    cu = request.app.state.get_current_user(request)
    if not cu:
        return _json({"status": "error", "error": "Not authenticated."}, 401)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    addon = str(payload.get("addon") or "").strip()
    target_user_id = str(payload.get("user_id") or "").strip()
    grant = bool(payload.get("grant", True))

    if not addon or not target_user_id:
        return _json({"status": "error", "error": "Missing addon or user_id."}, 400)

    # Owner is billing user (company admin wallet)
    billing = request.app.state.get_billing_user(request, cu) or cu
    owner_id = str(billing.get("id"))

    # Find latest active sub for this owner+addon
    from .subscriptions import list_subscriptions_for_owner, is_active, admin_update_subscription

    subs = list_subscriptions_for_owner(owner_id)
    sub = next((s for s in subs if str(s.get("addon") or "") == addon and is_active(s)), None)
    if not sub:
        return _json({"status": "error", "error": "No active subscription found for this add-on."}, 400)

    assigned = sub.get("assigned_user_ids") or []
    assigned = [str(x) for x in assigned] if isinstance(assigned, list) else []

    if grant:
        if target_user_id not in assigned:
            assigned.append(target_user_id)
    else:
        assigned = [x for x in assigned if x != target_user_id]

    ok, msg, updated = admin_update_subscription(str(sub.get("id")), {"assigned_user_ids": assigned})
    if not ok or not updated:
        return _json({"status": "error", "error": msg or "Failed."}, 400)

    return _json({"status": "ok", "subscription": updated})



# ---------- Admin ----------
@router.get("/admin/subscriptions", response_class=HTMLResponse, name="admin_subscriptions")
def admin_subscriptions_page(request: Request):
    return request.app.state.render(request, "admin/subscriptions.html", {})


@router.get("/admin/subscriptions/api/list", name="admin_subscriptions_list")
def admin_subscriptions_list(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu or not request.app.state.is_super_admin(cu):
        return _json({"status": "error", "error": "Forbidden."}, 403)
    users = request.app.state.load_users()
    subs = list_all_subscriptions()
    out = []
    for s in subs:
        if not isinstance(s, dict):
            continue
        owner_id = str(s.get("owner_user_id") or "")
        owner = request.app.state.find_user(users, owner_id) if owner_id else None
        owner_name = ""
        if isinstance(owner, dict):
            owner_name = owner.get("company_name") or owner.get("username") or owner.get("email") or owner_id
        else:
            owner_name = owner_id
        row = dict(s)
        row["owner_name"] = owner_name
        out.append(row)
    return _json({"status": "ok", "subscriptions": out, "addons": ADDONS})


@router.post("/admin/subscriptions/api/addons/update", name="admin_addons_update")
async def admin_addons_update(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu or not request.app.state.is_super_admin(cu):
        return _json({"status": "error", "error": "Forbidden."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    addon = str(payload.get("addon") or "").strip()
    monthly_price = payload.get("monthly_price")
    yearly_price = payload.get("yearly_price")

    ok, msg, addon_data = update_addon_prices(addon, monthly_price, yearly_price)
    if not ok or not addon_data:
        return _json({"status": "error", "error": msg or "Failed."}, 400)
    return _json({"status": "ok", "addon": addon_data, "addons": ADDONS})


@router.post("/admin/subscriptions/api/grant", name="admin_subscriptions_grant")
async def admin_subscriptions_grant(request: Request):
    cu = request.app.state.get_current_user(request)
    if not cu or not request.app.state.is_super_admin(cu):
        return _json({"status": "error", "error": "Forbidden."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    addon = str(payload.get("addon") or "").strip()
    period = str(payload.get("period") or "").strip().lower()
    user_id = str(payload.get("user_id") or "").strip()
    if not addon or not period or not user_id:
        return _json({"status": "error", "error": "Missing addon, period, or user."}, 400)

    users = request.app.state.load_users()
    target = request.app.state.find_user(users, user_id)
    if not target:
        return _json({"status": "error", "error": "User not found."}, 404)
    if _is_sub_user(target) or not request.app.state.is_company_admin(target):
        return _json({"status": "error", "error": "Add-ons can only be granted to company users, not sub users."}, 400)

    ok, msg, sub = grant_subscription_free(
        owner_user_id=user_id,
        addon=addon,
        period=period,
        granted_by_user_id=str(cu.get("id")),
    )
    if not ok or not sub:
        return _json({"status": "error", "error": msg or "Failed."}, 400)
    return _json({"status": "ok", "subscription": sub})


@router.post("/admin/subscriptions/api/{sub_id}/update", name="admin_subscriptions_update")
async def admin_subscriptions_update(request: Request, sub_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu or not request.app.state.is_super_admin(cu):
        return _json({"status": "error", "error": "Forbidden."}, 403)

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    ok, msg, sub = admin_update_subscription(sub_id, payload if isinstance(payload, dict) else {})
    if not ok or not sub:
        return _json({"status": "error", "error": msg or "Failed."}, 400)
    return _json({"status": "ok", "subscription": sub})


@router.post("/admin/subscriptions/api/{sub_id}/delete", name="admin_subscriptions_delete")
def admin_subscriptions_delete(request: Request, sub_id: str):
    cu = request.app.state.get_current_user(request)
    if not cu or not request.app.state.is_super_admin(cu):
        return _json({"status": "error", "error": "Forbidden."}, 403)
    deleted = admin_delete_subscription(sub_id)
    return _json({"status": "ok", "deleted": bool(deleted)})
