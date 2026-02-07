from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data"
SUBS_PATH = DATA_DIR / "subscriptions.json"
ADDONS_PATH = DATA_DIR / "addons.json"

ADDONS_DEFAULT = {
    "passenger_database": {
        "name": "Passenger Database",
        "description": "Profiles, family members, search, and automatic history linking.",
        "monthly_price": 10000,
        "yearly_price": 100000,
        "currency": "IQD",
    },
    "visa_vendor": {
        "name": "Visa Vendor",
        "description": "List visa prices, receive vendor submissions, and update status.",
        "monthly_price": 20000,
        "yearly_price": 200000,
        "currency": "IQD",
    },
    "esim": {
        "name": "eSIM",
        "description": "Browse, quote, and order eSIM bundles for customers.",
        "monthly_price": 15000,
        "yearly_price": 150000,
        "currency": "IQD",
    },
    "ai_assistant": {
        "name": "AI Assistant",
        "description": "Travel info + operations assistant for staff (airport codes, workflows, FAQs).",
        "monthly_price": 25000,
        "yearly_price": 250000,
        "currency": "IQD",
    },
}


def _normalize_addons(raw: Any) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    raw_dict = raw if isinstance(raw, dict) else {}
    for key, base in ADDONS_DEFAULT.items():
        merged = dict(base)
        if isinstance(raw_dict.get(key), dict):
            merged.update(raw_dict.get(key))
        out[key] = merged
    for key, val in raw_dict.items():
        if key not in out and isinstance(val, dict):
            out[key] = dict(val)
    return out


def load_addons() -> Dict[str, Dict[str, Any]]:
    try:
        if not ADDONS_PATH.exists():
            return _normalize_addons({})
        raw = ADDONS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else {}
        return _normalize_addons(data)
    except Exception:
        return _normalize_addons({})


def save_addons(addons: Dict[str, Dict[str, Any]]) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ADDONS_PATH.write_text(json.dumps(addons, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


ADDONS = load_addons()


def _ensure_file() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not SUBS_PATH.exists():
        SUBS_PATH.write_text("[]", encoding="utf-8")


def _load() -> List[Dict[str, Any]]:
    try:
        _ensure_file()
        raw = SUBS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else []
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(data: List[Dict[str, Any]]) -> None:
    _ensure_file()
    SUBS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _new_id() -> str:
    return f"sub_{uuid.uuid4().hex[:12]}"


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def parse_iso(dt: str) -> Optional[datetime]:
    try:
        dt = (dt or "").strip()
        if not dt:
            return None
        if dt.endswith("Z"):
            dt = dt[:-1]
        return datetime.fromisoformat(dt)
    except Exception:
        return None


def is_active(sub: Dict[str, Any], at: Optional[datetime] = None) -> bool:
    at = at or datetime.utcnow()
    if str(sub.get("status") or "").lower() != "active":
        return False
    end = parse_iso(str(sub.get("end_at") or ""))
    if not end:
        return False
    return at < end


def compute_period_dates(period: str, start_at: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    period = str(period or "").strip().lower()
    start = start_at or datetime.utcnow()
    if period == "yearly":
        end = start + timedelta(days=365)
    else:
        end = start + timedelta(days=30)
    return start, end


def update_addon_prices(
    addon: str,
    monthly_price: Optional[float] = None,
    yearly_price: Optional[float] = None,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    addon = str(addon or "").strip()
    if addon not in ADDONS:
        return False, "Unknown add-on.", None

    try:
        if monthly_price is not None:
            ADDONS[addon]["monthly_price"] = float(monthly_price)
        if yearly_price is not None:
            ADDONS[addon]["yearly_price"] = float(yearly_price)
    except Exception:
        return False, "Invalid price.", None

    save_addons(ADDONS)
    return True, "Updated.", ADDONS.get(addon)


def grant_subscription_free(
    owner_user_id: str,
    addon: str,
    period: str,
    granted_by_user_id: str,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    addon = str(addon or "").strip()
    period = str(period or "").strip().lower()

    if addon not in ADDONS:
        return False, "Unknown add-on.", None
    if period not in ("monthly", "yearly"):
        return False, "Invalid period.", None

    subs = _load()
    active_sub = next(
        (s for s in subs if str(s.get("owner_user_id") or "") == str(owner_user_id)
         and str(s.get("addon") or "") == addon and is_active(s)),
        None,
    )
    if active_sub:
        return False, "User already has an active subscription.", None

    start, end = compute_period_dates(period)
    sub = {
        "id": _new_id(),
        "owner_user_id": str(owner_user_id),
        "addon": addon,
        "addon_name": ADDONS[addon]["name"],
        "period": period,
        "price": 0,
        "currency": ADDONS[addon]["currency"],
        "status": "active",
        "start_at": start.isoformat() + "Z",
        "end_at": end.isoformat() + "Z",
        "recurring": False,
        "renewal_period": "",
        "recurring_stopped_at": "",
        "assigned_user_ids": [],
        "purchased_by_user_id": str(granted_by_user_id),
        "payment_method": "admin_grant",
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    subs.append(sub)
    _save(subs)
    return True, "Granted.", sub


def list_subscriptions_for_owner(owner_user_id: str) -> List[Dict[str, Any]]:
    subs = _load()
    out = [s for s in subs if str(s.get("owner_user_id") or "") == str(owner_user_id)]
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out


def list_active_addons_for_user(user_id: str, owner_user_id: Optional[str] = None) -> List[str]:
    """
    Determine which addons are active for this user:
      - owned by the user (company admin)
      - OR assigned to the user by their company admin owner (assigned_user_ids)
    If owner_user_id is provided, we also consider that owner's subscriptions.
    """
    subs = _load()
    active: List[str] = []
    for s in subs:
        addon = str(s.get("addon") or "")
        if not addon:
            continue
        if not is_active(s):
            continue

        owner = str(s.get("owner_user_id") or "")
        assigned = s.get("assigned_user_ids") or []
        assigned = [str(x) for x in assigned] if isinstance(assigned, list) else []

        if owner_user_id:
            if owner != str(owner_user_id):
                continue
        # active for:
        # - owner
        # - assigned users
        if owner == str(user_id) or str(user_id) in assigned:
            if addon not in active:
                active.append(addon)
    return active


def purchase_subscription(
    owner_user_id: str,
    addon: str,
    period: str,
    purchased_by_user_id: str,
    recurring: bool = False,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    addon = str(addon or "").strip()
    period = str(period or "").strip().lower()

    if addon not in ADDONS:
        return False, "Unknown add-on.", None
    if period not in ("monthly", "yearly"):
        return False, "Invalid period.", None

    start, end = compute_period_dates(period)
    price = ADDONS[addon]["monthly_price"] if period == "monthly" else ADDONS[addon]["yearly_price"]

    sub = {
        "id": _new_id(),
        "owner_user_id": str(owner_user_id),
        "addon": addon,
        "addon_name": ADDONS[addon]["name"],
        "period": period,
        "price": price,
        "currency": ADDONS[addon]["currency"],
        "status": "active",
        "start_at": start.isoformat() + "Z",
        "end_at": end.isoformat() + "Z",
        "recurring": bool(recurring),
        "renewal_period": period if recurring else "",
        "recurring_stopped_at": "",
        "assigned_user_ids": [],
        "purchased_by_user_id": str(purchased_by_user_id),
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }

    subs = _load()
    subs.append(sub)
    _save(subs)
    return True, "Purchased.", sub


def admin_update_subscription(sub_id: str, fields: Dict[str, Any]) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    subs = _load()
    sub = next((s for s in subs if str(s.get("id")) == str(sub_id)), None)
    if not sub:
        return False, "Subscription not found.", None

    # allow: status, end_at, assigned_user_ids
    if "status" in fields:
        sub["status"] = str(fields.get("status") or "").strip().lower() or sub.get("status")
    if "end_at" in fields:
        sub["end_at"] = str(fields.get("end_at") or "").strip() or sub.get("end_at")
    if "assigned_user_ids" in fields and isinstance(fields.get("assigned_user_ids"), list):
        sub["assigned_user_ids"] = [str(x) for x in fields.get("assigned_user_ids")]
    if "recurring" in fields:
        sub["recurring"] = bool(fields.get("recurring"))
    if "renewal_period" in fields:
        sub["renewal_period"] = str(fields.get("renewal_period") or "").strip()
    if "recurring_stopped_at" in fields:
        sub["recurring_stopped_at"] = str(fields.get("recurring_stopped_at") or "").strip()

    sub["updated_at"] = now_iso()
    _save(subs)
    return True, "Updated.", sub


def admin_delete_subscription(sub_id: str) -> bool:
    subs = _load()
    before = len(subs)
    subs = [s for s in subs if str(s.get("id")) != str(sub_id)]
    _save(subs)
    return len(subs) != before


def list_all_subscriptions() -> List[Dict[str, Any]]:
    subs = _load()
    subs.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return subs
