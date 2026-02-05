import os
import sys
import json
import time
import uuid
import re
import secrets
import string
from urllib.parse import quote
from datetime import date, timedelta, datetime
from pathlib import Path

import requests

# Ensure repo packages (shared features) are importable.
APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
DATA_DIR = ROOT_DIR / "data"
APP_PRODUCT = os.getenv("APP_PRODUCT", "all").strip().lower()

from packages.features.passenger_db.passenger_db import (
    add_history_event,
    attach_booking_to_passengers,
    attach_esim_to_passenger,
    create_member,
    create_profile,
    find_member_by_passport,
    load_profiles,
    save_profiles,
    upsert_member_passport,
)
from packages.features.subscriptions.subscriptions import ADDONS, list_active_addons_for_user
from packages.features.esim.orders import (
    record_order as record_esim_order,
    update_order_by_reference as update_esim_order,
    list_orders_for_owner as list_esim_orders_for_owner,
    list_orders_for_agent as list_esim_orders_for_agent,
)
from packages.features.passenger_db import router as passenger_db_router
from packages.features.subscriptions import router as subscriptions_router
from fastapi import FastAPI, Request, Form, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.templating import Jinja2Templates

BUILD_ID = "fastapi-searchwindow-v5"

BACKEND_BASE_URL = os.getenv("AVAILABILITY_BACKEND_URL", "").strip()
BACKEND_URLS_ENV = os.getenv("AVAILABILITY_BACKEND_URLS", "").strip()


def _backend_candidates() -> list[str]:
    urls = []
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
    out = []
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
    cache = getattr(app.state, "backend_base_cache", None)
    now = time.time()
    if isinstance(cache, dict):
        if cache.get("url") and (now - float(cache.get("ts") or 0)) < 30:
            return cache["url"]

    for base in _backend_candidates():
        if _probe_backend(base):
            app.state.backend_base_cache = {"url": base, "ts": now}
            return base

    fallback = _backend_candidates()[0]
    app.state.backend_base_cache = {"url": fallback, "ts": now}
    return fallback


def _backend_url(path: str) -> str:
    base = _resolve_backend_base()
    return base.rstrip("/") + path


def _get_backend_permissions() -> dict:
    cache = getattr(app.state, "permissions_cache", None)
    now = time.time()
    if isinstance(cache, dict):
        if cache.get("data") and (now - float(cache.get("ts") or 0)) < 30:
            return cache["data"]
    try:
        r = requests.get(_backend_url("/api/permissions"), timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                app.state.permissions_cache = {"data": data, "ts": now}
                return data
    except Exception:
        pass
    return {}


def _seat_estimation_enabled() -> bool:
    cfg = _get_backend_permissions()
    providers = cfg.get("providers") if isinstance(cfg, dict) else {}
    if isinstance(providers, dict):
        ota = providers.get("OTA") or providers.get("ota") or {}
        if isinstance(ota, dict):
            return ota.get("seats_estimation_enabled", True) is True
    return True


def _ensure_product_allowed(*allowed: str) -> None:
    if APP_PRODUCT == "all":
        return
    if allowed and APP_PRODUCT in allowed:
        return
    raise HTTPException(status_code=404, detail="Not found")

app = FastAPI(title="The Book for now")
app.state.app_product = APP_PRODUCT


@app.middleware("http")
async def _product_path_guard(request: Request, call_next):
    # Quick guard for admin routes when running product-specific apps.
    if APP_PRODUCT not in ("all", "admin") and request.url.path.startswith("/admin"):
        return Response(content="Not found", status_code=404)
    return await call_next(request)

# Routers
app.include_router(passenger_db_router)
app.include_router(subscriptions_router)

app.mount("/assets", StaticFiles(directory=str(APP_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


# ------------------------------
# Auth helpers + middleware
# ------------------------------

AUTH_ALLOWLIST = {
    "/login",
    "/signup",
    "/forgot-password",
    "/logout",
    "/__build",
    "/favicon.ico",
}


def _get_current_user(request: Request) -> dict | None:
    user_id = None
    try:
        user_id = request.session.get("user_id")
    except Exception:
        user_id = None
    if not user_id:
        return None
    users = _load_users()
    u = _find_user(users, str(user_id))
    if not u:
        try:
            request.session.pop("user_id", None)
        except Exception:
            pass
        return None
    return u


def _is_super_admin(u: dict | None) -> bool:
    if not isinstance(u, dict):
        return False
    return (u.get("role") or "").strip().lower() == "super_admin"


def _is_sub_user(u: dict | None) -> bool:
    """Employee/sub-user account that belongs to a company admin."""
    if not isinstance(u, dict):
        return False
    # Backward compatible: either explicit role or a company_admin_id link.
    if str(u.get("role") or "").strip().lower() == "sub_user":
        return True
    return bool(u.get("company_admin_id"))


def _is_company_admin(u: dict | None) -> bool:
    """Company admin: the main account for a company (not super admin, not sub user)."""
    if not isinstance(u, dict):
        return False
    if _is_super_admin(u):
        return False
    if _is_sub_user(u):
        return False
    # Default non-super users are company admins.
    return True


def _get_company_admin_for_user(users: list[dict], u: dict | None) -> dict | None:
    if not isinstance(u, dict):
        return None
    if _is_sub_user(u):
        admin_id = u.get("company_admin_id")
        if admin_id:
            return _find_user(users, str(admin_id))
    return u


def _get_billing_user(request: Request) -> dict | None:
    """Return the account whose wallet (cash/credit) should be used."""
    cu = _get_current_user(request)
    if not cu:
        return None
    users = _load_users()
    return _get_company_admin_for_user(users, cu)


def _get_billing_user_for_request(request: Request, cu: dict | None = None) -> dict | None:
    """Compatible helper used by routers: accepts request + optional current user."""
    if cu is None:
        cu = _get_current_user(request)
    if not cu:
        return None
    users = _load_users()
    return _get_company_admin_for_user(users, cu)


def _extract_provider_id(payload: dict | None) -> str:
    """Best-effort provider/OTA identifier extraction from backend result/booking payloads.

    Returns a **lowercased** provider id when detected, otherwise "".
    """
    if not isinstance(payload, dict):
        return ""

    # Common direct keys (string/int) or dict values that contain an id/code.
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
        # string/int -> normalize
        if isinstance(v, (str, int)):
            s = str(v).strip()
            return s.lower() if s else ""
        # dict -> try common id/code fields
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

    # Some backends tuck this under ticketing / pricing / meta / selection, etc.
    for k in ("ticketing", "pricing", "selection", "meta", "result", "offer", "source_info", "provider"):
        sub = payload.get(k)
        if isinstance(sub, dict):
            pid = _extract_provider_id(sub)
            if pid:
                return pid

    # As a last resort, scan one level deep for any dict value that looks like a provider descriptor.
    for v in payload.values():
        if isinstance(v, dict):
            pid = _extract_provider_id(v)
            if pid:
                return pid

    return ""


def _allowed_provider_ids_for_request(request: Request) -> list[str] | None:
    """None means no restriction (super admin). Empty list means no providers allowed."""
    cu = _get_current_user(request)
    if not cu:
        return []
    if _is_super_admin(cu):
        return None
    users = _load_users()
    billing = _get_company_admin_for_user(users, cu) or cu
    apis = billing.get("apis")
    if not isinstance(apis, list):
        apis = []
    apis = [str(x).strip().lower() for x in apis if x is not None and str(x).strip() != ""]
    return apis


def _normalize_api_ids(v) -> list[str]:
    """Normalize a user/company API list to lowercase provider ids."""
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v:
        if x is None:
            continue
        s = str(x).strip().lower()
        if not s:
            continue
        out.append(s)
    # stable unique while preserving order
    seen = set()
    uniq: list[str] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def _expand_provider_ids_for_backend(ids: list[str] | None) -> list[str] | None:
    """Expand provider IDs so case-sensitive backends still match.

    Internally we normalize provider ids to lowercase. Some backends may treat
    provider codes as case-sensitive (e.g., keys like "OTA"). To be defensive,
    we pass multiple casings so upstream filtering doesn't accidentally drop all
    results.
    """
    if ids is None:
        return None
    if not isinstance(ids, list):
        return []
    out: list[str] = []
    for x in ids:
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        out.append(s)
        out.append(s.lower())
        out.append(s.upper())
    # stable unique
    seen = set()
    uniq: list[str] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq



def _ensure_ticketing_vendor(payload: dict) -> None:
    """Ensure a usable TicketingVendor is present in the booking payload.

    The backend /api/book requires **TicketingVendor**. The UI may send booking data with:
      - different key casing (ticketingVendor / ticketing_vendor / provider / supplier / ota),
      - itinerary objects as dicts or JSON strings,
      - vendor hints nested in selection/offer/itinerary structures.

    This function **only fills missing vendor fields** (non-destructive).
    """
    if not isinstance(payload, dict):
        return

    # Keys that may contain the vendor value (prefer these in order).
    VENDOR_KEYS = (
        "TicketingVendor",
        "ticketingVendor",
        "ticketing_vendor",
        "vendor",
        "Vendor",
        "ticketingVendorName",
        "ticketing_vendor_name",
    )

    # Keys that may contain a provider hint that can be used as vendor.
    PROVIDER_KEYS = (
        "provider",
        "Provider",
        "providerCode",
        "provider_code",
        "provider_id",
        "providerId",
        "ota",
        "otaId",
        "ota_id",
        "supplier",
        "Supplier",
        "supplierCode",
        "supplier_code",
        "source",
        "channel",
        "gds",
        "api",
        "apiId",
        "api_id",
    )

    ITIN_KEYS = (
        # snake_case
        "outbound_itinerary_json",
        "return_itinerary_json",
        "outbound_itinerary",
        "return_itinerary",
        # camelCase
        "outboundItineraryJson",
        "returnItineraryJson",
        "outboundItinerary",
        "returnItinerary",
        # selection style
        "selectedOutbound",
        "selectedReturn",
        "selected_outbound",
        "selected_return",
        # generic
        "itinerary",
        "itinerary_json",
        "offer",
        "booking",
        "ticketing",
    )

    def _to_str(v):
        if v is None:
            return ""
        if isinstance(v, (str, int, float)):
            return str(v).strip()
        return ""

    def _pick_vendor_value(v: str) -> str:
        """Choose a vendor value that is likely to exist on the backend."""
        s = (v or "").strip()
        if not s:
            return ""

        # Normalize common vendor codes to the canonical backend vendor name.
        sl = s.lower().replace("_", "").replace("-", "").replace(" ", "")
        if sl in ("ota", "connectota", "connectotatravel", "connectotab2b"):
            return "ConnectOTA"

        return s

    def _find_first(obj, keys, depth=0) -> str:
        if depth > 7:
            return ""
        if isinstance(obj, dict):
            for k in keys:
                if k in obj:
                    vv = obj.get(k)
                    # Sometimes provider is a dict with a code/name
                    if isinstance(vv, dict):
                        for kk in ("code", "name", "id", "providerCode", "provider_code", "ota", "api"):
                            s = _to_str(vv.get(kk))
                            if s:
                                return s
                    s = _to_str(vv)
                    if s:
                        return s
            # Recurse
            for vv in obj.values():
                found = _find_first(vv, keys, depth + 1)
                if found:
                    return found
        elif isinstance(obj, list):
            for vv in obj:
                found = _find_first(vv, keys, depth + 1)
                if found:
                    return found
        return ""

    def _inject_vendor_into_dict(d: dict, vendor_value: str) -> None:
        if not isinstance(d, dict):
            return
        # Do not overwrite non-empty.
        if not _to_str(d.get("TicketingVendor")):
            d["TicketingVendor"] = vendor_value
        if not _to_str(d.get("ticketingVendor")):
            d["ticketingVendor"] = vendor_value
        if not _to_str(d.get("ticketing_vendor")):
            d["ticketing_vendor"] = vendor_value

        # If there's a nested ticketing block, keep it aligned.
        t = d.get("ticketing")
        if isinstance(t, dict):
            if not _to_str(t.get("TicketingVendor")):
                t["TicketingVendor"] = vendor_value
            if not _to_str(t.get("ticketingVendor")):
                t["ticketingVendor"] = vendor_value
            if not _to_str(t.get("ticketing_vendor")):
                t["ticketing_vendor"] = vendor_value

    # 1) Find existing vendor first (preserve original casing/value).
    vendor = _find_first(payload, VENDOR_KEYS)

    # 2) If missing, try provider hints (preserve casing, then map common "ota" to ConnectOTA).
    if not vendor:
        vendor = _find_first(payload, PROVIDER_KEYS)
    vendor = _pick_vendor_value(vendor)

    # 3) If still missing, look inside itinerary-like keys (dict or JSON string).
    def _handle_itin_value(val):
        nonlocal vendor
        if isinstance(val, dict):
            if not vendor:
                vendor = _pick_vendor_value(_find_first(val, VENDOR_KEYS) or _find_first(val, PROVIDER_KEYS))
            if vendor:
                _inject_vendor_into_dict(val, vendor)
            return val

        if isinstance(val, str) and val.strip():
            try:
                parsed = json.loads(val)
            except Exception:
                return None
            if isinstance(parsed, dict):
                if not vendor:
                    vendor = _pick_vendor_value(_find_first(parsed, VENDOR_KEYS) or _find_first(parsed, PROVIDER_KEYS))
                if vendor:
                    _inject_vendor_into_dict(parsed, vendor)
                return json.dumps(parsed, ensure_ascii=False)
        return None

    for k in ITIN_KEYS:
        if k not in payload:
            continue
        new_val = _handle_itin_value(payload.get(k))
        if new_val is not None:
            payload[k] = new_val

    # 4) Fallback: infer from extracted provider id (lowercased) and map.
    if not vendor:
        pid = _extract_provider_id(payload)
        vendor = _pick_vendor_value(_to_str(pid))

    # 5) Final fallback.
    if not vendor:
        vendor = "ConnectOTA"

    # 6) Ensure top-level + nested ticketing fields.
    _inject_vendor_into_dict(payload, vendor)


class _AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Static assets are always allowed.
        if path.startswith("/assets/") or path.startswith("/assets"):
            return await call_next(request)

        if path in AUTH_ALLOWLIST:
            return await call_next(request)

        u = _get_current_user(request)
        if not u:
            nxt = quote(path)
            return RedirectResponse(url=f"/login?next={nxt}", status_code=303)

        # Super admin-only routes.
        if (path.startswith("/admin") or path.startswith("/permissions") or path.startswith("/pending")) and not _is_super_admin(u):
            return RedirectResponse(url="/", status_code=303)

        # Company admin routes (also allowed for super admin).
        if path.startswith("/company-admin") and not (_is_super_admin(u) or _is_company_admin(u)):
            return RedirectResponse(url="/", status_code=303)

        return await call_next(request)


app.add_middleware(_AuthMiddleware)

# Session cookies (used for login) - MUST be added after auth middleware so sessions are available there
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("APP_SESSION_SECRET", "change-me-in-prod"),
    same_site="lax",
    https_only=False,
)


def _default_search():
    today = date.today()
    return {
        "from_code": "",
        "to_code": "",
        "trip_type": "oneway",
        "date": (today + timedelta(days=2)).isoformat(),
        "return_date": (today + timedelta(days=9)).isoformat(),
        "cabin": "economy",
        "adults": 1,
        "children": 0,
        "infants": 0,
    }


def _normalize_form(data: dict):
    d = _default_search()
    d["from_code"] = (data.get("from_code") or d["from_code"]).strip().upper()
    d["to_code"] = (data.get("to_code") or d["to_code"]).strip().upper()
    d["trip_type"] = (data.get("trip_type") or d["trip_type"]).strip().lower() or "oneway"
    d["date"] = (data.get("date") or d["date"]).strip()
    d["return_date"] = (data.get("return_date") or d["return_date"]).strip()
    d["cabin"] = (data.get("cabin") or d["cabin"]).strip().lower() or "economy"
    if d["cabin"] not in ("economy", "business"):
        d["cabin"] = "economy"

    def _int(name: str, default: int) -> int:
        try:
            return int(data.get(name) if data.get(name) is not None else default)
        except Exception:
            return default

    d["adults"] = _int("adults", 1)
    d["children"] = _int("children", 0)
    d["infants"] = _int("infants", 0)
    return d


def _pretty_date(value: str) -> str:
    """Format an ISO date (YYYY-MM-DD) to DD-Mon-YYYY (e.g., 14-Feb-2026)."""
    try:
        d = date.fromisoformat((value or "").strip())
        return d.strftime("%d-%b-%Y")
    except Exception:
        return value or ""


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    # Try a few common formats without being too strict.
    try:
        # Handles: 2026-02-14T11:25:00, 2026-02-14T11:25:00+00:00
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt in (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d-%m-%Y %H:%M",
    ):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            continue
    return None


def _fmt_time(dt: datetime | None, fallback: str | None = None) -> str:
    if not dt:
        return fallback or ""
    # 12-hour time without leading zero, e.g. 11:25 AM
    try:
        return dt.strftime("%-I:%M %p")
    except Exception:
        # Windows-compatible fallback
        return dt.strftime("%I:%M %p").lstrip("0")


def _fmt_duration_mins(total_mins: int) -> str:
    if total_mins <= 0:
        return "0 min"
    h = total_mins // 60
    m = total_mins % 60
    if h and m:
        return f"{h} hr {m} min"
    if h:
        return f"{h} hr"
    return f"{m} min"


def _fmt_iqd(value) -> str:
    """Format money as Iraqi Dinar with thousands separators.

    Examples:
      100000      -> IQD 100,000
      100000.999  -> IQD 100,000.999
    """
    if value is None:
        return "IQD 0"
    s = str(value).strip()
    if not s:
        return "IQD 0"

    # Accept numbers or strings that may already contain commas.
    neg = False
    if s.startswith("-"):
        neg = True
        s = s[1:].strip()

    # Keep all decimals (no rounding), but group integer part.
    if "." in s:
        int_part, dec_part = s.split(".", 1)
    else:
        int_part, dec_part = s, ""
    int_digits = "".join(ch for ch in int_part if ch.isdigit())
    if not int_digits:
        int_digits = "0"
    grouped = f"{int(int_digits):,}"
    out = grouped
    if dec_part is not None and str(dec_part).strip() != "":
        # Preserve only digits in decimals; keep as-is length.
        dec_digits = "".join(ch for ch in str(dec_part) if ch.isdigit())
        if dec_digits != "":
            out = f"{grouped}.{dec_digits}"
    if neg and out != "0":
        out = "-" + out
    return f"IQD {out}"


def _enrich_results(results: list, cabin: str) -> list:
    """Add display-friendly fields for the details view (times + layovers)."""
    if not results:
        return results

    def _has_included_bag(segs: list) -> bool:
        """Return True if any segment indicates a positive baggage allowance.

        The backend can represent baggage as:
        - string ("1pc", "0 pc", "0 kg", "No baggage")
        - number (0/1)
        - dict (e.g., {"pieces": 0} or {"weight": 0, "unit": "KG"})
        """

        def _num(v):
            try:
                return float(v)
            except Exception:
                return None

        for s in segs or []:
            if not isinstance(s, dict):
                continue
            b = s.get("baggage")
            if b is None or b == "":
                continue

            # Numeric baggage
            if isinstance(b, (int, float)):
                if float(b) > 0:
                    return True
                continue

            # Dict/object baggage
            if isinstance(b, dict):
                for k in ("pieces", "piece", "pc", "paxPieces", "quantity", "qty"):
                    if k in b:
                        n = _num(b.get(k))
                        if n is not None and n > 0:
                            return True
                for k in ("weight", "kg", "kilograms", "value", "amount"):
                    if k in b:
                        n = _num(b.get(k))
                        if n is not None and n > 0:
                            return True
                # If dict exists but doesn't carry a positive number, treat as no included bag.
                continue

            # String baggage
            t = str(b).strip().lower()
            if not t:
                continue
            # If the string contains numbers, treat <=0 as no bag, >0 as included.
            nums = re.findall(r"\d+(?:\.\d+)?", t)
            if nums:
                try:
                    vals = [float(n) for n in nums]
                    if all(v <= 0 for v in vals):
                        continue
                    return True
                except Exception:
                    pass
            # Common 'no baggage' patterns
            if t.startswith("0"):
                continue
            if "0pc" in t or "0 pc" in t:
                continue
            if "0kg" in t or "0 kg" in t:
                continue
            if "no" in t and "bag" in t:
                continue
            if "none" in t:
                continue
            if "without" in t and "bag" in t:
                continue
            if "not included" in t and "bag" in t:
                continue

            # Anything else is treated as included baggage.
            return True

        return False

    for r in results:
        segs = (r.get("segments") or []) if isinstance(r, dict) else []
        r["_has_bag"] = _has_included_bag(segs)
        prev_arr_dt = None
        for i, s in enumerate(segs):
            if not isinstance(s, dict):
                continue
            dep_dt = _parse_dt(s.get("dep_dt"))
            arr_dt = _parse_dt(s.get("arr_dt"))

            s["_dep_time"] = _fmt_time(dep_dt, s.get("dep_dt"))
            s["_arr_time"] = _fmt_time(arr_dt, s.get("arr_dt"))

            # Layover shown before this segment (i.e., at the intermediate airport).
            if prev_arr_dt and dep_dt:
                mins = int((dep_dt - prev_arr_dt).total_seconds() // 60)
                if mins > 0:
                    at_code = s.get("dep") or ""
                    s["_layover"] = f"{_fmt_duration_mins(mins)} layover · {at_code}"
            prev_arr_dt = arr_dt or prev_arr_dt

            # Cabin label for display inside details.
            s["_cabin"] = (cabin or "economy").capitalize()

        # Store on the itinerary too (helpful for templates).
        r["_cabin"] = (cabin or "economy").capitalize()
    return results


def _flight_key(result: dict | None) -> str:
    if not isinstance(result, dict):
        return ""
    segs = result.get("segments") or []
    parts = []
    for s in segs:
        if not isinstance(s, dict):
            continue
        parts.append(
            "|".join(
                [
                    str(s.get("airline") or ""),
                    str(s.get("flight") or ""),
                    str(s.get("dep") or ""),
                    str(s.get("arr") or ""),
                    str(s.get("dep_dt") or ""),
                    str(s.get("arr_dt") or ""),
                ]
            )
        )
    return "||".join(parts)


def _filter_by_allowed_providers(results: list, allowed_providers: list | None) -> list:
    if allowed_providers is None or not isinstance(allowed_providers, list):
        return results

    def _res_pid(res):
        if isinstance(res, dict):
            return _extract_provider_id(res)
        return ""

    return [r for r in results if (_res_pid(r) == "" or _res_pid(r) in allowed_providers)]


def _estimate_seats_for_results(
    payload: dict,
    outbound: list,
    inbound: list,
    allowed_providers: list | None,
) -> None:
    """Estimate seats by probing higher pax counts (Iraqi Airways workaround)."""
    try:
        pax = payload.get("pax") or {}
        base_adults = int(pax.get("adults") or 1)
        base_children = int(pax.get("children") or 0)
        base_infants = int(pax.get("infants") or 0)
        base_total = max(1, base_adults + base_children + base_infants)

        max_check = 8
        if base_total >= max_check:
            seats = min(9, base_total)
            for r in outbound:
                r["_seats_available"] = seats
            for r in inbound:
                r["_seats_available"] = seats
            return

        out_keys = {_flight_key(r) for r in outbound}
        in_keys = {_flight_key(r) for r in inbound}

        seats_out = {k: None for k in out_keys if k}
        seats_in = {k: None for k in in_keys if k}

        for pax_total in range(base_total + 1, max_check + 1):
            extra_payload = json.loads(json.dumps(payload))
            extra_pax = extra_payload.get("pax") or {}
            extra_pax["adults"] = max(1, base_adults + (pax_total - base_total))
            extra_pax["children"] = base_children
            extra_pax["infants"] = base_infants
            extra_payload["pax"] = extra_pax

            r = requests.post(_backend_url("/api/availability"), json=extra_payload, timeout=60)
            r.raise_for_status()
            data = r.json()

            res_out = _filter_by_allowed_providers(data.get("results") or [], allowed_providers)
            res_in = _filter_by_allowed_providers(data.get("results_return") or [], allowed_providers)
            out_set = {_flight_key(x) for x in res_out if _flight_key(x)}
            in_set = {_flight_key(x) for x in res_in if _flight_key(x)}

            for k in seats_out:
                if seats_out[k] is None and k not in out_set:
                    seats_out[k] = pax_total - 1
            for k in seats_in:
                if seats_in[k] is None and k not in in_set:
                    seats_in[k] = pax_total - 1

        for k in seats_out:
            if seats_out[k] is None:
                seats_out[k] = 9
        for k in seats_in:
            if seats_in[k] is None:
                seats_in[k] = 9

        for r in outbound:
            k = _flight_key(r)
            if k and k in seats_out:
                r["_seats_available"] = seats_out[k]
        for r in inbound:
            k = _flight_key(r)
            if k and k in seats_in:
                r["_seats_available"] = seats_in[k]
    except Exception:
        return


def _estimate_seats_for_keys(
    payload: dict,
    keys_out: list,
    keys_in: list,
    allowed_providers: list | None,
) -> tuple[dict, dict]:
    seats_out = {str(k): None for k in (keys_out or []) if str(k)}
    seats_in = {str(k): None for k in (keys_in or []) if str(k)}
    if not seats_out and not seats_in:
        return {}, {}

    try:
        pax = payload.get("pax") or {}
        base_adults = int(pax.get("adults") or 1)
        base_children = int(pax.get("children") or 0)
        base_infants = int(pax.get("infants") or 0)
        base_total = max(1, base_adults + base_children + base_infants)

        max_check = 8
        if base_total >= max_check:
            seats = min(9, base_total)
            return (
                {k: seats for k in seats_out},
                {k: seats for k in seats_in},
            )

        for pax_total in range(base_total + 1, max_check + 1):
            extra_payload = json.loads(json.dumps(payload))
            extra_pax = extra_payload.get("pax") or {}
            extra_pax["adults"] = max(1, base_adults + (pax_total - base_total))
            extra_pax["children"] = base_children
            extra_pax["infants"] = base_infants
            extra_payload["pax"] = extra_pax

            r = requests.post(_backend_url("/api/availability"), json=extra_payload, timeout=60)
            r.raise_for_status()
            data = r.json()

            res_out = _filter_by_allowed_providers(data.get("results") or [], allowed_providers)
            res_in = _filter_by_allowed_providers(data.get("results_return") or [], allowed_providers)
            out_set = {_flight_key(x) for x in res_out if _flight_key(x)}
            in_set = {_flight_key(x) for x in res_in if _flight_key(x)}

            for k in seats_out:
                if seats_out[k] is None and k not in out_set:
                    seats_out[k] = pax_total - 1
            for k in seats_in:
                if seats_in[k] is None and k not in in_set:
                    seats_in[k] = pax_total - 1

        for k in seats_out:
            if seats_out[k] is None:
                seats_out[k] = 9
        for k in seats_in:
            if seats_in[k] is None:
                seats_in[k] = 9
    except Exception:
        return {}, {}

    return seats_out, seats_in


def _render(request: Request, template_name: str, context: dict | None = None):
    cu = _get_current_user(request)
    users = _load_users() if cu else []
    billing_user = _get_company_admin_for_user(users, cu) if cu else None

    is_super_admin = _is_super_admin(cu)
    is_company_admin = _is_company_admin(cu)
    is_sub_user = _is_sub_user(cu)

    # Which account's balances should be shown/used (company admin for sub users).
    balances_user = billing_user or cu

    active_addons = (
        list_active_addons_for_user(
            str(cu.get("id")),
            owner_user_id=str((billing_user or cu).get("id")),
        )
        if cu
        else []
    )
    if is_super_admin:
        # Super admins have unrestricted access to all add-ons.
        active_addons = list(ADDONS.keys())

    ctx = {
        "request": request,
        "title": "The Book for now",
        "defaults": _default_search(),
        "build_id": BUILD_ID,
        "app_product": APP_PRODUCT,
        "current_user": cu,
        "billing_user": billing_user,
        "balances_user": balances_user,
        "is_super_admin": is_super_admin,
        "is_company_admin": is_company_admin,
        "is_sub_user": is_sub_user,
        "show_pending": bool(is_super_admin),
        "show_admin": bool(is_super_admin or is_company_admin),
        "fmt_iqd": _fmt_iqd,
        "active_addons": active_addons,
        "has_passenger_db": bool("passenger_database" in active_addons),
        "has_visa_vendor": bool("visa_vendor" in active_addons),
        "has_esim": bool("esim" in active_addons),
        "active_announcements": _active_announcements(),
    }

    # Admin button destination:
    # - super admin -> /admin
    # - company admin -> /company-admin
    try:
        if is_super_admin:
            ctx["admin_url"] = request.url_for("admin")
        elif is_company_admin:
            ctx["admin_url"] = request.url_for("company_admin")
    except Exception:
        ctx["admin_url"] = "/"

    if context:
        ctx.update(context)
    return templates.TemplateResponse(template_name, ctx)


# ---- Local users storage (data/users.json) ----
def _users_path() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return str(DATA_DIR / "users.json")


def _load_users() -> list[dict]:
    path = _users_path()
    if not os.path.exists(path):
        users: list[dict] = []
        users = _ensure_super_admin(users)
        return users
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            users = payload
        elif isinstance(payload, dict):
            users = payload.get("users") or []
        else:
            users = []
        if not isinstance(users, list):
            return []
        # Normalize minimal fields
        changed = False
        for u in users:
            if isinstance(u, dict) and not u.get("id"):
                u["id"] = uuid.uuid4().hex
                changed = True
            if isinstance(u, dict) and "active" not in u:
                u["active"] = True
                changed = True
            if isinstance(u, dict) and "apis" not in u:
                u["apis"] = []
                changed = True
            if isinstance(u, dict) and "apis" in u:
                # Normalize provider ids (case-insensitive input in older records)
                norm = _normalize_api_ids(u.get("apis"))
                if norm != (u.get("apis") if isinstance(u.get("apis"), list) else []):
                    u["apis"] = norm
                    changed = True
            if isinstance(u, dict) and "credit" not in u:
                u["credit"] = 0
                changed = True
            if isinstance(u, dict) and "cash" not in u:
                u["cash"] = 0
                changed = True
            if isinstance(u, dict) and "preferred_payment" not in u:
                u["preferred_payment"] = "cash"
                changed = True
            if isinstance(u, dict) and "email" not in u:
                u["email"] = ""
                changed = True
            if isinstance(u, dict) and "phone" not in u:
                u["phone"] = ""
                changed = True
            if isinstance(u, dict) and "contact" not in u:
                # User asked for a contact field at signup; keep it alongside phone.
                u["contact"] = u.get("phone") or ""
                changed = True
            if isinstance(u, dict) and "role" not in u:
                u["role"] = "user"
                changed = True
            if isinstance(u, dict) and "commission" not in u:
                u["commission"] = []
                changed = True
            if isinstance(u, dict) and "commission" in u and not isinstance(u.get("commission"), list):
                u["commission"] = []
                changed = True
            if isinstance(u, dict) and "markup" not in u:
                u["markup"] = []
                changed = True
            if isinstance(u, dict) and "markup" in u and not isinstance(u.get("markup"), list):
                u["markup"] = []
                changed = True
            if isinstance(u, dict) and "vendor_services" not in u:
                u["vendor_services"] = []
                changed = True
            if isinstance(u, dict) and "vendor_services" in u and not isinstance(u.get("vendor_services"), list):
                u["vendor_services"] = []
                changed = True
            if isinstance(u, dict) and "vendor_visa_prices" not in u:
                u["vendor_visa_prices"] = []
                changed = True
            if isinstance(u, dict) and "vendor_visa_prices" in u and not isinstance(u.get("vendor_visa_prices"), list):
                u["vendor_visa_prices"] = []
                changed = True
            if isinstance(u, dict) and "vendor_name" not in u:
                u["vendor_name"] = u.get("company_name") or u.get("username") or ""
                changed = True
        if changed:
            _save_users(users)
        # Ensure the required super admin exists.
        users = _ensure_super_admin(users)
        return users
    except Exception:
        users = []
        users = _ensure_super_admin(users)
        return users


def _save_users(users: list[dict]) -> None:
    path = _users_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"users": users}, f, ensure_ascii=False, indent=2)
    except Exception:
        # If saving fails, we silently ignore to avoid breaking the app UI.
        pass


# ---- Announcements storage (data/announcements.json) ----
def _announcements_path() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return str(DATA_DIR / "announcements.json")


def _load_announcements() -> list[dict]:
    path = _announcements_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            out = payload.get("announcements") or []
            return out if isinstance(out, list) else []
    except Exception:
        return []
    return []


def _save_announcements(items: list[dict]) -> None:
    path = _announcements_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"announcements": items}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _parse_announcement_dt(date_str: str, time_str: str, default_time: str | None = None) -> datetime | None:
    try:
        d = (date_str or "").strip()
        if not d:
            return None
        t = (time_str or "").strip() or (default_time or "")
        if not t:
            t = "00:00"
        return datetime.fromisoformat(f"{d}T{t}")
    except Exception:
        return None


def _announcement_status(a: dict, now: datetime | None = None) -> str:
    now = now or datetime.now()
    if not isinstance(a, dict):
        return "inactive"
    if not a.get("active", False):
        return "inactive"
    start_dt = _parse_announcement_dt(a.get("start_date") or "", a.get("start_time") or "00:00", "00:00")
    end_dt = None
    if a.get("end_date"):
        end_dt = _parse_announcement_dt(a.get("end_date") or "", a.get("end_time") or "23:59", "23:59")
    if start_dt and now < start_dt:
        return "upcoming"
    if end_dt and now > end_dt:
        return "expired"
    return "active"


def _active_announcements(now: datetime | None = None) -> list[dict]:
    now = now or datetime.now()
    items = _load_announcements()
    out = []
    for a in items:
        if _announcement_status(a, now) == "active":
            out.append(a)
    out.sort(key=lambda x: (x.get("start_date") or "", x.get("start_time") or ""), reverse=True)
    return out


def _find_user(users: list[dict], user_id: str) -> dict | None:
    for u in users:
        if isinstance(u, dict) and str(u.get("id")) == str(user_id):
            return u
    return None



# ------------------------------
# Pending requests store (super admin only view)
# ------------------------------

def _pending_path() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return str(DATA_DIR / "pending.json")


def _load_pending() -> list[dict]:
    path = _pending_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            items = payload.get("pending") or payload.get("items") or []
            return items if isinstance(items, list) else []
        return []
    except Exception:
        return []


def _save_pending(items: list[dict]) -> None:
    path = _pending_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"pending": items}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _add_pending(kind: str, requested_by_user_id: str, company_admin_id: str, provider_id: str, payload: dict | None, reason: str) -> str:
    items = _load_pending()
    pid = uuid.uuid4().hex
    items.insert(
        0,
        {
            "id": pid,
            "kind": kind,
            "provider_id": provider_id,
            "requested_by_user_id": requested_by_user_id,
            "company_admin_id": company_admin_id,
            "reason": reason,
            "created_at": datetime.utcnow().isoformat() + "Z",
            # Do NOT store the full payload in pending (user request). Keep only minimal metadata.
            "payload": {},
        },
    )
    _save_pending(items)
    return pid


# ------------------------------
# Transactions store
# ------------------------------

def _transactions_path() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return str(DATA_DIR / "transactions.json")


def _load_transactions() -> list[dict]:
    path = _transactions_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            items = payload.get("transactions") or payload.get("items") or []
            if isinstance(items, list):
                return [x for x in items if isinstance(x, dict)]
        return []
    except Exception:
        return []


def _save_transactions(items: list[dict]) -> None:
    path = _transactions_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"transactions": items}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ------------------------------
# Visa applications store
# ------------------------------

def _visa_path() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return str(DATA_DIR / "visas.json")


def _load_visas() -> list[dict]:
    path = _visa_path()
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            items = payload.get("visas") or payload.get("items") or []
            if isinstance(items, list):
                return [x for x in items if isinstance(x, dict)]
        return []
    except Exception:
        return []


def _save_visas(items: list[dict]) -> None:
    path = _visa_path()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"visas": items}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _normalize_visa_status(v: str) -> str:
    s = str(v or "").strip().lower()
    if s in ("approved", "rejected"):
        return s
    return "pending"


def _link_visa_passenger(owner_user_id: str, passport_number: str) -> dict:
    passport_number = (passport_number or "").strip()
    if not passport_number:
        return {"profile_id": "", "member_id": "", "passenger_name": ""}

    profiles = load_profiles()
    found = find_member_by_passport(profiles, passport_number, owner_user_id=owner_user_id)
    if found:
        prof, mem = found
    else:
        prof = create_profile(owner_user_id=owner_user_id, label=f"Visa - {passport_number}", phone="")
        mem = create_member(
            {
                "first_name": "",
                "last_name": "",
                "dob": "",
                "nationality": "",
                "national_id_number": "",
                "phone": "",
                "passports": [
                    {
                        "number": passport_number,
                        "issue_date": "",
                        "expiry_date": "",
                        "issue_place": "",
                    }
                ],
            }
        )
        prof.setdefault("members", []).append(mem)
        profiles.append(prof)

    upsert_member_passport(
        mem,
        {
            "number": passport_number,
            "issue_date": "",
            "expiry_date": "",
            "issue_place": "",
        },
    )

    prof["updated_at"] = datetime.utcnow().isoformat() + "Z"
    save_profiles(profiles)

    first = (mem.get("first_name") or "").strip()
    last = (mem.get("last_name") or "").strip()
    name = (first + (" " if (first and last) else "") + last).strip()

    return {
        "profile_id": str(prof.get("id") or ""),
        "member_id": str(mem.get("id") or ""),
        "passenger_name": name,
    }


def _list_visa_vendors(users: list[dict]) -> list[dict]:
    out = []
    for u in users:
        if not isinstance(u, dict):
            continue
        if _is_sub_user(u):
            continue
        if not u.get("active", True):
            continue
        if not _is_company_admin(u):
            continue
        # Visa vendor is now controlled by the add-on subscription.
        active = list_active_addons_for_user(str(u.get("id")), owner_user_id=str(u.get("id")))
        if "visa_vendor" not in active:
            continue
        out.append(u)
    return out


def _has_active_addon(request: Request, cu: dict | None, addon: str) -> bool:
    if not cu:
        return False
    addon = str(addon or "").strip()
    users = _load_users()
    billing = _get_company_admin_for_user(users, cu) or cu
    owner_id = str(billing.get("id") or "")
    active = list_active_addons_for_user(str(cu.get("id")), owner_user_id=owner_id)
    if _is_super_admin(cu):
        active = list(ADDONS.keys())
    return addon in active


def _visa_upload_dir(visa_id: str) -> Path:
    base = Path(__file__).resolve().parent / "static" / "uploads" / "visa" / str(visa_id)
    base.mkdir(parents=True, exist_ok=True)
    return base


def _sanitize_ext(name: str) -> str:
    ext = os.path.splitext(name or "")[1].lower().strip()
    if ext and len(ext) <= 10:
        return ext
    return ""


async def _save_visa_attachments(visa_id: str, files: list[UploadFile], kind: str) -> list[dict]:
    saved = []
    if not files:
        return saved
    target_dir = _visa_upload_dir(visa_id)
    for f in files:
        try:
            original = str(f.filename or "").strip()
            ext = _sanitize_ext(original)
            fname = f"{uuid.uuid4().hex}{ext}"
            out_path = target_dir / fname
            content = await f.read()
            out_path.write_bytes(content)
            saved.append(
                {
                    "type": kind,
                    "name": original or fname,
                    "url": f"/assets/uploads/visa/{visa_id}/{fname}",
                    "uploaded_at": datetime.utcnow().isoformat() + "Z",
                }
            )
        except Exception:
            continue
    return saved

def _add_transaction(tx: dict) -> str:
    items = _load_transactions()
    tid = uuid.uuid4().hex
    tx = tx if isinstance(tx, dict) else {}
    tx.setdefault("id", tid)
    tx.setdefault("ts", datetime.utcnow().isoformat() + "Z")
    items.insert(0, tx)
    _save_transactions(items)
    return tx.get("id") or tid


def _update_transaction_by_pending_id(pending_id: str, updates: dict) -> bool:
    if not pending_id:
        return False
    items = _load_transactions()
    changed = False
    for t in items:
        if not isinstance(t, dict):
            continue
        if str(t.get("pending_id") or "") == str(pending_id):
            for k, v in (updates or {}).items():
                t[k] = v
            # also mirror into details
            det = t.get("details")
            if isinstance(det, dict):
                for k, v in (updates or {}).items():
                    det[k] = v
            changed = True
            break
    if changed:
        _save_transactions(items)
    return changed


def _safe_json_loads(v):
    if not isinstance(v, str):
        return None
    s = v.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def _extract_first_passenger(payload: dict) -> tuple[str, str]:
    if not isinstance(payload, dict):
        return "", ""
    p0 = None
    ps = payload.get("passengers")
    if isinstance(ps, list) and ps:
        p0 = ps[0]
    if isinstance(p0, dict):
        return (str(p0.get("first_name") or "").strip(), str(p0.get("last_name") or "").strip())
    return "", ""


def _extract_segments_from_any(obj) -> list[dict]:
    """Return a list of segment-like dicts from multiple possible itinerary shapes."""
    if isinstance(obj, dict):
        # normalized
        segs = obj.get("segments") or obj.get("Segments") or obj.get("flight_segments") or obj.get("flightSegments") or obj.get("legs")
        if isinstance(segs, list) and segs and isinstance(segs[0], dict):
            return segs
        # nested selection/offer style
        if isinstance(obj.get("itinerary"), dict):
            segs2 = obj["itinerary"].get("segments") or obj["itinerary"].get("Segments")
            if isinstance(segs2, list) and segs2 and isinstance(segs2[0], dict):
                return segs2
        # wings-like
        air = obj.get("airItinerary") or obj.get("AirItinerary")
        if isinstance(air, dict):
            odo = air.get("originDestinationOptions") or air.get("OriginDestinationOptions")
            if isinstance(odo, dict):
                odopt = odo.get("originDestinationOption") or odo.get("OriginDestinationOption")
                if isinstance(odopt, list) and odopt:
                    fs = odopt[0].get("flightSegment") if isinstance(odopt[0], dict) else None
                    if isinstance(fs, list) and fs and isinstance(fs[0], dict):
                        return fs
    return []


def _extract_airline_from_segment(seg: dict) -> str:
    if not isinstance(seg, dict):
        return ""
    # normalized
    for k in ("airline_name", "airline", "airlineName", "carrier_name", "carrier"):
        v = seg.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # wings-like
    op = seg.get("operatingAirline") or seg.get("OperatingAirline")
    if isinstance(op, dict):
        v = op.get("companyShortName") or op.get("CompanyShortName")
        if isinstance(v, str) and v.strip():
            return v.strip()
    mk = seg.get("marketingAirline") or seg.get("MarketingAirline")
    if isinstance(mk, dict):
        v = mk.get("code") or mk.get("Code")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_from_to_from_segment(seg: dict) -> tuple[str, str]:
    if not isinstance(seg, dict):
        return "", ""
    # normalized
    dep = seg.get("dep") or seg.get("from") or seg.get("origin") or seg.get("departure") or seg.get("departure_code") or seg.get("origin_code")
    arr = seg.get("arr") or seg.get("to") or seg.get("destination") or seg.get("arrival") or seg.get("arrival_code") or seg.get("destination_code")
    if isinstance(dep, str) and isinstance(arr, str) and dep.strip() and arr.strip():
        return dep.strip(), arr.strip()
    # dict forms
    if isinstance(dep, dict) and isinstance(arr, dict):
        dc = dep.get("code") or dep.get("iata") or dep.get("LocationCode") or dep.get("locationCode")
        ac = arr.get("code") or arr.get("iata") or arr.get("LocationCode") or arr.get("locationCode")
        if isinstance(dc, str) and isinstance(ac, str) and dc.strip() and ac.strip():
            return dc.strip(), ac.strip()
    # wings-like
    d = seg.get("departureAirport") or seg.get("DepartureAirport")
    a = seg.get("arrivalAirport") or seg.get("ArrivalAirport")
    if isinstance(d, dict) and isinstance(a, dict):
        dep = d.get("locationCode") or d.get("LocationCode")
        arr = a.get("locationCode") or a.get("LocationCode")
        if isinstance(dep, str) and isinstance(arr, str) and dep.strip() and arr.strip():
            return dep.strip(), arr.strip()
    return "", ""


def _extract_departure_dt(seg: dict) -> str:
    if not isinstance(seg, dict):
        return ""
    for k in ("dep_dt", "DepartureDateTime", "departureDateTime", "departure", "Departure"):
        v = seg.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_price(payload: dict) -> tuple[float | None, str]:
    if not isinstance(payload, dict):
        return None, ""
    # amount
    amt = None
    for key in ("total_amount", "amount", "total", "ticket_total", "price", "total_price"):
        if key in payload:
            try:
                amt = float(_to_number(payload.get(key), 0))
                break
            except Exception:
                pass
    if amt is None:
        sel = payload.get("selection")
        if isinstance(sel, dict):
            for key in ("total_price", "price", "total"):
                if key in sel:
                    try:
                        amt = float(_to_number(sel.get(key), 0))
                        break
                    except Exception:
                        pass
    # currency
    cur = ""
    for key in ("currency", "total_currency", "CurrencyCode"):
        v = payload.get(key)
        if isinstance(v, str) and v.strip():
            cur = v.strip()
            break
    if not cur:
        sel = payload.get("selection")
        if isinstance(sel, dict):
            v = sel.get("currency")
            if isinstance(v, str) and v.strip():
                cur = v.strip()
    return amt, cur


def _build_tx_snapshot(payload: dict) -> dict:
    """Create a minimal transaction snapshot from the booking payload (no raw payload stored)."""
    first_name, last_name = _extract_first_passenger(payload)
    airline = ""
    from_c = ""
    to_c = ""
    depart_dt = ""

    # Prefer selection outbound if present
    sel = payload.get("selection") if isinstance(payload, dict) else None
    if isinstance(sel, dict):
        out = sel.get("selectedOutbound") or sel.get("outbound") or sel.get("selected_outbound")
        if isinstance(out, dict):
            segs = _extract_segments_from_any(out)
            if segs:
                airline = _extract_airline_from_segment(segs[0])
                from_c, to_c = _extract_from_to_from_segment(segs[0])
                depart_dt = _extract_departure_dt(segs[0])

    # Fallback: outbound itinerary JSON/dict
    if not (airline and from_c and to_c):
        out_it = payload.get("outbound_itinerary_json") or payload.get("outboundItineraryJson") or payload.get("outbound_itinerary") or payload.get("outboundItinerary")
        itin = out_it if isinstance(out_it, dict) else _safe_json_loads(out_it) or {}
        if isinstance(itin, dict):
            segs = _extract_segments_from_any(itin)
            if segs:
                airline = airline or _extract_airline_from_segment(segs[0])
                if not (from_c and to_c):
                    from_c, to_c = _extract_from_to_from_segment(segs[0])
                depart_dt = depart_dt or _extract_departure_dt(segs[0])

    amt, cur = _extract_price(payload)

    return {
        "first_name": first_name,
        "last_name": last_name,
        "airline": airline,
        "from": from_c,
        "to": to_c,
        "date": depart_dt,
        "price": amt,
        "currency": cur,
    }

# ------------------------------
# Auth: super admin bootstrap + email sender
# ------------------------------

SUPER_ADMIN_EMAIL = "dler@corevia-consultants.com"
SUPER_ADMIN_PASSWORD = "StrongPass123"


def _ensure_super_admin(users: list[dict]) -> list[dict]:
    """Ensure the required super admin account exists.

    Required credentials (per user request):
    - email: dler@corevia-consultants.com
    - password: StrongPass123
    """
    if not isinstance(users, list):
        users = []

    for u in users:
        if not isinstance(u, dict):
            continue
        if (u.get("email") or "").strip().lower() == SUPER_ADMIN_EMAIL.lower():
            # Ensure role exists.
            if (u.get("role") or "").strip().lower() != "super_admin":
                u["role"] = "super_admin"
            # Do NOT overwrite password if user changed it later.
            return users

    users.append(
        {
            "id": uuid.uuid4().hex,
            "type": "admin",
            "company_name": "Corevia Consultants",
            "username": "dler",
            "email": SUPER_ADMIN_EMAIL,
            "password": SUPER_ADMIN_PASSWORD,
            "phone": "",
            "contact": "",
            "role": "super_admin",
            "active": True,
            "credit": 0,
            "cash": 0,
            "preferred_payment": "cash",
            "apis": [],
            "employees": [],
            "commission": [],
            "markup": [],
        }
    )
    _save_users(users)
    return users


def _generate_password(length: int = 12) -> str:
    """Generate a reasonably strong password."""
    length = max(10, int(length or 12))
    alphabet = string.ascii_letters + string.digits
    symbols = "@#$%&*_-+!"
    # Ensure complexity: 1 upper, 1 lower, 1 digit, 1 symbol
    pw = [
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.digits),
        secrets.choice(symbols),
    ]
    for _ in range(length - len(pw)):
        pw.append(secrets.choice(alphabet + symbols))
    secrets.SystemRandom().shuffle(pw)
    return "".join(pw)


def _send_email_via_curl(to_email: str, subject: str, body: str) -> tuple[bool, str]:
    """Send email via backend notification endpoint (kept for compatibility)."""
    to_email = (to_email or "").strip()
    if not to_email:
        return False, "Missing recipient email"

    try:
        r = requests.post(
            _backend_url("/api/notify/email"),
            json={
                "to_email": to_email,
                "subject": subject,
                "body": body,
            },
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == "ok":
                return True, "sent"
            return False, data.get("error") or "Failed to send email."
        return False, f"Backend error (status {r.status_code})"
    except Exception as e:
        return False, str(e)



def _load_providers() -> list[dict]:
    # Best-effort: reuse backend permissions config if available.
    try:
        r = requests.get(_backend_url("/api/permissions"), timeout=10)
        r.raise_for_status()
        data = r.json()
        providers = data.get("providers")
        out = []
        if isinstance(providers, dict):
            for code, cfg in providers.items():
                name = code
                if isinstance(cfg, dict) and cfg.get("name"):
                    name = str(cfg.get("name"))
                out.append({"id": str(code), "name": str(name)})
            out.sort(key=lambda x: x["id"])
            return out
        if isinstance(providers, list):
            for p in providers:
                if isinstance(p, dict):
                    pid = p.get("id") or p.get("code") or p.get("name")
                    if pid:
                        out.append({"id": str(pid), "name": str(p.get("name") or pid)})
            out.sort(key=lambda x: x["id"])
            return out
    except Exception:
        pass
    # Fallback: empty list
    return []


def _to_number(v, default=0):
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return default


# Expose helpers for feature routers
app.state.render = _render
app.state.get_current_user = _get_current_user
app.state.load_users = _load_users
app.state.save_users = _save_users
app.state.find_user = _find_user
app.state.get_billing_user = _get_billing_user_for_request
app.state.to_number = _to_number
app.state.is_super_admin = _is_super_admin
app.state.is_sub_user = _is_sub_user
app.state.is_company_admin = _is_company_admin
app.state.send_email = _send_email_via_curl


@app.get("/__build", response_class=PlainTextResponse)
def __build():
    return BUILD_ID


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


# ------------------------------
# Auth routes
# ------------------------------

@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request, next: str | None = None, msg: str | None = None, err: str | None = None):
    # If already logged in, go home.
    if _get_current_user(request):
        return RedirectResponse(url="/", status_code=303)
    return _render(
        request,
        "auth/login.html",
        {
            "next": next or "",
            "msg": msg or "",
            "err": err or "",
        },
    )


@app.post("/login")
async def login_post(
    request: Request,
    identifier: str = Form(""),
    password: str = Form(""),
    next: str = Form(""),
):
    identifier = (identifier or "").strip()
    password = (password or "").strip()
    if not identifier or not password:
        return _render(request, "auth/login.html", {"next": next, "err": "Please enter your username/email and password."})

    users = _load_users()
    ident_l = identifier.lower()
    match = None
    for u in users:
        if not isinstance(u, dict):
            continue
        if (u.get("username") or "").strip().lower() == ident_l or (u.get("email") or "").strip().lower() == ident_l:
            match = u
            break

    if not match:
        return _render(request, "auth/login.html", {"next": next, "err": "Invalid credentials."})
    if not match.get("active", True):
        return _render(request, "auth/login.html", {"next": next, "err": "Your account is blocked."})
    if (match.get("password") or "") != password:
        return _render(request, "auth/login.html", {"next": next, "err": "Invalid credentials."})

    # OK
    request.session["user_id"] = str(match.get("id"))
    dest = (next or "").strip() or "/"
    # Never allow open redirects.
    if not dest.startswith("/"):
        dest = "/"
    return RedirectResponse(url=dest, status_code=303)


@app.get("/logout")
def logout(request: Request):
    try:
        request.session.pop("user_id", None)
    except Exception:
        pass
    return RedirectResponse(url="/login?msg=Logged%20out", status_code=303)


@app.get("/signup", response_class=HTMLResponse)
def signup_get(request: Request, msg: str | None = None, err: str | None = None):
    if _get_current_user(request):
        return RedirectResponse(url="/", status_code=303)
    return _render(request, "auth/signup.html", {"msg": msg or "", "err": err or ""})


@app.post("/signup")
async def signup_post(
    request: Request,
    email: str = Form(""),
    username: str = Form(""),
    contact: str = Form(""),
):
    email = (email or "").strip()
    username = (username or "").strip()
    contact = (contact or "").strip()
    if not email or "@" not in email:
        return _render(request, "auth/signup.html", {"err": "Please enter a valid email address."})
    if not username:
        return _render(request, "auth/signup.html", {"err": "Please enter a username."})

    users = _load_users()
    email_l = email.lower()
    user_l = username.lower()
    for u in users:
        if not isinstance(u, dict):
            continue
        if (u.get("email") or "").strip().lower() == email_l:
            return _render(request, "auth/signup.html", {"err": "This email is already registered."})
        if (u.get("username") or "").strip().lower() == user_l:
            return _render(request, "auth/signup.html", {"err": "This username is already taken."})

    new_pw = _generate_password(12)
    new_user = {
        "id": uuid.uuid4().hex,
        "type": "user",
        "company_name": username,
        "username": username,
        "email": email,
        "phone": contact,
        "contact": contact,
        "password": new_pw,
        "role": "user",
        "active": True,
        "credit": 0,
        "cash": 0,
        "preferred_payment": "cash",
        "apis": [],
        "employees": [],
        "commission": [],
        "markup": [],
    }
    users.append(new_user)
    _save_users(users)

    ok, err_msg = _send_email_via_curl(
        email,
        "Your new password",
        f"Hello {username},\n\nYour account has been created.\n\nUsername: {username}\nPassword: {new_pw}\n\nPlease login and change your password in the next phase.\n",
    )
    if not ok:
        return _render(
            request,
            "auth/signup.html",
            {"err": f"Account created but email failed to send: {err_msg}"},
        )

    return RedirectResponse(url="/login?msg=Account%20created.%20Please%20check%20your%20email%20for%20the%20password.", status_code=303)


@app.get("/forgot-password", response_class=HTMLResponse)
def forgot_get(request: Request, msg: str | None = None, err: str | None = None):
    if _get_current_user(request):
        return RedirectResponse(url="/", status_code=303)
    return _render(request, "auth/forgot_password.html", {"msg": msg or "", "err": err or ""})


@app.post("/forgot-password")
async def forgot_post(request: Request, identifier: str = Form("")):
    identifier = (identifier or "").strip()
    if not identifier:
        return _render(request, "auth/forgot_password.html", {"err": "Please enter your email or username."})

    users = _load_users()
    ident_l = identifier.lower()
    match = None
    for u in users:
        if not isinstance(u, dict):
            continue
        if (u.get("username") or "").strip().lower() == ident_l or (u.get("email") or "").strip().lower() == ident_l:
            match = u
            break

    if not match:
        return _render(request, "auth/forgot_password.html", {"err": "No account found for that email/username."})

    to_email = (match.get("email") or "").strip()
    if not to_email:
        return _render(request, "auth/forgot_password.html", {"err": "This account has no email address on file."})

    new_pw = _generate_password(12)
    match["password"] = new_pw
    _save_users(users)

    ok, err_msg = _send_email_via_curl(
        to_email,
        "Password reset",
        f"Hello {match.get('username') or ''},\n\nYour password has been reset.\n\nNew password: {new_pw}\n\nIf you did not request this, contact support.\n",
    )
    if not ok:
        return _render(request, "auth/forgot_password.html", {"err": f"Failed to send email: {err_msg}"})

    return RedirectResponse(url="/login?msg=New%20password%20sent%20to%20your%20email.", status_code=303)


@app.get("/", response_class=HTMLResponse, name="index")
def index(request: Request):
    if APP_PRODUCT == "esim":
        return RedirectResponse(url="/esim", status_code=302)
    if APP_PRODUCT == "hotels":
        return RedirectResponse(url="/hotels", status_code=302)
    if APP_PRODUCT == "admin":
        return RedirectResponse(url="/admin", status_code=302)
    return _render(
        request,
        "flights/search_window.html",
        {
            "defaults": _default_search(),
            "seats_enabled": _seat_estimation_enabled(),
        },
    )


@app.post("/search", response_class=HTMLResponse, name="search")
def search(
    request: Request,
    from_code: str = Form(""),
    to_code: str = Form(""),
    trip_type: str = Form("oneway"),
    date: str = Form(""),
    return_date: str = Form(""),
    cabin: str = Form("economy"),
    adults: int = Form(1),
    children: int = Form(0),
    infants: int = Form(0),
):
    _ensure_product_allowed("flights")
    defaults = _normalize_form(
        {
            "from_code": from_code,
            "to_code": to_code,
            "trip_type": trip_type,
            "date": date,
            "return_date": return_date,
            "cabin": cabin,
            "adults": adults,
            "children": children,
            "infants": infants,
        }
    )

    payload = {
        "from": defaults["from_code"],
        "to": defaults["to_code"],
        "date": defaults["date"],
        "trip_type": defaults["trip_type"],
        "return_date": defaults["return_date"],
        "cabin": defaults["cabin"],
        "pax": {
            "adults": defaults["adults"],
            "children": defaults["children"],
            "infants": defaults["infants"],
        },
    }
    # ---- Enforce provider (OTA) access per user/company admin ----
    allowed_providers = _allowed_provider_ids_for_request(request)
    if allowed_providers is not None:
        if len(allowed_providers) == 0:
            return _render(
                request,
                "flights/search_window.html",
                {
                    "defaults": defaults,
                    "results": [],
                    "results_return": [],
                    "meta": None,
                    "debug_error": "No providers are enabled for your account.",
                    "depart_label_date": _pretty_date(defaults["date"]),
                    "return_label_date": _pretty_date(defaults["return_date"]),
                },
            )


    try:
        r = requests.post(_backend_url("/api/availability"), json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()

        outbound = _enrich_results(data.get("results") or [], defaults["cabin"])
        inbound = _enrich_results(data.get("results_return") or [], defaults["cabin"])

        # Filter by allowed providers if applicable (best-effort based on result fields).
        outbound = _filter_by_allowed_providers(outbound, allowed_providers)
        inbound = _filter_by_allowed_providers(inbound, allowed_providers)

        # Attach stable keys for async seat estimation on the client.
        for r in outbound:
            r["_flight_key"] = _flight_key(r)
        for r in inbound:
            r["_flight_key"] = _flight_key(r)

        return _render(
            request,
            "flights/search_window.html",
            {
                "defaults": defaults,
                "results": outbound,
                "results_return": inbound,
                "meta": data.get("meta") or None,
                "seats_enabled": _seat_estimation_enabled(),
                "depart_label_date": _pretty_date(defaults["date"]),
                "return_label_date": _pretty_date(defaults["return_date"]),
            },
        )
    except Exception as e:
        return _render(
            request,
            "flights/search_window.html",
            {
                "defaults": defaults,
                "results": [],
                "results_return": [],
                "meta": None,
                "debug_error": str(e),
                "seats_enabled": _seat_estimation_enabled(),
                "depart_label_date": _pretty_date(defaults["date"]),
                "return_label_date": _pretty_date(defaults["return_date"]),
            },
        )




@app.get("/passenger-info", response_class=HTMLResponse, name="passenger_info")
def passenger_info(request: Request):
    # The page loads booking context from localStorage (set on Search->Continue).
    _ensure_product_allowed("flights")
    return _render(request, "flights/passenger_info.html", {})

@app.get("/api/features", name="api_features")
def api_features(request: Request):
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), media_type="application/json", status_code=401)

    user_id = str(cu.get("id"))
    active = list_active_addons_for_user(user_id)
    if _is_super_admin(cu) or _is_company_admin(cu):
        active = list(ADDONS.keys())
    return Response(content=json.dumps({"status": "ok", "active_addons": active}), media_type="application/json")


@app.post("/api/seats-estimate", name="seats_estimate")
async def seats_estimate(request: Request):
    _ensure_product_allowed("flights")
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), media_type="application/json", status_code=401)
    if not _seat_estimation_enabled():
        return Response(content=json.dumps({"status": "error", "error": "Seat estimation is disabled."}), media_type="application/json", status_code=403)

    try:
        data = await request.json()
    except Exception:
        data = {}

    defaults = _normalize_form(
        {
            "from_code": (data.get("from_code") or "").strip(),
            "to_code": (data.get("to_code") or "").strip(),
            "trip_type": data.get("trip_type") or "oneway",
            "date": data.get("date") or "",
            "return_date": data.get("return_date") or "",
            "cabin": data.get("cabin") or "economy",
            "adults": int(data.get("adults") or 1),
            "children": int(data.get("children") or 0),
            "infants": int(data.get("infants") or 0),
        }
    )

    payload = {
        "from": defaults["from_code"],
        "to": defaults["to_code"],
        "date": defaults["date"],
        "trip_type": defaults["trip_type"],
        "return_date": defaults["return_date"],
        "cabin": defaults["cabin"],
        "pax": {
            "adults": defaults["adults"],
            "children": defaults["children"],
            "infants": defaults["infants"],
        },
    }

    keys_out = data.get("keys_out") or []
    keys_in = data.get("keys_in") or []
    allowed_providers = _allowed_provider_ids_for_request(request)
    if allowed_providers is not None and len(allowed_providers) == 0:
        return Response(
            content=json.dumps({"status": "error", "error": "No providers are enabled for your account."}),
            media_type="application/json",
            status_code=403,
        )

    seats_out, seats_in = _estimate_seats_for_keys(payload, keys_out, keys_in, allowed_providers)
    return Response(
        content=json.dumps({"status": "ok", "seats_out": seats_out, "seats_in": seats_in}),
        media_type="application/json",
    )




# ------------------------------
# Empty service pages (placeholders for future edits)
# ------------------------------
@app.get("/hotels", response_class=HTMLResponse, name="search_hotels")
def search_hotels(request: Request):
    _ensure_product_allowed("hotels")
    return _render(request, "hotels/search_hotels.html", {})


@app.get("/esim", response_class=HTMLResponse, name="esim")
def esim(request: Request):
    _ensure_product_allowed("esim")
    return _render(request, "esim/esim.html", {})


def _esim_guard(request: Request):
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")
    if not _has_active_addon(request, cu, "esim"):
        return Response(content=json.dumps({"status": "error", "error": "eSIM access is not active for this user."}), status_code=403, media_type="application/json")
    return None


@app.get("/esim/api/bundles")
def esim_bundles(request: Request):
    _ensure_product_allowed("esim")
    err = _esim_guard(request)
    if err:
        return err
    try:
        params = dict(request.query_params)
        r = requests.get(_backend_url("/api/esim/bundles"), params=params, timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.post("/esim/api/quote")
async def esim_quote(request: Request):
    _ensure_product_allowed("esim")
    err = _esim_guard(request)
    if err:
        return err
    try:
        payload = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400)
    try:
        r = requests.post(_backend_url("/api/esim/quote"), json=payload, timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.post("/esim/api/orders")
async def esim_orders_create(request: Request):
    _ensure_product_allowed("esim")
    err = _esim_guard(request)
    if err:
        return err
    try:
        payload = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400)
    try:
        body = dict(payload or {})
        meta = body.pop("_meta", {}) if isinstance(body.get("_meta"), dict) else {}

        def _parse_amount(value):
            try:
                if value is None:
                    return None
                if isinstance(value, str):
                    value = value.replace(",", "").strip()
                if value == "":
                    return None
                return float(value)
            except Exception:
                return None

        payment_method = str(body.get("payment_method") or "").strip().lower() or "cash"
        payment_fib = bool(body.get("payment_fib"))
        fib_payment_override = body.pop("fib_payment", None)
        total_iqd = _parse_amount(body.get("total_iqd"))
        if total_iqd is None:
            try:
                unit_minor = _parse_amount(meta.get("unit_price_iqd_minor"))
                qty = _parse_amount(body.get("quantity")) or 1
                if unit_minor is not None:
                    total_iqd = float(unit_minor) * qty
            except Exception:
                total_iqd = None

        backend_payload = {
            "bundleName": body.get("bundleName"),
            "quantity": body.get("quantity", 1),
            "customerName": body.get("customerName"),
            "customerPhone": body.get("customerPhone"),
            "idempotencyKey": body.get("idempotencyKey"),
        }
        backend_payload = {k: v for k, v in backend_payload.items() if v is not None and v != ""}

        # Payment checks before placing the order
        cu = _get_current_user(request)
        users = _load_users()
        billing = _get_company_admin_for_user(users, cu) or cu or {}
        billing_id = str((billing or {}).get("id") or "")
        billing_u = _find_user(users, billing_id) if billing_id else None
        if not billing_u:
            billing_u = billing or {}
        billing = billing_u or billing

        if total_iqd is not None:
            if payment_method == "credit":
                credit_f = _parse_amount(billing_u.get("credit")) or 0
                if credit_f < float(total_iqd or 0):
                    return Response(
                        content=json.dumps({"status": "error", "error": "Insufficient credit."}),
                        status_code=400,
                        media_type="application/json",
                    )
            elif payment_method == "cash" and not payment_fib:
                cash_f = _parse_amount(billing_u.get("cash")) or 0
                if cash_f < float(total_iqd or 0):
                    return Response(
                        content=json.dumps({"status": "error", "error": "Insufficient cash."}),
                        status_code=400,
                        media_type="application/json",
                    )

        r = requests.post(_backend_url("/api/esim/orders"), json=backend_payload, timeout=30)
        try:
            data = r.json()
        except Exception:
            data = None

        fib_payment = fib_payment_override if isinstance(fib_payment_override, dict) else None
        if r.status_code < 400 and payment_method == "cash" and payment_fib and total_iqd is not None and fib_payment is None:
            try:
                pay_payload = {
                    "amount": int(round(total_iqd)),
                    "description": f"eSIM {backend_payload.get('bundleName') or ''}".strip(),
                }
                pr = requests.post(_backend_url("/api/other-apis/fib/create-payment"), json=pay_payload, timeout=30)
                if pr.status_code < 400:
                    fib_payment = pr.json()
            except Exception:
                fib_payment = None

        # Deduct balances after successful order placement
        if r.status_code < 400 and total_iqd is not None:
            if payment_method == "credit":
                new_credit = ( _parse_amount(billing_u.get("credit")) or 0 ) - float(total_iqd)
                billing_u["credit"] = int(new_credit) if int(new_credit) == new_credit else new_credit
                _save_users(users)
            elif payment_method == "cash" and not payment_fib:
                new_cash = ( _parse_amount(billing_u.get("cash")) or 0 ) - float(total_iqd)
                billing_u["cash"] = int(new_cash) if int(new_cash) == new_cash else new_cash
                _save_users(users)

        # Record local report + passenger history on success
        if r.status_code < 400 and isinstance(data, dict):
            owner_id = str((billing or {}).get("id") or "")

            agent_name = (cu.get("username") or cu.get("email") or "") if isinstance(cu, dict) else ""
            agent_id = str((cu or {}).get("id") or "") if isinstance(cu, dict) else ""
            company_name = str((billing or {}).get("company_name") or (billing or {}).get("company") or "")

            quantity = int(backend_payload.get("quantity") or 1)
            unit_minor = meta.get("unit_price_iqd_minor")
            total_iqd = None
            try:
                unit_minor = float(unit_minor) if unit_minor is not None else None
                if unit_minor is not None:
                    total_iqd = int(round(unit_minor * quantity))
            except Exception:
                total_iqd = None

            order_ref = data.get("orderReference") or data.get("order_id") or data.get("orderId") or ""
            status = data.get("status") or "processing"

            record_esim_order(
                {
                    "owner_user_id": owner_id,
                    "company_name": company_name,
                    "agent_user_id": agent_id,
                    "agent_name": agent_name,
                    "customer_name": backend_payload.get("customerName") or "",
                    "customer_phone": backend_payload.get("customerPhone") or "",
                    "bundle_name": backend_payload.get("bundleName") or "",
                    "bundle_description": meta.get("bundle_description") or "",
                    "country_name": meta.get("country_name") or "",
                    "country_iso": meta.get("country_iso") or "",
                    "quantity": quantity,
                    "total_iqd": total_iqd,
                    "currency": "IQD",
                    "status": status,
                    "status_message": data.get("statusMessage") or "",
                    "order_reference": order_ref,
                    "activation_codes": data.get("activationCodes") or [],
                    "payment_method": payment_method,
                    "payment_fib": payment_fib,
                    "payment_amount_iqd": total_iqd,
                }
            )

            try:
                attach_esim_to_passenger(
                    owner_user_id=owner_id,
                    customer_name=backend_payload.get("customerName") or "",
                    customer_phone=backend_payload.get("customerPhone") or "",
                    details={
                        "bundle_name": backend_payload.get("bundleName") or "",
                        "bundle_description": meta.get("bundle_description") or "",
                        "country_name": meta.get("country_name") or "",
                        "country_iso": meta.get("country_iso") or "",
                        "quantity": quantity,
                        "total_iqd": total_iqd,
                        "currency": "IQD",
                        "status": status,
                        "order_reference": order_ref,
                        "company_name": company_name,
                        "agent_name": agent_name,
                    },
                    allow_create=False,
                )
            except Exception:
                pass

        if r.status_code < 400 and isinstance(data, dict) and fib_payment:
            data["payment"] = fib_payment
            return Response(content=json.dumps(data), status_code=r.status_code, media_type="application/json")

        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/esim/api/orders")
def esim_orders_list(request: Request):
    _ensure_product_allowed("esim")
    err = _esim_guard(request)
    if err:
        return err
    try:
        params = dict(request.query_params)
        r = requests.get(_backend_url("/api/esim/orders"), params=params, timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/esim/api/orders/{order_id}")
def esim_orders_get(request: Request, order_id: str):
    _ensure_product_allowed("esim")
    err = _esim_guard(request)
    if err:
        return err
    try:
        r = requests.get(_backend_url(f"/api/esim/orders/{order_id}"), timeout=30)
        try:
            data = r.json()
        except Exception:
            data = None
        if r.status_code < 400 and isinstance(data, dict):
            update_esim_order(
                order_id,
                {
                    "status": data.get("status") or "",
                    "status_message": data.get("statusMessage") or "",
                    "activation_codes": data.get("activationCodes") or [],
                },
            )
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/esim/api/settings")
def esim_settings(request: Request):
    _ensure_product_allowed("esim")
    err = _esim_guard(request)
    if err:
        return err
    try:
        r = requests.get(_backend_url("/api/esim/settings"), timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/reports/esim", response_class=HTMLResponse, name="report_esim")
def report_esim(request: Request):
    _ensure_product_allowed("esim")
    cu = _get_current_user(request)
    if not cu:
        return RedirectResponse(url="/login", status_code=303)
    if not _has_active_addon(request, cu, "esim"):
        return RedirectResponse(url=str(request.url_for("subscriptions")), status_code=303)
    return _render(request, "esim/reports/esim.html", {})


@app.get("/reports/esim/api/list")
def report_esim_list(request: Request):
    _ensure_product_allowed("esim")
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")
    if not _has_active_addon(request, cu, "esim"):
        return Response(content=json.dumps({"status": "error", "error": "eSIM access is not active for this user."}), status_code=403, media_type="application/json")

    users = _load_users()
    billing = _get_company_admin_for_user(users, cu) or cu
    owner_id = str((billing or {}).get("id") or "")

    if _is_sub_user(cu):
        orders = list_esim_orders_for_agent(owner_id, str(cu.get("id")))
    else:
        orders = list_esim_orders_for_owner(owner_id)

    return Response(
        content=json.dumps({"status": "ok", "orders": orders}, ensure_ascii=False),
        media_type="application/json",
    )


@app.get("/airport-transportation", response_class=HTMLResponse, name="airport_transportation")
def airport_transportation(request: Request):
    _ensure_product_allowed()
    return _render(request, "extras/airport_transportation.html", {})


@app.get("/car-rental", response_class=HTMLResponse, name="car_rental")
def car_rental(request: Request):
    _ensure_product_allowed()
    return _render(request, "extras/car_rental.html", {})


@app.get("/cip-services", response_class=HTMLResponse, name="cip_services")
def cip_services(request: Request):
    _ensure_product_allowed()
    return _render(request, "extras/cip_services.html", {})


@app.get("/visa", response_class=HTMLResponse, name="visa")
def visa(request: Request):
    # Demo page (we will extend later)
    _ensure_product_allowed()
    return _render(request, "extras/visa.html", {})


@app.get("/visa/api/list", name="visa_list")
def visa_list(request: Request, q: str = ""):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")

    users = _load_users()
    owner = _get_company_admin_for_user(users, cu) or cu
    owner_id = str(owner.get("id") or "")

    qn = (q or "").strip().lower()
    items = [v for v in _load_visas() if str(v.get("owner_user_id") or "") == owner_id]
    if qn:
        items = [
            v for v in items
            if qn in str(v.get("passport_number") or "").lower()
            or qn in str(v.get("passenger_name") or "").lower()
            or qn in str(v.get("destination_country") or "").lower()
        ]
    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return Response(content=json.dumps({"status": "ok", "visas": items}), media_type="application/json")


@app.get("/visa/api/vendors", name="visa_vendors")
def visa_vendors(request: Request):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")

    users = _load_users()
    vendors = _list_visa_vendors(users)
    out = []
    for v in vendors:
        prices = v.get("vendor_visa_prices") if isinstance(v.get("vendor_visa_prices"), list) else []
        active_prices = [p for p in prices if isinstance(p, dict) and p.get("active", True)]
        out.append(
            {
                "id": str(v.get("id") or ""),
                "name": v.get("vendor_name") or v.get("company_name") or v.get("username") or v.get("email") or "",
                "prices": active_prices,
            }
        )
    return Response(content=json.dumps({"status": "ok", "vendors": out}), media_type="application/json")


@app.get("/visa/api/vendor/offers", name="visa_vendor_offers")
def visa_vendor_offers(request: Request):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")
    if not _has_active_addon(request, cu, "visa_vendor"):
        return Response(content=json.dumps({"status": "error", "error": "Visa Vendor add-on is not active."}), status_code=403, media_type="application/json")

    users = _load_users()
    owner = _get_company_admin_for_user(users, cu) or cu
    u = _find_user(users, str(owner.get("id"))) or owner
    prices = u.get("vendor_visa_prices") if isinstance(u.get("vendor_visa_prices"), list) else []
    return Response(content=json.dumps({"status": "ok", "offers": prices}), media_type="application/json")


@app.post("/visa/api/vendor/offer", name="visa_vendor_offer_create")
async def visa_vendor_offer_create(request: Request):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")
    if not _has_active_addon(request, cu, "visa_vendor"):
        return Response(content=json.dumps({"status": "error", "error": "Visa Vendor add-on is not active."}), status_code=403, media_type="application/json")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    country = str(payload.get("country") or "").strip()
    price = payload.get("price")
    currency = str(payload.get("currency") or "IQD").strip().upper()
    notes = str(payload.get("notes") or "").strip()
    active = False if str(payload.get("active") or "").lower() in ("0", "false", "no") else True

    try:
        price_val = float(price)
    except Exception:
        price_val = None

    if not country or price_val is None or price_val <= 0:
        return Response(content=json.dumps({"status": "error", "error": "Country and price are required."}), status_code=400, media_type="application/json")

    users = _load_users()
    owner = _get_company_admin_for_user(users, cu) or cu
    u = _find_user(users, str(owner.get("id")))
    if not u:
        return Response(content=json.dumps({"status": "error", "error": "User not found."}), status_code=404, media_type="application/json")

    offers = u.get("vendor_visa_prices") if isinstance(u.get("vendor_visa_prices"), list) else []
    offers.append(
        {
            "id": f"offer_{uuid.uuid4().hex[:10]}",
            "country": country,
            "price": price_val,
            "currency": currency or "IQD",
            "notes": notes,
            "active": active,
            "created_at": datetime.utcnow().isoformat() + "Z",
        }
    )
    u["vendor_visa_prices"] = offers
    _save_users(users)
    return Response(content=json.dumps({"status": "ok", "offers": offers}), media_type="application/json")


@app.post("/visa/api/vendor/offer/{offer_id}/delete", name="visa_vendor_offer_delete")
async def visa_vendor_offer_delete(request: Request, offer_id: str):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")
    if not _has_active_addon(request, cu, "visa_vendor"):
        return Response(content=json.dumps({"status": "error", "error": "Visa Vendor add-on is not active."}), status_code=403, media_type="application/json")

    users = _load_users()
    owner = _get_company_admin_for_user(users, cu) or cu
    u = _find_user(users, str(owner.get("id")))
    if not u:
        return Response(content=json.dumps({"status": "error", "error": "User not found."}), status_code=404, media_type="application/json")

    offers = u.get("vendor_visa_prices") if isinstance(u.get("vendor_visa_prices"), list) else []
    offers = [o for o in offers if not (isinstance(o, dict) and str(o.get("id") or "") == str(offer_id))]
    u["vendor_visa_prices"] = offers
    _save_users(users)
    return Response(content=json.dumps({"status": "ok", "offers": offers}), media_type="application/json")


@app.get("/visa/api/vendor/assigned", name="visa_vendor_assigned")
def visa_vendor_assigned(request: Request):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")
    if not _has_active_addon(request, cu, "visa_vendor"):
        return Response(content=json.dumps({"status": "error", "error": "Visa Vendor add-on is not active."}), status_code=403, media_type="application/json")

    users = _load_users()
    owner = _get_company_admin_for_user(users, cu) or cu
    items = [v for v in _load_visas() if str(v.get("vendor_id") or "") == str(owner.get("id"))]
    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return Response(content=json.dumps({"status": "ok", "visas": items}), media_type="application/json")


@app.post("/visa/api/create", name="visa_create")
async def visa_create(request: Request):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")

    payload = {}
    files_scans: list[UploadFile] = []
    files_photos: list[UploadFile] = []
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
    else:
        try:
            form = await request.form()
            payload = dict(form)
            try:
                files_scans = form.getlist("passport_scan")
            except Exception:
                files_scans = []
            try:
                files_photos = form.getlist("photo")
            except Exception:
                files_photos = []
        except Exception:
            payload = {}

    passport = str(payload.get("passport") or "").strip()
    country = str(payload.get("country") or "").strip()
    notes = str(payload.get("notes") or "").strip()

    if not passport or not country:
        return Response(content=json.dumps({"status": "error", "error": "Passport number and destination country are required."}), status_code=400, media_type="application/json")

    users = _load_users()
    owner = _get_company_admin_for_user(users, cu) or cu
    owner_id = str(owner.get("id") or "")

    link = _link_visa_passenger(owner_id, passport)
    status = _normalize_visa_status(payload.get("status"))

    visa_id = f"visa_{uuid.uuid4().hex[:12]}"
    attachments = []
    try:
        attachments.extend(await _save_visa_attachments(visa_id, files_scans, "passport_scan"))
        attachments.extend(await _save_visa_attachments(visa_id, files_photos, "photo"))
    except Exception:
        attachments = attachments or []

    vendor_id = str(payload.get("vendor_id") or "").strip()
    channel = str(payload.get("channel") or "direct").strip().lower()
    if channel not in ("direct", "vendor"):
        channel = "direct"

    vendor_name = ""
    vendor_price = None
    vendor_currency = ""
    if channel == "vendor":
        try:
            vendor_price = float(payload.get("price"))
        except Exception:
            vendor_price = None
        vendor_currency = str(payload.get("currency") or "IQD").strip().upper()
        if not vendor_id or vendor_price is None or vendor_price <= 0:
            return Response(content=json.dumps({"status": "error", "error": "Vendor, price, and currency are required for vendor submissions."}), status_code=400, media_type="application/json")
        # Ensure vendor exists
        vendors = _list_visa_vendors(_load_users())
        vmatch = next((v for v in vendors if str(v.get("id")) == vendor_id), None)
        if not vmatch:
            return Response(content=json.dumps({"status": "error", "error": "Vendor not found."}), status_code=400, media_type="application/json")
        vendor_name = vmatch.get("vendor_name") or vmatch.get("company_name") or vmatch.get("username") or ""

    visa = {
        "id": visa_id,
        "owner_user_id": owner_id,
        "created_by_user_id": str(cu.get("id") or ""),
        "passport_number": passport,
        "destination_country": country,
        "notes": notes,
        "status": status,
        "channel": channel,
        "vendor_id": vendor_id,
        "vendor_name": vendor_name,
        "vendor_price": vendor_price,
        "vendor_currency": vendor_currency,
        "attachments": attachments,
        "profile_id": link.get("profile_id") or "",
        "member_id": link.get("member_id") or "",
        "passenger_name": link.get("passenger_name") or "",
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "updated_by_user_id": str(cu.get("id") or ""),
    }

    items = _load_visas()
    items.insert(0, visa)
    _save_visas(items)

    # Add to passenger history (best-effort).
    try:
        if visa.get("profile_id") and visa.get("member_id"):
            add_history_event(
                owner_user_id=owner_id,
                profile_id=str(visa.get("profile_id")),
                member_id=str(visa.get("member_id")),
                kind="visa",
                details={
                    "status": visa.get("status"),
                    "country": visa.get("destination_country"),
                    "passport_number": visa.get("passport_number"),
                    "notes": visa.get("notes"),
                    "visa_id": visa.get("id"),
                },
            )
    except Exception:
        pass

    return Response(content=json.dumps({"status": "ok", "visa": visa}), media_type="application/json")


@app.post("/visa/api/{visa_id}/status", name="visa_update_status")
async def visa_update_status(request: Request, visa_id: str):
    _ensure_product_allowed()
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps({"status": "error", "error": "Not authenticated."}), status_code=401, media_type="application/json")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    status = _normalize_visa_status(payload.get("status"))

    users = _load_users()
    owner = _get_company_admin_for_user(users, cu) or cu
    owner_id = str(owner.get("id") or "")

    items = _load_visas()
    visa = next((v for v in items if str(v.get("id") or "") == str(visa_id)), None)
    if not visa:
        return Response(content=json.dumps({"status": "error", "error": "Visa application not found."}), status_code=404, media_type="application/json")

    is_owner = str(visa.get("owner_user_id") or "") == owner_id
    vendor_owner = _get_company_admin_for_user(users, cu) or cu
    is_vendor = str(visa.get("vendor_id") or "") == str(vendor_owner.get("id") or "")
    if is_vendor and not _has_active_addon(request, cu, "visa_vendor"):
        is_vendor = False
    if not (is_owner or is_vendor):
        return Response(content=json.dumps({"status": "error", "error": "Forbidden."}), status_code=403, media_type="application/json")

    visa["status"] = status
    visa["updated_at"] = datetime.utcnow().isoformat() + "Z"
    visa["updated_by_user_id"] = str(cu.get("id") or "")
    _save_visas(items)

    # Add history event for status change (best-effort).
    try:
        if visa.get("profile_id") and visa.get("member_id"):
            add_history_event(
                owner_user_id=owner_id,
                profile_id=str(visa.get("profile_id")),
                member_id=str(visa.get("member_id")),
                kind="visa",
                details={
                    "status": visa.get("status"),
                    "country": visa.get("destination_country"),
                    "passport_number": visa.get("passport_number"),
                    "notes": visa.get("notes"),
                    "visa_id": visa.get("id"),
                },
            )
    except Exception:
        pass

    return Response(content=json.dumps({"status": "ok", "visa": visa}), media_type="application/json")



@app.get("/payment", response_class=HTMLResponse, name="payment")
def payment(request: Request):
    _ensure_product_allowed("flights")
    return _render(request, "flights/payment.html", {})


@app.post("/issue-ticket")
async def issue_ticket(request: Request):
    _ensure_product_allowed("flights")
    try:
        payload = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400)

    if not isinstance(payload, dict):
        return Response(content="Invalid JSON", status_code=400)

    # Determine the acting user (session) and the billing user (company admin wallet for sub users).
    cu = _get_current_user(request)
    if not cu:
        return Response(
            content=json.dumps({"status": "error", "error": "Not authenticated."}),
            status_code=401,
            media_type="application/json",
        )

    users = _load_users()
    billing = _get_company_admin_for_user(users, cu) or cu
    billing_u = _find_user(users, str(billing.get("id"))) if billing else None
    if not billing_u:
        return Response(
            content=json.dumps({"status": "error", "error": "Billing user not found."}),
            status_code=401,
            media_type="application/json",
        )

    # Enforce provider access (OTA enable/disable).
    allowed = _allowed_provider_ids_for_request(request)
    provider_id = _extract_provider_id(payload)
    if allowed is not None:
        # If we can identify the provider and it's not allowed, push to pending for super admin.
        if provider_id and provider_id not in allowed:
            pending_id = _add_pending(
                kind="provider_disabled",
                requested_by_user_id=str(cu.get("id")),
                company_admin_id=str(billing_u.get("id")),
                provider_id=provider_id,
                payload=payload,
                reason="Provider is disabled for this account.",
            )

            snap = _build_tx_snapshot(payload)
            _add_transaction(
                {
                    "status": "pending",
                    "pnr": "",
                    "booking_code": "",
                    "pending_id": pending_id,
                    "provider_id": provider_id or "",
                    "company_admin_id": str(billing_u.get("id")),
                    "user_id": str(cu.get("id")),
                    "by_user_id": str(cu.get("id")),
                    "by": cu.get("username") or cu.get("email") or "",
                    **{k: snap.get(k) for k in ("first_name", "last_name", "airline", "from", "to")},
                    "price": snap.get("price"),
                    "currency": snap.get("currency"),
                    "details": {
                        **snap,
                        "payment_method": str((payload.get("payment") or {}).get("method") or ""),
                        "provider_id": provider_id or "",
                    },
                }
            )

            # Attach booking to passenger history (best-effort)
            try:
                pnr = locals().get("pnr", "")
                booking_code = locals().get("booking_code", "")
                snap_for_hist = _build_tx_snapshot(payload)
                attach_booking_to_passengers(
                    owner_user_id=str(billing_u.get("id")),
                    payload=payload,
                    kind="flight",
                    booking_meta={
                        "status": str(snap_for_hist.get("status") or ""),
                        "from": snap_for_hist.get("from"),
                        "to": snap_for_hist.get("to"),
                        "airline": snap_for_hist.get("airline"),
                        "price": snap_for_hist.get("price"),
                        "currency": snap_for_hist.get("currency"),
                        "pnr": str(pnr or ""),
                        "booking_code": str(booking_code or ""),
                    },
                )
            except Exception:
                pass

            return Response(
                content=json.dumps(
                    {
                        "status": "pending",
                        "pending": True,
                        "pending_kind": "provider_disabled",
                        "deducted": False,
                        "pending_id": pending_id,
                        "message": "Provider is disabled. Request sent to super admin pending.",
                    }
                ),
                status_code=202,
                media_type="application/json",
            )

        # If no provider id was found, be conservative and require at least one provider enabled.
        if (not provider_id) and len(allowed) == 0:
            pending_id = _add_pending(
                kind="no_providers_enabled",
                requested_by_user_id=str(cu.get("id")),
                company_admin_id=str(billing_u.get("id")),
                provider_id="",
                payload=payload,
                reason="No providers enabled for this account.",
            )

            snap = _build_tx_snapshot(payload)
            _add_transaction(
                {
                    "status": "pending",
                    "pnr": "",
                    "booking_code": "",
                    "pending_id": pending_id,
                    "provider_id": provider_id or "",
                    "company_admin_id": str(billing_u.get("id")),
                    "user_id": str(cu.get("id")),
                    "by_user_id": str(cu.get("id")),
                    "by": cu.get("username") or cu.get("email") or "",
                    **{k: snap.get(k) for k in ("first_name", "last_name", "airline", "from", "to")},
                    "price": snap.get("price"),
                    "currency": snap.get("currency"),
                    "details": {
                        **snap,
                        "payment_method": str((payload.get("payment") or {}).get("method") or ""),
                        "provider_id": provider_id or "",
                    },
                }
            )

            # Attach booking to passenger history (best-effort)
            try:
                pnr = locals().get("pnr", "")
                booking_code = locals().get("booking_code", "")
                snap_for_hist = _build_tx_snapshot(payload)
                attach_booking_to_passengers(
                    owner_user_id=str(billing_u.get("id")),
                    payload=payload,
                    kind="flight",
                    booking_meta={
                        "status": str(snap_for_hist.get("status") or ""),
                        "from": snap_for_hist.get("from"),
                        "to": snap_for_hist.get("to"),
                        "airline": snap_for_hist.get("airline"),
                        "price": snap_for_hist.get("price"),
                        "currency": snap_for_hist.get("currency"),
                        "pnr": str(pnr or ""),
                        "booking_code": str(booking_code or ""),
                    },
                )
            except Exception:
                pass

            return Response(
                content=json.dumps(
                    {
                        "status": "pending",
                        "pending": True,
                        "pending_kind": "no_providers_enabled",
                        "deducted": False,
                        "pending_id": pending_id,
                        "message": "No providers enabled. Request sent to super admin pending.",
                    }
                ),
                status_code=202,
                media_type="application/json",
            )

        # Ensure ticketing vendor is present for backends that require it.
    _ensure_ticketing_vendor(payload)

# ---- Payment enforcement (Cash / Credit) ----
    def _parse_amount(v):
        try:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            if not s:
                return None
            cleaned = []
            dot_used = False
            for ch in s:
                if ch.isdigit():
                    cleaned.append(ch)
                elif ch == ",":
                    continue
                elif ch == "." and not dot_used:
                    cleaned.append(".")
                    dot_used = True
            num = "".join(cleaned)
            if not num:
                return None
            return float(num)
        except Exception:
            return None

    payment_info = payload.get("payment") if isinstance(payload, dict) else None
    method = ""
    if isinstance(payment_info, dict):
        method = str(payment_info.get("method") or "").strip().lower()

    # Extract total price from common keys.
    total = None
    for key in ("total_amount", "amount", "total", "ticket_total", "price", "total_price"):
        if key in payload:
            total = _parse_amount(payload.get(key))
            if total is not None:
                break
    # allow nested selection.total_price
    if total is None:
        sel = payload.get("selection")
        if isinstance(sel, dict):
            total = _parse_amount(sel.get("total_price") or sel.get("price") or sel.get("total"))

    if method:
        if total is None:
            return Response(
                content=json.dumps({"status": "error", "error": "Missing total amount for payment."}),
                status_code=400,
                media_type="application/json",
            )

        if method == "cash":
            cash_f = float(_to_number(billing_u.get("cash"), 0) or 0)
            if cash_f < float(total):
                return Response(
                    content=json.dumps({"status": "error", "error": "Insufficient cash."}),
                    status_code=400,
                    media_type="application/json",
                )

        elif method == "credit":
            credit_f = float(_to_number(billing_u.get("credit"), 0) or 0)
            if credit_f < float(total):
                return Response(
                    content=json.dumps({"status": "error", "error": "Insufficient credit."}),
                    status_code=400,
                    media_type="application/json",
                )

        else:
            return Response(
                content=json.dumps({"status": "error", "error": "Invalid payment method."}),
                status_code=400,
                media_type="application/json",
            )

        # Call backend to issue.
        try:
            r = requests.post(_backend_url("/api/book"), json=payload, timeout=90)
            data = None
            try:
                data = r.json()
            except Exception:
                data = None

            is_pending = isinstance(data, dict) and (data.get("pending") is True or str(data.get("status") or "").lower() == "pending")

            if 200 <= int(r.status_code) < 300 and not is_pending:
                # Deduct after successful booking only.
                if method == "cash":
                    new_cash = float(_to_number(billing_u.get("cash"), 0) or 0) - float(total)
                    billing_u["cash"] = int(new_cash) if int(new_cash) == new_cash else new_cash
                elif method == "credit":
                    new_credit = float(_to_number(billing_u.get("credit"), 0) or 0) - float(total)
                    billing_u["credit"] = int(new_credit) if int(new_credit) == new_credit else new_credit
                _save_users(users)

                # Persist successful transaction (company-wide visibility).
                snap = _build_tx_snapshot(payload)
                pnr = ""
                booking_code = ""
                if isinstance(data, dict):
                    pnr = str(data.get("pnr") or data.get("ticket_number") or data.get("connectota_id") or "").strip()
                    booking_code = str(data.get("booking_code") or data.get("bookingCode") or "").strip()

                _add_transaction(
                    {
                        "status": "successful",
                        "pnr": pnr,
                        "booking_code": booking_code,
                        "pending_id": "",
                        "provider_id": provider_id or "",
                        "company_admin_id": str(billing_u.get("id")),
                        "user_id": str(cu.get("id")),
                        "by_user_id": str(cu.get("id")),
                        "by": cu.get("username") or cu.get("email") or "",
                        **{k: snap.get(k) for k in ("first_name", "last_name", "airline", "from", "to")},
                        "price": snap.get("price"),
                        "currency": snap.get("currency"),
                        "details": {
                            **snap,
                            "pnr": pnr,
                            "booking_code": booking_code,
                            "payment_method": method,
                            "provider_id": provider_id or "",
                        },
                    }
                )
            elif is_pending:
                # Backend returned pending (ticketing disabled / schedule / upstream).
                pending_id = _add_pending(
                    kind=str(data.get("pending_kind") or "ticketing_pending"),
                    requested_by_user_id=str(cu.get("id")),
                    company_admin_id=str(billing_u.get("id")),
                    provider_id=provider_id or str(data.get("provider") or ""),
                    payload=payload,
                    reason=str(data.get("reason") or "Ticketing pending."),
                )

                snap = _build_tx_snapshot(payload)
                _add_transaction(
                    {
                        "status": "pending",
                        "pnr": "",
                        "booking_code": "",
                        "pending_id": pending_id,
                        "provider_id": provider_id or "",
                        "company_admin_id": str(billing_u.get("id")),
                        "user_id": str(cu.get("id")),
                        "by_user_id": str(cu.get("id")),
                        "by": cu.get("username") or cu.get("email") or "",
                        **{k: snap.get(k) for k in ("first_name", "last_name", "airline", "from", "to")},
                        "price": snap.get("price"),
                        "currency": snap.get("currency"),
                        "details": {
                            **snap,
                            "payment_method": method,
                            "provider_id": provider_id or "",
                            "backend_pending_id": str(data.get("pending_id") or ""),
                        },
                    }
                )

                if method == "credit":
                    try:
                        new_credit = float(_to_number(billing_u.get("credit"), 0) or 0) - float(total)
                        billing_u["credit"] = int(new_credit) if int(new_credit) == new_credit else new_credit

                        audit = billing_u.get("audit")
                        if not isinstance(audit, list):
                            audit = []
                        audit.append(
                            {
                                "ts": datetime.utcnow().isoformat() + "Z",
                                "action": "payment_hold",
                                "kind": "credit",
                                "amount": float(total),
                                "notes": "Credit payment queued for manual ticketing.",
                            }
                        )
                        billing_u["audit"] = audit
                        _save_users(users)
                    except Exception:
                        pass

                return Response(
                    content=json.dumps(
                        {
                            "status": "pending",
                            "pending": True,
                            "pending_kind": str(data.get("pending_kind") or "ticketing_pending"),
                            "deducted": (method == "credit"),
                            "pending_id": pending_id,
                            "message": str(data.get("reason") or "Ticketing pending."),
                        }
                    ),
                    status_code=202,
                    media_type="application/json",
                )

            # Attach booking to passenger history (best-effort)
            try:
                pnr = locals().get("pnr", "")
                booking_code = locals().get("booking_code", "")
                snap_for_hist = _build_tx_snapshot(payload)
                attach_booking_to_passengers(
                    owner_user_id=str(billing_u.get("id")),
                    payload=payload,
                    kind="flight",
                    booking_meta={
                        "status": str(snap_for_hist.get("status") or ""),
                        "from": snap_for_hist.get("from"),
                        "to": snap_for_hist.get("to"),
                        "airline": snap_for_hist.get("airline"),
                        "price": snap_for_hist.get("price"),
                        "currency": snap_for_hist.get("currency"),
                        "pnr": str(pnr or ""),
                        "booking_code": str(booking_code or ""),
                    },
                )
            except Exception:
                pass

            return Response(
                content=r.text,
                status_code=r.status_code,
                media_type=r.headers.get("content-type", "application/json"),
            )
        except Exception as e:
            return Response(content=str(e), status_code=502)

    # ---- No payment info: keep original behavior ----
    try:
        r = requests.post(_backend_url("/api/book"), json=payload, timeout=90)
        if 200 <= int(r.status_code) < 300:
            snap = _build_tx_snapshot(payload)
            pnr = ""
            booking_code = ""
            try:
                data = r.json()
                if isinstance(data, dict):
                    pnr = str(data.get("pnr") or data.get("ticket_number") or data.get("connectota_id") or "").strip()
                    booking_code = str(data.get("booking_code") or data.get("bookingCode") or "").strip()
            except Exception:
                pass

            _add_transaction(
                {
                    "status": "successful",
                    "pnr": pnr,
                    "booking_code": booking_code,
                    "pending_id": "",
                    "provider_id": provider_id or "",
                    "company_admin_id": str(billing_u.get("id")),
                    "user_id": str(cu.get("id")),
                    "by_user_id": str(cu.get("id")),
                    "by": cu.get("username") or cu.get("email") or "",
                    **{k: snap.get(k) for k in ("first_name", "last_name", "airline", "from", "to")},
                    "price": snap.get("price"),
                    "currency": snap.get("currency"),
                    "details": {
                        **snap,
                        "pnr": pnr,
                        "booking_code": booking_code,
                        "payment_method": "",
                        "provider_id": provider_id or "",
                    },
                }
            )

            # Attach booking to passenger history (best-effort)
            try:
                pnr = locals().get("pnr", "")
                booking_code = locals().get("booking_code", "")
                snap_for_hist = _build_tx_snapshot(payload)
                attach_booking_to_passengers(
                    owner_user_id=str(billing_u.get("id")),
                    payload=payload,
                    kind="flight",
                    booking_meta={
                        "status": str(snap_for_hist.get("status") or ""),
                        "from": snap_for_hist.get("from"),
                        "to": snap_for_hist.get("to"),
                        "airline": snap_for_hist.get("airline"),
                        "price": snap_for_hist.get("price"),
                        "currency": snap_for_hist.get("currency"),
                        "pnr": str(pnr or ""),
                        "booking_code": str(booking_code or ""),
                    },
                )
            except Exception:
                pass
        return Response(
            content=r.text,
            status_code=r.status_code,
            media_type=r.headers.get("content-type", "application/json"),
        )
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/transactions", response_class=HTMLResponse, name="transactions")
def transactions(request: Request):
    return _render(request, "Userpanel/transaction.html", {})


@app.get("/api/transactions")
def api_transactions(request: Request):
    """Return transactions visible to the current user.

    - Super admin: all
    - Company admin: all company transactions
    - Sub user: all company transactions (company-wide visibility)
    """
    cu = _get_current_user(request)
    if not cu:
        return Response(content=json.dumps([]), status_code=401, media_type="application/json")

    txs = _load_transactions()
    if _is_super_admin(cu):
        return Response(content=json.dumps(txs), media_type="application/json")

    users = _load_users()
    admin = _get_company_admin_for_user(users, cu) or cu
    admin_id = str(admin.get("id")) if isinstance(admin, dict) else ""
    filtered = [t for t in txs if isinstance(t, dict) and str(t.get("company_admin_id") or "") == admin_id]
    return Response(content=json.dumps(filtered), media_type="application/json")


@app.get("/company-admin", response_class=HTMLResponse, name="company_admin")
def company_admin(request: Request):
    cu = _get_current_user(request)
    users = _load_users()
    subusers = []
    company_active = []
    if cu:
        try:
            company_active = list_active_addons_for_user(str(cu.get("id")), owner_user_id=str(cu.get("id")))
        except Exception:
            company_active = []
        for u in users:
            if isinstance(u, dict) and str(u.get("company_admin_id") or "") == str(cu.get("id")):
                try:
                    u["assigned_addons"] = list_active_addons_for_user(
                        str(u.get("id")), owner_user_id=str(cu.get("id"))
                    )
                except Exception:
                    u["assigned_addons"] = []
                subusers.append(u)
    return _render(
        request,
        "admin/company_admin.html",
        {"subusers": subusers, "addons": ADDONS, "company_active_addons": company_active},
    )


@app.post("/company-admin/users", name="company_admin_create_subuser")
async def company_admin_create_subuser(
    request: Request,
    first_name: str = Form(""),
    last_name: str = Form(""),
    email: str = Form(""),
    position: str = Form(""),
    username: str = Form(""),
    password: str = Form(""),
):
    cu = _get_current_user(request)
    if not cu:
        return RedirectResponse(url="/login", status_code=303)

    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    email = (email or "").strip()
    position = (position or "").strip()
    username = (username or "").strip()
    password = (password or "").strip()

    if not first_name or not last_name or not username or not password or not email or "@" not in email:
        return _render(
            request,
            "admin/company_admin.html",
            {
                "subusers": [u for u in _load_users() if isinstance(u, dict) and str(u.get("company_admin_id") or "") == str(cu.get("id"))],
                "err": "Please fill first name, last name, email, position, username, and password.",
            },
        )

    users = _load_users()
    email_l = email.lower()
    user_l = username.lower()
    for u in users:
        if not isinstance(u, dict):
            continue
        if (u.get("email") or "").strip().lower() == email_l:
            return _render(
                request,
                "admin/company_admin.html",
                {
                    "subusers": [x for x in users if isinstance(x, dict) and str(x.get("company_admin_id") or "") == str(cu.get("id"))],
                    "err": "This email is already registered.",
                },
            )
        if (u.get("username") or "").strip().lower() == user_l:
            return _render(
                request,
                "admin/company_admin.html",
                {
                    "subusers": [x for x in users if isinstance(x, dict) and str(x.get("company_admin_id") or "") == str(cu.get("id"))],
                    "err": "This username is already taken.",
                },
            )

    # Inherit company settings from the main user.
    inherited_apis = _normalize_api_ids(cu.get("apis"))
    sub = {
        "id": uuid.uuid4().hex,
        "type": "sub_user",
        "company_name": cu.get("company_name") or cu.get("company") or "",
        "first_name": first_name,
        "last_name": last_name,
        "position": position,
        "username": username,
        "email": email,
        "password": password,
        "role": "sub_user",
        "company_admin_id": str(cu.get("id")),
        "active": True,
        # Wallet is shared via company_admin_id; keep local values for compatibility only.
        "credit": 0,
        "cash": 0,
        "preferred_payment": (cu.get("preferred_payment") if (cu.get("preferred_payment") in ("cash", "credit")) else "cash"),
        "apis": inherited_apis,
        "phone": "",
        "contact": "",
        "employees": [],
        "commission": cu.get("commission") if isinstance(cu.get("commission"), list) else [],
        "markup": cu.get("markup") if isinstance(cu.get("markup"), list) else [],
    }
    users.append(sub)
    _save_users(users)
    return RedirectResponse(url=str(request.url_for("company_admin")), status_code=303)


@app.post("/company-admin/users/{sub_id}/toggle", name="company_admin_sub_user_toggle")
async def company_admin_sub_user_toggle(request: Request, sub_id: str):
    cu = _get_current_user(request)
    if not cu:
        return RedirectResponse(url="/login", status_code=303)

    users = _load_users()
    su = _find_user(users, sub_id)
    if not su or not _is_sub_user(su):
        return RedirectResponse(url=str(request.url_for("company_admin")), status_code=303)

    # Only the owning company admin (or super admin) can toggle.
    if not _is_super_admin(cu):
        if str(su.get("company_admin_id") or "") != str(cu.get("id")):
            return RedirectResponse(url=str(request.url_for("company_admin")), status_code=303)

    su["active"] = not bool(su.get("active"))
    _save_users(users)
    return RedirectResponse(url=str(request.url_for("company_admin")), status_code=303)


@app.post("/company-admin/users/{sub_id}/update", name="company_admin_sub_user_update")
async def company_admin_sub_user_update(request: Request, sub_id: str):
    cu = _get_current_user(request)
    if not cu:
        return RedirectResponse(url="/login", status_code=303)

    form = await request.form()
    first_name = str(form.get("first_name") or "").strip()
    last_name = str(form.get("last_name") or "").strip()
    position = str(form.get("position") or "").strip()
    username = str(form.get("username") or "").strip()
    email = str(form.get("email") or "").strip()
    new_password = str(form.get("new_password") or "").strip()
    active = str(form.get("active") or "1").strip()

    users = _load_users()
    su = _find_user(users, sub_id)
    if not su or not _is_sub_user(su):
        return RedirectResponse(url=str(request.url_for("company_admin")), status_code=303)

    # Only the owning company admin (or super admin) can update.
    if not _is_super_admin(cu):
        if str(su.get("company_admin_id") or "") != str(cu.get("id")):
            return RedirectResponse(url=str(request.url_for("company_admin")), status_code=303)

    if not first_name or not last_name or not username:
        return _render(
            request,
            "admin/company_admin.html",
            {
                "subusers": [u for u in users if isinstance(u, dict) and str(u.get("company_admin_id") or "") == str(cu.get("id"))],
                "err": "Please fill first name, last name, and username.",
            },
        )

    uname_l = username.lower()
    for x in users:
        if not isinstance(x, dict):
            continue
        if str(x.get("id")) == str(sub_id):
            continue
        if (x.get("username") or "").strip().lower() == uname_l:
            return _render(
                request,
                "admin/company_admin.html",
                {
                    "subusers": [u for u in users if isinstance(u, dict) and str(u.get("company_admin_id") or "") == str(cu.get("id"))],
                    "err": "This username is already taken.",
                },
            )

    su["first_name"] = first_name
    su["last_name"] = last_name
    su["position"] = position
    su["username"] = username
    su["email"] = email
    su["active"] = False if active in ("0", "false", "False") else True
    if new_password:
        su["password"] = new_password

    # Keep inherited settings in sync with company admin.
    company_admin_id = su.get("company_admin_id")
    if company_admin_id:
        ca = _find_user(users, str(company_admin_id))
        if ca:
            su["company_name"] = ca.get("company_name") or ""
            su["apis"] = _normalize_api_ids(ca.get("apis"))
            su["preferred_payment"] = (ca.get("preferred_payment") if (ca.get("preferred_payment") in ("cash", "credit")) else "cash")
            su["commission"] = ca.get("commission") if isinstance(ca.get("commission"), list) else []
            su["markup"] = ca.get("markup") if isinstance(ca.get("markup"), list) else []

    _save_users(users)
    return RedirectResponse(url=str(request.url_for("company_admin")), status_code=303)



@app.get("/pending", response_class=HTMLResponse, name="pending")
def pending(request: Request):
    _ensure_product_allowed("admin")
    # Super admin-only route (also enforced by middleware)
    items = _load_pending()
    users = _load_users()
    txs = _load_transactions()

    # Enrich with usernames for display
    def _u_name(uid: str):
        u = _find_user(users, str(uid))
        if not u:
            return ""
        return u.get("username") or u.get("email") or str(uid)

    # Build a lookup of pending_id -> transaction (for details display)
    tx_by_pending: dict[str, dict] = {}
    for t in txs:
        if not isinstance(t, dict):
            continue
        pid = str(t.get("pending_id") or "").strip()
        if pid:
            tx_by_pending[pid] = t

    for it in items:
        if isinstance(it, dict):
            it["requested_by_name"] = _u_name(it.get("requested_by_user_id"))
            it["company_admin_name"] = _u_name(it.get("company_admin_id"))

            # Attach transaction snapshot/details (we no longer store raw payload in pending)
            tx = tx_by_pending.get(str(it.get("id") or "").strip())
            if isinstance(tx, dict):
                it["tx"] = tx
                # Convenience copies for table columns
                it["airline"] = tx.get("airline") or ""
                it["from"] = tx.get("from") or ""
                it["to"] = tx.get("to") or ""
                it["date"] = tx.get("date") or tx.get("ts") or ""
                it["price"] = tx.get("price")
                it["currency"] = tx.get("currency") or ""
                it["passenger"] = (
                    (tx.get("first_name") or "") + (" " if (tx.get("first_name") and tx.get("last_name")) else "") + (tx.get("last_name") or "")
                ).strip()
            else:
                it["tx"] = {}
                it["airline"] = ""
                it["from"] = ""
                it["to"] = ""
                it["date"] = it.get("created_at") or ""
                it["price"] = None
                it["currency"] = ""
                it["passenger"] = ""

    return _render(request, "admin/pending_super_admin.html", {"pending_items": items})



@app.post("/pending/{pending_id}/complete", name="pending_complete")
async def pending_complete(
    request: Request,
    pending_id: str,
    ticket_number: str = Form(""),
    booking_code: str = Form(""),
):
    # Super admin-only route (also enforced by middleware)
    pending_id = str(pending_id or "").strip()
    ticket_number = str(ticket_number or "").strip()
    booking_code = str(booking_code or "").strip()

    items = _load_pending()
    found_idx = None
    for i, it in enumerate(items):
        if isinstance(it, dict) and str(it.get("id") or "") == pending_id:
            found_idx = i
            break

    if found_idx is None:
        return RedirectResponse(url=str(request.url_for("pending")), status_code=303)

    # Remove from pending list once completed (user request)
    try:
        items.pop(found_idx)
    except Exception:
        pass
    _save_pending(items)

    # Merge into transaction details (so both admin + user expand views show the updated refs)
    txs = _load_transactions()
    existing_details = {}
    for t in txs:
        if isinstance(t, dict) and str(t.get("pending_id") or "") == pending_id:
            d = t.get("details")
            if isinstance(d, dict):
                existing_details = d
            break

    merged_details = dict(existing_details or {})
    if ticket_number:
        merged_details["pnr"] = ticket_number
        merged_details["ticket_number"] = ticket_number
    if booking_code:
        merged_details["booking_code"] = booking_code

    # Deduct balance (cash/credit) upon manual completion so totals match the normal ticketing flow.
    tx_match = None
    for t in txs:
        if isinstance(t, dict) and str(t.get("pending_id") or "") == pending_id:
            tx_match = t
            break

    try:
        pm = ""
        if isinstance(tx_match, dict):
            d = tx_match.get("details")
            if isinstance(d, dict):
                pm = str(d.get("payment_method") or d.get("pay_method") or "").strip().lower()

        amt = None
        if isinstance(tx_match, dict):
            amt = tx_match.get("price")
        amount = float(_to_number(amt, 0) or 0)

        if pm in ("cash", "credit") and amount > 0:
            users = _load_users()
            admin_id = str((tx_match or {}).get("company_admin_id") or "")
            bu = _find_user(users, admin_id) if admin_id else None
            if isinstance(bu, dict):
                if pm == "cash":
                    new_cash = float(_to_number(bu.get("cash"), 0) or 0) - float(amount)
                    bu["cash"] = int(new_cash) if int(new_cash) == new_cash else new_cash
                elif pm == "credit":
                    new_credit = float(_to_number(bu.get("credit"), 0) or 0) - float(amount)
                    bu["credit"] = int(new_credit) if int(new_credit) == new_credit else new_credit
                _save_users(users)
    except Exception:
        pass

    # Promote the matching transaction to Successful.
    _update_transaction_by_pending_id(
        pending_id,
        {
            "status": "successful",
            "pnr": ticket_number,
            "booking_code": booking_code,
            "details": merged_details,
        },
    )

    return RedirectResponse(url=str(request.url_for("pending")), status_code=303)


    found["status"] = "successful"
    found["ticket_number"] = ticket_number
    found["booking_code"] = booking_code
    found["completed_at"] = datetime.utcnow().isoformat() + "Z"
    # Ensure payload isn't persisted/displayed
    found["payload"] = {}

    _save_pending(items)

    # Promote the matching transaction to Successful.
    _update_transaction_by_pending_id(
        pending_id,
        {
            "status": "successful",
            "pnr": ticket_number,
            "booking_code": booking_code,
        },
    )

    return RedirectResponse(url=str(request.url_for("pending")), status_code=303)


@app.get("/permissions", response_class=HTMLResponse, name="permissions")
def permissions(request: Request):
    _ensure_product_allowed("admin")
    # Backward-compatible alias; keep a single source of truth under /admin/permissions
    return RedirectResponse(url=str(request.url_for("admin_permissions")), status_code=303)


@app.get("/permissions-data")
def permissions_data():
    _ensure_product_allowed("admin")
    try:
        r = requests.get(_backend_url("/api/permissions"), timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/other-apis-data/fib")
def other_apis_fib_data():
    _ensure_product_allowed("admin")
    try:
        r = requests.get(_backend_url("/api/other-apis/fib"), timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/permissions-status")
def permissions_status():
    _ensure_product_allowed("admin")
    try:
        r = requests.get(_backend_url("/api/permissions/status"), timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.post("/permissions-data")
async def permissions_data_save(request: Request):
    _ensure_product_allowed("admin")
    try:
        payload = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400)

    try:
        r = requests.post(_backend_url("/api/permissions"), json=payload, timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.post("/other-apis-data/fib")
async def other_apis_fib_data_save(request: Request):
    _ensure_product_allowed("admin")
    try:
        payload = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400)

    try:
        r = requests.post(_backend_url("/api/other-apis/fib"), json=payload, timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.post("/other-apis-data/fib/create-payment")
async def other_apis_fib_create_payment(request: Request):
    _ensure_product_allowed("admin")
    try:
        payload = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400)

    try:
        r = requests.post(_backend_url("/api/other-apis/fib/create-payment"), json=payload, timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/other-apis-data/esim")
def other_apis_esim_data():
    _ensure_product_allowed("admin")
    try:
        r = requests.get(_backend_url("/api/other-apis/esim"), timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.post("/other-apis-data/esim")
async def other_apis_esim_data_save(request: Request):
    _ensure_product_allowed("admin")
    try:
        payload = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400)

    try:
        r = requests.post(_backend_url("/api/other-apis/esim"), json=payload, timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/other-apis-data/esim/ping")
def other_apis_esim_ping():
    _ensure_product_allowed("admin")
    try:
        r = requests.get(_backend_url("/api/other-apis/esim/ping"), timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.get("/other-apis-data/esim/balance")
def other_apis_esim_balance():
    _ensure_product_allowed("admin")
    try:
        r = requests.get(_backend_url("/api/esim/balance"), timeout=30)
        return Response(content=r.text, status_code=r.status_code, media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=str(e), status_code=502)


@app.post("/fib/webhook")
async def fib_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    # For now we simply acknowledge receipt.
    return Response(content=json.dumps({"status": "ok"}), media_type="application/json")


@app.get("/fib/return")
def fib_return():
    return Response(content="Payment completed. You can return to the app.", media_type="text/plain")


@app.get("/admin", response_class=HTMLResponse, name="admin")
def admin(request: Request):
    _ensure_product_allowed("admin")
    return _render(request, "admin/index.html", {})


@app.get("/admin/permissions", response_class=HTMLResponse, name="admin_permissions")
def admin_permissions(request: Request):
    return _render(request, "admin/permissions.html", {})


@app.get("/admin/other-apis", response_class=HTMLResponse, name="admin_other_apis")
def admin_other_apis(request: Request):
    return _render(request, "admin/other_apis.html", {})


@app.get("/admin/announcements", response_class=HTMLResponse, name="admin_announcements")
def admin_announcements(request: Request):
    cu = _get_current_user(request)
    if not cu or not _is_super_admin(cu):
        return Response(content="Forbidden", status_code=403)
    items = _load_announcements()
    now = datetime.now()
    # newest first
    items = list(items)
    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    for a in items:
        a["status"] = _announcement_status(a, now)
    return _render(request, "admin/announcements.html", {"announcements": items})


@app.post("/admin/announcements", name="admin_announcements_create")
async def admin_announcements_create(request: Request):
    cu = _get_current_user(request)
    if not cu or not _is_super_admin(cu):
        return Response(content="Forbidden", status_code=403)

    form = await request.form()
    title = str(form.get("title") or "").strip()
    start_date = str(form.get("start_date") or "").strip()
    start_time = str(form.get("start_time") or "").strip()
    end_date = str(form.get("end_date") or "").strip()
    end_time = str(form.get("end_time") or "").strip()
    kind = str(form.get("type") or "General").strip() or "General"
    description = str(form.get("description") or "").strip()
    active = True if str(form.get("active") or "") in ("on", "true", "1") else False

    # Simple validation
    if not title or not start_date or not start_time or not description:
        return RedirectResponse(url=str(request.url_for("admin_announcements")), status_code=303)
    if len(title) > 100:
        title = title[:100]

    items = _load_announcements()
    items.insert(
        0,
        {
            "id": uuid.uuid4().hex,
            "title": title,
            "start_date": start_date,
            "start_time": start_time,
            "end_date": end_date,
            "end_time": end_time,
            "type": kind,
            "description": description,
            "active": active,
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )
    _save_announcements(items)
    return RedirectResponse(url=str(request.url_for("admin_announcements")), status_code=303)


@app.post("/admin/announcements/{ann_id}/update", name="admin_announcements_update")
async def admin_announcements_update(request: Request, ann_id: str):
    cu = _get_current_user(request)
    if not cu or not _is_super_admin(cu):
        return Response(content="Forbidden", status_code=403)

    form = await request.form()
    title = str(form.get("title") or "").strip()
    start_date = str(form.get("start_date") or "").strip()
    start_time = str(form.get("start_time") or "").strip()
    end_date = str(form.get("end_date") or "").strip()
    end_time = str(form.get("end_time") or "").strip()
    kind = str(form.get("type") or "General").strip() or "General"
    description = str(form.get("description") or "").strip()
    active = True if str(form.get("active") or "") in ("on", "true", "1") else False

    items = _load_announcements()
    for a in items:
        if str(a.get("id")) == str(ann_id):
            if title:
                a["title"] = title[:100]
            if start_date:
                a["start_date"] = start_date
            if start_time:
                a["start_time"] = start_time
            a["end_date"] = end_date
            a["end_time"] = end_time
            a["type"] = kind
            if description:
                a["description"] = description
            a["active"] = active
            a["updated_at"] = datetime.utcnow().isoformat() + "Z"
            break
    _save_announcements(items)
    return RedirectResponse(url=str(request.url_for("admin_announcements")), status_code=303)


@app.post("/admin/announcements/{ann_id}/delete", name="admin_announcements_delete")
async def admin_announcements_delete(request: Request, ann_id: str):
    cu = _get_current_user(request)
    if not cu or not _is_super_admin(cu):
        return Response(content="Forbidden", status_code=403)

    items = _load_announcements()
    items = [a for a in items if str(a.get("id")) != str(ann_id)]
    _save_announcements(items)
    return RedirectResponse(url=str(request.url_for("admin_announcements")), status_code=303)


# --- Users: list page ---
@app.get("/admin/users", response_class=HTMLResponse, name="admin_users")
def admin_users(request: Request):
    users = _load_users()
    # Company users only (exclude super admin + sub users)
    companies = [u for u in users if isinstance(u, dict) and _is_company_admin(u)]
    return _render(request, "admin/users.html", {"users": companies})


@app.get("/admin/users/api/list", name="admin_users_api_list")
def admin_users_api_list(request: Request):
    cu = _get_current_user(request)
    if not cu or not _is_super_admin(cu):
        return Response(content=json.dumps({"status": "error", "error": "Forbidden."}), status_code=403, media_type="application/json")
    users = _load_users()
    out = []
    for u in users:
        if not isinstance(u, dict):
            continue
        if not _is_company_admin(u):
            continue
        label = u.get("company_name") or u.get("username") or u.get("email") or u.get("id")
        out.append(
            {
                "id": str(u.get("id") or ""),
                "label": str(label or ""),
                "email": str(u.get("email") or ""),
                "role": str(u.get("role") or ""),
                "active": bool(u.get("active", True)),
            }
        )
    return Response(content=json.dumps({"status": "ok", "users": out}), media_type="application/json")


# --- Users: create ---
@app.post("/admin/users", name="admin_users_create")
async def admin_users_create(request: Request):
    form = await request.form()

    company_name = str(form.get("company_name") or "").strip()
    username = str(form.get("username") or "").strip()
    email = str(form.get("email") or "").strip()
    password = str(form.get("password") or "").strip()
    phone = str(form.get("phone") or "").strip()
    credit = _to_number(form.get("credit"), 0)
    cash = _to_number(form.get("cash"), 0)
    preferred_payment = str(form.get("preferred_payment") or "cash").strip() or "cash"

    # Minimal validation: template already requires company_name/username/password
    if not company_name or not username or not password:
        return RedirectResponse(url=str(request.url_for("admin_users")), status_code=303)

    users = _load_users()

    # Prevent duplicate usernames (simple rule)
    for u in users:
        if isinstance(u, dict) and str(u.get("username", "")).strip().lower() == username.lower():
            # If duplicate, just go back without changing anything.
            return RedirectResponse(url=str(request.url_for("admin_users")), status_code=303)

    new_user = {
        "id": uuid.uuid4().hex,
        "company_name": company_name,
        "username": username,
        "email": email,
        "password": password,
        "phone": phone,
        "credit": credit,
        "cash": cash,
        "preferred_payment": preferred_payment if preferred_payment in ("cash", "credit") else "cash",
        "active": True,
        "apis": [],
        "audit": [],
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    users.append(new_user)
    _save_users(users)

    return RedirectResponse(url=str(request.url_for("admin_users")), status_code=303)


# --- Users: detail page (User actions) ---
@app.get("/admin/users/{user_id}", response_class=HTMLResponse, name="admin_user_detail")
def admin_user_detail(request: Request, user_id: str):
    users = _load_users()
    u = _find_user(users, user_id)
    providers = _load_providers()
    subusers = [x for x in users if isinstance(x, dict) and str(x.get("company_admin_id") or "") == str(user_id)]
    return _render(request, "admin/user_detail.html", {"u": u, "providers": providers, "subusers": subusers})


# --- Users: update profile (company users) ---
@app.post("/admin/users/{user_id}", name="admin_user_update")
async def admin_user_update(request: Request, user_id: str):
    form = await request.form()

    company_name = str(form.get("company_name") or "").strip()
    username = str(form.get("username") or "").strip()
    email = str(form.get("email") or "").strip()
    phone = str(form.get("phone") or "").strip()
    active = str(form.get("active") or "1").strip()
    new_password = str(form.get("new_password") or "").strip()

    if not company_name or not username:
        return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)

    users = _load_users()
    u = _find_user(users, user_id)
    if not u:
        return RedirectResponse(url=str(request.url_for("admin_users")), status_code=303)

    # Prevent duplicate usernames across all users (including sub users)
    uname_l = username.lower()
    for x in users:
        if not isinstance(x, dict):
            continue
        if str(x.get("id")) == str(user_id):
            continue
        if (x.get("username") or "").strip().lower() == uname_l:
            return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)

    u["company_name"] = company_name
    u["username"] = username
    u["email"] = email
    u["phone"] = phone
    u["contact"] = u.get("contact") or phone or ""
    u["active"] = False if active in ("0", "false", "False") else True
    if new_password:
        u["password"] = new_password
    # Keep vendor name synced if missing.
    if not u.get("vendor_name"):
        u["vendor_name"] = u.get("company_name") or u.get("username") or ""

    # If company settings change, keep sub users in sync for the fields they inherit.
    if _is_company_admin(u):
        inherited_apis = _normalize_api_ids(u.get("apis"))
        for su in users:
            if not isinstance(su, dict):
                continue
            if str(su.get("company_admin_id") or "") != str(u.get("id")):
                continue
            su["company_name"] = u.get("company_name") or ""
            su["apis"] = inherited_apis
            su["preferred_payment"] = (u.get("preferred_payment") if (u.get("preferred_payment") in ("cash", "credit")) else "cash")
            su["commission"] = u.get("commission") if isinstance(u.get("commission"), list) else []
            su["markup"] = u.get("markup") if isinstance(u.get("markup"), list) else []

    _save_users(users)
    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


# --- Users: delete company (cascade delete sub users) ---
@app.post("/admin/users/{user_id}/delete", name="admin_user_delete")
async def admin_user_delete(request: Request, user_id: str):
    users = _load_users()
    # Remove the company and all its sub users.
    kept: list[dict] = []
    for u in users:
        if not isinstance(u, dict):
            continue
        if str(u.get("id")) == str(user_id):
            continue
        if str(u.get("company_admin_id") or "") == str(user_id):
            continue
        kept.append(u)
    _save_users(kept)
    return RedirectResponse(url=str(request.url_for("admin_users")), status_code=303)


# --- Sub Users: list/manage ---
@app.get("/admin/sub-users", response_class=HTMLResponse, name="admin_sub_users")
def admin_sub_users(request: Request):
    users = _load_users()
    companies = [u for u in users if isinstance(u, dict) and _is_company_admin(u)]
    subs = [u for u in users if isinstance(u, dict) and _is_sub_user(u)]

    company_filter = str(request.query_params.get("company") or "").strip()
    if company_filter:
        subs = [s for s in subs if str(s.get("company_admin_id") or "") == company_filter]

    edit_id = str(request.query_params.get("edit") or "").strip()
    edit_user = None
    if edit_id:
        for s in subs:
            if str(s.get("id")) == edit_id:
                edit_user = s
                break
        if not edit_user:
            # If the edit user is not in the filtered list, search globally.
            for s in users:
                if isinstance(s, dict) and str(s.get("id")) == edit_id and _is_sub_user(s):
                    edit_user = s
                    break

    return _render(
        request,
        "admin/sub_users.html",
        {
            "companies": companies,
            "subusers": subs,
            "company_filter": company_filter,
            "edit_user": edit_user,
        },
    )


@app.post("/admin/sub-users/{sub_id}/toggle", name="admin_sub_user_toggle")
async def admin_sub_user_toggle(request: Request, sub_id: str):
    users = _load_users()
    su = _find_user(users, sub_id)
    if su and _is_sub_user(su):
        su["active"] = not bool(su.get("active"))
        _save_users(users)
    return RedirectResponse(url=str(request.url_for("admin_sub_users")), status_code=303)


@app.post("/admin/sub-users/{sub_id}/update", name="admin_sub_user_update")
async def admin_sub_user_update(request: Request, sub_id: str):
    form = await request.form()
    first_name = str(form.get("first_name") or "").strip()
    last_name = str(form.get("last_name") or "").strip()
    position = str(form.get("position") or "").strip()
    username = str(form.get("username") or "").strip()
    email = str(form.get("email") or "").strip()
    new_password = str(form.get("new_password") or "").strip()
    active = str(form.get("active") or "1").strip()

    users = _load_users()
    su = _find_user(users, sub_id)
    if not su or not _is_sub_user(su):
        return RedirectResponse(url=str(request.url_for("admin_sub_users")), status_code=303)

    if not first_name or not last_name or not username:
        return RedirectResponse(url=str(request.url_for("admin_sub_users", edit=sub_id)), status_code=303)

    uname_l = username.lower()
    for x in users:
        if not isinstance(x, dict):
            continue
        if str(x.get("id")) == str(sub_id):
            continue
        if (x.get("username") or "").strip().lower() == uname_l:
            return RedirectResponse(url=str(request.url_for("admin_sub_users", edit=sub_id)), status_code=303)

    su["first_name"] = first_name
    su["last_name"] = last_name
    su["position"] = position
    su["username"] = username
    su["email"] = email
    su["active"] = False if active in ("0", "false", "False") else True
    if new_password:
        su["password"] = new_password

    # Keep inherited settings in sync with company admin.
    company_admin_id = su.get("company_admin_id")
    if company_admin_id:
        ca = _find_user(users, str(company_admin_id))
        if ca:
            su["company_name"] = ca.get("company_name") or ""
            su["apis"] = _normalize_api_ids(ca.get("apis"))
            su["preferred_payment"] = (ca.get("preferred_payment") if (ca.get("preferred_payment") in ("cash", "credit")) else "cash")
            su["commission"] = ca.get("commission") if isinstance(ca.get("commission"), list) else []
            su["markup"] = ca.get("markup") if isinstance(ca.get("markup"), list) else []

    _save_users(users)
    return RedirectResponse(url=str(request.url_for("admin_sub_users")), status_code=303)


@app.post("/admin/sub-users/{sub_id}/delete", name="admin_sub_user_delete")
async def admin_sub_user_delete(request: Request, sub_id: str):
    users = _load_users()
    kept: list[dict] = []
    for u in users:
        if not isinstance(u, dict):
            continue
        if str(u.get("id")) == str(sub_id) and _is_sub_user(u):
            continue
        kept.append(u)
    _save_users(kept)
    return RedirectResponse(url=str(request.url_for("admin_sub_users")), status_code=303)


# --- Users: activate/deactivate ---
@app.post("/admin/users/{user_id}/toggle", name="admin_user_toggle")
async def admin_user_toggle(request: Request, user_id: str):
    users = _load_users()
    u = _find_user(users, user_id)
    if u:
        u["active"] = not bool(u.get("active"))
        _save_users(users)
    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


# --- Users: enable/disable provider access ---
@app.post("/admin/users/{user_id}/api", name="admin_user_api")
async def admin_user_api(request: Request, user_id: str):
    form = await request.form()
    provider_id = str(form.get("provider_id") or "").strip().lower()

    users = _load_users()
    u = _find_user(users, user_id)
    if u and provider_id:
        apis = _normalize_api_ids(u.get("apis"))
        if provider_id in apis:
            apis = [x for x in apis if x != provider_id]
        else:
            apis.append(provider_id)
        u["apis"] = _normalize_api_ids(apis)

        # Keep sub users in sync with company settings.
        if _is_company_admin(u):
            for su in users:
                if not isinstance(su, dict):
                    continue
                if str(su.get("company_admin_id") or "") != str(u.get("id")):
                    continue
                su["apis"] = u["apis"]

        _save_users(users)

    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


# --- Users: change balance (credit/cash) ---
@app.post("/admin/users/{user_id}/balance", name="admin_user_balance")
async def admin_user_balance(request: Request, user_id: str):
    form = await request.form()
    kind = str(form.get("kind") or "credit").strip().lower()
    direction = str(form.get("direction") or "increase").strip().lower()
    amount = _to_number(form.get("amount"), 0)
    reference = str(form.get("reference") or "").strip()
    notes = str(form.get("notes") or "").strip()

    if kind not in ("credit", "cash"):
        kind = "credit"
    if direction not in ("increase", "decrease"):
        direction = "increase"
    if amount is None:
        amount = 0
    try:
        amt = float(amount)
    except Exception:
        amt = 0.0

    users = _load_users()
    u = _find_user(users, user_id)
    if u and amt != 0:
        cur = _to_number(u.get(kind), 0)
        try:
            cur_f = float(cur)
        except Exception:
            cur_f = 0.0

        if direction == "decrease":
            new_val = cur_f - amt
        else:
            new_val = cur_f + amt

        # Keep it simple: allow negative if needed (can be prevented later)
        if int(new_val) == new_val:
            u[kind] = int(new_val)
        else:
            u[kind] = new_val

        audit = u.get("audit")
        if not isinstance(audit, list):
            audit = []
        audit.append(
            {
                "ts": datetime.utcnow().isoformat() + "Z",
                "action": "balance",
                "kind": kind,
                "direction": direction,
                "amount": amt,
                "reference": reference,
                "notes": notes,
            }
        )
        u["audit"] = audit
        _save_users(users)

    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


# --- Users: commission rules (flights only for now) ---
@app.post("/admin/users/{user_id}/commission", name="admin_user_commission_add")
async def admin_user_commission_add(request: Request, user_id: str):
    form = await request.form()
    airline_code = str(form.get("airline_code") or "").strip().upper()
    airline_name = str(form.get("airline_name") or "").strip()
    rule_type = str(form.get("rule_type") or "").strip().lower()
    amount_raw = form.get("amount")

    if rule_type not in ("percent", "fixed"):
        rule_type = "percent"

    amount = _to_number(amount_raw, None)
    try:
        amount_val = float(amount) if amount is not None else None
    except Exception:
        amount_val = None

    if not airline_code or amount_val is None or amount_val <= 0:
        return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)

    basis = "fare" if rule_type == "percent" else "total"

    users = _load_users()
    u = _find_user(users, user_id)
    if u:
        rules = u.get("commission")
        if not isinstance(rules, list):
            rules = []
        # Replace any existing rule for the same airline + type + service.
        rules = [
            r for r in rules
            if not (
                isinstance(r, dict)
                and str(r.get("service") or "") == "flight"
                and str(r.get("airline_code") or "").upper() == airline_code
                and str(r.get("type") or "").lower() == rule_type
            )
        ]
        rules.append(
            {
                "id": uuid.uuid4().hex,
                "service": "flight",
                "airline_code": airline_code,
                "airline_name": airline_name,
                "type": rule_type,
                "amount": amount_val,
                "basis": basis,
                "created_at": datetime.utcnow().isoformat() + "Z",
            }
        )
        u["commission"] = rules

        # Keep sub users in sync with company settings.
        if _is_company_admin(u):
            for su in users:
                if not isinstance(su, dict):
                    continue
                if str(su.get("company_admin_id") or "") != str(u.get("id")):
                    continue
                su["commission"] = u.get("commission") if isinstance(u.get("commission"), list) else []

        _save_users(users)

    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


@app.post("/admin/users/{user_id}/commission/delete", name="admin_user_commission_delete")
async def admin_user_commission_delete(request: Request, user_id: str):
    form = await request.form()
    rule_id = str(form.get("rule_id") or "").strip()
    if not rule_id:
        return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)

    users = _load_users()
    u = _find_user(users, user_id)
    if u:
        rules = u.get("commission")
        if not isinstance(rules, list):
            rules = []
        rules = [r for r in rules if not (isinstance(r, dict) and str(r.get("id")) == rule_id)]
        u["commission"] = rules

        if _is_company_admin(u):
            for su in users:
                if not isinstance(su, dict):
                    continue
                if str(su.get("company_admin_id") or "") != str(u.get("id")):
                    continue
                su["commission"] = u.get("commission") if isinstance(u.get("commission"), list) else []

        _save_users(users)

    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


# --- Users: markup rules (flights only for now) ---
@app.post("/admin/users/{user_id}/markup", name="admin_user_markup_add")
async def admin_user_markup_add(request: Request, user_id: str):
    form = await request.form()
    airline_code = str(form.get("airline_code") or "").strip().upper()
    airline_name = str(form.get("airline_name") or "").strip()
    rule_type = str(form.get("rule_type") or "").strip().lower()
    amount_raw = form.get("amount")

    if rule_type not in ("percent", "fixed"):
        rule_type = "percent"

    amount = _to_number(amount_raw, None)
    try:
        amount_val = float(amount) if amount is not None else None
    except Exception:
        amount_val = None

    if not airline_code or amount_val is None or amount_val <= 0:
        return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)

    basis = "fare" if rule_type == "percent" else "total"

    users = _load_users()
    u = _find_user(users, user_id)
    if u:
        rules = u.get("markup")
        if not isinstance(rules, list):
            rules = []
        rules = [
            r for r in rules
            if not (
                isinstance(r, dict)
                and str(r.get("service") or "") == "flight"
                and str(r.get("airline_code") or "").upper() == airline_code
                and str(r.get("type") or "").lower() == rule_type
            )
        ]
        rules.append(
            {
                "id": uuid.uuid4().hex,
                "service": "flight",
                "airline_code": airline_code,
                "airline_name": airline_name,
                "type": rule_type,
                "amount": amount_val,
                "basis": basis,
                "created_at": datetime.utcnow().isoformat() + "Z",
            }
        )
        u["markup"] = rules

        if _is_company_admin(u):
            for su in users:
                if not isinstance(su, dict):
                    continue
                if str(su.get("company_admin_id") or "") != str(u.get("id")):
                    continue
                su["markup"] = u.get("markup") if isinstance(u.get("markup"), list) else []

        _save_users(users)

    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


@app.post("/admin/users/{user_id}/markup/delete", name="admin_user_markup_delete")
async def admin_user_markup_delete(request: Request, user_id: str):
    form = await request.form()
    rule_id = str(form.get("rule_id") or "").strip()
    if not rule_id:
        return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)

    users = _load_users()
    u = _find_user(users, user_id)
    if u:
        rules = u.get("markup")
        if not isinstance(rules, list):
            rules = []
        rules = [r for r in rules if not (isinstance(r, dict) and str(r.get("id")) == rule_id)]
        u["markup"] = rules

        if _is_company_admin(u):
            for su in users:
                if not isinstance(su, dict):
                    continue
                if str(su.get("company_admin_id") or "") != str(u.get("id")):
                    continue
                su["markup"] = u.get("markup") if isinstance(u.get("markup"), list) else []

        _save_users(users)

    return RedirectResponse(url=str(request.url_for("admin_user_detail", user_id=user_id)), status_code=303)


@app.get("/credit", response_class=HTMLResponse, name="credit")
def credit(_request: Request):
    return "<h2 style='font-family:system-ui;margin:24px'>Credit (placeholder)</h2>"
