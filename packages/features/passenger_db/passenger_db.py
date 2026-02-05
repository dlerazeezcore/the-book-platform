from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------- Storage ----------
ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data"
PROFILES_PATH = DATA_DIR / "passenger_db" / "profiles.json"
HISTORY_PATH = DATA_DIR / "passenger_db" / "history.json"


def _ensure_files() -> None:
    (DATA_DIR / "passenger_db").mkdir(parents=True, exist_ok=True)
    if not PROFILES_PATH.exists():
        PROFILES_PATH.write_text("[]", encoding="utf-8")
    if not HISTORY_PATH.exists():
        HISTORY_PATH.write_text("[]", encoding="utf-8")


def _load_json(path: Path, default: Any) -> Any:
    try:
        _ensure_files()
        raw = path.read_text(encoding="utf-8")
        return json.loads(raw) if raw.strip() else default
    except Exception:
        return default


def _save_json(path: Path, data: Any) -> None:
    _ensure_files()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_profiles() -> List[Dict[str, Any]]:
    data = _load_json(PROFILES_PATH, [])
    return data if isinstance(data, list) else []


def save_profiles(profiles: List[Dict[str, Any]]) -> None:
    _save_json(PROFILES_PATH, profiles)


def load_history() -> List[Dict[str, Any]]:
    data = _load_json(HISTORY_PATH, [])
    return data if isinstance(data, list) else []


def save_history(history: List[Dict[str, Any]]) -> None:
    _save_json(HISTORY_PATH, history)


# ---------- Domain helpers ----------
def _parse_iso_date(d: str) -> Optional[date]:
    try:
        d = (d or "").strip()
        if not d:
            return None
        # Accept YYYY-MM-DD (primary)
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return None


def age_category(dob_iso: str, on: Optional[date] = None) -> str:
    """
    Adult / Child / Infant:
      - Infant: < 2 years
      - Child:  >= 2 and < 12 years
      - Adult:  >= 12 years
    Computed dynamically, so a passenger can move categories over time.
    """
    on = on or date.today()
    dob = _parse_iso_date(dob_iso)
    if not dob:
        return "unknown"

    # Compute age in years with day precision.
    years = on.year - dob.year - ((on.month, on.day) < (dob.month, dob.day))
    if years < 2:
        return "infant"
    if years < 12:
        return "child"
    return "adult"


def normalize(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def profile_has_user_access(profile: Dict[str, Any], user_id: str) -> bool:
    # Profiles are scoped per company admin owner, but can be shared to assigned users later.
    owner = str(profile.get("owner_user_id") or "")
    allowed = profile.get("allowed_user_ids") or []
    allowed = [str(x) for x in allowed] if isinstance(allowed, list) else []
    return (owner == str(user_id)) or (str(user_id) in allowed)


def find_member_by_passport(
    profiles: List[Dict[str, Any]],
    passport_number: str,
    owner_user_id: Optional[str] = None,
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    passport_number = normalize(passport_number)
    if not passport_number:
        return None
    for p in profiles:
        if owner_user_id and str(p.get("owner_user_id") or "") != str(owner_user_id):
            continue
        for m in (p.get("members") or []):
            for doc in (m.get("passports") or []):
                if normalize(str(doc.get("number") or "")) == passport_number:
                    return p, m
            # Backward compatibility: some flows may store single string passport.
            if normalize(str(m.get("passport_number") or "")) == passport_number:
                return p, m
    return None


def find_members_by_query(profiles: List[Dict[str, Any]], q: str) -> List[Dict[str, Any]]:
    qn = normalize(q)
    if not qn:
        return []

    out: List[Dict[str, Any]] = []
    for p in profiles:
        # profile fields
        if qn in normalize(str(p.get("phone") or "")) or qn in normalize(str(p.get("label") or "")):
            out.append(p)
            continue

        # member fields
        hit = False
        for m in (p.get("members") or []):
            if qn in normalize(str(m.get("first_name") or "")) or qn in normalize(str(m.get("last_name") or "")):
                hit = True
            if qn in normalize(str(m.get("nationality") or "")) or qn in normalize(str(m.get("national_id_number") or "")):
                hit = True
            for doc in (m.get("passports") or []):
                if qn in normalize(str(doc.get("number") or "")):
                    hit = True
            if hit:
                break
        if hit:
            out.append(p)

    return out


def create_profile(owner_user_id: str, label: str = "", phone: str = "", allowed_user_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "id": _new_id("prof"),
        "owner_user_id": str(owner_user_id),
        "label": (label or "").strip(),
        "phone": (phone or "").strip(),
        "allowed_user_ids": [str(x) for x in (allowed_user_ids or [])],
        "members": [],
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


def create_member(data: Dict[str, Any]) -> Dict[str, Any]:
    # Passports: store as list; supports renewed passports and multiple nationalities.
    passports = data.get("passports")
    if not isinstance(passports, list):
        passports = []

    # If caller provided single passport fields, fold them in.
    pn = (data.get("passport_number") or "").strip()
    if pn:
        passports.append(
            {
                "number": pn,
                "issue_date": (data.get("passport_issue_date") or "").strip(),
                "expiry_date": (data.get("passport_expiry_date") or "").strip(),
                "issue_place": (data.get("passport_issue_place") or "").strip(),
            }
        )

    # De-duplicate by passport number.
    seen = set()
    deduped = []
    for doc in passports:
        num = normalize(str(doc.get("number") or ""))
        if not num or num in seen:
            continue
        seen.add(num)
        deduped.append(
            {
                "number": (doc.get("number") or "").strip(),
                "issue_date": (doc.get("issue_date") or "").strip(),
                "expiry_date": (doc.get("expiry_date") or "").strip(),
                "issue_place": (doc.get("issue_place") or "").strip(),
            }
        )

    return {
        "id": _new_id("mem"),
        "title": (data.get("title") or "").strip(),
        "first_name": (data.get("first_name") or "").strip(),
        "last_name": (data.get("last_name") or "").strip(),
        "dob": (data.get("dob") or "").strip(),  # ISO YYYY-MM-DD recommended
        "nationality": (data.get("nationality") or "").strip(),
        "national_id_number": (data.get("national_id_number") or "").strip(),
        "phone": (data.get("phone") or "").strip(),  # optional per member
        "passports": deduped,
        "notes": (data.get("notes") or "").strip(),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


def upsert_member_passport(member: Dict[str, Any], passport: Dict[str, Any]) -> None:
    docs = member.get("passports") or []
    if not isinstance(docs, list):
        docs = []
    num = (passport.get("number") or "").strip()
    if not num:
        return
    num_n = normalize(num)
    updated = False
    for d in docs:
        if normalize(str(d.get("number") or "")) == num_n:
            d["issue_date"] = (passport.get("issue_date") or d.get("issue_date") or "").strip()
            d["expiry_date"] = (passport.get("expiry_date") or d.get("expiry_date") or "").strip()
            d["issue_place"] = (passport.get("issue_place") or d.get("issue_place") or "").strip()
            updated = True
            break
    if not updated:
        docs.append(
            {
                "number": num,
                "issue_date": (passport.get("issue_date") or "").strip(),
                "expiry_date": (passport.get("expiry_date") or "").strip(),
                "issue_place": (passport.get("issue_place") or "").strip(),
            }
        )
    member["passports"] = docs
    member["updated_at"] = datetime.utcnow().isoformat() + "Z"


def compute_view_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    # Add computed age category per member, without mutating the stored record.
    out = json.loads(json.dumps(profile))
    for m in out.get("members") or []:
        m["age_category"] = age_category(str(m.get("dob") or ""))
        # Provide a convenience primary passport number if present.
        docs = m.get("passports") or []
        m["primary_passport_number"] = str(docs[0].get("number") or "") if isinstance(docs, list) and docs else ""
    return out


# ---------- History ----------
def add_history_event(
    owner_user_id: str,
    profile_id: str,
    member_id: str,
    kind: str,
    details: Dict[str, Any],
) -> Dict[str, Any]:
    history = load_history()
    ev = {
        "id": _new_id("hist"),
        "owner_user_id": str(owner_user_id),
        "profile_id": str(profile_id),
        "member_id": str(member_id),
        "kind": str(kind),  # flight / hotel / esim / visa / etc
        "details": details if isinstance(details, dict) else {},
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    history.append(ev)
    save_history(history)
    return ev


def history_for_member(owner_user_id: str, member_id: str) -> List[Dict[str, Any]]:
    history = load_history()
    out = []
    for ev in history:
        if str(ev.get("owner_user_id") or "") != str(owner_user_id):
            continue
        if str(ev.get("member_id") or "") == str(member_id):
            out.append(ev)
    # newest first
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out


def _history_has_esim(owner_user_id: str, member_id: str, order_reference: str) -> bool:
    if not order_reference:
        return False
    history = load_history()
    for ev in history:
        if str(ev.get("owner_user_id") or "") != str(owner_user_id):
            continue
        if str(ev.get("member_id") or "") != str(member_id):
            continue
        if str(ev.get("kind") or "") != "esim":
            continue
        details = ev.get("details") or {}
        if str(details.get("order_reference") or "") == str(order_reference):
            return True
    return False


def backfill_esim_history_for_member(
    owner_user_id: str,
    profile: Dict[str, Any],
    member: Dict[str, Any],
    orders: List[Dict[str, Any]],
) -> int:
    """
    Attach existing eSIM orders to passenger history if they match and are missing.
    Returns how many events were added.
    """
    if not isinstance(orders, list):
        return 0

    fn = normalize(str(member.get("first_name") or ""))
    ln = normalize(str(member.get("last_name") or ""))
    mem_phone = normalize(str(member.get("phone") or ""))
    prof_phone = normalize(str(profile.get("phone") or ""))

    if not fn and not ln:
        return 0

    added = 0
    for o in orders:
        if not isinstance(o, dict):
            continue
        customer_name = str(o.get("customer_name") or "")
        customer_phone = normalize(str(o.get("customer_phone") or ""))
        ofn, oln = _split_name(customer_name)
        if normalize(ofn) != fn or normalize(oln) != ln:
            continue
        if customer_phone:
            if customer_phone != mem_phone and customer_phone != prof_phone:
                continue
        # Deduplicate by order reference when present.
        order_ref = str(o.get("order_reference") or "")
        if order_ref and _history_has_esim(owner_user_id, str(member.get("id")), order_ref):
            continue

        details = {
            "bundle_name": o.get("bundle_name") or "",
            "bundle_description": o.get("bundle_description") or "",
            "country_name": o.get("country_name") or "",
            "country_iso": o.get("country_iso") or "",
            "quantity": o.get("quantity") or 1,
            "total_iqd": o.get("total_iqd"),
            "currency": o.get("currency") or "IQD",
            "status": o.get("status") or "",
            "order_reference": order_ref,
            "company_name": o.get("company_name") or "",
            "agent_name": o.get("agent_name") or "",
            "customer_phone": o.get("customer_phone") or "",
        }

        add_history_event(
            owner_user_id=owner_user_id,
            profile_id=str(profile.get("id")),
            member_id=str(member.get("id")),
            kind="esim",
            details=details,
        )
        added += 1

    return added


def _split_name(full: str) -> tuple[str, str]:
    full = (full or "").strip()
    if not full:
        return "", ""
    parts = [p for p in full.split(" ") if p.strip()]
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def find_member_by_name_phone(
    profiles: List[Dict[str, Any]],
    owner_user_id: str,
    first_name: str,
    last_name: str,
    phone: str = "",
) -> Optional[tuple[Dict[str, Any], Dict[str, Any]]]:
    fn = normalize(first_name)
    ln = normalize(last_name)
    ph = normalize(phone)
    if not fn and not ln:
        return None
    for p in profiles:
        if str(p.get("owner_user_id") or "") != str(owner_user_id):
            continue
        prof_phone = normalize(str(p.get("phone") or ""))
        for m in (p.get("members") or []):
            if normalize(str(m.get("first_name") or "")) != fn:
                continue
            if normalize(str(m.get("last_name") or "")) != ln:
                continue
            if ph:
                mem_phone = normalize(str(m.get("phone") or ""))
                if mem_phone != ph and prof_phone != ph:
                    continue
            return p, m
    return None


def find_member_by_name_only(
    profiles: List[Dict[str, Any]],
    owner_user_id: str,
    first_name: str,
    last_name: str,
) -> Optional[tuple[Dict[str, Any], Dict[str, Any]]]:
    fn = normalize(first_name)
    ln = normalize(last_name)
    if not fn and not ln:
        return None
    matches: List[tuple[Dict[str, Any], Dict[str, Any]]] = []
    for p in profiles:
        if str(p.get("owner_user_id") or "") != str(owner_user_id):
            continue
        for m in (p.get("members") or []):
            if normalize(str(m.get("first_name") or "")) != fn:
                continue
            if normalize(str(m.get("last_name") or "")) != ln:
                continue
            matches.append((p, m))
    if len(matches) == 1:
        return matches[0]
    return None


def attach_esim_to_passenger(
    owner_user_id: str,
    customer_name: str,
    customer_phone: str,
    details: Dict[str, Any],
    *,
    allow_create: bool = False,
) -> Optional[Dict[str, Any]]:
    profiles = load_profiles()
    first_name, last_name = _split_name(customer_name)
    found = find_member_by_name_phone(
        profiles,
        owner_user_id=owner_user_id,
        first_name=first_name,
        last_name=last_name,
        phone=customer_phone,
    )
    if not found:
        found = find_member_by_name_only(
            profiles,
            owner_user_id=owner_user_id,
            first_name=first_name,
            last_name=last_name,
        )

    if found:
        prof, mem = found
    else:
        if not allow_create:
            return None
        prof = create_profile(owner_user_id=owner_user_id, label=f"eSIM - {customer_name}".strip(), phone=customer_phone)
        mem = create_member(
            {
                "first_name": first_name,
                "last_name": last_name,
                "phone": customer_phone,
            }
        )
        prof.setdefault("members", []).append(mem)
        profiles.append(prof)

    if customer_phone:
        mem["phone"] = customer_phone
    prof["updated_at"] = datetime.utcnow().isoformat() + "Z"

    ev = add_history_event(
        owner_user_id=owner_user_id,
        profile_id=str(prof.get("id")),
        member_id=str(mem.get("id")),
        kind="esim",
        details=details if isinstance(details, dict) else {},
    )
    save_profiles(profiles)
    return ev


# ---------- Booking integration ----------
def attach_booking_to_passengers(
    owner_user_id: str,
    payload: Dict[str, Any],
    kind: str,
    booking_meta: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Given a booking payload (from Passenger Info / Payment), find or create passengers and add history entries.
    This supports the requirement: even if the employee doesn't select a profile, the system still connects
    based on passport number (preferred) or name+DOB.
    """
    profiles = load_profiles()
    events: List[Dict[str, Any]] = []

    pax_list = payload.get("passengers") if isinstance(payload, dict) else None
    if not isinstance(pax_list, list):
        return events

    contact = payload.get("contact") if isinstance(payload, dict) else {}
    contact_phone = ""
    if isinstance(contact, dict):
        contact_phone = str(contact.get("phone") or "").strip()

    for pax in pax_list:
        if not isinstance(pax, dict):
            continue

        # If Passenger Info explicitly selected a passenger from the DB, honor it.
        explicit_member_id = str(pax.get("member_id") or pax.get("passenger_db_member_id") or "").strip()
        explicit_profile_id = str(pax.get("profile_id") or pax.get("passenger_db_profile_id") or "").strip()
        explicit_passport_number = str(pax.get("selected_passport_number") or pax.get("passenger_db_passport_number") or "").strip()

        first_name = str(pax.get("first_name") or "").strip()
        last_name = str(pax.get("last_name") or "").strip()
        dob = str(pax.get("birth_date") or pax.get("dob") or "").strip()
        passport_number = str(pax.get("passport") or pax.get("passport_number") or "").strip()
        passport_exp = str(pax.get("expire_date") or pax.get("passport_expiry_date") or "").strip()
        nationality = str(pax.get("nationality") or "").strip()

        found = None
        # Explicit selection (strongest match)
        if explicit_member_id:
            for p in profiles:
                for m in (p.get("members") or []):
                    if str(m.get("id")) == explicit_member_id:
                        # Optional profile id sanity check
                        if not explicit_profile_id or str(p.get("id")) == explicit_profile_id:
                            found = (p, m)
                            break
                if found:
                    break

        # If a specific passport was chosen, prefer it for matching/upserting.
        if explicit_passport_number and not passport_number:
            passport_number = explicit_passport_number

        if passport_number:
            found = find_member_by_passport(profiles, passport_number, owner_user_id=owner_user_id)

        # Fallback: name + dob
        if not found and (first_name or last_name) and dob:
            fn = normalize(first_name)
            ln = normalize(last_name)
            for p in profiles:
                for m in (p.get("members") or []):
                    if normalize(str(m.get("first_name") or "")) == fn and normalize(str(m.get("last_name") or "")) == ln:
                        if normalize(str(m.get("dob") or "")) == normalize(dob):
                            found = (p, m)
                            break
                if found:
                    break

        if found:
            prof, mem = found
        else:
            # Auto-create a single-member profile (owner-scoped).
            prof = create_profile(owner_user_id=owner_user_id, label=f"Auto - {last_name or first_name}".strip(), phone=contact_phone)
            mem = create_member(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "dob": dob,
                    "nationality": nationality,
                    "national_id_number": "",
                    "phone": "",
                    "passports": [
                        {
                            "number": passport_number,
                            "issue_date": "",
                            "expiry_date": passport_exp,
                            "issue_place": "",
                        }
                    ]
                    if passport_number
                    else [],
                }
            )
            prof["members"].append(mem)
            profiles.append(prof)

        # Ensure passport doc exists/updated (renewal / new doc).
        if passport_number:
            upsert_member_passport(
                mem,
                {
                    "number": passport_number,
                    "issue_date": "",
                    "expiry_date": passport_exp,
                    "issue_place": "",
                },
            )

        prof["updated_at"] = datetime.utcnow().isoformat() + "Z"

        details = {
            **(booking_meta or {}),
            "first_name": first_name,
            "last_name": last_name,
            "dob": dob,
            "passport_number": passport_number,
        }
        ev = add_history_event(
            owner_user_id=owner_user_id,
            profile_id=str(prof.get("id")),
            member_id=str(mem.get("id")),
            kind=kind,
            details=details,
        )
        events.append(ev)

    save_profiles(profiles)
    return events
