from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


# ------------------------------------------------------------
# Provider permissions / filters / schedules (simple JSON store)
# ------------------------------------------------------------
PERMISSIONS_PATH = Path(__file__).with_name("permissions.json")

DEFAULT_PERMISSIONS = {
    "providers": {
        "OTA": {
            # Availability switch (search results)
            "availability_enabled": True,
            # Seat estimation via extra searches
            "seats_estimation_enabled": True,
            # Ticketing mode: "full" issues ticket via provider; "availability_only" queues as pending
            "ticketing_mode": "full",  # "full" | "availability_only"
            # Filters
            "filters_enabled": True,
            "blocked_airlines": [],      # e.g. ["IA", "TK"]
            "blocked_suppliers": [],     # future use; e.g. ["OTA"]
            # Ticketing schedule (availability can remain on)
            "ticketing_schedule": {
                "enabled": False,
                "timezone": "Asia/Baghdad",
                # Rules are evaluated in order; any match enables ticketing
                # days: 0=Mon ... 6=Sun
                "rules": [
                    {"days": [0, 1, 2, 3, 4, 5, 6], "start": "00:00", "end": "23:59"}
                ],
            },
        }
    }
}


def _load_permissions() -> dict:
    """Load permissions config from disk.

    Note: Providers can be removed by admins. If a provider is missing, it is treated as disabled.
    """
    try:
        if PERMISSIONS_PATH.exists():
            data = json.loads(PERMISSIONS_PATH.read_text(encoding="utf-8") or "{}")
            if isinstance(data, dict):
                providers = data.get("providers")
                if not isinstance(providers, dict):
                    data["providers"] = {}
                return data
    except Exception:
        pass
    # default
    return json.loads(json.dumps(DEFAULT_PERMISSIONS))


def _save_permissions(cfg: dict) -> dict:
    # Minimal validation / normalization. Do NOT force-insert providers (admins may delete them).
    if not isinstance(cfg, dict):
        cfg = {}
    providers = cfg.get("providers")
    if not isinstance(providers, dict):
        cfg["providers"] = {}
    PERMISSIONS_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    return cfg


def _parse_hhmm(v: str) -> time | None:
    try:
        v = (v or "").strip()
        if not v:
            return None
        hh, mm = v.split(":", 1)
        return time(int(hh), int(mm))
    except Exception:
        return None


def _ticketing_schedule_allows(schedule: dict) -> bool:
    try:
        if not isinstance(schedule, dict):
            return True
        if not schedule.get("enabled"):
            return True

        tzname = (schedule.get("timezone") or "Asia/Baghdad").strip() or "Asia/Baghdad"
        try:
            tz = ZoneInfo(tzname)
        except Exception:
            tz = ZoneInfo("Asia/Baghdad")

        now = datetime.now(tz)
        wd = int(now.weekday())  # 0=Mon .. 6=Sun
        rules = schedule.get("rules") or []
        if not isinstance(rules, list) or not rules:
            return False

        for r in rules:
            if not isinstance(r, dict):
                continue
            days = r.get("days") or []
            if isinstance(days, str):
                try:
                    days = json.loads(days)
                except Exception:
                    days = []
            if wd not in set(int(x) for x in days if str(x).isdigit() or isinstance(x, int)):
                continue
            st = _parse_hhmm(str(r.get("start") or ""))
            en = _parse_hhmm(str(r.get("end") or ""))
            if not st or not en:
                continue

            tnow = now.time()
            # Normal range (e.g. 09:00-18:00)
            if st <= en and st <= tnow <= en:
                return True
            # Overnight range (e.g. 22:00-02:00)
            if st > en and (tnow >= st or tnow <= en):
                return True

        return False
    except Exception:
        return True


def _compute_schedule_windows(schedule: dict) -> dict:
    """Return schedule context: now_local, current_window, next_window, timezone."""
    try:
        if not isinstance(schedule, dict):
            return {"enabled": False}
        enabled = bool(schedule.get("enabled"))
        tzname = (schedule.get("timezone") or "Asia/Baghdad").strip() or "Asia/Baghdad"
        try:
            tz = ZoneInfo(tzname)
        except Exception:
            tz = ZoneInfo("Asia/Baghdad")

        now = datetime.now(tz)
        rules = schedule.get("rules") or []
        if not isinstance(rules, list):
            rules = []

        windows = []
        for r in rules:
            if not isinstance(r, dict):
                continue
            days = r.get("days") or []
            if isinstance(days, str):
                try:
                    days = json.loads(days)
                except Exception:
                    days = []
            days = [int(x) for x in days if str(x).isdigit() or isinstance(x, int)]
            st = _parse_hhmm(str(r.get("start") or ""))
            en = _parse_hhmm(str(r.get("end") or ""))
            if not st or not en:
                continue

            for offset in range(0, 8):
                d = (now.date() + timedelta(days=offset))
                if int(d.weekday()) not in set(days):
                    continue
                start_dt = datetime.combine(d, st, tzinfo=tz)
                if en >= st:
                    end_dt = datetime.combine(d, en, tzinfo=tz)
                else:
                    end_dt = datetime.combine(d + timedelta(days=1), en, tzinfo=tz)
                windows.append((start_dt, end_dt))

        windows.sort(key=lambda x: x[0])
        current = None
        next_win = None

        for w in windows:
            if w[0] <= now <= w[1]:
                current = w
                break

        if current:
            for w in windows:
                if w[0] > current[1]:
                    next_win = w
                    break
        else:
            for w in windows:
                if w[0] > now:
                    next_win = w
                    break

        def _fmt(w):
            if not w:
                return None
            return {
                "start": w[0].isoformat(),
                "end": w[1].isoformat(),
            }

        return {
            "enabled": enabled,
            "timezone": tzname,
            "now": now.isoformat(),
            "current_window": _fmt(current),
            "next_window": _fmt(next_win),
        }
    except Exception:
        return {"enabled": False}


def _ota_policy() -> dict:
    cfg = _load_permissions()
    p = (cfg.get("providers") or {}).get("OTA")
    # If OTA is not present (deleted), treat as disabled.
    if not isinstance(p, dict):
        return {
            "availability": False,
            "ticketing_effective": False,
            "ticketing_mode": "availability_only",
            "ticketing_schedule_ok": False,
            "filters_enabled": True,
            "blocked_airlines": [],
        }

    availability = bool(p.get("availability_enabled", True))
    blocked_suppliers = p.get("blocked_suppliers") or []
    if "OTA" in blocked_suppliers:
        availability = False

    ticketing_mode = (p.get("ticketing_mode") or "full").strip().lower()
    ticketing_allowed_by_schedule = _ticketing_schedule_allows(p.get("ticketing_schedule") or {})
    ticketing_effective = availability and (ticketing_mode == "full") and ticketing_allowed_by_schedule

    filters_enabled = bool(p.get("filters_enabled", True))
    blocked_airlines = [str(x).strip().upper() for x in (p.get("blocked_airlines") or []) if str(x).strip()]
    return {
        "availability": availability,
        "ticketing_effective": ticketing_effective,
        "ticketing_mode": "full" if ticketing_mode == "full" else "availability_only",
        "ticketing_schedule_ok": ticketing_allowed_by_schedule,
        "filters_enabled": filters_enabled,
        "blocked_airlines": blocked_airlines,
    }
