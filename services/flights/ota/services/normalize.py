from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def _safe_get(obj: Any, path: List[Any], default=None):
    cur = obj
    for p in path:
        try:
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur.get(p)
        except Exception:
            return default
        if cur is None:
            return default
    return cur


def _parse_dt(dt: str) -> Optional[datetime]:
    """
    WINGS returns strings like: 2026-02-02T14:20:00.000+0300
    Python's %z can parse +0300.
    """
    if not dt or not isinstance(dt, str):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(dt, fmt)
        except Exception:
            continue
    return None


def _fmt_hhmm(dt: str) -> str:
    d = _parse_dt(dt)
    if not d:
        return ""
    return d.strftime("%H:%M")


def _duration_minutes(dep_dt: str, arr_dt: str) -> int:
    d0 = _parse_dt(dep_dt)
    d1 = _parse_dt(arr_dt)
    if not d0 or not d1:
        return 0
    delta = d1 - d0
    return max(0, int(delta.total_seconds() // 60))


def _fmt_duration(mins: int) -> str:
    if mins <= 0:
        return ""
    h = mins // 60
    m = mins % 60
    if h and m:
        return f"{h}h {m}m"
    if h:
        return f"{h}h"
    return f"{m}m"


def _money_amount(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def normalize_priced_itineraries(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes WINGS AirLowFareSearch response into the shape expected by the frontend.

    Returns:
      {
        "meta": {"echoToken": "...", "targetName": "..."},
        "results_outbound": [ { ... normalized itinerary ... }, ... ]
      }
    """
    pis = _safe_get(resp, ["pricedItineraries", "pricedItinerary"], []) or []
    if not isinstance(pis, list):
        pis = [pis]

    echo = resp.get("echoToken") if isinstance(resp, dict) else None
    target = resp.get("targetName") if isinstance(resp, dict) else None
    meta = {"echoToken": echo, "targetName": target}

    out: List[Dict[str, Any]] = []

    for idx, pi in enumerate(pis, start=1):
        odo = _safe_get(
            pi,
            ["airItinerary", "originDestinationOptions", "originDestinationOption", 0],
            {},
        ) or {}
        segs = _safe_get(odo, ["flightSegment"], []) or []
        if not isinstance(segs, list):
            segs = [segs]
        if not segs:
            continue

        segments_norm: List[Dict[str, Any]] = []
        for s in segs:
            dep = _safe_get(s, ["departureAirport", "locationCode"], "") or ""
            arr = _safe_get(s, ["arrivalAirport", "locationCode"], "") or ""
            dep_dt = s.get("departureDateTime") or ""
            arr_dt = s.get("arrivalDateTime") or ""
            airline = _safe_get(s, ["operatingAirline", "code"], "") or _safe_get(s, ["marketingAirline", "code"], "") or ""
            airline_name = _safe_get(s, ["operatingAirline", "companyShortName"], "") or ""
            flight_no = s.get("flightNumber") or ""
            res_class = s.get("resBookDesigCode") or ""
            fare_basis = s.get("fareBasisCode") or ""
            equip = ""
            eq = s.get("equipment")
            if isinstance(eq, list) and eq:
                equip = (eq[0] or {}).get("airEquipType") or ""
            elif isinstance(eq, dict):
                equip = eq.get("airEquipType") or ""

            ext_any = _safe_get(s, ["tpaextensions", "any", 0], {}) or {}
            baggage = ext_any.get("freeBaggage") or ""
            duration_raw = ext_any.get("duration") or ""
            aircraft_name = ext_any.get("aircraftName") or ""

            segments_norm.append(
                {
                    "dep": dep,
                    "arr": arr,
                    "dep_dt": dep_dt,
                    "arr_dt": arr_dt,
                    "airline": airline,
                    "airline_name": airline_name,
                    "flight": flight_no,
                    "class": res_class,
                    "fare_basis": fare_basis,
                    "equipment": equip,
                    "aircraft": aircraft_name,
                    "baggage": baggage,
                    "duration_raw": duration_raw,
                }
            )

        first = segments_norm[0]
        last = segments_norm[-1]
        total_mins = _duration_minutes(first["dep_dt"], last["arr_dt"])
        stops = max(0, len(segments_norm) - 1)
        stops_label = "Non-stop" if stops == 0 else ("1 stop" if stops == 1 else f"{stops} stops")

        # Fare
        fare0 = _safe_get(pi, ["airItineraryPricingInfo", "itinTotalFare", 0], {}) or {}
        total_fare = fare0.get("totalFare") or {}
        currency = (total_fare.get("currencyCode") or "IQD") if isinstance(total_fare, dict) else "IQD"
        amount = _money_amount(total_fare.get("amount") if isinstance(total_fare, dict) else 0.0)

        # Keep decimals if they exist (your curl shows .80 etc.)
        if abs(amount - round(amount)) < 1e-9:
            total_amount_str = f"{int(round(amount)):,}"
        else:
            total_amount_str = f"{amount:,.2f}"

        # Ticketing
        ticketing_vendor = _safe_get(pi, ["ticketingInfo", "ticketingVendor"], {}) or {}
        ticketing = {
            "companyShortName": ticketing_vendor.get("companyShortName"),
            "code": ticketing_vendor.get("code"),
            "codeContext": ticketing_vendor.get("codeContext"),
        }

        out.append(
            {
                "sequenceNumber": pi.get("sequenceNumber") or idx,
                "segments": segments_norm,
                "summary": {
                    "depart_time": _fmt_hhmm(first["dep_dt"]),
                    "arrive_time": _fmt_hhmm(last["arr_dt"]),
                    "duration_mins": total_mins,
                    "duration": _fmt_duration(total_mins),
                    "stops": stops,
                    "stops_label": stops_label,
                },
                "total_currency": currency,
                "total_amount": total_amount_str,
                "amount_raw": amount,
                "ticketing": ticketing,
            }
        )

    return {"meta": meta, "results_outbound": out}
