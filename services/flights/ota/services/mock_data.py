from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import random


def generate_search_results(
    from_code: str,
    to_code: str,
    date: str,
    trip_type: str,
    return_date: Optional[str],
    adults: int,
    children: int,
    infants: int,
) -> Dict[str, Any]:
    """
    Demo-only generator. Used when BOOK_MODE != "wings".
    Kept intentionally simple and syntactically safe.
    """
    random.seed(f"{from_code}-{to_code}-{date}-{trip_type}-{return_date}-{adults}-{children}-{infants}")

    def mk(seg_dep: str, seg_arr: str, dep_dt: str, arr_dt: str, airline: str, flight: str, price: float):
        return {
            "sequenceNumber": int(random.randint(1, 9999)),
            "segments": [
                {
                    "dep": seg_dep,
                    "arr": seg_arr,
                    "dep_dt": dep_dt,
                    "arr_dt": arr_dt,
                    "airline": airline,
                    "airline_name": airline,
                    "flight": flight,
                    "class": "Y",
                    "fare_basis": "YOW",
                    "equipment": "320",
                    "aircraft": "",
                    "baggage": "ADT,30,KG",
                    "duration_raw": "",
                }
            ],
            "summary": {
                "depart_time": dep_dt[11:16] if len(dep_dt) >= 16 else "",
                "arrive_time": arr_dt[11:16] if len(arr_dt) >= 16 else "",
                "duration_mins": 60,
                "duration": "1h",
                "stops": 0,
                "stops_label": "Non-stop",
            },
            "total_currency": "IQD",
            "total_amount": f"{price:,.2f}",
            "amount_raw": price,
            "ticketing": {"companyShortName": airline, "code": "", "codeContext": "DEMO"},
        }

    base = datetime.strptime(date, "%Y-%m-%d")
    results = []
    for i in range(8):
        dep_dt = (base + timedelta(hours=6 + i * 2)).strftime("%Y-%m-%dT%H:%M:00.000+0300")
        arr_dt = (base + timedelta(hours=7 + i * 2)).strftime("%Y-%m-%dT%H:%M:00.000+0300")
        price = 105040.0 + i * 25000.0
        results.append(mk(from_code, to_code, dep_dt, arr_dt, "IA", f"{900+i}", price))

    out = {"meta": {"echoToken": "DEMO", "targetName": "DEMO"}, "results_outbound": results}
    if trip_type == "roundtrip":
        out["results_return"] = results[:]
    return out


def generate_booking_confirmation(itinerary_json: str) -> Dict[str, Any]:
    return {"status": "ok", "pnr": "DEMO123", "itinerary": itinerary_json}
