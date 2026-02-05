from __future__ import annotations

import json
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services.flights.ota.services.normalize import normalize_priced_itineraries
from services.flights.ota.services.wings_client import get_client_from_env
from services.gateway.flights_utils import _wings_config_missing
from services.gateway.permissions_store import _ota_policy

router = APIRouter()


# ------------------------------------------------------------
# Models
# ------------------------------------------------------------
class Pax(BaseModel):
    adults: int = Field(1, ge=0)
    children: int = Field(0, ge=0)
    infants: int = Field(0, ge=0)


class AvailabilityRequest(BaseModel):
    from_: str = Field(..., alias="from")
    to: str
    date: str
    trip_type: str = Field("oneway")
    return_date: Optional[str] = None
    cabin: str = Field("economy")
    pax: Pax = Field(default_factory=Pax)


class Passenger(BaseModel):
    first_name: str
    last_name: str
    birth_date: str  # YYYY-MM-DD
    pax_type: str = Field("ADT")  # ADT / CHD / INF (if needed)
    name_prefix: str = Field("MR")  # MR/MS

    # Optional (if you want to collect later)
    gender: str = Field("M")  # M/F/U
    passport: Optional[str] = None
    issue_country: Optional[str] = None
    nationality: Optional[str] = None
    expire_date: Optional[str] = None
    doc_type: Optional[str] = None


class Contact(BaseModel):
    phone: Optional[str] = None
    email: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None


class BookingRequest(BaseModel):
    trip_type: str = Field("oneway")
    outbound_itinerary_json: str
    return_itinerary_json: Optional[str] = None
    passengers: list[Passenger] = Field(default_factory=list)
    contact: Optional[Contact] = None


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _normalize_cabin(v: str | None) -> str:
    """Map user cabin to WINGS cabin value."""
    v = (v or "").strip().lower()
    if v == "business":
        return "Business"
    return "Economy"


@router.post("/api/availability")
async def availability(req: AvailabilityRequest):
    client = get_client_from_env()
    if not client or _wings_config_missing():
        return JSONResponse(
            {
                "error": (
                    "WINGS credentials not configured. "
                    "Set WINGS_AUTH_TOKEN (or AUTH_TOKEN) and optionally "
                    "WINGS_BASE_URL/SEARCH_URL/BOOK_URL."
                )
            },
            status_code=500,
        )

    # Provider permissions / filters
    pol = _ota_policy()
    if not pol["availability"]:
        return {"meta": {"disabled": True, "reason": "Provider OTA is disabled"}, "results_outbound": []}

    def _seg_sig(seg: dict) -> str:
        """Stable signature for matching a return option to a roundtrip-priced itinerary."""
        dep = ((seg.get("departureAirport") or {}).get("locationCode") or "").upper()
        arr = ((seg.get("arrivalAirport") or {}).get("locationCode") or "").upper()
        dep_dt = (seg.get("departureDateTime") or "").strip()
        arr_dt = (seg.get("arrivalDateTime") or "").strip()
        op = (seg.get("operatingAirline") or {})
        mk = (seg.get("marketingAirline") or {})
        airline = (op.get("code") or mk.get("code") or "").upper()
        flight = str(seg.get("flightNumber") or "").strip()
        return "|".join([dep, arr, dep_dt, arr_dt, airline, flight])

    def _norm_sig_from_result(r: dict) -> str:
        try:
            s0 = (r.get("segments") or [])[0] or {}
        except Exception:
            s0 = {}
        dep = str(s0.get("dep") or "").upper()
        arr = str(s0.get("arr") or "").upper()
        dep_dt = str(s0.get("dep_dt") or "").strip()
        arr_dt = str(s0.get("arr_dt") or "").strip()
        airline = str(s0.get("airline") or "").upper()
        flight = str(s0.get("flight") or "").strip()
        return "|".join([dep, arr, dep_dt, arr_dt, airline, flight])

    def _fmt_money(v):
        """Format WINGS amounts consistently with the frontend (commas, 0 or 2 decimals).
        Returns: (formatted_str, raw_float_or_none)
        """
        try:
            n = float(v)
        except Exception:
            s = "" if v is None else str(v)
            return (s, None)

        if abs(n - round(n)) < 1e-9:
            return (f"{int(round(n)):,}", float(n))
        return (f"{n:,.2f}", float(n))

    cabin = _normalize_cabin(req.cabin)

    async def _roundtrip_price_map() -> dict:
        """Build a lookup: return-segment signature -> (currency, amount)."""
        if req.trip_type != "roundtrip" or not req.return_date:
            return {}

        payload_rt = {
            "ProcessingInfo": {"SearchType": "STANDARD"},
            "OriginDestinationInformation": [
                {
                    "DepartureDateTime": {"value": req.date},
                    "OriginLocation": {"LocationCode": req.from_},
                    "DestinationLocation": {"LocationCode": req.to},
                },
                {
                    "DepartureDateTime": {"value": req.return_date},
                    "OriginLocation": {"LocationCode": req.to},
                    "DestinationLocation": {"LocationCode": req.from_},
                },
            ],
            "TravelPreferences": [{"CabinPref": [{"Cabin": cabin}]}],
            "TravelerInfoSummary": {
                "AirTravelerAvail": [
                    {
                        "PassengerTypeQuantity": [
                            {"Code": "ADT", "Quantity": req.pax.adults},
                            {"Code": "CHD", "Quantity": req.pax.children},
                            {"Code": "INF", "Quantity": req.pax.infants},
                        ]
                    }
                ]
            },
        }

        resp_rt = await client.air_low_fare_search(payload_rt)

        pis = (resp_rt or {}).get("pricedItineraries", {}).get("pricedItinerary") or []
        if not isinstance(pis, list):
            pis = [pis]

        out = {}
        for pi in pis:
            try:
                odo_list = (
                    (pi.get("airItinerary", {}) or {})
                    .get("originDestinationOptions", {})
                    .get("originDestinationOption")
                    or []
                )
                if not isinstance(odo_list, list):
                    odo_list = [odo_list]
                if len(odo_list) < 2:
                    continue

                segs_ret = (odo_list[1] or {}).get("flightSegment") or []
                if not isinstance(segs_ret, list):
                    segs_ret = [segs_ret]
                if not segs_ret:
                    continue

                sig = _seg_sig(segs_ret[0] or {})

                fare0 = ((pi.get("airItineraryPricingInfo") or {}).get("itinTotalFare") or [])
                if not isinstance(fare0, list):
                    fare0 = [fare0]
                fare0 = fare0[0] if fare0 else {}
                total_fare = (fare0 or {}).get("totalFare") or {}

                ccy = (
                    (total_fare.get("currencyCode") or "IQD")
                    if isinstance(total_fare, dict)
                    else "IQD"
                )
                amt = total_fare.get("amount") if isinstance(total_fare, dict) else None
                if amt is None:
                    continue

                amt_disp, amt_f = _fmt_money(amt)
                out[sig] = {"currency": ccy, "amount": amt_disp, "amount_raw": amt_f}
            except Exception:
                continue

        return out

    payload = {
        "ProcessingInfo": {"SearchType": "STANDARD"},
        "OriginDestinationInformation": [
            {
                "DepartureDateTime": {"value": req.date},
                "OriginLocation": {"LocationCode": req.from_},
                "DestinationLocation": {"LocationCode": req.to},
            }
        ],
        "TravelPreferences": [{"CabinPref": [{"Cabin": cabin}]}],
        "TravelerInfoSummary": {
            "AirTravelerAvail": [
                {
                    "PassengerTypeQuantity": [
                        {"Code": "ADT", "Quantity": req.pax.adults},
                        {"Code": "CHD", "Quantity": req.pax.children},
                        {"Code": "INF", "Quantity": req.pax.infants},
                    ]
                }
            ]
        },
    }

    try:
        resp_out = await client.air_low_fare_search(payload)
        norm_out = normalize_priced_itineraries(resp_out)

        meta = norm_out.get("meta")
        results = norm_out.get("results_outbound") or []

        # Separate return call for independent selection
        results_return = []
        if req.trip_type == "roundtrip" and req.return_date:
            payload_ret = {
                **payload,
                "OriginDestinationInformation": [
                    {
                        "DepartureDateTime": {"value": req.return_date},
                        "OriginLocation": {"LocationCode": req.to},
                        "DestinationLocation": {"LocationCode": req.from_},
                    }
                ],
            }
            resp_ret = await client.air_low_fare_search(payload_ret)
            norm_ret = normalize_priced_itineraries(resp_ret)
            results_return = norm_ret.get("results_outbound") or []

            # Enrich return results with true roundtrip totals
            rt_map = await _roundtrip_price_map()
            if rt_map:
                for rr in results_return:
                    sig = _norm_sig_from_result(rr)
                    info = rt_map.get(sig)
                    if info:
                        rr["roundtrip_total_currency"] = info.get("currency")
                        rr["roundtrip_total_amount"] = info.get("amount")
                        rr["roundtrip_amount_raw"] = info.get("amount_raw")

        # Apply provider filters (blocked airlines)
        if pol.get("filters_enabled", True) and pol.get("blocked_airlines"):
            blocked = set([str(x).upper() for x in (pol.get("blocked_airlines") or []) if str(x).strip()])

            def _is_blocked(it: dict) -> bool:
                try:
                    s0 = (it.get("segments") or [])[0] or {}
                except Exception:
                    s0 = {}
                code = str(s0.get("airline") or "").strip().upper()
                return code in blocked

            if isinstance(results, list):
                results = [r for r in results if not _is_blocked(r)]
            if isinstance(results_return, list):
                results_return = [r for r in results_return if not _is_blocked(r)]

        return JSONResponse({"meta": meta, "results": results, "results_return": results_return})

    except HTTPException:
        raise
    except Exception as e:
        # When WINGS rejects payloads etc., it's often better to surface as 502,
        # but we keep your original behavior and message.
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/book")
async def book(req: BookingRequest):
    client = get_client_from_env()
    if not client or _wings_config_missing():
        return JSONResponse(
            {
                "error": (
                    "WINGS credentials not configured. "
                    "Set WINGS_AUTH_TOKEN (or AUTH_TOKEN) and optionally "
                    "WINGS_BASE_URL/SEARCH_URL/BOOK_URL."
                )
            },
            status_code=500,
        )

    # Provider permissions / filters
    pol = _ota_policy()
    if not pol["availability"]:
        return {"meta": {"disabled": True, "reason": "Provider OTA is disabled"}, "results_outbound": []}

    # If ticketing is not effective (permissions set to availability-only or schedule disables ticketing),
    # return a pending response so the frontend can queue it in Pending bookings.
    if not pol.get("ticketing_effective", True):
        from uuid import uuid4
        pending_id = "PND-" + uuid4().hex[:10].upper()
        return {
            "pending": True,
            "status": "pending",
            "pending_id": pending_id,
            "provider": "OTA",
            "reason": "Ticketing is disabled by permissions or schedule.",
        }

    import json as _json
    import re as _re
    from datetime import datetime as _dt
    from xml.etree import ElementTree as _ET

    def _norm(s: str | None) -> str:
        if s is None:
            return ""
        return str(s).replace("\r", "").replace("\n", " ").strip()

    def _esc_attr(s: str | None) -> str:
        s = _norm(s)
        return (
            s.replace("&", "&amp;")
            .replace('"', "&quot;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("'", "&apos;")
        )

    def _esc_text(s: str | None) -> str:
        s = _norm(s)
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def _as_list(v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        return [v]

    def _pick_equipment(seg: dict) -> str:
        eq = seg.get("equipment")
        if isinstance(eq, list) and eq:
            return str((eq[0] or {}).get("airEquipType") or "")
        if isinstance(eq, dict):
            return str(eq.get("airEquipType") or "")
        return ""

    def _pick_tpa_any(seg: dict) -> dict:
        # WINGS JSON structure often: tpaextensions.any[0]
        tpa = seg.get("tpaextensions") or seg.get("tpaExtensions") or {}
        any0 = (tpa.get("any") or [])
        if isinstance(any0, list) and any0:
            return any0[0] or {}
        if isinstance(any0, dict):
            return any0
        return {}

    def _ticketing_vendor(pi: dict) -> dict:
        """Extract TicketingVendor in a case/shape-tolerant way.

        Supported shapes:
          - priced itinerary (WINGS JSON): pi.ticketingInfo.ticketingVendor
          - case variants: TicketingInfo/TicketingVendor
          - normalized itinerary (frontend): pi.ticketing {companyShortName, code, codeContext}
          - already-normalized ticketing block: pi.ticketingVendor / TicketingVendor (dict)
        """
        def _as_dict(v):
            return v if isinstance(v, dict) else {}

        # Normalized itinerary often has: {"ticketing": {"companyShortName":..,"code":..,"codeContext":..}}
        t_norm = _as_dict(pi.get("ticketing"))
        if t_norm:
            return {
                "companyShortName": (t_norm.get("companyShortName") or t_norm.get("CompanyShortName") or ""),
                "code": (t_norm.get("code") or t_norm.get("Code") or ""),
                "codeContext": (t_norm.get("codeContext") or t_norm.get("CodeContext") or ""),
            }

        # Direct dict vendor blocks
        for k in ("ticketingVendor", "TicketingVendor"):
            v = _as_dict(pi.get(k))
            if v:
                return {
                    "companyShortName": (v.get("companyShortName") or v.get("CompanyShortName") or ""),
                    "code": (v.get("code") or v.get("Code") or ""),
                    "codeContext": (v.get("codeContext") or v.get("CodeContext") or ""),
                }

        # WINGS-like structures
        ti = _as_dict(pi.get("ticketingInfo")) or _as_dict(pi.get("TicketingInfo"))
        tv = _as_dict(ti.get("ticketingVendor")) or _as_dict(ti.get("TicketingVendor"))
        return {
            "companyShortName": tv.get("companyShortName") or tv.get("CompanyShortName") or "",
            "code": tv.get("code") or tv.get("Code") or "",
            "codeContext": tv.get("codeContext") or tv.get("CodeContext") or "",
        }

    def _pricing(pi: dict) -> dict:
        """Extract pricing in a tolerant way.

        Supported shapes:
          - priced itinerary (WINGS JSON): airItineraryPricingInfo.itinTotalFare[0].baseFare/totalFare
          - normalized itinerary (frontend): total_currency + amount_raw / total_amount
        """
        # Normalized itinerary
        if isinstance(pi, dict) and (pi.get("total_currency") or pi.get("amount_raw") or pi.get("total_amount")) and not pi.get("airItineraryPricingInfo"):
            cur = str(pi.get("total_currency") or "IQD")
            amt = pi.get("amount_raw")
            if amt is None:
                # total_amount is often string, keep numeric-ish characters
                try:
                    amt = float(str(pi.get("total_amount") or "").replace(",", "").strip() or 0)
                except Exception:
                    amt = 0
            try:
                amt_str = str(int(float(amt))) if float(amt).is_integer() else str(float(amt))
            except Exception:
                amt_str = str(amt or "")
            return {
                "baseCur": cur,
                "baseDec": "2",
                "baseAmt": amt_str,
                "totCur": cur,
                "totDec": "2",
                "totAmt": amt_str,
            }

        # WINGS priced itinerary
        itin_total = _as_list((pi.get("airItineraryPricingInfo") or {}).get("itinTotalFare"))
        itin_total = itin_total[0] if itin_total else {}
        base = itin_total.get("baseFare") or {}
        total = itin_total.get("totalFare") or {}

        # Fallbacks: if base missing, use total
        if not isinstance(base, dict):
            base = {}
        if not isinstance(total, dict):
            total = {}

        if not base and total:
            base = dict(total)

        def _f(n):
            try:
                return str(n)
            except Exception:
                return ""

        return {
            "baseCur": _f(base.get("currencyCode") or total.get("currencyCode") or "IQD"),
            "baseDec": _f(base.get("decimalPlaces") or total.get("decimalPlaces") or "2"),
            "baseAmt": _f(base.get("amount") or ""),
            "totCur": _f(total.get("currencyCode") or base.get("currencyCode") or "IQD"),
            "totDec": _f(total.get("decimalPlaces") or base.get("decimalPlaces") or "2"),
            "totAmt": _f(total.get("amount") or ""),
        }

    def _segments_from_pi(pi: dict, leg_index: int) -> list[dict]:
        """Return a list of segment dicts in WINGS-like structure.

        Accepts either:
          1) WINGS priced itinerary dict (airItinerary.originDestinationOptions.originDestinationOption[].flightSegment[])
          2) normalized itinerary dict (segments[] with dep/arr/dep_dt/arr_dt/flight/airline/airline_name/equipment/baggage/aircraft)
        """
        # Case 1: WINGS priced itinerary shape (already supported)
        odo_list = _as_list(
            ((pi.get("airItinerary") or {}).get("originDestinationOptions") or {}).get("originDestinationOption")
        )
        if odo_list and leg_index < len(odo_list):
            segs = _as_list((odo_list[leg_index] or {}).get("flightSegment"))
            return [s for s in segs if isinstance(s, dict)]

        # Case 1b: capitalization variants
        odo_list2 = _as_list(
            ((pi.get("AirItinerary") or {}).get("OriginDestinationOptions") or {}).get("OriginDestinationOption")
        )
        if odo_list2 and leg_index < len(odo_list2):
            segs = _as_list((odo_list2[leg_index] or {}).get("FlightSegment"))
            return [s for s in segs if isinstance(s, dict)]

        # Case 2: normalized itinerary
        segs_norm = pi.get("segments")
        if isinstance(segs_norm, list):
            if leg_index != 0:
                return []
            out: list[dict] = []
            for s in segs_norm:
                if not isinstance(s, dict):
                    continue
                dep = (s.get("dep") or "").upper()
                arr = (s.get("arr") or "").upper()
                dep_dt = s.get("dep_dt") or ""
                arr_dt = s.get("arr_dt") or ""
                flt = s.get("flight") or ""
                airline = (s.get("airline") or "")
                airline_name = (s.get("airline_name") or "")
                equip = s.get("equipment") or ""
                baggage = s.get("baggage") or ""
                aircraft = s.get("aircraft") or ""

                seg = {
                    "departureDateTime": dep_dt,
                    "arrivalDateTime": arr_dt,
                    "flightNumber": flt,
                    "departureAirport": {"locationCode": dep},
                    "arrivalAirport": {"locationCode": arr},
                    "operatingAirline": {"code": airline, "companyShortName": airline_name},
                    "marketingAirline": {"code": airline},
                    "equipment": {"airEquipType": equip} if equip else {},
                    "tpaextensions": {"any": [{"freeBaggage": baggage, "aircraftName": aircraft}]},
                }
                out.append(seg)
            return out

        return []

    def _build_air_travelers(passengers: list[Passenger], contact: Contact | None) -> str:
        # Provide contact + document defaults if missing.
        default_phone = (contact.phone if contact and contact.phone else "9647500000000")
        default_email = (contact.email if contact and contact.email else "dler.azeez@example.com")
        default_issue = (contact.country if contact and contact.country else "IQ")
        default_nation = default_issue

        def _doc_for(i: int, p: Passenger) -> dict:
            # Generate a reasonably unique fallback passport if not supplied.
            passport = p.passport or ("P" + "".join([str((i + 7) % 10) for _ in range(8)]))
            issue = (p.issue_country or default_issue)[:2].upper()
            nation = (p.nationality or default_nation)[:2].upper()
            exp = p.expire_date or "2030-01-01"
            doc_type = p.doc_type or "2"
            return {"passport": passport, "issue": issue, "nation": nation, "exp": exp, "doc_type": doc_type}

        chunks = []
        for i, p in enumerate(passengers):
            doc = _doc_for(i, p)
            # Only first traveler carries contact fields (keeps XML closer to common OTA patterns)
            tel_xml = f'      <Telephone PhoneNumber="{_esc_attr(default_phone)}"/>\n' if i == 0 else ""
            email_xml = f"      <Email>{_esc_text(default_email)}</Email>\n" if i == 0 else ""

            chunks.append(
                f'''    <AirTraveler BirthDate="{_esc_attr(p.birth_date)}" PassengerTypeCode="{_esc_attr(p.pax_type)}" AccompaniedByInfantInd="false" Gender="{_esc_attr(p.gender or "M")}">
      <PersonName>
        <NamePrefix>{_esc_text(p.name_prefix or ("MS" if (p.gender or "M").upper()=="F" else "MR"))}</NamePrefix>
        <GivenName>{_esc_text(p.first_name)}</GivenName>
        <Surname>{_esc_text(p.last_name)}</Surname>
      </PersonName>

{tel_xml}{email_xml}      <Document DocID="{_esc_attr(doc["passport"])}"
                DocType="{_esc_attr(doc["doc_type"])}"
                DocIssueCountry="{_esc_attr(doc["issue"])}"
                DocHolderNationality="{_esc_attr(doc["nation"])}"
                ExpireDate="{_esc_attr(doc["exp"])}"/>
    </AirTraveler>'''
            )
        return "\n".join(chunks)

    def _build_fulfillment(passengers: list[Passenger], contact: Contact | None) -> str:
        p0 = passengers[0] if passengers else Passenger(first_name="Test", last_name="User", birth_date="1990-01-01")
        phone = (contact.phone if contact and contact.phone else "9647500000000")
        email = (contact.email if contact and contact.email else "dler.azeez@example.com")
        country = (contact.country if contact and contact.country else "IQ")
        city = (contact.city if contact and contact.city else "Erbil")
        gender_text = "Female" if (p0.gender or "M").upper() == "F" else "Male"

        return f'''  <Fulfillment>
    <Name>
      <GivenName>{_esc_text(p0.first_name)}</GivenName>
      <Surname>{_esc_text(p0.last_name)}</Surname>
      <TPA_Extensions>
        <TPA_Extension>
          <Username>{_esc_text(email)}</Username>
          <Country>{_esc_text(country)}</Country>
          <PersianLasttName>{_esc_text(p0.last_name)}</PersianLasttName>
          <Gender>{_esc_text(gender_text)}</Gender>
          <City>{_esc_text(city)}</City>
          <PersianFirstName>{_esc_text(p0.first_name)}</PersianFirstName>
          <Mobile>{_esc_text(phone)}</Mobile>
          <Nationality>{_esc_text(country)}</Nationality>
          <NationalityNum>{_esc_text((passengers[0].passport if passengers and passengers[0].passport else "P12345678"))}</NationalityNum>
        </TPA_Extension>
      </TPA_Extensions>
    </Name>
  </Fulfillment>'''

    def _build_leg_xml(pi: dict, leg_index: int) -> str:
        segs = _segments_from_pi(pi, leg_index)
        if not segs:
            return ""
        seg_xml = []
        for idx, seg in enumerate(segs, start=1):
            dep_dt = seg.get("departureDateTime") or ""
            arr_dt = seg.get("arrivalDateTime") or ""
            flt_no = seg.get("flightNumber") or ""
            dep_lc = ((seg.get("departureAirport") or {}).get("locationCode") or "").upper()
            arr_lc = ((seg.get("arrivalAirport") or {}).get("locationCode") or "").upper()

            op = seg.get("operatingAirline") or {}
            mk = seg.get("marketingAirline") or {}
            op_code = (op.get("code") or mk.get("code") or "IA").upper()
            mk_code = (mk.get("code") or op_code).upper()
            op_name = op.get("companyShortName") or "Iraqi Airways"
            eq_type = _pick_equipment(seg)

            tpa_any = _pick_tpa_any(seg)
            dep_full = tpa_any.get("departureAirport") or tpa_any.get("DepartureAirport") or ""
            arr_full = tpa_any.get("arrivalAirport") or tpa_any.get("ArrivalAirport") or ""
            dep_country = tpa_any.get("departureCountry") or ""
            arr_country = tpa_any.get("arrivalCountry") or ""
            dep_city = tpa_any.get("departureCity") or ""
            arr_city = tpa_any.get("arrivalCity") or ""
            free_bag = tpa_any.get("freeBaggage") or ""
            aircraft_name = tpa_any.get("aircraftName") or ""

            equipment_xml = f'          <Equipment AirEquipType="{_esc_attr(eq_type)}"/>\n' if eq_type else ""

            seg_xml.append(
                f'''        <FlightSegment DepartureDateTime="{_esc_attr(dep_dt)}"
                       ArrivalDateTime="{_esc_attr(arr_dt)}"
                       StopQuantity="0"
                       RPH="{idx}"
                       FlightNumber="{_esc_attr(flt_no)}">
          <DepartureAirport LocationCode="{_esc_attr(dep_lc)}"/>
          <ArrivalAirport LocationCode="{_esc_attr(arr_lc)}"/>
          <OperatingAirline CompanyShortName="{_esc_attr(op_name)}" Code="{_esc_attr(op_code)}"/>
{equipment_xml}          <TPA_Extensions>
            <TPA_Extension>
              <DepartureAirport>{_esc_text(dep_full)}</DepartureAirport>
              <departureCountry>{_esc_text(dep_country)}</departureCountry>
              <departureCity>{_esc_text(dep_city)}</departureCity>
              <arrivalCity>{_esc_text(arr_city)}</arrivalCity>
              <ArrivalAirport>{_esc_text(arr_full)}</ArrivalAirport>
              <arrivalCountry>{_esc_text(arr_country)}</arrivalCountry>
              <freeBaggage>{_esc_text(free_bag)}</freeBaggage>
              <aircraftName>{_esc_text(aircraft_name)}</aircraftName>
            </TPA_Extension>
          </TPA_Extensions>
          <MarketingAirline Code="{_esc_attr(mk_code)}"/>
        </FlightSegment>'''
            )
        return "\n".join(seg_xml)

    def _build_airbook_xml(
        outbound_pi: dict,
        return_pi: dict | None,
        passengers: list[Passenger],
        contact: Contact | None,
        trip_type: str,
    ) -> str:
        tv = _ticketing_vendor(outbound_pi)
        if not (tv.get("companyShortName") and tv.get("code") and tv.get("codeContext")):
            raise ValueError("TicketingVendor not found. Cannot book without vendor.")

        pr = _pricing(outbound_pi)

        # DirectionInd
        direction = "OneWay" if (trip_type or "oneway").lower() != "roundtrip" else "Return"

        # Build legs
        out_leg = _build_leg_xml(outbound_pi, 0)
        if not out_leg:
            raise ValueError("No outbound segments found in itinerary.")

        rt_leg = ""
        if (trip_type or "").lower() == "roundtrip":
            # Return PI may contain the return leg at index 0 if it was searched as 'reverse one-way'
            # or at index 1 if it came from a roundtrip priced itinerary.
            if return_pi:
                rt_leg = _build_leg_xml(return_pi, 0) or _build_leg_xml(return_pi, 1)
            else:
                rt_leg = ""

        traveler_xml = _build_air_travelers(passengers, contact)
        fulfillment_xml = _build_fulfillment(passengers, contact)

        # Price section: keep BaseFare + TotalFare like your working XML
        base_amt = pr.get("baseAmt") or pr.get("totAmt") or ""
        tot_amt = pr.get("totAmt") or pr.get("baseAmt") or ""

        xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<OTA_AirBookRQ>

  <AirItinerary DirectionInd="{_esc_attr(direction)}">
    <OriginDestinationOptions>
      <OriginDestinationOption>
{out_leg}
      </OriginDestinationOption>'''

        if rt_leg:
            xml += f'''
      <OriginDestinationOption>
{rt_leg}
      </OriginDestinationOption>'''

        xml += f'''
    </OriginDestinationOptions>
  </AirItinerary>

  <PriceInfo>
    <ItinTotalFare>
      <BaseFare CurrencyCode="{_esc_attr(pr.get("baseCur") or "IQD")}" DecimalPlaces="{_esc_attr(pr.get("baseDec") or "2")}" Amount="{_esc_attr(base_amt)}"/>
      <TotalFare CurrencyCode="{_esc_attr(pr.get("totCur") or "IQD")}" DecimalPlaces="{_esc_attr(pr.get("totDec") or "2")}" Amount="{_esc_attr(tot_amt)}"/>
    </ItinTotalFare>
  </PriceInfo>

  <TravelerInfo>
{traveler_xml}
  </TravelerInfo>

{fulfillment_xml}

  <Ticketing>
    <TicketingVendor CompanyShortName="{_esc_attr(tv.get("companyShortName"))}" Code="{_esc_attr(tv.get("code"))}" CodeContext="{_esc_attr(tv.get("codeContext"))}"/>
  </Ticketing>

</OTA_AirBookRQ>
'''
        return xml

    def _extract_refs(xml_text: str) -> dict:
        # Try XML parse first
        pnr = None
        connectota = None
        try:
            root = _ET.fromstring(xml_text)
            for br in root.findall(".//BookingReferenceID"):
                if br is None:
                    continue
                val = br.attrib.get("ID")
                ctx = br.attrib.get("ID_Context")
                if val and not pnr:
                    pnr = val
                if ctx and ctx.lower() == "connectota":
                    connectota = val
        except Exception:
            # Regex fallback
            m = _re.search(r'<BookingReferenceID[^>]*\sID="([^"]+)"', xml_text or "")
            if m:
                pnr = m.group(1)

        return {"pnr": pnr, "connectota_id": connectota}

    # -----------------------------
    # Parse input priced itineraries
    # -----------------------------
    try:
        outbound_pi = _json.loads(req.outbound_itinerary_json)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid outbound_itinerary_json: {e}")

    return_pi = None
    if req.return_itinerary_json:
        try:
            return_pi = _json.loads(req.return_itinerary_json)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid return_itinerary_json: {e}")

    if not req.passengers:
        raise HTTPException(status_code=400, detail="passengers is required")

    # Build + book
    try:
        airbook_xml = _build_airbook_xml(
            outbound_pi=outbound_pi,
            return_pi=return_pi,
            passengers=req.passengers,
            contact=req.contact,
            trip_type=req.trip_type,
        )
        airbook_resp = await client.air_book(airbook_xml)
        refs = _extract_refs(airbook_resp)
        return JSONResponse(
            {
                "status": "success",
                "pnr": refs.get("pnr"),
                "connectota_id": refs.get("connectota_id"),
                "request_xml": airbook_xml,
                "response_xml": airbook_resp,
            }
        )
    except httpx.HTTPStatusError:
        # When ticketing fails (e.g. provider offline), return a pending response so it can be completed manually.
        from uuid import uuid4
        pending_id = "PND-" + uuid4().hex[:10].upper()
        return JSONResponse(
            {
                "pending": True,
                "status": "pending",
                "pending_id": pending_id,
                "provider": "OTA",
                "reason": "Ticketing failed upstream. Manual completion required.",
            },
            status_code=202,
        )
    except Exception:
        from uuid import uuid4
        pending_id = "PND-" + uuid4().hex[:10].upper()
        return JSONResponse(
            {
                "pending": True,
                "status": "pending",
                "pending_id": pending_id,
                "provider": "OTA",
                "reason": "Ticketing failed. Manual completion required.",
            },
            status_code=202,
        )
