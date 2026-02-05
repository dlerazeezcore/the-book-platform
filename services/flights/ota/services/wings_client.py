from __future__ import annotations

import os
from typing import Any, Dict, Optional
import httpx


class WingsClient:
    def __init__(self, base_url: str, token: str, timeout_s: float = 45.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout_s

    async def air_low_fare_search(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/AirLowFareSearch"
        headers = {
            "Authorization": self.token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(url, headers=headers, json=payload)
            r.raise_for_status()
            return r.json()

    async def air_book(self, xml_body: str) -> str:
        url = f"{self.base_url}/AirBook"
        headers = {
            "Authorization": self.token,
            "Accept": "application/xml",
            "Content-Type": "application/xml",
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(url, headers=headers, content=xml_body.encode("utf-8"))
            r.raise_for_status()
            return r.text


def _derive_base_from_full_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    u = u.rstrip("/")
    # If they provide full endpoint URLs (SEARCH_URL/BOOK_URL), chop the last path component.
    if "/" in u:
        return u.rsplit("/", 1)[0]
    return u


def get_client_from_env() -> Optional[WingsClient]:
    """Create a WingsClient from environment variables.

    Supported env var names (for backwards compatibility with your .env.example and README):
      - Token: WINGS_AUTH_TOKEN or AUTH_TOKEN
      - Base:  WINGS_BASE_URL or derived from SEARCH_URL/BOOK_URL
      - Full URLs: SEARCH_URL / BOOK_URL (or WINGS_SEARCH_URL / WINGS_BOOK_URL)
    """

    token = (os.getenv("WINGS_AUTH_TOKEN") or os.getenv("AUTH_TOKEN") or "").strip()
    if not token:
        return None

    base = (os.getenv("WINGS_BASE_URL") or "").strip()

    if not base:
        search_url = (os.getenv("SEARCH_URL") or os.getenv("WINGS_SEARCH_URL") or "").strip()
        book_url = (os.getenv("BOOK_URL") or os.getenv("WINGS_BOOK_URL") or "").strip()
        base = _derive_base_from_full_url(search_url) or _derive_base_from_full_url(book_url)

    if not base:
        base = "https://wings.laveen-air.com/RIAM_main/rest/api"

    return WingsClient(base_url=base, token=token)
