from __future__ import annotations

import os
from pathlib import Path


def _wings_config_missing() -> bool:
    """
    Match the expectations of services.flights.ota.services.wings_client.get_client_from_env().
    Your earlier error message indicates it needs:
      - WINGS_AUTH_TOKEN (or AUTH_TOKEN)
      - optionally WINGS_BASE_URL / SEARCH_URL / BOOK_URL
    """
    # Token
    _ = (Path(__file__).with_name(".env").exists() and None)  # no-op; keeps intent clear
    auth = (
        (str(os.getenv("WINGS_AUTH_TOKEN") or "")).strip()
        or (str(os.getenv("AUTH_TOKEN") or "")).strip()
    )

    # Endpoints can be derived in wings_client, but we follow your own error wording
    base_url = (str(os.getenv("WINGS_BASE_URL") or "")).strip()
    search_url = (str(os.getenv("SEARCH_URL") or "")).strip()
    book_url = (str(os.getenv("BOOK_URL") or "")).strip()

    if not auth:
        return True

    # If no base_url, at least one of these should exist (depending on your wings_client implementation)
    if not base_url and (not search_url and not book_url):
        return True

    return False
