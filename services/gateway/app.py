from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# ------------------------------------------------------------
# Load .env from PROJECT ROOT (The book test/.env)
# ------------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")

from fastapi import FastAPI

from services.flights.ota.services.wings_client import get_client_from_env
from services.gateway.flights_utils import _wings_config_missing
from services.gateway.routers import (
    esim_router,
    flights_router,
    notifications_router,
    payments_router,
    permissions_router,
)

BUILD_ID = "backend-live-wings-fix-v2"

app = FastAPI(title="The Book Backend (API only)", version="1.0.0")

# Routers
app.include_router(notifications_router)
app.include_router(permissions_router)
app.include_router(flights_router)
app.include_router(payments_router)
app.include_router(esim_router)


@app.on_event("startup")
async def _startup_check():
    # Fail fast (so you don’t get “mystery 500” later)
    client = get_client_from_env()
    if not client or _wings_config_missing():
        # We don't crash the server hard; we just make it explicit in logs/health.
        # But you can change this to raise RuntimeError(...) if you prefer hard-fail.
        print(
            "WARNING: WINGS credentials not configured. "
            "Set WINGS_AUTH_TOKEN (or AUTH_TOKEN) and optionally WINGS_BASE_URL/SEARCH_URL/BOOK_URL."
        )


@app.get("/__build")
async def build():
    return {"build": BUILD_ID, "mode": "wings"}


@app.get("/health")
async def health():
    client = get_client_from_env()
    ok = bool(client) and not _wings_config_missing()
    return {
        "ok": ok,
        "build": BUILD_ID,
        "mode": "wings",
        "wings_configured": ok,
    }
