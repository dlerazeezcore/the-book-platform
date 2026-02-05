from __future__ import annotations

from fastapi import APIRouter, HTTPException

from services.payments.fib.service import (
    create_payment as fib_create_payment,
    load_config as load_fib_config,
    save_config as save_fib_config,
)

router = APIRouter()


@router.get("/api/other-apis/fib")
async def fib_config_get():
    return load_fib_config()


@router.post("/api/other-apis/fib")
async def fib_config_set(payload: dict):
    try:
        cfg = save_fib_config(payload or {})
        return cfg
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/other-apis/fib/create-payment")
async def fib_create_payment_endpoint(payload: dict):
    try:
        amount = int(payload.get("amount") or 0)
        if amount <= 0:
            raise ValueError("Amount must be greater than 0.")
        description = payload.get("description") or "Payment"
        data = fib_create_payment(amount, description)
        return data
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
