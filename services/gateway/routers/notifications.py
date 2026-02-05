from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services.notifications.email.service import send_email

router = APIRouter()


class EmailRequest(BaseModel):
    to_email: str = Field(..., min_length=3)
    subject: str = Field(..., min_length=1)
    body: str = Field(..., min_length=1)


@router.post("/api/notify/email")
def notify_email(req: EmailRequest):
    ok, msg = send_email(req.to_email, req.subject, req.body)
    if ok:
        return {"status": "ok"}
    return JSONResponse(status_code=500, content={"status": "error", "error": msg})
