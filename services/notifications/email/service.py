from __future__ import annotations

import os
import subprocess


SMTP_URL = os.getenv("SMTP_URL") or "smtps://smtppro.zoho.com:465"
SMTP_USER = os.getenv("SMTP_USER") or "dler@corevia-consultants.com"
SMTP_PASS = os.getenv("SMTP_PASS") or "p1tEuc41NDDB"
SMTP_FROM = os.getenv("SMTP_FROM") or SMTP_USER


def send_email(to_email: str, subject: str, body: str) -> tuple[bool, str]:
    """Send email using a simple SMTP curl command."""
    to_email = (to_email or "").strip()
    if not to_email:
        return False, "Missing recipient email"

    if not SMTP_USER or not SMTP_PASS:
        return False, "SMTP credentials are missing"

    cmd = [
        "curl",
        "--url",
        SMTP_URL,
        "--ssl-reqd",
        "--user",
        f"{SMTP_USER}:{SMTP_PASS}",
        "--mail-from",
        SMTP_FROM,
        "--mail-rcpt",
        to_email,
        "-T",
        "-",
    ]

    msg = (
        f"From: {SMTP_FROM}\n"
        f"To: {to_email}\n"
        f"Subject: {subject}\n\n"
        f"{body}\n"
    )

    try:
        res = subprocess.run(
            cmd,
            input=msg.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=25,
        )
        if res.returncode == 0:
            return True, "sent"
        err = (res.stderr or res.stdout or b"").decode("utf-8", errors="ignore").strip()
        return False, err or f"curl failed (code {res.returncode})"
    except Exception as exc:
        return False, str(exc)

