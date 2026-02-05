from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data"
ORDERS_PATH = DATA_DIR / "esim_orders.json"


def _ensure_file() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not ORDERS_PATH.exists():
        ORDERS_PATH.write_text("[]", encoding="utf-8")


def _load() -> List[Dict[str, Any]]:
    try:
        _ensure_file()
        raw = ORDERS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else []
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(items: List[Dict[str, Any]]) -> None:
    _ensure_file()
    ORDERS_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _new_id() -> str:
    return "esim_" + uuid.uuid4().hex[:12]


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def record_order(order: Dict[str, Any]) -> Dict[str, Any]:
    items = _load()
    row = dict(order or {})
    if not row.get("id"):
        row["id"] = _new_id()
    if not row.get("created_at"):
        row["created_at"] = now_iso()
    row["updated_at"] = now_iso()
    items.append(row)
    _save(items)
    return row


def update_order_by_reference(reference: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not reference:
        return None
    items = _load()
    updated = None
    for row in items:
        if str(row.get("order_reference") or "") == str(reference):
            for k, v in (fields or {}).items():
                row[k] = v
            row["updated_at"] = now_iso()
            updated = row
            break
    if updated is not None:
        _save(items)
    return updated


def list_orders_for_owner(owner_user_id: str) -> List[Dict[str, Any]]:
    items = _load()
    out = [x for x in items if str(x.get("owner_user_id") or "") == str(owner_user_id)]
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out


def list_orders_for_agent(owner_user_id: str, agent_user_id: str) -> List[Dict[str, Any]]:
    items = _load()
    out = [
        x for x in items
        if str(x.get("owner_user_id") or "") == str(owner_user_id)
        and str(x.get("agent_user_id") or "") == str(agent_user_id)
    ]
    out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return out
