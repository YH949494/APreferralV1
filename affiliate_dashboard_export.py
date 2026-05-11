from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any
from uuid import uuid4

import pytz
import requests
from pymongo import ASCENDING

from database import db, safe_create_index

logger = logging.getLogger(__name__)

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")
QUALIFIED_STATUSES = {"ISSUED", "APPROVED", "SUCCESS", "QUALIFIED", "COMPLETED"}
PENDING_STATUSES = {"PENDING", "PENDING_REVIEW", "PENDING_MANUAL", "SETTLING", "SIMULATED_PENDING"}
REJECTED_STATUSES = {"BLOCKED", "REJECTED", "CANCELLED", "FAILED"}

def _cfg():
    return {
        "affiliate_ledger_collection": os.getenv("AFFILIATE_LEDGER_COLLECTION", "affiliate_ledger"),
        "sheets_webhook_url": os.getenv("SHEETS_WEBHOOK_URL", "").strip(),
        "allow_full_export_rebuild": os.getenv("ALLOW_FULL_EXPORT_REBUILD", "0") == "1",
        "reporting_max_major_read_rows": int(os.getenv("REPORTING_MAX_MAJOR_READ_ROWS", "50000")),
    }


PROJECTION = {
    "_id": 1,
    "status": 1,
    "type": 1,
    "reward_type": 1,
    "tier": 1,
    "year_month": 1,
    "created_at": 1,
    "updated_at": 1,
    "issued_at": 1,
    "claimed_at": 1,
    "approved_at": 1,
    "qualified_at": 1,
    "user_id": 1,
    "invitee_user_id": 1,
    "referred_user_id": 1,
    "referrer_user_id": 1,
    "inviter_user_id": 1,
    "inviter_telegram_id": 1,
    "invitee_telegram_id": 1,
    "inviter_username": 1,
    "invitee_username": 1,
    "username": 1,
    "invitee_username_lower": 1,
    "inviter_username_lower": 1,
    "voucher_code": 1,
    "coupon_code": 1,
    "code": 1,
    "voucher": 1,
}


def ensure_affiliate_dashboard_indexes(db_ref=None) -> None:
    db_ref = db_ref or db
    coll = db_ref[_cfg()["affiliate_ledger_collection"]]
    safe_create_index(coll, [("updated_at", ASCENDING)], name="affiliate_ledger_updated_at_idx")
    safe_create_index(coll, [("created_at", ASCENDING)], name="affiliate_ledger_created_at_idx")
    safe_create_index(coll, [("year_month", ASCENDING), ("status", ASCENDING)], name="affiliate_ledger_year_month_status_idx")
    safe_create_index(coll, [("status", ASCENDING), ("updated_at", ASCENDING)], name="affiliate_ledger_status_updated_at_idx")
    safe_create_index(coll, [("inviter_user_id", ASCENDING)], name="affiliate_ledger_inviter_idx")
    safe_create_index(coll, [("invitee_user_id", ASCENDING)], name="affiliate_ledger_invitee_idx")
    safe_create_index(coll, [("voucher_code", ASCENDING)], name="affiliate_ledger_code_idx")


def _to_dt(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return None


def _to_iso(v):
    dt = _to_dt(v)
    return dt.isoformat() if dt else None


def _derive_year_month(row: dict[str, Any]) -> str | None:
    ym = row.get("year_month")
    if ym:
        return str(ym)
    dt = _to_dt(row.get("issued_at")) or _to_dt(row.get("claimed_at")) or _to_dt(row.get("created_at"))
    return dt.astimezone(KL_TZ).strftime("%Y%m") if dt else None


def _canonical_code(row: dict[str, Any]):
    code = row.get("voucher_code") or row.get("coupon_code") or row.get("code")
    if not code and isinstance(row.get("voucher"), dict):
        code = row["voucher"].get("code")
    return code


def _referral_status(status: str | None) -> str:
    s = (status or "").upper()
    if s in QUALIFIED_STATUSES:
        return "qualified"
    if s in PENDING_STATUSES:
        return "pending"
    if s in REJECTED_STATUSES:
        return "rejected"
    return "unknown"


def build_welcome_rows(docs: list[dict[str, Any]], *, export_run_id: str, exported_at: datetime):
    out = []
    for d in docs:
        code = _canonical_code(d)
        inviter_username = d.get("inviter_username") or d.get("username")
        invitee_user_id = d.get("invitee_user_id") or d.get("referred_user_id")
        inviter_user_id = d.get("inviter_user_id") or d.get("referrer_user_id") or d.get("user_id")
        out.append({
            "export_run_id": export_run_id,
            "exported_at": exported_at.isoformat(),
            "ledger_id": str(d.get("_id")),
            "ledger_status": d.get("status"),
            "ledger_type": d.get("type"),
            "reward_type": d.get("reward_type"),
            "tier": d.get("tier"),
            "year_month": _derive_year_month(d),
            "inviter_user_id": inviter_user_id,
            "inviter_telegram_id": d.get("inviter_telegram_id"),
            "inviter_username": inviter_username,
            "inviter_username_lower": d.get("inviter_username_lower") or (inviter_username.lower() if isinstance(inviter_username, str) else None),
            "invitee_user_id": invitee_user_id,
            "invitee_telegram_id": d.get("invitee_telegram_id"),
            "invitee_username": d.get("invitee_username"),
            "invitee_username_lower": d.get("invitee_username_lower") or (d.get("invitee_username").lower() if isinstance(d.get("invitee_username"), str) else None),
            "voucher_code": code,
            "coupon_code": d.get("coupon_code") or code,
            "code": d.get("code") or code,
            "issued_at": _to_iso(d.get("issued_at")),
            "claimed_at": _to_iso(d.get("claimed_at")),
            "created_at": _to_iso(d.get("created_at")),
            "updated_at": _to_iso(d.get("updated_at")),
            "source_collection": _cfg()["affiliate_ledger_collection"],
        })
    return out


def build_referral_rows(welcome_rows: list[dict[str, Any]]):
    dedup: dict[tuple, dict] = {}
    for row in welcome_rows:
        key = (
            row.get("inviter_user_id"), row.get("invitee_user_id"), row.get("voucher_code") or row.get("code"), row.get("year_month")
        )
        if not any(key[:3]):
            key = ("ledger", row.get("ledger_id"), None, None)
        if key in dedup:
            continue
        dedup[key] = {
            "export_run_id": row.get("export_run_id"),
            "exported_at": row.get("exported_at"),
            "inviter_user_id": row.get("inviter_user_id"),
            "inviter_telegram_id": row.get("inviter_telegram_id"),
            "inviter_username": row.get("inviter_username"),
            "inviter_username_lower": row.get("inviter_username_lower"),
            "invitee_user_id": row.get("invitee_user_id"),
            "invitee_telegram_id": row.get("invitee_telegram_id"),
            "invitee_username": row.get("invitee_username"),
            "invitee_username_lower": row.get("invitee_username_lower"),
            "referral_status": _referral_status(row.get("ledger_status")),
            "referred_at": row.get("created_at"),
            "qualified_at": row.get("issued_at") or row.get("claimed_at"),
            "voucher_code": row.get("voucher_code"),
            "coupon_code": row.get("coupon_code"),
            "code": row.get("code"),
            "year_month": row.get("year_month"),
            "source_collection": _cfg()["affiliate_ledger_collection"],
            "source_ledger_id": row.get("ledger_id"),
        }
    return list(dedup.values())


def _load_checkpoint(db_ref=None):
    db_ref = db_ref or db
    return db_ref.export_state.find_one({"_id": "affiliate_dashboard:last_success"}) or {}


def _save_checkpoint(exported_at: datetime, db_ref=None):
    db_ref = db_ref or db
    db_ref.export_state.update_one({"_id": "affiliate_dashboard:last_success"}, {"$set": {"updated_at": exported_at}}, upsert=True)


def run_affiliate_dashboard_export(*, db_ref=None, webhook_url: str | None = None):
    db_ref = db_ref or db
    cfg = _cfg()
    webhook_url = (webhook_url or cfg["sheets_webhook_url"]).strip()
    ensure_affiliate_dashboard_indexes(db_ref)
    export_run_id = uuid4().hex
    exported_at = datetime.now(timezone.utc)
    cp = _load_checkpoint(db_ref)
    last_success = cp.get("updated_at")
    query = {"status": {"$in": sorted(QUALIFIED_STATUSES | PENDING_STATUSES)}}
    bounded = bool(last_success)
    if bounded:
        query["updated_at"] = {"$gt": last_success}
    elif not cfg["allow_full_export_rebuild"]:
        logger.warning("[AFF_DASH_EXPORT] skipped reason=no_checkpoint_no_full_rebuild")
        return {"status": "skipped"}
    else:
        logger.warning("[AFF_DASH_EXPORT] full_rebuild_enabled")
    cursor = db_ref[cfg["affiliate_ledger_collection"]].find(query, PROJECTION).sort("updated_at", ASCENDING).batch_size(500)
    docs = []
    for i, doc in enumerate(cursor, start=1):
        docs.append(doc)
        if i >= cfg["reporting_max_major_read_rows"]:
            logger.warning("[AFF_DASH_EXPORT] truncated max_rows=%s", cfg["reporting_max_major_read_rows"])
            break
    welcome_rows = build_welcome_rows(docs, export_run_id=export_run_id, exported_at=exported_at)
    referral_rows = build_referral_rows(welcome_rows)
    payload = {
        "export_run_id": export_run_id,
        "exported_at": exported_at.isoformat(),
        "tabs": {
            "raw_welcome_code_map": welcome_rows,
            "raw_referral_map": referral_rows,
        },
    }
    if webhook_url:
        requests.post(webhook_url, json=payload, timeout=20).raise_for_status()
    _save_checkpoint(exported_at, db_ref)
    logger.info("[AFF_DASH_EXPORT] done bounded=%s rows_welcome=%s rows_referral=%s", bounded, len(welcome_rows), len(referral_rows))
    return payload


def _previous_completed_year_month(now_utc: datetime, tz_name: str) -> str:
    tz = pytz.timezone(tz_name)
    now_local = now_utc.astimezone(tz)
    first_of_month = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_day = first_of_month - timedelta(days=1)
    return prev_day.strftime("%Y%m")


def run_affiliate_dashboard_export_monthly_scheduled(now_utc_ts: datetime | None = None):
    cfg = _cfg()
    tz_name = os.getenv("SCHEDULER_CRON_TIMEZONE", "Asia/Kuala_Lumpur")
    now_utc_ts = now_utc_ts or datetime.now(timezone.utc)
    target_year_month = _previous_completed_year_month(now_utc_ts, tz_name)
    logger.info("[AFF_DASH_EXPORT] monthly affiliate dashboard export started timezone=%s target_year_month=%s query_mode=monthly_snapshot target_tabs=raw_welcome_code_map,raw_referral_map", tz_name, target_year_month)
    try:
        ensure_affiliate_dashboard_indexes()
        export_run_id = uuid4().hex
        exported_at = datetime.now(timezone.utc)
        query = {"year_month": target_year_month, "status": {"$in": sorted(QUALIFIED_STATUSES | PENDING_STATUSES)}}
        cursor = db[cfg["affiliate_ledger_collection"]].find(query, PROJECTION).sort("updated_at", ASCENDING).batch_size(500)
        docs = []
        for i, doc in enumerate(cursor, start=1):
            docs.append(doc)
            if i >= cfg["reporting_max_major_read_rows"]:
                logger.warning("[AFF_DASH_EXPORT] monthly_snapshot truncated max_rows=%s", cfg["reporting_max_major_read_rows"])
                break
        welcome_rows = build_welcome_rows(docs, export_run_id=export_run_id, exported_at=exported_at)
        referral_rows = build_referral_rows(welcome_rows)
        payload = {
            "export_run_id": export_run_id,
            "exported_at": exported_at.isoformat(),
            "target_year_month": target_year_month,
            "tabs": {"raw_welcome_code_map": welcome_rows, "raw_referral_map": referral_rows},
        }
        if cfg["sheets_webhook_url"]:
            requests.post(cfg["sheets_webhook_url"], json=payload, timeout=20).raise_for_status()
        logger.info("[AFF_DASH_EXPORT] monthly export success target_year_month=%s rows_welcome=%s rows_referral=%s", target_year_month, len(welcome_rows), len(referral_rows))
        return payload
    except Exception:
        logger.exception("[AFF_DASH_EXPORT] monthly export failure target_year_month=%s", target_year_month)
        raise


if __name__ == "__main__":
    run_affiliate_dashboard_export_monthly_scheduled()
