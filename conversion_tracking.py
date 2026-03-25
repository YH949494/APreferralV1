from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone

import requests
from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger(__name__)

META_GRAPH_URL = os.getenv("META_GRAPH_URL", "https://graph.facebook.com/v18.0")
TIKTOK_EVENTS_URL = os.getenv("TIKTOK_EVENTS_URL", "https://business-api.tiktok.com/open_api/v1.3/event/track/")
TRACKING_TIMEOUT_SECONDS = float(os.getenv("TRACKING_TIMEOUT_SECONDS", "6"))


def sha256_hex(value) -> str:
    if value is None:
        value = ""
    if not isinstance(value, str):
        value = str(value)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def reserve_conversion_event(conversion_events_col, *, network: str, event_name: str, user_id: int, drop_id: str, event_id: str):
    key = f"{network}|welcome_claim|{user_id}|{drop_id}"
    now = datetime.now(timezone.utc)
    existing = conversion_events_col.find_one({"key": key})
    if existing:
        return existing, False
    doc = {
        "key": key,
        "network": network,
        "event_name": event_name,
        "user_id": int(user_id),
        "drop_id": str(drop_id),
        "event_id": event_id,
        "status": "pending",
        "http_status": None,
        "response_text": "",
        "created_at": now,
        "updated_at": now,
    }
    try:
        conversion_events_col.insert_one(doc)
    except DuplicateKeyError:
        existing = conversion_events_col.find_one({"key": key})
        return existing, False
    except PyMongoError:
        logger.exception("[TRACKING] reserve_failed network=%s uid=%s drop_id=%s", network, user_id, drop_id)
        raise
    return doc, True


def mark_conversion_event(conversion_events_col, *, key: str, status: str, http_status=None, response_text=""):
    conversion_events_col.update_one(
        {"key": key},
        {
            "$set": {
                "status": status,
                "http_status": http_status,
                "response_text": (response_text or "")[:4000],
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )


def _tracking_base_url() -> str:
    return (os.getenv("TRACKING_BASE_URL") or "").strip()


def _event_source_url(attribution: dict | None) -> str:
    attribution = attribution or {}
    landing_url = (attribution.get("landing_url") or "").strip()
    if landing_url:
        return landing_url
    return _tracking_base_url()


def send_meta_complete_registration(*, event_id: str, user_id: int, drop_id: str, attribution: dict | None):
    pixel_id = (os.getenv("META_PIXEL_ID") or "").strip()
    access_token = (os.getenv("META_CAPI_ACCESS_TOKEN") or "").strip()
    if not pixel_id or not access_token:
        return {"ok": False, "skipped": True, "reason": "missing_meta_env", "http_status": None, "response_text": ""}

    attribution = attribution or {}
    user_data = {"external_id": sha256_hex(user_id)}
    for source_key, payload_key in (
        ("fbc", "fbc"),
        ("fbp", "fbp"),
        ("ip", "client_ip_address"),
        ("user_agent", "client_user_agent"),
    ):
        value = (attribution.get(source_key) or "").strip() if isinstance(attribution.get(source_key), str) else attribution.get(source_key)
        if value:
            user_data[payload_key] = value

    payload = {
        "data": [
            {
                "event_name": "CompleteRegistration",
                "event_time": int(datetime.now(timezone.utc).timestamp()),
                "event_id": event_id,
                "action_source": "website",
                "event_source_url": _event_source_url(attribution),
                "user_data": user_data,
                "custom_data": {
                    "content_name": "welcome_voucher_claim",
                    "drop_id": str(drop_id),
                    "status": "claimed",
                },
            }
        ]
    }
    url = f"{META_GRAPH_URL.rstrip('/')}/{pixel_id}/events"
    try:
        resp = requests.post(url, params={"access_token": access_token}, json=payload, timeout=TRACKING_TIMEOUT_SECONDS)
        return {"ok": resp.ok, "http_status": resp.status_code, "response_text": resp.text[:4000], "skipped": False}
    except requests.RequestException as exc:
        return {"ok": False, "http_status": None, "response_text": str(exc), "skipped": False}


def send_tiktok_complete_registration(*, event_id: str, user_id: int, drop_id: str, attribution: dict | None):
    pixel_code = (os.getenv("TIKTOK_PIXEL_CODE") or "").strip()
    access_token = (os.getenv("TIKTOK_EVENTS_ACCESS_TOKEN") or "").strip()
    if not pixel_code or not access_token:
        return {"ok": False, "skipped": True, "reason": "missing_tiktok_env", "http_status": None, "response_text": ""}

    attribution = attribution or {}
    event = {
        "event": "CompleteRegistration",
        "event_id": event_id,
        "event_time": int(datetime.now(timezone.utc).timestamp()),
        "event_source": "web",
        "event_source_id": pixel_code,
        "user": {"external_id": sha256_hex(user_id)},
        "page": {},
        "properties": {
            "content_name": "welcome_voucher_claim",
            "drop_id": str(drop_id),
            "status": "claimed",
        },
    }
    ttclid = (attribution.get("ttclid") or "").strip() if isinstance(attribution.get("ttclid"), str) else attribution.get("ttclid")
    if ttclid:
        event["ad"] = {"callback": ttclid}
    page_url = _event_source_url(attribution)
    if page_url:
        event["page"]["url"] = page_url
    if not event["page"]:
        event.pop("page", None)

    payload = {"event_source": "web", "event_source_id": pixel_code, "data": [event]}
    headers = {"Access-Token": access_token, "Content-Type": "application/json"}
    try:
        resp = requests.post(TIKTOK_EVENTS_URL, headers=headers, json=payload, timeout=TRACKING_TIMEOUT_SECONDS)
        return {"ok": resp.ok, "http_status": resp.status_code, "response_text": resp.text[:4000], "skipped": False}
    except requests.RequestException as exc:
        return {"ok": False, "http_status": None, "response_text": str(exc), "skipped": False}
