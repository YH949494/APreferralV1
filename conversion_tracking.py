import hashlib
import os
import time

import requests


META_PIXEL_ID = os.getenv("META_PIXEL_ID")
META_CAPI_ACCESS_TOKEN = os.getenv("META_CAPI_ACCESS_TOKEN")
TIKTOK_PIXEL_CODE = os.getenv("TIKTOK_PIXEL_CODE")
TIKTOK_EVENTS_ACCESS_TOKEN = os.getenv("TIKTOK_EVENTS_ACCESS_TOKEN")


def _sha256(value):
    return hashlib.sha256(str(value).encode()).hexdigest() if value else None


def send_meta_event(user_id, attribution):
    if not META_PIXEL_ID or not META_CAPI_ACCESS_TOKEN:
        return
    endpoint = f"https://graph.facebook.com/v18.0/{META_PIXEL_ID}/events"
    fbc = (attribution or {}).get("fbc") or (attribution or {}).get("_fbc")
    fbp = (attribution or {}).get("fbp") or (attribution or {}).get("_fbp")
    payload = {
        "data": [
            {
                "event_name": "CompleteRegistration",
                "event_time": int(time.time()),
                "action_source": "website",
                "user_data": {
                    "external_id": _sha256(user_id),
                    "fbc": fbc,
                    "fbp": fbp,
                },
            }
        ],
        "access_token": META_CAPI_ACCESS_TOKEN,
    }
    try:
        requests.post(endpoint, json=payload, timeout=3)
    except Exception:
        return


def send_tiktok_event(user_id, attribution):
    if not TIKTOK_PIXEL_CODE or not TIKTOK_EVENTS_ACCESS_TOKEN:
        return
    endpoint = "https://business-api.tiktok.com/open_api/v1.3/event/track/"
    payload = {
        "pixel_code": TIKTOK_PIXEL_CODE,
        "event": "CompleteRegistration",
        "timestamp": int(time.time()),
        "context": {
            "user": {
                "external_id": _sha256(user_id),
            }
        },
    }
    headers = {"Access-Token": TIKTOK_EVENTS_ACCESS_TOKEN}
    try:
        requests.post(endpoint, json=payload, headers=headers, timeout=3)
    except Exception:
        return
