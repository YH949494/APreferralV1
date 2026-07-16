"""External subscription-voucher website integration (Phase 7).

AP only verifies Telegram identity + official-channel subscription for this
campaign type — the external website issues its own voucher. AP never
returns or issues a voucher code here.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from campaign_centre import get_campaign, is_publicly_active
from campaign_providers import get_provider

logger = logging.getLogger(__name__)

subscription_verification_bp = Blueprint("subscription_verification", __name__)

_RATE_WINDOW_S = 60
_RATE_MAX_ATTEMPTS = 20
_rate_state: dict[str, list[float]] = {}


def _rate_limited(key: str) -> bool:
    now = time.time()
    attempts = [t for t in _rate_state.get(key, []) if now - t < _RATE_WINDOW_S]
    limited = len(attempts) >= _RATE_MAX_ATTEMPTS
    attempts.append(now)
    _rate_state[key] = attempts
    if len(_rate_state) > 20000:
        _rate_state.clear()
    return limited


def _client_ip() -> str:
    return request.headers.get("Fly-Client-IP") or request.remote_addr or "unknown"


@subscription_verification_bp.post("/api/integrations/subscription/verify")
def verify_subscription_campaign():
    if _rate_limited(_client_ip()):
        return jsonify({"ok": False, "code": "rate_limited"}), 429

    body = request.get_json(force=True, silent=True) or {}
    campaign_id = str(body.get("campaign_id") or "").strip()
    init_data_raw = body.get("init_data") or ""

    if not campaign_id or not init_data_raw:
        return jsonify({"ok": False, "code": "missing_fields"}), 400

    campaign = get_campaign(campaign_id)
    if not campaign:
        return jsonify({"ok": False, "code": "campaign_not_found"}), 404
    if campaign.get("type") != "external_subscription_verification":
        return jsonify({"ok": False, "code": "wrong_campaign_type"}), 400

    provider = get_provider((campaign.get("destination") or {}).get("provider_id") or "")
    if not is_publicly_active(campaign, provider):
        return jsonify({"ok": False, "code": "campaign_not_active"}), 400

    from vouchers import verify_telegram_init_data

    ok, data, reason = verify_telegram_init_data(init_data_raw)
    if not ok:
        return jsonify({"ok": False, "code": f"init_data_invalid:{reason}"}), 401

    try:
        user_json = json.loads(data.get("user", "{}"))
        telegram_user_id = int(user_json.get("id"))
    except Exception:
        return jsonify({"ok": False, "code": "invalid_user"}), 401

    from subscription_gate import verify_campaign_subscription

    gate = verify_campaign_subscription(campaign, telegram_user_id)
    checked_at = datetime.now(timezone.utc)

    from campaign_events import emit_campaign_event

    emit_campaign_event(
        event_type="subscription_pass" if gate.get("subscribed") else "subscription_fail",
        campaign_id=campaign_id,
        campaign_type=campaign.get("type"),
        telegram_user_id=telegram_user_id,
        source="external_subscription_verify",
        status="success" if gate.get("subscribed") else "fail",
    )

    return jsonify({
        "ok": True,
        "campaign_id": campaign_id,
        "telegram_user_id": telegram_user_id,
        "subscribed": bool(gate.get("subscribed", False)),
        "checked_at": checked_at.isoformat(),
    })
