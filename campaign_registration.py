"""Campaign Registration — reusable "register once for the active campaign"
flow for the Mini App (e.g. the Community Lucky Draw).

Design notes
------------
This module deliberately does NOT introduce a new campaign type. Every
Campaign Centre campaign (``gc_campaigns``, see campaign_centre.py) can carry
an optional ``registration`` config block; when present and enabled, the Mini
App will prompt eligible users to register once for that campaign,
independent of whether they arrived through a deep link. Reusing the
existing ``gc_campaigns`` document keeps campaign_centre.py the single admin
CRUD surface (status, schedule, telegram/channel config) instead of a
second, parallel campaign config table.

Two dedicated collections are added:

  * ``campaign_registrations`` — one row per (campaign_id, telegram_user_id),
    unique-indexed, the source of truth for "has this user registered".
  * ``campaign_registration_state`` — lightweight dismissal/reminder state,
    unique-indexed the same way, kept separate from any referral/voucher
    ledger.

Telegram identity (user id + username) is ALWAYS derived server-side from
verified initData via ``miniapp_identity.resolve_authenticated_telegram_user_id``
(see vouchers.verify_telegram_init_data) — never trusted from the request
body. Official-channel verification reuses ``subscription_gate`` rather than
re-implementing Telegram membership checks.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from datetime import datetime, timedelta, timezone

from flask import Blueprint, Response, jsonify, request

import database
from time_utils import as_aware_utc

logger = logging.getLogger(__name__)

campaign_registration_bp = Blueprint("campaign_registration", __name__)
campaign_registration_admin_bp = Blueprint("campaign_registration_admin", __name__)

REGISTRATIONS_COLLECTION = "campaign_registrations"
STATE_COLLECTION = "campaign_registration_state"

REQUIRED_FIELD_KEYS = ["full_name", "contact_number", "country_region", "delivery_address"]

# Reasonable max lengths — not Malaysia-specific, just abuse guards.
_FIELD_MAX_LEN = {
    "full_name": 120,
    "contact_number": 32,
    "country_region": 80,
    "delivery_address": 300,
}
_FIELD_MIN_LEN = 1

DEFAULT_REMINDER_HOURS = 24
DEFAULT_BASE_ENTRIES = 1

# Errors from subscription_gate._get_chat_member that mean "we could not
# reach Telegram", never "confirmed not subscribed" — must not be shown to
# the user as a subscription failure.
_SUBSCRIPTION_SYSTEM_ERROR_PREFIXES = ("network_error", "http_", "rate_limited", "bad_json",
                                       "max_attempts_exceeded", "missing_bot_token")


def _ensure_indexes() -> None:
    try:
        regs = database.db[REGISTRATIONS_COLLECTION]
        regs.create_index([("campaign_id", 1), ("telegram_user_id", 1)],
                           name="ux_campaign_registrations_campaign_user", unique=True)
        regs.create_index([("campaign_id", 1)], name="ix_campaign_registrations_campaign")
        regs.create_index([("telegram_user_id", 1)], name="ix_campaign_registrations_user")
        regs.create_index([("registered_at", -1)], name="ix_campaign_registrations_registered_at")

        state = database.db[STATE_COLLECTION]
        state.create_index([("campaign_id", 1), ("telegram_user_id", 1)],
                            name="ux_campaign_registration_state_campaign_user", unique=True)
    except Exception:
        logger.warning("[CAMPAIGN_REGISTRATION] index creation failed", exc_info=True)


_ensure_indexes()


# ---------------------------------------------------------------------------
# Registration config (stored on gc_campaigns.registration)
# ---------------------------------------------------------------------------

def default_registration_config() -> dict:
    return {
        "enabled": False,
        "miniapp_visible": True,
        "modal_enabled": True,
        "required_fields": list(REQUIRED_FIELD_KEYS),
        "reminder_hours": DEFAULT_REMINDER_HOURS,
        "audience": {"scope": "all", "regions": []},
        "shipping": {"scope": "all", "regions": []},
        "require_channel_subscription": False,
        "base_entries": DEFAULT_BASE_ENTRIES,
    }


def _validate_scope_block(raw: dict | None, *, field: str) -> tuple[dict | None, str | None]:
    raw = raw or {}
    scope = (raw.get("scope") or "all").strip()
    if scope not in ("all", "selected"):
        return None, f"invalid_{field}_scope"
    regions = raw.get("regions") or []
    if not isinstance(regions, list):
        return None, f"invalid_{field}_regions"
    clean_regions = []
    for r in regions:
        r = str(r or "").strip()
        if r:
            clean_regions.append(r)
    if scope == "selected" and not clean_regions:
        return None, f"{field}_regions_required"
    return {"scope": scope, "regions": clean_regions}, None


def validate_registration_config(raw: dict | None, *, partial: bool = False) -> tuple[dict | None, str | None]:
    """Validates the admin-supplied ``registration`` config block.

    Mirrors mission_pool.validate_mission_config's (config, error_code)
    contract so campaign_centre._validate_body can delegate to this exactly
    the way it already delegates to mission_pool for Mission Pool config.
    ``partial`` merges are handled by the caller (campaign_centre), which
    passes the full merged block back in on update — this function always
    validates a complete block.
    """
    raw = raw or {}
    cfg = default_registration_config()

    cfg["enabled"] = bool(raw.get("enabled", cfg["enabled"]))
    cfg["miniapp_visible"] = bool(raw.get("miniapp_visible", cfg["miniapp_visible"]))
    cfg["modal_enabled"] = bool(raw.get("modal_enabled", cfg["modal_enabled"]))
    cfg["require_channel_subscription"] = bool(
        raw.get("require_channel_subscription", cfg["require_channel_subscription"])
    )

    if "required_fields" in raw:
        fields = raw.get("required_fields") or []
        if not isinstance(fields, list) or not fields:
            return None, "invalid_required_fields"
        for f in fields:
            if f not in REQUIRED_FIELD_KEYS:
                return None, "invalid_required_fields"
        cfg["required_fields"] = list(dict.fromkeys(fields))
    else:
        cfg["required_fields"] = list(REQUIRED_FIELD_KEYS)

    if "reminder_hours" in raw:
        try:
            hours = int(raw.get("reminder_hours"))
        except (TypeError, ValueError):
            return None, "invalid_reminder_hours"
        if hours < 1 or hours > 24 * 30:
            return None, "invalid_reminder_hours"
        cfg["reminder_hours"] = hours

    if "base_entries" in raw:
        try:
            base_entries = int(raw.get("base_entries"))
        except (TypeError, ValueError):
            return None, "invalid_base_entries"
        if base_entries < 0 or base_entries > 1000:
            return None, "invalid_base_entries"
        cfg["base_entries"] = base_entries

    audience, err = _validate_scope_block(raw.get("audience"), field="audience")
    if err:
        return None, err
    cfg["audience"] = audience

    shipping, err = _validate_scope_block(raw.get("shipping"), field="shipping")
    if err:
        return None, err
    cfg["shipping"] = shipping

    return cfg, None


def registration_is_open(campaign: dict, now: datetime | None = None) -> bool:
    """Whether ``campaign`` currently accepts registrations: live status,
    registration.enabled + miniapp_visible, and inside the schedule window.
    Deliberately independent of Campaign Centre's ``destination``/provider
    readiness gate (registration.py's own campaign_centre) — registration has
    no external destination to be ready."""
    if not campaign:
        return False
    reg = campaign.get("registration") or {}
    if not reg.get("enabled") or not reg.get("miniapp_visible"):
        return False
    if campaign.get("status") != "live":
        return False

    now = as_aware_utc(now or datetime.now(timezone.utc))
    schedule = campaign.get("schedule") or {}
    starts_at = as_aware_utc(schedule.get("starts_at"))
    if not starts_at or starts_at > now:
        return False
    ends_at = as_aware_utc(schedule.get("ends_at"))
    if ends_at and now >= ends_at:
        return False
    return True


def find_active_registration_campaign(now: datetime | None = None) -> dict | None:
    """The single campaign (highest priority) currently open for
    registration, or None. A normal Mini App open with no active campaign
    costs one indexed query and renders nothing."""
    now = now or datetime.now(timezone.utc)
    docs = database.db["gc_campaigns"].find(
        {"status": "live", "registration.enabled": True, "registration.miniapp_visible": True},
        sort=[("priority", -1), ("schedule.starts_at", 1)],
        limit=20,
    )
    for doc in docs:
        if registration_is_open(doc, now):
            return doc
    return None


# ---------------------------------------------------------------------------
# Registration / dismissal state helpers
# ---------------------------------------------------------------------------

def get_registration(campaign_id: str, telegram_user_id: int) -> dict | None:
    return database.db[REGISTRATIONS_COLLECTION].find_one(
        {"campaign_id": campaign_id, "telegram_user_id": telegram_user_id}
    )


def get_dismissal_state(campaign_id: str, telegram_user_id: int) -> dict | None:
    return database.db[STATE_COLLECTION].find_one(
        {"campaign_id": campaign_id, "telegram_user_id": telegram_user_id}
    )


def _sanitize_text(value, *, field: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    max_len = _FIELD_MAX_LEN.get(field, 300)
    return text[:max_len]


def _validate_registration_fields(body: dict, required_fields: list[str]) -> tuple[dict | None, str | None]:
    clean: dict = {}
    for field in REQUIRED_FIELD_KEYS:
        value = _sanitize_text(body.get(field), field=field)
        clean[field] = value
        if field in required_fields and len(value) < _FIELD_MIN_LEN:
            return None, f"missing_{field}"
    return clean, None


def _subscription_check(campaign: dict, telegram_user_id: int) -> tuple[bool, str | None]:
    """Returns (ok, error_code). error_code is only set for a genuine
    blocking failure — a transient Telegram/network error is distinguished
    from a confirmed non-subscription so callers never tell a user they are
    "not subscribed" because of a system error."""
    from subscription_gate import verify_campaign_subscription

    gate = verify_campaign_subscription(campaign, telegram_user_id)
    if gate.get("subscribed"):
        return True, None
    reason = str(gate.get("reason") or "")
    if gate.get("source") == "live" and reason.startswith(_SUBSCRIPTION_SYSTEM_ERROR_PREFIXES):
        return False, "subscription_check_failed"
    return False, "channel_subscription_required"


def _serialize_registration(doc: dict) -> dict:
    return {
        "campaign_id": doc.get("campaign_id"),
        "telegram_user_id": doc.get("telegram_user_id"),
        "telegram_username": doc.get("telegram_username"),
        "full_name": doc.get("full_name"),
        "contact_number": doc.get("contact_number"),
        "country_region": doc.get("country_region"),
        "delivery_address": doc.get("delivery_address"),
        "channel_verified": bool(doc.get("channel_verified")),
        "base_entries": doc.get("base_entries", DEFAULT_BASE_ENTRIES),
        "status": doc.get("status"),
        "registered_at": doc["registered_at"].isoformat() if doc.get("registered_at") else None,
    }


def _public_campaign_fields(campaign: dict) -> dict:
    reg = campaign.get("registration") or {}
    return {
        "campaign_id": campaign.get("campaign_id"),
        "name": campaign.get("name"),
        "description": campaign.get("description", ""),
        "required_fields": reg.get("required_fields", REQUIRED_FIELD_KEYS),
        "reminder_hours": reg.get("reminder_hours", DEFAULT_REMINDER_HOURS),
        "base_entries": reg.get("base_entries", DEFAULT_BASE_ENTRIES),
        "require_channel_subscription": bool(reg.get("require_channel_subscription")),
        "channel_username": (campaign.get("telegram") or {}).get("channel_username", ""),
    }


# ---------------------------------------------------------------------------
# Public Mini App API
# ---------------------------------------------------------------------------

@campaign_registration_bp.get("/api/campaign-registration/active")
def active_campaign_registration():
    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    resp = jsonify(_active_registration_payload(uid))
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _active_registration_payload(uid: int) -> dict:
    campaign = find_active_registration_campaign()
    if not campaign:
        return {"status": "ok", "campaign": None, "registered": False, "should_prompt": False}

    campaign_id = campaign["campaign_id"]
    registration = get_registration(campaign_id, uid)
    if registration:
        return {
            "status": "ok",
            "campaign": _public_campaign_fields(campaign),
            "registered": True,
            "should_prompt": False,
            "registration": _serialize_registration(registration),
        }

    reminder_hours = (campaign.get("registration") or {}).get("reminder_hours", DEFAULT_REMINDER_HOURS)
    state = get_dismissal_state(campaign_id, uid)
    now = datetime.now(timezone.utc)
    next_prompt_at = as_aware_utc(state.get("next_prompt_at")) if state else None
    dismissed_until = next_prompt_at.isoformat() if next_prompt_at and next_prompt_at > now else None
    should_prompt = not dismissed_until

    payload = {
        "status": "ok",
        "campaign": _public_campaign_fields(campaign),
        "registered": False,
        "should_prompt": should_prompt,
        "reminder_hours": reminder_hours,
    }
    if dismissed_until:
        payload["dismissed_until"] = dismissed_until
        payload["next_prompt_at"] = dismissed_until
    return payload


@campaign_registration_bp.post("/api/campaign-registration/<campaign_id>/register")
def register_for_campaign(campaign_id: str):
    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    from campaign_centre import get_campaign, log_funnel_event

    campaign = get_campaign(campaign_id)
    if not campaign or not registration_is_open(campaign):
        return jsonify({"status": "error", "code": "registration_unavailable"}), 404

    # Idempotent retry: an already-successful registration for this user is
    # returned as a success, never a duplicate/error, so a client retry after
    # a dropped response cannot surface a false failure.
    existing = get_registration(campaign_id, uid)
    if existing:
        return jsonify({"status": "ok", "already_registered": True,
                         "registration": _serialize_registration(existing)})

    body = request.get_json(force=True, silent=True) or {}
    reg_cfg = campaign.get("registration") or {}
    required_fields = reg_cfg.get("required_fields") or REQUIRED_FIELD_KEYS

    fields, code = _validate_registration_fields(body, required_fields)
    if code:
        return jsonify({"status": "error", "code": code}), 400

    audience = reg_cfg.get("audience") or {"scope": "all", "regions": []}
    if audience.get("scope") == "selected":
        allowed = {r.strip().lower() for r in audience.get("regions") or []}
        if fields["country_region"].strip().lower() not in allowed:
            return jsonify({"status": "error", "code": "region_not_eligible"}), 403

    channel_verified = False
    if reg_cfg.get("require_channel_subscription"):
        ok, sub_err = _subscription_check(campaign, uid)
        if sub_err == "subscription_check_failed":
            return jsonify({"status": "error", "code": "subscription_check_failed"}), 503
        if sub_err:
            return jsonify({
                "status": "error", "code": "channel_subscription_required",
                "channel_username": (campaign.get("telegram") or {}).get("channel_username", ""),
            }), 403
        channel_verified = ok

    try:
        username = _extract_username(body)
    except Exception:
        username = ""

    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": campaign_id,
        "telegram_user_id": uid,
        "telegram_username": username,
        **fields,
        "channel_verified": channel_verified,
        "base_entries": reg_cfg.get("base_entries", DEFAULT_BASE_ENTRIES),
        "status": "registered",
        "registered_at": now,
        "updated_at": now,
    }
    try:
        database.db[REGISTRATIONS_COLLECTION].insert_one(doc)
    except Exception as exc:
        if "duplicate" in str(exc).lower():
            # A concurrent request won the race; the unique index is the
            # authority, so this is a success from the caller's perspective.
            existing = get_registration(campaign_id, uid)
            return jsonify({"status": "ok", "already_registered": True,
                             "registration": _serialize_registration(existing)}) if existing else \
                (jsonify({"status": "error", "code": "internal_error"}), 500)
        logger.exception("[CAMPAIGN_REGISTRATION] insert_failed")
        return jsonify({"status": "error", "code": "internal_error"}), 500

    # Registering permanently clears any dismissal suppression state.
    database.db[STATE_COLLECTION].delete_one({"campaign_id": campaign_id, "telegram_user_id": uid})

    log_funnel_event("registration_completed", campaign_id=campaign_id, user_id=uid, source="miniapp")

    return jsonify({
        "status": "ok",
        "already_registered": False,
        "registration": _serialize_registration(doc),
    }), 201


def _extract_username(body: dict) -> str:
    """Telegram username is derived from verified initData, never the
    request body. Mirrors miniapp_identity.resolve_authenticated_telegram_user_id's
    query-then-body-fallback lookup exactly, so a request authenticated via
    either path still resolves a real (verified) username here."""
    from vouchers import extract_raw_init_data_from_query, verify_telegram_init_data
    import json as _json

    init_data_raw = extract_raw_init_data_from_query(request)
    if not init_data_raw:
        init_data_raw = body.get("init_data", "")
    if not init_data_raw:
        return ""
    ok, data, _reason = verify_telegram_init_data(init_data_raw)
    if not ok:
        return ""
    try:
        user = _json.loads((data or {}).get("user", "{}"))
        return str(user.get("username") or "").strip()
    except Exception:
        return ""


@campaign_registration_bp.post("/api/campaign-registration/<campaign_id>/dismiss")
def dismiss_campaign_registration(campaign_id: str):
    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    from campaign_centre import get_campaign

    campaign = get_campaign(campaign_id)
    reminder_hours = DEFAULT_REMINDER_HOURS
    if campaign:
        reminder_hours = (campaign.get("registration") or {}).get("reminder_hours", DEFAULT_REMINDER_HOURS)

    now = datetime.now(timezone.utc)
    next_prompt_at = now + timedelta(hours=reminder_hours)
    database.db[STATE_COLLECTION].update_one(
        {"campaign_id": campaign_id, "telegram_user_id": uid},
        {"$set": {
            "campaign_id": campaign_id,
            "telegram_user_id": uid,
            "dismissed_at": now,
            "next_prompt_at": next_prompt_at,
        }},
        upsert=True,
    )
    resp = jsonify({"status": "ok", "next_prompt_at": next_prompt_at.isoformat()})
    resp.headers["Cache-Control"] = "no-store"
    return resp


# ---------------------------------------------------------------------------
# Deep link (mirrors mission_pool_ux.py's canonical-link pattern)
# ---------------------------------------------------------------------------

CAMPAIGN_REG_START_PARAM_PREFIX = "campaign_"
_TELEGRAM_START_PARAM_SAFE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def campaign_id_is_link_safe(campaign_id: str | None) -> bool:
    if not campaign_id:
        return False
    return bool(_TELEGRAM_START_PARAM_SAFE.match(CAMPAIGN_REG_START_PARAM_PREFIX + campaign_id))


def campaign_start_param(campaign_id: str) -> str:
    return CAMPAIGN_REG_START_PARAM_PREFIX + campaign_id


def parse_campaign_start_param(raw: str | None) -> str | None:
    """Inverse of campaign_start_param. Returns None for anything that is
    not a ``campaign_`` start parameter, including the pre-existing
    ``mission_``/``attr_`` prefixes, which must keep working unchanged."""
    if not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not raw.startswith(CAMPAIGN_REG_START_PARAM_PREFIX):
        return None
    campaign_id = raw[len(CAMPAIGN_REG_START_PARAM_PREFIX):]
    if not campaign_id or not campaign_id_is_link_safe(campaign_id):
        return None
    return campaign_id


def campaign_deep_link(campaign_id: str) -> str | None:
    import os

    bot_username = (os.environ.get("BOT_USERNAME") or "").strip().lstrip("@")
    if not bot_username or not campaign_id_is_link_safe(campaign_id):
        return None
    return f"https://t.me/{bot_username}?startapp={campaign_start_param(campaign_id)}"


# ---------------------------------------------------------------------------
# Admin: registrations list / summary / CSV export
# ---------------------------------------------------------------------------

def _require_admin():
    from vouchers import require_admin

    return require_admin()


_CSV_FIELDS = [
    "campaign_id", "telegram_user_id", "telegram_username", "full_name", "contact_number",
    "country_region", "delivery_address", "channel_verified", "base_entries", "registered_at", "status",
]


def _admin_query() -> dict:
    query: dict = {}
    campaign_id = (request.args.get("campaign_id") or "").strip()
    if campaign_id:
        query["campaign_id"] = campaign_id
    region = (request.args.get("region") or "").strip()
    if region:
        query["country_region"] = {"$regex": re.escape(region), "$options": "i"}
    channel_verified = (request.args.get("channel_verified") or "").strip().lower()
    if channel_verified in ("true", "false"):
        query["channel_verified"] = channel_verified == "true"
    search = (request.args.get("q") or "").strip()
    if search:
        rx = {"$regex": re.escape(search), "$options": "i"}
        query["$or"] = [
            {"full_name": rx}, {"telegram_username": rx}, {"contact_number": rx},
        ]
        if search.isdigit():
            query["$or"].append({"telegram_user_id": int(search)})
    return query


@campaign_registration_admin_bp.get("/api/admin/campaign-registrations")
def list_campaign_registrations():
    _, err = _require_admin()
    if err:
        return err

    query = _admin_query()
    page = max(1, request.args.get("page", default=1, type=int) or 1)
    page_size = min(200, max(1, request.args.get("page_size", default=50, type=int) or 50))

    total = database.db[REGISTRATIONS_COLLECTION].count_documents(query)
    docs = list(
        database.db[REGISTRATIONS_COLLECTION].find(
            query, sort=[("registered_at", -1)], skip=(page - 1) * page_size, limit=page_size
        )
    )
    return jsonify({
        "status": "ok",
        "total": total,
        "page": page,
        "page_size": page_size,
        "registrations": [_serialize_registration(d) for d in docs],
    })


@campaign_registration_admin_bp.get("/api/admin/campaign-registrations/summary")
def campaign_registrations_summary():
    _, err = _require_admin()
    if err:
        return err

    campaign_id = (request.args.get("campaign_id") or "").strip()
    query = {"campaign_id": campaign_id} if campaign_id else {}

    total = database.db[REGISTRATIONS_COLLECTION].count_documents(query)
    channel_verified = database.db[REGISTRATIONS_COLLECTION].count_documents({**query, "channel_verified": True})

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_query = {**query, "registered_at": {"$gte": today_start}}
    today = database.db[REGISTRATIONS_COLLECTION].count_documents(today_query)

    campaign_status = None
    if campaign_id:
        from campaign_centre import get_campaign

        campaign = get_campaign(campaign_id)
        campaign_status = campaign.get("status") if campaign else None

    return jsonify({
        "status": "ok",
        "total_registrations": total,
        "registrations_today": today,
        "channel_verified": channel_verified,
        "campaign_status": campaign_status,
    })


@campaign_registration_admin_bp.get("/api/admin/campaign-registrations/export")
def export_campaign_registrations():
    _, err = _require_admin()
    if err:
        return err

    query = _admin_query()
    docs = database.db[REGISTRATIONS_COLLECTION].find(query, sort=[("registered_at", -1)], limit=50000)

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=_CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for d in docs:
        row = _serialize_registration(d)
        writer.writerow(row)

    filename = f"campaign_registrations_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"', "Cache-Control": "no-store"},
    )
