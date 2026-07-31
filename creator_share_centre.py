"""Creator Share Centre — creator-facing surface for the referral share package.

This module does NOT rebuild caption/pool/invite-link logic. It is a thin,
access-controlled wrapper around the existing ``referral_share_content``
module: hook/playback selection, the canonical invite link
(``main.get_or_create_referral_invite_link_sync``), and the
``share_generations`` collection are all reused as-is.

Adds:
  - ``creator_members`` collection + admin CRUD (approve/suspend/remove).
  - Creator-only Mini App APIs: status, generate, copied, share-clicked,
    results.
  - Telegram initData authentication + creator authorization, independent
    of admin auth (``vouchers.require_admin``) and independent of the
    general referral/XP system.

Access control: every ``/api/creator/...`` route requires (1) valid
Telegram Mini App initData, (2) an ``active`` ``creator_members`` record for
the authenticated user_id, and (3) — when ``CREATOR_GROUP_CHAT_ID`` is
configured — a live (short-cached) Telegram membership check against that
group. user_id is always derived from verified initData; a ``user_id`` sent
in the request body/query is never trusted.
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument

import database
from referral_rate_limit import consume_referral_rate_limits
from referral_share_content import build_creator_share_text, generate_share_package, now_utc

logger = logging.getLogger(__name__)

creator_share_bp = Blueprint("creator_share_centre", __name__)

ALLOWED_PLATFORMS = {"generic", "whatsapp", "facebook", "x", "telegram"}
CREATOR_STATUSES = {"active", "suspended", "removed"}

GENERATE_HOURLY_LIMIT = 20
MAX_BULK_CREATOR_IMPORT = 2000

# Short cache period for a *confirmed* live membership check, and a shorter
# grace window used only when Telegram is temporarily unavailable (so a
# transient outage never immediately removes an otherwise-active creator —
# see module docstring + docs/creator-share-centre.md for the full policy).
MEMBERSHIP_VERIFY_CACHE_SEC = 900
MEMBERSHIP_UNRESOLVABLE_GRACE_SEC = 120

_TELEGRAM_MEMBER_STATUSES = {"member", "administrator", "creator"}
_TELEGRAM_NONMEMBER_STATUSES = {"left", "kicked"}


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _ensure_indexes() -> None:
    try:
        col = database.db["creator_members"]
        database.safe_create_index(col, [("user_id", 1)], name="ux_creator_members_user_id", unique=True)
        database.safe_create_index(col, [("status", 1)], name="ix_creator_members_status")
        database.safe_create_index(
            col, [("source_group_id", 1), ("status", 1)], name="ix_creator_members_group_status"
        )

        # Legacy (pre-Creator-Share-Centre) share_generations documents have
        # no package_id field at all; the partial filter keeps this index
        # from ever considering those documents, so they remain valid.
        database.safe_create_index(
            database.db["share_generations"],
            [("package_id", 1)],
            name="ux_share_generations_package_id",
            unique=True,
            partialFilterExpression={"package_id": {"$exists": True}},
        )

        database.safe_create_index(
            database.db["creator_generation_rate_limits"],
            [("key", 1)],
            name="ux_creator_gen_rate_limit_key",
            unique=True,
        )
    except Exception:
        logger.warning("[CREATOR_SHARE] index creation failed", exc_info=True)


_ensure_indexes()


# ---------------------------------------------------------------------------
# Telegram Mini App auth (initData) — independent of admin auth.
# ---------------------------------------------------------------------------

def _extract_authenticated_user():
    """Returns (user_id, username, error) where error is (code, http_status) or None.

    user_id is always derived from verified Telegram initData — a user_id
    sent in JSON/query is never trusted.
    """
    from vouchers import extract_raw_init_data_from_query, verify_telegram_init_data

    init_data = extract_raw_init_data_from_query(request)
    if not init_data:
        return None, None, ("invalid_telegram_auth", 401)
    ok, parsed, _ = verify_telegram_init_data(init_data)
    if not ok:
        return None, None, ("invalid_telegram_auth", 401)

    user_payload = (parsed or {}).get("user", {})
    if isinstance(user_payload, str):
        try:
            user_payload = json.loads(user_payload)
        except (TypeError, ValueError):
            user_payload = {}
    try:
        user_id = int((user_payload or {}).get("id"))
    except (TypeError, ValueError):
        user_id = None
    if not user_id:
        return None, None, ("invalid_telegram_auth", 401)

    username = (user_payload or {}).get("username") or ""
    return user_id, username, None


# ---------------------------------------------------------------------------
# Creator authorization
# ---------------------------------------------------------------------------

def _creator_group_chat_id() -> str | None:
    value = (os.environ.get("CREATOR_GROUP_CHAT_ID") or "").strip()
    return value or None


def _as_aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _check_group_membership(user_id: int, chat_id: str) -> bool | None:
    """Return True/False for a definitive verdict, or None when Telegram is
    temporarily unavailable / the response is ambiguous (never treated as a
    confirmed "not a member" — see module docstring)."""
    import requests

    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        return None
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getChatMember",
            params={"chat_id": chat_id, "user_id": user_id},
            timeout=8,
        )
    except requests.RequestException:
        logger.warning("[CREATOR_SHARE] membership check network error user_id=%s", user_id)
        return None
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    if not data.get("ok"):
        return None
    result = data.get("result") or {}
    status = str(result.get("status") or "").lower()
    if status in _TELEGRAM_MEMBER_STATUSES:
        return True
    if status == "restricted":
        return bool(result.get("is_member", True))
    if status in _TELEGRAM_NONMEMBER_STATUSES:
        return False
    return None


def _verify_live_membership(record: dict, chat_id: str) -> bool | None:
    """True = confirmed member, False = confirmed left/kicked, None = keep
    the existing access (temporarily unresolvable, within the grace window)."""
    now = now_utc()
    last_verified = _as_aware_utc(record.get("last_membership_verified_at"))
    if last_verified and (now - last_verified).total_seconds() < MEMBERSHIP_VERIFY_CACHE_SEC:
        return True

    last_unresolvable = _as_aware_utc(record.get("last_membership_unresolvable_at"))
    verdict = _check_group_membership(record["user_id"], chat_id)
    if verdict is True:
        database.db["creator_members"].update_one(
            {"_id": record["_id"]},
            {"$set": {"last_membership_verified_at": now, "updated_at": now}, "$unset": {"last_membership_unresolvable_at": ""}},
        )
        return True
    if verdict is False:
        return False
    # Telegram lookup temporarily unavailable: use the short cache instead of
    # permanently removing the creator.
    if last_unresolvable and (now - last_unresolvable).total_seconds() < MEMBERSHIP_UNRESOLVABLE_GRACE_SEC:
        return None
    database.db["creator_members"].update_one(
        {"_id": record["_id"]}, {"$set": {"last_membership_unresolvable_at": now, "updated_at": now}}
    )
    return None


def _verify_creator_access(user_id: int):
    """Returns (creator_doc, error) where error is (code, http_status) or None."""
    record = database.db["creator_members"].find_one({"user_id": user_id})
    if not record:
        return None, ("creator_not_authorized", 403)
    if record.get("status") == "suspended":
        return None, ("creator_suspended", 403)
    if record.get("status") != "active":
        return None, ("creator_not_authorized", 403)

    chat_id = _creator_group_chat_id()
    if chat_id:
        verdict = _verify_live_membership(record, chat_id)
        if verdict is False:
            database.db["creator_members"].update_one(
                {"_id": record["_id"]}, {"$set": {"status": "removed", "updated_at": now_utc()}}
            )
            return None, ("creator_membership_required", 403)
        if verdict is None:
            return None, ("creator_membership_unresolvable", 503)

    return record, None


def _authenticate_and_authorize():
    user_id, username, auth_err = _extract_authenticated_user()
    if auth_err:
        return None, None, None, auth_err
    record, access_err = _verify_creator_access(user_id)
    if access_err:
        logger.info("[CREATOR_SHARE][ACCESS_DENIED] user_id=%s reason_code=%s", user_id, access_err[0])
        return user_id, username, None, access_err
    logger.info("[CREATOR_SHARE][ACCESS_GRANTED] user_id=%s", user_id)
    return user_id, username, record, None


# ---------------------------------------------------------------------------
# Creator API
# ---------------------------------------------------------------------------

@creator_share_bp.get("/api/creator/share/status")
def creator_share_status():
    user_id, _username, record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status
    return jsonify(
        {
            "status": "ok",
            "creator": {
                "user_id": user_id,
                "creator_tier": record.get("creator_tier"),
                "access": True,
            },
        }
    )


def _current_week_start_utc(now: datetime) -> datetime:
    monday = now - timedelta(days=now.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


@creator_share_bp.post("/api/creator/share/generate")
def creator_share_generate():
    user_id, username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    body = request.get_json(force=True, silent=True) or {}
    platform = body.get("platform") or "generic"
    if platform not in ALLOWED_PLATFORMS:
        return jsonify({"status": "error", "code": "invalid_platform"}), 400

    allowed, _reason, _meta = consume_referral_rate_limits(
        database.db["creator_generation_rate_limits"],
        inviter_id=user_id,
        now_utc=now_utc(),
        hourly_limit=GENERATE_HOURLY_LIMIT,
        daily_limit=0,
    )
    if not allowed:
        logger.info("[CREATOR_SHARE][RATE_LIMITED] user_id=%s reason_code=creator_generation_rate_limited", user_id)
        return jsonify({"status": "error", "code": "creator_generation_rate_limited"}), 429

    result = generate_share_package(
        user_id, username, generated_by="creator_share_centre", platform=platform
    )
    if not result.get("ok"):
        logger.warning(
            "[CREATOR_SHARE][GENERATION_FAILED] user_id=%s reason_code=%s", user_id, result.get("code")
        )
        return jsonify({"status": "error", "code": result.get("code") or "generation_failed"}), 502

    share_text = build_creator_share_text(
        hook_text=result.get("hook_text"),
        playback_url=result.get("playback_url"),
        referral_link=result.get("invite_link"),
    )

    logger.info(
        "[CREATOR_SHARE][GENERATED] user_id=%s package_id=%s hook_id=%s playback_record_id=%s platform=%s",
        user_id,
        result.get("package_id"),
        result.get("hook_id"),
        result.get("playback_record_id"),
        platform,
    )
    return jsonify(
        {
            "status": "ok",
            "package_id": result.get("package_id"),
            "hook_text": result.get("hook_text"),
            "playback_url": result.get("playback_url"),
            "referral_link": result.get("invite_link"),
            "share_text": share_text,
        }
    )


def _mutate_owned_package(package_id: str, user_id: int, update: dict):
    """Ownership-checked mutation of a share_generations document: the query
    filters on both package_id and the authenticated user_id, so a
    non-owner's request simply matches nothing (same 404 as a nonexistent
    package_id — existence is never leaked)."""
    return database.db["share_generations"].find_one_and_update(
        {"package_id": package_id, "user_id": user_id}, update, return_document=ReturnDocument.AFTER
    )


@creator_share_bp.post("/api/creator/share/<package_id>/copied")
def creator_share_copied(package_id):
    user_id, _username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    body = request.get_json(force=True, silent=True) or {}
    platform = body.get("platform") if body.get("platform") in ALLOWED_PLATFORMS else "generic"

    updated = _mutate_owned_package(
        package_id,
        user_id,
        {"$set": {"copied_at": now_utc(), "latest_copy_platform": platform}, "$inc": {"copy_count": 1}},
    )
    if not updated:
        return jsonify({"status": "error", "code": "not_found"}), 404
    logger.info("[CREATOR_SHARE][COPIED] user_id=%s package_id=%s platform=%s", user_id, package_id, platform)
    return jsonify({"status": "ok"})


@creator_share_bp.post("/api/creator/share/<package_id>/share-clicked")
def creator_share_clicked(package_id):
    user_id, _username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    body = request.get_json(force=True, silent=True) or {}
    platform = body.get("platform") if body.get("platform") in ALLOWED_PLATFORMS else "generic"

    updated = _mutate_owned_package(
        package_id,
        user_id,
        {
            "$set": {"share_clicked_at": now_utc(), "latest_share_click_platform": platform},
            "$inc": {"share_click_count": 1},
        },
    )
    if not updated:
        return jsonify({"status": "error", "code": "not_found"}), 404
    logger.info(
        "[CREATOR_SHARE][SHARE_CLICKED] user_id=%s package_id=%s platform=%s", user_id, package_id, platform
    )
    return jsonify({"status": "ok"})


@creator_share_bp.get("/api/creator/share/results")
def creator_share_results():
    user_id, _username, _record, err = _authenticate_and_authorize()
    if err:
        code, http_status = err
        return jsonify({"status": "error", "code": code}), http_status

    from dashboard_panels import _PENDING_STATUSES, _QUALIFIED_STATUSES, _REVOKED_STATUSES

    pending_col = database.db["pending_referrals"]
    now = now_utc()
    week_start = _current_week_start_utc(now)

    base_filter = {"inviter_user_id": user_id}
    week_filter = {**base_filter, "created_at_utc": {"$gte": week_start}}

    total_referral_joins = pending_col.count_documents(base_filter)
    qualified_referrals = pending_col.count_documents({**base_filter, "status": {"$in": _QUALIFIED_STATUSES}})
    pending_referrals_count = pending_col.count_documents({**base_filter, "status": {"$in": _PENDING_STATUSES}})
    revoked_referrals = pending_col.count_documents({**base_filter, "status": {"$in": _REVOKED_STATUSES}})
    current_week_referrals = pending_col.count_documents(week_filter)
    current_week_qualified = pending_col.count_documents(
        {**week_filter, "status": {"$in": _QUALIFIED_STATUSES}}
    )

    share_col = database.db["share_generations"]
    creator_filter = {"user_id": user_id, "generated_by": "creator_share_centre"}
    latest_doc = share_col.find_one(
        creator_filter, sort=[("generated_at", -1)], projection={"generated_at": 1}
    )
    latest_generated_at = (
        latest_doc["generated_at"].isoformat() if latest_doc and latest_doc.get("generated_at") else None
    )
    total_packages_generated = share_col.count_documents(creator_filter)

    return jsonify(
        {
            "status": "ok",
            "results": {
                "total_referral_joins": int(total_referral_joins),
                "qualified_referrals": int(qualified_referrals),
                "pending_referrals": int(pending_referrals_count),
                "revoked_referrals": int(revoked_referrals),
                "current_week_referrals": int(current_week_referrals),
                "current_week_qualified": int(current_week_qualified),
                "latest_generated_at": latest_generated_at,
                "total_packages_generated": int(total_packages_generated),
            },
        }
    )


# ---------------------------------------------------------------------------
# Admin API — Creator Access
# ---------------------------------------------------------------------------

def _validate_telegram_user_id(raw) -> int | None:
    try:
        uid = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return uid if uid > 0 else None


def _serialize_creator(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    for key in ("approved_at", "last_membership_verified_at", "last_membership_unresolvable_at", "created_at", "updated_at"):
        if out.get(key):
            out[key] = out[key].isoformat()
    return out


@creator_share_bp.get("/api/admin/referral/creators")
def admin_list_creators():
    _, err = _require_admin()
    if err:
        return err
    status = request.args.get("status")
    q = (request.args.get("q") or "").strip()
    filt: dict = {}
    if status in CREATOR_STATUSES:
        filt["status"] = status
    if q:
        or_clauses: list[dict] = [{"username": {"$regex": re.escape(q), "$options": "i"}}]
        uid = _validate_telegram_user_id(q)
        if uid:
            or_clauses.append({"user_id": uid})
        filt["$or"] = or_clauses
    docs = list(database.db["creator_members"].find(filt, sort=[("created_at", -1)], limit=500))
    active_count = database.db["creator_members"].count_documents({"status": "active"})
    return jsonify(
        {
            "status": "ok",
            "creators": [_serialize_creator(d) for d in docs],
            "active_count": active_count,
            "creator_group_configured": bool(_creator_group_chat_id()),
        }
    )


@creator_share_bp.post("/api/admin/referral/creators")
def admin_create_creator():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    user_id = _validate_telegram_user_id(body.get("user_id"))
    if not user_id:
        return jsonify({"status": "error", "code": "invalid_user_id"}), 400
    username = (body.get("username") or "").strip()
    creator_tier = (body.get("creator_tier") or "pilot").strip() or "pilot"
    source_group_id = body.get("source_group_id")
    now = now_utc()
    admin_id = (admin or {}).get("id")

    existing = database.db["creator_members"].find_one({"user_id": user_id})
    if existing:
        database.db["creator_members"].update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "status": "active",
                    "username": username or existing.get("username", ""),
                    "creator_tier": creator_tier,
                    "source_group_id": source_group_id,
                    "approved_at": now,
                    "approved_by": admin_id,
                    "updated_at": now,
                }
            },
        )
        return jsonify({"status": "ok", "user_id": user_id, "reactivated": True})

    doc = {
        "user_id": user_id,
        "status": "active",
        "username": username,
        "source_group_id": source_group_id,
        "creator_tier": creator_tier,
        "approved_at": now,
        "approved_by": admin_id,
        "last_membership_verified_at": None,
        "created_at": now,
        "updated_at": now,
    }
    database.db["creator_members"].insert_one(doc)
    return jsonify({"status": "ok", "user_id": user_id}), 201


@creator_share_bp.post("/api/admin/referral/creators/bulk")
def admin_bulk_creators():
    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    raw = body.get("user_ids") or body.get("lines") or ""
    if isinstance(raw, str):
        candidates = [c for c in re.split(r"[\s,]+", raw) if c.strip()]
    elif isinstance(raw, list):
        candidates = [str(c).strip() for c in raw if str(c).strip()]
    else:
        candidates = []

    if len(candidates) > MAX_BULK_CREATOR_IMPORT:
        return jsonify({"status": "error", "code": "too_many_lines", "max_lines": MAX_BULK_CREATOR_IMPORT}), 400

    now = now_utc()
    admin_id = (admin or {}).get("id")
    seen: set[int] = set()
    inserted = reactivated = skipped = rejected = 0
    results = []

    for raw_line in candidates:
        uid = _validate_telegram_user_id(raw_line)
        if not uid:
            results.append({"line": raw_line, "status": "rejected", "reason": "invalid_user_id"})
            rejected += 1
            continue
        if uid in seen:
            results.append({"line": raw_line, "status": "skipped", "reason": "duplicate_in_batch"})
            skipped += 1
            continue
        seen.add(uid)

        existing = database.db["creator_members"].find_one({"user_id": uid})
        if existing:
            database.db["creator_members"].update_one(
                {"user_id": uid},
                {"$set": {"status": "active", "approved_at": now, "approved_by": admin_id, "updated_at": now}},
            )
            results.append({"line": raw_line, "status": "reactivated"})
            reactivated += 1
            continue

        database.db["creator_members"].insert_one(
            {
                "user_id": uid,
                "status": "active",
                "username": "",
                "source_group_id": None,
                "creator_tier": "pilot",
                "approved_at": now,
                "approved_by": admin_id,
                "last_membership_verified_at": None,
                "created_at": now,
                "updated_at": now,
            }
        )
        results.append({"line": raw_line, "status": "inserted"})
        inserted += 1

    return jsonify(
        {
            "status": "ok",
            "inserted": inserted,
            "reactivated": reactivated,
            "skipped": skipped,
            "rejected": rejected,
            "results": results,
        }
    )


def _set_creator_status(raw_user_id: str, status_val: str):
    _, err = _require_admin()
    if err:
        return err
    uid = _validate_telegram_user_id(raw_user_id)
    if not uid:
        return jsonify({"status": "error", "code": "invalid_user_id"}), 400
    result = database.db["creator_members"].update_one(
        {"user_id": uid}, {"$set": {"status": status_val, "updated_at": now_utc()}}
    )
    if result.matched_count == 0:
        return jsonify({"status": "error", "code": "not_found"}), 404
    return jsonify({"status": "ok"})


@creator_share_bp.post("/api/admin/referral/creators/<user_id>/suspend")
def admin_suspend_creator(user_id):
    return _set_creator_status(user_id, "suspended")


@creator_share_bp.post("/api/admin/referral/creators/<user_id>/activate")
def admin_activate_creator(user_id):
    return _set_creator_status(user_id, "active")


@creator_share_bp.delete("/api/admin/referral/creators/<user_id>")
def admin_remove_creator(user_id):
    # Soft-delete (status="removed") rather than a hard document delete, so
    # the action is reversible via the activate endpoint and history/audit
    # fields (approved_by/approved_at) are preserved.
    return _set_creator_status(user_id, "removed")
