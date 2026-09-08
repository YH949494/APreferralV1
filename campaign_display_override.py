"""Campaign-scoped manual referral leaderboard display override.

Presentation-only layer that lets ops seed a public campaign leaderboard
with manually-entered rows (managed directly in MongoDB Atlas/Compass) so a
brand-new referral campaign does not appear empty before genuine
participation exists.

Hard boundaries (do not weaken these without re-reading the design doc):
  - Manual rows live in their own `campaign_display_overrides` collection,
    never in `qualified_events`, `referral_events`, `affiliate_ledger`,
    `voucher_pools`, `users.total_referrals`, or any KPI/anti-abuse
    collection.
  - Manual rows never influence `affiliate_rewards.py` tier evaluation,
    voucher issuance, or KPI computation -- they are only combined into the
    public leaderboard view built here, at read time.
  - `build_public_campaign_activity()` is the single shared builder every
    public surface (Affiliate page, public referral leaderboard, and any
    future consumer such as Money Room or an announcement preview) must
    call, so they can never disagree on totals/ranks.
"""

from __future__ import annotations

import logging
import os
import re
import secrets
from datetime import datetime, timezone
from html import escape as html_escape
from typing import Any

from flask import Blueprint, jsonify, request
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from time_utils import as_aware_utc

logger = logging.getLogger(__name__)

CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION = "campaign_display_overrides"

# Reuses the repository's existing generic app-settings collection
# (settings_service.py, one document per group, keyed by _id=group name)
# for the single global "which campaign is active" pointer -- but read and
# written directly here rather than through settings_service's cached
# get_setting()/update_settings(), whose in-process TTL cache (per
# gunicorn worker) would let an admin's Disable/Activate stay stale on a
# worker that didn't handle the write. A campaign toggle needs every
# worker to see the change on its very next request, so this always does
# a direct, uncached point read/write by _id.
ACTIVE_CAMPAIGN_SETTINGS_COLLECTION = "app_settings"
ACTIVE_CAMPAIGN_SETTINGS_ID = "campaign_display"

# Safe identifier convention for an admin-entered campaign_id: this
# repository has no pre-existing shared slug regex to reuse (event_id /
# gc_campaigns campaign_id are only checked for non-empty + uniqueness),
# so this is a new, minimal, clearly-scoped rule for a value that becomes
# both a Mongo _id and a URL path segment: lowercase letters/digits plus
# "_"/"-", 3-64 characters, starting with a letter or digit.
CAMPAIGN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{2,63}$")

# Prevents an accidentally huge Atlas-edited document from ballooning
# response size -- see the operator guide for the rationale.
MAX_OVERRIDE_PARTICIPANTS = 100

# Sensible display ceiling for a single manual participant's qualified
# count. Values above this are treated as malformed (ignored, not
# clamped) so an operator typo is visible in the logs rather than silently
# rewritten.
MAX_DISPLAY_QUALIFIED_COUNT = 100_000

# Sensible display ceiling for a single manual participant's name. Without
# this, the 100-participant cap does not actually bound response/render
# size -- an operator could paste one arbitrarily long string into
# display_name (up to Mongo's document-size limit) and every campaign
# activity read would serialize and render it.
MAX_DISPLAY_NAME_LENGTH = 40


def get_active_campaign_id(db) -> str | None:
    """Single resolution point for "which campaign is currently live",
    read at call time (never cached) so every public surface that doesn't
    already have an explicit campaign_id -- the Affiliate page, Money
    Room, an announcement preview -- resolves the exact same campaign
    without each surface guessing or hardcoding it independently.

    Tri-state precedence, MongoDB authoritative once initialized:
      1. The app_settings/campaign_display document exists and
         active_campaign_id is a non-empty string -> use it.
      2. That document exists but active_campaign_id is null, missing, or
         blank (an admin pressed Disable) -> no active campaign, full
         stop -- this is checked *before* the legacy env var so an old
         CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID left set on Fly can never
         reactivate a campaign an admin explicitly turned off.
      3. That document does not exist at all yet (no admin has ever saved
         Campaign Display Control, or the lookup itself failed) -> fall
         back to the CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID env var / Fly.io
         secret, so an existing deployment keeps working unmodified until
         the first Dashboard save.
    """
    try:
        settings_doc = db[ACTIVE_CAMPAIGN_SETTINGS_COLLECTION].find_one({"_id": ACTIVE_CAMPAIGN_SETTINGS_ID})
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] active_campaign_settings_lookup_failed", exc_info=True)
        settings_doc = None

    if settings_doc is not None:
        value = settings_doc.get("active_campaign_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    legacy_value = (os.getenv("CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID") or "").strip()
    return legacy_value or None


def get_campaign_display_settings(db) -> dict[str, Any]:
    """Raw app_settings/campaign_display document for the admin dashboard
    (which campaign is selected, who last changed it, when), defaulted
    when the document doesn't exist yet. Never raises."""
    try:
        doc = db[ACTIVE_CAMPAIGN_SETTINGS_COLLECTION].find_one({"_id": ACTIVE_CAMPAIGN_SETTINGS_ID})
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] active_campaign_settings_lookup_failed", exc_info=True)
        doc = None
    if not doc:
        return {"active_campaign_id": None, "updated_by": None, "updated_at": None, "initialized": False}
    updated_at = _coerce_aware_utc(doc.get("updated_at"))
    return {
        "active_campaign_id": doc.get("active_campaign_id"),
        "updated_by": doc.get("updated_by"),
        "updated_at": updated_at.isoformat() if updated_at else None,
        "initialized": True,
    }


def set_active_campaign_id(db, campaign_id: str | None, *, updated_by) -> None:
    """Write the single global "active campaign" pointer. Pass None to
    disable the feature everywhere -- this always creates/overwrites the
    app_settings/campaign_display document (upsert), which is exactly what
    makes MongoDB authoritative afterward: once this document exists, the
    legacy env var is never consulted again (see get_active_campaign_id)."""
    db[ACTIVE_CAMPAIGN_SETTINGS_COLLECTION].update_one(
        {"_id": ACTIVE_CAMPAIGN_SETTINGS_ID},
        {
            "$set": {
                "active_campaign_id": campaign_id,
                "updated_by": updated_by,
                "updated_at": _now_ms_utc(),
            }
        },
        upsert=True,
    )


def ensure_campaign_display_override_indexes(db_ref) -> None:
    """No secondary index is created here on purpose.

    Every read of this collection is an exact-match lookup on `_id`
    (`_id` is always set equal to `campaign_id`), which Mongo already
    indexes as the primary key. Adding a redundant `campaign_id` index
    would not be justified by the access pattern. This function exists so
    startup wiring stays symmetric with the other `ensure_*_indexes()`
    calls, and as a safe extension point if a future access pattern needs
    one -- it must never raise, matching the rest of the app's
    "an optional index must not crash startup" convention.
    """
    try:
        db_ref[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({}, {"_id": 1})
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] index_check_failed", exc_info=True)


def _coerce_aware_utc(value: Any) -> datetime | None:
    """Project convention (see affiliate_leaderboard._coerce_datetime_utc /
    campaign_centre._as_utc): a naive datetime is reinterpreted as UTC
    (PyMongo can hand back naive datetimes for values written as UTC-aware),
    never rejected outright. Anything else that fails to parse is
    malformed and yields None so the caller can fail closed."""
    if value is None:
        return None
    try:
        return as_aware_utc(value)
    except Exception:
        return None


def _normalize_name_for_sort(name: str) -> str:
    return str(name or "").strip().casefold()


def _now_ms_utc() -> datetime:
    """datetime.now(utc) truncated to millisecond precision -- real MongoDB
    already truncates datetimes to milliseconds on write, so doing it here
    too keeps the optimistic-concurrency `updated_at` token stable across
    a write/read round trip regardless of backend (production Mongo or
    mongomock in tests), instead of a microsecond-precision Python value
    silently never matching what a later find_one() reads back."""
    now = datetime.now(timezone.utc)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def _serialize_updated_at(value: Any) -> str | None:
    coerced = _coerce_aware_utc(value)
    return coerced.isoformat() if coerced else None


def _check_participant_fields(raw: Any, *, seen_entry_ids: set[str]) -> tuple[dict[str, Any] | None, str | None]:
    """Single source of truth for participant validation: returns
    (normalized_dict, None) on success or (None, reason_code) on failure.

    Both the public read path (_validate_participant, below -- used when
    loading a possibly Atlas-hand-edited document) and the admin write
    path (campaign_display_override_admin_bp, below -- used when the
    Dashboard itself creates/edits a participant) call this exact
    function, so a payload the admin UI would reject can never sneak past
    a bypassed frontend either: the backend is authoritative either way.
    """
    if not isinstance(raw, dict):
        return None, "not_a_dict"

    entry_id = raw.get("entry_id")
    if not isinstance(entry_id, str) or not entry_id.strip():
        return None, "missing_entry_id"
    entry_id = entry_id.strip()
    if entry_id in seen_entry_ids:
        return None, "duplicate_entry_id"

    display_name = raw.get("display_name")
    if not isinstance(display_name, str) or not display_name.strip():
        return None, "missing_display_name"
    display_name = display_name.strip()
    if len(display_name) > MAX_DISPLAY_NAME_LENGTH:
        return None, "display_name_too_long"

    qualified_count = raw.get("qualified_count")
    # bool is a subclass of int in Python -- must be excluded explicitly, a
    # float or numeric string must be excluded too (no implicit coercion).
    if isinstance(qualified_count, bool) or not isinstance(qualified_count, int):
        return None, "bad_qualified_count_type"
    if qualified_count < 0 or qualified_count > MAX_DISPLAY_QUALIFIED_COUNT:
        return None, "qualified_count_out_of_range"

    visible = raw.get("visible", True)
    if not isinstance(visible, bool):
        return None, "bad_visible_type"

    return {
        "entry_id": entry_id,
        "display_name": display_name,
        "qualified_count": qualified_count,
        "visible": visible,
    }, None


def _validate_participant(raw: Any, *, campaign_id: str, seen_entry_ids: set[str]) -> dict[str, Any] | None:
    normalized, reason = _check_participant_fields(raw, seen_entry_ids=seen_entry_ids)
    if reason:
        entry_id = raw.get("entry_id") if isinstance(raw, dict) else None
        logger.warning(
            "[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=%s entry_id=%s",
            campaign_id,
            reason,
            entry_id,
        )
        return None
    return normalized


def validate_campaign_id(raw: Any) -> str | None:
    """Normalizes and validates an admin-entered campaign_id against
    CAMPAIGN_ID_PATTERN. Returns the lowercased, trimmed id, or None."""
    if not isinstance(raw, str):
        return None
    value = raw.strip().lower()
    if not CAMPAIGN_ID_PATTERN.match(value):
        return None
    return value


def _generate_entry_id(existing_ids: set[str]) -> str:
    """Server-generated, stable entry_id for a newly-added participant --
    never derived from array position, so a later reorder/edit can never
    target the wrong row. Collision is astronomically unlikely (32 bits of
    randomness) but is still defended against for a clean guarantee."""
    for _ in range(10):
        candidate = f"m-{secrets.token_hex(4)}"
        if candidate not in existing_ids:
            return candidate
    # Practically unreachable; falls through to a longer id rather than
    # ever returning a colliding one.
    return f"m-{secrets.token_hex(8)}"


def load_active_campaign_override(db_ref, campaign_id: str, *, reference_utc: datetime | None = None) -> dict[str, Any]:
    """Load and validate the override document for exactly `campaign_id`.

    Never raises. Fails closed (returns active=False, participants=[]) on
    any lookup failure or malformed document, always with a structured log
    line, so a broken/expired override can never break or leak into the
    genuine leaderboard or another campaign.

    The returned dict also carries the campaign's own `starts_at`/`ends_at`
    (UTC-aware) whenever the document exists with a parseable schedule --
    even when the campaign is currently disabled, not-yet-started, or
    expired -- so a caller can window the *genuine* leaderboard to this
    campaign's own dates instead of an unrelated calendar month. Both are
    None when no schedule is known (no document, lookup failure, or a
    malformed/mismatched document), in which case the caller should fall
    back to its own default window.
    """
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)

    no_window = {"active": False, "participants": [], "starts_at": None, "ends_at": None}

    try:
        doc = db_ref[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] campaign=%s override_lookup_failed", campaign_id, exc_info=True)
        return dict(no_window)

    if not doc:
        logger.debug("[CAMPAIGN_DISPLAY] campaign=%s override_not_found", campaign_id)
        return dict(no_window)

    try:
        if doc.get("campaign_id") != campaign_id:
            logger.warning(
                "[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=campaign_id_mismatch",
                campaign_id,
            )
            return dict(no_window)

        starts_at = _coerce_aware_utc(doc.get("starts_at"))
        ends_at = _coerce_aware_utc(doc.get("ends_at"))
        if starts_at is None or ends_at is None:
            logger.warning("[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=bad_schedule", campaign_id)
            return dict(no_window)

        if doc.get("enabled") is not True:
            logger.debug("[CAMPAIGN_DISPLAY] campaign=%s override_disabled", campaign_id)
            return {"active": False, "participants": [], "starts_at": starts_at, "ends_at": ends_at}

        if not (starts_at <= now_utc_ts < ends_at):
            logger.debug(
                "[CAMPAIGN_DISPLAY] campaign=%s override_outside_window starts_at=%s ends_at=%s now=%s",
                campaign_id,
                starts_at.isoformat(),
                ends_at.isoformat(),
                now_utc_ts.isoformat(),
            )
            return {"active": False, "participants": [], "starts_at": starts_at, "ends_at": ends_at}

        raw_participants = doc.get("participants")
        if not isinstance(raw_participants, list):
            logger.warning(
                "[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=participants_not_a_list",
                campaign_id,
            )
            # The schedule itself is still well-formed -- keep the window so
            # the genuine leaderboard is correctly scoped even though the
            # manual side of this document is unusable.
            return {"active": False, "participants": [], "starts_at": starts_at, "ends_at": ends_at}

        bounded = raw_participants[:MAX_OVERRIDE_PARTICIPANTS]
        if len(raw_participants) > MAX_OVERRIDE_PARTICIPANTS:
            logger.warning(
                "[CAMPAIGN_DISPLAY] campaign=%s override_truncated total=%s max=%s",
                campaign_id,
                len(raw_participants),
                MAX_OVERRIDE_PARTICIPANTS,
            )

        seen_entry_ids: set[str] = set()
        validated: list[dict[str, Any]] = []
        for raw in bounded:
            normalized = _validate_participant(raw, campaign_id=campaign_id, seen_entry_ids=seen_entry_ids)
            if normalized is None:
                continue
            seen_entry_ids.add(normalized["entry_id"])
            validated.append(normalized)

        visible_rows = [p for p in validated if p["visible"]]
        logger.debug(
            "[CAMPAIGN_DISPLAY] campaign=%s override_loaded participant_rows=%s visible_rows=%s",
            campaign_id,
            len(validated),
            len(visible_rows),
        )
        return {"active": True, "participants": visible_rows, "starts_at": starts_at, "ends_at": ends_at}
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=unexpected_error", campaign_id, exc_info=True)
        return dict(no_window)


def _genuine_display_names(db_ref, referrer_ids: list[int]) -> dict[int, str]:
    """Best-effort masked display name per referrer, for the combined
    public view only. Reuses the exact masking convention already used for
    public affiliate/Money Room announcements (mask_public_username) --
    lazy-imported to avoid a heavy import cycle with scheduler.py, the same
    pattern creator_share_centre.py already uses."""
    if not referrer_ids:
        return {}
    try:
        from scheduler import mask_public_username
    except Exception:
        mask_public_username = None

    names: dict[int, str] = {}
    try:
        cursor = db_ref.users.find(
            {"user_id": {"$in": referrer_ids}},
            {"user_id": 1, "username": 1, "first_name": 1},
        )
        for user_doc in cursor:
            try:
                uid = int(user_doc.get("user_id"))
            except (TypeError, ValueError):
                continue
            username = user_doc.get("username")
            first_name = user_doc.get("first_name")
            if username and str(username).strip():
                names[uid] = mask_public_username(username) if mask_public_username else f"@{username}"
            elif first_name and str(first_name).strip():
                names[uid] = str(first_name).strip()
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] genuine_display_name_lookup_failed", exc_info=True)
    return names


def build_public_campaign_activity(db, campaign_id: str, reference_utc: datetime | None = None) -> dict[str, Any]:
    """Single shared builder for the public campaign leaderboard/activity.

    Combines the genuine qualified-referral leaderboard with any active
    manual display override for `campaign_id`, and returns one result
    every public surface (Affiliate page, public referral leaderboard,
    Money Room, an announcement preview, and any future consumer) must
    render from -- so they can never disagree.

    The genuine side is windowed to the campaign's own `starts_at`
    (inclusive) / `ends_at` (exclusive) whenever that schedule is known
    (via the same canonical per-window aggregation
    affiliate_leaderboard._compute_affiliate_monthly_rows already uses to
    build the real monthly leaderboard -- just parameterized by a
    different window, never a different definition of "qualified"), so a
    campaign that starts mid-month never pulls in qualified referrals from
    before it started. When no schedule is known at all (no override
    document, or one that's malformed/unreachable), this falls back to the
    current KL calendar month -- the same default the Affiliate page
    already showed before any campaign existed.

    Never raises: any failure in either the genuine leaderboard or the
    override falls back to an empty contribution from that side rather
    than breaking the whole response.
    """
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)

    override_result = {"active": False, "participants": [], "starts_at": None, "ends_at": None}
    try:
        override_result = load_active_campaign_override(db, campaign_id, reference_utc=now_utc_ts)
    except Exception:
        logger.exception("[CAMPAIGN_DISPLAY] campaign=%s override_load_unexpected_error", campaign_id)
        override_result = {"active": False, "participants": [], "starts_at": None, "ends_at": None}

    override_active = bool(override_result.get("active"))
    manual_participants = override_result.get("participants") or []
    window_starts_at = override_result.get("starts_at")
    window_ends_at = override_result.get("ends_at")
    window_known = window_starts_at is not None and window_ends_at is not None

    genuine_rows: list[dict[str, Any]] = []
    try:
        if window_known:
            from affiliate_leaderboard import _compute_affiliate_monthly_rows

            genuine_rows = _compute_affiliate_monthly_rows(db, window_starts_at, window_ends_at)
        else:
            from affiliate_leaderboard import compute_affiliate_monthly_kpis_live

            snapshot = compute_affiliate_monthly_kpis_live(db, reference_utc=now_utc_ts)
            genuine_rows = list(snapshot.get("affiliate_leaderboard_month") or [])
    except Exception:
        logger.exception("[CAMPAIGN_DISPLAY] campaign=%s genuine_leaderboard_failed", campaign_id)
        genuine_rows = []

    referrer_ids: list[int] = []
    for row in genuine_rows:
        try:
            referrer_ids.append(int(row.get("referrer_id")))
        except (TypeError, ValueError):
            continue
    display_names = _genuine_display_names(db, referrer_ids)

    combined: list[dict[str, Any]] = []
    for row in genuine_rows:
        try:
            referrer_id = int(row.get("referrer_id"))
        except (TypeError, ValueError):
            continue
        combined.append(
            {
                "entry_id": f"genuine:{referrer_id}",
                "display_name": display_names.get(referrer_id) or f"Member #{str(referrer_id)[-4:]}",
                "qualified_count": int(row.get("qualified_month", 0) or 0),
                "_sort_joins": int(row.get("joins_month", 0) or 0),
                "_sort_conversion": float(row.get("conversion_month") or 0.0),
                "_source": "genuine",
            }
        )
    for participant in manual_participants:
        combined.append(
            {
                "entry_id": f"manual:{participant['entry_id']}",
                "display_name": participant["display_name"],
                "qualified_count": participant["qualified_count"],
                "_sort_joins": 0,
                "_sort_conversion": 0.0,
                "_source": "manual",
            }
        )

    combined.sort(
        key=lambda r: (
            -r["qualified_count"],
            -r["_sort_joins"],
            -r["_sort_conversion"],
            _normalize_name_for_sort(r["display_name"]),
            r["entry_id"],
        )
    )

    leaderboard = []
    qualified_total = 0
    for idx, row in enumerate(combined, start=1):
        qualified_total += row["qualified_count"]
        leaderboard.append(
            {
                "rank": idx,
                "display_name": row["display_name"],
                "qualified_count": row["qualified_count"],
            }
        )

    diagnostics = {
        "genuine_rows": len(genuine_rows),
        "manual_rows": len(manual_participants),
        "override_active": override_active,
        "genuine_window": "campaign" if window_known else "calendar_month",
    }
    logger.debug(
        "[CAMPAIGN_DISPLAY] campaign=%s active=%s genuine_rows=%s manual_rows=%s window=%s",
        campaign_id,
        str(override_active).lower(),
        diagnostics["genuine_rows"],
        diagnostics["manual_rows"],
        diagnostics["genuine_window"],
    )

    return {
        "campaign_id": campaign_id,
        "state": "active" if override_active else "genuine_only",
        "participant_count": len(combined),
        "qualified_total": qualified_total,
        "leaderboard": leaderboard,
        "diagnostics": diagnostics,
        # Internal-only, stripped by the public API layer before
        # serialization -- kept here for tests/debugging.
        "_combined_rows": combined,
    }


def public_campaign_activity_view(activity: dict[str, Any]) -> dict[str, Any]:
    """Strip internal-only fields before returning `activity` through any
    public API response. Never expose diagnostics, source, or entry_id."""
    return {
        "campaign_id": activity.get("campaign_id"),
        "state": activity.get("state"),
        "participant_count": activity.get("participant_count"),
        "qualified_total": activity.get("qualified_total"),
        "leaderboard": activity.get("leaderboard"),
    }


def render_campaign_activity_announcement_text(activity: dict[str, Any], *, title: str = "Campaign Leaderboard") -> str:
    """Render a Telegram-HTML announcement preview from a
    build_public_campaign_activity() result, so any future announcement
    tool (manual preview, or a new automatic post) reads the exact same
    combined totals/ranks as the Affiliate page and public leaderboard --
    never a separately-calculated total.

    This repository has no existing campaign-scoped announcement sender
    (the current weekly "Top 5 Growth Leaders" post is a distinct,
    non-campaign-scoped mechanism -- see docs/campaign_display_override
    notes) -- this function is the reusable output for that future
    consumer, not a new delivery pipeline.
    """
    medals = ["\U0001F947", "\U0001F948", "\U0001F949"]
    lines = [f"<b>{html_escape(title)}</b>", ""]
    for row in activity.get("leaderboard") or []:
        rank = int(row.get("rank", 0) or 0)
        prefix = medals[rank - 1] if 1 <= rank <= 3 else f"#{rank}"
        name = html_escape(str(row.get("display_name") or "Anonymous"))
        count = int(row.get("qualified_count", 0) or 0)
        lines.append(f"{prefix} {name} — {count} qualified invites")
    if not activity.get("leaderboard"):
        lines.append("No activity yet.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Admin Dashboard: Campaign Display Control
#
# Everything below is authenticated-admin-only (vouchers.require_admin(),
# the same gate event_banner.py's admin routes use) and replaces the
# operational dependency on `fly secrets set
# CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID=...` with a Mongo-backed control an
# admin can operate from the Dashboard, with no deploy/restart. It creates
# and edits documents in campaign_display_overrides using the exact same
# schema and validation the public read path above enforces -- there is no
# separate, weaker set of admin-only rules a bypassed frontend could slip
# past.
# ---------------------------------------------------------------------------

CAMPAIGN_DISPLAY_STATES = ("active", "scheduled", "expired", "disabled", "invalid")


def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _admin_identity(admin: dict | None) -> str:
    admin = admin or {}
    return admin.get("usernameLower") or str(admin.get("id", ""))


def _log_admin_audit(action: str, admin: dict | None, campaign_id: str, details: dict | None = None) -> None:
    """Mirrors event_banner.py's _log_audit / campaign_admin_audit_log
    convention -- never logs session credentials, only the resolved admin
    identity string and the mutation's own (non-sensitive) fields."""
    try:
        from database import db as default_db

        default_db["campaign_admin_audit_log"].insert_one(
            {
                "action": action,
                "entity": "campaign_display_override",
                "entity_id": campaign_id,
                "admin": _admin_identity(admin),
                "details": details or {},
                "at": datetime.now(timezone.utc),
            }
        )
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] audit_write_failed", exc_info=True)


def describe_campaign_state(doc: dict[str, Any] | None, *, is_selected_active: bool, reference_utc: datetime | None = None) -> str:
    """Admin-facing state label -- distinct from (and more granular than)
    the strict active/inactive gate load_active_campaign_override() uses
    for the public path. Never raises; a malformed document reads as
    "invalid" rather than crashing the Dashboard."""
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    try:
        if not isinstance(doc, dict):
            return "invalid"
        if doc.get("campaign_id") != doc.get("_id"):
            return "invalid"
        if not isinstance(doc.get("participants"), list):
            return "invalid"
        starts_at = _coerce_aware_utc(doc.get("starts_at"))
        ends_at = _coerce_aware_utc(doc.get("ends_at"))
        if starts_at is None or ends_at is None or ends_at <= starts_at:
            return "invalid"
        if doc.get("enabled") is not True or not is_selected_active:
            return "disabled"
        if now_utc_ts < starts_at:
            return "scheduled"
        if now_utc_ts >= ends_at:
            return "expired"
        return "active"
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] describe_state_failed", exc_info=True)
        return "invalid"


def serialize_campaign_override_admin(doc: dict[str, Any], *, is_selected_active: bool, reference_utc: datetime | None = None) -> dict[str, Any]:
    """Admin-only serialization (never used for a public response): unlike
    public_campaign_activity_view(), this is allowed to expose entry_id,
    per-row visible flags, and raw counts, since only an authenticated
    admin ever sees it."""
    starts_at = _coerce_aware_utc(doc.get("starts_at"))
    ends_at = _coerce_aware_utc(doc.get("ends_at"))
    created_at = _coerce_aware_utc(doc.get("created_at"))
    updated_at = _coerce_aware_utc(doc.get("updated_at"))
    participants = doc.get("participants") if isinstance(doc.get("participants"), list) else []
    visible_count = sum(1 for p in participants if isinstance(p, dict) and p.get("visible") is True)
    return {
        "campaign_id": doc.get("campaign_id"),
        "enabled": bool(doc.get("enabled") is True),
        "is_selected_active": is_selected_active,
        "state": describe_campaign_state(doc, is_selected_active=is_selected_active, reference_utc=reference_utc),
        "starts_at": starts_at.isoformat() if starts_at else None,
        "ends_at": ends_at.isoformat() if ends_at else None,
        "participants": participants,
        "visible_participant_count": visible_count,
        "participant_count": len(participants),
        "created_at": created_at.isoformat() if created_at else None,
        "created_by": doc.get("created_by"),
        "updated_at": updated_at.isoformat() if updated_at else None,
        "updated_by": doc.get("updated_by"),
    }


def create_campaign_override(
    db,
    *,
    campaign_id: str,
    starts_at_utc: datetime | None,
    ends_at_utc: datetime | None,
    enabled: bool = True,
    updated_by,
) -> tuple[dict[str, Any] | None, str | None]:
    """Creates a new campaign_display_overrides document. campaign_id is
    immutable after this (there is no rename endpoint -- _id can't change
    without a new document identity anyway)."""
    normalized_id = validate_campaign_id(campaign_id)
    if not normalized_id:
        return None, "invalid_campaign_id"
    if starts_at_utc is None or starts_at_utc.tzinfo is None:
        return None, "invalid_schedule"
    if ends_at_utc is None or ends_at_utc.tzinfo is None:
        return None, "invalid_schedule"
    if ends_at_utc <= starts_at_utc:
        return None, "ends_at_before_starts_at"
    if not isinstance(enabled, bool):
        return None, "invalid_enabled"

    if db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": normalized_id}, {"_id": 1}):
        return None, "campaign_id_exists"

    now = _now_ms_utc()
    doc = {
        "_id": normalized_id,
        "campaign_id": normalized_id,
        "enabled": enabled,
        "starts_at": starts_at_utc.astimezone(timezone.utc),
        "ends_at": ends_at_utc.astimezone(timezone.utc),
        "participants": [],
        "created_at": now,
        "updated_at": now,
        "created_by": updated_by,
        "updated_by": updated_by,
    }
    try:
        db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].insert_one(doc)
    except DuplicateKeyError:
        return None, "campaign_id_exists"
    return doc, None


def update_campaign_override(
    db,
    campaign_id: str,
    *,
    enabled: bool | None = None,
    starts_at_utc: datetime | None = None,
    ends_at_utc: datetime | None = None,
    updated_by,
    expected_updated_at: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Partial update of schedule/enabled. Never touches campaign_id/_id or
    participants. Optimistic concurrency: when `expected_updated_at` is
    given, the write is rejected with "stale_update" unless it still
    matches the document's current updated_at."""
    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    if not doc or doc.get("campaign_id") != campaign_id:
        return None, "not_found"
    if expected_updated_at is not None and _serialize_updated_at(doc.get("updated_at")) != expected_updated_at:
        return None, "stale_update"

    new_starts = starts_at_utc.astimezone(timezone.utc) if starts_at_utc is not None else _coerce_aware_utc(doc.get("starts_at"))
    new_ends = ends_at_utc.astimezone(timezone.utc) if ends_at_utc is not None else _coerce_aware_utc(doc.get("ends_at"))
    if new_starts is None or new_ends is None:
        return None, "invalid_schedule"
    if new_ends <= new_starts:
        return None, "ends_at_before_starts_at"

    updates: dict[str, Any] = {
        "starts_at": new_starts,
        "ends_at": new_ends,
        "updated_at": _now_ms_utc(),
        "updated_by": updated_by,
    }
    if enabled is not None:
        if not isinstance(enabled, bool):
            return None, "invalid_enabled"
        updates["enabled"] = enabled

    # Always compare-and-swap on the document's own current updated_at
    # (not only when the caller passed expected_updated_at) -- this closes
    # the read/write race between the find_one() above and this write even
    # for a caller that didn't ask for the concurrency check, so a lost
    # update can never happen silently.
    result = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one_and_update(
        {"_id": campaign_id, "updated_at": doc.get("updated_at")},
        {"$set": updates},
        return_document=ReturnDocument.AFTER,
    )
    if result is None:
        return None, "stale_update"
    return result, None


def activate_campaign(db, campaign_id: str, *, updated_by, reference_utc: datetime | None = None) -> tuple[dict[str, Any] | None, str | None]:
    """Selects `campaign_id` as the single global active campaign. Requires
    the campaign's own document to already be enabled with a valid,
    not-already-expired schedule -- a future-scheduled campaign is allowed
    (it simply won't show publicly until its starts_at arrives), but an
    expired one must have its schedule corrected first, and a
    malformed/disabled one can't be activated at all. Returns the freshly
    built public activity result on success, per the same shared builder
    every public surface reads from."""
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    if not doc or doc.get("campaign_id") != campaign_id:
        return None, "not_found"
    if not isinstance(doc.get("participants"), list):
        return None, "invalid_campaign"
    if doc.get("enabled") is not True:
        return None, "campaign_disabled"
    starts_at = _coerce_aware_utc(doc.get("starts_at"))
    ends_at = _coerce_aware_utc(doc.get("ends_at"))
    if starts_at is None or ends_at is None or ends_at <= starts_at:
        return None, "invalid_schedule"
    if ends_at <= now_utc_ts:
        return None, "campaign_expired"

    set_active_campaign_id(db, campaign_id, updated_by=updated_by)
    activity = build_public_campaign_activity(db, campaign_id, reference_utc=now_utc_ts)
    return activity, None


def disable_campaign_display(db, *, updated_by) -> None:
    """Clears the global active-campaign pointer. Never deletes the
    campaign_display_overrides document itself -- it remains stored and
    can be re-activated later. Authoritative even if the legacy
    CAMPAIGN_DISPLAY_ACTIVE_CAMPAIGN_ID env var is still set (see the
    tri-state precedence in get_active_campaign_id())."""
    set_active_campaign_id(db, None, updated_by=updated_by)


def add_participant(
    db,
    campaign_id: str,
    *,
    display_name: Any,
    qualified_count: Any,
    visible: Any = True,
    updated_by,
    expected_updated_at: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    if not doc or doc.get("campaign_id") != campaign_id:
        return None, "not_found"
    if expected_updated_at is not None and _serialize_updated_at(doc.get("updated_at")) != expected_updated_at:
        return None, "stale_update"

    participants = doc.get("participants") if isinstance(doc.get("participants"), list) else []
    if len(participants) >= MAX_OVERRIDE_PARTICIPANTS:
        return None, "max_participants_reached"

    existing_ids = {p.get("entry_id") for p in participants if isinstance(p, dict) and isinstance(p.get("entry_id"), str)}
    entry_id = _generate_entry_id(existing_ids)
    candidate = {"entry_id": entry_id, "display_name": display_name, "qualified_count": qualified_count, "visible": visible}
    normalized, reason = _check_participant_fields(candidate, seen_entry_ids=set())
    if reason:
        return None, reason

    # Always compare-and-swap on the document's current updated_at -- see
    # update_campaign_override for why this isn't gated on the caller
    # having passed expected_updated_at.
    result = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one_and_update(
        {"_id": campaign_id, "updated_at": doc.get("updated_at")},
        {"$push": {"participants": normalized}, "$set": {"updated_at": _now_ms_utc(), "updated_by": updated_by}},
        return_document=ReturnDocument.AFTER,
    )
    if result is None:
        return None, "stale_update"
    return result, None


def update_participant(
    db,
    campaign_id: str,
    entry_id: str,
    *,
    display_name: Any = None,
    qualified_count: Any = None,
    visible: Any = None,
    updated_by,
    expected_updated_at: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Targets the participant by its stable entry_id (never array
    position/order), so a concurrent edit or a reordered array can never
    silently update the wrong row. Only the fields explicitly passed (not
    None) are changed.

    Rebuilds and writes back the whole `participants` array (rather than
    Mongo's positional `$`/`$[elem]` operators) locating the target purely
    by entry_id in Python first -- both because this repo's test suite
    (mongomock) does not reliably resolve positional array-element updates
    to the array-filter-matched index, and because this keeps the
    "identify by entry_id, never by position" guarantee explicit and
    directly inspectable rather than delegated to query-engine semantics.
    Still fully guarded by the compare-and-swap on updated_at below, so a
    concurrent write between the read and this write is rejected rather
    than silently lost.
    """
    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    if not doc or doc.get("campaign_id") != campaign_id:
        return None, "not_found"
    if expected_updated_at is not None and _serialize_updated_at(doc.get("updated_at")) != expected_updated_at:
        return None, "stale_update"

    participants = doc.get("participants") if isinstance(doc.get("participants"), list) else []
    current = next((p for p in participants if isinstance(p, dict) and p.get("entry_id") == entry_id), None)
    if current is None:
        return None, "participant_not_found"

    candidate = dict(current)
    if display_name is not None:
        candidate["display_name"] = display_name
    if qualified_count is not None:
        candidate["qualified_count"] = qualified_count
    if visible is not None:
        candidate["visible"] = visible

    normalized, reason = _check_participant_fields(candidate, seen_entry_ids=set())
    if reason:
        return None, reason
    normalized["entry_id"] = entry_id  # immutable regardless of candidate contents

    new_participants = [normalized if (isinstance(p, dict) and p.get("entry_id") == entry_id) else p for p in participants]

    result = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one_and_update(
        {"_id": campaign_id, "updated_at": doc.get("updated_at")},
        {"$set": {"participants": new_participants, "updated_at": _now_ms_utc(), "updated_by": updated_by}},
        return_document=ReturnDocument.AFTER,
    )
    if result is None:
        return None, "stale_update"
    return result, None


def remove_participant(
    db,
    campaign_id: str,
    entry_id: str,
    *,
    updated_by,
    expected_updated_at: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    if not doc or doc.get("campaign_id") != campaign_id:
        return None, "not_found"
    if expected_updated_at is not None and _serialize_updated_at(doc.get("updated_at")) != expected_updated_at:
        return None, "stale_update"

    participants = doc.get("participants") if isinstance(doc.get("participants"), list) else []
    if not any(isinstance(p, dict) and p.get("entry_id") == entry_id for p in participants):
        return None, "participant_not_found"

    result = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one_and_update(
        {"_id": campaign_id, "updated_at": doc.get("updated_at")},
        {"$pull": {"participants": {"entry_id": entry_id}}, "$set": {"updated_at": _now_ms_utc(), "updated_by": updated_by}},
        return_document=ReturnDocument.AFTER,
    )
    if result is None:
        return None, "stale_update"
    return result, None


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

campaign_display_admin_bp = Blueprint("campaign_display_admin", __name__)


def _parse_iso_utc(value: Any) -> datetime | None:
    """Strict-ish parse for admin-submitted schedule strings: accepts a
    datetime or an ISO-8601 string (the frontend always sends
    `.toISOString()` output, already UTC with a "Z"/offset suffix) and
    always returns a timezone-aware UTC value or None -- never a naive
    one, so storage can never accidentally persist an ambiguous time."""
    return _coerce_aware_utc(value)


@campaign_display_admin_bp.get("/api/admin/campaign-display")
def api_admin_campaign_display_overview():
    from database import db

    _, err = _require_admin()
    if err:
        return err

    now_utc_ts = datetime.now(timezone.utc)
    settings = get_campaign_display_settings(db)
    active_campaign_id = settings.get("active_campaign_id")

    docs = list(db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find({}).sort("created_at", -1).limit(200))
    campaigns = [
        serialize_campaign_override_admin(d, is_selected_active=(d.get("campaign_id") == active_campaign_id), reference_utc=now_utc_ts)
        for d in docs
    ]

    active_activity = None
    if active_campaign_id:
        try:
            active_activity = build_public_campaign_activity(db, active_campaign_id, reference_utc=now_utc_ts)
        except Exception:
            logger.exception("[CAMPAIGN_DISPLAY] campaign=%s admin_overview_activity_failed", active_campaign_id)
            active_activity = None

    return jsonify(
        {
            "status": "ok",
            "settings": settings,
            "active_activity": active_activity,
            "campaigns": campaigns,
        }
    )


@campaign_display_admin_bp.post("/api/admin/campaign-display/campaigns")
def api_admin_create_campaign():
    from database import db

    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}

    starts_at = _parse_iso_utc(body.get("starts_at"))
    ends_at = _parse_iso_utc(body.get("ends_at"))
    enabled = body.get("enabled", True)

    doc, code = create_campaign_override(
        db,
        campaign_id=body.get("campaign_id"),
        starts_at_utc=starts_at,
        ends_at_utc=ends_at,
        enabled=enabled if isinstance(enabled, bool) else True,
        updated_by=_admin_identity(admin),
    )
    if code:
        return jsonify({"status": "error", "code": code}), 409 if code == "campaign_id_exists" else 400

    _log_admin_audit("campaign_created", admin, doc["campaign_id"])
    return jsonify({"status": "ok", "campaign": serialize_campaign_override_admin(doc, is_selected_active=False)}), 201


@campaign_display_admin_bp.get("/api/admin/campaign-display/campaigns/<campaign_id>")
def api_admin_get_campaign(campaign_id: str):
    from database import db

    _, err = _require_admin()
    if err:
        return err
    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    settings = get_campaign_display_settings(db)
    is_active = settings.get("active_campaign_id") == campaign_id
    return jsonify({"status": "ok", "campaign": serialize_campaign_override_admin(doc, is_selected_active=is_active)})


@campaign_display_admin_bp.put("/api/admin/campaign-display/campaigns/<campaign_id>")
def api_admin_update_campaign(campaign_id: str):
    from database import db

    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}

    starts_at = _parse_iso_utc(body["starts_at"]) if "starts_at" in body else None
    if "starts_at" in body and starts_at is None:
        return jsonify({"status": "error", "code": "invalid_schedule"}), 400
    ends_at = _parse_iso_utc(body["ends_at"]) if "ends_at" in body else None
    if "ends_at" in body and ends_at is None:
        return jsonify({"status": "error", "code": "invalid_schedule"}), 400
    enabled = body.get("enabled") if isinstance(body.get("enabled"), bool) else None

    doc, code = update_campaign_override(
        db,
        campaign_id,
        enabled=enabled,
        starts_at_utc=starts_at,
        ends_at_utc=ends_at,
        updated_by=_admin_identity(admin),
        expected_updated_at=body.get("expected_updated_at"),
    )
    if code:
        status_code = {"not_found": 404, "stale_update": 409}.get(code, 400)
        return jsonify({"status": "error", "code": code}), status_code

    settings = get_campaign_display_settings(db)
    is_active = settings.get("active_campaign_id") == campaign_id
    _log_admin_audit("campaign_updated", admin, campaign_id, {"fields": list(body.keys())})
    return jsonify({"status": "ok", "campaign": serialize_campaign_override_admin(doc, is_selected_active=is_active)})


@campaign_display_admin_bp.post("/api/admin/campaign-display/campaigns/<campaign_id>/activate")
def api_admin_activate_campaign(campaign_id: str):
    from database import db

    admin, err = _require_admin()
    if err:
        return err
    activity, code = activate_campaign(db, campaign_id, updated_by=_admin_identity(admin))
    if code:
        status_code = {"not_found": 404}.get(code, 400)
        return jsonify({"status": "error", "code": code}), status_code
    _log_admin_audit("campaign_activated", admin, campaign_id)
    return jsonify({"status": "ok", "activity": activity})


@campaign_display_admin_bp.post("/api/admin/campaign-display/disable")
def api_admin_disable_campaign_display():
    from database import db

    admin, err = _require_admin()
    if err:
        return err
    disable_campaign_display(db, updated_by=_admin_identity(admin))
    _log_admin_audit("campaign_display_disabled", admin, "")
    return jsonify({"status": "ok"})


@campaign_display_admin_bp.post("/api/admin/campaign-display/campaigns/<campaign_id>/participants")
def api_admin_add_participant(campaign_id: str):
    from database import db

    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    doc, code = add_participant(
        db,
        campaign_id,
        display_name=body.get("display_name"),
        qualified_count=body.get("qualified_count"),
        visible=body.get("visible", True),
        updated_by=_admin_identity(admin),
        expected_updated_at=body.get("expected_updated_at"),
    )
    if code:
        status_code = {"not_found": 404, "stale_update": 409}.get(code, 400)
        return jsonify({"status": "error", "code": code}), status_code
    settings = get_campaign_display_settings(db)
    is_active = settings.get("active_campaign_id") == campaign_id
    _log_admin_audit("participant_added", admin, campaign_id)
    return jsonify({"status": "ok", "campaign": serialize_campaign_override_admin(doc, is_selected_active=is_active)}), 201


@campaign_display_admin_bp.patch("/api/admin/campaign-display/campaigns/<campaign_id>/participants/<entry_id>")
def api_admin_update_participant(campaign_id: str, entry_id: str):
    from database import db

    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    doc, code = update_participant(
        db,
        campaign_id,
        entry_id,
        display_name=body.get("display_name"),
        qualified_count=body.get("qualified_count"),
        visible=body.get("visible"),
        updated_by=_admin_identity(admin),
        expected_updated_at=body.get("expected_updated_at"),
    )
    if code:
        status_code = {"not_found": 404, "participant_not_found": 404, "stale_update": 409}.get(code, 400)
        return jsonify({"status": "error", "code": code}), status_code
    settings = get_campaign_display_settings(db)
    is_active = settings.get("active_campaign_id") == campaign_id
    _log_admin_audit("participant_updated", admin, campaign_id, {"entry_id": entry_id})
    return jsonify({"status": "ok", "campaign": serialize_campaign_override_admin(doc, is_selected_active=is_active)})


@campaign_display_admin_bp.delete("/api/admin/campaign-display/campaigns/<campaign_id>/participants/<entry_id>")
def api_admin_remove_participant(campaign_id: str, entry_id: str):
    from database import db

    admin, err = _require_admin()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    doc, code = remove_participant(
        db,
        campaign_id,
        entry_id,
        updated_by=_admin_identity(admin),
        expected_updated_at=body.get("expected_updated_at"),
    )
    if code:
        status_code = {"not_found": 404, "participant_not_found": 404, "stale_update": 409}.get(code, 400)
        return jsonify({"status": "error", "code": code}), status_code
    settings = get_campaign_display_settings(db)
    is_active = settings.get("active_campaign_id") == campaign_id
    _log_admin_audit("participant_removed", admin, campaign_id, {"entry_id": entry_id})
    return jsonify({"status": "ok", "campaign": serialize_campaign_override_admin(doc, is_selected_active=is_active)})


@campaign_display_admin_bp.get("/api/admin/campaign-display/campaigns/<campaign_id>/preview")
def api_admin_preview_campaign(campaign_id: str):
    """Preview for ANY campaign_id -- including one not currently selected
    as globally active -- so an admin can check a campaign's leaderboard
    and announcement text before activating it. Uses the exact same
    build_public_campaign_activity()/render_campaign_activity_announcement_text()
    every public surface uses, so what an admin previews here is exactly
    what would go live on activation."""
    from database import db

    _, err = _require_admin()
    if err:
        return err
    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id}, {"_id": 1})
    if not doc:
        return jsonify({"status": "error", "code": "not_found"}), 404
    try:
        activity = build_public_campaign_activity(db, campaign_id)
        preview_text = render_campaign_activity_announcement_text(activity)
    except Exception:
        logger.exception("[CAMPAIGN_DISPLAY] campaign=%s admin_preview_failed", campaign_id)
        return jsonify({"status": "error", "code": "preview_unavailable"}), 500
    return jsonify({"status": "ok", **activity, "preview_text": preview_text})
