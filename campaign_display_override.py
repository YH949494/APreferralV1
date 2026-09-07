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

from datetime import datetime, timezone
from html import escape as html_escape
import logging
from typing import Any

from time_utils import as_aware_utc

logger = logging.getLogger(__name__)

CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION = "campaign_display_overrides"

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


def _validate_participant(raw: Any, *, campaign_id: str, seen_entry_ids: set[str]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        logger.warning("[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=not_a_dict", campaign_id)
        return None

    entry_id = raw.get("entry_id")
    if not isinstance(entry_id, str) or not entry_id.strip():
        logger.warning("[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=missing_entry_id", campaign_id)
        return None
    entry_id = entry_id.strip()
    if entry_id in seen_entry_ids:
        logger.warning(
            "[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=duplicate_entry_id entry_id=%s",
            campaign_id,
            entry_id,
        )
        return None

    display_name = raw.get("display_name")
    if not isinstance(display_name, str) or not display_name.strip():
        logger.warning(
            "[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=missing_display_name entry_id=%s",
            campaign_id,
            entry_id,
        )
        return None
    display_name = display_name.strip()
    if len(display_name) > MAX_DISPLAY_NAME_LENGTH:
        logger.warning(
            "[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=display_name_too_long entry_id=%s length=%s",
            campaign_id,
            entry_id,
            len(display_name),
        )
        return None

    qualified_count = raw.get("qualified_count")
    # bool is a subclass of int in Python -- must be excluded explicitly, a
    # float or numeric string must be excluded too (no implicit coercion).
    if isinstance(qualified_count, bool) or not isinstance(qualified_count, int):
        logger.warning(
            "[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=bad_qualified_count_type entry_id=%s",
            campaign_id,
            entry_id,
        )
        return None
    if qualified_count < 0 or qualified_count > MAX_DISPLAY_QUALIFIED_COUNT:
        logger.warning(
            "[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=qualified_count_out_of_range entry_id=%s",
            campaign_id,
            entry_id,
        )
        return None

    visible = raw.get("visible", True)
    if not isinstance(visible, bool):
        logger.warning(
            "[CAMPAIGN_DISPLAY] campaign=%s invalid_participant reason=bad_visible_type entry_id=%s",
            campaign_id,
            entry_id,
        )
        return None

    return {
        "entry_id": entry_id,
        "display_name": display_name,
        "qualified_count": qualified_count,
        "visible": visible,
    }


def load_active_campaign_override(db_ref, campaign_id: str, *, reference_utc: datetime | None = None) -> dict[str, Any]:
    """Load and validate the override document for exactly `campaign_id`.

    Never raises. Fails closed (returns active=False, participants=[]) on
    any lookup failure or malformed document, always with a structured log
    line, so a broken/expired override can never break or leak into the
    genuine leaderboard or another campaign.
    """
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)

    try:
        doc = db_ref[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": campaign_id})
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] campaign=%s override_lookup_failed", campaign_id, exc_info=True)
        return {"active": False, "participants": []}

    if not doc:
        logger.debug("[CAMPAIGN_DISPLAY] campaign=%s override_not_found", campaign_id)
        return {"active": False, "participants": []}

    try:
        if doc.get("campaign_id") != campaign_id:
            logger.warning(
                "[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=campaign_id_mismatch",
                campaign_id,
            )
            return {"active": False, "participants": []}

        if doc.get("enabled") is not True:
            logger.debug("[CAMPAIGN_DISPLAY] campaign=%s override_disabled", campaign_id)
            return {"active": False, "participants": []}

        starts_at = _coerce_aware_utc(doc.get("starts_at"))
        ends_at = _coerce_aware_utc(doc.get("ends_at"))
        if starts_at is None or ends_at is None:
            logger.warning("[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=bad_schedule", campaign_id)
            return {"active": False, "participants": []}

        if not (starts_at <= now_utc_ts < ends_at):
            logger.debug(
                "[CAMPAIGN_DISPLAY] campaign=%s override_outside_window starts_at=%s ends_at=%s now=%s",
                campaign_id,
                starts_at.isoformat(),
                ends_at.isoformat(),
                now_utc_ts.isoformat(),
            )
            return {"active": False, "participants": []}

        raw_participants = doc.get("participants")
        if not isinstance(raw_participants, list):
            logger.warning(
                "[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=participants_not_a_list",
                campaign_id,
            )
            return {"active": False, "participants": []}

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
        return {"active": True, "participants": visible_rows}
    except Exception:
        logger.warning("[CAMPAIGN_DISPLAY] campaign=%s override_malformed reason=unexpected_error", campaign_id, exc_info=True)
        return {"active": False, "participants": []}


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

    Combines the genuine (canonical, current-month) qualified-referral
    leaderboard with any active manual display override for
    `campaign_id`, and returns one result every public surface (Affiliate
    page, public referral leaderboard, and any future consumer) must
    render from -- so they can never disagree.

    Never raises: any failure in either the genuine leaderboard or the
    override falls back to an empty contribution from that side rather
    than breaking the whole response.
    """
    now_utc_ts = reference_utc or datetime.now(timezone.utc)
    if now_utc_ts.tzinfo is None:
        now_utc_ts = now_utc_ts.replace(tzinfo=timezone.utc)
    now_utc_ts = now_utc_ts.astimezone(timezone.utc)

    genuine_rows: list[dict[str, Any]] = []
    try:
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

    override_result = {"active": False, "participants": []}
    try:
        override_result = load_active_campaign_override(db, campaign_id, reference_utc=now_utc_ts)
    except Exception:
        logger.exception("[CAMPAIGN_DISPLAY] campaign=%s override_load_unexpected_error", campaign_id)
        override_result = {"active": False, "participants": []}

    override_active = bool(override_result.get("active"))
    manual_participants = override_result.get("participants") or []

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
    }
    logger.debug(
        "[CAMPAIGN_DISPLAY] campaign=%s active=%s genuine_rows=%s manual_rows=%s",
        campaign_id,
        str(override_active).lower(),
        diagnostics["genuine_rows"],
        diagnostics["manual_rows"],
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
