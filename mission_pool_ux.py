"""Mission Reward Pool — PHASE 2 user/operator experience layer.

WHY THIS IS A SEPARATE MODULE
-----------------------------
Phase 1 (``mission_pool.py`` / ``mission_pool_processor.py``) owns every
safety guarantee: identity dedupe, eligibility, winner selection, the
selection seed, atomic voucher allocation, reward idempotency, processing
fencing, the ``closed_at`` cutoff and the kill switch. None of that is
touched, re-implemented or wrapped in a way that could change its meaning
here.

Phase 2 owns only the *presentation contract*: one read-only endpoint the
Mini App can render a Mission card from, one read-only endpoint the admin
editor needs to know what it may still edit, mission-compatible voucher pool
discovery, and centralised Telegram deep-link generation.

Keeping it in its own module and its own blueprints is deliberate: Phase 2
can be rolled back by not registering these blueprints, leaving the Phase 1
engine deployed and dormant exactly as it was (§59 of the Phase 2 spec).

WHAT THIS MODULE DELIBERATELY DOES **NOT** DO
---------------------------------------------
  * It never writes to ``mission_entries``, ``mission_identity_claims``,
    ``campaign_rewards``, ``voucher_pools`` or the worker-owned
    ``gc_campaigns.mission_pool.*`` processing fields. Every route here is a
    GET.
  * It never re-derives eligibility, winner state or the close cutoff. The
    user-facing state is computed from Phase 1's own primitives
    (``mission_pool.submission_state``, the entry's stored status and the
    worker's ``processing_stage``), so a Phase 1 change cannot silently
    diverge from what the UI shows.
  * It never exposes an internal abuse reason to a participant. A
    disqualified entry and a non-winning entry return the identical public
    state (§44).
  * It never trusts a deep-link value for identity, ownership or winner
    state. A start parameter is a *navigation reference only*; every answer
    below is recomputed from the authenticated session and the database.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

import database
import mission_pool as mp

logger = logging.getLogger(__name__)

mission_pool_ux_bp = Blueprint("mission_pool_ux", __name__)
mission_pool_ux_admin_bp = Blueprint("mission_pool_ux_admin", __name__)


# ---------------------------------------------------------------------------
# Telegram deep link (§6, §41)
# ---------------------------------------------------------------------------

# One canonical start-parameter shape for Mission Pool, used by BOTH the
# marketing "🎯 Join Mission" post and the winner "🎁 Redeem Reward" button,
# so a campaign reference is built in exactly one place (§41).
#
# Telegram restricts start parameters to [A-Za-z0-9_-], so the campaign_id has
# to survive that alphabet unchanged; a campaign_id that does not is reported
# rather than silently mangled into a link that opens the wrong thing.
MISSION_START_PARAM_PREFIX = "mission_"
_TELEGRAM_START_PARAM_SAFE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def campaign_id_is_link_safe(campaign_id: str | None) -> bool:
    """Whether ``campaign_id`` can be carried verbatim in a Telegram start
    parameter. Checked against the *encoded* parameter, so the prefix counts
    toward Telegram's 64-character budget."""
    if not campaign_id:
        return False
    return bool(_TELEGRAM_START_PARAM_SAFE.match(MISSION_START_PARAM_PREFIX + campaign_id))


def mission_start_param(campaign_id: str) -> str:
    return MISSION_START_PARAM_PREFIX + campaign_id


def parse_mission_start_param(raw: str | None) -> str | None:
    """Inverse of :func:`mission_start_param`.

    Returns the referenced campaign_id, or ``None`` for anything that is not
    a Mission Pool start parameter — including the pre-existing ``attr_``
    ad-attribution parameter, which must keep working untouched.

    The returned value is a *navigation hint*. It is looked up server-side
    and re-checked against ``mechanic == mission_pool`` before any Mission UI
    is rendered (§5): a forged parameter can only ever produce a 404."""
    if not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not raw.startswith(MISSION_START_PARAM_PREFIX):
        return None
    campaign_id = raw[len(MISSION_START_PARAM_PREFIX):]
    if not campaign_id or not campaign_id_is_link_safe(campaign_id):
        return None
    return campaign_id


def bot_username() -> str:
    return (os.environ.get("BOT_USERNAME") or "").strip().lstrip("@")


def mission_deep_link(campaign_id: str) -> str | None:
    """The single place a Mission Mini App link is constructed (§41).

    ``https://t.me/<bot>?startapp=mission_<campaign_id>`` — the same
    ``?startapp=`` Mini App entry the existing ad-attribution flow already
    uses (main.py ``/go``), so no new link architecture is introduced.

    Returns ``None`` (never a half-built URL) when ``BOT_USERNAME`` is not
    configured or the campaign_id cannot be carried safely. Callers must
    treat ``None`` as "no CTA button", never as a reason to fail."""
    user = bot_username()
    if not user or not campaign_id_is_link_safe(campaign_id):
        return None
    return f"https://t.me/{user}?startapp={mission_start_param(campaign_id)}"


def winner_cta_reply_markup(campaign_id: str) -> dict | None:
    """Inline keyboard for the Phase 1 winner notification (§14).

    A plain ``url`` button rather than a ``web_app`` button: ``?startapp=``
    opens the same Mini App, works from any chat context, and needs no extra
    per-message validation. Returns ``None`` when no link can be built, so
    the notification still sends as text and the reward stays redeemable
    from Campaign Rewards regardless."""
    url = mission_deep_link(campaign_id)
    if not url:
        return None
    return {"inline_keyboard": [[{"text": "🎁 Redeem Reward", "url": url}]]}


# ---------------------------------------------------------------------------
# Public user-facing state machine (§13)
# ---------------------------------------------------------------------------

STATE_SCHEDULED = "scheduled"
STATE_LIVE = "live"
STATE_SUBMITTED = "submitted"
STATE_PAUSED = "paused"
STATE_CLOSED_PROCESSING = "closed_processing"
STATE_WON = "won"
STATE_NOT_WON = "not_won"
STATE_ENDED = "ended"
STATE_CANCELLED = "cancelled"

USER_STATES = (
    STATE_SCHEDULED, STATE_LIVE, STATE_SUBMITTED, STATE_PAUSED,
    STATE_CLOSED_PROCESSING, STATE_WON, STATE_NOT_WON, STATE_ENDED,
    STATE_CANCELLED,
)

# Entry statuses that mean "this identity was selected". Allocation and
# notification are separate, later steps — the reward row is what the user
# actually redeems, and it is delivered through Campaign Rewards, so the
# Mission card only ever points there and never renders a code itself.
_WINNER_ENTRY_STATUSES = frozenset({
    mp.ENTRY_STATUS_WINNER,
    mp.ENTRY_STATUS_REWARD_ALLOCATING,
    mp.ENTRY_STATUS_REWARD_ALLOCATED,
})


def user_state(campaign: dict | None, entry: dict | None,
               now: datetime | None = None) -> str:
    """The single public state for one (campaign, viewer) pair.

    Result states (``won``/``not_won``) are gated on the worker having
    reached ``processing_stage == completed`` — NOT on the entry's own
    status. An entry is stamped ``disqualified`` during the eligibility pass,
    long before selection runs; surfacing that immediately would tell an
    excluded participant they were excluded, and would do it earlier than
    everyone else's result. Waiting for ``completed`` makes a disqualified
    entry and a non-winning entry indistinguishable in both content and
    timing (§44)."""
    now = now or datetime.now(timezone.utc)
    block = (campaign or {}).get("mission_pool") or {}

    if block.get("cancelled"):
        return STATE_CANCELLED

    stage = block.get("processing_stage") or mp.STAGE_PENDING
    submitted = entry is not None

    if stage == mp.STAGE_COMPLETED and submitted:
        return STATE_WON if entry.get("status") in _WINNER_ENTRY_STATUSES else STATE_NOT_WON

    open_now, reason = mp.submission_state(campaign, now)
    if open_now:
        return STATE_SUBMITTED if submitted else STATE_LIVE
    if reason == "campaign_paused":
        return STATE_PAUSED
    if reason in ("campaign_not_started", "campaign_not_live"):
        return STATE_SCHEDULED
    # Closed: still finalising for anyone who took part, plainly over for
    # anyone who did not.
    return STATE_CLOSED_PROCESSING if submitted else STATE_ENDED


def _iso(value) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


@mission_pool_ux_bp.get("/api/mission-pool/<campaign_id>/view")
def mission_view(campaign_id: str):
    """Everything the Mission card renders, in one authenticated round trip.

    This is the ONLY request the Mini App makes for Mission Pool, and it is
    made only when a mission deep link is present, so a normal Mini App open
    is unchanged (§22).

    It is strictly read-only and strictly additive: Phase 1's
    ``GET /api/mission-pool/<id>`` and ``.../status`` keep their exact
    contracts and their existing callers."""
    if not mp.mission_pool_enabled():
        return jsonify({"status": "error", "code": "mission_pool_disabled"}), 503

    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    from campaign_centre import get_campaign

    campaign = get_campaign(campaign_id)
    # Mechanic is re-confirmed server-side. A deep link naming a standard
    # drop, a tournament or a campaign that does not exist all resolve
    # identically to 404, so the Mission UI can never activate for a
    # non-Mission campaign (§5).
    if not campaign or not mp.is_mission_pool(campaign):
        return jsonify({"status": "error", "code": "campaign_not_found"}), 404

    entry = database.db[mp.ENTRIES_COLLECTION].find_one(
        {"campaign_id": campaign_id, "telegram_user_id": uid},
        projection={"status": 1, "submitted_at": 1},
    )
    now = datetime.now(timezone.utc)
    state = user_state(campaign, entry, now)
    open_now, reason = mp.submission_state(campaign, now)
    block = campaign.get("mission_pool") or {}
    schedule = campaign.get("schedule") or {}

    cfg = campaign.get("mission_config") or {}
    return jsonify({
        "status": "ok",
        "campaign_id": campaign_id,
        "campaign_name": campaign.get("name", ""),
        # Echoed so the client can assert it before rendering anything
        # Mission-shaped, instead of inferring the mechanic from a route.
        "mechanic": mp.MECHANIC_MISSION_POOL,
        "user_state": state,
        "submissions_open": open_now,
        "reason": reason,
        "already_submitted": entry is not None,
        "mission": {
            # correct_answer is never included — answers are graded on the
            # server, exactly as in Phase 1's _public_mission_view.
            "mission_type": cfg.get("mission_type"),
            "prompt": cfg.get("prompt", ""),
            "options": [{"id": o.get("id"), "label": o.get("label")}
                        for o in (cfg.get("options") or [])],
            "min_chars": cfg.get("min_chars"),
            "max_chars": cfg.get("max_chars"),
            "max_answer_chars": mp.MAX_ANSWER_CHARS,
        },
        "schedule": {
            "starts_at": _iso(schedule.get("starts_at")),
            "ends_at": _iso(schedule.get("ends_at")),
        },
        "winner_count": block.get("winner_count"),
    })


# ---------------------------------------------------------------------------
# Admin edit state (§25, §26, §27, §29, §41)
# ---------------------------------------------------------------------------

def _require_admin():
    from vouchers import require_admin

    return require_admin()


# The Phase 1 freeze (campaign_centre.update_campaign) rejects a changed
# `mission_config` once any entry exists. These are the admin-form fields
# that live inside that document, so the UI can grey out exactly what the
# backend will refuse — rather than letting an operator type a change and
# then surfacing a 409 (§26).
MISSION_CONFIG_FIELDS = (
    "mission_type", "prompt", "options", "correct_answer",
    "keyword_case_insensitive", "min_chars", "max_chars",
)


@mission_pool_ux_admin_bp.get("/api/admin/mission-pool/<campaign_id>/edit-state")
def admin_edit_state(campaign_id: str):
    """What the admin editor is still allowed to change, decided by the same
    rule the write path enforces.

    ``mission_config_locked`` is computed the way
    ``campaign_centre.update_campaign`` computes it — "does at least one
    entry exist for this campaign" — so the UI cannot drift from the
    backend. It is advisory for the form only: the write path remains the
    authority and still rejects a frozen edit with ``mission_config_locked``.
    """
    _, err = _require_admin()
    if err:
        return err

    from campaign_centre import get_campaign

    campaign = get_campaign(campaign_id)
    if not campaign or not mp.is_mission_pool(campaign):
        return jsonify({"status": "error", "code": "not_found"}), 404

    entries = database.db[mp.ENTRIES_COLLECTION].count_documents({"campaign_id": campaign_id})
    block = campaign.get("mission_pool") or {}
    locked = entries > 0

    # Reward facts the read-only detail view renders and the edit view gates
    # on. `sufficient` is the SAME publish rule the create wizard asks for
    # (inventory_verdict), so an operator cannot be told "20 winners / 50
    # codes is fine" in one screen and blocked in the other.
    verdict = inventory_verdict(block.get("pool_id"), block.get("winner_count"))
    stage = block.get("processing_stage") or mp.STAGE_PENDING
    allocation_started = bool(
        stage in _ALLOCATION_STARTED_STAGES
        or database.db["campaign_rewards"].count_documents(
            {"campaign_id": campaign_id, "category": "mission_pool"})
    )

    return jsonify({
        "status": "ok",
        "campaign_id": campaign_id,
        "campaign_status": campaign.get("status"),
        "state": operational_state(campaign),
        "entries": entries,
        "reward": {
            **verdict,
            "allocation_method": block.get("allocation_method"),
            "allocation_started": allocation_started,
            # Phase 1 has no freeze rule for the pool itself, so this is an
            # operator-safety gate, not a backend guarantee: once winners are
            # being paid, the admin UI stops offering to repoint the pool.
            "pool_editable": not allocation_started,
        },
        "mission_config_locked": locked,
        "locked_fields": list(MISSION_CONFIG_FIELDS) if locked else [],
        # Phase 1 freezes `mission_config` ONLY. `schedule` is a separate
        # top-level field with no freeze rule of its own, so it stays
        # editable under the shared Campaign Centre lifecycle and is
        # reported independently rather than being assumed frozen too (§27).
        "schedule_editable": campaign.get("status") not in ("ended", "archived"),
        "cancelled": bool(block.get("cancelled")),
        "closed_at": _iso(block.get("closed_at")),
        "processing_stage": block.get("processing_stage") or mp.STAGE_PENDING,
        "mission_link": mission_deep_link(campaign_id),
        "mission_link_unavailable_reason": (
            None if mission_deep_link(campaign_id)
            else ("bot_username_not_configured" if not bot_username()
                  else "campaign_id_not_link_safe")
        ),
    })


@mission_pool_ux_admin_bp.get("/api/admin/mission-pool/pools")
def admin_mission_pools():
    """Voucher pools a Mission campaign is actually allowed to draw from.

    The filter is not a hardcoded list in the admin UI: it is
    ``voucher_pool_service.CAMPAIGN_ALLOCATABLE_SCOPES`` (the same set
    ``allocate_voucher`` enforces) minus ``RESERVED_LEGACY_POOL_IDS`` (the
    protected WELCOME/T1-T5 and affiliate denomination pools). If the
    backend rule changes, this list changes with it (§29)."""
    _, err = _require_admin()
    if err:
        return err

    import voucher_pool_service

    listed = [p for p in voucher_pool_service.list_pools() if pool_selectable(p)]
    stock = voucher_pool_service.pool_stock_bulk(
        [p.get("pool_id") or "" for p in listed]
    )

    out = []
    for pool in listed:
        pool_id = pool.get("pool_id") or ""
        out.append({
            "pool_id": pool_id,
            "name": pool.get("name", ""),
            "pool_type": pool.get("pool_type"),
            "allocation_scope": pool.get("allocation_scope"),
            "status": pool.get("status"),
            "stock": stock.get(pool_id) or {"available": 0, "issued": 0},
        })
    # The pool-type vocabulary is the backend's, so the inline "create a new
    # reward pool" form offers exactly what register_pool accepts rather than
    # a list hardcoded in the admin UI (§7).
    return jsonify({"status": "ok", "pools": out,
                    "pool_types": list(voucher_pool_service.POOL_TYPES)})


# ---------------------------------------------------------------------------
# Mission admin landing list + inventory gate (Phase 2.1 operator UX)
# ---------------------------------------------------------------------------
#
# Everything below is READ-ONLY, exactly like the rest of this module. The
# Phase 2.1 work is an admin *experience* change: it adds no new campaign
# type, no Mission-specific voucher inventory and no second write path. The
# operator-facing surface simply needs three answers the existing endpoints
# could not give without the browser fanning out per-campaign requests or
# aggregating raw ``mission_entries`` client-side (both explicitly out of
# bounds):
#
#   1. one list of Mission campaigns with their live counters (§2),
#   2. one authoritative "is there enough inventory to publish this?" verdict
#      shared by the create wizard and the edit view (§8), and
#   3. the reward/pool facts the read-only detail view renders (§12, §18).

# NOTE the OPS_ prefix. The STATE_* vocabulary above (submitted / won /
# not_won / ...) answers "what does this player see?"; these answer "what can
# the operator do?". Several words appear in both and mean the same thing
# today — which is exactly why they must not share a Python name.
OPS_DRAFT = "draft"
OPS_SCHEDULED = "scheduled"
OPS_LIVE = "live"
OPS_PAUSED = "paused"
OPS_CANCELLED = "cancelled"
OPS_CLOSED = "closed"
OPS_PROCESSING = "processing"
OPS_COMPLETED = "completed"

# The operator-facing state. Derived, never stored: `gc_campaigns.status`
# keeps its exact existing meaning (campaign_centre owns it) and
# `mission_pool.processing_stage` keeps its exact existing meaning (the
# worker owns it). This is only a presentation of the two together, so a
# lifecycle change in either place cannot silently disagree with the UI.
OPERATIONAL_STATES = (
    OPS_DRAFT, OPS_SCHEDULED, OPS_LIVE, OPS_PAUSED,
    OPS_CANCELLED, OPS_CLOSED, OPS_PROCESSING, OPS_COMPLETED,
)


def operational_state(campaign: dict | None) -> str:
    campaign = campaign or {}
    block = campaign.get("mission_pool") or {}
    status = campaign.get("status") or "draft"
    stage = block.get("processing_stage") or mp.STAGE_PENDING

    if block.get("cancelled"):
        return OPS_CANCELLED
    if status in ("ended", "archived"):
        if stage == mp.STAGE_COMPLETED:
            return OPS_COMPLETED
        if stage == mp.STAGE_PENDING:
            return OPS_CLOSED
        return OPS_PROCESSING
    if status == "live":
        return OPS_LIVE
    if status == "paused":
        return OPS_PAUSED
    if status == "scheduled":
        return OPS_SCHEDULED
    return OPS_DRAFT


# An entry in any of these statuses passed eligibility; the same set the
# Phase 1 summary endpoint counts as "qualified".
_QUALIFIED_ENTRY_STATUSES = [
    mp.ENTRY_STATUS_QUALIFIED, mp.ENTRY_STATUS_WINNER, mp.ENTRY_STATUS_NON_WINNER,
    mp.ENTRY_STATUS_REWARD_ALLOCATING, mp.ENTRY_STATUS_REWARD_ALLOCATED,
]
_WINNER_ENTRY_STATUSES = [
    mp.ENTRY_STATUS_WINNER, mp.ENTRY_STATUS_REWARD_ALLOCATING, mp.ENTRY_STATUS_REWARD_ALLOCATED,
]

# Processing stages at or beyond which reward allocation has begun. Past this
# point the configured pool is what winners are actually being paid from, so
# the admin UI stops offering to change it (§18).
_ALLOCATION_STARTED_STAGES = frozenset({
    mp.STAGE_ALLOCATING_REWARDS, mp.STAGE_NOTIFYING, mp.STAGE_COMPLETED,
})


def _entry_rollup(campaign_ids: list[str]) -> dict:
    """Submissions/qualified/disqualified/winners per campaign in ONE indexed
    aggregation ({campaign_id, status} is ``ix_mission_entries_campaign_status_order``).

    The alternative the spec rules out is the browser pulling raw
    ``mission_entries`` and counting them; the alternative that merely looks
    cheaper is four ``count_documents`` per campaign per page load."""
    ids = [c for c in (campaign_ids or []) if c]
    out: dict = {}
    if not ids:
        return out
    rows = database.db[mp.ENTRIES_COLLECTION].aggregate([
        {"$match": {"campaign_id": {"$in": ids}}},
        {"$group": {
            "_id": "$campaign_id",
            "submissions": {"$sum": 1},
            "qualified": {"$sum": {"$cond": [{"$in": ["$status", _QUALIFIED_ENTRY_STATUSES]}, 1, 0]}},
            "disqualified": {"$sum": {"$cond": [{"$eq": ["$status", mp.ENTRY_STATUS_DISQUALIFIED]}, 1, 0]}},
            "winners": {"$sum": {"$cond": [{"$in": ["$status", _WINNER_ENTRY_STATUSES]}, 1, 0]}},
        }},
    ])
    for row in rows:
        out[row.get("_id")] = {
            "submissions": row.get("submissions", 0),
            "qualified": row.get("qualified", 0),
            "disqualified": row.get("disqualified", 0),
            "winners": row.get("winners", 0),
        }
    return out


def _reward_rollup(campaign_ids: list[str]) -> dict:
    """Allocated/notified/failed reward counts per campaign, one aggregation.

    Scoped to ``category == "mission_pool"`` so a campaign that also produced
    tournament rewards can never inflate a Mission number."""
    ids = [c for c in (campaign_ids or []) if c]
    out: dict = {}
    if not ids:
        return out
    rows = database.db["campaign_rewards"].aggregate([
        {"$match": {"campaign_id": {"$in": ids}, "category": "mission_pool"}},
        {"$group": {
            "_id": "$campaign_id",
            "allocated": {"$sum": {"$cond": [{"$eq": ["$status", "assigned"]}, 1, 0]}},
            "notified": {"$sum": {"$cond": [{"$eq": ["$notification_status", "sent"]}, 1, 0]}},
            "notify_failed": {"$sum": {"$cond": [
                {"$in": ["$notification_status", ["failed_retryable", "failed_terminal"]]}, 1, 0]}},
        }},
    ])
    for row in rows:
        out[row.get("_id")] = {
            "allocated": row.get("allocated", 0),
            "notified": row.get("notified", 0),
            "notify_failed": row.get("notify_failed", 0),
        }
    return out


def pool_selectable(pool: dict | None) -> bool:
    """The same predicate ``admin_mission_pools`` filters the dropdown with,
    named once so the detail/edit views can report "your stored pool is no
    longer offered for NEW selection" without re-deriving the rule (§18)."""
    import voucher_pool_service

    if not pool:
        return False
    pool_id = str(pool.get("pool_id") or "").strip()
    if not pool_id or pool_id.upper() in voucher_pool_service.RESERVED_LEGACY_POOL_IDS:
        return False
    if pool.get("allocation_scope") not in voucher_pool_service.CAMPAIGN_ALLOCATABLE_SCOPES:
        return False
    return True


def inventory_verdict(pool_id: str | None, winner_count) -> dict:
    """THE publish-safety rule (§8), in one server-side place.

    ``winner_count <= available_codes``. Both the create wizard and the edit
    view ask this endpoint rather than each implementing the comparison, so
    there is exactly one definition of "enough inventory" and it is computed
    from the live registry/inventory rather than from whatever the form last
    rendered."""
    import voucher_pool_service

    pool_id = (pool_id or "").strip()
    try:
        winner_count = int(winner_count)
    except (TypeError, ValueError):
        winner_count = 0

    pool = voucher_pool_service.get_pool(pool_id) if pool_id else None
    stock = voucher_pool_service.pool_stock(pool_id) if pool_id else {"available": 0, "issued": 0}
    available = stock.get("available", 0)
    return {
        "pool_id": pool_id,
        "pool_exists": bool(pool),
        "pool_name": (pool or {}).get("name", ""),
        # The registry's REAL type. The admin UI must submit this rather than
        # a guess: the processor passes mission_pool.pool_type to
        # voucher_pool_service.allocate_voucher as expected_pool_type.
        "pool_type": (pool or {}).get("pool_type"),
        "allocation_scope": (pool or {}).get("allocation_scope"),
        "pool_active": bool(pool) and pool.get("status") == "active",
        "pool_selectable": pool_selectable(pool),
        "winner_count": winner_count,
        "available": available,
        "issued": stock.get("issued", 0),
        "shortfall": max(0, winner_count - available),
        "sufficient": bool(pool) and winner_count > 0 and available >= winner_count,
    }


@mission_pool_ux_admin_bp.get("/api/admin/mission-pool/inventory-check")
def admin_inventory_check():
    _, err = _require_admin()
    if err:
        return err
    verdict = inventory_verdict(request.args.get("pool_id"), request.args.get("winner_count"))
    return jsonify({"status": "ok", **verdict})


@mission_pool_ux_admin_bp.get("/api/admin/mission-pool/campaigns")
def admin_mission_campaigns():
    """The Mission Reward Pool landing list (§2).

    One request returns every Mission campaign with the counters the landing
    page shows, computed server-side in three aggregations total (entries,
    rewards, pool stock) regardless of how many missions exist. The browser
    never sees a raw ``mission_entries`` document."""
    _, err = _require_admin()
    if err:
        return err

    # `mechanic` is stamped server-side by campaign_centre for every campaign
    # it writes; `type` is matched too so a document written before the
    # mechanic field existed (or by a direct DB fix) still appears here
    # rather than silently vanishing from the operator's list.
    docs = list(database.db["gc_campaigns"].find(
        {"$or": [{"mechanic": mp.MECHANIC_MISSION_POOL},
                 {"type": mp.CAMPAIGN_TYPE_MISSION_POOL}]},
        sort=[("created_at", -1)],
        limit=200,
    ))

    campaign_ids = [d.get("campaign_id") for d in docs if d.get("campaign_id")]
    entries = _entry_rollup(campaign_ids)
    rewards = _reward_rollup(campaign_ids)

    import voucher_pool_service

    pool_ids = sorted({(d.get("mission_pool") or {}).get("pool_id")
                       for d in docs if (d.get("mission_pool") or {}).get("pool_id")})
    stock = voucher_pool_service.pool_stock_bulk(list(pool_ids))

    out = []
    for doc in docs:
        campaign_id = doc.get("campaign_id") or ""
        block = doc.get("mission_pool") or {}
        schedule = doc.get("schedule") or {}
        counts = entries.get(campaign_id) or {}
        reward_counts = rewards.get(campaign_id) or {}
        pool_id = block.get("pool_id") or ""
        out.append({
            "campaign_id": campaign_id,
            "name": doc.get("name", ""),
            "state": operational_state(doc),
            "campaign_status": doc.get("status"),
            "cancelled": bool(block.get("cancelled")),
            "processing_stage": block.get("processing_stage") or mp.STAGE_PENDING,
            "starts_at": _iso(schedule.get("starts_at")),
            "ends_at": _iso(schedule.get("ends_at")),
            "closed_at": _iso(block.get("closed_at")),
            "mission_type": (doc.get("mission_config") or {}).get("mission_type"),
            "winner_count": block.get("winner_count"),
            "allocation_method": block.get("allocation_method"),
            "pool_id": pool_id,
            "pool_available": (stock.get(pool_id) or {}).get("available", 0) if pool_id else 0,
            "submissions": counts.get("submissions", 0),
            "qualified": counts.get("qualified", 0),
            "disqualified": counts.get("disqualified", 0),
            "winners": counts.get("winners", 0),
            "rewards_allocated": reward_counts.get("allocated", 0),
            "notifications_sent": reward_counts.get("notified", 0),
            "notifications_failed": reward_counts.get("notify_failed", 0),
        })
    return jsonify({"status": "ok", "states": list(OPERATIONAL_STATES), "campaigns": out})
