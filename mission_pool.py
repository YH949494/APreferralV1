"""Mission Reward Pool — campaign mechanic, submission hot path, admin controls.

WHAT THIS IS
------------
A *second* reward mechanic alongside the pre-existing Standard Voucher Drop
(``db.drops`` / ``vouchers.py``). Standard Drop is untouched by this module:
no route, function, collection or field owned by ``vouchers.py`` is read or
written here.

Mission Pool reuses the Campaign Centre stack that was explicitly built to be
extended by new campaign types (see ``campaign_centre.py`` /
``campaign_rewards_api.py`` module docstrings, both of which already name
"mission" as a future type):

  * campaign record   -> ``gc_campaigns`` (campaign_centre.py), with the new
                         explicit ``mechanic`` field described below
  * voucher inventory -> ``db.voucher_pools`` via ``voucher_pool_service``
                         (atomic claim; NO second inventory collection)
  * reward instance   -> ``campaign_rewards`` with ``category="mission_pool"``
                         (same collection + same Mini App API as tournament
                         rewards, so winners see their code in the existing
                         "Campaign Rewards" section)
  * analytics         -> ``campaign_events`` via ``campaign_centre.log_funnel_event``
  * identity          -> ``miniapp_identity.resolve_authenticated_telegram_user_id``
                         (verified Telegram initData only — never a
                         client-supplied user id)

THE ``mechanic`` FIELD (§2 of the spec)
---------------------------------------
``drops.type`` (pooled/personalised) and ``voucher_pools.pool_type`` already
mean "inventory / distribution behaviour". They are NOT touched. The campaign
*mechanic* is a separate, explicit field on ``gc_campaigns``:

    mechanic = "standard_drop" | "mission_pool"

It is stamped server-side from the campaign ``type`` (never accepted from a
client) and every pre-existing document, which has no such field, resolves to
``standard_drop`` via :func:`resolve_mechanic`. Mission code only ever runs for
``resolve_mechanic(campaign) == MECHANIC_MISSION_POOL``.

SUBMISSION HOT PATH (§9)
------------------------
``POST /api/mission-pool/<campaign_id>/submit`` performs only:
verify initData -> one indexed ``gc_campaigns`` lookup -> pure in-process
validation/normalisation -> one idempotent indexed insert into
``mission_entries`` -> best-effort event. No identity resolution, no
anti-abuse lookup, no voucher work, no aggregation. Everything expensive is
done later by ``mission_pool_processor.py`` on the worker.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request
from pymongo.errors import DuplicateKeyError

import database

logger = logging.getLogger(__name__)

mission_pool_bp = Blueprint("mission_pool", __name__)
mission_pool_admin_bp = Blueprint("mission_pool_admin", __name__)


# ---------------------------------------------------------------------------
# Mechanic routing (§2)
# ---------------------------------------------------------------------------

MECHANIC_STANDARD_DROP = "standard_drop"
MECHANIC_MISSION_POOL = "mission_pool"
MECHANICS = (MECHANIC_STANDARD_DROP, MECHANIC_MISSION_POOL)

# gc_campaigns.type value for a Mission Pool campaign. Additive: no existing
# campaign type changes meaning, and campaign_centre keeps owning the shared
# draft/scheduled/live/paused/ended/archived lifecycle.
CAMPAIGN_TYPE_MISSION_POOL = "mission_pool"


def resolve_mechanic(campaign: dict | None) -> str:
    """Lazy backwards compatibility (§2/§54): a campaign document written
    before this feature existed has no ``mechanic`` field and must behave
    exactly as it always did — as a standard drop. Never migrate old rows to
    get this answer."""
    raw = (campaign or {}).get("mechanic")
    if raw in MECHANICS:
        return raw
    return MECHANIC_STANDARD_DROP


def is_mission_pool(campaign: dict | None) -> bool:
    return resolve_mechanic(campaign) == MECHANIC_MISSION_POOL


def mechanic_for_type(campaign_type: str | None) -> str:
    """Server-side derivation used by campaign_centre when writing a campaign.
    The mechanic is stored explicitly (so queries can filter on it) but is
    never accepted from the request body."""
    return MECHANIC_MISSION_POOL if campaign_type == CAMPAIGN_TYPE_MISSION_POOL else MECHANIC_STANDARD_DROP


# ---------------------------------------------------------------------------
# Kill switch / feature flag (§30, §42)
# ---------------------------------------------------------------------------

_TRUE_VALUES = ("1", "true", "yes", "on")
_FALSE_VALUES = ("0", "false", "no", "off")


def _env_flag() -> bool | None:
    """Tri-state read of ``MISSION_POOL_ENABLED``: True / False / unset."""
    raw = os.getenv("MISSION_POOL_ENABLED")
    if raw is None:
        return None
    raw = raw.strip().lower()
    if raw in _FALSE_VALUES:
        return False
    if raw in _TRUE_VALUES:
        return True
    return None


def mission_pool_enabled() -> bool:
    """Global kill switch (§30/§42), off by default.

    Two off-switches, either of which disables every Mission Pool code path
    (submission API, admin process endpoint, and the worker):

      * ``MISSION_POOL_ENABLED=0`` — hard off, wins over everything.
      * Settings -> Feature Flags -> ``mission_pool`` — live, no deploy.
        Its schema default is ``False`` and it takes its initial value from
        the same ``MISSION_POOL_ENABLED`` env var (settings_service._env_default),
        so a fresh deploy ships with Mission Pool dark until it is turned on
        deliberately (§55).

    Fails CLOSED: if the settings backend is unreachable the feature is only
    considered on when the env var says so explicitly. A settings outage must
    never silently switch a dark feature on.
    """
    env = _env_flag()
    if env is False:
        return False
    try:
        from settings_service import get_setting

        return get_setting("feature_flags", "mission_pool") is True
    except Exception:
        logger.warning("[MISSION_POOL] feature flag lookup failed; failing closed to env", exc_info=True)
        return env is True


# ---------------------------------------------------------------------------
# Mission types + answer validation (§7, §33)
# ---------------------------------------------------------------------------

MISSION_TYPE_MULTIPLE_CHOICE = "multiple_choice"
MISSION_TYPE_SINGLE_CHOICE = "single_choice"
MISSION_TYPE_KEYWORD = "keyword"
MISSION_TYPE_FEEDBACK = "feedback"

MISSION_TYPES = (
    MISSION_TYPE_MULTIPLE_CHOICE,
    MISSION_TYPE_SINGLE_CHOICE,
    MISSION_TYPE_KEYWORD,
    MISSION_TYPE_FEEDBACK,
)

# Hard server-side payload bounds. The frontend's option list is never trusted
# (§33) and an oversized body can never reach the database.
MAX_ANSWER_CHARS = 2000
MAX_OPTIONS = 20
MAX_OPTION_ID_CHARS = 64
DEFAULT_FEEDBACK_MIN_CHARS = 1
DEFAULT_FEEDBACK_MAX_CHARS = 500

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class MissionValidationError(ValueError):
    """Structured, machine-readable validation failure."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _strip_control_chars(value: str) -> str:
    return _CONTROL_CHARS.sub("", value)


def _coerce_answer_text(answer) -> str:
    """Accept only a scalar string/number. A dict/list answer is rejected
    outright — this is what stops a Mongo-operator-shaped payload
    (``{"$ne": null}``) from ever reaching a query or a stored document."""
    if isinstance(answer, bool) or answer is None:
        raise MissionValidationError("invalid_answer_type")
    if isinstance(answer, (dict, list, tuple, set)):
        raise MissionValidationError("invalid_answer_type")
    if isinstance(answer, (int, float)):
        answer = str(answer)
    if not isinstance(answer, str):
        raise MissionValidationError("invalid_answer_type")
    if len(answer) > MAX_ANSWER_CHARS:
        raise MissionValidationError("answer_too_long")
    return answer


def _option_ids(mission_config: dict) -> list[str]:
    out = []
    for opt in (mission_config.get("options") or [])[:MAX_OPTIONS]:
        if isinstance(opt, dict):
            oid = opt.get("id")
        else:
            oid = opt
        if isinstance(oid, str) and oid.strip():
            out.append(oid.strip())
    return out


def normalize_submission(mission_config: dict, answer) -> str:
    """Pure function: raw client answer -> canonical stored form.

    Normalisation is deliberately conservative (§33): NFKC + control-char
    strip + whitespace collapse always; case folding *only* when the campaign
    opted in via ``keyword_case_insensitive``. Nothing here strips
    punctuation or does fuzzy matching, so two genuinely different answers
    can never collapse into one."""
    text = _coerce_answer_text(answer)
    text = unicodedata.normalize("NFKC", text)
    text = _strip_control_chars(text)
    text = " ".join(text.split())

    mission_type = mission_config.get("mission_type")
    if mission_type in (MISSION_TYPE_MULTIPLE_CHOICE, MISSION_TYPE_SINGLE_CHOICE):
        return text
    if mission_type == MISSION_TYPE_KEYWORD:
        if mission_config.get("keyword_case_insensitive", True):
            text = text.casefold()
        return text
    return text


def validate_submission(mission_config: dict, answer) -> dict:
    """Validate + normalise one submission against a campaign's mission config.

    Returns ``{"answer": <raw bounded>, "answer_normalized": <str>,
    "is_correct": bool|None}``. Raises :class:`MissionValidationError` with a
    machine-readable ``.code`` on any rejection. Pure — no I/O, safe to call
    on the request hot path."""
    mission_type = (mission_config or {}).get("mission_type")
    if mission_type not in MISSION_TYPES:
        raise MissionValidationError("invalid_mission_config")

    raw = _coerce_answer_text(answer)
    normalized = normalize_submission(mission_config, answer)

    if mission_type in (MISSION_TYPE_MULTIPLE_CHOICE, MISSION_TYPE_SINGLE_CHOICE):
        options = _option_ids(mission_config)
        if not options:
            raise MissionValidationError("invalid_mission_config")
        if normalized not in options:
            raise MissionValidationError("invalid_option")
        correct = mission_config.get("correct_answer")
        is_correct = None
        if isinstance(correct, str) and correct.strip():
            is_correct = normalized == correct.strip()
        return {"answer": raw, "answer_normalized": normalized, "is_correct": is_correct}

    if mission_type == MISSION_TYPE_KEYWORD:
        if not normalized:
            raise MissionValidationError("empty_answer")
        correct = mission_config.get("correct_answer")
        is_correct = None
        if isinstance(correct, str) and correct.strip():
            expected = normalize_submission(mission_config, correct)
            is_correct = normalized == expected
        return {"answer": raw, "answer_normalized": normalized, "is_correct": is_correct}

    # feedback / questionnaire
    min_chars = int(mission_config.get("min_chars") or DEFAULT_FEEDBACK_MIN_CHARS)
    max_chars = int(mission_config.get("max_chars") or DEFAULT_FEEDBACK_MAX_CHARS)
    max_chars = min(max_chars, MAX_ANSWER_CHARS)
    if len(normalized) < max(1, min_chars):
        raise MissionValidationError("answer_too_short")
    if len(normalized) > max_chars:
        raise MissionValidationError("answer_too_long")
    return {"answer": raw, "answer_normalized": normalized, "is_correct": None}


def validate_mission_config(raw: dict | None) -> tuple[dict | None, str | None]:
    """Admin-side config validation. Returns (config, error_code)."""
    raw = raw or {}
    mission_type = (raw.get("mission_type") or "").strip()
    if mission_type not in MISSION_TYPES:
        return None, "invalid_mission_type"

    prompt = (raw.get("prompt") or "").strip()
    if not prompt:
        return None, "missing_mission_prompt"
    if len(prompt) > 1000:
        return None, "mission_prompt_too_long"

    config: dict = {
        "mission_type": mission_type,
        "prompt": prompt,
        "options": [],
        "correct_answer": "",
    }

    if mission_type in (MISSION_TYPE_MULTIPLE_CHOICE, MISSION_TYPE_SINGLE_CHOICE):
        options = []
        seen = set()
        for opt in (raw.get("options") or [])[: MAX_OPTIONS + 1]:
            if isinstance(opt, dict):
                oid, label = opt.get("id"), opt.get("label")
            else:
                oid, label = opt, opt
            if not isinstance(oid, str) or not oid.strip():
                return None, "invalid_option"
            oid = oid.strip()
            if len(oid) > MAX_OPTION_ID_CHARS:
                return None, "invalid_option"
            if oid in seen:
                return None, "duplicate_option"
            seen.add(oid)
            options.append({"id": oid, "label": (str(label) if label else oid)[:200]})
        if len(options) < 2:
            return None, "not_enough_options"
        if len(options) > MAX_OPTIONS:
            return None, "too_many_options"
        config["options"] = options
        correct = (raw.get("correct_answer") or "").strip()
        if correct and correct not in seen:
            return None, "correct_answer_not_an_option"
        config["correct_answer"] = correct

    elif mission_type == MISSION_TYPE_KEYWORD:
        correct = (raw.get("correct_answer") or "").strip()
        if not correct:
            return None, "missing_correct_answer"
        if len(correct) > MAX_ANSWER_CHARS:
            return None, "correct_answer_too_long"
        config["correct_answer"] = correct
        config["keyword_case_insensitive"] = bool(raw.get("keyword_case_insensitive", True))

    else:  # feedback
        try:
            min_chars = int(raw.get("min_chars") or DEFAULT_FEEDBACK_MIN_CHARS)
            max_chars = int(raw.get("max_chars") or DEFAULT_FEEDBACK_MAX_CHARS)
        except (TypeError, ValueError):
            return None, "invalid_length_bounds"
        if min_chars < 1 or max_chars > MAX_ANSWER_CHARS or min_chars > max_chars:
            return None, "invalid_length_bounds"
        config["min_chars"] = min_chars
        config["max_chars"] = max_chars

    return config, None


# ---------------------------------------------------------------------------
# Mission Pool campaign config (§6)
# ---------------------------------------------------------------------------

ALLOCATION_RANDOM_QUALIFIED = "random_qualified"
ALLOCATION_FIRST_QUALIFIED = "first_qualified"
ALLOCATION_METHODS = (ALLOCATION_RANDOM_QUALIFIED, ALLOCATION_FIRST_QUALIFIED)

# Worker-owned processing stages (§18). Stored under gc_campaigns.mission_pool
# so the *shared* campaign_centre `status` field keeps its exact existing
# meaning and a tournament campaign can never transition into one of these.
STAGE_PENDING = "pending"
STAGE_PROCESSING_ELIGIBILITY = "processing_eligibility"
STAGE_QUALIFIED_SNAPSHOT_READY = "qualified_snapshot_ready"
STAGE_SELECTING_WINNERS = "selecting_winners"
STAGE_WINNERS_SELECTED = "winners_selected"
STAGE_ALLOCATING_REWARDS = "allocating_rewards"
STAGE_NOTIFYING = "notifying"
STAGE_COMPLETED = "completed"

PROCESSING_STAGES = (
    STAGE_PENDING,
    STAGE_PROCESSING_ELIGIBILITY,
    STAGE_QUALIFIED_SNAPSHOT_READY,
    STAGE_SELECTING_WINNERS,
    STAGE_WINNERS_SELECTED,
    STAGE_ALLOCATING_REWARDS,
    STAGE_NOTIFYING,
    STAGE_COMPLETED,
)

_TERMINAL_STAGES = frozenset({STAGE_COMPLETED})

DEFAULT_ELIGIBILITY_POLICY = {
    "require_correct_answer": True,
    "exclude_voucher_hunter": True,
    "exclude_multi_account_risk": True,
    "exclude_blocked": True,
    "require_gaming_account": False,
}


def validate_mission_pool_config(raw: dict | None) -> tuple[dict | None, str | None]:
    """Validate the Mission-Pool-specific campaign block. Only the operator-
    settable fields are accepted here; all processing/state fields are owned
    by the worker and can never be set through the admin API."""
    raw = raw or {}
    pool_id = (raw.get("pool_id") or "").strip()
    if not pool_id:
        return None, "missing_pool_id"

    try:
        winner_count = int(raw.get("winner_count"))
    except (TypeError, ValueError):
        return None, "invalid_winner_count"
    if winner_count < 1 or winner_count > 100000:
        return None, "invalid_winner_count"

    allocation_method = (raw.get("allocation_method") or ALLOCATION_RANDOM_QUALIFIED).strip()
    if allocation_method not in ALLOCATION_METHODS:
        return None, "invalid_allocation_method"

    policy_raw = raw.get("eligibility_policy") or {}
    if not isinstance(policy_raw, dict):
        return None, "invalid_eligibility_policy"
    policy = {k: bool(policy_raw.get(k, v)) for k, v in DEFAULT_ELIGIBILITY_POLICY.items()}

    return {
        "pool_id": pool_id,
        "pool_type": (raw.get("pool_type") or "voucher_drop").strip(),
        "winner_count": winner_count,
        "allocation_method": allocation_method,
        "eligibility_policy": policy,
    }, None


def merge_mission_pool_config(existing: dict | None, validated: dict) -> dict:
    """Merge operator-settable fields into the stored block, preserving every
    worker-owned processing field. Prevents an admin PUT from resetting
    ``processing_generation`` (which would break fencing) or wiping a
    ``selection_seed`` (which would let a retry reshuffle winners)."""
    out = dict(existing or {})
    out.update(validated)
    out.setdefault("cancelled", False)
    out.setdefault("processing_stage", STAGE_PENDING)
    out.setdefault("processing_generation", 0)
    return out


# ---------------------------------------------------------------------------
# Entry model (§11) + indexes (§12)
# ---------------------------------------------------------------------------

ENTRIES_COLLECTION = "mission_entries"
IDENTITY_CLAIMS_COLLECTION = "mission_identity_claims"

ENTRY_STATUS_SUBMITTED = "submitted"
ENTRY_STATUS_QUALIFIED = "qualified"
ENTRY_STATUS_DISQUALIFIED = "disqualified"
ENTRY_STATUS_WINNER = "winner"
ENTRY_STATUS_NON_WINNER = "non_winner"
ENTRY_STATUS_REWARD_ALLOCATING = "reward_allocating"
ENTRY_STATUS_REWARD_ALLOCATED = "reward_allocated"

# Machine-readable disqualification reasons (§17) — never free text.
REASON_INCORRECT_ANSWER = "incorrect_answer"
REASON_DUPLICATE_IDENTITY = "duplicate_identity"
REASON_DUPLICATE_GAMING_ACCOUNT = "duplicate_gaming_account"
REASON_VOUCHER_HUNTER = "voucher_hunter"
REASON_MULTI_ACCOUNT_RISK = "multi_account_risk"
REASON_BLOCKED = "blocked"
REASON_ALREADY_REWARDED = "already_rewarded"
REASON_MISSING_GAMING_ACCOUNT = "missing_gaming_account"
REASON_INVALID_SUBMISSION = "invalid_submission"
REASON_CAMPAIGN_CANCELLED = "campaign_cancelled"
REASON_OUT_OF_STOCK = "out_of_stock"
REASON_OTHER = "other"

DISQUALIFICATION_REASONS = (
    REASON_INCORRECT_ANSWER,
    REASON_DUPLICATE_IDENTITY,
    REASON_DUPLICATE_GAMING_ACCOUNT,
    REASON_VOUCHER_HUNTER,
    REASON_MULTI_ACCOUNT_RISK,
    REASON_BLOCKED,
    REASON_ALREADY_REWARDED,
    REASON_MISSING_GAMING_ACCOUNT,
    REASON_INVALID_SUBMISSION,
    REASON_CAMPAIGN_CANCELLED,
    REASON_OUT_OF_STOCK,
    REASON_OTHER,
)

IDENTITY_TYPE_GAMING_ACCOUNT = "gaming_account"
IDENTITY_TYPE_TELEGRAM = "telegram"


def ensure_mission_indexes() -> None:
    """Every index below exists for a query this module or the processor
    actually issues; nothing speculative (§12).

      ux_mission_entries_campaign_user
          UNIQUE. The final protection against duplicate submissions — the
          hot-path insert relies on this, not on a read-then-write (§32).
      ix_mission_entries_campaign_status_order
          Processor batch scan: {campaign_id, status} sorted by
          (submitted_at, _id) — also the deterministic selection ordering.
      ix_mission_entries_campaign_identity
          Post-run audit / admin summary by resolved identity, and the
          duplicate-identity lookups in the eligibility pass.
      ux_mission_identity_claims_campaign_key
          UNIQUE. The DB-level guarantee behind "one entry per identity"
          (§13/§14) — see mission_pool_processor._claim_identity.
    """
    try:
        entries = database.db[ENTRIES_COLLECTION]
        entries.create_index(
            [("campaign_id", 1), ("telegram_user_id", 1)],
            name="ux_mission_entries_campaign_user",
            unique=True,
        )
        entries.create_index(
            [("campaign_id", 1), ("status", 1), ("submitted_at", 1), ("_id", 1)],
            name="ix_mission_entries_campaign_status_order",
        )
        entries.create_index(
            [("campaign_id", 1), ("identity_key", 1)],
            name="ix_mission_entries_campaign_identity",
        )
        claims = database.db[IDENTITY_CLAIMS_COLLECTION]
        claims.create_index(
            [("campaign_id", 1), ("identity_key", 1)],
            name="ux_mission_identity_claims_campaign_key",
            unique=True,
        )
    except Exception:
        logger.warning("[MISSION_POOL] index creation failed", exc_info=True)


ensure_mission_indexes()


def reward_idempotency_key(campaign_id: str, entry_id) -> str:
    """Stable reward key (§25). Built from the *entry* id, not the identity
    key, so no gaming-account identifier is ever embedded in a value that
    gets logged or returned."""
    return f"MISSION:{campaign_id}:{entry_id}"


def mission_reward_id(campaign_id: str, entry_id) -> str:
    """Deterministic reward_id derived from the idempotency key, so a retry
    that races past the unique-index check still targets the same
    ``campaign_rewards`` document and the same
    ``voucher_pools.issued_for_reward_id``."""
    digest = hashlib.sha256(reward_idempotency_key(campaign_id, entry_id).encode()).hexdigest()[:24]
    return f"rw_mp_{digest}"


def mask_identity_key(identity_key: str | None) -> str | None:
    """Structured logs must never carry a raw gaming-account id (§52)."""
    if not identity_key:
        return None
    key = str(identity_key)
    prefix, _, value = key.partition(":")
    if not value:
        return "***"
    return f"{prefix}:***{value[-3:]}" if len(value) > 3 else f"{prefix}:***"


# ---------------------------------------------------------------------------
# Campaign state helpers (§30, §31)
# ---------------------------------------------------------------------------

def _as_utc(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def submission_state(campaign: dict | None, now: datetime | None = None) -> tuple[bool, str]:
    """Server-authoritative answer to "may this user submit right now?" (§31).

    Interval convention, stated explicitly: ``starts_at <= now < ends_at``.
    A submission that arrives exactly at ``ends_at`` is rejected. Client
    clocks are never consulted."""
    now = now or datetime.now(timezone.utc)
    if not campaign:
        return False, "campaign_not_found"
    if not is_mission_pool(campaign):
        return False, "not_a_mission_campaign"

    block = campaign.get("mission_pool") or {}
    if block.get("cancelled"):
        return False, "campaign_cancelled"

    status = campaign.get("status")
    if status == "paused":
        return False, "campaign_paused"
    if status in ("ended", "archived"):
        return False, "campaign_closed"
    if status != "live":
        return False, "campaign_not_live"

    schedule = campaign.get("schedule") or {}
    starts_at = _as_utc(schedule.get("starts_at"))
    ends_at = _as_utc(schedule.get("ends_at"))
    if not starts_at or now < starts_at:
        return False, "campaign_not_started"
    if ends_at and now >= ends_at:
        return False, "campaign_closed"
    return True, "open"


def is_closed_for_processing(campaign: dict | None, now: datetime | None = None) -> bool:
    """A campaign is eligible for finalization once submissions can no longer
    be accepted for a *permanent* reason (closed/ended), not a temporary one
    (paused). Paused campaigns are deliberately excluded so pausing halts
    the processor as well (§30)."""
    now = now or datetime.now(timezone.utc)
    if not is_mission_pool(campaign):
        return False
    if (campaign.get("mission_pool") or {}).get("cancelled"):
        return False
    status = campaign.get("status")
    if status in ("ended", "archived"):
        return True
    if status != "live":
        return False
    ends_at = _as_utc((campaign.get("schedule") or {}).get("ends_at"))
    return bool(ends_at and now >= ends_at)


# ---------------------------------------------------------------------------
# Public API (§35)
# ---------------------------------------------------------------------------

def _get_campaign(campaign_id: str) -> dict | None:
    from campaign_centre import get_campaign

    return get_campaign(campaign_id)


def _emit(event: str, *, campaign_id: str, user_id: int | None = None, **extra) -> None:
    try:
        from campaign_centre import log_funnel_event

        log_funnel_event(event, campaign_id=campaign_id, user_id=user_id, **extra)
    except Exception:
        logger.warning("[MISSION_POOL] event emit failed event=%s", event, exc_info=True)


def _public_mission_view(campaign: dict) -> dict:
    """Never exposes ``correct_answer`` — answers are graded server-side."""
    cfg = campaign.get("mission_config") or {}
    return {
        "mission_type": cfg.get("mission_type"),
        "prompt": cfg.get("prompt", ""),
        "options": [{"id": o.get("id"), "label": o.get("label")} for o in (cfg.get("options") or [])],
        "min_chars": cfg.get("min_chars"),
        "max_chars": cfg.get("max_chars"),
    }


@mission_pool_bp.get("/api/mission-pool/<campaign_id>")
def get_mission(campaign_id: str):
    """Mission discovery. Authenticated so the caller's own submission state
    can be returned in the same round trip (two indexed lookups)."""
    if not mission_pool_enabled():
        return jsonify({"status": "error", "code": "mission_pool_disabled"}), 503

    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    campaign = _get_campaign(campaign_id)
    if not campaign or not is_mission_pool(campaign):
        return jsonify({"status": "error", "code": "campaign_not_found"}), 404

    open_now, reason = submission_state(campaign)
    entry = database.db[ENTRIES_COLLECTION].find_one(
        {"campaign_id": campaign_id, "telegram_user_id": uid},
        projection={"status": 1, "submitted_at": 1},
    )
    return jsonify({
        "status": "ok",
        "campaign_id": campaign_id,
        "campaign_name": campaign.get("name", ""),
        "mechanic": MECHANIC_MISSION_POOL,
        "submissions_open": open_now,
        "reason": reason,
        "mission": _public_mission_view(campaign),
        "already_submitted": bool(entry),
        "entry_status": (entry or {}).get("status"),
    })


@mission_pool_bp.get("/api/mission-pool/<campaign_id>/status")
def get_mission_status(campaign_id: str):
    if not mission_pool_enabled():
        return jsonify({"status": "error", "code": "mission_pool_disabled"}), 503

    from miniapp_identity import resolve_authenticated_telegram_user_id

    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    campaign = _get_campaign(campaign_id)
    if not campaign or not is_mission_pool(campaign):
        return jsonify({"status": "error", "code": "campaign_not_found"}), 404

    entry = database.db[ENTRIES_COLLECTION].find_one(
        {"campaign_id": campaign_id, "telegram_user_id": uid},
        projection={"status": 1, "submitted_at": 1, "disqualification_reason": 1},
    )
    if not entry:
        return jsonify({"status": "ok", "submitted": False, "entry_status": None})
    return jsonify({
        "status": "ok",
        "submitted": True,
        "entry_status": entry.get("status"),
        "submitted_at": entry["submitted_at"].isoformat() if entry.get("submitted_at") else None,
    })


@mission_pool_bp.post("/api/mission-pool/<campaign_id>/submit")
def submit_mission(campaign_id: str):
    """THE HOT PATH (§9). Bounded work only:

        initData verify
      + 1 indexed gc_campaigns lookup (ux_gc_campaigns_campaign_id)
      + pure in-process validation/normalisation
      + 1 projected re-read of the same indexed campaign document, so the
        state/time check is the last thing before the write (§31)
      + 1 idempotent indexed insert (ux_mission_entries_campaign_user)
      + 1 best-effort append-only event

    No user document read, no risk lookup, no voucher work, no aggregation,
    no account-graph traversal. Identity resolution and every eligibility
    decision happen later, on the worker.
    """
    if not mission_pool_enabled():
        return jsonify({"status": "error", "code": "mission_pool_disabled"}), 503

    from miniapp_identity import resolve_authenticated_telegram_user_id

    # 1. Identity comes only from server-verified Telegram initData (§10).
    uid, err = resolve_authenticated_telegram_user_id()
    if err:
        return err

    # 2/3. Campaign + mechanic routing.
    campaign = _get_campaign(campaign_id)
    if not campaign or not is_mission_pool(campaign):
        return jsonify({"status": "error", "code": "campaign_not_found"}), 404

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"status": "error", "code": "invalid_body"}), 400

    # 6/7. Validate + normalise BEFORE the state re-check, so the re-check is
    # the very last thing that happens before the write (§31).
    try:
        validated = validate_submission(campaign.get("mission_config") or {}, body.get("answer"))
    except MissionValidationError as exc:
        _emit("mission_submission_rejected", campaign_id=campaign_id, user_id=uid,
              status="fail", reason=exc.code, source="miniapp")
        return jsonify({"status": "error", "code": exc.code}), 400

    # 4/5. Campaign status + time window, evaluated against server time only.
    # Checked TWICE on purpose: once here on the already-loaded document to
    # reject the common closed/paused case without further work, and again
    # immediately before the write on a freshly re-read, tightly projected
    # document. The second read is what makes "admin closes the campaign
    # while a request is in flight" and "request starts before end_at but
    # reaches the database after it" both resolve as closed (§31). It is one
    # extra hit on ux_gc_campaigns_campaign_id returning ~5 fields, and only
    # requests that are actually about to write ever pay for it.
    now = datetime.now(timezone.utc)
    open_now, reason = submission_state(campaign, now)
    if not open_now:
        _emit("mission_submission_rejected", campaign_id=campaign_id, user_id=uid,
              status="fail", reason=reason, source="miniapp")
        return jsonify({"status": "error", "code": reason}), 409

    fresh = database.db["gc_campaigns"].find_one(
        {"campaign_id": campaign_id},
        projection={"status": 1, "mechanic": 1, "schedule": 1, "mission_pool.cancelled": 1},
    )
    now = datetime.now(timezone.utc)
    open_now, reason = submission_state(fresh, now)
    if not open_now:
        _emit("mission_submission_rejected", campaign_id=campaign_id, user_id=uid,
              status="fail", reason=reason, source="miniapp")
        return jsonify({"status": "error", "code": reason}), 409

    doc = {
        "campaign_id": campaign_id,
        "telegram_user_id": uid,
        "answer": validated["answer"],
        "answer_normalized": validated["answer_normalized"],
        "is_correct": validated["is_correct"],
        "status": ENTRY_STATUS_SUBMITTED,
        "identity_key": None,
        "identity_type": None,
        "disqualification_reason": None,
        "reward_id": None,
        "submitted_at": now,
        "created_at": now,
        "updated_at": now,
    }

    # 8. The unique index is the authority — never find_one()-then-insert (§32).
    try:
        database.db[ENTRIES_COLLECTION].insert_one(doc)
    except DuplicateKeyError:
        _emit("mission_submission_duplicate", campaign_id=campaign_id, user_id=uid, source="miniapp")
        return jsonify({"status": "ok", "submitted": True, "state": "already_submitted"})
    except Exception as exc:
        # Some drivers/fakes surface a duplicate as a generic write error.
        if "duplicate" in str(exc).lower():
            _emit("mission_submission_duplicate", campaign_id=campaign_id, user_id=uid, source="miniapp")
            return jsonify({"status": "ok", "submitted": True, "state": "already_submitted"})
        logger.exception("[MISSION_POOL] submission_insert_failed campaign=%s uid=%s", campaign_id, uid)
        _emit("mission_submission_error", campaign_id=campaign_id, user_id=uid, status="fail", reason="db_error")
        return jsonify({"status": "error", "code": "internal_error"}), 500

    _emit("mission_submitted", campaign_id=campaign_id, user_id=uid, source="miniapp")
    return jsonify({"status": "ok", "submitted": True, "state": "submitted"})


# ---------------------------------------------------------------------------
# Admin controls (§30, §40, §41)
# ---------------------------------------------------------------------------

def _require_admin():
    from vouchers import require_admin

    return require_admin()


def _audit(action: str, admin: dict, campaign_id: str, details: dict | None = None) -> None:
    try:
        database.db["campaign_admin_audit_log"].insert_one({
            "action": action,
            "entity": "mission_pool_campaign",
            "entity_id": campaign_id,
            "admin": (admin or {}).get("usernameLower") or str((admin or {}).get("id", "")),
            "details": details or {},
            "at": datetime.now(timezone.utc),
        })
    except Exception:
        logger.warning("[MISSION_POOL] audit_write_failed", exc_info=True)


def _load_mission_campaign(campaign_id: str):
    campaign = _get_campaign(campaign_id)
    if not campaign or not is_mission_pool(campaign):
        return None, (jsonify({"status": "error", "code": "not_found"}), 404)
    return campaign, None


@mission_pool_admin_bp.post("/api/admin/mission-pool/<campaign_id>/close")
def admin_close_mission(campaign_id: str):
    """CLOSED: submissions stop, campaign becomes eligible for processing.
    Already-issued history is never touched."""
    admin, err = _require_admin()
    if err:
        return err
    campaign, err = _load_mission_campaign(campaign_id)
    if err:
        return err

    database.db["gc_campaigns"].update_one(
        {"campaign_id": campaign_id},
        {"$set": {"status": "ended", "updated_at": datetime.now(timezone.utc)}},
    )
    _audit("mission_campaign_closed", admin, campaign_id)
    _emit("mission_campaign_closed", campaign_id=campaign_id, source="admin")
    return jsonify({"status": "ok", "campaign_status": "ended"})


@mission_pool_admin_bp.post("/api/admin/mission-pool/<campaign_id>/cancel")
def admin_cancel_mission(campaign_id: str):
    """CANCELLED: submissions blocked, no new winner selection, no new
    allocation, no new notification. Vouchers already atomically allocated
    stay allocated and stay visible in Campaign Rewards — cancelling never
    reclaims a reward that already belongs to a winner (§30, §62)."""
    admin, err = _require_admin()
    if err:
        return err
    campaign, err = _load_mission_campaign(campaign_id)
    if err:
        return err

    now = datetime.now(timezone.utc)
    database.db["gc_campaigns"].update_one(
        {"campaign_id": campaign_id},
        {"$set": {
            "mission_pool.cancelled": True,
            "mission_pool.cancelled_at": now,
            "mission_pool.updated_at": now,
            "updated_at": now,
        }},
    )
    _audit("mission_campaign_cancelled", admin, campaign_id)
    _emit("mission_campaign_cancelled", campaign_id=campaign_id, source="admin")
    return jsonify({"status": "ok", "cancelled": True})


@mission_pool_admin_bp.post("/api/admin/mission-pool/<campaign_id>/resume")
def admin_resume_mission(campaign_id: str):
    """Undo a cancel. Deliberately does NOT rewind ``processing_stage``: a
    campaign whose winners were already selected resumes from where it was,
    it never re-runs selection."""
    admin, err = _require_admin()
    if err:
        return err
    campaign, err = _load_mission_campaign(campaign_id)
    if err:
        return err

    now = datetime.now(timezone.utc)
    database.db["gc_campaigns"].update_one(
        {"campaign_id": campaign_id},
        {"$set": {"mission_pool.cancelled": False, "mission_pool.updated_at": now, "updated_at": now}},
    )
    _audit("mission_campaign_resumed", admin, campaign_id)
    return jsonify({"status": "ok", "cancelled": False})


@mission_pool_admin_bp.post("/api/admin/mission-pool/<campaign_id>/process")
def admin_process_mission(campaign_id: str):
    """Manual reprocess. Safe by construction: it runs the same resumable
    state machine the worker runs, which never re-selects winners once
    ``selection_seed`` is set and never allocates a second voucher for an
    entry that already has one (§34, §40)."""
    admin, err = _require_admin()
    if err:
        return err
    campaign, err = _load_mission_campaign(campaign_id)
    if err:
        return err
    if not mission_pool_enabled():
        return jsonify({"status": "error", "code": "mission_pool_disabled"}), 503

    import mission_pool_processor

    result = mission_pool_processor.process_campaign(campaign_id, source="admin")
    _audit("mission_campaign_processed", admin, campaign_id, {"result": result})
    return jsonify({"status": "ok", "result": result})


@mission_pool_admin_bp.get("/api/admin/mission-pool/<campaign_id>/summary")
def admin_mission_summary(campaign_id: str):
    """Admin summary (§41). Every metric is labelled with its grain — a
    voucher count is never presented as a player count."""
    _, err = _require_admin()
    if err:
        return err
    campaign, err = _load_mission_campaign(campaign_id)
    if err:
        return err

    entries = database.db[ENTRIES_COLLECTION]
    block = campaign.get("mission_pool") or {}

    def _count(query):
        return entries.count_documents({"campaign_id": campaign_id, **query})

    reasons = {}
    for reason in DISQUALIFICATION_REASONS:
        n = _count({"disqualification_reason": reason})
        if n:
            reasons[reason] = n

    rewards = database.db["campaign_rewards"]
    reward_base = {"campaign_id": campaign_id, "category": "mission_pool"}

    return jsonify({
        "status": "ok",
        "campaign_id": campaign_id,
        "campaign_name": campaign.get("name", ""),
        "campaign_status": campaign.get("status"),
        "cancelled": bool(block.get("cancelled")),
        "processing_stage": block.get("processing_stage", STAGE_PENDING),
        "processing_generation": block.get("processing_generation", 0),
        "grains": {
            "submissions_telegram_user_grain": _count({}),
            "deduplicated_identity_grain": len(
                database.db[IDENTITY_CLAIMS_COLLECTION].distinct(
                    "identity_key", {"campaign_id": campaign_id}
                )
            ),
            "qualified_identity_grain": _count({"status": {"$in": [
                ENTRY_STATUS_QUALIFIED, ENTRY_STATUS_WINNER, ENTRY_STATUS_NON_WINNER,
                ENTRY_STATUS_REWARD_ALLOCATING, ENTRY_STATUS_REWARD_ALLOCATED,
            ]}}),
            "disqualified_telegram_user_grain": _count({"status": ENTRY_STATUS_DISQUALIFIED}),
            "winners_identity_grain": _count({"status": {"$in": [
                ENTRY_STATUS_WINNER, ENTRY_STATUS_REWARD_ALLOCATING, ENTRY_STATUS_REWARD_ALLOCATED,
            ]}}),
            "rewards_allocated_voucher_grain": rewards.count_documents(
                {**reward_base, "status": "assigned"}
            ),
            "notifications_sent_voucher_grain": rewards.count_documents(
                {**reward_base, "notification_status": "sent"}
            ),
            "notifications_failed_voucher_grain": rewards.count_documents(
                {**reward_base, "notification_status": {"$in": ["failed_retryable", "failed_terminal"]}}
            ),
        },
        "winner_count_requested": block.get("winner_count_requested", block.get("winner_count")),
        "winner_count_actual": block.get("winner_count_actual"),
        "qualified_count": block.get("qualified_count"),
        "disqualification_reasons": reasons,
        "selection": {
            "allocation_method": block.get("allocation_method"),
            "selection_seed_present": bool(block.get("selection_seed")),
            "selection_started_at": (block.get("selection_started_at").isoformat()
                                      if isinstance(block.get("selection_started_at"), datetime) else None),
            "selection_completed_at": (block.get("selection_completed_at").isoformat()
                                        if isinstance(block.get("selection_completed_at"), datetime) else None),
        },
    })
