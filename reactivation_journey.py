from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Callable

from config import KL_TZ

try:
    from pymongo import ASCENDING, ReturnDocument
    from pymongo.errors import DuplicateKeyError
except ModuleNotFoundError:  # pragma: no cover
    ASCENDING = 1

    class _ReturnDocument:
        AFTER = True

    ReturnDocument = _ReturnDocument()

    class DuplicateKeyError(Exception):
        pass

try:
    from telegram_utils import send_telegram_http_message
except Exception:  # pragma: no cover
    send_telegram_http_message = None

try:
    from xp import grant_xp
except Exception:  # pragma: no cover
    grant_xp = None

logger = logging.getLogger(__name__)

CAMPAIGN_ID = "official_channel_reactivation_phase1"
REWARD_REPEAT_DAYS = int(os.getenv("REACTIVATION_REWARD_REPEAT_DAYS", "180"))
POOLS = {
    1: "COMEBACK_T1",
    2: "COMEBACK_T2",
    3: "COMEBACK_T3",
}
TIER_MESSAGES = {
    1: "🎁 Welcome back! Your Comeback Voucher is ready.",
    2: "🔥 You stayed active for 7 days. Your second voucher is ready.",
    3: "🏆 30 days active! Your final comeback voucher is ready.",
}

# Admin-controlled rollout config. Everything defaults OFF so deploying this
# module never starts handing out rewards on its own — an admin must opt in
# via the dashboard (or the config endpoints) before any real user is affected.
CONFIG_ID = "reactivation_journey"
VALID_MODES = {"disabled", "test_users_only", "enabled"}
VALID_REWARD_TYPES = {"disabled", "xp_only", "tiered_vouchers", "xp_plus_tiered_vouchers"}

DEFAULT_CONFIG = {
    "_id": CONFIG_ID,
    "mode": "disabled",
    "reward_type": "tiered_vouchers",
    "test_user_ids": [],
    "tier1": {"pool_enabled": True, "xp_amount": 0},
    "tier2": {"pool_enabled": True, "threshold_days": 5, "window_days": 7, "xp_amount": 0},
    "tier3": {"pool_enabled": True, "threshold_days": 20, "window_days": 30, "xp_amount": 0},
    "campaign_start_at": None,
    "campaign_end_at": None,
}


def _config_col(db_ref):
    return _col(db_ref, "reactivation_journey_config")


def get_journey_config(db_ref) -> dict:
    stored = _config_col(db_ref).find_one({"_id": CONFIG_ID}) or {}
    cfg = {
        "mode": DEFAULT_CONFIG["mode"],
        "reward_type": DEFAULT_CONFIG["reward_type"],
        "test_user_ids": list(DEFAULT_CONFIG["test_user_ids"]),
        "tier1": dict(DEFAULT_CONFIG["tier1"]),
        "tier2": dict(DEFAULT_CONFIG["tier2"]),
        "tier3": dict(DEFAULT_CONFIG["tier3"]),
        "campaign_start_at": DEFAULT_CONFIG["campaign_start_at"],
        "campaign_end_at": DEFAULT_CONFIG["campaign_end_at"],
        "updated_at": None,
    }
    for key in ("mode", "reward_type", "test_user_ids", "campaign_start_at", "campaign_end_at", "updated_at"):
        if key in stored:
            cfg[key] = stored[key]
    for tier_key in ("tier1", "tier2", "tier3"):
        if isinstance(stored.get(tier_key), dict):
            cfg[tier_key] = {**cfg[tier_key], **stored[tier_key]}
    if cfg["mode"] not in VALID_MODES:
        cfg["mode"] = "disabled"
    if cfg["reward_type"] not in VALID_REWARD_TYPES:
        cfg["reward_type"] = "disabled"
    cfg["test_user_ids"] = {int(x) for x in (cfg.get("test_user_ids") or []) if str(x).strip().lstrip("-").isdigit()}
    return cfg


TIER_FIELD_BOUNDS = {
    "pool_enabled": bool,
    "threshold_days": (1, 3650),
    "window_days": (1, 3650),
    "xp_amount": (0, 1_000_000),
}


def _validate_tier_cfg(tier_key: str, raw: dict) -> tuple[dict | None, str | None]:
    unknown = set(raw.keys()) - set(TIER_FIELD_BOUNDS.keys())
    if unknown:
        return None, f"bad_{tier_key}_field:{','.join(sorted(unknown))}"
    cleaned = {}
    for field, value in raw.items():
        bound = TIER_FIELD_BOUNDS[field]
        if bound is bool:
            if not isinstance(value, bool):
                return None, f"bad_{tier_key}_{field}"
            cleaned[field] = value
            continue
        lo, hi = bound
        try:
            num = int(value)
        except (TypeError, ValueError):
            return None, f"bad_{tier_key}_{field}"
        if isinstance(value, bool) or not (lo <= num <= hi):
            return None, f"bad_{tier_key}_{field}"
        cleaned[field] = num
    return cleaned, None


def update_journey_config(db_ref, updates: dict, *, now_ref: datetime | None = None) -> dict:
    ts = _coerce_utc(now_ref) or now_utc()
    allowed = {"mode", "reward_type", "test_user_ids", "tier1", "tier2", "tier3", "campaign_start_at", "campaign_end_at"}
    changes = {k: v for k, v in (updates or {}).items() if k in allowed}
    if "mode" in changes and changes["mode"] not in VALID_MODES:
        return {"success": False, "reason": "bad_mode"}
    if "reward_type" in changes and changes["reward_type"] not in VALID_REWARD_TYPES:
        return {"success": False, "reason": "bad_reward_type"}
    if "test_user_ids" in changes:
        raw = changes["test_user_ids"]
        if isinstance(raw, str):
            raw = [x.strip() for x in raw.replace(",", "\n").split("\n")]
        cleaned = []
        for x in raw or []:
            try:
                cleaned.append(int(x))
            except (TypeError, ValueError):
                return {"success": False, "reason": "bad_test_user_ids"}
        changes["test_user_ids"] = cleaned
    for tier_key in ("tier1", "tier2", "tier3"):
        if tier_key not in changes:
            continue
        if not isinstance(changes[tier_key], dict):
            return {"success": False, "reason": f"bad_{tier_key}"}
        cleaned_tier, err = _validate_tier_cfg(tier_key, changes[tier_key])
        if err:
            return {"success": False, "reason": err}
        changes[tier_key] = cleaned_tier
    for dt_key in ("campaign_start_at", "campaign_end_at"):
        if dt_key not in changes:
            continue
        if changes[dt_key] in (None, ""):
            changes[dt_key] = None
            continue
        coerced = _coerce_utc(changes[dt_key])
        if coerced is None:
            return {"success": False, "reason": f"bad_{dt_key}"}
        changes[dt_key] = coerced
    if "campaign_start_at" in changes and "campaign_end_at" in changes:
        start, end = changes["campaign_start_at"], changes["campaign_end_at"]
        if start and end and start >= end:
            return {"success": False, "reason": "bad_campaign_window"}
    changes["updated_at"] = ts
    _config_col(db_ref).update_one({"_id": CONFIG_ID}, {"$set": changes}, upsert=True)
    return {"success": True, "config": get_journey_config(db_ref)}


def _within_campaign_window(cfg: dict, ts: datetime) -> bool:
    start = _coerce_utc(cfg.get("campaign_start_at"))
    end = _coerce_utc(cfg.get("campaign_end_at"))
    if start and ts < start:
        return False
    if end and ts > end:
        return False
    return True


def is_journey_enabled_for_user(db_ref, uid: int, *, cfg: dict | None = None, now_ref: datetime | None = None) -> tuple[bool, str]:
    ts = _coerce_utc(now_ref) or now_utc()
    cfg = cfg or get_journey_config(db_ref)
    mode = cfg.get("mode", "disabled")
    if mode == "disabled":
        return False, "mode_disabled"
    if cfg.get("reward_type") == "disabled":
        return False, "reward_type_disabled"
    if not _within_campaign_window(cfg, ts):
        return False, "outside_campaign_window"
    if mode == "test_users_only" and int(uid) not in cfg.get("test_user_ids", set()):
        return False, "not_test_user"
    return True, "ok"


VALID_COMPUTED_STATUSES = {"disabled", "test_only", "live", "scheduled", "expired", "config_error"}


def compute_journey_status(cfg: dict, now_ref: datetime | None = None) -> str:
    """Human-facing rollout status derived from the raw config, distinct from
    is_journey_enabled_for_user's per-user gate."""
    ts = _coerce_utc(now_ref) or now_utc()
    mode = cfg.get("mode")
    reward_type = cfg.get("reward_type")
    if mode not in VALID_MODES or reward_type not in VALID_REWARD_TYPES:
        return "config_error"
    start = _coerce_utc(cfg.get("campaign_start_at"))
    end = _coerce_utc(cfg.get("campaign_end_at"))
    if start and end and start >= end:
        return "config_error"
    if mode == "disabled" or reward_type == "disabled":
        return "disabled"
    if start and ts < start:
        return "scheduled"
    if end and ts > end:
        return "expired"
    if mode == "test_users_only":
        return "test_only"
    if mode == "enabled":
        return "live"
    return "config_error"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_utc(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _col(db_ref, name: str):
    try:
        return db_ref[name]
    except Exception:
        return getattr(db_ref, name)


def _journeys(db_ref):
    return _col(db_ref, "reactivation_journey")


def _voucher_pools(db_ref):
    return _col(db_ref, "voucher_pools")


def ensure_reactivation_journey_indexes(db_ref) -> None:
    journeys = _journeys(db_ref)
    journeys.create_index([("user_id", ASCENDING), ("campaign_id", ASCENDING)], unique=True, name="uq_reactivation_journey_user_campaign")
    journeys.create_index([("status", ASCENDING), ("updated_at", ASCENDING)], name="ix_reactivation_journey_status_updated")
    journeys.create_index([("reactivated_at", ASCENDING)], name="ix_reactivation_journey_reactivated")
    for field in ("tier1_completed_at", "tier2_completed_at", "tier3_completed_at"):
        journeys.create_index([(field, ASCENDING)], name=f"ix_reactivation_journey_{field}")
    _voucher_pools(db_ref).create_index([("pool_id", ASCENDING), ("code", ASCENDING)], unique=True, name="uq_voucher_pool_code")
    _voucher_pools(db_ref).create_index([("pool_id", ASCENDING), ("status", ASCENDING)], name="ix_voucher_pool_status")


def _is_blocked_user(user_doc: dict | None) -> tuple[bool, str | None]:
    user_doc = user_doc or {}
    for field in ("blocked", "banned", "is_banned"):
        if user_doc.get(field):
            return True, field
    if str(user_doc.get("status") or "").strip().lower() == "banned":
        return True, "status_banned"
    return False, None


def _user_doc(db_ref, uid: int) -> dict:
    return (_col(db_ref, "users").find_one({"user_id": int(uid)}) or {})


def _repeat_cutoff(now_ref: datetime) -> datetime:
    return now_ref - timedelta(days=REWARD_REPEAT_DAYS)


def create_or_update_journey(
    db_ref,
    uid: int,
    *,
    campaign_id: str = CAMPAIGN_ID,
    verified_at: datetime | None = None,
    now_ref: datetime | None = None,
    allow_repeat: bool = False,
) -> dict:
    ts = _coerce_utc(now_ref) or now_utc()
    verified_ts = _coerce_utc(verified_at) or ts
    uid = int(uid)
    cfg = get_journey_config(db_ref)
    allowed, reason = is_journey_enabled_for_user(db_ref, uid, cfg=cfg, now_ref=ts)
    if not allowed:
        logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s reason=%s", uid, campaign_id, reason)
        return {"success": False, "code": "disabled", "reason": reason}

    user_doc = _user_doc(db_ref, uid)
    blocked, reason = _is_blocked_user(user_doc)
    if blocked:
        logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s reason=%s", uid, campaign_id, reason)
        return {"success": False, "code": "blocked", "reason": reason}

    if not allow_repeat:
        recent = _journeys(db_ref).find_one(
            {
                "user_id": uid,
                "campaign_id": campaign_id,
                "$or": [
                    {"tier1_claimed_at": {"$gte": _repeat_cutoff(ts)}},
                    {"tier2_claimed_at": {"$gte": _repeat_cutoff(ts)}},
                    {"tier3_claimed_at": {"$gte": _repeat_cutoff(ts)}},
                    {"status": "completed", "updated_at": {"$gte": _repeat_cutoff(ts)}},
                ],
            }
        )
        if recent:
            logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s reason=repeat_window", uid, campaign_id)
            return {"success": False, "code": "repeat_window", "journey": recent}

    doc = {
        "user_id": uid,
        "campaign_id": campaign_id,
        "reactivated_at": verified_ts,
        "verified_at": verified_ts,
        "tier1_completed_at": None,
        "tier1_claimed_at": None,
        "tier1_voucher_code": None,
        "tier2_completed_at": None,
        "tier2_claimed_at": None,
        "tier2_voucher_code": None,
        "tier3_completed_at": None,
        "tier3_claimed_at": None,
        "tier3_voucher_code": None,
        "active_days_7": 0,
        "active_days_30": 0,
        "status": "active",
        "blocked_reason": None,
        "created_at": ts,
        "updated_at": ts,
    }
    try:
        _journeys(db_ref).insert_one(doc)
        logger.info("[REACT_JOURNEY][CREATE] uid=%s campaign_id=%s", uid, campaign_id)
        return {"success": True, "code": "created", "journey": doc}
    except DuplicateKeyError:
        reset = {
            "verified_at": verified_ts,
            "reactivated_at": verified_ts,
            "status": "active",
            "blocked_reason": None,
            "updated_at": ts,
            "tier1_completed_at": None,
            "tier1_claimed_at": None,
            "tier1_voucher_code": None,
            "tier1_voucher_status": None,
            "tier2_completed_at": None,
            "tier2_claimed_at": None,
            "tier2_voucher_code": None,
            "tier2_voucher_status": None,
            "tier3_completed_at": None,
            "tier3_claimed_at": None,
            "tier3_voucher_code": None,
            "tier3_voucher_status": None,
            "active_days_7": 0,
            "active_days_30": 0,
        }
        _journeys(db_ref).update_one(
            {"user_id": uid, "campaign_id": campaign_id},
            {"$set": reset},
        )
        existing = _journeys(db_ref).find_one({"user_id": uid, "campaign_id": campaign_id}) or {}
        logger.info("[REACT_JOURNEY][RESET] uid=%s campaign_id=%s reason=repeat_journey", uid, campaign_id)
        return {"success": True, "code": "reset", "journey": existing}


def upload_pool_codes(db_ref, pool_id: str, codes: list[str], *, metadata: dict | None = None, now_ref: datetime | None = None) -> dict:
    pool_id = str(pool_id or "").strip().upper()
    if pool_id not in set(POOLS.values()):
        return {"success": False, "reason": "bad_pool_id", "inserted": 0, "received": 0, "duplicates": 0}
    ts = _coerce_utc(now_ref) or now_utc()
    metadata = metadata or {}
    seen = set()
    inserted = duplicates = 0
    normalized = []
    for raw in codes or []:
        code = str(raw or "").strip()
        if not code or code.lower() == "code":
            continue
        if code in seen:
            duplicates += 1
            continue
        seen.add(code)
        normalized.append(code)
    for code in normalized:
        doc = {
            "pool_id": pool_id,
            "code": code,
            "status": "available",
            "issued_to": None,
            "issued_at": None,
            "journey_id": None,
            "tier": None,
            "created_at": ts,
            **metadata,
        }
        try:
            _voucher_pools(db_ref).insert_one(doc)
            inserted += 1
        except DuplicateKeyError:
            duplicates += 1
    return {"success": True, "pool_id": pool_id, "inserted": inserted, "received": len(normalized), "duplicates": duplicates}


def _issue_code(db_ref, uid: int, tier: int, *, journey_id=None, now_ref: datetime | None = None) -> tuple[str | None, str]:
    ts = _coerce_utc(now_ref) or now_utc()
    pool_id = POOLS[int(tier)]
    update = {
        "$set": {
            "status": "issued",
            "issued_to": int(uid),
            "issued_at": ts,
            "journey_id": journey_id,
            "tier": f"T{tier}",
        }
    }
    doc = _voucher_pools(db_ref).find_one_and_update(
        {"pool_id": pool_id, "status": "available"},
        update,
        sort=[("created_at", ASCENDING), ("code", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        logger.warning("[REACT_JOURNEY][OUT_OF_STOCK] uid=%s campaign_id=%s tier=%s reason=pool_empty", uid, CAMPAIGN_ID, tier)
        return None, "OUT_OF_STOCK"
    return doc.get("code"), "ISSUED"


def _send_pm(uid: int, text: str, send_fn=None) -> None:
    sender = send_fn or send_telegram_http_message
    if not sender:
        return
    try:
        result = sender(int(uid), text)
        if isinstance(result, tuple) and result and not result[0]:
            logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s reason=pm_not_sent:%s", uid, CAMPAIGN_ID, result[1] if len(result) > 1 else "")
    except Exception:
        logger.exception("[REACT_JOURNEY][ERROR] uid=%s campaign_id=%s reason=pm_send_failed", uid, CAMPAIGN_ID)


def complete_tier(db_ref, uid: int, tier: int, *, campaign_id: str = CAMPAIGN_ID, now_ref: datetime | None = None, send_fn=None, cfg: dict | None = None) -> dict:
    ts = _coerce_utc(now_ref) or now_utc()
    uid = int(uid)
    tier = int(tier)
    completed_field = f"tier{tier}_completed_at"
    claimed_field = f"tier{tier}_claimed_at"
    code_field = f"tier{tier}_voucher_code"
    status_field = f"tier{tier}_voucher_status"

    cfg = cfg or get_journey_config(db_ref)
    allowed, reason = is_journey_enabled_for_user(db_ref, uid, cfg=cfg, now_ref=ts)
    if not allowed:
        logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s tier=%s reason=%s", uid, campaign_id, tier, reason)
        return {"success": False, "code": "disabled", "reason": reason}

    user_doc = _user_doc(db_ref, uid)
    blocked, reason = _is_blocked_user(user_doc)
    if blocked:
        _journeys(db_ref).update_one(
            {"user_id": uid, "campaign_id": campaign_id},
            {"$set": {"status": "blocked", "blocked_reason": reason, "updated_at": ts}},
        )
        logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s tier=%s reason=%s", uid, campaign_id, tier, reason)
        return {"success": False, "code": "blocked", "reason": reason}

    journey = _journeys(db_ref).find_one({"user_id": uid, "campaign_id": campaign_id, completed_field: None, "status": {"$ne": "blocked"}})
    if not journey:
        logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s tier=%s reason=already_completed_or_missing", uid, campaign_id, tier)
        return {"success": True, "code": "noop"}
    if tier == 2 and not journey.get("tier1_completed_at"):
        return {"success": False, "code": "tier1_required"}
    if tier == 3 and not journey.get("tier2_completed_at"):
        return {"success": False, "code": "tier2_required"}

    # Atomically claim this tier before touching voucher inventory or granting
    # XP. This is the idempotency boundary: only the caller whose update
    # actually flips completed_field from None -> ts may issue a reward, so a
    # check-in request racing the scheduler (or two scheduler runs overlapping)
    # cannot both pull a code from the pool / grant XP for the same tier.
    claim = _journeys(db_ref).update_one(
        {"user_id": uid, "campaign_id": campaign_id, completed_field: None, "status": {"$ne": "blocked"}},
        {"$set": {completed_field: ts, "updated_at": ts}},
    )
    if not getattr(claim, "modified_count", 0):
        logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s tier=%s reason=race_lost", uid, campaign_id, tier)
        return {"success": True, "code": "noop"}

    tier_cfg = cfg.get(f"tier{tier}", {}) or {}
    reward_type = cfg.get("reward_type", "disabled")
    want_voucher = bool(tier_cfg.get("pool_enabled", True)) and reward_type in ("tiered_vouchers", "xp_plus_tiered_vouchers")
    want_xp = reward_type in ("xp_only", "xp_plus_tiered_vouchers")
    xp_amount = int(tier_cfg.get("xp_amount") or 0)

    voucher_code, voucher_status = (None, "SKIPPED")
    if want_voucher:
        voucher_code, voucher_status = _issue_code(db_ref, uid, tier, journey_id=journey.get("_id"), now_ref=ts)

    xp_granted = 0
    if want_xp and xp_amount > 0 and grant_xp:
        try:
            if grant_xp(db_ref, uid, "reactivation_journey", f"reactivation_journey:tier{tier}:{uid}", xp_amount):
                xp_granted = xp_amount
        except Exception:
            logger.exception("[REACT_JOURNEY][ERROR] uid=%s campaign_id=%s tier=%s reason=xp_grant_failed", uid, campaign_id, tier)

    update_set = {status_field: voucher_status, "updated_at": ts}
    if voucher_code:
        update_set[claimed_field] = ts
        update_set[code_field] = voucher_code
    if xp_granted:
        update_set[f"tier{tier}_xp_granted"] = xp_granted
    if tier == 3:
        update_set["status"] = "completed"
    _journeys(db_ref).update_one({"user_id": uid, "campaign_id": campaign_id}, {"$set": update_set})

    if voucher_code or xp_granted:
        logger.info("[REACT_JOURNEY][T%s_ISSUED] uid=%s campaign_id=%s tier=%s", tier, uid, campaign_id, tier)
        _send_pm(uid, TIER_MESSAGES[tier], send_fn=send_fn)
        return {"success": True, "code": "issued", "voucher_code": voucher_code, "xp_granted": xp_granted}
    if want_voucher:
        logger.warning("[REACT_JOURNEY][OUT_OF_STOCK] uid=%s campaign_id=%s tier=%s reason=completed_pending_stock", uid, campaign_id, tier)
        return {"success": True, "code": "out_of_stock", "voucher_code": None, "xp_granted": 0}
    return {"success": True, "code": "noop", "voucher_code": None, "xp_granted": 0}


def handle_successful_checkin(db_ref, uid: int, *, campaign_id: str = CAMPAIGN_ID, now_ref: datetime | None = None, send_fn=None) -> dict:
    cfg = get_journey_config(db_ref)
    allowed, _reason = is_journey_enabled_for_user(db_ref, int(uid), cfg=cfg, now_ref=now_ref)
    if not allowed:
        return {"success": True, "code": "disabled"}
    journey = _journeys(db_ref).find_one({"user_id": int(uid), "campaign_id": campaign_id, "status": "active"})
    if not journey:
        return {"success": True, "code": "no_journey"}
    if journey.get("tier1_completed_at"):
        return {"success": True, "code": "tier1_already_done"}
    return complete_tier(db_ref, int(uid), 1, campaign_id=campaign_id, now_ref=now_ref, send_fn=send_fn, cfg=cfg)


def _event_created_at(row: dict):
    return row.get("created_at") or row.get("ts") or row.get("createdAt")


def count_unique_checkin_days(db_ref, uid: int, start_utc: datetime, days: int) -> int:
    start = _coerce_utc(start_utc)
    if not start:
        return 0
    end = start + timedelta(days=int(days))
    filt = {
        "user_id": int(uid),
        "created_at": {"$gte": start, "$lt": end},
        "$or": [{"type": "checkin"}, {"source": "checkin"}, {"reason": "checkin"}],
    }
    try:
        rows = _col(db_ref, "xp_events").find(filt, {"created_at": 1, "ts": 1, "createdAt": 1})
    except Exception:
        rows = _col(db_ref, "xp_events").find({"user_id": int(uid)})
    unique_days = set()
    for row in rows:
        created = _coerce_utc(_event_created_at(row))
        if created and start <= created < end:
            if row.get("type") not in (None, "checkin") and row.get("source") not in (None, "checkin") and row.get("reason") not in (None, "checkin"):
                continue
            unique_days.add(created.astimezone(KL_TZ).date().isoformat())
    return len(unique_days)


def evaluate_pending_journeys(
    db_ref,
    *,
    membership_checker: Callable[[int], tuple[bool, str]] | None = None,
    now_ref: datetime | None = None,
    batch_limit: int = 200,
    send_fn=None,
) -> dict:
    ts = _coerce_utc(now_ref) or now_utc()
    stats = {"scanned": 0, "tier2_issued": 0, "tier3_issued": 0, "skipped": 0, "out_of_stock": 0, "blocked": 0}
    cfg = get_journey_config(db_ref)
    if cfg.get("mode") == "disabled" or cfg.get("reward_type") == "disabled" or not _within_campaign_window(cfg, ts):
        logger.info("[REACT_JOURNEY][SCHEDULER_SKIP] reason=mode_%s", cfg.get("mode"))
        return stats
    tier2_cfg = cfg.get("tier2", {}) or {}
    tier3_cfg = cfg.get("tier3", {}) or {}
    tier2_days = int(tier2_cfg.get("threshold_days") or 5)
    tier2_window = int(tier2_cfg.get("window_days") or 7)
    tier3_days = int(tier3_cfg.get("threshold_days") or 20)
    tier3_window = int(tier3_cfg.get("window_days") or 30)
    cursor = _journeys(db_ref).find({"status": "active"}, {"user_id": 1, "campaign_id": 1, "reactivated_at": 1, "tier1_completed_at": 1, "tier2_completed_at": 1, "tier3_completed_at": 1})
    if hasattr(cursor, "limit"):
        cursor = cursor.limit(batch_limit)
    for journey in cursor:
        stats["scanned"] += 1
        uid = int(journey.get("user_id"))
        try:
            if cfg.get("mode") == "test_users_only" and uid not in cfg.get("test_user_ids", set()):
                stats["skipped"] += 1
                continue
            blocked, reason = _is_blocked_user(_user_doc(db_ref, uid))
            if blocked:
                _journeys(db_ref).update_one({"user_id": uid, "campaign_id": journey.get("campaign_id")}, {"$set": {"status": "blocked", "blocked_reason": reason, "updated_at": ts}})
                stats["blocked"] += 1
                continue
            if membership_checker:
                subscribed, sub_reason = membership_checker(uid)
                if not subscribed:
                    logger.info("[REACT_JOURNEY][SKIP] uid=%s campaign_id=%s reason=not_subscribed:%s", uid, journey.get("campaign_id"), sub_reason)
                    stats["skipped"] += 1
                    continue
            reactivated_at = _coerce_utc(journey.get("reactivated_at"))
            if not reactivated_at:
                stats["skipped"] += 1
                continue
            active7 = count_unique_checkin_days(db_ref, uid, reactivated_at, tier2_window)
            active30 = count_unique_checkin_days(db_ref, uid, reactivated_at, tier3_window)
            _journeys(db_ref).update_one({"user_id": uid, "campaign_id": journey.get("campaign_id")}, {"$set": {"active_days_7": active7, "active_days_30": active30, "updated_at": ts}})
            if reactivated_at + timedelta(days=tier2_window) <= ts and journey.get("tier1_completed_at") and not journey.get("tier2_completed_at") and active7 >= tier2_days:
                result = complete_tier(db_ref, uid, 2, campaign_id=journey.get("campaign_id") or CAMPAIGN_ID, now_ref=ts, send_fn=send_fn, cfg=cfg)
                if result.get("code") == "issued":
                    stats["tier2_issued"] += 1
                elif result.get("code") == "out_of_stock":
                    stats["out_of_stock"] += 1
            if reactivated_at + timedelta(days=tier3_window) <= ts and journey.get("tier2_completed_at") and not journey.get("tier3_completed_at") and active30 >= tier3_days:
                result = complete_tier(db_ref, uid, 3, campaign_id=journey.get("campaign_id") or CAMPAIGN_ID, now_ref=ts, send_fn=send_fn, cfg=cfg)
                if result.get("code") == "issued":
                    stats["tier3_issued"] += 1
                elif result.get("code") == "out_of_stock":
                    stats["out_of_stock"] += 1
        except Exception:
            logger.exception("[REACT_JOURNEY][ERROR] uid=%s campaign_id=%s reason=evaluate_failed", uid, journey.get("campaign_id"))
            stats["skipped"] += 1
    return stats


def journey_summary(db_ref) -> dict:
    journeys = _journeys(db_ref)
    pools = _voucher_pools(db_ref)
    pool_rows = []
    out_by_tier = {}
    for tier, pool_id in POOLS.items():
        available = int(pools.count_documents({"pool_id": pool_id, "status": "available"}))
        issued = int(pools.count_documents({"pool_id": pool_id, "status": "issued"}))
        out = int(journeys.count_documents({f"tier{tier}_voucher_status": "OUT_OF_STOCK"}))
        out_by_tier[f"tier{tier}"] = out
        pool_rows.append({"pool_id": pool_id, "tier": tier, "available": available, "issued": issued})
    return {
        "success": True,
        "campaign_id": CAMPAIGN_ID,
        "tier1_completed": int(journeys.count_documents({"tier1_completed_at": {"$ne": None}})),
        "tier1_issued": int(journeys.count_documents({"tier1_voucher_code": {"$nin": [None, ""]}})),
        "tier2_completed": int(journeys.count_documents({"tier2_completed_at": {"$ne": None}})),
        "tier2_issued": int(journeys.count_documents({"tier2_voucher_code": {"$nin": [None, ""]}})),
        "tier3_completed": int(journeys.count_documents({"tier3_completed_at": {"$ne": None}})),
        "tier3_issued": int(journeys.count_documents({"tier3_voucher_code": {"$nin": [None, ""]}})),
        "out_of_stock_by_tier": out_by_tier,
        "pools": pool_rows,
    }


def journey_users(db_ref, *, status: str | None = None, tier: str | None = None, limit: int = 100) -> list[dict]:
    filt = {}
    if status:
        filt["status"] = status
    tier = str(tier or "").strip().lower()
    if tier in {"1", "t1", "tier1"}:
        filt["tier1_completed_at"] = {"$ne": None}
    elif tier in {"2", "t2", "tier2"}:
        filt["tier2_completed_at"] = {"$ne": None}
    elif tier in {"3", "t3", "tier3"}:
        filt["tier3_completed_at"] = {"$ne": None}
    cursor = _journeys(db_ref).find(filt, {"_id": 0}).sort("updated_at", -1).limit(max(1, min(int(limit or 100), 500)))
    return list(cursor)
