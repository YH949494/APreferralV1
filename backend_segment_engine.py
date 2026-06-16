"""Phase 6A — Backend-Owned Segment Engine (shadow mode).

Direction change from Phase 5/5B: instead of the backend only copying UIM's
pre-computed ``for_bot_segment`` (``bot_segment_sync.py``) and claim-risk
fields (``claim_risk_sync.py``) verbatim, the backend now computes its own
segment + claim-risk classification from source data it owns or can ingest.

This module is **shadow mode only**:
  - Writes go to a brand new ``backend_segment_snapshots`` collection, never
    to ``users.for_bot_segment`` / ``users.bot_segment`` / ``users.bot_segment_probability``.
  - ``bot_segment_sync.py``, ``claim_risk_sync.py``, the validation dashboard
    and ``vouchers.py`` probability logic are untouched and keep running
    exactly as before — they remain the production reference/fallback.
  - Nothing here is read by the bot's runtime behaviour (vouchers, rewards,
    public-pool probability). This is a comparison/audit tool only.

Data sources
------------
Marketing raw data (``after_bet_amount``, ``withdrawal_amount``,
``is_new_player``) does not exist anywhere in this backend yet — confirmed
in ``uim_kpi_mapping.py`` (Phase 5B gap report). This module reads it from a
new ``marketing_raw_data`` collection (``database.marketing_raw_data_col``)
keyed by ``user_id``, which some future ingestion job is expected to
populate. Building that ingestion job is out of scope for Phase 6A — until
it exists, every user's marketing fields resolve to ``None`` and the engine
correctly reports ``unclassified`` / ``confidence="low"`` for them (this is
required behaviour per the Phase 6A acceptance criteria, not a bug).

Bot-database fields are read from the existing ``users`` collection and
``voucher_claims`` collection using the closest already-existing field for
each concept (there is no dedicated "checkin_count" or "last_active_at"
field in this schema today, so the best available proxies are used and
called out explicitly in ``_BOT_DB_FIELD_NOTES`` below).

Segment priority
-----------------
The task spec lists High Value / Low Value / Normal Actual / Voucher Hunter
/ Ghost / New Player / Old Player / Active Community Player as if they were
independent outcomes, but the snapshot schema stores a single
``backend_segment`` string. Priority order below is this module's explicit
design choice (not specified by the business) and should be confirmed
before Phase 6B promotes this out of shadow mode:

    high_value > low_value > normal_actual > voucher_hunter > ghost
    > new_player > old_player > active_community_player > unclassified

Financial/behavioural signals (value, activity, claim abuse, inactivity)
outrank the is_new_player attribute and the provisional "active community"
rule, since the latter two are coarser/lower-confidence signals.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from pymongo import UpdateOne
from pymongo.errors import PyMongoError

import config
import database

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

# Segment rule thresholds.
HIGH_VALUE_AFTER_BET_MULTIPLE = 8.0
VOUCHER_HUNTER_CLAIM_THRESHOLD = 3
GHOST_INACTIVITY_DAYS = 30

# "Active Community Player" is explicitly provisional per the task spec —
# configurable via env vars, always stored with confidence="low".
ACTIVE_COMMUNITY_XP_THRESHOLD = int(os.getenv("BACKEND_SEGMENT_ACTIVE_COMMUNITY_XP_THRESHOLD", "5000"))
ACTIVE_COMMUNITY_CHECKIN_THRESHOLD = int(os.getenv("BACKEND_SEGMENT_ACTIVE_COMMUNITY_CHECKIN_THRESHOLD", "14"))

# Claim risk thresholds (claim_count cutoffs).
CLAIM_RISK_MEDIUM_MIN = 10
CLAIM_RISK_HIGH_MIN = 20
CLAIM_RISK_ABUSE_MIN = 50

# Documents which `users`/`voucher_claims` field each bot-database concept
# is read from today, since several concepts (checkin count, last activity,
# channel status, welcome voucher history) have no dedicated field in this
# schema and are approximated from the closest existing one.
_BOT_DB_FIELD_NOTES = {
    "claim_count": "voucher_claims collection, count of all claim docs for user_id",
    "referral_count": "users.total_referrals",
    "checkin_count": "users.checkin_count if present, else users.checkin_streak (current streak, not lifetime count) as best-effort proxy",
    "xp": "users.total_xp",
    "last_active_at": "users.last_active_at if present, else users.last_checkin as best-effort proxy",
    "channel_status": "users.channel_status (not used by any v1 rule; carried through for future rules)",
    "welcome_voucher_history": "users.welcome_voucher_claimed (not used by any v1 rule; carried through for future rules)",
}


def _chunks(items: list, size: int) -> Iterable[list]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _snapshot_month_key(moment: datetime) -> str:
    return f"{moment.year:04d}-{moment.month:02d}"


def _as_float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_new_player_flag(value: Any) -> bool | None:
    """Tri-state: True/False if known, None if marketing didn't supply it."""
    if value is None or value == "":
        return None
    try:
        return int(value) == 1
    except (TypeError, ValueError):
        return str(value).strip().lower() in {"true", "yes", "new"}


def _days_since(moment: Any, now: datetime) -> float | None:
    # Malformed historical values (string/int/etc, same as users.last_checkin
    # elsewhere in this codebase) must not abort the whole snapshot run.
    if not isinstance(moment, datetime):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return (now - moment).total_seconds() / 86400.0


# ---------------------------------------------------------------------------
# Claim risk
# ---------------------------------------------------------------------------


def classify_claim_risk(claim_count: int) -> tuple[str, str]:
    """Return ``(claim_risk_level, claim_risk_reason)`` from a claim count."""
    claim_count = _as_int(claim_count, 0)
    if claim_count >= CLAIM_RISK_ABUSE_MIN:
        level = "abuse_freeze"
    elif claim_count >= CLAIM_RISK_HIGH_MIN:
        level = "high_risk_review"
    elif claim_count >= CLAIM_RISK_MEDIUM_MIN:
        level = "medium_risk"
    else:
        level = "normal"
    return level, f"claim_count={claim_count}"


# ---------------------------------------------------------------------------
# Segment classification
# ---------------------------------------------------------------------------


def classify_segment(metrics: dict, *, now: datetime | None = None) -> dict:
    """Classify one user's backend segment from normalized input metrics.

    ``metrics`` keys (all optional except none — missing keys are treated
    as ``None``/0 per field, see inline handling):
      - after_bet_amount, withdrawal_amount: float|None (Marketing)
      - is_new_player: 0/1/None (Marketing)
      - claim_count, referral_count, checkin_count, xp: int (bot DB)
      - last_active_at: datetime|None (bot DB)

    Returns ``{"segment": str, "segment_reason": str, "confidence": str,
    "source_fields_used": list[str]}``.
    """
    now = now or _utc_now()
    after_bet_amount = _as_float_or_none(metrics.get("after_bet_amount"))
    withdrawal_amount = _as_float_or_none(metrics.get("withdrawal_amount"))
    is_new_player = _is_new_player_flag(metrics.get("is_new_player"))
    claim_count = _as_int(metrics.get("claim_count"), 0)
    referral_count = _as_int(metrics.get("referral_count"), 0)
    checkin_count = _as_int(metrics.get("checkin_count"), 0)
    xp = _as_int(metrics.get("xp"), 0)
    last_active_at = metrics.get("last_active_at")

    marketing_available = after_bet_amount is not None and withdrawal_amount is not None

    if marketing_available:
        if withdrawal_amount > 0:
            ratio = after_bet_amount / withdrawal_amount
            if ratio >= HIGH_VALUE_AFTER_BET_MULTIPLE:
                return {
                    "segment": "high_value",
                    "segment_reason": "after_bet_multiple >= 8x",
                    "confidence": "high",
                    "source_fields_used": ["after_bet_amount", "withdrawal_amount"],
                }
            return {
                "segment": "low_value",
                "segment_reason": "after_bet_multiple < 8x",
                "confidence": "high",
                "source_fields_used": ["after_bet_amount", "withdrawal_amount"],
            }
        if after_bet_amount > 0:
            return {
                "segment": "normal_actual",
                "segment_reason": "has play activity",
                "confidence": "high",
                "source_fields_used": ["after_bet_amount", "withdrawal_amount"],
            }
        # after_bet_amount == 0 and withdrawal_amount == 0.
        if claim_count >= VOUCHER_HUNTER_CLAIM_THRESHOLD:
            return {
                "segment": "voucher_hunter",
                "segment_reason": "repeat claims with no play",
                "confidence": "high",
                "source_fields_used": ["after_bet_amount", "claim_count"],
            }
        days_inactive = _days_since(last_active_at, now)
        if (
            referral_count == 0
            and checkin_count == 0
            and days_inactive is not None
            and days_inactive > GHOST_INACTIVITY_DAYS
        ):
            return {
                "segment": "ghost",
                "segment_reason": "inactive user",
                "confidence": "high",
                "source_fields_used": [
                    "after_bet_amount",
                    "referral_count",
                    "checkin_count",
                    "last_active_at",
                ],
            }

    # Fall through to lower-confidence attribute-based classification.
    if is_new_player is True:
        return {
            "segment": "new_player",
            "segment_reason": "is_new_player=1",
            "confidence": "high" if marketing_available else "low",
            "source_fields_used": ["is_new_player"],
        }
    if is_new_player is False:
        return {
            "segment": "old_player",
            "segment_reason": "is_new_player=0",
            "confidence": "high" if marketing_available else "low",
            "source_fields_used": ["is_new_player"],
        }

    if xp >= ACTIVE_COMMUNITY_XP_THRESHOLD or checkin_count >= ACTIVE_COMMUNITY_CHECKIN_THRESHOLD:
        return {
            "segment": "active_community_player",
            "segment_reason": f"xp={xp} checkin_count={checkin_count} (provisional rule)",
            # Always low confidence per spec — provisional until business sign-off.
            "confidence": "low",
            "source_fields_used": ["xp", "checkin_count"],
        }

    return {
        "segment": "unclassified",
        "segment_reason": "missing marketing data: after_bet_amount/withdrawal_amount"
        if not marketing_available
        else "no play, no claims, no clear inactivity signal",
        "confidence": "low",
        "source_fields_used": [],
    }


# ---------------------------------------------------------------------------
# UIM comparison
# ---------------------------------------------------------------------------


def compare_with_uim(*, backend_segment: str, uim_segment_raw: Any) -> dict:
    """Compare the backend segment against UIM's existing ``for_bot_segment``.

    Both sides are normalized through ``config.normalize_for_bot_segment``
    (backend's ``new_player``/``old_player`` map onto the same canonical
    buckets UIM already uses) so a label-casing/alias difference isn't
    reported as a mismatch. Never writes anything — comparison output only.
    """
    backend_canonical = _backend_segment_to_canonical(backend_segment)
    uim_canonical = config.normalize_for_bot_segment(uim_segment_raw)
    match = backend_canonical == uim_canonical
    return {
        "uim_segment": uim_canonical,
        "backend_segment": backend_canonical,
        "match": match,
    }


_BACKEND_TO_CANONICAL_ALIASES = {
    "new_player": "new_user",
}


def _backend_segment_to_canonical(backend_segment: str) -> str:
    alias = _BACKEND_TO_CANONICAL_ALIASES.get(backend_segment, backend_segment)
    return config.normalize_for_bot_segment(alias)


# ---------------------------------------------------------------------------
# Collecting metrics from bot DB / marketing collections
# ---------------------------------------------------------------------------


def _claim_counts(voucher_claims_col, user_ids: list[int]) -> dict[int, int]:
    counts: dict[int, int] = {}
    if voucher_claims_col is None:
        return counts
    for batch in _chunks(user_ids, BATCH_SIZE):
        cursor = voucher_claims_col.aggregate(
            [
                {"$match": {"user_id": {"$in": batch}}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            ]
        )
        for doc in cursor:
            try:
                counts[int(doc["_id"])] = int(doc.get("count", 0))
            except (TypeError, ValueError, KeyError):
                continue
    return counts


def _marketing_rows(marketing_col, user_ids: list[int]) -> dict[int, dict]:
    rows: dict[int, dict] = {}
    if marketing_col is None:
        return rows
    for batch in _chunks(user_ids, BATCH_SIZE):
        cursor = marketing_col.find({"user_id": {"$in": batch}})
        for doc in cursor:
            try:
                rows[int(doc["user_id"])] = doc
            except (TypeError, ValueError, KeyError):
                continue
    return rows


def _build_metrics_for_user(user_doc: dict, claim_count: int, marketing_row: dict | None) -> dict:
    marketing_row = marketing_row or {}
    return {
        "after_bet_amount": marketing_row.get("after_bet_amount"),
        "withdrawal_amount": marketing_row.get("withdrawal_amount"),
        "is_new_player": marketing_row.get("is_new_player"),
        "claim_count": claim_count,
        "referral_count": user_doc.get("total_referrals", 0),
        "checkin_count": user_doc.get("checkin_count", user_doc.get("checkin_streak", 0)),
        "xp": user_doc.get("total_xp", 0),
        "last_active_at": user_doc.get("last_active_at", user_doc.get("last_checkin")),
    }


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------


def build_snapshot_doc(
    *,
    user_id: int,
    metrics: dict,
    now: datetime,
    uim_segment_raw: Any = None,
) -> dict:
    """Build one ``backend_segment_snapshots`` document (does not write it)."""
    classification = classify_segment(metrics, now=now)
    claim_level, claim_reason = classify_claim_risk(metrics.get("claim_count", 0))
    doc = {
        "user_id": user_id,
        "telegram_user_id": user_id,
        "backend_segment": classification["segment"],
        "claim_risk_level": claim_level,
        "segment_reason": classification["segment_reason"],
        "claim_risk_reason": claim_reason,
        "source_fields_used": classification["source_fields_used"],
        "confidence": classification["confidence"],
        "snapshot_month": _snapshot_month_key(now),
        "calculated_at": now,
    }
    if uim_segment_raw is not None:
        doc["uim_comparison"] = compare_with_uim(
            backend_segment=classification["segment"], uim_segment_raw=uim_segment_raw
        )
    return doc


def _empty_summary(*, dry_run: bool) -> dict:
    return {
        "ok": False,
        "users_evaluated": 0,
        "snapshots_written": 0,
        "segment_distribution": {},
        "claim_risk_distribution": {},
        "uim_matches": 0,
        "uim_mismatches": 0,
        "uim_compared": 0,
        "dry_run": bool(dry_run),
        "error": None,
    }


def run_shadow_segment_engine(
    *,
    users_col=None,
    voucher_claims_col=None,
    marketing_col=None,
    snapshots_col=None,
    user_ids: list[int] | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
) -> dict:
    """Evaluate users and upsert ``backend_segment_snapshots`` (shadow mode).

    Never touches ``users`` or any production segment/voucher/reward field.
    Idempotent on ``(user_id, snapshot_month)`` — re-running within the same
    month replaces the existing snapshot rather than duplicating it.
    """
    now = now or _utc_now()
    summary = _empty_summary(dry_run=dry_run)
    try:
        if users_col is None:
            database.init_db()
            users_col = database.users_collection
        if voucher_claims_col is None:
            # database.py has no dedicated voucher_claims_col (it's only
            # constructed in vouchers.py as db["voucher_claims"]) — resolve
            # the real collection the same way so claim_count isn't silently 0.
            voucher_claims_col = database.db["voucher_claims"]
        if marketing_col is None:
            marketing_col = database.marketing_raw_data_col
        if snapshots_col is None:
            snapshots_col = database.backend_segment_snapshots_col

        query = {"user_id": {"$in": user_ids}} if user_ids is not None else {}
        user_docs = list(users_col.find(query))
        ids = [int(u["user_id"]) for u in user_docs if u.get("user_id") is not None]

        claim_counts = _claim_counts(voucher_claims_col, ids)
        marketing_rows = _marketing_rows(marketing_col, ids)

        docs: list[dict] = []
        for user_doc in user_docs:
            uid = int(user_doc["user_id"])
            metrics = _build_metrics_for_user(user_doc, claim_counts.get(uid, 0), marketing_rows.get(uid))
            uim_raw = user_doc.get("for_bot_segment") or user_doc.get("bot_segment")
            doc = build_snapshot_doc(user_id=uid, metrics=metrics, now=now, uim_segment_raw=uim_raw)
            docs.append(doc)

        summary["users_evaluated"] = len(docs)
        for doc in docs:
            summary["segment_distribution"][doc["backend_segment"]] = (
                summary["segment_distribution"].get(doc["backend_segment"], 0) + 1
            )
            summary["claim_risk_distribution"][doc["claim_risk_level"]] = (
                summary["claim_risk_distribution"].get(doc["claim_risk_level"], 0) + 1
            )
            comparison = doc.get("uim_comparison")
            if comparison is not None:
                summary["uim_compared"] += 1
                if comparison["match"]:
                    summary["uim_matches"] += 1
                else:
                    summary["uim_mismatches"] += 1

        if dry_run or not docs:
            summary["ok"] = True
            logger.info("[BACKEND_SEGMENT_ENGINE] dry_run_summary=%s", {k: v for k, v in summary.items() if k != "error"})
            return summary

        ops = [
            UpdateOne(
                {"user_id": doc["user_id"], "snapshot_month": doc["snapshot_month"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]
        written = 0
        for batch in _chunks(ops, BATCH_SIZE):
            result = snapshots_col.bulk_write(batch, ordered=False)
            written += int(getattr(result, "upserted_count", 0) or 0) + int(getattr(result, "modified_count", 0) or 0)
        summary["snapshots_written"] = written
        summary["ok"] = True
        logger.info("[BACKEND_SEGMENT_ENGINE] commit_summary=%s", {k: v for k, v in summary.items() if k != "error"})
        return summary
    except (RuntimeError, ValueError, PyMongoError) as exc:
        summary["error"] = str(exc)
        logger.error("[BACKEND_SEGMENT_ENGINE] failed err=%s", str(exc))
        return summary


def main() -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Phase 6A backend segment engine (shadow mode)")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate and report only; do not write snapshots")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    summary = run_shadow_segment_engine(dry_run=args.dry_run)
    print(json.dumps(summary, default=str, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
