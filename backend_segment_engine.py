"""Phase 3 — Backend-Owned Segment Engine (Shadow Mode, Real Data).

Direction change from Phase 6A: the engine now ingests real marketing data
from the ``marketing_raw_data`` collection, resolves Telegram user identity
via coupon code (``marketing_raw_data.coupon_code`` →
``voucher_claims.voucher_code`` → ``voucher_claims.user_id``), looks up the
``users`` document by that ``user_id``, and stores weekly snapshots keyed by
``(account, snapshot_week)``.

Identity resolution rationale
-------------------------------
Marketing uploads use a gaming-platform ``account`` field (not Telegram
username).  The shared key between the two systems is the voucher/coupon code:
the player redeems a code in the gaming platform → that same code appears in
``voucher_claims.voucher_code`` alongside their Telegram ``user_id``.
Using this join raises the identity match rate from 0 % (account→username) to
≈91 % (coupon_code→voucher_claims).

This module is **shadow mode only**:
  - Writes go to ``backend_segment_snapshots`` collection, never to
    ``users.for_bot_segment`` / ``users.bot_segment`` / ``users.bot_segment_probability``.
  - ``bot_segment_sync.py``, ``claim_risk_sync.py``, the validation dashboard
    and ``vouchers.py`` probability logic are untouched and keep running
    exactly as before — they remain the production reference/fallback.
  - Nothing here is read by the bot's runtime behaviour (vouchers, rewards,
    public-pool probability). This is a comparison/audit tool only.

Key changes from Phase 6A
--------------------------
- Identity join: coupon_code → voucher_claims → user_id (replaces account → username).
- ``new_player`` / ``old_player`` are NOT segments — stored as ``player_age_type``
  separately via ``classify_player_age_type()``.
- Ghost rule: ``after_total_bet_amount == 0 AND referral_count == 0 AND
  checkin_count == 0`` — no ``last_active_at`` check.
- Normal Actual rule: ``after_total_bet_amount > 0 AND NOT high_value AND NOT
  low_value`` (covers withdrawal_amount=0 case too).
- ``actual_players`` KPI = high_value + low_value + normal_actual.
- Supports both field-name conventions: ``after_total_bet_amount`` /
  ``withdraw_amount`` (spec) and ``after_bet_amount`` / ``withdrawal_amount``
  (legacy) — whichever is non-null wins.

Segment priority
-----------------
    high_value > low_value > normal_actual > voucher_hunter > ghost
    > active_community_player > unclassified

``player_age_type`` is determined independently:
    "new_player" if is_new_player=1 else "old_player"
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

_USER_PROJECTION = {
    "_id": 0,
    "username": 1,
    "user_id": 1,
    "total_referrals": 1,
    "streak": 1,           # actual check-in streak field (not checkin_count/checkin_streak)
    "total_xp": 1,
    "for_bot_segment": 1,
    "bot_segment": 1,
    "last_active_at": 1,
    "last_checkin": 1,
}

# Segment rule thresholds.
HIGH_VALUE_AFTER_BET_MULTIPLE = 8.0

# VH v2 thresholds (Phase 6B — deployed 2026-06).
# A user is Voucher Hunter when ALL of:
#   claim_count >= VH_V2_CLAIM_THRESHOLD
#   after_total_bet_amount < VH_V2_AFTER_BET_MAX
#   referral_count < VH_V2_REFERRAL_MAX
# Plus protection rules applied in classify_segment():
#   withdraw_amount > 0  →  not VH (falls to high/low value)
#   after_total_bet >= 1000  →  not VH (handled implicitly by < 100 check)
VH_V2_CLAIM_THRESHOLD: int   = 10
VH_V2_AFTER_BET_MAX: float   = 100.0
VH_V2_REFERRAL_MAX: int      = 20

# Legacy constant kept for backward-compat references in tests/tooling.
VOUCHER_HUNTER_CLAIM_THRESHOLD = VH_V2_CLAIM_THRESHOLD

# "Active Community Player" is explicitly provisional per the task spec —
# configurable via env vars, always stored with confidence="low".
ACTIVE_COMMUNITY_XP_THRESHOLD = int(os.getenv("BACKEND_SEGMENT_ACTIVE_COMMUNITY_XP_THRESHOLD", "5000"))
ACTIVE_COMMUNITY_CHECKIN_THRESHOLD = int(os.getenv("BACKEND_SEGMENT_ACTIVE_COMMUNITY_CHECKIN_THRESHOLD", "14"))

# Claim risk thresholds (claim_count cutoffs).
CLAIM_RISK_MEDIUM_MIN = 10
CLAIM_RISK_HIGH_MIN = 20
CLAIM_RISK_ABUSE_MIN = 50

# Documents which `users`/`voucher_claims` field each bot-database concept
# is read from today.
_BOT_DB_FIELD_NOTES = {
    "claim_count": "voucher_claims collection, count of all claim docs for user_id",
    "referral_count": "users.total_referrals",
    "checkin_count": "users.streak (current check-in streak)",
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


def _snapshot_week_key(moment: datetime) -> str:
    """Return ISO week string e.g. '2026-W24'."""
    iso_year, iso_week, _ = moment.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


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


def _days_since(moment: Any, now: datetime) -> float | None:
    # Malformed historical values must not abort the whole snapshot run.
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
# Player age type (independent from segment)
# ---------------------------------------------------------------------------


def classify_player_age_type(is_new_player_value: Any) -> str:
    """Return 'new_player' if is_new_player=1, else 'old_player'.

    This is stored as ``player_age_type`` in the snapshot, separate from
    ``backend_segment``. new_player / old_player are NOT segments in Phase 3.
    """
    if is_new_player_value is None or is_new_player_value == "":
        return "old_player"
    try:
        return "new_player" if int(is_new_player_value) == 1 else "old_player"
    except (TypeError, ValueError):
        val = str(is_new_player_value).strip().lower()
        return "new_player" if val in {"true", "yes", "new"} else "old_player"


# ---------------------------------------------------------------------------
# Segment classification
# ---------------------------------------------------------------------------


def classify_segment(metrics: dict, *, now: datetime | None = None) -> dict:
    """Classify one user's backend segment from normalized input metrics.

    Supports both field-name conventions:
      - after_total_bet_amount / withdraw_amount  (Phase 3 spec)
      - after_bet_amount / withdrawal_amount       (legacy / compatibility)

    ``metrics`` keys (all optional — missing keys are treated as None/0):
      - after_total_bet_amount (or after_bet_amount): float|None (Marketing)
      - withdraw_amount (or withdrawal_amount): float|None (Marketing)
      - is_new_player: 0/1/None — used only for player_age_type, not segment
      - claim_count, referral_count, checkin_count, xp: int (bot DB)

    Priority order (Phase 6B VH v2):
      high_value > low_value > voucher_hunter > normal_actual > ghost
      > active_community_player > unclassified

    VH v2 rule (Phase 6B): claim_count >= 10 AND after_bet < 100 AND
    referral_count < 20. Withdraw > 0 triggers high/low value first so the
    withdrawal protection is implicit. after_bet >= 1000 fails the < 100
    check so the high-bet protection is also implicit.

    Ghost rule (Phase 3): after_total_bet == 0 AND referral_count == 0 AND
    checkin_count == 0  (NO last_active_at check).

    Returns ``{"segment": str, "segment_reason": str, "confidence": str,
    "source_fields_used": list[str]}``.
    """
    now = now or _utc_now()

    # Support both naming conventions — prefer Phase 3 spec names.
    after_total_bet = _as_float_or_none(
        metrics.get("after_total_bet_amount") if metrics.get("after_total_bet_amount") is not None
        else metrics.get("after_bet_amount")
    )
    withdraw = _as_float_or_none(
        metrics.get("withdraw_amount") if metrics.get("withdraw_amount") is not None
        else metrics.get("withdrawal_amount")
    )

    claim_count = _as_int(metrics.get("claim_count"), 0)
    referral_count = _as_int(metrics.get("referral_count"), 0)
    checkin_count = _as_int(metrics.get("checkin_count"), 0)
    xp = _as_int(metrics.get("xp"), 0)

    marketing_available = after_total_bet is not None and withdraw is not None

    if marketing_available:
        # 1. High Value / 2. Low Value — withdrawal users always land here first.
        #    Implicit withdrawal protection for VH: withdraw > 0 never reaches VH block.
        if withdraw > 0:
            ratio = after_total_bet / withdraw
            if ratio >= HIGH_VALUE_AFTER_BET_MULTIPLE:
                return {
                    "segment": "high_value",
                    "segment_reason": "after_bet_multiple >= 8x",
                    "confidence": "high",
                    "source_fields_used": ["after_total_bet_amount", "withdraw_amount"],
                }
            return {
                "segment": "low_value",
                "segment_reason": "after_bet_multiple < 8x",
                "confidence": "high",
                "source_fields_used": ["after_total_bet_amount", "withdraw_amount"],
            }

        # withdraw == 0 from here.

        # 3. Voucher Hunter v2 (Phase 6B) — evaluated before normal_actual so
        #    low-bet claimers are captured even when after_bet > 0.
        #    after_bet < 100 implicitly handles the high-bet protection
        #    (after_bet >= 1000 fails this check and falls to normal_actual).
        if (
            claim_count >= VH_V2_CLAIM_THRESHOLD
            and after_total_bet < VH_V2_AFTER_BET_MAX
            and referral_count < VH_V2_REFERRAL_MAX
        ):
            return {
                "segment": "voucher_hunter",
                "segment_reason": (
                    f"claim_count={claim_count} >= {VH_V2_CLAIM_THRESHOLD}"
                    f", after_bet={after_total_bet} < {VH_V2_AFTER_BET_MAX}"
                    f", referral_count={referral_count} < {VH_V2_REFERRAL_MAX}"
                ),
                "confidence": "high",
                "source_fields_used": [
                    "after_total_bet_amount",
                    "claim_count",
                    "referral_count",
                ],
            }

        # 5. Normal Actual — played (withdraw == 0, after_bet > 0, didn't qualify VH).
        if after_total_bet > 0:
            return {
                "segment": "normal_actual",
                "segment_reason": "has play activity",
                "confidence": "high",
                "source_fields_used": ["after_total_bet_amount", "withdraw_amount"],
            }

        # 6. Ghost — after_bet == 0, withdraw == 0, not VH, no community signals.
        if referral_count == 0 and checkin_count == 0:
            return {
                "segment": "ghost",
                "segment_reason": "inactive user",
                "confidence": "high",
                "source_fields_used": [
                    "after_total_bet_amount",
                    "referral_count",
                    "checkin_count",
                ],
            }

    # 4. Active Community — attribute-based fallthrough (no marketing data or
    #    has community activity but no qualifying play/claim signal above).
    if xp >= ACTIVE_COMMUNITY_XP_THRESHOLD or checkin_count >= ACTIVE_COMMUNITY_CHECKIN_THRESHOLD:
        return {
            "segment": "active_community_player",
            "segment_reason": f"xp={xp} checkin_count={checkin_count} (provisional rule)",
            # Always low confidence per spec — provisional until business sign-off.
            "confidence": "low",
            "source_fields_used": ["xp", "checkin_count"],
        }

    # 7. Unclassified.
    return {
        "segment": "unclassified",
        "segment_reason": "missing marketing data: after_total_bet_amount/withdraw_amount"
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
    so a label-casing/alias difference isn't reported as a mismatch.
    Never writes anything — comparison output only.
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


def _doc_account(doc: dict) -> str:
    """Extract the account value from a marketing doc case-insensitively.

    marketing_upload._normalize_header does NOT lowercase, so the stored field
    name reflects the original CSV/XLSX header casing: 'account', 'Account',
    or 'ACCOUNT' are all valid. Try each common casing before giving up.
    """
    for key in ("account", "Account", "ACCOUNT"):
        val = doc.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    # Fallback: scan all keys case-insensitively (handles any other casing).
    for k, v in doc.items():
        if k.lower() == "account" and v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _doc_coupon(doc: dict) -> str | None:
    """Extract coupon_code from a marketing row, tolerating mixed-case headers."""
    for key in ("coupon_code", "Coupon_Code", "COUPON_CODE"):
        v = doc.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    for k, v in doc.items():
        if k.lower().replace(" ", "_") == "coupon_code" and v is not None and str(v).strip():
            return str(v).strip()
    return None


def _marketing_rows_by_account(marketing_col, snapshot_week: str) -> dict[str, dict]:
    """Return dict[account_lower -> doc] for the given snapshot_week.

    Deduplicates by account (first occurrence wins). Lookups are
    case-insensitive to match whatever casing the CSV upload used.
    """
    rows: dict[str, dict] = {}
    if marketing_col is None:
        return rows
    cursor = marketing_col.find({"snapshot_week": snapshot_week})
    for doc in cursor:
        acct = _doc_account(doc).lower()
        if acct and acct not in rows:
            rows[acct] = doc
    return rows


def _build_metrics_for_user(user_doc: dict, claim_count: int, marketing_row: dict | None) -> dict:
    marketing_row = marketing_row or {}

    def _first_not_none(*keys):
        """Return the value of the first key that is not None/missing."""
        for k in keys:
            v = marketing_row.get(k)
            if v is not None:
                return v
        return None

    return {
        # Phase 3 spec field names (primary), legacy aliases as fallback.
        "after_total_bet_amount": _first_not_none("after_total_bet_amount", "after_bet_amount"),
        "withdraw_amount": _first_not_none("withdraw_amount", "withdrawal_amount"),
        "is_new_player": marketing_row.get("is_new_player"),
        "claim_count": claim_count,
        "referral_count": user_doc.get("total_referrals", 0),
        "checkin_count": user_doc.get("streak", 0),
        "xp": user_doc.get("total_xp", 0),
        "last_active_at": user_doc.get("last_active_at", user_doc.get("last_checkin")),
    }


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------


def build_snapshot_doc(
    *,
    account: str,
    username: str | None = None,
    user_id: int | None,
    telegram_user_id: int | None,
    metrics: dict,
    now: datetime,
    snapshot_week: str,
    uim_segment_raw: Any = None,
) -> dict:
    """Build one ``backend_segment_snapshots`` document (does not write it)."""
    classification = classify_segment(metrics, now=now)
    claim_level, claim_reason = classify_claim_risk(metrics.get("claim_count", 0))
    player_age_type = classify_player_age_type(metrics.get("is_new_player"))

    doc: dict = {
        "account": account,
        "username": username,
        "user_id": user_id,
        "telegram_user_id": telegram_user_id,
        "backend_segment": classification["segment"],
        "player_age_type": player_age_type,
        "claim_risk_level": claim_level,
        "segment_reason": classification["segment_reason"],
        "claim_risk_reason": claim_reason,
        "confidence": classification["confidence"],
        "snapshot_week": snapshot_week,
        "snapshot_month": _snapshot_month_key(now),
        "calculated_at": now,
        # Phase 4: raw input metrics stored for dashboard mismatch detail table.
        # Never used for re-classification; shadow mode only.
        "metrics_snapshot": {
            "after_total_bet_amount": metrics.get("after_total_bet_amount"),
            "withdraw_amount": metrics.get("withdraw_amount"),
            "claim_count": _as_int(metrics.get("claim_count"), 0),
            "referral_count": _as_int(metrics.get("referral_count"), 0),
            "checkin_count": _as_int(metrics.get("checkin_count"), 0),
        },
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
        "total_rows": 0,
        "rows_processed": 0,
        "matched_rows": 0,
        "unmatched_rows": 0,
        "identity_match_rate": 0.0,
        "elapsed_seconds": 0.0,
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
    snapshot_week: str | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
    progress_cb=None,
) -> dict:
    """Evaluate users and upsert ``backend_segment_snapshots`` (shadow mode).

    Identity resolution (Method B):
        marketing_raw_data.coupon_code
            → voucher_claims.voucher_code → user_id
            → users document (by user_id)

    For rows without a coupon_code or whose code has no matching claim,
    ``user_id`` / ``telegram_user_id`` are stored as None and bot-DB fields
    default to 0.  The snapshot is still written for every marketing row so
    the full account list is preserved.

    Idempotent on ``(account, snapshot_week)`` — re-running replaces the
    existing snapshot rather than duplicating it.

    Never touches ``users`` or any production segment/voucher/reward field.

    ``progress_cb(rows_done, total_rows)`` is called after each bulk_write
    batch (and once at 0 after marketing data loads) so callers can track
    real-time progress without polling the engine internals.
    """
    now = now or _utc_now()
    started_at = _utc_now()
    if snapshot_week is None:
        snapshot_week = _snapshot_week_key(now)

    summary = _empty_summary(dry_run=dry_run)

    def _call_progress(rows_done: int, total: int) -> None:
        if progress_cb is not None:
            try:
                progress_cb(rows_done, total)
            except Exception:
                pass

    try:
        if users_col is None:
            database.init_db()
            users_col = database.users_collection
        if voucher_claims_col is None:
            voucher_claims_col = database.db["voucher_claims"]
        if marketing_col is None:
            marketing_col = database.marketing_raw_data_col
        if snapshots_col is None:
            snapshots_col = database.backend_segment_snapshots_col

        # Step 1: Load all marketing rows for the week, keyed by account.
        marketing_rows = _marketing_rows_by_account(marketing_col, snapshot_week)

        if not marketing_rows:
            summary["ok"] = True
            summary["elapsed_seconds"] = (_utc_now() - started_at).total_seconds()
            logger.info(
                "[BACKEND_SEGMENT_ENGINE] no_marketing_data snapshot_week=%s", snapshot_week
            )
            return summary

        total_rows = len(marketing_rows)
        summary["total_rows"] = total_rows
        _call_progress(0, total_rows)

        # Step 2: Resolve identity via coupon_code → voucher_claims.voucher_code.
        # Extract all coupon codes present in this week's marketing rows, then
        # batch-query voucher_claims to get the Telegram user_id for each code.
        all_coupons: list[str] = [
            c for c in (_doc_coupon(row) for row in marketing_rows.values()) if c
        ]
        coupon_to_uid: dict[str, int] = {}
        for batch in _chunks(all_coupons, BATCH_SIZE):
            for claim in voucher_claims_col.find(
                {"voucher_code": {"$in": batch}, "user_id": {"$ne": None}},
                {"_id": 0, "voucher_code": 1, "user_id": 1},
            ):
                code = claim.get("voucher_code")
                uid = claim.get("user_id")
                if code and uid is not None:
                    try:
                        coupon_to_uid[code] = int(uid)
                    except (TypeError, ValueError):
                        pass

        # Step 3: Batch-query users by the resolved user_ids.
        resolved_user_ids: list[int] = list(set(coupon_to_uid.values()))
        users_by_uid: dict[int, dict] = {}
        for batch in _chunks(resolved_user_ids, BATCH_SIZE):
            for u in users_col.find({"user_id": {"$in": batch}}, _USER_PROJECTION):
                try:
                    uid_key = int(u["user_id"])
                    users_by_uid[uid_key] = u
                except (TypeError, ValueError, KeyError):
                    pass

        # claim_counts: all historical claims per resolved user_id.
        claim_counts = _claim_counts(voucher_claims_col, resolved_user_ids)

        # Step 4: Build snapshot docs (pure in-memory, no extra DB calls).
        matched_rows = 0
        unmatched_rows = 0
        docs: list[dict] = []
        for acct_lower, mrow in marketing_rows.items():
            account = _doc_account(mrow) or acct_lower
            coupon = _doc_coupon(mrow)
            user_id: int | None = coupon_to_uid.get(coupon) if coupon else None
            user_doc: dict = users_by_uid.get(user_id) or {} if user_id is not None else {}

            telegram_user_id: int | None = user_id
            username: str | None = (user_doc.get("username") or "").strip() or None

            if user_id is not None:
                matched_rows += 1
            else:
                unmatched_rows += 1

            claim_count = claim_counts.get(user_id, 0) if user_id is not None else 0
            metrics = _build_metrics_for_user(user_doc, claim_count, mrow)
            uim_raw = user_doc.get("for_bot_segment") or user_doc.get("bot_segment")

            doc = build_snapshot_doc(
                account=account,
                username=username,
                user_id=user_id,
                telegram_user_id=telegram_user_id,
                metrics=metrics,
                now=now,
                snapshot_week=snapshot_week,
                uim_segment_raw=uim_raw,
            )
            docs.append(doc)

        summary["matched_rows"] = matched_rows
        summary["unmatched_rows"] = unmatched_rows
        summary["identity_match_rate"] = (
            round(matched_rows / total_rows * 100, 2) if total_rows else 0.0
        )

        summary["users_evaluated"] = len(docs)
        for doc in docs:
            seg = doc.get("backend_segment", "unclassified")
            summary["segment_distribution"][seg] = summary["segment_distribution"].get(seg, 0) + 1
            risk = doc.get("claim_risk_level", "normal")
            summary["claim_risk_distribution"][risk] = summary["claim_risk_distribution"].get(risk, 0) + 1
            comparison = doc.get("uim_comparison")
            if comparison is not None:
                summary["uim_compared"] += 1
                if comparison["match"]:
                    summary["uim_matches"] += 1
                else:
                    summary["uim_mismatches"] += 1

        if dry_run or not docs:
            summary["ok"] = True
            summary["rows_processed"] = total_rows
            summary["elapsed_seconds"] = (_utc_now() - started_at).total_seconds()
            _call_progress(total_rows, total_rows)
            logger.info(
                "[BACKEND_SEGMENT_ENGINE] dry_run_summary=%s",
                {k: v for k, v in summary.items() if k != "error"},
            )
            return summary

        # Step 5: Bulk-write in batches; report progress after each batch.
        ops = [
            UpdateOne(
                {"account": doc["account"], "snapshot_week": doc["snapshot_week"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]
        written = 0
        for batch in _chunks(ops, BATCH_SIZE):
            result = snapshots_col.bulk_write(batch, ordered=False)
            written += int(getattr(result, "upserted_count", 0) or 0) + int(
                getattr(result, "modified_count", 0) or 0
            )
            _call_progress(written, total_rows)

        summary["snapshots_written"] = written
        summary["rows_processed"] = total_rows
        summary["elapsed_seconds"] = (_utc_now() - started_at).total_seconds()
        summary["ok"] = True
        logger.info(
            "[BACKEND_SEGMENT_ENGINE] commit_summary=%s",
            {k: v for k, v in summary.items() if k != "error"},
        )
        return summary
    except Exception as exc:
        summary["error"] = str(exc)
        summary["elapsed_seconds"] = (_utc_now() - started_at).total_seconds()
        logger.error("[BACKEND_SEGMENT_ENGINE] failed err=%s", str(exc))
        return summary


def main() -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Phase 3 backend segment engine (shadow mode)")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate and report only; do not write snapshots")
    parser.add_argument("--snapshot-week", default=None, help="ISO week key e.g. 2026-W24 (default: current week)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    summary = run_shadow_segment_engine(dry_run=args.dry_run, snapshot_week=args.snapshot_week)
    print(json.dumps(summary, default=str, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
