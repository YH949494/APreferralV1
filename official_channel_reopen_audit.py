"""Historical official_channel revocation audit + repair.

Policy change: official_channel referrals no longer require first_checkin
to settle (scheduler.settle_pending_referrals now qualifies them on
retained channel subscription through the hold period alone — see
"official_channel_retained" in scheduler.py). That change is NOT applied
retroactively by the production patch: rows already revoked under the old
policy stay revoked until an operator explicitly runs this script.

This script is entirely separate from the production settlement path. It
is never imported by scheduler.py/main.py and never runs automatically.

Read-only audit (default)
--------------------------
Reports every pending_referrals row matching:
    destination_type = "official_channel"
    status            = "revoked"
    reason            = "insufficient_engagement"

Dry-run repair (still the default without --commit)
----------------------------------------------------
For each audited row, re-checks — fresh, right now — every safeguard that
governs a normal settlement:
  - attribution still valid (inviter_user_id and invitee_user_id present)
  - not a self-referral (invitee_user_id != inviter_user_id)
  - no previous successful settlement exists for the invitee
    (referral_award_events / qualified_events / referral_events)
  - invitee is not abuse-blocked (no referral_audit deny/blocked/abuse
    row in the last 7 days, no active claim_rate_limits cooldown/kill key)
  - invitee is CURRENTLY subscribed to the official channel (a fresh
    getChatMember call — never the subscription_cache, which may be
    stale)
  - invitee was not recorded leaving the channel at any point inside the
    row's *original* hold window (join .. join+hold_hours) — mirrors
    scheduler.py's continuous-subscription check, so a leave-then-rejoin
    that happened during the original hold still blocks reopening even
    though the invitee is subscribed again today
Rows that pass every check are reported as eligible; rows that fail any
check are reported with the failing reason. Nothing is written without
--commit.

Commit repair (--commit)
-------------------------
Only eligible rows are mutated, and only their status — never
deleted/replaced — back to "pending" so the next
settle_pending_referrals() pass re-evaluates and settles them through the
current, real qualification rule (no XP or referral_settled event is
written directly by this script):
    status           = "pending"
    reopened_reason  = "policy_change_remove_checkin_requirement"
    original_status  = "revoked"
    original_reason  = "insufficient_engagement"
    reopened_at      = <now>
    next_retry_at_utc removed so it is immediately eligible for settlement

Safe to re-run: rows already reopened are excluded from a subsequent scan
(they're no longer status="revoked").

Usage:
    python official_channel_reopen_audit.py                 # dry-run report
    python official_channel_reopen_audit.py --json           # dry-run, JSON
    python official_channel_reopen_audit.py --commit          # reopen eligible rows
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _coerce_utc(dt_value):
    if not dt_value:
        return None
    if isinstance(dt_value, datetime):
        return dt_value.astimezone(timezone.utc) if dt_value.tzinfo else dt_value.replace(tzinfo=timezone.utc)
    if isinstance(dt_value, str):
        try:
            parsed = datetime.fromisoformat(dt_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _find_candidate_rows(db) -> list[dict]:
    return list(
        db.pending_referrals.find(
            {
                "destination_type": "official_channel",
                "status": "revoked",
                "revoked_reason": "insufficient_engagement",
            }
        )
    )


def _has_abuse_block(db, invitee_user_id: int, now_ts: datetime) -> bool:
    from datetime import timedelta

    deny_count = db.referral_audit.count_documents(
        {
            "invitee_user_id": invitee_user_id,
            "created_at": {"$gte": now_ts - timedelta(days=7), "$lt": now_ts},
            "reason": {"$in": ["deny", "deny_severe", "blocked", "abuse"]},
        }
    )
    if deny_count > 0:
        return True
    if db.claim_rate_limits.find_one({"key": f"cooldown:uid:{invitee_user_id}"}, {"_id": 1}):
        return True
    if db.claim_rate_limits.find_one({"key": f"kill:uid:{invitee_user_id}"}, {"_id": 1}):
        return True
    return False


def _evaluate_row(db, row: dict, now_ts: datetime) -> dict:
    from referral_historical_success import HistoricalSuccessResult, has_historical_success

    inviter_user_id = row.get("inviter_user_id")
    invitee_user_id = row.get("invitee_user_id")
    result = {
        "pending_id": str(row.get("_id")),
        "inviter_user_id": inviter_user_id,
        "invitee_user_id": invitee_user_id,
        "eligible": False,
        "reason": None,
    }

    if not inviter_user_id or not invitee_user_id:
        result["reason"] = "invalid_attribution"
        return result
    if inviter_user_id == invitee_user_id:
        result["reason"] = "self_invite"
        return result

    history = has_historical_success(db, invitee_user_id=invitee_user_id)
    if history == HistoricalSuccessResult.LOOKUP_FAILED:
        result["reason"] = "historical_success_lookup_failed"
        return result
    if history == HistoricalSuccessResult.FOUND:
        result["reason"] = "already_settled_historically"
        return result

    if _has_abuse_block(db, invitee_user_id, now_ts):
        result["reason"] = "abuse_blocked"
        return result

    import scheduler

    chat_id = row.get("destination_chat_id") or scheduler.OFFICIAL_CHANNEL_ID
    try:
        status = scheduler._get_official_channel_member_status(invitee_user_id, chat_id)
    except Exception as exc:  # noqa: BLE001 — any Telegram/API uncertainty fails closed, not eligible
        result["reason"] = f"membership_check_failed:{exc}"
        return result

    if status not in {"member", "administrator", "creator"}:
        result["reason"] = f"not_currently_subscribed:{status}"
        return result

    # Mirror settle_pending_referrals' continuous-subscription check: a
    # currently-subscribed invitee who nonetheless left at some point
    # *inside their original hold window* was not retained through the
    # hold, and reopening the row would just have the next
    # settle_pending_referrals() pass re-revoke it. Catch that here so the
    # audit report doesn't count it as a real repair candidate.
    reference_time = _coerce_utc(row.get("referral_join_seen_at_utc")) or _coerce_utc(
        row.get("created_at_utc")
    )
    left_at = _coerce_utc(
        (db.users.find_one({"user_id": invitee_user_id}, {"left_official_channel_at": 1}) or {}).get(
            "left_official_channel_at"
        )
    )
    if reference_time is not None and left_at is not None:
        hold_end = reference_time + timedelta(hours=scheduler._referral_hold_hours())
        if reference_time <= left_at <= hold_end:
            result["reason"] = "left_during_hold"
            return result

    result["eligible"] = True
    result["subscription_status"] = status
    result["chat_id"] = chat_id
    return result


def build_report(db, now_ts: datetime | None = None) -> dict:
    now_ts = now_ts or datetime.now(timezone.utc)
    rows = _find_candidate_rows(db)
    evaluations = [_evaluate_row(db, row, now_ts) for row in rows]
    eligible = [e for e in evaluations if e["eligible"]]
    ineligible = [e for e in evaluations if not e["eligible"]]
    return {
        "generated_at_utc": now_ts.isoformat(),
        "candidate_count": len(rows),
        "eligible_count": len(eligible),
        "ineligible_count": len(ineligible),
        "eligible_rows": eligible,
        "ineligible_rows": ineligible,
    }


def _reopen(db, eligible_rows: list[dict], now_ts: datetime) -> dict:
    import referral_invitee_lock
    from bson import ObjectId

    reopened = 0
    lock_blocked_pending_ids = []
    for row in eligible_rows:
        # Reacquire the invitee-scoped lock atomically right before mutating
        # the row: if the invitee has since started a newer active referral
        # (any destination), that referral now owns the lock and this
        # historical row must NOT be reopened alongside it — settlement
        # processes oldest created_at_utc first, so reopening unconditionally
        # would let the stale inviter win the award over the legitimate one.
        claimed = referral_invitee_lock.claim(
            db,
            invitee_user_id=row["invitee_user_id"],
            inviter_user_id=row["inviter_user_id"],
            chat_id=row.get("chat_id"),
            destination_type="official_channel",
            now_utc_ts=now_ts,
        )
        if claimed is not True:
            lock_blocked_pending_ids.append(row["pending_id"])
            continue

        result = db.pending_referrals.update_one(
            {"_id": ObjectId(row["pending_id"]), "status": "revoked"},
            {
                "$set": {
                    "status": "pending",
                    "reopened_reason": "policy_change_remove_checkin_requirement",
                    "original_status": "revoked",
                    "original_reason": "insufficient_engagement",
                    "reopened_at": now_ts,
                },
                "$unset": {
                    "next_retry_at_utc": "",
                    # Clear the live revocation fields, not just leave them
                    # stale — build_public_referral_status() in
                    # referral_rules.py prioritizes revoked_reason over even
                    # a qualified/awarded status, so a reopened-then-settled
                    # referral would otherwise still show as "Not eligible"
                    # to the inviter. History is preserved in original_*.
                    "revoked_reason": "",
                    "qualification_failure_reason": "",
                    "revoked_at": "",
                },
            },
        )
        if getattr(result, "modified_count", 0):
            reopened += 1
        else:
            # Row changed underneath us between eligibility evaluation and
            # this update (e.g. no longer "revoked") — release the lock we
            # just claimed so it doesn't strand a phantom claim.
            referral_invitee_lock.release(
                db,
                invitee_user_id=row["invitee_user_id"],
                status="revoked",
                now_utc_ts=now_ts,
                expected_inviter_user_id=row["inviter_user_id"],
            )
    return {"reopened_count": reopened, "lock_blocked_pending_ids": lock_blocked_pending_ids}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL"), help="Mongo connection URI")
    parser.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "referral_bot"), help="Mongo database name")
    parser.add_argument("--commit", action="store_true", help="Reopen eligible rows (default: dry-run, report only)")
    parser.add_argument("--json", action="store_true", help="Print the report as JSON")
    args = parser.parse_args()

    if not args.mongo_url:
        raise SystemExit("--mongo-url or MONGO_URL is required")

    import database

    database.init_db(args.mongo_url, args.mongo_db)
    db = database.db

    now_ts = datetime.now(timezone.utc)
    report = build_report(db, now_ts)
    report["dry_run"] = not args.commit
    report["reopened_count"] = 0
    report["lock_blocked_pending_ids"] = []

    if args.commit and report["eligible_rows"]:
        reopen_result = _reopen(db, report["eligible_rows"], now_ts)
        report["reopened_count"] = reopen_result["reopened_count"]
        report["lock_blocked_pending_ids"] = reopen_result["lock_blocked_pending_ids"]

    if args.json:
        print(json.dumps(report, default=str, indent=2))
    else:
        print("=== official_channel insufficient_engagement revocation audit ===")
        print(f"generated_at_utc: {report['generated_at_utc']}")
        print(f"dry_run: {report['dry_run']}")
        print(f"candidate_count: {report['candidate_count']}")
        print(f"eligible_count: {report['eligible_count']}")
        print(f"ineligible_count: {report['ineligible_count']}")
        print(f"reopened_count: {report['reopened_count']}")
        if report["lock_blocked_pending_ids"]:
            print(f"lock_blocked_pending_ids: {report['lock_blocked_pending_ids']}")
        for row in report["ineligible_rows"][:20]:
            print(f"  ineligible pending_id={row['pending_id']} reason={row['reason']}")
        for row in report["eligible_rows"][:20]:
            print(f"  eligible pending_id={row['pending_id']} invitee={row['invitee_user_id']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
