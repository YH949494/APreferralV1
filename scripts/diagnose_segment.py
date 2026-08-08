#!/usr/bin/env python3
"""Read-only production segmentation diagnostic for one Telegram user.

Strictly read-only: issues only find/find_one/count_documents/aggregate
calls. Never writes, never calls classify_segment(), never triggers a
sync/scheduler job, never touches voucher/reward/campaign logic.

Usage:
    python3 scripts/diagnose_segment.py <telegram_user_id> [gaming_account]

Traces exactly what Part 1-2 of the segmentation audit proved:
  - System A ("UIM"): users.for_bot_segment / users.bot_segment, synced
    weekly from an external Google Sheet by bot_segment_sync.py. This is
    what vouchers.py / campaign eligibility currently read.
  - System B (shadow): backend_segment_snapshots, written only when an
    admin runs backend_segment_engine.run_shadow_segment_engine(). Not
    read by any production bot-behavior path today.
  - claim_risk_level on `users` is a separate weekly sync
    (claim_risk_sync.py) from the same external sheet; claim_risk_level on
    a backend_segment_snapshots doc is computed by
    backend_segment_engine.classify_claim_risk() from lifetime claim_count.
  - The only claim-count window this codebase computes anywhere is
    lifetime (voucher_claims, no date filter). 7D/30D counts below are
    printed for comparison only — no classifier in this repo consumes them.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

# Ensure the repository root is on sys.path regardless of working directory —
# `python3 scripts/diagnose_segment.py` puts scripts/, not the repo root, on
# sys.path[0], so a plain `import database` fails outside PYTHONPATH tricks.
# Mirrors scripts/backfill_public_pool_claim_state.py's bootstrap.
_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

import database  # noqa: E402 – must come after sys.path fix


def _iso(dt):
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def diagnose(user_id: int, gaming_account: str | None = None) -> None:
    database.init_db()
    users_col = database.users_collection
    claims_col = database.db["voucher_claims"]
    snapshots_col = database.backend_segment_snapshots_col
    marketing_col = database.marketing_raw_data_col

    now = datetime.now(timezone.utc)

    print(f"=== Diagnostic for telegram user_id={user_id} "
          f"gaming_account={gaming_account or '(not provided)'} @ {now.isoformat()} ===\n")

    fallback_fields_used: list[str] = []
    unresolved_mapping_steps: list[str] = []

    # -----------------------------------------------------------------
    # 1. users doc (System A source)
    # -----------------------------------------------------------------
    user = users_col.find_one({"user_id": user_id})
    print("--- users collection ---")
    print("found:", bool(user))
    if not user:
        unresolved_mapping_steps.append(
            "No users document for this user_id — bot_segment_sync.py writes "
            "with upsert=False, so a user_id absent from `users` is silently "
            "skipped by the weekly sheet sync and never gets for_bot_segment set."
        )
    raw_for_bot_segment = (user or {}).get("for_bot_segment")
    raw_bot_segment = (user or {}).get("bot_segment")
    raw_backend_segment_on_user = (user or {}).get("backend_segment")

    if raw_for_bot_segment not in (None, ""):
        segment_source = "users.for_bot_segment"
        segment_raw_value = raw_for_bot_segment
    elif raw_bot_segment not in (None, ""):
        segment_source = "users.bot_segment (legacy alias)"
        segment_raw_value = raw_bot_segment
        fallback_fields_used.append("bot_segment used because for_bot_segment was blank/missing")
    else:
        segment_source = "none (both for_bot_segment and bot_segment blank/missing)"
        segment_raw_value = None

    try:
        from config import normalize_for_bot_segment, is_blank_or_unknown_for_bot_segment
        segment_normalized_value = normalize_for_bot_segment(segment_raw_value)
        is_fallback_label = is_blank_or_unknown_for_bot_segment(segment_raw_value)
    except Exception as exc:
        segment_normalized_value = None
        is_fallback_label = None
        print(f"  (config.normalize_for_bot_segment unavailable: {exc})")

    print("displayed segment (users.for_bot_segment or users.bot_segment):", segment_raw_value)
    print("segment_source:", segment_source)
    print("segment_normalized_value:", segment_normalized_value)
    print("normalize() flagged this as blank/unknown -> unclassified:", is_fallback_label)
    print("users.backend_segment (should be unset in production today — no writer exists):",
          raw_backend_segment_on_user)
    print("users.bot_segment_synced_at (last System A sync write):", _iso((user or {}).get("bot_segment_synced_at")))
    print("users.claim_risk_level (System A, raw pass-through from sheet):", (user or {}).get("claim_risk_level"))
    print("users.claim_risk_reason:", (user or {}).get("claim_risk_reason"))
    print("users.claim_risk_synced_at:", _iso((user or {}).get("claim_risk_synced_at")))
    print("users.player_age_type (if ever written to users — not by any synced writer found in audit):",
          (user or {}).get("player_age_type"))

    # -----------------------------------------------------------------
    # 2. Claims — lifetime / 7D / 30D
    # -----------------------------------------------------------------
    print("\n--- voucher_claims collection ---")
    claims_lifetime = claims_col.count_documents({"user_id": user_id})
    claims_7d = claims_col.count_documents({
        "user_id": user_id,
        "created_at": {"$gte": now - timedelta(days=7)},
    })
    claims_30d = claims_col.count_documents({
        "user_id": user_id,
        "created_at": {"$gte": now - timedelta(days=30)},
    })
    print("claims_lifetime (this is the ONLY window classify_segment()/claim_risk actually use):", claims_lifetime)
    print("claims_7d (printed for comparison only — no classifier reads this):", claims_7d)
    print("claims_30d (printed for comparison only — no classifier reads this):", claims_30d)

    # The real engine's join (backend_segment_engine.py:688-702) matches
    # marketing_raw_data.coupon_code against ALL of a user's voucher_codes,
    # not just the most recent — so this diagnostic queries the complete
    # claim-code set here to reproduce that mapping behavior accurately.
    # (A capped preview is still printed separately for readability.)
    all_claim_docs = list(
        claims_col.find({"user_id": user_id}, {"_id": 0, "voucher_code": 1, "status": 1, "created_at": 1})
        .sort("created_at", -1)
    )
    voucher_codes = [d.get("voucher_code") for d in all_claim_docs if d.get("voucher_code")]
    print(f"total voucher_codes claimed ({len(voucher_codes)}), most recent 10 shown:", voucher_codes[:10])

    # -----------------------------------------------------------------
    # 3. Identity mapping chain (Telegram user_id <-> coupon_code <-> account)
    # -----------------------------------------------------------------
    print("\n--- identity mapping chain (backend_segment_engine.py:688-716 join logic) ---")
    marketing_rows_by_code = []
    if voucher_codes:
        marketing_rows_by_code = list(
            marketing_col.find({"coupon_code": {"$in": voucher_codes}})
        )
    print("marketing_raw_data rows resolvable via voucher_code -> coupon_code:", len(marketing_rows_by_code))
    if not voucher_codes:
        unresolved_mapping_steps.append(
            "No voucher_claims exist for this user_id -> System B's "
            "coupon_code -> voucher_claims.voucher_code -> user_id join has "
            "nothing to anchor on for this user."
        )
    accounts_seen = sorted({str(r.get("account") or r.get("Account") or r.get("ACCOUNT") or "").strip()
                             for r in marketing_rows_by_code if r})
    print("gaming `account` value(s) reachable via this identity chain:", accounts_seen)

    if gaming_account:
        direct_account_rows = list(marketing_col.find({"account": gaming_account}).limit(5))
        print(f"marketing_raw_data rows with account == '{gaming_account}' (direct string match, "
              f"NOT an identity-proven join per backend_segment_engine.py:12-17):", len(direct_account_rows))
        if gaming_account == str(user_id) and gaming_account not in accounts_seen:
            print("  WARNING: gaming_account numerically equals the Telegram user_id, but no code "
                  "path in this repo maps account-string to user_id directly. This is coincidental "
                  "unless proven via the coupon_code chain above.")
        elif gaming_account in accounts_seen:
            print("  CONFIRMED: this account is reachable via the coupon_code -> voucher_claims -> "
                  "user_id chain above (not a coincidental numeric match).")
        else:
            unresolved_mapping_steps.append(
                f"gaming_account={gaming_account!r} provided, but not found among accounts reachable "
                "via this user's own voucher claims -> cannot confirm the join."
            )

    # -----------------------------------------------------------------
    # 4. Latest backend_segment_snapshots doc (System B)
    # -----------------------------------------------------------------
    print("\n--- backend_segment_snapshots collection (System B / shadow engine) ---")
    snapshot = None
    for doc in snapshots_col.find({"telegram_user_id": user_id}).sort("calculated_at", -1).limit(1):
        snapshot = doc
    snapshot_exists = snapshot is not None
    print("snapshot_exists:", snapshot_exists)
    if snapshot:
        print("snapshot_week:", snapshot.get("snapshot_week"))
        print("snapshot_month:", snapshot.get("snapshot_month"))
        print("calculated_at:", _iso(snapshot.get("calculated_at")))
        print("backend_segment:", snapshot.get("backend_segment"))
        print("segment_reason:", snapshot.get("segment_reason"))
        print("confidence:", snapshot.get("confidence"))
        print("player_age_type:", snapshot.get("player_age_type"),
              "(missing is_new_player silently becomes 'old_player' per "
              "classify_player_age_type, backend_segment_engine.py:195-207)")
        print("claim_risk_level (System B, from classify_claim_risk on lifetime claim_count):",
              snapshot.get("claim_risk_level"), "|", snapshot.get("claim_risk_reason"))
        print("classifier_version / rule_version field: NOT PRESENT — "
              "this schema has no version marker; the specific rule set applied "
              "(e.g. 'VH v2 Phase 7D') is only inferable from code comments/git "
              "history at the time of the run, not from stored data.")
        ms = snapshot.get("metrics_snapshot") or {}
        print("metrics_snapshot.after_total_bet_amount (imported PERIOD value, not rolling):",
              ms.get("after_total_bet_amount"))
        print("metrics_snapshot.withdraw_amount (imported PERIOD value, not rolling):",
              ms.get("withdraw_amount"))
        print("metrics_snapshot.claim_count (LIFETIME, embedded into this week's doc):",
              ms.get("claim_count"))
        print("metrics_snapshot.referral_count (users.total_referrals AT CALC TIME, not historical):",
              ms.get("referral_count"))
        print("metrics_snapshot.checkin_count (users.streak AT CALC TIME, not historical):",
              ms.get("checkin_count"))
        turnover_source = "marketing_raw_data (via backend_segment_snapshots.metrics_snapshot)"
        turnover_window_proven = (
            f"period={snapshot.get('snapshot_week')} "
            f"(source={snapshot.get('snapshot_period_source')}) — NOT a 7-day rolling window; "
            "no rolling-window query exists anywhere in this codebase (audit part 2, Q8)."
        )
    else:
        fallback_fields_used.append(
            "No backend_segment_snapshots document exists for this user this period — "
            "any panel doing `doc.get('backend_segment') or \"unclassified\"` cannot "
            "distinguish this from a genuine classifier result of unclassified."
        )
        turnover_source = "none (no snapshot to read from)"
        turnover_window_proven = "N/A — no snapshot exists"

    # Apply the documented fallback for real: users.claim_risk_level (System A)
    # first, else the latest snapshot's claim_risk_level (System B) — matching
    # the claim_risk_source text below, which previously described this
    # fallback without the code actually performing it.
    users_claim_risk = (user or {}).get("claim_risk_level")
    if users_claim_risk not in (None, ""):
        claim_risk_displayed_value = users_claim_risk
        claim_risk_source_used = "users.claim_risk_level (System A, external sheet pass-through)"
    elif snapshot and snapshot.get("claim_risk_level") not in (None, ""):
        claim_risk_displayed_value = snapshot.get("claim_risk_level")
        claim_risk_source_used = (
            "backend_segment_snapshots.claim_risk_level (System B, "
            "classify_claim_risk(lifetime claim_count))"
        )
        fallback_fields_used.append(
            "claim_risk_displayed_value fell back to the backend snapshot because "
            "users.claim_risk_level was blank/missing"
        )
    else:
        claim_risk_displayed_value = None
        claim_risk_source_used = "none (neither users.claim_risk_level nor a snapshot value present)"

    print("\n--- SUMMARY (fields requested for the diagnostic contract) ---")
    summary = {
        "displayed_segment": segment_raw_value,
        "segment_source": segment_source,
        "raw_source_value": segment_raw_value,
        "normalized_value": segment_normalized_value,
        "snapshot_exists": snapshot_exists,
        "snapshot_week": snapshot.get("snapshot_week") if snapshot else None,
        "snapshot_month": snapshot.get("snapshot_month") if snapshot else None,
        "calculated_at": _iso(snapshot.get("calculated_at")) if snapshot else None,
        "segment_reason": snapshot.get("segment_reason") if snapshot else None,
        "classifier_version": None,  # not stored anywhere in this schema — see note above
        "users_for_bot_segment": raw_for_bot_segment,
        "users_bot_segment": raw_bot_segment,
        "users_backend_segment": raw_backend_segment_on_user,
        "backend_segment_snapshots_value": snapshot.get("backend_segment") if snapshot else None,
        "claim_risk_displayed_value": claim_risk_displayed_value,
        "claim_risk_source": claim_risk_source_used,
        "claims_lifetime": claims_lifetime,
        "claims_7d": claims_7d,
        "claims_30d": claims_30d,
        "turnover_raw_value": (snapshot.get("metrics_snapshot") or {}).get("after_total_bet_amount") if snapshot else None,
        "turnover_source_collection": turnover_source,
        "turnover_window_proven": turnover_window_proven,
        "withdrawal_raw_value": (snapshot.get("metrics_snapshot") or {}).get("withdraw_amount") if snapshot else None,
        "identity_mapping_chain": "telegram_user_id -> voucher_claims.user_id -> voucher_claims.voucher_code "
                                   "-> marketing_raw_data.coupon_code -> marketing_raw_data.account",
        "unresolved_mapping_steps": unresolved_mapping_steps,
        "fallback_fields_used": fallback_fields_used,
    }
    for k, v in summary.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(1)
    uid = int(sys.argv[1])
    account = sys.argv[2] if len(sys.argv) > 2 else None
    diagnose(uid, account)
