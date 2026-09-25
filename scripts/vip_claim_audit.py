#!/usr/bin/env python3
"""Read-only VIP1 voucher-claim audit.

Classifies why VIP1 users could not claim tier-gated drops, using production
data only (no writes). Telegram IDs are masked; voucher codes, tokens and
initData are never read or printed.

    fly ssh console -a apreferralv1 -C "python scripts/vip_claim_audit.py --uids-file ids.csv"
    python scripts/vip_claim_audit.py --uids 1329748443,123 --drop <dropId>
    python scripts/vip_claim_audit.py --population          # counts only

Gates checked per user (first failing gate wins, in claim-path order):
  no_user_doc            no users row for the Telegram ID
  drop_alias_vip         drop allow=["VIP"] (Campaign Builder default) — matched nobody pre-fix
  monthly_race_demoted   >=800 ledger XP last month but tier job wrote Normal this month
  vip_tier_status_split  vip_tier=VIP1 for the current month but status!=VIP1
  stale_vip_month        vip_month is not the current KL month (job skipped this user)
  not_vip1               genuinely below VIP1
  region_mismatch        drop audience.regions excludes the user's region
  sub_negative_cache     last getChatMember said left/kicked
  claimed                has a claim for the drop (success)
  cross_drop_collision   claimed another drop within the cooldown/kill window of this drop's open
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz  # noqa: E402

KL = pytz.timezone("Asia/Kuala_Lumpur")
VIP_XP = 800
COOLDOWN_S = int(os.getenv("CLAIM_COOLDOWN_SECONDS", "180"))
KILL_WINDOW_S = int(os.getenv("IP_KILL_WINDOW_SECONDS", "600"))


def mask(uid) -> str:
    s = str(uid)
    return s if len(s) <= 4 else f"{s[:2]}…{s[-3:]}"


def month_bounds(ref_kl: datetime):
    start = ref_kl.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev = (start - timedelta(days=1)).replace(day=1)
    return prev.astimezone(timezone.utc), start.astimezone(timezone.utc), start.strftime("%Y-%m")


def ledger_xp(db, uids, start, end) -> dict:
    rows = db.xp_events.aggregate([
        {"$match": {"user_id": {"$in": list(uids)}, "created_at": {"$gte": start, "$lt": end}, "invalidated": {"$ne": True}}},
        {"$group": {"_id": "$user_id", "xp": {"$sum": "$xp"}}},
    ])
    return {r["_id"]: int(r["xp"] or 0) for r in rows}


def norm_tier(v) -> str:
    return "".join(str(v or "").upper().split()).replace("-", "").replace("_", "")


def load_uids(args) -> list[int]:
    raw = []
    if args.uids:
        raw += args.uids.split(",")
    if args.uids_file:
        with open(args.uids_file, encoding="utf-8-sig") as fh:
            raw += [row[0] for row in csv.reader(fh) if row]
    out = []
    for r in raw:
        try:
            out.append(int(str(r).strip()))
        except ValueError:
            continue
    return out


def tier_drops(db, drop_id, since):
    q = {"eligibility.mode": "tier"}
    if drop_id:
        q = {"_id": drop_id}
    else:
        q["endsAt"] = {"$gte": since}
    return list(db.drops.find(q, {"name": 1, "eligibility": 1, "audience": 1, "startsAt": 1, "endsAt": 1, "status": 1, "type": 1}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uids")
    ap.add_argument("--uids-file")
    ap.add_argument("--drop")
    ap.add_argument("--days", type=int, default=30, help="tier drops ending within N days")
    ap.add_argument("--population", action="store_true")
    args = ap.parse_args()

    from database import init_db, get_db
    init_db()
    db = get_db()

    now_kl = datetime.now(KL)
    prev_start, prev_end, cur_month = month_bounds(now_kl)
    drops = tier_drops(db, args.drop, datetime.now(timezone.utc) - timedelta(days=args.days))

    print(f"# tier drops (last {args.days}d): {len(drops)}   current vip_month={cur_month}")
    for d in drops:
        allow = (d.get("eligibility") or {}).get("allow") or []
        claims = db.voucher_claims.count_documents({"drop_id": d["_id"], "status": "claimed"})
        free = db.vouchers.count_documents({"dropId": {"$in": [d["_id"], str(d["_id"])]}, "status": "free"})
        exact = {str(a).strip().upper() for a in allow}
        alias = "ALIAS_VIP(matched nobody pre-fix)" if "VIP1" not in exact and {"VIP", "VIP1"} & {norm_tier(a) for a in allow} else ""
        print(f"  drop={d['_id']} allow={allow} regions={(d.get('audience') or {}).get('regions') or '-'} "
              f"starts={d.get('startsAt')} claimed={claims} free={free} {alias}")

    if args.population:
        split = db.users.count_documents({"vip_tier": "VIP1", "vip_month": cur_month, "status": {"$ne": "VIP1"}})
        stale = db.users.count_documents({"status": "VIP1", "vip_month": {"$ne": cur_month}})
        normal_uids = [u["user_id"] for u in db.users.find({"status": {"$ne": "VIP1"}, "user_id": {"$ne": None}}, {"user_id": 1})]
        demoted = 0
        for i in range(0, len(normal_uids), 1000):
            demoted += sum(1 for xp in ledger_xp(db, normal_uids[i:i + 1000], prev_start, prev_end).values() if xp >= VIP_XP)
        vip = db.users.count_documents({"status": "VIP1"})
        kill = db.claim_rate_limits.count_documents({"scope": {"$in": ["kill_ip", "kill_subnet"]}, "blockedUntil": {"$gt": datetime.now(timezone.utc)}})
        print(f"# population: status=VIP1 {vip} | ledger>=800 last month but status!=VIP1 {demoted} "
              f"| vip_tier/status split {split} | VIP1 with stale vip_month {stale} | active kill blocks {kill}")
        hist = db.monthly_xp_history.aggregate([
            {"$match": {"month": cur_month}},
            {"$group": {"_id": "$status_after_reset", "n": {"$sum": 1}, "zero_xp": {"$sum": {"$cond": [{"$eq": ["$monthly_xp", 0]}, 1, 0]}}}},
        ])
        print("# monthly_xp_history", cur_month, {h["_id"]: (h["n"], f"zero_xp={h['zero_xp']}") for h in hist})

    uids = load_uids(args)
    if not uids:
        return
    xp = ledger_xp(db, uids, prev_start, prev_end)
    verdicts = Counter()
    print("\nmasked_uid | drop | status | vip_tier | vip_month | prev_month_xp | region | gate | evidence")
    for uid in uids:
        u = db.users.find_one({"user_id": uid}, {"status": 1, "vip_tier": 1, "vip_month": 1, "region": 1}) or None
        sub = db.subscription_cache.find_one({"_id": f"sub:{uid}"}, {"subscribed": 1, "checked_at": 1}) or {}
        for d in drops or [{"_id": "-"}]:
            allow = [str(a).strip().upper() for a in ((d.get("eligibility") or {}).get("allow") or [])]
            regions = (d.get("audience") or {}).get("regions") or []
            claim = db.voucher_claims.find_one({"drop_id": d["_id"], "user_id": uid}, {"status": 1, "claimed_at": 1}) if d["_id"] != "-" else None
            ev = ""
            if u is None:
                gate = "no_user_doc"
            elif claim and claim.get("status") == "claimed":
                gate, ev = "claimed", f"at={claim.get('claimed_at')}"
            elif allow and "VIP1" not in allow and "VIP" in allow:
                gate = "drop_alias_vip"
            elif xp.get(uid, 0) >= VIP_XP and u.get("status") != "VIP1":
                gate, ev = "monthly_race_demoted", f"ledger={xp.get(uid, 0)}"
            elif u.get("vip_tier") == "VIP1" and u.get("vip_month") == cur_month and u.get("status") != "VIP1":
                gate = "vip_tier_status_split"
            elif u.get("vip_month") != cur_month:
                gate = "stale_vip_month"
            elif u.get("status") != "VIP1":
                gate = "not_vip1"
            elif regions and u.get("region") not in regions:
                gate = "region_mismatch"
            elif sub and sub.get("subscribed") is False:
                gate, ev = "sub_negative_cache", f"checked={sub.get('checked_at')}"
            else:
                gate = "unknown_see_logs"
                start = d.get("startsAt")
                if start:
                    lo, hi = start - timedelta(seconds=KILL_WINDOW_S), start + timedelta(seconds=KILL_WINDOW_S)
                    other = db.voucher_claims.find_one(
                        {"user_id": uid, "drop_id": {"$ne": d["_id"]}, "status": "claimed", "claimed_at": {"$gte": lo, "$lte": hi}},
                        {"drop_id": 1, "claimed_at": 1, "claim_subnet": 1},
                    )
                    if other:
                        gate = "cross_drop_collision"
                        ev = f"other_drop={other.get('drop_id')} at={other.get('claimed_at')} subnet={other.get('claim_subnet')}"
            verdicts[gate] += 1
            s = u or {}
            print(f"{mask(uid)} | {d['_id']} | {s.get('status')} | {s.get('vip_tier')} | {s.get('vip_month')} | "
                  f"{xp.get(uid, 0)} | {s.get('region')} | {gate} | {ev}")
    print("\n# verdicts:", dict(verdicts.most_common()))


if __name__ == "__main__":
    main()
