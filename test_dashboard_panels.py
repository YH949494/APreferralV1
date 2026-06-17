"""Unit tests for the read-only admin dashboard panel builders.

These use lightweight in-memory fake collections so the pure builders in
``dashboard_panels`` can be exercised without MongoDB or importing ``main.py``.
The fakes implement only the query operators the builders actually use.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import dashboard_panels as dp


NOW = datetime(2026, 6, 13, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Minimal fake collection supporting the subset of query operators used.
# ---------------------------------------------------------------------------

class FakeCollection:
    def __init__(self, docs=None):
        self.docs = [dict(d) for d in (docs or [])]

    # --- matching ---
    def _match_value(self, value, cond):
        if isinstance(cond, dict):
            for op, expected in cond.items():
                if op == "$gte" and not (value is not None and value >= expected):
                    return False
                elif op == "$lte" and not (value is not None and value <= expected):
                    return False
                elif op == "$lt" and not (value is not None and value < expected):
                    return False
                elif op == "$gt" and not (value is not None and value > expected):
                    return False
                elif op == "$ne" and value == expected:
                    return False
                elif op == "$in" and value not in expected:
                    return False
                elif op == "$nin" and value in expected:
                    return False
                elif op == "$exists" and (value is not None) != bool(expected):
                    return False
                elif op == "$regex":
                    flags = re.IGNORECASE if "i" in cond.get("$options", "") else 0
                    if value is None or not re.search(expected, str(value), flags):
                        return False
            return True
        return value == cond

    def _match(self, doc, filt):
        for key, cond in (filt or {}).items():
            if key == "$or":
                if not any(self._match(doc, sub) for sub in cond):
                    return False
            elif not self._match_value(doc.get(key), cond):
                return False
        return True

    # --- cursor-ish helpers ---
    def _filtered(self, filt):
        return [d for d in self.docs if self._match(d, filt or {})]

    def count_documents(self, filt):
        return len(self._filtered(filt))

    def distinct(self, field, filt=None):
        return list({d.get(field) for d in self._filtered(filt) if d.get(field) is not None})

    def find_one(self, filt, projection=None):
        res = self._filtered(filt)
        return res[0] if res else None

    def find(self, filt, projection=None):
        return _Cursor(self._filtered(filt))

    def aggregate(self, pipeline):
        docs = self.docs
        out = docs
        groups = None
        for stage in pipeline:
            if "$match" in stage:
                out = [d for d in out if self._match(d, stage["$match"])]
            elif "$group" in stage:
                spec = stage["$group"]
                key = spec["_id"]
                buckets = {}
                for d in out:
                    k = d.get(key[1:]) if isinstance(key, str) and key.startswith("$") else key
                    b = buckets.setdefault(k, {"_id": k})
                    for fld, agg in spec.items():
                        if fld == "_id":
                            continue
                        if "$sum" in agg:
                            val = agg["$sum"]
                            if isinstance(val, dict) and "$cond" in val:
                                cond = val["$cond"]
                                test = cond[0]
                                ok = False
                                if "$in" in test:
                                    field_ref, allowed = test["$in"]
                                    fv = d.get(field_ref[1:])
                                    ok = fv in allowed
                                b[fld] = b.get(fld, 0) + (cond[1] if ok else cond[2])
                            else:
                                b[fld] = b.get(fld, 0) + (val if isinstance(val, (int, float)) else 1)
                        elif "$push" in agg:
                            b.setdefault(fld, []).append(d.get(agg["$push"][1:]))
                    buckets[k] = b
                out = list(buckets.values())
                groups = True
            elif "$sort" in stage:
                for fld, direction in reversed(list(stage["$sort"].items())):
                    out = sorted(out, key=lambda d: (d.get(fld) is None, d.get(fld)), reverse=direction < 0)
            elif "$limit" in stage:
                out = out[: stage["$limit"]]
            elif "$count" in stage:
                out = [{stage["$count"]: len(out)}]
        return iter(out)


class _Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, field, direction=1):
        self.docs = sorted(self.docs, key=lambda d: (d.get(field) is None, d.get(field)), reverse=direction < 0)
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def __iter__(self):
        return iter(self.docs)


# ---------------------------------------------------------------------------
# Vouchers
# ---------------------------------------------------------------------------

def test_vouchers_panel_status_and_counts():
    drops = FakeCollection([
        {"_id": "d1", "name": "Active Drop", "type": "pooled", "status": "active",
         "startsAt": NOW - timedelta(days=1), "endsAt": NOW + timedelta(days=1),
         "public_remaining": 5, "my_remaining": 3},
        {"_id": "d2", "name": "Future Drop", "type": "personalised", "status": "upcoming",
         "startsAt": NOW + timedelta(days=1), "endsAt": NOW + timedelta(days=2)},
        {"_id": "d3", "name": "Past Drop", "type": "pooled", "status": "active",
         "startsAt": NOW - timedelta(days=5), "endsAt": NOW - timedelta(days=1)},
    ])
    vouchers = FakeCollection([
        {"dropId": "d1", "status": "free"}, {"dropId": "d1", "status": "free"},
        {"dropId": "d2", "status": "unclaimed"},
    ])
    claims = FakeCollection([
        {"drop_id": "d1", "status": "claimed", "claimed_at": NOW - timedelta(hours=1), "user_id": 1},
        {"drop_id": "d1", "status": "claimed", "claimed_at": NOW - timedelta(hours=2), "user_id": 1},
        {"drop_id": "d1", "status": "failed", "created_at": NOW - timedelta(hours=1), "error": "sold_out"},
    ])
    welcome = FakeCollection([
        {"uid": 7, "claimed": True, "claimed_at": NOW - timedelta(days=1)},
    ])

    # Use window="all" so all three drops are returned — this test verifies
    # status-label logic, not window filtering (covered by a dedicated test).
    out = dp.build_vouchers_panel(
        drops_col=drops, vouchers_col=vouchers, voucher_claims_col=claims,
        welcome_eligibility_col=welcome, now=NOW, window="all",
    )
    s = out["summary"]
    assert s["active_campaigns"]["value"] == 1   # d1 active; d3 computed expired
    assert s["upcoming_campaigns"]["value"] == 1
    assert s["ended_campaigns"]["value"] == 1
    assert s["failed_claims"]["value"] == 1
    assert s["repeat_claimers"]["value"] == 1   # user 1 claimed twice
    assert s["welcome_claims"]["value"] == 1
    # d1 row detail
    d1 = next(c for c in out["campaigns"] if c["drop_id"] == "d1")
    assert d1["claimed"] == 2
    assert d1["remaining"] == 8  # pooled counters 5+3
    assert d1["detail"]["failure_reasons"] == [{"reason": "sold_out", "count": 1}]


def test_vouchers_panel_window_filters_claim_stats():
    drops = FakeCollection([
        {"_id": "d1", "name": "Active Drop", "type": "personalised", "status": "active",
         "startsAt": NOW - timedelta(days=20), "endsAt": NOW + timedelta(days=1)},
    ])
    vouchers = FakeCollection([
        {"dropId": "d1", "status": "claimed"},
        {"dropId": "d1", "status": "unclaimed"},
        {"dropId": "d1", "status": "unclaimed"},
    ])
    claims = FakeCollection([
        {"drop_id": "d1", "status": "claimed", "claimed_at": NOW - timedelta(days=1), "user_id": 1},
        {"drop_id": "d1", "status": "claimed", "claimed_at": NOW - timedelta(days=20), "user_id": 2},
        {"drop_id": "d1", "status": "failed", "created_at": NOW - timedelta(days=1), "error": "sold_out"},
        {"drop_id": "d1", "status": "failed", "created_at": NOW - timedelta(days=20), "error": "not_eligible"},
    ])
    welcome = FakeCollection([
        {"uid": 7, "claimed": True, "claimed_at": NOW - timedelta(days=1)},
        {"uid": 8, "claimed": True, "claimed_at": NOW - timedelta(days=20)},
    ])

    d7 = dp.build_vouchers_panel(
        drops_col=drops, vouchers_col=vouchers, voucher_claims_col=claims,
        welcome_eligibility_col=welcome, now=NOW, window="7d",
    )
    all_time = dp.build_vouchers_panel(
        drops_col=drops, vouchers_col=vouchers, voucher_claims_col=claims,
        welcome_eligibility_col=welcome, now=NOW, window="all",
    )

    assert d7["window"] == "7d"
    assert d7["summary"]["claimed_codes"]["value"] == 1
    assert d7["summary"]["failed_claims"]["value"] == 1
    assert d7["summary"]["welcome_claims"]["value"] == 1
    assert d7["campaigns"][0]["claimed"] == 1
    assert d7["campaigns"][0]["detail"]["claim_attempts"]["failed"] == 1
    assert d7["campaigns"][0]["detail"]["failure_reasons"] == [{"reason": "sold_out", "count": 1}]

    assert all_time["window"] == "all"
    assert all_time["window_start"] is None
    assert all_time["summary"]["claimed_codes"]["value"] == 2
    assert all_time["summary"]["failed_claims"]["value"] == 2
    assert all_time["summary"]["welcome_claims"]["value"] == 2
    assert all_time["campaigns"][0]["claimed"] == 2
    assert all_time["campaigns"][0]["detail"]["claim_attempts"]["failed"] == 2


def test_vouchers_panel_window_filters_campaign_rows():
    """7d should exclude expired 2025 campaigns; All Time should include them."""
    past_2025 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    past_2025_end = datetime(2025, 12, 31, tzinfo=timezone.utc)
    drops = FakeCollection([
        # Old 2025 campaign — fully before the 7d window
        {"_id": "old", "name": "2025 Campaign", "type": "personalised",
         "startsAt": past_2025, "endsAt": past_2025_end},
        # Campaign overlapping the last 7 days (started before, ends after)
        {"_id": "overlap", "name": "Overlap Campaign", "type": "personalised",
         "startsAt": NOW - timedelta(days=10), "endsAt": NOW + timedelta(days=2)},
        # Campaign entirely within the last 7 days
        {"_id": "recent", "name": "Recent Campaign", "type": "personalised",
         "startsAt": NOW - timedelta(days=3), "endsAt": NOW + timedelta(days=1)},
    ])
    empty = FakeCollection([])

    d7 = dp.build_vouchers_panel(
        drops_col=drops, vouchers_col=empty, voucher_claims_col=empty,
        welcome_eligibility_col=empty, now=NOW, window="7d",
    )
    campaign_ids_7d = {c["drop_id"] for c in d7["campaigns"]}
    assert "old" not in campaign_ids_7d, "7d should exclude expired 2025 campaign"
    assert "overlap" in campaign_ids_7d, "7d should include campaign overlapping the window"
    assert "recent" in campaign_ids_7d, "7d should include campaign within the window"
    # Status counts must reflect the filtered set only
    total_counted = sum(d7["summary"][k]["value"] for k in
                        ("active_campaigns", "upcoming_campaigns", "ended_campaigns", "paused_campaigns"))
    assert total_counted == len(d7["campaigns"])

    all_time = dp.build_vouchers_panel(
        drops_col=drops, vouchers_col=empty, voucher_claims_col=empty,
        welcome_eligibility_col=empty, now=NOW, window="all",
    )
    campaign_ids_all = {c["drop_id"] for c in all_time["campaigns"]}
    assert "old" in campaign_ids_all, "All Time should include historical 2025 campaigns"
    assert len(all_time["campaigns"]) == 3


def test_vouchers_panel_handles_empty():
    empty = FakeCollection([])
    out = dp.build_vouchers_panel(
        drops_col=empty, vouchers_col=empty, voucher_claims_col=empty,
        welcome_eligibility_col=empty, now=NOW,
    )
    assert out["success"] is True
    assert out["campaigns"] == []
    assert out["summary"]["claim_rate_pct"]["data_quality"] == "missing"


# ---------------------------------------------------------------------------
# Referrals
# ---------------------------------------------------------------------------

def test_referrals_panel_aggregates_referrers():
    pending = FakeCollection([
        {"inviter_user_id": 100, "invitee_user_id": 1, "status": "awarded", "created_at_utc": NOW},
        {"inviter_user_id": 100, "invitee_user_id": 2, "status": "pending", "created_at_utc": NOW},
        {"inviter_user_id": 100, "invitee_user_id": 3, "status": "revoked", "created_at_utc": NOW},
        {"inviter_user_id": 200, "invitee_user_id": 4, "status": "awarded", "created_at_utc": NOW},
    ])
    users = FakeCollection([
        {"user_id": 1, "first_checkin_at": NOW - timedelta(days=1), "username": "inv1"},
        {"user_id": 100, "username": "boss"},
    ])
    welcome = FakeCollection([
        {"uid": 1, "user_id": 1, "claimed": True},
    ])
    out = dp.build_referrals_panel(
        pending_referrals_col=pending, qualified_events_col=FakeCollection([]),
        users_col=users, welcome_eligibility_col=welcome, now=NOW,
    )
    s = out["summary"]
    assert s["total_referrers"]["value"] == 2
    assert s["total_invitees"]["value"] == 4
    assert s["qualified_referrals"]["value"] == 2
    assert s["pending_referrals"]["value"] == 1
    assert s["revoked_referrals"]["value"] == 1
    top = next(r for r in out["referrers"] if r["referrer_id"] == 100)
    assert top["invitees"] == 3 and top["qualified"] == 1
    assert top["welcome_claimed"] == 1 and top["checkin_completed"] == 1
    assert top["username"] == "boss"
    # Default window is 7d and is echoed back in the response.
    assert out["window"] == "7d"


def test_referrals_panel_applies_time_window():
    pending = FakeCollection([
        # Recent (within 7d / 30d)
        {"inviter_user_id": 100, "invitee_user_id": 1, "status": "awarded", "created_at_utc": NOW - timedelta(days=2)},
        {"inviter_user_id": 100, "invitee_user_id": 2, "status": "pending", "created_at_utc": NOW - timedelta(days=2)},
        # Old (outside 7d, inside 30d)
        {"inviter_user_id": 200, "invitee_user_id": 3, "status": "awarded", "created_at_utc": NOW - timedelta(days=20)},
        # Ancient (outside everything but "all")
        {"inviter_user_id": 300, "invitee_user_id": 4, "status": "awarded", "created_at_utc": NOW - timedelta(days=200)},
    ])
    users = FakeCollection([])
    welcome = FakeCollection([])

    def _run(window):
        return dp.build_referrals_panel(
            pending_referrals_col=pending, qualified_events_col=FakeCollection([]),
            users_col=users, welcome_eligibility_col=welcome, now=NOW, window=window,
        )

    out7 = _run("7d")
    assert out7["window"] == "7d"
    assert out7["summary"]["total_referrers"]["value"] == 1
    assert out7["summary"]["total_invitees"]["value"] == 2
    assert out7["summary"]["qualified_referrals"]["value"] == 1

    out30 = _run("30d")
    assert out30["summary"]["total_referrers"]["value"] == 2
    assert out30["summary"]["total_invitees"]["value"] == 3

    out_all = _run("all")
    assert out_all["window"] == "all"
    assert out_all["summary"]["total_referrers"]["value"] == 3
    assert out_all["summary"]["total_invitees"]["value"] == 4

    # Unknown windows fall back to the 7d default.
    assert _run("bogus")["window"] == "7d"


def test_referral_detail_lists_invitees():
    pending = FakeCollection([
        {"inviter_user_id": 100, "invitee_user_id": 1, "status": "awarded", "created_at_utc": NOW},
        {"inviter_user_id": 100, "invitee_user_id": 2, "status": "revoked", "created_at_utc": NOW,
         "revoked_reason": "left_channel"},
    ])
    users = FakeCollection([{"user_id": 1, "first_checkin_at": NOW, "username": "a"}])
    welcome = FakeCollection([{"uid": 1, "user_id": 1, "claimed": True, "claimed_at": NOW}])
    out = dp.build_referral_detail(
        referrer_id=100, pending_referrals_col=pending, users_col=users,
        welcome_eligibility_col=welcome, now=NOW,
    )
    assert out["success"] is True
    assert len(out["invitees"]) == 2
    inv1 = next(i for i in out["invitees"] if i["invitee_id"] == 1)
    assert inv1["referral_status"] == "qualified"
    assert inv1["checkin_completed"] is True and inv1["welcome_claimed"] is True
    inv2 = next(i for i in out["invitees"] if i["invitee_id"] == 2)
    assert inv2["referral_status"] == "revoked" and inv2["revoked_reason"] == "left_channel"


# ---------------------------------------------------------------------------
# Affiliate
# ---------------------------------------------------------------------------

def test_affiliate_panel_status_pools_and_table():
    ledger = FakeCollection([
        {"user_id": 1, "ledger_type": "AFFILIATE_MONTHLY", "tier": "T2", "status": "PENDING_REVIEW",
         "qualified_count": 30, "year_month": "202606", "updated_at": NOW},
        {"user_id": 2, "ledger_type": "AFFILIATE_MONTHLY", "tier": "T1", "status": "ISSUED",
         "qualified_count": 12, "year_month": "202606", "updated_at": NOW, "voucher_code": "ABC"},
        {"user_id": 3, "ledger_type": "WELCOME", "tier": "WELCOME", "status": "ISSUED",
         "year_month": None, "updated_at": NOW},
    ])
    pools = FakeCollection([
        {"pool_id": "T1", "status": "available"}, {"pool_id": "T1", "status": "issued"},
        {"pool_id": "T2", "status": "available"},
    ])
    out = dp.build_affiliate_panel(affiliate_ledger_col=ledger, voucher_pools_col=pools, now=NOW)
    assert out["summary"]["pending_review"]["value"] == 1
    assert out["summary"]["issued"]["value"] == 2  # T1 issued + WELCOME issued
    t1 = next(p for p in out["pool_availability"] if p["pool_id"] == "T1")
    assert t1["available"] == 1 and t1["issued"] == 1
    # WELCOME ledger excluded from affiliate table
    assert all(r["user_id"] != 3 for r in out["affiliates"])
    assert {m["status"] for m in out["monthly_issuance"]["by_status"]} == {"PENDING_REVIEW", "ISSUED"}


def test_affiliate_detail_ledger_and_vouchers():
    ledger = FakeCollection([
        {"_id": "x1", "user_id": 1, "ledger_type": "AFFILIATE_MONTHLY", "tier": "T1",
         "status": "ISSUED", "voucher_code": "CODE1", "pool_id": "T1", "updated_at": NOW,
         "risk_flags": ["ip_cluster"]},
    ])
    out = dp.build_affiliate_detail(user_id=1, affiliate_ledger_col=ledger, now=NOW)
    assert out["success"] is True
    assert out["ledger"][0]["risk_flags"] == ["ip_cluster"]
    assert out["vouchers_issued"][0]["voucher_code"] == "CODE1"
    assert out["status_history"][0]["status"] == "ISSUED"


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------

def test_audit_panel_merges_sources():
    logins = FakeCollection([
        {"event": "login_ok", "user_id": 5, "username": "admin", "at": NOW, "ip": "1.2.3.4"},
        {"event": "login_denied", "user_id": 9, "username": "x", "at": NOW - timedelta(minutes=1),
         "reason": "not_admin"},
    ])
    events = FakeCollection([
        {"_id": "monthly_job:last_run", "type": "monthly_tier_update", "month": "202606",
         "run_at_utc": NOW, "total_processed": 10},
    ])
    refaudit = FakeCollection([
        {"ts_utc": NOW, "inviter_user_id": 1, "invitee_user_id": 2, "status": "skipped",
         "reason": "self_invite"},
    ])
    cache = FakeCollection([{"_id": "snapshot_heartbeat", "ts_utc": NOW}])
    out = dp.build_audit_panel(
        admin_login_audit_col=logins, audit_events_col=events,
        referral_audit_col=refaudit, admin_cache_col=cache, now=NOW,
    )
    assert out["summary"]["admin_logins"]["value"] == 1
    assert out["summary"]["auth_events"]["value"] == 2
    assert out["summary"]["scheduler_events"]["value"] == 1
    assert out["summary"]["referral_operations"]["value"] == 1
    assert out["summary"]["voucher_operations"]["data_quality"] == "missing"
    assert out["summary"]["last_scheduler_heartbeat"]["data_quality"] == "delayed"
    # Events are merged and newest-first.
    assert len(out["events"]) == 4
    assert out["events"][0]["time"] >= out["events"][-1]["time"]


# ---------------------------------------------------------------------------
# User drilldown
# ---------------------------------------------------------------------------

def test_user_drilldown_by_id_and_username():
    users = FakeCollection([
        {"user_id": 42, "username": "Neo", "first_name": "Thomas", "status": "VIP1",
         "total_xp": 500, "streak": 7, "first_checkin_at": NOW, "for_bot_segment": "voucher_hunter",
         "total_referrals": 3},
    ])
    welcome = FakeCollection([{"uid": 42, "user_id": 42, "claimed": True, "claimed_at": NOW,
                               "lifecycle_state": "claimed"}])
    claims = FakeCollection([{"user_id": 42, "drop_id": "d1", "status": "claimed",
                              "voucher_code": "V1", "created_at": NOW, "claimed_at": NOW}])
    ledger = FakeCollection([{"user_id": 42, "ledger_type": "AFFILIATE_MONTHLY", "tier": "T1",
                              "status": "ISSUED", "updated_at": NOW, "risk_flags": ["ip_cluster"]}])
    pending = FakeCollection([
        {"inviter_user_id": 42, "invitee_user_id": 1, "status": "awarded"},
        {"inviter_user_id": 42, "invitee_user_id": 2, "status": "pending"},
    ])
    qualified = FakeCollection([])

    kwargs = dict(users_col=users, welcome_eligibility_col=welcome, voucher_claims_col=claims,
                  affiliate_ledger_col=ledger, pending_referrals_col=pending,
                  qualified_events_col=qualified, now=NOW)

    by_id = dp.build_user_drilldown(query="42", **kwargs)
    assert by_id["success"] is True
    assert by_id["profile"]["username"] == "Neo"
    assert by_id["referral_stats"]["referrals_made"] == 2
    assert by_id["referral_stats"]["referrals_qualified"] == 1
    assert by_id["welcome_status"]["claimed"] is True
    assert by_id["voucher_history"][0]["voucher_code"] == "V1"
    assert "segment:voucher_hunter" in by_id["risk_flags"]
    assert "affiliate:ip_cluster" in by_id["risk_flags"]

    by_name = dp.build_user_drilldown(query="@neo", **kwargs)
    assert by_name["success"] is True and by_name["profile"]["user_id"] == 42

    missing = dp.build_user_drilldown(query="99999", **kwargs)
    assert missing["success"] is False and missing["data_quality"] == "missing"


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def test_settings_panel_masks_secrets():
    env = {
        "BOT_TOKEN": "supersecret", "FLASK_SECRET_KEY": "k", "MONGO_URL": "mongodb://x",
        "WELCOME_WINDOW_HOURS": "48", "AFF_T1_THRESHOLD": "10", "BOT_USERNAME": "apbot",
    }
    out = dp.build_settings_panel(env, constants={"XP_BASE_PER_CHECKIN": 20, "GROUP_ID": -100})
    assert out["read_only"] is True
    payload = repr(out)
    # No secret value ever leaks.
    assert "supersecret" not in payload
    assert "mongodb://x" not in payload
    assert out["sections"]["security"]["secrets_configured"]["BOT_TOKEN"] is True
    assert out["sections"]["voucher_settings"]["welcome_window_hours"] == "48"
    assert out["sections"]["bot_settings"]["bot_username"] == "apbot"
    assert out["sections"]["xp_checkin_settings"]["xp_base_per_checkin"] == 20


def test_settings_panel_secret_value_field_is_masked_dict():
    env = {"PUBLIC_POOL_FINGERPRINT_SALT": "salty"}
    out = dp.build_settings_panel(env, constants={})
    salt = out["sections"]["voucher_settings"]["public_pool_fingerprint_salt"]
    assert isinstance(salt, dict) and salt["masked"] is True and salt["configured"] is True
    assert "salty" not in repr(out)


# ---------------------------------------------------------------------------
# Segment overview
# ---------------------------------------------------------------------------

def test_segments_panel_counts_and_top_segments():
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "High Value", "has_ever_claimed_public_pool": True},
        {"user_id": 2, "for_bot_segment": "high_value", "has_ever_claimed_public_pool": True},
        {"user_id": 3, "bot_segment": "voucher_hunter", "has_ever_claimed_public_pool": False},
        {"user_id": 4, "for_bot_segment": "", "bot_segment": None, "has_ever_claimed_public_pool": False},
        {"user_id": 5},
    ])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="snapshot")
    assert out["mode"] == "snapshot"
    assert out["month_start"] is None and out["month_end"] is None
    s = out["summary"]
    assert s["total_users"]["value"] == 5
    assert s["users_without_segment"]["value"] == 2
    assert s["users_with_segment"]["value"] == 3
    assert s["public_pool_claimed"]["value"] == 2
    assert s["public_pool_not_claimed"]["value"] == 3
    top = {row["segment"]: row["count"] for row in out["top_segments"]}
    assert top["high_value"] == 2
    assert top["voucher_hunter"] == 1


def test_segments_panel_segment_filter():
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "new_user"},
        {"user_id": 2, "for_bot_segment": "new_user"},
        {"user_id": 3, "for_bot_segment": "low_value"},
    ])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="snapshot", segment_filter="new_user")
    assert out["segment_filter"] == "new_user"
    assert out["filtered_count"] == 2


def test_segments_panel_never_writes():
    # Builder only ever calls read methods on the collection; no write/update
    # method exists on FakeCollection, so any accidental write call would
    # raise AttributeError and fail this test.
    users = FakeCollection([{"user_id": 1, "for_bot_segment": "new_user"}])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="snapshot")
    assert out["success"] is True


def test_segments_panel_unknown_labels_count_as_missing():
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "Unknown"},
        {"user_id": 2, "for_bot_segment": "N/A"},
        {"user_id": 3, "for_bot_segment": "high_value"},
    ])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="snapshot")
    s = out["summary"]
    assert s["users_without_segment"]["value"] == 2
    assert s["users_with_segment"]["value"] == 1


def test_segments_panel_filter_uses_same_normalizer():
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "High Value"},
        {"user_id": 2, "for_bot_segment": "highvalue"},
        {"user_id": 3, "for_bot_segment": "low_value"},
    ])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="snapshot", segment_filter="High-Value")
    assert out["filtered_count"] == 2


def test_segments_panel_this_month_filters_by_sync_timestamp():
    this_month_ts = datetime(NOW.year, NOW.month, 15, tzinfo=timezone.utc)
    last_month_start, _ = dp._month_bounds(NOW, months_back=1)
    last_month_ts = datetime(last_month_start.year, last_month_start.month, 10, tzinfo=timezone.utc)
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "high_value", "bot_segment_synced_at": this_month_ts},
        {"user_id": 2, "for_bot_segment": "low_value", "bot_segment_synced_at": last_month_ts},
        {"user_id": 3, "for_bot_segment": "high_value"},  # never synced
    ])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="this_month")
    assert out["mode"] == "this_month"
    assert out["month_start"] is not None and out["month_end"] is not None
    s = out["summary"]
    assert s["total_users"]["value"] == 1
    top = {row["segment"]: row["count"] for row in out["top_segments"]}
    assert top == {"high_value": 1}


def test_segments_panel_last_month_filters_by_sync_timestamp():
    this_month_ts = datetime(NOW.year, NOW.month, 15, tzinfo=timezone.utc)
    last_month_start, _ = dp._month_bounds(NOW, months_back=1)
    last_month_ts = datetime(last_month_start.year, last_month_start.month, 10, tzinfo=timezone.utc)
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "high_value", "bot_segment_synced_at": this_month_ts},
        {"user_id": 2, "for_bot_segment": "low_value", "bot_segment_synced_at": last_month_ts},
        {"user_id": 3, "for_bot_segment": "voucher_hunter", "bot_segment_synced_at": last_month_ts},
    ])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="last_month")
    s = out["summary"]
    assert s["total_users"]["value"] == 2
    top = {row["segment"]: row["count"] for row in out["top_segments"]}
    assert top == {"low_value": 1, "voucher_hunter": 1}


def test_segments_panel_unknown_mode_defaults_to_snapshot():
    users = FakeCollection([{"user_id": 1, "for_bot_segment": "high_value"}])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="30d")
    assert out["mode"] == "snapshot"


def test_segments_panel_explicit_month_filters_by_sync_timestamp():
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "high_value", "bot_segment_synced_at": datetime(2026, 3, 5, tzinfo=timezone.utc)},
        {"user_id": 2, "for_bot_segment": "low_value", "bot_segment_synced_at": datetime(2026, 4, 1, tzinfo=timezone.utc)},
        {"user_id": 3, "for_bot_segment": "voucher_hunter", "bot_segment_synced_at": datetime(2026, 3, 31, 23, 59, tzinfo=timezone.utc)},
    ])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="month", month="2026-03")
    assert out["mode"] == "month"
    assert out["selected_month"] == "2026-03"
    assert out["month_start"] == datetime(2026, 3, 1, tzinfo=timezone.utc).isoformat()
    assert out["month_end"] == datetime(2026, 4, 1, tzinfo=timezone.utc).isoformat()
    top = {row["segment"]: row["count"] for row in out["top_segments"]}
    assert top == {"high_value": 1, "voucher_hunter": 1}


def test_segments_panel_invalid_month_defaults_to_snapshot():
    users = FakeCollection([{"user_id": 1, "for_bot_segment": "high_value"}])
    out = dp.build_segments_panel(users_col=users, now=NOW, mode="month", month="not-a-month")
    assert out["mode"] == "snapshot"
    assert out["selected_month"] is None


# ---------------------------------------------------------------------------
# Segment snapshot history (monthly, from segment_snapshots collection)
# ---------------------------------------------------------------------------

def test_monthly_segment_distribution_latest_per_user():
    snapshots = FakeCollection([
        {"user_id": 1, "normalized_segment": "high_value", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W23", "created_at": datetime(2026, 6, 1, tzinfo=timezone.utc)},
        # Same user, later week in same month, segment changed — only the latest should count.
        {"user_id": 1, "normalized_segment": "low_value", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W24", "created_at": datetime(2026, 6, 8, tzinfo=timezone.utc)},
        {"user_id": 2, "normalized_segment": "voucher_hunter", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W23", "created_at": datetime(2026, 6, 1, tzinfo=timezone.utc)},
        # Different month, should be excluded.
        {"user_id": 3, "normalized_segment": "high_value", "snapshot_month": "2026-05",
         "snapshot_week": "2026-W20", "created_at": datetime(2026, 5, 1, tzinfo=timezone.utc)},
    ])
    out = dp.build_monthly_segment_distribution(segment_snapshots_col=snapshots, month="2026-06")
    assert out["has_data"] is True
    assert out["total_users"] == 2
    counts = out["segment_counts"]
    assert counts == {"low_value": 1, "voucher_hunter": 1}


def test_monthly_segment_distribution_no_duplicate_count_across_weekly_snapshots():
    snapshots = FakeCollection([
        {"user_id": 1, "normalized_segment": "high_value", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W23", "created_at": datetime(2026, 6, 1, tzinfo=timezone.utc)},
        {"user_id": 1, "normalized_segment": "high_value", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W24", "created_at": datetime(2026, 6, 8, tzinfo=timezone.utc)},
        {"user_id": 1, "normalized_segment": "high_value", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W25", "created_at": datetime(2026, 6, 15, tzinfo=timezone.utc)},
    ])
    out = dp.build_monthly_segment_distribution(segment_snapshots_col=snapshots, month="2026-06")
    assert out["total_users"] == 1
    assert out["segment_counts"] == {"high_value": 1}


def test_monthly_segment_distribution_empty_state_for_missing_month():
    snapshots = FakeCollection([])
    out = dp.build_monthly_segment_distribution(segment_snapshots_col=snapshots, month="2026-07")
    assert out["has_data"] is False
    assert out["total_users"] == 0
    assert out["top_segments"] == []


def test_segments_panel_snapshot_month_mode_uses_snapshot_collection():
    snapshots = FakeCollection([
        {"user_id": 1, "normalized_segment": "high_value", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W23", "created_at": datetime(2026, 6, 1, tzinfo=timezone.utc)},
        {"user_id": 2, "normalized_segment": "high_value", "snapshot_month": "2026-06",
         "snapshot_week": "2026-W23", "created_at": datetime(2026, 6, 1, tzinfo=timezone.utc)},
    ])
    users = FakeCollection([{"user_id": 1}, {"user_id": 2}])
    out = dp.build_segments_panel(
        users_col=users, now=NOW, mode="snapshot_month", month="2026-06", segment_snapshots_col=snapshots,
    )
    assert out["mode"] == "snapshot_month"
    assert out["selected_month"] == "2026-06"
    assert out["has_data"] is True
    assert out["summary"]["total_users"]["value"] == 2
    top = {row["segment"]: row["count"] for row in out["top_segments"]}
    assert top == {"high_value": 2}


def test_segments_panel_snapshot_month_mode_empty_state_no_data():
    users = FakeCollection([{"user_id": 1}])
    snapshots = FakeCollection([])
    out = dp.build_segments_panel(
        users_col=users, now=NOW, mode="snapshot_month", month="2026-08", segment_snapshots_col=snapshots,
    )
    assert out["mode"] == "snapshot_month"
    assert out["has_data"] is False
    assert out["summary"]["total_users"]["value"] == 0
    assert out["top_segments"] == []


def test_segments_panel_snapshot_month_mode_missing_month_returns_clear_state():
    users = FakeCollection([{"user_id": 1}])
    snapshots = FakeCollection([])
    out = dp.build_segments_panel(
        users_col=users, now=NOW, mode="snapshot_month", month=None, segment_snapshots_col=snapshots,
    )
    assert out["mode"] == "snapshot_month"
    assert out["selected_month"] is None
    assert out["has_data"] is False
    assert out["partial_errors"]


# ---------------------------------------------------------------------------
# Validation panel (Phase 5: UIM vs Backend)
# ---------------------------------------------------------------------------

def test_validation_panel_computes_variance_and_status():
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "high_value"},
        {"user_id": 2, "for_bot_segment": "high_value"},
        {"user_id": 3, "for_bot_segment": "low_value"},
        {"user_id": 4, "for_bot_segment": "voucher_hunter"},
        {"user_id": 5, "for_bot_segment": "new_user"},
    ])
    uim_result = {
        "ok": True,
        "error": None,
        "values": {
            "total_campaign_players": 5,
            "high_value_players": 2,
            "new_player_total": 2,  # backend will compute 1 (only new_user matches) -> red variance
            "total_claims": 40,
        },
        "notes": {"total_claims": "weekly export"},
        "spreadsheet_id": "sheet123",
        "worksheet_title": "dashboard",
    }
    out = dp.build_validation_panel(users_col=users, uim_result=uim_result, now=NOW)
    assert out["success"] is True
    assert out["uim_source"]["worksheet_title"] == "dashboard"
    by_metric = {m["metric"]: m for m in out["metrics"]}

    assert by_metric["total_campaign_players"]["uim_value"] == 5
    assert by_metric["total_campaign_players"]["backend_value"] == 5
    assert by_metric["total_campaign_players"]["status"] == "green"

    assert by_metric["high_value_players"]["backend_value"] == 2
    assert by_metric["high_value_players"]["status"] == "green"

    assert by_metric["new_player_total"]["uim_value"] == 2
    assert by_metric["new_player_total"]["backend_value"] == 1
    assert by_metric["new_player_total"]["difference"] == -1
    assert by_metric["new_player_total"]["status"] == "red"

    # No backend equivalent yet -> gray, never invented.
    assert by_metric["actual_players"]["backend_value"] is None
    assert by_metric["actual_players"]["status"] == "gray"
    assert by_metric["welcome_abuse_invitees"]["status"] == "gray"
    assert by_metric["total_claims"]["uim_note"] == "weekly export"

    assert out["summary"]["total_metrics_compared"] == 14
    assert out["summary"]["missing_metrics"] >= 11

    # Phase 5B: each metric carries a "gap" explanation when documented.
    assert by_metric["total_campaign_players"]["gap"]["implementation_status"] == "definition_mismatch"
    assert by_metric["high_value_players"]["gap"]["implementation_status"] == "definition_mismatch"
    assert by_metric["new_player_total"]["gap"]["implementation_status"] == "definition_mismatch"
    # Metrics with no Phase 5B writeup yet (out of this phase's 7-KPI focus)
    # report gap=None rather than inventing one.
    assert by_metric["welcome_abuse_invitees"]["gap"] is None


def test_validation_panel_status_thresholds():
    users = FakeCollection([])
    cases = [
        (100, 101, "green"),   # 1% diff
        (100, 104, "yellow"),  # 4% diff
        (100, 90, "red"),      # 10% diff
    ]
    for uim_v, backend_v, expected in cases:
        diff, pct, status = dp._validation_compare(uim_v, backend_v)
        assert status == expected, (uim_v, backend_v, status)
    # zero baseline edge cases
    assert dp._validation_compare(0, 0) == (0.0, 0.0, "green")
    diff, pct, status = dp._validation_compare(0, 5)
    assert status == "red"
    assert pct is None


def test_validation_panel_missing_uim_source_returns_gray_not_crash():
    users = FakeCollection([{"user_id": 1, "for_bot_segment": "high_value"}])
    uim_result = {
        "ok": False,
        "error": "missing Google service account credentials",
        "values": {},
        "spreadsheet_id": "sheet123",
        "worksheet_title": "dashboard",
    }
    out = dp.build_validation_panel(users_col=users, uim_result=uim_result, now=NOW)
    assert out["success"] is True
    assert out["uim_source"]["ok"] is False
    assert out["partial_errors"] == ["missing Google service account credentials"]
    for m in out["metrics"]:
        assert m["uim_value"] is None
        assert m["status"] == "gray"
    assert out["summary"]["missing_metrics"] == 14


def test_validation_panel_does_not_crash_on_partial_uim_values():
    users = FakeCollection([
        {"user_id": 1, "for_bot_segment": "high_value"},
        {"user_id": 2, "bot_segment": "low_value"},
    ])
    uim_result = {"ok": True, "error": None, "values": {"total_campaign_players": 2}, "spreadsheet_id": "s", "worksheet_title": "dashboard"}
    out = dp.build_validation_panel(users_col=users, uim_result=uim_result, now=NOW)
    by_metric = {m["metric"]: m for m in out["metrics"]}
    assert by_metric["total_campaign_players"]["status"] == "green"
    # Metrics with no UIM value provided fall back to gray, no crash.
    assert by_metric["high_value_players"]["uim_value"] is None
    assert by_metric["high_value_players"]["status"] == "gray"


# ---------------------------------------------------------------------------
# KPI gap report (Phase 5B: UIM Formula Mapping / Backend KPI Gap Report)
# ---------------------------------------------------------------------------

def test_kpi_gap_report_documents_seven_focus_kpis():
    out = dp.build_kpi_gap_report_panel(now=NOW)
    assert out["success"] is True
    assert out["summary"]["total_kpis_documented"] == 7
    keys = {k["uim_metric_key"] for k in out["kpis"]}
    assert keys == {
        "total_campaign_players",
        "voucher_claimer_accounts",
        "actual_players",
        "high_value_players",
        "new_player_total",
        "old_player_total",
        "claim_risk",
    }


def test_kpi_gap_report_status_counts_match_entries():
    out = dp.build_kpi_gap_report_panel(now=NOW)
    s = out["summary"]
    total = s["exact_available"] + s["backend_missing"] + s["definition_mismatch"] + s["source_missing"]
    assert total == s["total_kpis_documented"]


def test_kpi_gap_report_never_invents_proxy_calculations():
    out = dp.build_kpi_gap_report_panel(now=NOW)
    by_key = {k["uim_metric_key"]: k for k in out["kpis"]}
    # No KPI is marked exact_available -- none of these are actually solved
    # yet, and the report must not pretend otherwise.
    assert all(k["implementation_status"] != "exact_available" for k in out["kpis"])
    assert by_key["total_campaign_players"]["implementation_status"] == "definition_mismatch"
    assert by_key["high_value_players"]["implementation_status"] == "definition_mismatch"
    assert by_key["new_player_total"]["implementation_status"] == "definition_mismatch"


# ---------------------------------------------------------------------------
# P2 regression: snapshot_week filter reaches the panel builder query layer
# ---------------------------------------------------------------------------

def _make_bse_snapshots():
    """Two weeks in the same month with distinct segment distributions."""
    return FakeCollection([
        # Week 24 — high_value
        {"account": "alice", "backend_segment": "high_value",   "snapshot_week": "2026-W24", "snapshot_month": "2026-06",
         "claim_risk_level": "normal", "player_age_type": "old_player", "confidence": "high"},
        # Week 25 — low_value
        {"account": "bob",   "backend_segment": "low_value",    "snapshot_week": "2026-W25", "snapshot_month": "2026-06",
         "claim_risk_level": "normal", "player_age_type": "old_player", "confidence": "high"},
        # Week 25 — ghost
        {"account": "carol", "backend_segment": "ghost",        "snapshot_week": "2026-W25", "snapshot_month": "2026-06",
         "claim_risk_level": "normal", "player_age_type": "new_player", "confidence": "high"},
    ])


def test_bse_panel_snapshot_week_scopes_to_single_week():
    """P2 regression: passing snapshot_week returns only that week's docs."""
    col = _make_bse_snapshots()
    out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W24")
    assert out["summary"]["total_users_evaluated"] == 1
    assert out["summary"]["high_value"] == 1
    assert out["summary"]["low_value"] == 0
    assert out["snapshot_week"] == "2026-W24"
    assert out["snapshot_month"] is None


def test_bse_panel_different_weeks_return_different_data():
    """P2 regression: W24 and W25 must not return the same result set."""
    col = _make_bse_snapshots()
    out_w24 = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W24")
    out_w25 = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
    assert out_w24["summary"]["total_users_evaluated"] == 1
    assert out_w25["summary"]["total_users_evaluated"] == 2
    assert out_w24["summary"]["high_value"] == 1
    assert out_w25["summary"]["low_value"] == 1
    assert out_w25["summary"]["ghost"] == 1


def test_bse_panel_month_fallback_returns_all_weeks_in_month():
    """When no snapshot_week is given, month query covers all weeks."""
    col = _make_bse_snapshots()
    out = dp.build_backend_segment_engine_panel(
        snapshots_col=col, month="2026-06",
        now=datetime(2026, 6, 16, tzinfo=timezone.utc),
    )
    assert out["summary"]["total_users_evaluated"] == 3
    assert out["snapshot_month"] == "2026-06"
    assert out["snapshot_week"] is None


def test_bse_panel_snapshot_week_overrides_month():
    """snapshot_week takes precedence; month is ignored when week is set."""
    col = _make_bse_snapshots()
    out = dp.build_backend_segment_engine_panel(
        snapshots_col=col, snapshot_week="2026-W25", month="2026-06",
        now=datetime(2026, 6, 16, tzinfo=timezone.utc),
    )
    # Only the two W25 docs, not all three
    assert out["summary"]["total_users_evaluated"] == 2


def test_bse_panel_actual_players_kpi():
    """actual_players = high_value + low_value + normal_actual."""
    col = FakeCollection([
        {"account": "u1", "backend_segment": "high_value",    "snapshot_week": "2026-W25",
         "snapshot_month": "2026-06", "claim_risk_level": "normal", "player_age_type": "old_player", "confidence": "high"},
        {"account": "u2", "backend_segment": "low_value",     "snapshot_week": "2026-W25",
         "snapshot_month": "2026-06", "claim_risk_level": "normal", "player_age_type": "old_player", "confidence": "high"},
        {"account": "u3", "backend_segment": "normal_actual", "snapshot_week": "2026-W25",
         "snapshot_month": "2026-06", "claim_risk_level": "normal", "player_age_type": "new_player", "confidence": "high"},
        {"account": "u4", "backend_segment": "ghost",         "snapshot_week": "2026-W25",
         "snapshot_month": "2026-06", "claim_risk_level": "normal", "player_age_type": "old_player", "confidence": "high"},
    ])
    out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
    s = out["summary"]
    assert s["high_value"] == 1
    assert s["low_value"] == 1
    assert s["normal_actual"] == 1
    assert s["actual_players"] == 3  # KPI = HV + LV + NA


def test_bse_panel_player_age_distribution():
    """player_age_distribution must reflect player_age_type field, not segment."""
    col = FakeCollection([
        {"account": "u1", "backend_segment": "ghost", "player_age_type": "new_player",
         "snapshot_week": "2026-W25", "snapshot_month": "2026-06", "claim_risk_level": "normal", "confidence": "high"},
        {"account": "u2", "backend_segment": "ghost", "player_age_type": "old_player",
         "snapshot_week": "2026-W25", "snapshot_month": "2026-06", "claim_risk_level": "normal", "confidence": "high"},
        {"account": "u3", "backend_segment": "high_value", "player_age_type": "old_player",
         "snapshot_week": "2026-W25", "snapshot_month": "2026-06", "claim_risk_level": "normal", "confidence": "high"},
    ])
    out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
    age = out["player_age_distribution"]
    assert age["new_player"] == 1
    assert age["old_player"] == 2
    assert age.get("unknown", 0) == 0


# ---------------------------------------------------------------------------
# Phase 4: Segment Dashboard Refinement
# ---------------------------------------------------------------------------

def _make_p4_snapshots():
    """Three backend snapshots across two weeks with UIM comparison data."""
    return FakeCollection([
        {
            "account": "alice",
            "backend_segment": "high_value",
            "player_age_type": "old_player",
            "snapshot_week": "2026-W24",
            "snapshot_month": "2026-06",
            "claim_risk_level": "normal",
            "confidence": "high",
            "segment_reason": "after_bet_multiple >= 8x",
            "uim_comparison": {"backend_segment": "high_value", "uim_segment": "high_value", "match": True},
            "metrics_snapshot": {
                "after_total_bet_amount": 800.0,
                "withdraw_amount": 100.0,
                "claim_count": 2,
                "referral_count": 1,
                "checkin_count": 5,
            },
        },
        {
            "account": "bob",
            "backend_segment": "low_value",
            "player_age_type": "old_player",
            "snapshot_week": "2026-W25",
            "snapshot_month": "2026-06",
            "claim_risk_level": "normal",
            "confidence": "high",
            "segment_reason": "after_bet_multiple < 8x",
            "uim_comparison": {"backend_segment": "low_value", "uim_segment": "high_value", "match": False},
            "metrics_snapshot": {
                "after_total_bet_amount": 300.0,
                "withdraw_amount": 200.0,
                "claim_count": 5,
                "referral_count": 3,
                "checkin_count": 7,
            },
        },
        {
            "account": "carol",
            "backend_segment": "ghost",
            "player_age_type": "new_player",
            "snapshot_week": "2026-W25",
            "snapshot_month": "2026-06",
            "claim_risk_level": "normal",
            "confidence": "high",
            "segment_reason": "no play, no referrals, no checkins",
            "uim_comparison": {"backend_segment": "ghost", "uim_segment": "normal_actual", "match": False},
            "metrics_snapshot": {
                "after_total_bet_amount": 0.0,
                "withdraw_amount": 0.0,
                "claim_count": 0,
                "referral_count": 0,
                "checkin_count": 0,
            },
        },
    ])


def _make_p4_uim_snapshots():
    """Matching UIM segment_snapshots for the same weeks."""
    return FakeCollection([
        {"user_id": 1, "segment": "high_value", "snapshot_week": "2026-W24", "snapshot_month": "2026-06"},
        {"user_id": 2, "segment": "high_value", "snapshot_week": "2026-W25", "snapshot_month": "2026-06"},
        {"user_id": 3, "segment": "normal_actual", "snapshot_week": "2026-W25", "snapshot_month": "2026-06"},
        {"user_id": 4, "segment": "ghost", "snapshot_week": "2026-W25", "snapshot_month": "2026-06"},
    ])


class TestPhase4WeeklyFilter:
    def test_weekly_filter_scopes_backend_counts_by_week(self):
        """snapshot_week filter produces a single-entry backend_counts_by_week."""
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
        assert out["backend_counts_by_week"] == {"2026-W25": 2}

    def test_weekly_filter_correct_totals(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
        assert out["summary"]["total_users_evaluated"] == 2
        assert out["snapshot_week"] == "2026-W25"
        assert out["snapshot_month"] is None

    def test_weekly_filter_uim_counts_by_week(self):
        col = _make_p4_snapshots()
        uim_col = _make_p4_uim_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, segment_snapshots_col=uim_col, snapshot_week="2026-W25"
        )
        assert out["uim_counts_by_week"] == {"2026-W25": 3}

    def test_weekly_filter_no_uim_col_gives_empty_dict(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
        assert out["uim_counts_by_week"] == {}


class TestPhase4MonthlyFilter:
    def test_monthly_filter_backend_counts_by_week_has_both_weeks(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        byw = out["backend_counts_by_week"]
        assert byw.get("2026-W24") == 1
        assert byw.get("2026-W25") == 2

    def test_monthly_filter_uim_counts_by_week_all_weeks(self):
        col = _make_p4_snapshots()
        uim_col = _make_p4_uim_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, segment_snapshots_col=uim_col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        uyw = out["uim_counts_by_week"]
        assert uyw.get("2026-W24") == 1
        assert uyw.get("2026-W25") == 3

    def test_monthly_filter_snapshot_month_set_week_none(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        assert out["snapshot_month"] == "2026-06"
        assert out["snapshot_week"] is None

    def test_week_overrides_month(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, snapshot_week="2026-W24", month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        assert out["summary"]["total_users_evaluated"] == 1
        assert out["snapshot_week"] == "2026-W24"
        assert out["snapshot_month"] is None


class TestPhase4MatchRate:
    def test_overall_match_rate_top_level(self):
        """match_rate is surfaced at the top level as well as inside summary."""
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        # 3 compared, 1 match → 33.33 %
        assert out["summary"]["uim_compared"] == 3
        assert out["summary"]["uim_matches"] == 1
        assert out["summary"]["uim_mismatches"] == 2
        assert out["match_rate"] == round(100.0 / 3, 2)
        assert out["mismatch_rate"] == round(200.0 / 3, 2)
        assert out["match_rate"] == out["summary"]["match_rate"]

    def test_match_rate_none_when_no_uim_comparison(self):
        col = FakeCollection([
            {"account": "x", "backend_segment": "ghost", "snapshot_week": "2026-W25",
             "snapshot_month": "2026-06", "claim_risk_level": "normal",
             "player_age_type": "old_player", "confidence": "high"},
        ])
        out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
        assert out["match_rate"] is None
        assert out["summary"]["match_rate"] is None

    def test_match_rate_100_when_all_match(self):
        col = FakeCollection([
            {
                "account": "u1", "backend_segment": "high_value", "snapshot_week": "2026-W25",
                "snapshot_month": "2026-06", "claim_risk_level": "normal",
                "player_age_type": "old_player", "confidence": "high",
                "uim_comparison": {"backend_segment": "high_value", "uim_segment": "high_value", "match": True},
            },
        ])
        out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
        assert out["match_rate"] == 100.0
        assert out["mismatch_rate"] == 0.0


class TestPhase4MismatchTable:
    def test_mismatch_by_segment_pair_counts(self):
        """mismatch_by_segment_pair groups mismatched (backend, uim) pairs."""
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        pairs = {(r["backend_segment"], r["uim_segment"]): r["count"] for r in out["mismatch_by_segment_pair"]}
        assert pairs[("low_value", "high_value")] == 1
        assert pairs[("ghost", "normal_actual")] == 1
        assert len(pairs) == 2

    def test_mismatch_details_only_contains_mismatches(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        # alice matched → not in mismatch_details
        accounts = {r["account"] for r in out["mismatch_details"]}
        assert "alice" not in accounts
        assert "bob" in accounts
        assert "carol" in accounts

    def test_mismatch_details_all_required_fields(self):
        """Every row must have all Phase 4 required fields."""
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        required = {
            "account", "backend_segment", "uim_segment", "match",
            "confidence", "reason",
            "after_total_bet_amount", "withdraw_amount",
            "claim_count", "referral_count", "checkin_count",
        }
        for row in out["mismatch_details"]:
            assert required <= row.keys(), f"Missing fields in row: {required - row.keys()}"

    def test_mismatch_details_metric_values_correct(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        by_account = {r["account"]: r for r in out["mismatch_details"]}
        bob = by_account["bob"]
        assert bob["after_total_bet_amount"] == 300.0
        assert bob["withdraw_amount"] == 200.0
        assert bob["claim_count"] == 5
        assert bob["referral_count"] == 3
        assert bob["checkin_count"] == 7
        carol = by_account["carol"]
        assert carol["after_total_bet_amount"] == 0.0
        assert carol["claim_count"] == 0

    def test_mismatch_details_match_field_always_false(self):
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        assert all(r["match"] is False for r in out["mismatch_details"])

    def test_comparison_rows_backwards_compat_includes_matches(self):
        """comparison_rows (legacy field) must still include matched rows."""
        col = _make_p4_snapshots()
        out = dp.build_backend_segment_engine_panel(
            snapshots_col=col, month="2026-06",
            now=datetime(2026, 6, 16, tzinfo=timezone.utc),
        )
        # alice matched — must appear in comparison_rows
        accounts = {r["account"] for r in out["comparison_rows"]}
        assert "alice" in accounts
        assert "bob" in accounts
        assert "carol" in accounts

    def test_mismatch_details_empty_when_no_mismatches(self):
        col = FakeCollection([
            {
                "account": "u1", "backend_segment": "high_value", "snapshot_week": "2026-W25",
                "snapshot_month": "2026-06", "claim_risk_level": "normal",
                "player_age_type": "old_player", "confidence": "high",
                "uim_comparison": {"backend_segment": "high_value", "uim_segment": "high_value", "match": True},
                "metrics_snapshot": {"after_total_bet_amount": 800.0, "withdraw_amount": 100.0,
                                      "claim_count": 2, "referral_count": 1, "checkin_count": 5},
            },
        ])
        out = dp.build_backend_segment_engine_panel(snapshots_col=col, snapshot_week="2026-W25")
        assert out["mismatch_details"] == []
        assert out["mismatch_by_segment_pair"] == []
