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

    out = dp.build_vouchers_panel(
        drops_col=drops, vouchers_col=vouchers, voucher_claims_col=claims,
        welcome_eligibility_col=welcome, now=NOW,
    )
    s = out["summary"]
    assert s["active_campaigns"]["value"] == 1   # d1 active; d3 computed expired
    assert s["upcoming_campaigns"]["value"] == 1
    assert s["ended_campaigns"]["value"] == 1
    assert s["failed_claims_7d"]["value"] == 1
    assert s["repeat_claimers_7d"]["value"] == 1   # user 1 claimed twice
    assert s["welcome_claims_7d"]["value"] == 1
    # d1 row detail
    d1 = next(c for c in out["campaigns"] if c["drop_id"] == "d1")
    assert d1["claimed"] == 2
    assert d1["remaining"] == 8  # pooled counters 5+3
    assert d1["detail"]["failure_reasons"] == [{"reason": "sold_out", "count": 1}]


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
