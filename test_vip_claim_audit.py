"""Regression tests for the VIP1 voucher-claim audit.

Each class pins one proven failure path that blocked genuine VIP1 users:

1. Campaign Builder "vip" audience compiled to eligibility.allow=["VIP"],
   but users.status / users.vip_tier are stored as "VIP1" and tier matching
   was an exact string compare -> every VIP1 user tier_mismatch'ed.
2. is_user_eligible_for_drop() / is_drop_allowed() read users.status only,
   while the Mini App crown reads vip_tier-first -> a user shown as VIP1
   could be denied. vip_tier now counts only for the CURRENT KL vip_month,
   so a stale vip_tier can never grant access.
3. _acquire_request_dedup_lock() never checked expiresAt, so the nominal 5s
   lock lived until Mongo's TTL monitor swept it (~60s) -> every retry after
   a transient failure got 429 "busy" until the FCFS pool was gone.
4. apply_monthly_tier_update() derived VIP1 from live users.monthly_xp at
   00:00 KL on the 1st, the same instant tick_5min's XP snapshot rollover
   zeroes it -> users processed after the reset were demoted to Normal.
5. An existing successful claim was checked only AFTER the subscription /
   sold-out / cooldown gates, so a re-tap by the owner could render
   "fully redeemed" instead of returning the already-issued code.

Plus end-to-end /vouchers/claim coverage of a VIP tier pooled drop:
Telegram API failure (503 verification_failed) vs confirmed non-membership
(403 not_subscribed) vs a normal successful claim.
"""
import json
import sys
import os
import unittest.mock as mock
from datetime import datetime, timedelta, timezone

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import mongomock
import pytest

import database

if database._db is None:
    with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
        import main  # noqa: E402
else:  # pragma: no cover
    import main  # noqa: E402

import campaign_builder
import vouchers as v


@pytest.fixture(autouse=True)
def _mongomock_db(monkeypatch):
    # Other suites in the full run reset database._db; re-point the lazy
    # collection proxies at a fresh in-memory DB for each test.
    if database._db is None:
        monkeypatch.setattr(database, "_db", mongomock.MongoClient()["referral_bot"])
    yield


def _current_vip_month():
    return datetime.now(main.KL_TZ).strftime("%Y-%m")


def _tier_drop(allow, **extra):
    now = datetime.now(timezone.utc)
    drop = {
        "_id": "vip-drop-1",
        "name": "VIP Drop",
        "type": "pooled",
        "status": "active",
        "startsAt": now - timedelta(minutes=5),
        "endsAt": now + timedelta(hours=1),
        "eligibility": {"mode": "tier", "allow": list(allow)},
        "audience": {},
    }
    drop.update(extra)
    return drop


def _tg(uid):
    return {"id": uid, "username": f"user{uid}"}


# ---------------------------------------------------------------------------
# 1 + 2. Tier matching
# ---------------------------------------------------------------------------

class TestTierEligibility:
    def setup_method(self):
        self.app = main.app

    def _eligible(self, user_doc, allow):
        with self.app.app_context():
            return v.is_user_eligible_for_drop(user_doc, _tg(user_doc.get("user_id", 1)), _tier_drop(allow))

    def test_vip1_status_matches_vip1_drop(self):
        assert self._eligible({"user_id": 1, "status": "VIP1"}, ["VIP1"]) is True

    def test_normal_user_denied(self):
        assert self._eligible({"user_id": 2, "status": "Normal", "vip_tier": "Normal"}, ["VIP1"]) is False

    def test_campaign_builder_vip_alias_matches_vip1_user(self):
        # Campaign Builder's default tier is "VIP" (backend + UI prefill).
        assert self._eligible({"user_id": 3, "status": "VIP1"}, ["VIP"]) is True

    def test_vip_alias_does_not_admit_normal_user(self):
        assert self._eligible({"user_id": 4, "status": "Normal"}, ["VIP"]) is False

    def test_spacing_variant_vip_1_matches(self):
        assert self._eligible({"user_id": 5, "status": "VIP1"}, ["vip 1"]) is True

    def test_vip1_in_vip_tier_with_status_normal_current_month(self):
        doc = {"user_id": 6, "status": "Normal", "vip_tier": "VIP1", "vip_month": _current_vip_month()}
        assert self._eligible(doc, ["VIP1"]) is True

    def test_stale_vip_tier_from_previous_month_does_not_grant(self):
        doc = {"user_id": 7, "status": "Normal", "vip_tier": "VIP1", "vip_month": "2000-01"}
        assert self._eligible(doc, ["VIP1"]) is False

    def test_vip_tier_without_vip_month_does_not_grant(self):
        doc = {"user_id": 8, "status": "Normal", "vip_tier": "VIP1"}
        assert self._eligible(doc, ["VIP1"]) is False

    def test_missing_user_doc_denied(self):
        with self.app.app_context():
            assert v.is_user_eligible_for_drop(None, _tg(9), _tier_drop(["VIP1"])) is False

    def test_audience_statuses_uses_same_effective_tier(self):
        v.users_collection.delete_many({})
        v.users_collection.insert_one(
            {"user_id": 10, "usernameLower": "user10", "status": "Normal", "vip_tier": "VIP1", "vip_month": _current_vip_month()}
        )
        drop = {"_id": "d-aud", "audience": {"statuses": ["VIP1"]}}
        with self.app.app_context():
            ctx = v.load_user_context(uid=10, username="user10")
            assert v.is_drop_allowed(drop, 10, "user10", ctx) is True
        v.users_collection.update_one({"user_id": 10}, {"$set": {"vip_month": "2000-01"}})
        with self.app.app_context():
            ctx = v.load_user_context(uid=10, username="user10")
            assert v.is_drop_allowed(drop, 10, "user10", ctx) is False


class TestCampaignBuilderVipAudience:
    def test_default_vip_mode_compiles_to_vip1(self):
        elig, _aud, _w = campaign_builder._build_audience({"audience_mode": "vip", "audience_params": {}}, None)
        assert elig == {"mode": "tier", "allow": ["VIP1"]}

    def test_explicit_vip_alias_compiles_to_vip1(self):
        elig, _aud, _w = campaign_builder._build_audience({"audience_mode": "vip", "audience_params": {"tier": "VIP"}}, None)
        assert elig == {"mode": "tier", "allow": ["VIP1"]}


# ---------------------------------------------------------------------------
# 3. Request dedup lock must honour its own expiresAt
# ---------------------------------------------------------------------------

class TestRequestDedupLock:
    def setup_method(self):
        # mongomock emulates the TTL monitor by deleting expired docs on read;
        # real Mongo sweeps only every ~60s. Drop the TTL index so the test
        # sees what production sees between sweeps: an expired doc still there.
        v.request_dedup_col.delete_many({})
        v.request_dedup_col.drop_indexes()
        self._ttl_patch = mock.patch.object(v, "_REQUEST_DEDUP_TTL_READY", True)
        self._ttl_patch.start()

    def teardown_method(self):
        self._ttl_patch.stop()

    def test_second_click_inside_ttl_is_busy(self):
        assert v._acquire_request_dedup_lock(drop_id="d1", uid=1, ttl_seconds=5) is True
        assert v._acquire_request_dedup_lock(drop_id="d1", uid=1, ttl_seconds=5) is False

    def test_retry_after_ttl_is_allowed_even_before_ttl_monitor_sweeps(self):
        assert v._acquire_request_dedup_lock(drop_id="d1", uid=2, ttl_seconds=5) is True
        # Simulate 6s elapsed with the doc still present (Mongo TTL monitor
        # runs only every ~60s).
        v.request_dedup_col.update_one(
            {"_id": "dedup:drop:d1:uid:2"},
            {"$set": {"expiresAt": datetime.now(timezone.utc) - timedelta(seconds=1)}},
        )
        assert v._acquire_request_dedup_lock(drop_id="d1", uid=2, ttl_seconds=5) is True
        # ...and the re-acquired lock is live again.
        assert v._acquire_request_dedup_lock(drop_id="d1", uid=2, ttl_seconds=5) is False

    def test_lock_is_per_drop_and_per_user(self):
        assert v._acquire_request_dedup_lock(drop_id="d1", uid=3) is True
        assert v._acquire_request_dedup_lock(drop_id="d2", uid=3) is True
        assert v._acquire_request_dedup_lock(drop_id="d1", uid=4) is True


# ---------------------------------------------------------------------------
# 4. Monthly tier job vs XP snapshot rollover
# ---------------------------------------------------------------------------

OCT1_0000 = datetime(2026, 10, 1, 0, 0, 5, tzinfo=main.KL_TZ)


class TestMonthlyTierUsesLedger:
    def setup_method(self):
        main.users_collection.delete_many({})
        main.db["xp_events"].delete_many({})
        main.monthly_xp_history_collection.delete_many({})
        main.admin_cache_col.delete_many({})

    def _xp(self, uid, amount, when, **extra):
        doc = {"user_id": uid, "xp": amount, "created_at": when, "reason": "test"}
        doc.update(extra)
        main.db["xp_events"].insert_one(doc)

    def test_rollover_already_zeroed_monthly_xp_keeps_genuine_vip1(self):
        sept = datetime(2026, 9, 15, 12, 0, tzinfo=main.KL_TZ)
        # Genuine VIP1: 900 XP in September. Rollover already zeroed the
        # live counter before the tier job reached this user.
        main.users_collection.insert_one({"user_id": 101, "username": "a", "monthly_xp": 0, "status": "VIP1", "vip_tier": "VIP1", "vip_month": "2026-09"})
        self._xp(101, 900, sept)
        # Normal user: 300 XP.
        main.users_collection.insert_one({"user_id": 102, "username": "b", "monthly_xp": 0, "status": "Normal"})
        self._xp(102, 300, sept)
        # Crossed 800 in the last minutes of Sept, not yet settled into
        # users.monthly_xp by the 5-min snapshot.
        main.users_collection.insert_one({"user_id": 103, "username": "c", "monthly_xp": 780, "status": "Normal"})
        self._xp(103, 780, sept)
        self._xp(103, 40, datetime(2026, 9, 30, 23, 58, tzinfo=main.KL_TZ))
        # Invalidated XP and October XP must not count toward Sept.
        main.users_collection.insert_one({"user_id": 104, "username": "d", "monthly_xp": 0, "status": "Normal"})
        self._xp(104, 500, sept)
        self._xp(104, 500, sept, invalidated=True)
        self._xp(104, 500, datetime(2026, 10, 1, 0, 0, 1, tzinfo=main.KL_TZ))

        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")

        def tier(uid):
            d = main.users_collection.find_one({"user_id": uid})
            return d["status"], d["vip_tier"], d["vip_month"]

        assert tier(101) == ("VIP1", "VIP1", "2026-10")
        assert tier(102) == ("Normal", "Normal", "2026-10")
        assert tier(103) == ("VIP1", "VIP1", "2026-10")
        assert tier(104) == ("Normal", "Normal", "2026-10")

    def test_tier_job_does_not_overwrite_new_month_monthly_xp(self):
        main.users_collection.insert_one({"user_id": 201, "username": "a", "monthly_xp": 0, "status": "Normal"})
        self._xp(201, 900, datetime(2026, 9, 10, tzinfo=main.KL_TZ))
        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        doc = main.users_collection.find_one({"user_id": 201})
        assert doc["monthly_xp"] == 0  # owned by the XP snapshot, not this job
        hist = main.monthly_xp_history_collection.find_one({"user_id": 201, "month": "2026-10"})
        assert hist["monthly_xp"] == 900

    def test_rerun_mid_month_does_not_downgrade_in_month_unlock(self):
        # maybe_unlock_vip1 promoted this user mid-October.
        main.users_collection.insert_one({"user_id": 301, "username": "a", "monthly_xp": 850, "status": "VIP1", "vip_tier": "VIP1", "vip_month": "2026-10"})
        main.apply_monthly_tier_update(run_time=datetime(2026, 10, 15, tzinfo=main.KL_TZ), run_id="t")
        assert main.users_collection.find_one({"user_id": 301})["status"] == "VIP1"

    def test_stale_resume_cursor_from_other_month_is_ignored(self):
        main.users_collection.insert_one({"user_id": 401, "username": "a", "monthly_xp": 0, "status": "Normal"})
        self._xp(401, 900, datetime(2026, 9, 10, tzinfo=main.KL_TZ))
        last = main.users_collection.find_one({"user_id": 401})["_id"]
        # Leftover cursor from a failed September run points past this user.
        main.admin_cache_col.update_one(
            {"_id": "vip_monthly:last_id"}, {"$set": {"last_id": last, "month": "2026-09"}}, upsert=True
        )
        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        assert main.users_collection.find_one({"user_id": 401})["status"] == "VIP1"

    # --- ledger semantics: boundaries, reversals, timestamps -----------------

    def _run_oct1_and_tier(self, uid):
        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        hist = main.monthly_xp_history_collection.find_one({"user_id": uid, "month": "2026-10"})
        return hist["monthly_xp"], main.users_collection.find_one({"user_id": uid})["status"]

    def test_kl_month_boundaries_inclusive_start_exclusive_end(self):
        kl = main.KL_TZ
        main.users_collection.insert_one({"user_id": 501, "username": "a", "status": "Normal"})
        self._xp(501, 100, datetime(2026, 9, 1, 0, 0, 0).replace(tzinfo=kl).astimezone(timezone.utc))   # in (start)
        self._xp(501, 200, datetime(2026, 9, 30, 23, 59, 59, 999000).replace(tzinfo=kl).astimezone(timezone.utc))  # in
        self._xp(501, 400, datetime(2026, 8, 31, 23, 59, 59, 999000).replace(tzinfo=kl).astimezone(timezone.utc))  # out
        self._xp(501, 800, datetime(2026, 10, 1, 0, 0, 0).replace(tzinfo=kl).astimezone(timezone.utc))  # out (end)
        assert self._run_oct1_and_tier(501) == (300, "Normal")

    def test_utc_dates_are_bucketed_by_kl_month(self):
        main.users_collection.insert_one({"user_id": 502, "username": "a", "status": "Normal"})
        # 2026-08-31 16:30Z is Sept 1 00:30 KL -> September.
        self._xp(502, 500, datetime(2026, 8, 31, 16, 30, tzinfo=timezone.utc))
        # 2026-09-30 16:30Z is Oct 1 00:30 KL -> October, not September.
        self._xp(502, 500, datetime(2026, 9, 30, 16, 30, tzinfo=timezone.utc))
        # Naive datetimes (as pymongo returns them) are UTC.
        self._xp(502, 300, datetime(2026, 9, 15, 4, 0))
        assert self._run_oct1_and_tier(502) == (800, "VIP1")

    def test_threshold_is_exactly_800(self):
        sept = datetime(2026, 9, 10, tzinfo=main.KL_TZ)
        main.users_collection.insert_many([
            {"user_id": 503, "username": "a", "status": "Normal"},
            {"user_id": 504, "username": "b", "status": "VIP1", "vip_tier": "VIP1", "vip_month": "2026-09"},
        ])
        self._xp(503, 800, sept)
        self._xp(504, 799, sept)
        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        assert main.users_collection.find_one({"user_id": 503})["status"] == "VIP1"
        assert main.users_collection.find_one({"user_id": 504})["status"] == "Normal"  # new month -> may drop

    def test_null_or_missing_created_at_falls_back_to_ts(self):
        main.users_collection.insert_one({"user_id": 505, "username": "a", "status": "Normal"})
        sept = datetime(2026, 9, 10, tzinfo=timezone.utc)
        main.db["xp_events"].insert_many([
            {"user_id": 505, "xp": 400, "ts": sept},                       # missing created_at
            {"user_id": 505, "xp": 400, "created_at": None, "ts": sept},   # null created_at
            {"user_id": 505, "xp": 400, "created_at": datetime(2026, 8, 1, tzinfo=timezone.utc), "ts": sept},  # created_at wins
        ])
        assert self._run_oct1_and_tier(505) == (800, "VIP1")

    def test_reversal_after_counting_is_excluded(self):
        sept = datetime(2026, 9, 10, tzinfo=main.KL_TZ)
        main.users_collection.insert_one({"user_id": 506, "username": "a", "status": "Normal"})
        self._xp(506, 500, sept)
        # Referral XP counted into monthly_xp, then revoked (flag only; no
        # negative event is ever written — see rollback_pending_referral_xp).
        self._xp(506, 500, sept, invalidated=True, xp_counted=True, invalidated_at=datetime(2026, 9, 20, tzinfo=timezone.utc))
        self._xp(506, 0, sept, invalidated=False)
        assert self._run_oct1_and_tier(506) == (500, "Normal")

    def test_matches_xp_snapshot_monthly_xp_at_month_end(self):
        """The ledger sum must equal what the production XP snapshot would have
        held in users.monthly_xp at the end of September, for sample users."""
        import xp_snapshot
        main.db["xp_snapshot_state"].delete_many({})
        kl = main.KL_TZ
        samples = {
            601: [(900, datetime(2026, 9, 3, 12)), (50, datetime(2026, 8, 30, 12))],
            602: [(300, datetime(2026, 9, 1, 0, 0)), (500, datetime(2026, 9, 30, 23, 59)), (400, datetime(2026, 10, 1, 0, 1))],
            603: [(799, datetime(2026, 9, 12, 9))],
            604: [(1000, datetime(2026, 9, 5)), (200, datetime(2026, 9, 6))],
        }
        for uid, events in samples.items():
            main.users_collection.insert_one({"user_id": uid, "username": str(uid), "status": "Normal"})
            for amt, local in events:
                # Stored as naive UTC, exactly as pymongo round-trips dates.
                self._xp(uid, amt, local.replace(tzinfo=kl).astimezone(timezone.utc).replace(tzinfo=None))
        sept_mid = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
        sept_end = datetime(2026, 9, 30, 23, 59, 59, tzinfo=kl).astimezone(timezone.utc)
        # Production steady state: incremental settler with an existing cursor.
        main.db["xp_snapshot_state"].insert_one({
            "_id": xp_snapshot.CURSOR_ID,
            "last_event_id": None,
            "last_correction_at": datetime(2026, 9, 2),
            "week_key": xp_snapshot._week_window_utc(sept_mid)[0].date().isoformat(),
            "month_key": xp_snapshot._month_window_utc(sept_mid)[0].date().isoformat(),
        })
        # 604's 200 XP was revoked before settlement (never counted). The
        # counted-then-revoked path is covered by
        # test_reversal_after_counting_is_excluded; the snapshot's own
        # correction step can't run here (naive/aware compare, see PR notes).
        main.db["xp_events"].update_one(
            {"user_id": 604, "xp": 200},
            {"$set": {"invalidated": True, "invalidated_at": datetime(2026, 9, 1)}},
        )
        xp_snapshot.settle_xp_snapshots_incremental(main.db, now_utc_ts=sept_mid)
        xp_snapshot.settle_xp_snapshots_incremental(main.db, now_utc_ts=sept_end)
        snapshot = {u["user_id"]: int(u.get("monthly_xp", 0)) for u in main.users_collection.find({"user_id": {"$in": list(samples)}})}
        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        ledger = {h["user_id"]: h["monthly_xp"] for h in main.monthly_xp_history_collection.find({"month": "2026-10"})}
        assert ledger == {601: 900, 602: 800, 603: 799, 604: 1000}
        # Snapshot counts 602's Oct-1 event only after rollover, so on Sept 30
        # it must agree with the ledger for every sample user.
        assert ledger == snapshot, (ledger, snapshot)

    # --- resume safety --------------------------------------------------------

    def test_same_month_cursor_resumes_without_reprocessing_earlier_users(self):
        main.users_collection.insert_many([
            {"user_id": 701, "username": "a", "status": "Normal"},
            {"user_id": 702, "username": "b", "status": "Normal"},
        ])
        for uid in (701, 702):
            self._xp(uid, 900, datetime(2026, 9, 10, tzinfo=main.KL_TZ))
        first = main.users_collection.find_one({"user_id": 701})["_id"]
        main.admin_cache_col.update_one(
            {"_id": "vip_monthly:last_id"}, {"$set": {"last_id": first, "month": "2026-10"}}, upsert=True
        )
        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        assert main.users_collection.find_one({"user_id": 701})["status"] == "Normal"  # before cursor: skipped
        assert main.users_collection.find_one({"user_id": 702})["status"] == "VIP1"
        assert main.admin_cache_col.find_one({"_id": "vip_monthly:last_id"}) is None  # cleared on success

    def test_legacy_cursor_without_month_is_ignored(self):
        main.users_collection.insert_one({"user_id": 801, "username": "a", "status": "Normal"})
        self._xp(801, 900, datetime(2026, 9, 10, tzinfo=main.KL_TZ))
        last = main.users_collection.find_one({"user_id": 801})["_id"]
        main.admin_cache_col.update_one({"_id": "vip_monthly:last_id"}, {"$set": {"last_id": last}}, upsert=True)
        main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        assert main.users_collection.find_one({"user_id": 801})["status"] == "VIP1"

    def test_cursor_saved_on_interruption_is_tagged_with_run_month(self):
        from pymongo.errors import CursorNotFound
        main.users_collection.insert_many([
            {"user_id": 900, "username": "z", "status": "Normal"},
            {"user_id": 901, "username": "a", "status": "Normal"},
        ])
        self._xp(901, 900, datetime(2026, 9, 10, tzinfo=main.KL_TZ))
        real_update = main._users_update_one
        calls = {"n": 0}

        def boom(*a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                return real_update(*a, **k)  # user 900 succeeds
            raise CursorNotFound("cursor lost")  # user 901's write keeps failing

        with mock.patch.object(main, "_users_update_one", side_effect=boom):
            with pytest.raises(CursorNotFound):
                main.apply_monthly_tier_update(run_time=OCT1_0000, run_id="t")
        doc = main.admin_cache_col.find_one({"_id": "vip_monthly:last_id"})
        assert doc is not None and doc.get("month") == "2026-10"
        # Cursor sits on the last SUCCESSFUL user, so 901 is retried, not skipped.
        assert doc["last_id"] == main.users_collection.find_one({"user_id": 900})["_id"]
        assert calls["n"] == 4  # 900 once + 901 attempted on each of 3 tries
        # A later run in a DIFFERENT month must not resume from it...
        main.apply_monthly_tier_update(run_time=datetime(2026, 11, 1, 0, 0, 5, tzinfo=main.KL_TZ), run_id="t2")
        u = main.users_collection.find_one({"user_id": 901})
        assert u["vip_month"] == "2026-11"  # processed from the start, with November's own window
        assert u["status"] == "Normal"      # October ledger has 0 XP -> not VIP for November


# ---------------------------------------------------------------------------
# End-to-end /vouchers/claim for a VIP tier pooled drop
# ---------------------------------------------------------------------------

class _Resp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class TestVipPooledClaimEndToEnd:
    DROP_ID = "vip-e2e-drop"

    def setup_method(self):
        db = v.db
        for name in ("drops", "vouchers", "voucher_claims"):
            db[name].delete_many({})
        v.users_collection.delete_many({})
        v.subscription_cache_col.delete_many({})
        v.request_dedup_col.delete_many({})
        v.claim_rate_limits_col.delete_many({})
        now = datetime.now(timezone.utc)
        db.drops.insert_one({
            "_id": self.DROP_ID,
            "name": "VIP1 Drop",
            "type": "pooled",
            "status": "active",
            "startsAt": now - timedelta(minutes=5),
            "endsAt": now + timedelta(hours=1),
            "eligibility": {"mode": "tier", "allow": ["VIP1"]},
            "audience": {},
        })
        for i in range(3):
            db.vouchers.insert_one({"type": "pooled", "dropId": self.DROP_ID, "code": f"VIPCODE{i}", "status": "free", "pool": "public"})
        self.tg_calls = 0
        self.tg_response = _Resp(200, {"ok": True, "result": {"status": "member"}})
        self.patches = [
            mock.patch.object(v, "extract_raw_init_data_from_query", lambda req: "signed-init-data"),
            mock.patch.object(v.time, "sleep", lambda *_a, **_k: None),
            mock.patch.object(v.requests, "get", self._fake_get),
            mock.patch.object(v, "OFFICIAL_CHANNEL_ID", -1001),
        ]
        for p in self.patches:
            p.start()
        self._uid = None

    def teardown_method(self):
        for p in reversed(self.patches):
            p.stop()

    def _fake_get(self, url, params=None, timeout=None):  # noqa: ARG002
        self.tg_calls += 1
        return self.tg_response

    def _claim(self, uid, ip="203.0.113.10"):
        user_json = json.dumps({"id": uid, "username": f"user{uid}"})
        with mock.patch.object(v, "verify_telegram_init_data", lambda _d: (True, {"user": user_json, "auth_date": "1"}, "ok")):
            client = main.app.test_client()
            resp = client.post(
                "/v2/miniapp/vouchers/claim",
                json={"dropId": self.DROP_ID},
                headers={"Fly-Client-IP": ip},
            )
        return resp.status_code, resp.get_json() or {}

    def _user(self, uid, **fields):
        doc = {"user_id": uid, "username": f"user{uid}", "usernameLower": f"user{uid}", "region": "Thailand"}
        doc.update(fields)
        v.users_collection.insert_one(doc)

    def test_normal_successful_claim_then_idempotent_retap(self):
        self._user(501, status="VIP1", vip_tier="VIP1", vip_month=_current_vip_month())
        status, body = self._claim(501)
        assert status == 200, body
        code = body.get("voucher", {}).get("code") or body.get("code") or body.get("voucher_code")
        assert code and code.startswith("VIPCODE")
        # Re-tap after the 5s dedup window: must return the same code.
        v.request_dedup_col.delete_many({})
        status2, body2 = self._claim(501)
        assert status2 == 200, body2
        assert body2.get("status") == "already_claimed"
        assert body2.get("voucher_code") == code

    def test_vip1_in_vip_tier_with_status_normal_can_claim(self):
        self._user(502, status="Normal", vip_tier="VIP1", vip_month=_current_vip_month())
        status, body = self._claim(502)
        assert status == 200, body

    def test_normal_user_is_not_eligible(self):
        self._user(503, status="Normal", vip_tier="Normal", vip_month=_current_vip_month())
        status, body = self._claim(503)
        assert status == 403
        assert body.get("code") == "not_eligible"

    def test_telegram_api_failure_is_verification_failed_not_not_subscribed(self):
        self._user(504, status="VIP1")
        self.tg_response = _Resp(429, {"ok": False, "error_code": 429})
        status, body = self._claim(504)
        assert status == 503, body
        assert body.get("code") == "verification_failed"
        assert body.get("code") != "not_subscribed"
        assert v.db.voucher_claims.count_documents({}) == 0

    def test_confirmed_non_member_is_not_subscribed(self):
        self._user(505, status="VIP1")
        self.tg_response = _Resp(200, {"ok": True, "result": {"status": "left"}})
        status, body = self._claim(505)
        assert status == 403, body
        assert body.get("code") == "not_subscribed"
        assert v.db.voucher_claims.count_documents({}) == 0

    def test_existing_claim_returned_even_when_pool_now_empty(self):
        self._user(506, status="VIP1")
        status, body = self._claim(506)
        assert status == 200, body
        # Pool drains to zero after this user's successful claim.
        v.db.vouchers.update_many({"status": "free"}, {"$set": {"status": "claimed"}})
        v.request_dedup_col.delete_many({})
        status2, body2 = self._claim(506)
        assert status2 == 200, body2
        assert body2.get("status") == "already_claimed"


# ---------------------------------------------------------------------------
# scripts/vip_claim_audit.py (read-only diagnostic)
# ---------------------------------------------------------------------------

def _load_audit_script():
    import importlib.util
    spec = importlib.util.spec_from_file_location("vip_claim_audit", os.path.join(os.path.dirname(__file__), "scripts", "vip_claim_audit.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestVipClaimAuditScript:
    def _run(self, argv, capsys):
        mod = _load_audit_script()
        with mock.patch.object(sys, "argv", ["vip_claim_audit.py", *argv]), \
             mock.patch.object(database, "init_db", lambda *a, **k: None):
            mod.main()
        return capsys.readouterr().out

    def setup_method(self):
        for name in ("drops", "users", "xp_events", "voucher_claims", "vouchers", "subscription_cache"):
            database.get_db()[name].delete_many({})

    def test_drop_flag_finds_objectid_keyed_drop_and_ts_only_xp(self, capsys):
        from bson import ObjectId
        db = database.get_db()
        oid = ObjectId()
        now = datetime.now(timezone.utc)
        db.drops.insert_one({"_id": oid, "eligibility": {"mode": "tier", "allow": ["VIP1"]}, "audience": {},
                             "startsAt": now, "endsAt": now + timedelta(hours=1)})
        cur = datetime.now(main.KL_TZ)
        prev = (cur.replace(day=1) - timedelta(days=1)).replace(day=15, tzinfo=None)
        db.users.insert_one({"user_id": 1329748443, "status": "Normal", "vip_tier": "Normal",
                             "vip_month": cur.strftime("%Y-%m"), "region": "Thailand"})
        db.xp_events.insert_one({"user_id": 1329748443, "xp": 900, "ts": prev})  # legacy: ts only
        db.voucher_claims.insert_one({"drop_id": ObjectId(), "user_id": 1329748443, "status": "claimed",
                                      "voucher_code": "SECRET-CODE-123", "claimed_at": now})
        out = self._run(["--uids", "1329748443", "--drop", str(oid)], capsys)
        assert "tier drops (last 30d): 1" in out
        assert f"drop={oid}" in out
        assert "13…443" in out and "1329748443" not in out  # masked
        assert "| 900 |" in out and "monthly_race_demoted" in out
        assert "SECRET-CODE-123" not in out  # never prints voucher codes

    def test_drop_flag_still_finds_legacy_string_keyed_drop(self, capsys):
        db = database.get_db()
        now = datetime.now(timezone.utc)
        db.drops.insert_one({"_id": "legacy-vip-drop", "eligibility": {"mode": "tier", "allow": ["VIP"]}, "audience": {},
                             "startsAt": now, "endsAt": now + timedelta(hours=1)})
        out = self._run(["--drop", "legacy-vip-drop"], capsys)
        assert "drop=legacy-vip-drop" in out and "ALIAS_VIP" in out
