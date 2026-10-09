"""Databot UIM redemption evidence -> sync -> Qualification Preview / live validator.

The fake feed below speaks Databot's real contract (databot.uim_redemption_feed.v1,
see Databot app/services/uim_redemption_feed.py): batches with commit status,
per-batch rows with stable refs, naive GMT+8 wall clock stored as UTC digits,
provenance bases, ``deleted`` observations for stale-row removals.

The qualification rule stays DISABLED in every test except the explicit parity
test, which activates it on its own copy to prove preview == live.

The multi-threaded real-MongoDB variant lives in test_uim_redemption_sync_real_mongo.py.
"""
from __future__ import annotations

import re
import threading
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import mongomock
import pytest

import affiliate_qualification as aq
import affiliate_qualification_preview as aqp
import uim_redemption_sync as usync
from scripts import affiliate_qualification_admin as admin_cli
from scripts import migrate_affiliate_account_dedupe as migration
from test_affiliate_welcome_redemption_qualification import (  # noqa: F401  (_scheduler is an autouse fixture)
    CUTOFF, KL, NOW, World, _ReadOnlyDb, _scheduler, _snapshot, run,
)

DATABOT_CONFIG = {
    "source": aq.SOURCE_DATABOT_UIM,
    **aq.DATABOT_FIXED_COLUMNS,
    "namespace_column": None,
    "account_namespace": "advantplay",
    "source_timezone": "Asia/Kuala_Lumpur",
    "success_basis": aq.SUCCESS_BASIS_SOURCE_CONTRACT,
    "success_attestation": {"by": "Marketing data owner", "reference": "OPS-1234"},
    "legacy_time_basis": aq.LEGACY_TIME_REVIEW,
    "campaign_ids": [],
}


def disabled_world():
    w = World(mode=aq.MODE_DISABLED, cutoff=None)
    w.db[aq.CONTROL_COLLECTION].update_one(
        {"_id": aq.CONTROL_ID},
        {"$set": {"source_config": dict(DATABOT_CONFIG)},
         "$unset": {"seed_completed_at": "", "seeded_identity_config": ""}},
    )
    return w


def urow(code, account, wall, *, basis="naive_wallclock", observation="upsert", account_basis="text",
         file_type="csv", column="Coupon Redeem Time", campaign="WelcomeBonus"):
    """A feed row exactly as Databot serialises one. ``wall`` is the UIM
    export's GMT+8 wall clock; Databot stores naive digits as if UTC."""
    digits = datetime.strptime(wall, "%Y-%m-%d %H:%M:%S") if wall else None
    return {
        "observation": observation, "coupon_code": code, "account": account, "account_basis": account_basis,
        "file_type": file_type,
        "redeemed_at_utc": digits.replace(tzinfo=timezone.utc).isoformat() if digits else None,
        "redeemed_at_wallclock": digits.isoformat() if digits and basis != "source_offset" else None,
        "redeemed_at_basis": basis if digits else None, "redeemed_at_raw": wall, "redeemed_at_column": column,
        "campaign": campaign, "source_period": "2026-W41", "provenance": "import_log", "provenance_version": 1,
    }


class FakeFeed:
    def __init__(self):
        self.batches: list[dict] = []
        self.calls = 0
        self.fail_after: int | None = None
        self.lock = threading.Lock()

    def add_batch(self, rows, *, status="committed", committed_at=None):
        batch_id = f"b{len(self.batches) + 1}-{uuid.uuid4().hex[:6]}"
        stamped = []
        for i, row in enumerate(rows):
            stamped.append({**row, "ref": f"{batch_id}:log:{i:06d}"} if row is not None else None)
        self.batches.append({"batch_id": batch_id, "status": status, "rows": stamped,
                             "committed_at": committed_at or NOW - timedelta(days=1) + timedelta(minutes=len(self.batches))})
        return batch_id

    def batch(self, batch_id):
        return next(b for b in self.batches if b["batch_id"] == batch_id)

    def __call__(self, path, params=None):
        params = params or {}
        with self.lock:
            self.calls += 1
            if self.fail_after is not None and self.calls > self.fail_after:
                raise usync.FeedUnavailable(f"feed_http_503 path={path}")
        meta = lambda b: {"batch_id": b["batch_id"], "status": b["status"],  # noqa: E731
                          "committed_at": b["committed_at"].isoformat(), "file_type": "csv",
                          "rolled_back_at": NOW.isoformat() if b["status"] == "rolled_back" else None}
        if path == "/batches":
            start = int(params.get("after") or 0)
            limit = int(params.get("limit") or 200)
            page = self.batches[start:start + limit]
            more = start + limit < len(self.batches)
            return {"contract": usync.FEED_CONTRACT, "batches": [meta(b) for b in page],
                    "next_after": str(start + limit) if more else None}
        m = re.match(r"^/batches/(.+)/rows$", path)
        b = self.batch(m.group(1))
        out = {"contract": usync.FEED_CONTRACT, "batch": meta(b), "rows": [], "skipped_no_code": 0}
        if b["status"] != "committed":
            return {**out, "rows_available": False, "reason": f"batch_status_{b['status']}", "done": True}
        start = int(params.get("after") or 0)
        limit = int(params.get("limit") or 1000)
        page = b["rows"][start:start + limit]
        for row in page:
            if row is None or not row.get("coupon_code"):
                out["skipped_no_code"] += 1
            else:
                out["rows"].append(row)
        done = start + limit >= len(b["rows"])
        return {**out, "rows_available": True, "rows_in_page": len(page), "done": done,
                "next_after": None if done else str(start + limit)}


def sync(w, feed, now=NOW, **kw):
    return usync.sync_once(w.db, fetch=feed, now_utc=now, **kw)


def preview(db, month="202610", now=NOW, config=None):
    return aqp.build_preview(_ReadOnlyDb(db), month=month, now_utc=now, source_config=config)


def nothing_live_written(db):
    assert db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 0
    assert db[aq.REGISTRY_COLLECTION].count_documents({}) == 0
    assert db[aq.EVIDENCE_COLLECTION].count_documents({}) == 0
    assert db.xp_events.count_documents({}) == 0
    assert db.affiliate_ledger.count_documents({"ledger_type": {"$ne": "WELCOME"}}) == 0
    control = aq.get_control(db)
    assert control["mode"] == aq.MODE_DISABLED and "launch_cutoff_utc" not in control


# 1 — five invited, one valid Welcome redemption ------------------------------

def test_five_invited_one_redemption_previews_one_and_live_is_untouched(monkeypatch):
    w = disabled_world()
    for invitee in range(11, 16):
        w.join(1, invitee)
        w.welcome_code(invitee, f"WELC{invitee}")
    feed = FakeFeed()
    feed.add_batch([urow("WELC13", "0001234", "2026-10-05 14:00:00"), urow("OTHER9", "x9", "2026-10-05 15:00:00")])
    result = sync(w, feed)
    assert result["ok"] and result["rows_stored"] == 1  # the non-Welcome row is counted, not stored
    stored = w.db[usync.ROWS_COLLECTION].find_one()
    assert stored["account"] == "0001234" and stored["coupon_code"] == "WELC13"

    before = _snapshot(w.db)
    out = preview(w.db)
    assert _snapshot(w.db) == before  # preview wrote nothing
    t = out["totals"]
    assert (t["joined"], t["current_qualified"], t["historical_would_qualify"], t["at_launch_eligible"]) == (5, 0, 1, 1)
    assert t["historical_awaiting_redemption"] == 4
    ev = out["evidence_summary"]
    assert (ev["rows_received"], ev["unmatched_not_welcome"], ev["matched_to_one_welcome_recipient"],
            ev["would_qualify"]) == (2, 1, 1, 1)
    src = out["source"]
    assert src["name"] == aq.SOURCE_DATABOT_UIM and not src["blocked"]
    assert src["success_basis"]["attested"] is True and src["sync"]["last_success_at"] == NOW.isoformat()
    # Source coverage spans every received row (incl. non-Welcome), as GMT+8 wall clock.
    assert (src["sync"]["coverage_start"], src["sync"]["coverage_end"]) == ("2026-10-05T14:00:00", "2026-10-05T15:00:00")
    # The rule is disabled: the live pipeline does nothing with the synced evidence.
    assert run(w, monkeypatch) == {"active": False}
    nothing_live_written(w.db)


# 2 / 3 — lifetime Account ID dedupe -------------------------------------------

def test_two_welcome_codes_same_account_give_one_candidate():
    w = disabled_world()
    w.join(1, 11)
    w.join(2, 12)
    w.welcome_code(11, "WELC11")
    w.welcome_code(12, "WELC12")
    feed = FakeFeed()
    feed.add_batch([urow("WELC11", "acctA", "2026-10-05 10:00:00"), urow("WELC12", "acctA", "2026-10-06 10:00:00")])
    sync(w, feed)
    out = preview(w.db)
    t = out["totals"]
    assert (t["historical_would_qualify"], t["historical_duplicate_account_excluded"]) == (1, 1)
    assert (t["at_launch_eligible"], t["at_launch_duplicate_account_excluded"]) == (1, 1)
    rows = {r["referrer_id"]: r for r in out["affiliates"]}
    assert rows["1"]["historical"]["would_qualify"] == 1 and rows["2"]["historical"]["duplicate_account_excluded"] == 1
    nothing_live_written(w.db)


def test_same_account_across_affiliates_and_months_never_credits_twice():
    w = disabled_world()
    w.join(1, 11, at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    w.join(2, 12)
    w.welcome_code(11, "WELC11")
    w.welcome_code(12, "WELC12")
    feed = FakeFeed()
    feed.add_batch([urow("WELC11", "acctX", "2026-09-10 12:00:00")])
    feed.add_batch([urow("WELC12", "acctX", "2026-10-07 12:00:00")])
    sync(w, feed)
    sept, octo = preview(w.db, month="202609"), preview(w.db, month="202610")
    assert sept["totals"]["historical_would_qualify"] == 1
    assert (octo["totals"]["historical_would_qualify"], octo["totals"]["historical_duplicate_account_excluded"]) == (0, 1)
    assert octo["totals"]["at_launch_eligible"] == 1  # one lifetime credit, whichever month
    # An account already credited in the real registry is never credited again.
    w.db[aq.REGISTRY_COLLECTION].insert_one({"account_key": "advantplay:acctX", "state": aq.REG_SEEDED})
    assert preview(w.db)["totals"]["at_launch_eligible"] == 0


# 4 — non-Welcome, claim-only, invalid evidence ---------------------------------

def test_non_welcome_claim_only_and_untrusted_evidence_never_qualify():
    w = disabled_world()
    for invitee in range(11, 18):
        w.join(1, invitee)
        w.welcome_code(invitee, f"WELC{invitee}")
    w.db.voucher_pools.insert_one({"pool_id": "AFFILIATE_5", "code": "AFF1", "status": "issued", "issued_to_user_id": 11})
    feed = FakeFeed()
    feed.add_batch([
        urow("AFF1", "a1", "2026-10-05 10:00:00"),                                    # non-Welcome code
        urow("WELC12", "a2", "2026-10-05 10:00:00", column="claim_time"),            # claim time, not redemption
        urow("WELC13", "a3", "2026-10-05 10:00:00", basis="unknown_legacy"),         # timezone unverified
        urow("WELC14", "777", "2026-10-05 10:00:00", account_basis="numeric", file_type="xlsx"),  # zeros may be lost
        urow("WELC15", "778", "2026-10-05 10:00:00", account_basis="unknown_legacy", file_type="xlsx"),
        urow("WELC16", "a6", "2026-10-05 00:00:00", basis="date_only"),
        None,                                                                          # row without a code
    ])
    # WELC11/WELC17: claimed only (issued, never redeemed) -> nothing in the feed at all.
    sync(w, feed)
    out = preview(w.db)
    assert out["totals"]["historical_would_qualify"] == 0 and out["totals"]["at_launch_eligible"] == 0
    ev = out["evidence_summary"]
    assert ev["review_reasons"] == {
        "redeemed_at_column_is_a_claim_time": 1, "redeemed_at_timezone_unverified": 1,
        "account_numeric_coerced": 1, "account_numeric_unverifiable": 1, "redeemed_at_date_only": 1,
    }
    assert ev["unmatched_not_welcome"] == 1 and ev["rows_without_code"] == 1
    # date_only has no in-period redeemed_at, so it is in the all-time summary only.
    assert ev["requiring_review"] == 5
    nothing_live_written(w.db)


def test_success_basis_is_never_fabricated():
    w = disabled_world()
    cfg = {**DATABOT_CONFIG, "success_attestation": {}}
    assert "missing:success_attestation" in aq.validate_source_config(cfg)
    out = preview(w.db, config=cfg)
    assert out["source"]["blocked"] is True and out["affiliates"] == [] or all(
        r["historical"] is None for r in out["affiliates"])
    # The preview-only override labels success as ASSUMED, never attested.
    merged, overridden = aqp.source_config_from_args(
        {"source": "databot_uim", "success_basis": aq.SUCCESS_BASIS_SOURCE_CONTRACT, "account_namespace": "advantplay"},
        None)
    assert overridden and aq.validate_source_config(merged) == []
    assert merged["success_attestation"]["preview_only"] is True
    w.join(1, 11)
    w.welcome_code(11, "WELC11")
    feed = FakeFeed()
    feed.add_batch([urow("WELC11", "a1", "2026-10-05 10:00:00")])
    sync(w, feed)
    out = aqp.build_preview(_ReadOnlyDb(w.db), month="202610", now_utc=NOW, source_config=merged, config_overridden=True)
    assert out["source"]["success_basis"]["attested"] is False
    assert out["totals"]["historical_would_qualify"] == 1


# 5 — reimports, concurrency, crashes, cursor retries ----------------------------

def test_reimport_crash_and_retry_never_duplicate_or_lose_rows():
    w = disabled_world()
    for invitee in range(11, 41):
        w.join(1, invitee)
        w.welcome_code(invitee, f"WELC{invitee}")
    rows = [urow(f"WELC{i}", f"acct{i:04d}", "2026-10-05 10:00:00") for i in range(11, 41)]
    feed = FakeFeed()
    feed.add_batch(rows)
    feed.add_batch(rows[:10])  # overlapping re-import of the same redemptions
    monkey_page = usync.ROW_PAGE
    usync.ROW_PAGE = 7
    try:
        feed.fail_after = 3  # crash mid-batch (listing + 2 row pages succeed)
        first = sync(w, feed)
        assert first["ok"] is False and "feed_http_503" in first["error"]
        health = usync.sync_health(w.db, now_utc=NOW)
        assert health["last_error"].startswith("feed_http_503") and health["last_success_at"] is None
        feed.fail_after = None
        done = sync(w, feed, now=NOW + timedelta(minutes=5))
        assert done["ok"]
    finally:
        usync.ROW_PAGE = monkey_page
    assert w.db[usync.ROWS_COLLECTION].count_documents({}) == 40  # 30 + 10 re-observed, each stored once
    received = sum(b["counters"]["received"] for b in w.db[usync.BATCHES_COLLECTION].find())
    assert received == 40
    # A third full pass is a no-op.
    again = sync(w, feed, now=NOW + timedelta(minutes=10))
    assert again["rows_stored"] == 0 and w.db[usync.ROWS_COLLECTION].count_documents({}) == 40
    # Re-imports collapse to one evidence per (code, account): 30 would qualify, none duplicated.
    out = preview(w.db, now=NOW + timedelta(minutes=10))
    assert out["totals"]["historical_would_qualify"] == 30
    assert out["totals"]["historical_duplicate_account_excluded"] == 0


def test_lease_blocks_a_concurrent_run_and_expires():
    w = disabled_world()
    feed = FakeFeed()
    w.db[usync.STATE_COLLECTION].insert_one({"_id": usync.STATE_ID, "lease_until": NOW + timedelta(minutes=5),
                                            "lease_owner": "other"})
    assert sync(w, feed)["skipped"] == "lease_held"
    assert sync(w, feed, now=NOW + timedelta(minutes=6))["ok"] is True


def test_interleaved_runs_never_double_count_or_skip():
    """Deterministic cursor race: while run A holds a page, run B syncs the
    same batch to completion. A's conditional cursor update must lose (no
    double-counted counters), and nothing is skipped or duplicated.
    (A real multi-threaded run against MongoDB: test_uim_redemption_sync_real_mongo.py.)"""
    w = disabled_world()
    for i in range(20):
        w.welcome_code(100 + i, f"W{i}")
    feed = FakeFeed()
    feed.add_batch([urow(f"W{i}", f"0{i:04d}", "2026-10-05 10:00:00") for i in range(20)])
    page = usync.ROW_PAGE
    usync.ROW_PAGE = 6
    nested = {"done": False}

    def fresh():
        return {"batches_listed": 0, "rollbacks_propagated": 0, "rows_received": 0, "rows_stored": 0,
                "batches_completed": 0, "batches_unavailable": 0, "cursor_races": 0}

    def racing_fetch(path, params=None):
        body = feed(path, params)
        if path.endswith("/rows") and not nested["done"]:
            nested["done"] = True
            usync._sync_rows(w.db, feed, now_utc=NOW, max_rows=1000, out=fresh())  # run B
        return body

    try:
        usync._sync_batches(w.db, feed, now_utc=NOW, out=fresh())
        out_a = fresh()
        usync._sync_rows(w.db, racing_fetch, now_utc=NOW, max_rows=1000, out=out_a)  # run A
    finally:
        usync.ROW_PAGE = page
    assert out_a["cursor_races"] == 1
    batch = w.db[usync.BATCHES_COLLECTION].find_one()
    assert batch["rows_complete"] is True
    assert batch["counters"]["received"] == 20                     # counted exactly once
    assert w.db[usync.ROWS_COLLECTION].count_documents({}) == 20   # stored exactly once


# 6 — leading zeros and GMT+8 ---------------------------------------------------

def test_leading_zero_accounts_and_gmt8_month_boundary():
    w = disabled_world()
    w.join(1, 11)
    w.welcome_code(11, "WELC11")
    feed = FakeFeed()
    # 31 Oct 23:30 GMT+8 is still October in GMT+8 (15:30Z). Read as UTC it would be November.
    feed.add_batch([urow("WELC11", "000123", "2026-10-31 23:30:00")])
    sync(w, feed)
    octo = preview(w.db, month="202610", now=datetime(2026, 11, 2, tzinfo=timezone.utc))
    nov = preview(w.db, month="202611", now=datetime(2026, 11, 2, tzinfo=timezone.utc))
    assert octo["totals"]["historical_would_qualify"] == 1 and nov["totals"]["historical_would_qualify"] == 0
    obs = aq.parse_source_row(w.db[usync.ROWS_COLLECTION].find_one(), DATABOT_CONFIG)
    assert obs["redeemed_at"] == datetime(2026, 10, 31, 15, 30, tzinfo=timezone.utc)
    doc = aq.build_evidence_doc(source=aq.SOURCE_DATABOT_UIM, recipient={}, now_utc=NOW,
                                **{k: v for k, v in obs.items() if k != "code"}, code="WELC11")
    assert doc["account_key"] == "advantplay:000123"
    # An explicit-offset source time is taken as the exact instant.
    exact = aq.parse_source_row(urow("C", "a", "2026-10-31 15:30:00", basis="source_offset"), DATABOT_CONFIG)
    assert exact["redeemed_at"] == datetime(2026, 10, 31, 15, 30, tzinfo=timezone.utc)
    # Attested legacy rows are read as naive GMT+8 wall clock; unattested ones go to review.
    legacy = urow("C", "a", "2026-10-31 23:30:00", basis="unknown_legacy")
    assert aq.parse_source_row(legacy, DATABOT_CONFIG)["redeemed_at"] is None
    attested = aq.parse_source_row(legacy, {**DATABOT_CONFIG, "legacy_time_basis": aq.LEGACY_TIME_NAIVE})
    assert attested["redeemed_at"] == datetime(2026, 10, 31, 15, 30, tzinfo=timezone.utc)


def test_seeding_and_live_use_the_same_account_identity():
    w = disabled_world()
    w.welcome_code(11, "WELC11")
    feed = FakeFeed()
    feed.add_batch([urow("WELC11", "000123", "2026-10-05 10:00:00")])
    sync(w, feed)
    by_code = migration._redemptions_by_code(w.db, DATABOT_CONFIG, migration._welcome_codes(w.db))
    assert by_code == {"WELC11": {"advantplay:000123"}}


# 7 — corrections, rollback, outage ----------------------------------------------

def test_databot_rollback_is_propagated_through_the_void_policy():
    w = disabled_world()
    w.join(1, 11)
    w.welcome_code(11, "WELC11")
    feed = FakeFeed()
    b1 = feed.add_batch([urow("WELC11", "a1", "2026-10-05 10:00:00")])
    feed.add_batch([urow("OTHER", "zz", "2026-10-06 10:00:00")])  # an unrelated batch stays committed
    sync(w, feed)
    assert preview(w.db)["totals"]["historical_would_qualify"] == 1
    feed.batch(b1)["status"] = "rolled_back"
    out = sync(w, feed, now=NOW + timedelta(minutes=5))
    assert out["rollbacks_propagated"] == 1
    local = w.db[usync.BATCHES_COLLECTION].find_one({"_id": b1})
    assert local["rolled_back_at"] and local["status"] == "rolled_back"
    assert w.db[usync.ROWS_COLLECTION].count_documents({}) == 1  # kept for audit, never deleted
    assert preview(w.db, now=NOW + timedelta(minutes=5))["totals"]["historical_would_qualify"] == 0
    assert sync(w, feed, now=NOW + timedelta(minutes=10))["rollbacks_propagated"] == 0  # once only


def test_rollback_after_live_qualification_flags_but_never_revokes(monkeypatch):
    w = disabled_world()
    w.join(1, 11)
    w.welcome_code(11, "WELC11")
    feed = FakeFeed()
    b1 = feed.add_batch([urow("WELC11", "a1", "2026-10-05 10:00:00")])
    sync(w, feed)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID},
                                           {"$set": {"mode": aq.MODE_ACTIVE, "launch_cutoff_utc": CUTOFF}})
    run(w, monkeypatch)
    assert w.db.qualified_events.count_documents({"rule_version": aq.RULE_VERSION}) == 1
    feed.batch(b1)["status"] = "rolled_back"
    sync(w, feed, now=NOW + timedelta(minutes=5))
    q = w.db.qualified_events.find_one({"rule_version": aq.RULE_VERSION})
    assert q["evidence_status"] == "voided_pending_reconciliation"
    assert w.db[aq.REGISTRY_COLLECTION].find_one({"account_key": "advantplay:a1"})["state"] == aq.REG_COMMITTED


def test_stale_deletion_routes_to_review():
    w = disabled_world()
    w.join(1, 11)
    w.welcome_code(11, "WELC11")
    feed = FakeFeed()
    feed.add_batch([urow("WELC11", "a1", "2026-10-05 10:00:00")])
    feed.add_batch([urow("WELC11", "a1", "2026-10-05 10:00:00", observation="deleted")])
    sync(w, feed)
    out = preview(w.db)
    assert out["totals"]["historical_would_qualify"] == 0
    assert out["outcome_reasons"] == {"pending_review:source_row_removed_by_later_import": 1}


def test_outage_shows_unavailable_or_stale_never_zero():
    w = disabled_world()
    w.join(1, 11)
    w.welcome_code(11, "WELC11")
    feed = FakeFeed()
    feed.add_batch([urow("WELC11", "a1", "2026-10-05 10:00:00")])
    feed.fail_after = 0
    sync(w, feed)
    out = preview(w.db)
    assert out["source"]["blocked"] is True
    assert "evidence_sync_never_succeeded" in out["source"]["problems"]
    assert all(r["historical"] is None and r["at_launch"] is None for r in out["affiliates"])
    feed.fail_after = None
    sync(w, feed)
    feed.fail_after = 0
    later = NOW + timedelta(hours=3)
    sync(w, feed, now=later)
    out = preview(w.db, now=later)
    assert out["source"]["blocked"] is False
    assert out["source"]["evidence_incomplete"] == ["evidence_sync_stale"]
    assert out["source"]["sync"]["stale"] is True and out["source"]["sync"]["last_error"].startswith("feed_http_503")
    assert out["totals"]["historical_would_qualify"] == 1  # last known evidence, flagged stale
    # Preflight (activation gate) refuses stale evidence.
    assert "evidence_sync_stale" in aq.integration_readiness(w.db, DATABOT_CONFIG, now_utc=later)["problems"]


def test_sync_disabled_by_default(monkeypatch):
    monkeypatch.delenv("UIM_REDEMPTION_SYNC_ENABLED", raising=False)
    w = disabled_world()
    assert usync.run_scheduled_sync(w.db) == {"skipped": "sync_disabled"}
    assert w.db[usync.STATE_COLLECTION].count_documents({}) == 0


# 8 — preview == live validator ----------------------------------------------------

def test_preview_matches_live_validator_on_databot_evidence(monkeypatch):
    w = disabled_world()
    for invitee in (11, 12, 13, 14, 15):
        w.join(1, invitee)
        w.welcome_code(invitee, f"WELC{invitee}")
    w.join(2, 21)
    w.welcome_code(21, "WELC21")
    w.user(14, linked=["someOtherAcct"], created=datetime(2026, 10, 2, tzinfo=timezone.utc))
    feed = FakeFeed()
    feed.add_batch([
        urow("WELC11", "acctA", "2026-10-05 10:00:00"),
        urow("WELC12", "acctA", "2026-10-05 11:00:00"),                        # duplicate account
        urow("WELC13", "acct13", "2026-10-05 10:00:00", basis="unknown_legacy"),  # review
        urow("WELC14", "acct14", "2026-10-05 10:00:00"),                       # linkage conflict -> review
        urow("WELC21", "000777", "2026-10-06 10:00:00"),
    ])
    sync(w, feed)
    w.db[aq.CONTROL_COLLECTION].update_one({"_id": aq.CONTROL_ID},
                                           {"$set": {"mode": aq.MODE_ACTIVE, "launch_cutoff_utc": CUTOFF}})
    shadow = aqp.build_preview(w.db, month="202610", now_utc=NOW)
    run(w, monkeypatch)
    live = Counter(str(q["referrer_id"]) for q in w.db.qualified_events.find({"rule_version": aq.RULE_VERSION}))
    assert {r["referrer_id"]: r["at_launch"]["eligible"] for r in shadow["affiliates"]
            if r["at_launch"]["eligible"]} == dict(live) == {"1": 1, "2": 1}
    statuses = Counter(e["status"] for e in w.db[aq.EVIDENCE_COLLECTION].find())
    assert statuses[aq.EV_DUPLICATE_ACCOUNT] == shadow["totals"]["at_launch_duplicate_account_excluded"] == 1
    assert statuses[aq.EV_REVIEW] == shadow["totals"]["at_launch_review"] == 2
    keys = {q["account_key"] for q in w.db.qualified_events.find({"rule_version": aq.RULE_VERSION})}
    assert keys == {"advantplay:acctA", "advantplay:000777"}


# 9 — configuration never activates anything ------------------------------------------

def test_configure_databot_source_keeps_rule_disabled(capsys):
    db = mongomock.MongoClient().db
    argv = ["configure-databot-source", "--account-namespace", "advantplay", "--attested-by", "Data owner",
            "--attestation-ref", "OPS-1234", "--commit"]
    assert admin_cli.main(argv, db_factory=lambda: db, now_fn=lambda: NOW) == 0
    control = aq.get_control(db)
    assert control["mode"] == aq.MODE_DISABLED and "launch_cutoff_utc" not in control
    assert aq.validate_source_config(control["source_config"]) == []
    assert control["source_config"]["success_attestation"]["reference"] == "OPS-1234"
    assert not aq.new_rule_active(control, NOW) and aq.legacy_award_allowed(control, NOW)
