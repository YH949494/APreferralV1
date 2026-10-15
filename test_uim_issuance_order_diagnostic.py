"""Read-only diagnostic for 'redeemed before issued' records: gap histogram + clock-basis hypotheses."""
from __future__ import annotations

import json
from datetime import timedelta

import affiliate_qualification as aq
import uim_issuance_order_diagnostic as dio
from scripts import affiliate_qualification_admin as admin_cli
from test_affiliate_welcome_redemption_qualification import NOW, _snapshot  # noqa: F401
from test_uim_evidence_supersession import FINAL, final_cov, row, world  # noqa: F401
from test_uim_redemption_sync import DATABOT_CONFIG, FakeFeed, sync

from datetime import datetime

R = datetime(2026, 10, 3, 4, 0)           # WALL 12:00 GMT+8 -> 04:00Z


def _pool(w, uid, code, issued_at=None):
    w.db.voucher_pools.insert_one({"pool_id": aq.WELCOME_POOL_ID, "code": code, "status": "issued",
                                   "issued_to_user_id": uid, "issued_at": issued_at})


def _scenario():
    w = world(3, active=True)
    w.db.voucher_pools.delete_many({})
    w.db.affiliate_ledger.delete_many({})
    # A: claimed_at is KL wall clock stored as UTC (8h late): true claim R-3h, stored R+5h
    _pool(w, 11, "WELC11")
    w.db.new_joiner_claims.insert_one({"uid": 11, "code": "WELC11", "claimed_at": R + timedelta(hours=5)})
    # B: genuinely issued two days after the redemption
    _pool(w, 12, "WELC12", R + timedelta(days=2))
    # C: issued before the redemption - fine
    _pool(w, 13, "WELC13", R - timedelta(hours=1))
    feed = FakeFeed()
    feed.add_batch([row(code=f"WELC{i}", account=f"a{i}") for i in (11, 12, 13)], coverage=final_cov())
    sync(w, feed)
    return w


def test_reports_gap_source_and_which_hypothesis_clears_which_records():
    w = _scenario()
    before = _snapshot(w.db)
    out = dio.diagnose(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert _snapshot(w.db) == before                                    # read-only
    assert out["ok"] and out["records_with_redemption_time"] == 3 and out["before_issuance"] == 2
    assert out["gap_issued_minus_redeemed"]["4-8h"] == 1 and out["gap_issued_minus_redeemed"]["1-7d"] == 1
    assert out["earliest_issuance_source"] == {"new_joiner_claims.claimed_at": 1, "voucher_pools.issued_at": 1}
    assert out["would_clear_if"] == {"claim_time_is_kl_wall_clock_stored_as_utc": 1,
                                     "all_issuance_times_are_kl_wall_clock_stored_as_utc": 1,
                                     "redemption_time_is_utc_not_kl": 1}
    gaps = sorted(s["gap_hours"] for s in out["samples"])
    assert gaps == [5.0, 48.0] and all("account" in s and "WELC" not in json.dumps(s) for s in out["samples"])


def test_genuine_gaps_clear_under_no_hypothesis():
    w = world(1, active=True)
    w.db.voucher_pools.delete_many({})
    w.db.affiliate_ledger.delete_many({})
    _pool(w, 11, "WELC11", R + timedelta(days=3))
    feed = FakeFeed()
    feed.add_batch([row()], coverage=final_cov())
    sync(w, feed)
    out = dio.diagnose(w.db, config=DATABOT_CONFIG, now_utc=NOW)
    assert out["before_issuance"] == 1 and set(out["would_clear_if"].values()) == {0}


def test_refuses_non_uim_source_and_scan_cap():
    w = _scenario()
    assert dio.diagnose(w.db, config={"source": "marketing_raw_data"}, now_utc=NOW)["error"] == "source_is_not_databot_uim"
    capped = dio.diagnose(w.db, config=DATABOT_CONFIG, now_utc=NOW, max_scan_rows=1)
    assert capped["ok"] is False and capped["error"] == "scan_exceeds_max_scan_rows"


def test_cli_is_read_only_and_exits_zero(capsys):
    w = _scenario()
    before = _snapshot(w.db)
    rc = admin_cli.main(["diagnose-issuance-order"], db_factory=lambda: (_ for _ in ()).throw(AssertionError("write db")),
                        read_only_db_factory=lambda: w.db, now_fn=lambda: NOW)
    assert rc == 0 and json.loads(capsys.readouterr().out)["before_issuance"] == 2 and _snapshot(w.db) == before
