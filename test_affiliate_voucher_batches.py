"""Tests for monthly scheduled affiliate voucher batches.

Covers: active-window claim gating (future/active/expired/disabled),
adjacent vs overlapping batch windows, cross-tier independence, exact
boundary semantics (inclusive start / exclusive end), legacy undated
voucher backward compatibility, duplicate-safe/atomic batch uploads,
concurrency-safe claiming, dashboard status derivation, KL/UTC timezone
conversion, the admin HTTP layer (including auth), and frontend error-code
mapping / form-state preservation on failure.
"""

from datetime import datetime, timezone

import pytest
from flask import Flask

import fake_mongo
import affiliate_rewards as ar
import affiliate_voucher_batches as avb


def _db():
    return fake_mongo.FakeDb({"voucher_pools": [("pool_id", "code")]})


def _create(db, *, pool_id="T1", name="Batch", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00",
            codes=None, now=None, notes=None):
    return avb.create_batch(
        db,
        admin_identity="admin1",
        batch_name=name,
        pool_id=pool_id,
        starts_at_local=starts,
        ends_at_local=ends,
        timezone_name="Asia/Kuala_Lumpur",
        codes=codes if codes is not None else ["A1", "A2", "A3"],
        notes=notes,
        now_utc=now or datetime(2026, 7, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Active-window claim gating
# ---------------------------------------------------------------------------

class TestActiveWindowClaiming:
    def test_future_batch_cannot_distribute(self):
        db = _db()
        _create(db)
        before_window = datetime(2026, 7, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=before_window)
        assert voucher is None

    def test_active_batch_distributes(self):
        db = _db()
        _create(db)
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is not None
        assert voucher["code"] in ("A1", "A2", "A3")

    def test_expired_batch_cannot_distribute(self):
        db = _db()
        _create(db)
        after_window = datetime(2026, 9, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=after_window)
        assert voucher is None

    def test_disabled_batch_cannot_distribute(self):
        db = _db()
        res = _create(db)
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        avb.set_batch_distribution_disabled(
            db, res["batch"]["batch_id"], admin_identity="admin1", disabled=True, now_utc=during_window
        )
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is None

    def test_reenabled_batch_distributes_again(self):
        db = _db()
        res = _create(db)
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        avb.set_batch_distribution_disabled(
            db, res["batch"]["batch_id"], admin_identity="admin1", disabled=True, now_utc=during_window
        )
        avb.set_batch_distribution_disabled(
            db, res["batch"]["batch_id"], admin_identity="admin1", disabled=False, now_utc=during_window
        )
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is not None

    def test_exact_boundary_at_batch_start_is_inclusive(self):
        db = _db()
        _create(db)
        at_start = datetime(2026, 7, 31, 16, 0, 0, tzinfo=timezone.utc)  # 2026-08-01 00:00 KL
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=at_start)
        assert voucher is not None

    def test_exact_boundary_at_batch_end_is_exclusive(self):
        db = _db()
        _create(db)
        at_end = datetime(2026, 8, 31, 16, 0, 0, tzinfo=timezone.utc)  # 2026-09-01 00:00 KL == ends_at
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=at_end)
        assert voucher is None

    def test_issued_vouchers_remain_visible_after_expiry(self):
        db = _db()
        res = _create(db)
        batch_id = res["batch"]["batch_id"]
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is not None

        after_window = datetime(2026, 9, 15, tzinfo=timezone.utc)
        detail = avb.get_batch_detail(db, batch_id, now_utc=after_window)
        assert detail["status"] == "expired"
        issued_rows = [v for v in detail["vouchers"] if v["status"] == "issued"]
        assert len(issued_rows) == 1
        assert issued_rows[0]["code"] == voucher["code"]

    def test_legacy_undated_voucher_keeps_old_behaviour(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY1", "status": "available"})
        # No batch was ever created for T1 here — this row predates the
        # feature (no batch_id/starts_at/ends_at) and must stay claimable
        # regardless of "now".
        far_past = datetime(2000, 1, 1, tzinfo=timezone.utc)
        far_future = datetime(2100, 1, 1, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=far_past)
        assert voucher is not None
        assert voucher["code"] == "LEGACY1"

        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY2", "status": "available"})
        voucher2 = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L2", user_id=2, now_utc=far_future)
        assert voucher2 is not None
        assert voucher2["code"] == "LEGACY2"

    def test_two_workers_cannot_claim_same_code(self):
        db = _db()
        _create(db, codes=["ONLYCODE"])
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        first = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        second = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L2", user_id=2, now_utc=during_window)
        assert first is not None
        assert first["code"] == "ONLYCODE"
        assert second is None


# ---------------------------------------------------------------------------
# Overlap protection
# ---------------------------------------------------------------------------

class TestOverlapProtection:
    def test_adjacent_batches_are_accepted(self):
        db = _db()
        res1 = _create(db, name="Aug", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00")
        res2 = _create(db, name="Sept", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["B1"])
        assert res1["ok"] is True
        assert res2["ok"] is True

    def test_overlapping_same_tier_batches_are_rejected(self):
        db = _db()
        res1 = _create(db, name="Aug", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00")
        res2 = _create(db, name="Aug overlap", starts="2026-08-20 00:00:00", ends="2026-09-20 00:00:00", codes=["B1"])
        assert res1["ok"] is True
        assert res2["ok"] is False
        assert res2["code"] == "batch_window_overlap"
        assert res2["conflicting_batch_id"] == res1["batch"]["batch_id"]

    def test_different_tiers_may_have_overlapping_dates(self):
        db = _db()
        res1 = _create(db, pool_id="T1", name="T1 Aug", codes=["A1"])
        res2 = _create(db, pool_id="T2", name="T2 Aug", codes=["B1"])
        assert res1["ok"] is True
        assert res2["ok"] is True


# ---------------------------------------------------------------------------
# Duplicate-safe / atomic uploads
# ---------------------------------------------------------------------------

class TestBatchUpload:
    def test_duplicate_upload_does_not_create_duplicate_codes(self):
        db = _db()
        res1 = _create(db, name="First", codes=["A1", "A2"])
        assert res1["ok"] is True
        res2 = _create(db, name="Second", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["A1", "A3"])
        assert res2["ok"] is True
        assert res2["counts"]["duplicates"] == 1
        assert res2["counts"]["inserted"] == 1
        assert db.voucher_pools.count_documents({"pool_id": "T1", "code": "A1"}) == 1

    def test_duplicates_within_single_upload_are_counted(self):
        db = _db()
        res = _create(db, codes=["A1", "A1", "A2"])
        assert res["ok"] is True
        assert res["counts"]["duplicates"] == 1
        assert res["counts"]["inserted"] == 2

    def test_failed_upload_is_recoverable_no_orphan_batch(self):
        db = _db()
        res = _create(db, codes=["   ", "\n", ","])
        assert res["ok"] is False
        assert res["code"] == "no_codes"
        assert db.affiliate_voucher_batches.count_documents({}) == 0

    def test_zero_inserted_all_duplicates_cleans_up_batch_doc(self):
        db = _db()
        _create(db, name="First", codes=["A1"])
        res2 = _create(db, name="Second", starts="2026-09-01 00:00:00", ends="2026-10-01 00:00:00", codes=["A1"])
        assert res2["ok"] is False
        assert res2["code"] == "duplicate_codes"
        # Only the first batch document should remain — the failed second
        # upload must not leave a batch with zero inserted vouchers behind.
        assert db.affiliate_voucher_batches.count_documents({}) == 1

    def test_invalid_pool_id_rejected(self):
        db = _db()
        res = _create(db, pool_id="T5", codes=["A1"])
        assert res["ok"] is False
        assert res["code"] == "invalid_pool_id"

    def test_end_before_start_rejected(self):
        db = _db()
        res = _create(db, starts="2026-09-01 00:00:00", ends="2026-08-01 00:00:00")
        assert res["ok"] is False
        assert res["code"] == "end_before_start"


# ---------------------------------------------------------------------------
# Dashboard status derivation / listing
# ---------------------------------------------------------------------------

class TestListingAndStatus:
    def test_dashboard_list_correctly_derives_statuses(self):
        db = _db()
        _create(db, pool_id="T1", name="Scheduled", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        _create(db, pool_id="T2", name="Active", starts="2026-07-01 00:00:00", ends="2026-08-01 00:00:00", codes=["B1"])
        _create(db, pool_id="T3", name="Expired", starts="2026-05-01 00:00:00", ends="2026-06-01 00:00:00", codes=["C1"])

        now = datetime(2026, 7, 15, tzinfo=timezone.utc)
        result = avb.list_batches(db, now_utc=now)
        by_name = {item["batch_name"]: item["status"] for item in result["items"]}
        assert by_name["Scheduled"] == "scheduled"
        assert by_name["Active"] == "active"
        # Expired excluded by default (include_expired=False)
        assert "Expired" not in by_name

        result_all = avb.list_batches(db, include_expired=True, now_utc=now)
        by_name_all = {item["batch_name"]: item["status"] for item in result_all["items"]}
        assert by_name_all["Expired"] == "expired"

    def test_exhausted_status_when_active_but_no_available_codes(self):
        db = _db()
        res = _create(db, codes=["ONLY1"])
        now = datetime(2026, 8, 15, tzinfo=timezone.utc)
        ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=now)
        result = avb.list_batches(db, pool_id="T1", now_utc=now)
        assert result["items"][0]["status"] == "exhausted"
        assert res["batch"]["batch_id"] == result["items"][0]["batch_id"]

    def test_sort_order_active_scheduled_exhausted_expired(self):
        db = _db()
        _create(db, pool_id="T1", name="Scheduled-far", starts="2026-10-01 00:00:00", ends="2026-11-01 00:00:00", codes=["A1"])
        _create(db, pool_id="T2", name="Scheduled-near", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["B1"])
        _create(db, pool_id="T3", name="Active", starts="2026-07-01 00:00:00", ends="2026-08-15 00:00:00", codes=["C1"])
        _create(db, pool_id="T4", name="Expired-old", starts="2026-01-01 00:00:00", ends="2026-02-01 00:00:00", codes=["D1"])

        now = datetime(2026, 7, 15, tzinfo=timezone.utc)
        result = avb.list_batches(db, include_expired=True, now_utc=now)
        names = [item["batch_name"] for item in result["items"]]
        assert names == ["Active", "Scheduled-near", "Scheduled-far", "Expired-old"]

    def test_legacy_summary_reported_separately_from_batches(self):
        db = _db()
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY1", "status": "available"})
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "LEGACY2", "status": "issued"})
        result = avb.list_batches(db, pool_id="T1")
        assert result["legacy_summary"] == [{"pool_id": "T1", "available": 1, "issued": 1, "total": 2}]


# ---------------------------------------------------------------------------
# Update / edit restrictions
# ---------------------------------------------------------------------------

class TestUpdateBatch:
    def test_editing_notes_and_name_always_allowed(self):
        db = _db()
        res = _create(db)
        out = avb.update_batch(db, res["batch"]["batch_id"], admin_identity="admin1",
                                updates={"batch_name": "Renamed", "notes": "hi"})
        assert out["ok"] is True
        assert out["batch"]["batch_name"] == "Renamed"
        assert out["batch"]["notes"] == "hi"

    def test_date_edit_blocked_once_a_voucher_is_issued(self):
        db = _db()
        res = _create(db)
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        out = avb.update_batch(
            db, res["batch"]["batch_id"], admin_identity="admin1",
            updates={"starts_at_local": "2026-08-05 00:00:00"},
        )
        assert out["ok"] is False
        assert out["code"] == "active_batch_edit_restricted"

    def test_date_edit_allowed_before_any_issuance_and_rows_follow(self):
        db = _db()
        res = _create(db)
        batch_id = res["batch"]["batch_id"]
        out = avb.update_batch(
            db, batch_id, admin_identity="admin1",
            updates={"starts_at_local": "2026-08-05 00:00:00", "ends_at_local": "2026-09-05 00:00:00"},
        )
        assert out["ok"] is True
        row = db.voucher_pools.find_one({"pool_id": "T1", "code": "A1"})
        assert row["starts_at"] == avb.parse_kl_local_to_utc("2026-08-05 00:00:00")

    def test_date_edit_rejects_new_overlap(self):
        db = _db()
        res1 = _create(db, name="Aug", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00")
        res2 = _create(db, name="Oct", starts="2026-10-01 00:00:00", ends="2026-11-01 00:00:00", codes=["Z1"])
        out = avb.update_batch(
            db, res2["batch"]["batch_id"], admin_identity="admin1",
            updates={"starts_at_local": "2026-08-15 00:00:00"},
        )
        assert out["ok"] is False
        assert out["code"] == "batch_window_overlap"

    def test_batch_not_found(self):
        db = _db()
        out = avb.update_batch(db, "000000000000000000000000", admin_identity="admin1", updates={"notes": "x"})
        assert out["ok"] is False
        assert out["code"] == "batch_not_found"


# ---------------------------------------------------------------------------
# Timezone conversion
# ---------------------------------------------------------------------------

class TestTimezoneConversion:
    def test_kl_to_utc_offset(self):
        utc_dt = avb.parse_kl_local_to_utc("2026-08-01 00:00:00")
        assert utc_dt == datetime(2026, 7, 31, 16, 0, 0, tzinfo=timezone.utc)

    def test_utc_round_trips_back_to_kl(self):
        utc_dt = avb.parse_kl_local_to_utc("2026-08-01 12:30:00")
        kl_iso = avb._to_kl_iso(utc_dt)
        assert kl_iso.startswith("2026-08-01T12:30:00+08:00")

    def test_invalid_datetime_returns_none(self):
        assert avb.parse_kl_local_to_utc("not-a-date") is None
        assert avb.parse_kl_local_to_utc("") is None


# ---------------------------------------------------------------------------
# Admin HTTP layer (including authentication)
# ---------------------------------------------------------------------------

def _app_with_auth(db, *, authorized=True):
    app = Flask(__name__)
    bp = avb.register_routes(
        lambda: (True, None) if authorized else (False, ("Admins only", 403)),
        lambda: "admin1",
        lambda: db,
    )
    app.register_blueprint(bp)
    return app


class TestAdminHttpLayer:
    def test_unauthenticated_request_rejected(self):
        db = _db()
        app = _app_with_auth(db, authorized=False)
        client = app.test_client()
        resp = client.get("/api/admin/affiliate-voucher-batches")
        assert resp.status_code == 403
        body = resp.get_json()
        assert body["ok"] is False
        assert body["code"] == "unauthorized"

    def test_create_and_list_round_trip(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "Affiliate August 2026 - T1",
                "pool_id": "T1",
                "starts_at_local": "2026-08-01 00:00:00",
                "ends_at_local": "2026-09-01 00:00:00",
                "timezone": "Asia/Kuala_Lumpur",
                "codes": ["ABC123", "ABC124"],
                "notes": "",
            },
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["counts"]["inserted"] == 2

        list_resp = client.get("/api/admin/affiliate-voucher-batches?pool_id=T1")
        assert list_resp.status_code == 200
        list_body = list_resp.get_json()
        assert len(list_body["items"]) == 1
        assert list_body["items"][0]["pool_id"] == "T1"

    def test_overlap_returns_409(self):
        db = _db()
        _create(db)
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "Overlap",
                "pool_id": "T1",
                "starts_at_local": "2026-08-15 00:00:00",
                "ends_at_local": "2026-09-15 00:00:00",
                "codes": ["X1"],
            },
        )
        assert resp.status_code == 409
        assert resp.get_json()["code"] == "batch_window_overlap"

    def test_patch_disable_then_detail_reflects_disabled(self):
        db = _db()
        res = _create(db)
        batch_id = res["batch"]["batch_id"]
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.patch(
            f"/api/admin/affiliate-voucher-batches/{batch_id}",
            json={"distribution_disabled": True},
        )
        assert resp.status_code == 200
        detail = client.get(f"/api/admin/affiliate-voucher-batches/{batch_id}").get_json()
        assert detail["batch"]["distribution_disabled"] is True

    def test_get_unknown_batch_returns_404(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.get("/api/admin/affiliate-voucher-batches/000000000000000000000000")
        assert resp.status_code == 404
        assert resp.get_json()["code"] == "batch_not_found"


# ---------------------------------------------------------------------------
# Frontend: error-code mapping / preserved form state on failure
# ---------------------------------------------------------------------------

class TestFrontendErrorMapping:
    """The dashboard JS (static/admin-dashboard.js) maps every documented
    backend error code to a specific, actionable message via
    AFF_BATCH_ERROR_MESSAGES and never falls back to a generic
    "Something went wrong" for a *known* code. It also keeps all typed
    form fields intact on failure (abResetForm() is only ever called after
    a *successful* submit). These are pure-JS string/DOM assertions, so we
    parse the source directly rather than spin up a browser.
    """

    @staticmethod
    def _source():
        with open("static/admin-dashboard.js", encoding="utf-8") as fh:
            return fh.read()

    def test_all_documented_error_codes_are_mapped(self):
        src = self._source()
        start = src.index("var AFF_BATCH_ERROR_MESSAGES = {")
        end = src.index("};", start)
        block = src[start:end]
        required_codes = [
            "batch_window_overlap", "invalid_start_at", "invalid_end_at", "end_before_start",
            "invalid_pool_id", "no_codes", "duplicate_codes", "unauthorized",
            "batch_not_found", "active_batch_edit_restricted",
        ]
        for code in required_codes:
            assert code in block, f"missing frontend mapping for {code}"

    def test_abErrorMessage_never_falls_back_to_generic_for_known_code(self):
        src = self._source()
        assert 'abErrorMessage(d)' in src
        # abResetForm (which clears every typed field) must only be called
        # on the success path inside abSubmit, never on the error branch.
        submit_start = src.index("function abSubmit()")
        submit_end = src.index("function abSetDisabled", submit_start)
        submit_body = src[submit_start:submit_end]
        error_branch = submit_body[submit_body.index("if (!res.ok"):submit_body.index("toast(isEdit")]
        assert "abResetForm" not in error_branch
        assert "abShowFormError" in error_branch

    def test_submit_button_disabled_and_relabelled_while_in_flight(self):
        src = self._source()
        submit_start = src.index("function abSubmit()")
        submit_end = src.index("function abSetDisabled", submit_start)
        submit_body = src[submit_start:submit_end]
        assert "btnStart(btn" in submit_body
        assert "btnStop(btn)" in submit_body


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
