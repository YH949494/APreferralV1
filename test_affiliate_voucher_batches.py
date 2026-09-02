"""Tests for monthly scheduled affiliate voucher batches.

Covers: active-window claim gating (future/active/expired/disabled),
adjacent vs overlapping batch windows, cross-tier independence, exact
boundary semantics (inclusive start / exclusive end), legacy undated
voucher backward compatibility, duplicate-safe/atomic batch uploads,
concurrency-safe claiming, dashboard status derivation, KL/UTC timezone
conversion, the admin HTTP layer (including auth), and frontend error-code
mapping / form-state preservation on failure.
"""

import threading
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
        res = _create(db, pool_id="NOT_A_POOL", codes=["A1"])
        assert res["ok"] is False
        assert res["code"] == "invalid_pool_id"

    def test_t5_pool_id_is_schedulable(self):
        # T5 used to be the one tier that could not take a scheduled batch
        # (BATCH_POOL_IDS omitted it), leaving T5 entitlements permanently
        # dependent on undated legacy uploads.
        db = _db()
        res = _create(db, pool_id="T5", codes=["A1"])
        assert res["ok"] is True
        assert db.voucher_pools.count_documents({"pool_id": "T5"}) == 1

    def test_denomination_pool_rows_carry_voucher_value(self):
        # Denomination-pool rows must be independently priceable: one pool
        # serves several tiers, so the value cannot be inferred from a tier.
        db = _db()
        res = _create(db, pool_id="AFFILIATE_10", codes=["D1", "D2"])
        assert res["ok"] is True
        rows = list(db.voucher_pools.find({"pool_id": "AFFILIATE_10"}))
        assert len(rows) == 2
        assert all(r["voucher_value"] == 10 for r in rows)

    def test_legacy_tier_pool_rows_have_no_voucher_value(self):
        # Per-tier legacy pools are untouched: their value stays a property
        # of the tier (read from the legacy plan), never stamped on the row.
        db = _db()
        res = _create(db, pool_id="T1", codes=["L1"])
        assert res["ok"] is True
        row = db.voucher_pools.find_one({"pool_id": "T1"})
        assert "voucher_value" not in row

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
                "entitlement_month": "202608",
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
                "entitlement_month": "202608",
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
            "upload_failed", "target_batch_failed_cannot_enable",
            "batch_disabled", "batch_not_ready", "database_error", "batch_expired",
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


# ---------------------------------------------------------------------------
# Regression coverage for code-review findings
# ---------------------------------------------------------------------------

class TestNaiveDatetimeFromRealMongo:
    """``database.py`` opens ``MongoClient`` without ``tz_aware=True``, so a
    real (non-fake) MongoDB hands back naive datetimes for every stored
    field while ``now_utc`` stays aware. Every comparison/conversion must
    survive that without raising ``TypeError: can't compare offset-naive
    and offset-aware datetimes``.
    """

    def _naive(self, dt):
        return dt.replace(tzinfo=None)

    def test_derive_batch_status_handles_naive_stored_datetimes(self):
        aware_now = datetime(2026, 8, 15, tzinfo=timezone.utc)
        batch = {
            "starts_at": self._naive(datetime(2026, 8, 1, tzinfo=timezone.utc)),
            "ends_at": self._naive(datetime(2026, 9, 1, tzinfo=timezone.utc)),
            "available_count": 3,
        }
        assert avb.derive_batch_status(batch, aware_now) == "active"

    def test_claim_handles_naive_stored_window_fields(self):
        db = _db()
        res = _create(db, codes=["A1"])
        # Simulate what a real (non-tz_aware) MongoClient round-trip would
        # hand back: naive datetimes on the stored row.
        db.voucher_pools.update_one(
            {"pool_id": "T1", "code": "A1"},
            {"$set": {
                "starts_at": self._naive(avb.parse_kl_local_to_utc("2026-08-01 00:00:00")),
                "ends_at": self._naive(avb.parse_kl_local_to_utc("2026-09-01 00:00:00")),
            }},
        )
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is not None
        assert voucher["code"] == "A1"

    def test_kl_iso_conversion_treats_naive_value_as_utc(self):
        naive = self._naive(datetime(2026, 8, 1, tzinfo=timezone.utc))
        assert avb._to_kl_iso(naive) == "2026-08-01T08:00:00+08:00"
        assert avb._to_utc_iso(naive) == "2026-08-01T00:00:00+00:00"

    def test_list_batches_survives_naive_datetimes(self):
        db = _db()
        _create(db)
        for doc in db.affiliate_voucher_batches.find({}):
            db.affiliate_voucher_batches.update_one(
                {"_id": doc["_id"]},
                {"$set": {"starts_at": self._naive(doc["starts_at"]), "ends_at": self._naive(doc["ends_at"])}},
            )
        result = avb.list_batches(db, now_utc=datetime(2026, 8, 15, tzinfo=timezone.utc))
        assert result["ok"] is True


class TestDisableRaceInAtomicClaim:
    def test_stale_row_flag_is_never_trusted_when_batch_doc_is_disabled(self):
        """The batch document, not the denormalized voucher-row field, is
        authoritative for distribution_disabled. Even if a row incorrectly
        still says distribution_disabled=False (e.g. a missed/partial
        denormalization write), it must not be claimable once the batch
        document itself is disabled.
        """
        db = _db()
        res = _create(db, codes=["ONLYCODE"])
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)

        # Disable only the batch document directly — deliberately bypassing
        # set_batch_distribution_disabled() so the voucher_pools row keeps
        # its stale distribution_disabled=False.
        db.affiliate_voucher_batches.update_one(
            {"_id": avb._as_object_id(res["batch"]["batch_id"])},
            {"$set": {"distribution_disabled": True}},
        )
        row = db.voucher_pools.find_one({"code": "ONLYCODE"})
        assert row["distribution_disabled"] is False

        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)

        assert voucher is None
        row = db.voucher_pools.find_one({"code": "ONLYCODE"})
        assert row["status"] == "available"  # never flipped to issued

    def test_two_workers_still_cannot_claim_same_code_via_target_batch(self):
        db = _db()
        _create(db, codes=["ONLYCODE"])
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        first = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        second = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L2", user_id=2, now_utc=during_window)
        assert first is not None
        assert second is None

    def test_reenable_restores_eligibility_through_batch_document(self):
        db = _db()
        res = _create(db, codes=["ONLYCODE"])
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        avb.set_batch_distribution_disabled(db, res["batch"]["batch_id"], admin_identity="admin1", disabled=True, now_utc=during_window)
        assert ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window) is None
        avb.set_batch_distribution_disabled(db, res["batch"]["batch_id"], admin_identity="admin1", disabled=False, now_utc=during_window)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is not None


class TestOverlapCreateRace:
    def test_concurrent_creates_deterministically_resolve_to_one_winner(self):
        db = _db()
        real_find_overlap = avb._find_overlapping_batch
        call_count = {"n": 0}

        def racy_find_overlap(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # The pre-insert overlap check for the *second* racing
                # request runs before the first request's batch is visible.
                return None
            return real_find_overlap(*args, **kwargs)

        first = _create(db, name="Winner", starts="2026-08-01 00:00:00", ends="2026-09-01 00:00:00", codes=["A1"])
        assert first["ok"] is True

        avb._find_overlapping_batch = racy_find_overlap
        try:
            second = _create(db, name="Loser", starts="2026-08-15 00:00:00", ends="2026-09-15 00:00:00", codes=["B1"])
        finally:
            avb._find_overlapping_batch = real_find_overlap

        assert second["ok"] is False
        assert second["code"] == "batch_window_overlap"
        # The loser's batch document and voucher rows must both be gone —
        # no orphaned batch, no orphaned (still-claimable) codes.
        assert db.affiliate_voucher_batches.count_documents({"batch_name": "Loser"}) == 0
        assert db.voucher_pools.count_documents({"code": "B1"}) == 0
        # The winner is untouched.
        assert db.affiliate_voucher_batches.count_documents({"batch_name": "Winner"}) == 1


class TestPartialUploadFailureCleansUpRows:
    def test_non_duplicate_insert_error_marks_batch_failed_and_keeps_rows_non_claimable(self):
        """Risk 3: an interrupted upload (or a process crash resuming mid-
        insert) must be recoverable and auditable, not silently deleted —
        but the partially-inserted rows must never be claimable while the
        batch is in this state.
        """
        db = _db()
        real_insert_one = db.voucher_pools.insert_one
        call_count = {"n": 0}

        def flaky_insert_one(doc):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("simulated transient write failure")
            return real_insert_one(doc)

        db.voucher_pools.insert_one = flaky_insert_one
        try:
            result = _create(db, codes=["A1", "A2", "A3"])
        finally:
            db.voucher_pools.insert_one = real_insert_one

        assert result["ok"] is False
        assert result["code"] == "upload_failed"

        # "A1" survives (audit trail) but the batch — and therefore "A1" —
        # must never be claimable.
        assert db.voucher_pools.count_documents({"code": "A1"}) == 1
        batch = db.affiliate_voucher_batches.find_one({})
        assert batch["upload_status"] == "failed"
        assert avb.derive_batch_status(batch) == "failed"

        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        voucher = ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        assert voucher is None


class TestAddCodesToBatch:
    """+ Add Codes: top up an existing batch without creating a new one,
    touching its schedule/pool, or resetting existing vouchers.
    """

    def test_add_unique_codes_to_active_batch(self):
        db = _db()
        res = _create(db, codes=["A1", "A2"])
        batch_id = res["batch"]["batch_id"]
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1\nB2\nB3")
        assert result["ok"] is True
        assert result["submitted_count"] == 3
        assert result["inserted_count"] == 3
        assert result["duplicate_count"] == 0
        assert result["invalid_count"] == 0
        assert result["available_count"] == 5
        assert result["uploaded_count"] == 5

    def test_existing_codes_remain_untouched(self):
        db = _db()
        res = _create(db, codes=["A1", "A2"])
        batch_id = res["batch"]["batch_id"]
        avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1")
        original = db.voucher_pools.find_one({"code": "A1"})
        assert original is not None
        assert original["status"] == "available"
        assert original["batch_id"] == avb._as_object_id(batch_id)

    def test_codes_attach_to_same_batch_id_and_no_new_batch_created(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        assert db.affiliate_voucher_batches.count_documents({}) == 1
        avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1,B2")
        assert db.affiliate_voucher_batches.count_documents({}) == 1
        new_row = db.voucher_pools.find_one({"code": "B1"})
        assert new_row["batch_id"] == avb._as_object_id(batch_id)
        assert new_row["batch_name"] == res["batch"]["batch_name"]

    def test_added_codes_do_not_appear_as_legacy_undated_inventory(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1")
        legacy = avb._legacy_unbounded_summary(db, pool_id="T1")
        assert legacy == [] or all(l["pool_id"] != "T1" for l in legacy)
        row = db.voucher_pools.find_one({"code": "B1"})
        assert row.get("batch_id") is not None

    def test_mixed_new_and_duplicate_codes_partial_success(self):
        db = _db()
        res = _create(db, codes=["A1", "A2"])
        batch_id = res["batch"]["batch_id"]
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="A1\nB1\nB2")
        assert result["ok"] is True
        assert result["submitted_count"] == 3
        assert result["inserted_count"] == 2
        assert result["duplicate_count"] == 1

    def test_all_duplicates_inserts_zero(self):
        db = _db()
        res = _create(db, codes=["A1", "A2"])
        batch_id = res["batch"]["batch_id"]
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="A1\nA2")
        assert result["ok"] is False
        assert result["code"] == "duplicate_codes"
        assert "already exist" in result["message"]
        assert result["inserted_count"] == 0

    def test_duplicate_entries_within_submitted_payload_handled_safely(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1\nB1\nB2")
        assert result["ok"] is True
        assert result["submitted_count"] == 3
        assert result["inserted_count"] == 2
        assert result["duplicate_count"] == 1
        assert db.voucher_pools.count_documents({"code": "B1"}) == 1

    def test_empty_input_rejected(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="   \n  ")
        assert result["ok"] is False
        assert result["code"] == "no_codes"

    def test_missing_batch_returns_batch_not_found(self):
        db = _db()
        result = avb.add_codes_to_batch(db, "000000000000000000000000", admin_identity="admin1", codes="A1")
        assert result["ok"] is False
        assert result["code"] == "batch_not_found"

    def test_disabled_batch_cannot_be_topped_up(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        avb.set_batch_distribution_disabled(db, batch_id, admin_identity="admin1", disabled=True)
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1")
        assert result["ok"] is False
        assert result["code"] == "batch_disabled"
        assert db.voucher_pools.count_documents({"code": "B1"}) == 0

    def test_two_sequential_topups_cannot_create_duplicate_voucher_rows(self):
        """Simulates two concurrent admins racing to top up the same batch
        with an overlapping code — the DB unique index is the final
        arbiter, so the second submission must see the code as a duplicate
        rather than inserting a second row.
        """
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        first = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="RACE1")
        second = avb.add_codes_to_batch(db, batch_id, admin_identity="admin2", codes="RACE1")
        assert first["ok"] is True
        assert first["inserted_count"] == 1
        assert second["ok"] is False
        assert second["duplicate_count"] == 1
        assert db.voucher_pools.count_documents({"code": "RACE1"}) == 1

    def test_uploaded_and_available_counts_correct_after_topup(self):
        db = _db()
        res = _create(db, codes=["A1", "A2"])
        batch_id = res["batch"]["batch_id"]
        during_window = datetime(2026, 8, 15, tzinfo=timezone.utc)
        ar._claim_voucher_from_pool(db, pool_id="T1", ledger_id="L1", user_id=1, now_utc=during_window)
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1\nB2\nB3", now_utc=during_window)
        assert result["ok"] is True
        # 1 issued + (1 remaining original + 3 new) available = 4 available, 5 uploaded total
        assert result["available_count"] == 4
        assert result["uploaded_count"] == 5
        detail = avb.get_batch_detail(db, batch_id, now_utc=during_window)
        assert detail["available_count"] == 4
        assert detail["uploaded_count"] == 5

    def test_expired_batch_cannot_be_topped_up(self):
        db = _db()
        res = _create(db, codes=["A1"], starts="2026-08-01 00:00:00", ends="2026-08-02 00:00:00")
        batch_id = res["batch"]["batch_id"]
        after_window = datetime(2026, 8, 3, tzinfo=timezone.utc)
        result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1", now_utc=after_window)
        assert result["ok"] is False
        assert result["code"] == "batch_expired"
        assert db.voucher_pools.count_documents({"code": "B1"}) == 0

    def test_database_error_reports_partial_inserted_count_and_keeps_rows(self):
        """A genuine mid-loop write failure (not a duplicate-key conflict)
        must report how many codes landed before the failure — the caller
        must not treat this as a total no-op — and must never roll back or
        reset the codes that already made it in.
        """
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        real_insert_one = db.voucher_pools.insert_one
        call_count = {"n": 0}

        def flaky_insert_one(doc):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("simulated transient write failure")
            return real_insert_one(doc)

        db.voucher_pools.insert_one = flaky_insert_one
        try:
            result = avb.add_codes_to_batch(db, batch_id, admin_identity="admin1", codes="B1\nB2\nB3")
        finally:
            db.voucher_pools.insert_one = real_insert_one

        assert result["ok"] is False
        assert result["code"] == "database_error"
        assert result["inserted_count"] == 1
        assert db.voucher_pools.count_documents({"code": "B1"}) == 1
        assert db.voucher_pools.count_documents({"code": "B2"}) == 0
        assert db.voucher_pools.count_documents({"code": "B3"}) == 0
        # Original code untouched, no second batch created.
        assert db.voucher_pools.find_one({"code": "A1"})["status"] == "available"
        assert db.affiliate_voucher_batches.count_documents({}) == 1

    def test_true_concurrent_threads_race_same_code_exactly_one_wins(self):
        """Genuine multi-threaded race (not a sequential simulation): two
        threads call add_codes_to_batch for the same batch with the same
        new code at (as close to) the same instant, synchronized with a
        barrier. The unique (pool_id, code) index must guarantee exactly
        one insert wins; the loser must report the code as a duplicate,
        no second batch may be created, and the final authoritative counts
        (re-derived from voucher_pools, not either thread's local view)
        must reflect exactly one new row.
        """
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        barrier = threading.Barrier(2)
        results = [None, None]
        errors = []

        def worker(idx, admin_identity):
            try:
                barrier.wait(timeout=5)
                results[idx] = avb.add_codes_to_batch(
                    db, batch_id, admin_identity=admin_identity, codes="RACE-CONC"
                )
            except Exception as exc:  # pragma: no cover - surfaced via assertion below
                errors.append(exc)

        t1 = threading.Thread(target=worker, args=(0, "admin1"))
        t2 = threading.Thread(target=worker, args=(1, "admin2"))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert not errors, f"worker thread(s) raised: {errors}"
        assert results[0] is not None and results[1] is not None

        winners = [r for r in results if r["ok"] is True]
        losers = [r for r in results if r["ok"] is False]
        assert len(winners) == 1, f"expected exactly one winner, got results={results}"
        assert len(losers) == 1
        assert winners[0]["inserted_count"] == 1
        assert losers[0]["code"] == "duplicate_codes"
        assert losers[0]["duplicate_count"] == 1
        assert losers[0]["inserted_count"] == 0

        # Exactly one voucher row exists for the raced code.
        assert db.voucher_pools.count_documents({"code": "RACE-CONC"}) == 1
        # No second batch was created by either request.
        assert db.affiliate_voucher_batches.count_documents({}) == 1
        # Final counts come from live voucher_pools state, not either
        # thread's locally-computed number.
        final = avb.get_batch_detail(db, batch_id)
        assert final["available_count"] == 2  # original A1 + the one RACE-CONC winner
        assert final["uploaded_count"] == 2


class TestAddCodesAdminHttpLayer:
    def test_unauthenticated_request_rejected(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        app = _app_with_auth(db, authorized=False)
        client = app.test_client()
        resp = client.post(f"/api/admin/affiliate-voucher-batches/{batch_id}/add-codes", json={"codes": "B1"})
        assert resp.status_code == 403
        assert resp.get_json()["code"] == "unauthorized"

    def test_add_codes_round_trip_returns_required_fields(self):
        db = _db()
        res = _create(db, codes=["A1", "A2"])
        batch_id = res["batch"]["batch_id"]
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            f"/api/admin/affiliate-voucher-batches/{batch_id}/add-codes",
            json={"codes": "B1\nB2\nB3"},
        )
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["submitted_count"] == 3
        assert body["inserted_count"] == 3
        assert body["duplicate_count"] == 0
        assert body["invalid_count"] == 0
        assert body["available_count"] == 5
        assert body["uploaded_count"] == 5

        detail = client.get(f"/api/admin/affiliate-voucher-batches/{batch_id}").get_json()
        assert detail["batch"]["available_count"] == 5
        assert detail["batch"]["uploaded_count"] == 5

    def test_missing_batch_returns_404(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches/000000000000000000000000/add-codes",
            json={"codes": "A1"},
        )
        assert resp.status_code == 404
        assert resp.get_json()["code"] == "batch_not_found"

    def test_disabled_batch_returns_409(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        app = _app_with_auth(db)
        client = app.test_client()
        client.patch(f"/api/admin/affiliate-voucher-batches/{batch_id}", json={"distribution_disabled": True})
        resp = client.post(f"/api/admin/affiliate-voucher-batches/{batch_id}/add-codes", json={"codes": "B1"})
        assert resp.status_code == 409
        assert resp.get_json()["code"] == "batch_disabled"

    def test_empty_codes_returns_400(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(f"/api/admin/affiliate-voucher-batches/{batch_id}/add-codes", json={"codes": ""})
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "no_codes"

    def test_expired_batch_returns_409(self):
        db = _db()
        res = _create(db, codes=["A1"], starts="2020-01-01 00:00:00", ends="2020-02-01 00:00:00")
        batch_id = res["batch"]["batch_id"]
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(f"/api/admin/affiliate-voucher-batches/{batch_id}/add-codes", json={"codes": "B1"})
        assert resp.status_code == 409
        assert resp.get_json()["code"] == "batch_expired"

    def test_database_error_returns_500_with_partial_inserted_count(self):
        db = _db()
        res = _create(db, codes=["A1"])
        batch_id = res["batch"]["batch_id"]
        app = _app_with_auth(db)
        client = app.test_client()
        real_insert_one = db.voucher_pools.insert_one
        call_count = {"n": 0}

        def flaky_insert_one(doc):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("simulated transient write failure")
            return real_insert_one(doc)

        db.voucher_pools.insert_one = flaky_insert_one
        try:
            resp = client.post(f"/api/admin/affiliate-voucher-batches/{batch_id}/add-codes", json={"codes": "B1\nB2"})
        finally:
            db.voucher_pools.insert_one = real_insert_one

        assert resp.status_code == 500
        body = resp.get_json()
        assert body["code"] == "database_error"
        assert body["inserted_count"] == 1


class TestAddCodesButtonGating:
    """The dashboard must not let an admin open the Add Codes modal for a
    batch the backend will reject anyway — disabled/uploading/failed/
    expired batches gate add_codes_to_batch server-side (see
    TestAddCodesToBatch / TestAddCodesAdminHttpLayer), so the button must
    be disabled with an explanatory reason for exactly those statuses.
    """

    @staticmethod
    def _source():
        with open("static/admin-dashboard.js", encoding="utf-8") as fh:
            return fh.read()

    def test_block_reasons_cover_every_backend_guard_status(self):
        src = self._source()
        start = src.index("var AB_ADD_CODES_BLOCK_REASON = {")
        end = src.index("};", start)
        block = src[start:end]
        for status in ("disabled", "uploading", "failed", "expired"):
            assert status in block, f"+ Add Codes button not gated for status={status}"

    def test_button_rendered_disabled_when_blocked(self):
        src = self._source()
        assert 'addCodesBlockReason' in src
        assert 'disabled title="' in src
        assert 'data-ab-addcodes=' in src


class TestEntitlementMonthApiEnforcement:
    """P2 regression: the HTTP admin API must fail closed against a direct
    API client trying to schedule an entitlement-month pool (T1-T5,
    AFFILIATE_5/10/50) from client-supplied starts_at_local/ends_at_local
    instead of a canonical entitlement_month -- even a window that LOOKS
    like the right month (e.g. "2026-09-01 00:01" .. "2026-09-30 23:59")
    is not the exact canonical boundary the claim path's full-containment
    check requires.

    Enforced at this HTTP boundary rather than inside create_batch/
    update_batch themselves, which stay available to internal maintenance
    tooling (scripts/fix_affiliate_batch_month_boundaries.py) and to this
    test suite's own edge-case fixtures that deliberately construct
    non-canonical windows to exercise the claimability logic.
    """

    def test_create_missing_entitlement_month_rejected(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "Suspicious September",
                "pool_id": "T2",
                "starts_at_local": "2026-09-01 00:01:00",
                "ends_at_local": "2026-09-30 23:59:00",
                "codes": ["X1"],
            },
        )
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["ok"] is False
        assert body["code"] == "entitlement_month_required"
        # Nothing was created.
        assert db.affiliate_voucher_batches.count_documents({}) == 0

    def test_create_missing_entitlement_month_rejected_for_denomination_pool(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "Suspicious September",
                "pool_id": "AFFILIATE_10",
                "starts_at_local": "2026-09-01 00:01:00",
                "ends_at_local": "2026-09-30 23:59:00",
                "codes": ["X1"],
            },
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "entitlement_month_required"
        assert db.affiliate_voucher_batches.count_documents({}) == 0

    def test_create_malformed_entitlement_month_rejected(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "Bad Month",
                "pool_id": "T2",
                "entitlement_month": "not-a-month",
                "codes": ["X1"],
            },
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "invalid_entitlement_month"
        assert db.affiliate_voucher_batches.count_documents({}) == 0

    def test_create_valid_entitlement_month_derives_canonical_boundaries(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "September T2",
                "pool_id": "T2",
                "entitlement_month": "202609",
                "codes": ["X1"],
            },
        )
        assert resp.status_code == 200
        batch = resp.get_json()["batch"]
        assert batch["starts_at_kl"].startswith("2026-09-01T00:00:00")
        assert batch["ends_at_kl"].startswith("2026-10-01T00:00:00")
        # Canonical UTC boundaries (KL = UTC+8), matching
        # affiliate_rewards._month_window_from_yyyymm exactly.
        assert batch["starts_at_utc"].startswith("2026-08-31T16:00:00")
        assert batch["ends_at_utc"].startswith("2026-09-30T16:00:00")

    def test_create_arbitrary_dates_alongside_valid_entitlement_month_are_ignored(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "September T2",
                "pool_id": "T2",
                "entitlement_month": "202609",
                # Deliberately wrong/noncanonical -- must be entirely
                # ignored in favour of the canonical month boundary.
                "starts_at_local": "2026-09-01 00:01:00",
                "ends_at_local": "2026-09-30 23:59:00",
                "codes": ["X1"],
            },
        )
        assert resp.status_code == 200
        batch = resp.get_json()["batch"]
        assert batch["starts_at_kl"].startswith("2026-09-01T00:00:00")
        assert batch["ends_at_kl"].startswith("2026-10-01T00:00:00")

    def test_welcome_still_supports_free_form_dates(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "Welcome Free Form",
                "pool_id": "WELCOME",
                "starts_at_local": "2026-09-01 00:01:00",
                "ends_at_local": "2026-09-30 23:59:00",
                "codes": ["W1"],
            },
        )
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True

    def test_update_cannot_convert_canonical_batch_to_free_form(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        create_resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "September T2",
                "pool_id": "T2",
                "entitlement_month": "202609",
                "codes": ["X1"],
            },
        )
        batch_id = create_resp.get_json()["batch"]["batch_id"]
        original = avb.get_batch_detail(db, batch_id)

        resp = client.patch(
            f"/api/admin/affiliate-voucher-batches/{batch_id}",
            json={
                "starts_at_local": "2026-09-01 00:01:00",
                "ends_at_local": "2026-09-30 23:59:00",
            },
        )
        assert resp.status_code == 400
        assert resp.get_json()["code"] == "entitlement_month_required"
        # The batch's schedule is entirely untouched.
        after = avb.get_batch_detail(db, batch_id)
        assert after["starts_at_utc"] == original["starts_at_utc"]
        assert after["ends_at_utc"] == original["ends_at_utc"]

    def test_update_with_valid_entitlement_month_still_allowed(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        create_resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "October T2",
                "pool_id": "T2",
                "entitlement_month": "202610",
                "codes": ["X1"],
            },
        )
        batch_id = create_resp.get_json()["batch"]["batch_id"]

        resp = client.patch(
            f"/api/admin/affiliate-voucher-batches/{batch_id}",
            json={"entitlement_month": "202611"},
        )
        assert resp.status_code == 200
        batch = resp.get_json()["batch"]
        assert batch["starts_at_kl"].startswith("2026-11-01T00:00:00")
        assert batch["ends_at_kl"].startswith("2026-12-01T00:00:00")

    def test_update_notes_only_is_unaffected(self):
        db = _db()
        app = _app_with_auth(db)
        client = app.test_client()
        create_resp = client.post(
            "/api/admin/affiliate-voucher-batches",
            json={
                "batch_name": "September T2",
                "pool_id": "T2",
                "entitlement_month": "202609",
                "codes": ["X1"],
            },
        )
        batch_id = create_resp.get_json()["batch"]["batch_id"]
        resp = client.patch(
            f"/api/admin/affiliate-voucher-batches/{batch_id}",
            json={"notes": "just a note"},
        )
        assert resp.status_code == 200
        assert resp.get_json()["batch"]["notes"] == "just a note"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
