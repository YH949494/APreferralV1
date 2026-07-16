"""Tests for voucher_pool_service.py: reuse of the existing Voucher Centre
inventory table (db.voucher_pools) — no second inventory collection."""

from datetime import datetime, timezone

import pytest

import database
import voucher_pool_service as vps
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(unique_keys_by_collection={
        "voucher_pool_registry": [("pool_id",)],
        "voucher_pools": [("pool_id", "code")],
    })
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(vps, "database", database)
    return fdb


def test_register_pool_writes_only_registry_metadata(fake_db):
    vps.register_pool("gold", name="Gold", pool_type="tournament_reward", campaign_id="c1", reward_usage="rank-1")
    pool = vps.get_pool("gold")
    assert pool["name"] == "Gold"
    assert pool["pool_type"] == "tournament_reward"
    assert pool["campaign_id"] == "c1"
    assert pool["status"] == "active"
    # No inventory row was created just by registering.
    assert fake_db["voucher_pools"].count_documents({"pool_id": "gold"}) == 0


def test_upload_codes_writes_into_shared_voucher_pools_collection(fake_db):
    vps.register_pool("gold", name="Gold")
    result = vps.upload_codes("gold", ["A1", "A2", "A1"])
    assert result == {"inserted": 2, "skipped_duplicates": 1}
    rows = list(fake_db["voucher_pools"].find({"pool_id": "gold"}))
    assert len(rows) == 2
    assert all(r["status"] == "available" for r in rows)


def test_pool_stock_counts_available_and_issued(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "A1", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "A2", "status": "issued", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    assert vps.pool_stock("gold") == {"available": 1, "issued": 1}


def test_allocate_voucher_is_atomic_and_idempotent(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "gold", "code": "A1", "status": "available", "created_at": now, "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards"})
    first = vps.allocate_voucher("gold", reward_id="rw_1", telegram_user_id=111)
    assert first["status"] == "issued"
    assert first["issued_to_user_id"] == 111

    second = vps.allocate_voucher("gold", reward_id="rw_2", telegram_user_id=222)
    assert second is None  # pool exhausted, no second code available


def test_allocate_voucher_out_of_stock_returns_none(fake_db):
    assert vps.allocate_voucher("empty-pool", reward_id="rw_1", telegram_user_id=111) is None


def test_pool_is_active_reflects_registry_status(fake_db):
    vps.register_pool("gold", name="Gold")
    assert vps.pool_is_active("gold") is True
    vps.set_pool_status("gold", "inactive")
    assert vps.pool_is_active("gold") is False


def test_list_pools_filters_by_campaign(fake_db):
    vps.register_pool("gold", name="Gold", campaign_id="camp-1")
    vps.register_pool("silver", name="Silver", campaign_id="camp-2")
    result = vps.list_pools(campaign_id="camp-1")
    assert [p["pool_id"] for p in result] == ["gold"]


# ---------------------------------------------------------------------------
# Shared-pool safety: campaign rewards and affiliate/welcome allocations
# must never be able to consume each other's stock, even against the same
# db.voucher_pools collection.
# ---------------------------------------------------------------------------

import pytest as _pytest  # noqa: E402

import affiliate_rewards


@_pytest.mark.parametrize("reserved_id", ["WELCOME", "T1", "T2", "T3", "T4", "T5", "t1", "welcome"])
def test_register_pool_refuses_reserved_legacy_pool_ids(fake_db, reserved_id):
    with _pytest.raises(vps.ReservedPoolIdError):
        vps.register_pool(reserved_id, name="Collision Attempt")
    assert fake_db["voucher_pool_registry"].count_documents({}) == 0


@_pytest.mark.parametrize("reserved_id", ["WELCOME", "T1", "t3"])
def test_upload_codes_refuses_reserved_legacy_pool_ids(fake_db, reserved_id):
    with _pytest.raises(vps.ReservedPoolIdError):
        vps.upload_codes(reserved_id, ["X1", "X2"])
    assert fake_db["voucher_pools"].count_documents({"pool_id": reserved_id.upper()}) == 0


def test_campaign_allocation_never_claims_a_legacy_style_row(fake_db):
    """Even in the hypothetical case of a pool_id collision (which
    registration/upload already refuse), a code row inserted the legacy way
    — no pool_source marker, as affiliate_rewards._claim_voucher_from_pool's
    own inserts look like — must never be claimable by campaign allocation."""
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({
        "pool_id": "shared-pool-id", "code": "LEGACY-CODE", "status": "available", "created_at": now,
        # deliberately no "pool_source" field, matching real legacy rows
    })
    result = vps.allocate_voucher("shared-pool-id", reward_id="rw_1", telegram_user_id=111)
    assert result is None
    # the legacy row must remain untouched
    legacy_row = fake_db["voucher_pools"].find_one({"code": "LEGACY-CODE"})
    assert legacy_row["status"] == "available"


def test_affiliate_allocation_rejects_explicit_campaign_rewards_scope(fake_db):
    """The reverse direction: affiliate_rewards._claim_voucher_from_pool now
    carries a minimal additive guard (Part 1.4) that rejects any row whose
    allocation_scope is explicitly "campaign_rewards" (or any other
    non-affiliate scope) — the explicit scope model is now the primary
    isolation control, not pool_id separation alone."""
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({
        "pool_id": "shared-pool-id", "code": "CAMPAIGN-CODE", "status": "available", "created_at": now,
        "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards",
    })
    claimed = affiliate_rewards._claim_voucher_from_pool(
        fake_db, pool_id="shared-pool-id", ledger_id="ledger-1", user_id=999, now_utc=now,
    )
    assert claimed is None
    row = fake_db["voucher_pools"].find_one({"code": "CAMPAIGN-CODE"})
    assert row["status"] == "available"


def test_affiliate_allocation_still_claims_legacy_rows_with_no_scope(fake_db):
    """Backward compatibility: rows written before this feature existed
    (no allocation_scope field at all) must keep working in affiliate flows
    exactly as before."""
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({
        "pool_id": "T1", "code": "LEGACY-AFF-1", "status": "available", "created_at": now,
    })
    claimed = affiliate_rewards._claim_voucher_from_pool(
        fake_db, pool_id="T1", ledger_id="ledger-1", user_id=555, now_utc=now,
    )
    assert claimed is not None
    assert claimed["code"] == "LEGACY-AFF-1"


def test_affiliate_allocation_claims_rows_explicitly_scoped_affiliate_or_shared(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({
        "pool_id": "T2", "code": "AFF-SCOPED", "status": "available", "created_at": now,
        "allocation_scope": "affiliate_rewards",
    })
    fake_db["voucher_pools"].insert_one({
        "pool_id": "T3", "code": "SHARED-SCOPED", "status": "available", "created_at": now,
        "allocation_scope": "shared",
    })
    claimed_affiliate = affiliate_rewards._claim_voucher_from_pool(
        fake_db, pool_id="T2", ledger_id="ledger-1", user_id=1, now_utc=now,
    )
    claimed_shared = affiliate_rewards._claim_voucher_from_pool(
        fake_db, pool_id="T3", ledger_id="ledger-2", user_id=2, now_utc=now,
    )
    assert claimed_affiliate["code"] == "AFF-SCOPED"
    assert claimed_shared["code"] == "SHARED-SCOPED"


def test_concurrent_affiliate_and_campaign_allocation_on_different_pools_are_independent(fake_db):
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({"pool_id": "T1", "code": "AFF-1", "status": "available", "created_at": now})
    fake_db["voucher_pools"].insert_one({
        "pool_id": "tourney-gold", "code": "TOUR-1", "status": "available", "created_at": now,
        "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards",
    })

    affiliate_claim = affiliate_rewards._claim_voucher_from_pool(
        fake_db, pool_id="T1", ledger_id="ledger-1", user_id=111, now_utc=now,
    )
    campaign_claim = vps.allocate_voucher("tourney-gold", reward_id="rw_1", telegram_user_id=222)

    assert affiliate_claim["code"] == "AFF-1"
    assert campaign_claim["code"] == "TOUR-1"
    # Each pool's other-side stock is unaffected.
    assert fake_db["voucher_pools"].count_documents({"pool_id": "T1", "status": "available"}) == 0
    assert fake_db["voucher_pools"].count_documents({"pool_id": "tourney-gold", "status": "available"}) == 0
    assert vps.pool_stock("T1") == {"available": 0, "issued": 0}  # T1 rows carry no pool_source marker


def test_concurrent_double_allocation_on_same_campaign_pool_never_double_issues(fake_db):
    """Two 'concurrent' campaign reward allocators racing for the same
    single-code pool: only one can win, mirroring the atomic
    find_one_and_update guarantee real concurrent requests rely on."""
    now = datetime.now(timezone.utc)
    fake_db["voucher_pools"].insert_one({
        "pool_id": "tourney-gold", "code": "TOUR-ONLY", "status": "available", "created_at": now,
        "pool_source": "campaign_centre", "allocation_scope": "campaign_rewards",
    })
    first = vps.allocate_voucher("tourney-gold", reward_id="rw_1", telegram_user_id=111, now=now)
    second = vps.allocate_voucher("tourney-gold", reward_id="rw_2", telegram_user_id=222, now=now)
    assert first["code"] == "TOUR-ONLY"
    assert second is None
    assert fake_db["voucher_pools"].count_documents({"pool_id": "tourney-gold", "status": "issued"}) == 1
