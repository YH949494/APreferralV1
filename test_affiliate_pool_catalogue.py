"""Drift guard: every operator-facing pool allowlist must equal the backend
canonical catalogue.

This list used to be restated in five independent places (two backends,
three frontends). They drifted: T5 and the three denomination pools were
missing from all three admin UIs, so September inventory could not be
uploaded through any supported operator workflow even though the backend
accepted it. These tests parse the REAL static assets that ship to
operators, so a future edit to one copy and not another fails here.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from affiliate_reward_plans import (
    ADMIN_AFFILIATE_POOL_IDS,
    DENOMINATION_POOL_IDS,
    ENTITLEMENT_MONTH_POOL_IDS,
    POOL_DISPLAY_LABELS,
    pool_denomination,
)

ROOT = Path(__file__).parent
ADMIN_HTML = ROOT / "static" / "admin-dashboard.html"
ADMIN_JS = ROOT / "static" / "admin-dashboard.js"
INDEX_HTML = ROOT / "static" / "index.html"


def _select_options(html: str, select_id: str) -> list[str]:
    """The `value=` of every <option> inside the named <select>."""
    start = html.index(f'id="{select_id}"')
    end = html.index("</select>", start)
    return re.findall(r'<option value="([^"]+)"', html[start:end])


class TestBackendCatalogue:
    def test_catalogue_contains_legacy_tiers_and_denomination_pools(self):
        assert set(ADMIN_AFFILIATE_POOL_IDS) == {
            "T1", "T2", "T3", "T4", "T5",
            "AFFILIATE_5", "AFFILIATE_10", "AFFILIATE_50",
            "WELCOME",
        }

    def test_legacy_tier_pools_are_preserved_for_august(self):
        # August and any historical/back-dated entitlement still draws from
        # the per-tier pools, so they must never be dropped from the UI.
        for tier in ("T1", "T2", "T3", "T4", "T5"):
            assert tier in ADMIN_AFFILIATE_POOL_IDS

    def test_every_pool_has_an_operator_facing_label(self):
        for pool_id in ADMIN_AFFILIATE_POOL_IDS:
            label = POOL_DISPLAY_LABELS[pool_id]
            assert label and label != pool_id, f"{pool_id} has no operator label"

    def test_denomination_pools_require_the_entitlement_month_picker(self):
        for pool_id in DENOMINATION_POOL_IDS:
            assert pool_id in ENTITLEMENT_MONTH_POOL_IDS
        # T5 too -- it was previously excluded, leaving it the only tier
        # that could not take a scheduled month-bounded batch.
        assert "T5" in ENTITLEMENT_MONTH_POOL_IDS
        # WELCOME keeps free-form scheduling.
        assert "WELCOME" not in ENTITLEMENT_MONTH_POOL_IDS

    def test_batch_module_reuses_the_catalogue_rather_than_restating_it(self):
        import affiliate_voucher_batches as batches

        assert tuple(batches.BATCH_POOL_IDS) == tuple(ADMIN_AFFILIATE_POOL_IDS)
        assert tuple(batches.ENTITLEMENT_MONTH_POOL_IDS) == tuple(ENTITLEMENT_MONTH_POOL_IDS)


class TestFrontendMatchesBackend:
    def test_scheduled_batch_selector_offers_every_pool(self):
        options = _select_options(ADMIN_HTML.read_text(), "ab-pool-id")
        assert options == list(ADMIN_AFFILIATE_POOL_IDS), (
            "static/admin-dashboard.html batch pool selector has drifted from "
            "affiliate_reward_plans.ADMIN_AFFILIATE_POOL_IDS"
        )

    def test_legacy_upload_selector_offers_every_pool(self):
        options = _select_options(INDEX_HTML.read_text(), "aff_pool_id")
        assert options == list(ADMIN_AFFILIATE_POOL_IDS), (
            "static/index.html affiliate pool selector has drifted from the catalogue"
        )

    def test_client_allowlist_matches_backend_allowlist(self):
        text = INDEX_HTML.read_text()
        raw = re.search(r"const ALLOWED_AFFILIATE_POOLS = (\[[^\]]*\]);", text).group(1)
        assert json.loads(raw) == list(ADMIN_AFFILIATE_POOL_IDS)

    def test_entitlement_month_pool_map_matches_backend(self):
        text = ADMIN_JS.read_text()
        raw = re.search(r"var AB_ENTITLEMENT_MONTH_POOLS = \{([^}]*)\};", text).group(1)
        keys = re.findall(r"(\w+)\s*:\s*true", raw)
        assert keys == list(ENTITLEMENT_MONTH_POOL_IDS), (
            "static/admin-dashboard.js AB_ENTITLEMENT_MONTH_POOLS has drifted; a pool "
            "missing here gets free-form window fields and silently produces a batch "
            "window that fails the full-month-containment check"
        )

    def test_no_frontend_offers_a_pool_the_backend_would_reject(self):
        for path, select_id in ((ADMIN_HTML, "ab-pool-id"), (INDEX_HTML, "aff_pool_id")):
            for pool_id in _select_options(path.read_text(), select_id):
                assert pool_id in ADMIN_AFFILIATE_POOL_IDS, (
                    f"{path.name} offers {pool_id!r}, which the backend rejects"
                )


class TestSubmittedPayloadIsAccepted:
    """Deterministic proof that the exact pool_id an operator's browser
    submits is the exact pool_id every backend validator accepts."""

    @pytest.mark.parametrize("pool_id", ["AFFILIATE_5", "AFFILIATE_10", "AFFILIATE_50"])
    def test_denomination_batch_payload_round_trips(self, pool_id):
        from fake_mongo import FakeDb
        import affiliate_voucher_batches as batches

        # The value the <option> carries is what the form POSTs.
        options = _select_options(ADMIN_HTML.read_text(), "ab-pool-id")
        assert pool_id in options

        db = FakeDb({"voucher_pools": [("pool_id", "code")]})
        result = batches.create_batch(
            db,
            admin_identity="reviewer",
            batch_name=f"{pool_id} Sept 2026",
            pool_id=pool_id,
            entitlement_month="202609",
            codes=[f"{pool_id}-A", f"{pool_id}-B"],
        )
        assert result["ok"] is True, result

        # Canonical KL month window, not an admin-typed approximation.
        batch = db.affiliate_voucher_batches.find_one({"pool_id": pool_id})
        expected_start, expected_end = batches.canonical_entitlement_month_window("202609")
        assert batch["starts_at"] == expected_start
        assert batch["ends_at"] == expected_end

        # Every physical row is priced by its pool's denomination.
        rows = list(db.voucher_pools.find({"pool_id": pool_id}))
        assert len(rows) == 2
        assert all(r["voucher_value"] == pool_denomination(pool_id) for r in rows)

    def test_t5_batch_payload_round_trips(self):
        from fake_mongo import FakeDb
        import affiliate_voucher_batches as batches

        assert "T5" in _select_options(ADMIN_HTML.read_text(), "ab-pool-id")
        db = FakeDb({"voucher_pools": [("pool_id", "code")]})
        result = batches.create_batch(
            db, admin_identity="reviewer", batch_name="T5 Aug 2026",
            pool_id="T5", entitlement_month="202608", codes=["T5-A"],
        )
        assert result["ok"] is True, result

    def test_unknown_pool_is_still_rejected(self):
        from fake_mongo import FakeDb
        import affiliate_voucher_batches as batches

        db = FakeDb({"voucher_pools": [("pool_id", "code")]})
        result = batches.create_batch(
            db, admin_identity="reviewer", batch_name="bogus",
            pool_id="AFFILIATE_25", entitlement_month="202609", codes=["X"],
        )
        assert result["ok"] is False
        assert result["code"] == "invalid_pool_id"
