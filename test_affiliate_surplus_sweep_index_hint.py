"""Production found the surplus sweep's plan carrying a blocking SORT:

    stages=['SORT', 'FETCH', 'IXSCAN']

instead of the intended

    stages=['LIMIT', 'FETCH', 'IXSCAN']

The real MongoDB planner prefers `affiliate_type_entitlement_tier` (ledger_type,
entitlement_month, tier) for the query's `entitlement_month: {$gte: ...}`
range — especially while the denomination-plan eligible set is small or
empty, where that index proves "no match" fastest during the multi-plan
trial — over `affiliate_type_surplus_checked` (ledger_type, surplus_checked_at,
_id), which provides the sort but must examine every AFFILIATE_MONTHLY
ledger as a residual filter. Winning the trial does not mean providing the
sort, so MongoDB falls back to an in-memory SORT over the whole eligible set.

The fix pins the query to its own purpose-built index with an explicit
`.hint()`. That index is created under a fixed name by this same module's
own startup path (never routed through the legacy-name-adopting
`_ensure_equivalent_index`), so the hint can never point at an
unpredictable name from a partial rollout.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pymongo.errors import OperationFailure

import affiliate_rewards as ar
import affiliate_reward_plans as arp
from fake_mongo import FakeDb

SEP = datetime(2026, 9, 10, 4, 0, tzinfo=timezone.utc)
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}


def _seed_one(db):
    db.affiliate_ledger.insert_one({
        "_id": "L0000", "ledger_type": "AFFILIATE_MONTHLY", "user_id": 1,
        "status": "ISSUED", "tier": "T1", "year_month": "202609",
        "entitlement_month": "202609", "reward_plan": arp.DENOMINATION_PLAN_ID,
        "bundle_recipe": arp.tier_recipe("202609", "T1"),
        "dedup_key": "AFF:1:202609:T1", "voucher_code": "CODE0000",
        "vouchers": [{"code": "CODE0000", "value": 10, "pool_id": "AFFILIATE_10"}],
        "updated_at": SEP,
    })


class _HintSpy:
    """Wraps FakeDb and records the `hint` kwarg every affiliate_ledger.find
    call carries, without validating it against any real index catalogue."""

    def __init__(self, inner):
        self._inner = inner
        self.hints = []

    def __getattr__(self, name):
        col = getattr(self._inner, name)
        if name != "affiliate_ledger":
            return col
        spy = self

        class _Col:
            def find(self, query=None, **kwargs):
                spy.hints.append(kwargs.get("hint"))
                return col.find(query, **kwargs)

            def __getattr__(self, item):
                return getattr(col, item)

        return _Col()

    def __getitem__(self, name):
        return getattr(self, name)


class _MissingIndexDb:
    """Simulates a real MongoDB rejecting `.hint()` for an index that does
    not exist — exactly what production would do if the hinted index were
    ever dropped, renamed, or never created by startup."""

    class _Col:
        def find(self, query=None, **kwargs):
            hint = kwargs.get("hint")
            if hint == ar.SURPLUS_SWEEP_INDEX_NAME:
                raise OperationFailure(
                    "error processing query: ns=db.affiliate_ledger "
                    "Sort: {surplus_checked_at: 1, _id: 1} Proj: {} "
                    f"planner returned error :: caused by :: hint provided "
                    f"does not correspond to an existing index",
                    code=2,
                )
            return []

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._Col()

    def __getitem__(self, name):
        return getattr(self, name)


class TestTheSweepHintsItsOwnIndex:
    def test_the_query_hints_the_surplus_index(self):
        db = FakeDb(UNIQUE)
        _seed_one(db)
        spy = _HintSpy(db)
        ar.reconcile_surplus_denomination_allocations(spy, now_utc=SEP, batch_limit=5)
        assert spy.hints, "the sweep never queried affiliate_ledger"
        assert spy.hints[0] == ar.SURPLUS_SWEEP_INDEX_NAME, (
            f"expected hint={ar.SURPLUS_SWEEP_INDEX_NAME!r}, got {spy.hints[0]!r}"
        )

    def test_the_hinted_index_name_matches_what_startup_creates(self):
        requested = []

        class _Rec:
            def create_index(self, keys, **kw):
                requested.append(kw.get("name"))
                return kw.get("name")

            def list_indexes(self):
                return []

            def __getattr__(self, item):
                return lambda *a, **k: None

        class _Db:
            def __getattr__(self, name):
                if name.startswith("_"):
                    raise AttributeError(name)
                return _Rec()

            def __getitem__(self, name):
                return getattr(self, name)

        ar.ensure_affiliate_indexes(_Db())
        assert ar.SURPLUS_SWEEP_INDEX_NAME in requested, (
            "startup does not create the index the sweep hints — the hint "
            "would be invalid the moment startup ran before this change"
        )


class TestAMissingHintedIndexFailsVisibly:
    def test_a_missing_hinted_index_raises_rather_than_silently_returning(self):
        db = _MissingIndexDb()
        with pytest.raises(OperationFailure):
            ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)

    def test_an_unrelated_query_failure_is_still_swallowed_into_stats(self):
        """Only the specific "hint does not correspond to an existing index"
        failure is re-raised. Any other query failure (a transient network
        blip, a real server error unrelated to the hint) keeps the sweep's
        existing behaviour of recording it and returning cleanly, so one bad
        tick does not crash the whole 5-minute job for unrelated reasons."""

        class _FlakyDb:
            class _Col:
                def find(self, query=None, **kwargs):
                    raise OperationFailure("connection reset", code=6)

            def __getattr__(self, name):
                if name.startswith("_"):
                    raise AttributeError(name)
                return self._Col()

            def __getitem__(self, name):
                return getattr(self, name)

        stats = ar.reconcile_surplus_denomination_allocations(
            _FlakyDb(), now_utc=SEP, batch_limit=5
        )
        assert stats["errors"] == 1
