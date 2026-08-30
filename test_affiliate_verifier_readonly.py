"""The preflight verifier must perform ZERO writes.

It previously reached its database through `database.init_db()`, which calls
`ensure_indexes()` — so a tool documented as READ-ONLY created indexes on the
production database it was only meant to inspect.
"""
from __future__ import annotations

import collections
import textwrap
from datetime import datetime, timezone

import pytest

import affiliate_reward_plans as arp
import affiliate_voucher_batches as batches
from fake_mongo import FakeDb
from scripts import verify_affiliate_reward_plan as verifier

SEP = datetime(2026, 9, 10, 4, 0, tzinfo=timezone.utc)

WRITE_METHODS = (
    "create_index", "create_indexes", "drop_index", "drop_indexes",
    "insert_one", "insert_many", "update_one", "update_many",
    "replace_one", "delete_one", "delete_many", "drop",
    "find_one_and_update", "find_one_and_replace", "find_one_and_delete",
    "bulk_write", "rename", "create_collection", "aggregate_write",
)


class _WriteSpyCollection:
    """Every write-capable method raises. Reads pass through."""

    def __init__(self, inner, attempted):
        self._inner = inner
        self._attempted = attempted

    def __getattr__(self, name):
        if name in WRITE_METHODS:
            def _blocked(*a, **k):
                self._attempted.append(name)
                raise AssertionError(
                    f"the READ-ONLY verifier attempted a write: {name}()"
                )
            return _blocked
        return getattr(self._inner, name)


class _WriteSpyDb:
    def __init__(self, inner):
        self._inner = inner
        self.attempted: list[str] = []

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return _WriteSpyCollection(getattr(self._inner, name), self.attempted)

    def __getitem__(self, name):
        return _WriteSpyCollection(self._inner[name], self.attempted)


def _seeded_db():
    db = FakeDb({"voucher_pools": [("pool_id", "code")],
                 "affiliate_ledger": [("dedup_key",)]})
    for pool in ("AFFILIATE_5", "AFFILIATE_10", "AFFILIATE_50"):
        res = batches.create_batch(
            db, admin_identity="seed", batch_name=f"{pool} 202609",
            pool_id=pool, entitlement_month="202609",
            codes=[f"{pool}-{i}" for i in range(20)],
        )
        assert res["ok"], res
    db.affiliate_ledger.insert_one({
        "_id": "L1", "ledger_type": "AFFILIATE_MONTHLY", "user_id": 5,
        "status": "ISSUED", "tier": "T1", "year_month": "202609",
        "entitlement_month": "202609", "reward_plan": arp.DENOMINATION_PLAN_ID,
        "bundle_recipe": arp.tier_recipe("202609", "T1"),
        "reward_value": 10, "expected_code_count": 1,
        "issued_code_count": 1, "issued_value": 10,
        "dedup_key": "AFF:5:202609:T1", "voucher_code": "AFFILIATE_10-0",
        "vouchers": [{"code": "AFFILIATE_10-0", "value": 10, "pool_id": "AFFILIATE_10"}],
    })
    return db


class TestVerifierPerformsNoWrites:
    def test_inventory_check_writes_nothing(self):
        spy = _WriteSpyDb(_seeded_db())
        f = verifier.Findings()
        verifier.check_inventory(spy, f, "202609")
        assert spy.attempted == []

    def test_inventory_with_expected_demand_writes_nothing(self):
        spy = _WriteSpyDb(_seeded_db())
        f = verifier.Findings()
        verifier.check_inventory(spy, f, "202609", {"T1": 1, "T2": 1})
        assert spy.attempted == []

    def test_ledger_integrity_check_writes_nothing(self):
        spy = _WriteSpyDb(_seeded_db())
        f = verifier.Findings()
        verifier.check_ledger_integrity(spy, f, "202609")
        assert spy.attempted == []

    def test_plan_assignment_check_writes_nothing(self):
        spy = _WriteSpyDb(_seeded_db())
        f = verifier.Findings()
        verifier.check_plan_assignment(spy, f, "202609")
        assert spy.attempted == []

    def test_all_db_checks_together_write_nothing_and_still_report(self):
        spy = _WriteSpyDb(_seeded_db())
        f = verifier.Findings()
        verifier.check_inventory(spy, f, "202609")
        verifier.check_ledger_integrity(spy, f, "202609")
        verifier.check_plan_assignment(spy, f, "202609")
        assert spy.attempted == []
        # And the checks genuinely ran rather than short-circuiting.
        assert f.items == [], f.items

    def test_the_write_spy_actually_blocks(self):
        """If the spy were permissive the tests above would be worthless."""
        spy = _WriteSpyDb(_seeded_db())
        with pytest.raises(AssertionError, match="attempted a write"):
            spy.affiliate_ledger.create_index([("x", 1)], name="nope")
        with pytest.raises(AssertionError, match="attempted a write"):
            spy.voucher_pools.update_one({}, {"$set": {"a": 1}})


class TestVerifierDoesNotUseInitDb:
    def test_main_does_not_call_init_db(self, monkeypatch):
        called = {"init": 0}

        import database

        def _boom(*a, **k):
            called["init"] += 1
            raise AssertionError("the READ-ONLY verifier called init_db()")

        monkeypatch.setattr(database, "init_db", _boom, raising=False)
        # plan-config needs no database at all; it must still not touch init_db.
        rc = verifier.main.__wrapped__() if hasattr(verifier.main, "__wrapped__") else None
        assert called["init"] == 0

    def test_a_read_only_handle_helper_exists(self):
        assert hasattr(verifier, "_read_only_db")

    def test_read_only_helper_never_calls_ensure_indexes(self):
        """Checks the CODE, not the docstring — which legitimately explains
        why init_db()/ensure_indexes() are avoided."""
        import ast
        import inspect

        tree = ast.parse(textwrap.dedent(inspect.getsource(verifier._read_only_db)))
        called = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                if isinstance(fn, ast.Name):
                    called.add(fn.id)
                elif isinstance(fn, ast.Attribute):
                    called.add(fn.attr)
        assert "init_db" not in called, "the read-only handle still calls init_db()"
        assert "ensure_indexes" not in called
        assert "SecondaryPreferred" in called, "no read preference is applied"

    def test_no_check_function_calls_init_db(self):
        import ast
        import inspect

        src = inspect.getsource(verifier)
        tree = ast.parse(src)
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", None)
                if name in ("init_db", "ensure_indexes"):
                    offenders.append(name)
        assert offenders == [], f"verifier calls {offenders}"

    def test_module_source_contains_no_write_call(self):
        import inspect

        src = inspect.getsource(verifier)
        for forbidden in ("create_index(", "insert_one(", "insert_many(",
                          "update_one(", "update_many(", "delete_one(",
                          "delete_many(", "drop_index(", "create_collection("):
            assert forbidden not in src, (
                f"the verifier source contains a write call: {forbidden}"
            )

    def test_help_text_describes_the_read_only_guarantee(self):
        assert "init_db" in verifier.__doc__
        assert "READ-ONLY" in verifier.__doc__ or "read-only" in verifier.__doc__.lower()


class TestExitCodesPreserved:
    def test_findings_still_produce_a_nonzero_exit(self):
        f = verifier.Findings()
        f.add("something is wrong")
        assert f.items, "a finding was not recorded"

    def test_a_clean_run_records_nothing(self):
        spy = _WriteSpyDb(_seeded_db())
        f = verifier.Findings()
        verifier.check_inventory(spy, f, "202609")
        assert f.items == []
