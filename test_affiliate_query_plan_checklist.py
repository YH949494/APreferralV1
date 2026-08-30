"""The staging explain() checklist must describe the queries actually issued.

`scripts/verify_affiliate_reward_plan.py --check query-plans` runs
`explain("executionStats")` against a real cluster and fails deployment on a
COLLSCAN, a blocking SORT, or a plan that examines disproportionately more
documents than it returns.

That check is only worth anything if the shapes it explains are the shapes
production runs. These tests capture the queries the sweeps really build and
compare them against the checklist, so a change to a sweep that is not
reflected in the checklist fails here rather than silently leaving a hot query
unverified.

Nothing here proves an index is USED. Only the winning plan against
production-shaped data can say that, which is why the checklist exists.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import affiliate_rewards as ar
from fake_mongo import FakeDb
from scripts.verify_affiliate_reward_plan import (
    _MAX_EXAMINED_PER_RETURNED,
    _query_plan_specs,
    _stage_names,
    CHECKS,
)

SEP = datetime(2026, 9, 10, 4, 0, tzinfo=timezone.utc)
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}


def _shape(value):
    """A query with every timestamp flattened, so two runs compare equal."""
    if isinstance(value, dict):
        return {k: _shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_shape(v) for v in value]
    if isinstance(value, datetime):
        return "<datetime>"
    return value


class _FindSpy:
    """Records every `affiliate_ledger.find` that carries a sort — i.e. every
    bounded selection query, and nothing else."""

    def __init__(self, inner):
        self._inner = inner
        self.selections = []

    def __getattr__(self, name):
        col = getattr(self._inner, name)
        if name != "affiliate_ledger":
            return col
        spy = self

        class _Col:
            def find(self, query=None, **kwargs):
                if kwargs.get("sort"):
                    spy.selections.append({
                        "query": query,
                        "sort": kwargs["sort"],
                        "hint": kwargs.get("hint"),
                    })
                return col.find(query, **kwargs)

            def __getattr__(self, item):
                return getattr(col, item)

        return _Col()

    def __getitem__(self, name):
        return getattr(self, name)


def _captured_selections():
    db = FakeDb(UNIQUE)
    spy = _FindSpy(db)
    ar._retry_stuck_pending_manual_affiliate_ledgers(spy, now_utc=SEP, batch_limit=5)
    ar.reconcile_surplus_denomination_allocations(spy, now_utc=SEP, batch_limit=5)
    return spy.selections


SPECS = _query_plan_specs("202609")
SPEC_BY_LABEL = {s[0]: s for s in SPECS}


def test_query_plans_is_a_selectable_check():
    assert "query-plans" in CHECKS


def test_every_bounded_sweep_query_is_on_the_checklist():
    captured = _captured_selections()
    assert len(captured) == 3, (
        f"expected the two retry selections and the surplus selection, got "
        f"{len(captured)}"
    )
    listed = [_shape(spec[2]) for spec in SPECS]
    for sel in captured:
        assert _shape(sel["query"]) in listed, (
            "a bounded selection query is not on the explain() checklist, so it "
            "would ship without its plan ever being verified:\n"
            f"  {_shape(sel['query'])}"
        )


def test_every_checklist_sort_matches_the_query_it_describes():
    captured = _captured_selections()
    by_shape = {}
    for sel in captured:
        by_shape[str(_shape(sel["query"]))] = [tuple(k) for k in sel["sort"]]
    for label, _coll, query, sort, _idx, _hint in SPECS:
        key = str(_shape(query))
        if key not in by_shape:
            continue  # a read-path query with no sweep counterpart
        assert sort is not None, f"{label}: the sweep sorts but the checklist does not"
        assert [tuple(s) for s in sort] == by_shape[key], (
            f"{label}: checklist sort {sort} does not match the sweep's "
            f"{by_shape[key]}"
        )


@pytest.mark.parametrize("label", [
    "retry sweep / PENDING_MANUAL",
    "retry sweep / stale SETTLING",
])
def test_both_retry_branches_are_listed_against_the_retry_index(label):
    _l, _c, _q, sort, expected, _hint = SPEC_BY_LABEL[label]
    assert expected == "affiliate_type_status_retry_checked"
    assert [tuple(s) for s in sort] == [("retry_checked_at", 1), ("_id", 1)]


def test_the_surplus_sweep_hints_its_own_index():
    """The real MongoDB planner prefers a different index for the query's
    entitlement_month range, especially while the denomination-plan eligible
    set is small or empty, and falls back to a blocking in-memory SORT. The
    sweep must pin the sort-providing index explicitly, and the checklist
    must explain the query the same way production actually issues it."""
    captured = _captured_selections()
    surplus = [
        sel for sel in captured
        if any(clause.get("status") == "ISSUED" for clause in (sel["query"].get("$or") or []))
    ]
    assert len(surplus) == 1, f"expected exactly one surplus selection, got {surplus}"
    assert surplus[0]["hint"] == ar.SURPLUS_SWEEP_INDEX_NAME

    _label, _c, _q, _sort, _expected, checklist_hint = SPEC_BY_LABEL["surplus sweep"]
    assert checklist_hint == surplus[0]["hint"], (
        "the checklist's surplus-sweep hint has drifted from what "
        "reconcile_surplus_denomination_allocations actually sends"
    )


def test_the_retry_index_matches_the_catalogue_definition():
    """The checklist names the index the catalogue actually creates."""
    requested = []

    class _Rec:
        name = "affiliate_ledger"

        def create_index(self, keys, **kw):
            requested.append((kw.get("name"), tuple(tuple(k) for k in keys)))
            return kw.get("name")

        def list_indexes(self):
            return []

        def __getattr__(self, item):
            return lambda *a, **k: None

    class _Db:
        def __getattr__(self, name):
            if name.startswith("_"):
                raise AttributeError(name)
            return _Rec() if name == "affiliate_ledger" else _Rec()

        def __getitem__(self, name):
            return getattr(self, name)

    ar.ensure_affiliate_indexes(_Db())
    by_name = dict(requested)
    assert by_name.get("affiliate_type_status_retry_checked") == (
        ("ledger_type", 1), ("status", 1), ("retry_checked_at", 1), ("_id", 1)
    ), f"the retry index is not what the checklist expects: {by_name.get('affiliate_type_status_retry_checked')}"


class TestThePlanGateActuallyGates:
    """The failure conditions must be detected, not just described."""

    def test_a_collscan_is_detected(self):
        assert "COLLSCAN" in _stage_names(
            {"stage": "LIMIT", "inputStage": {"stage": "COLLSCAN"}})

    def test_a_blocking_sort_is_detected(self):
        assert "SORT" in _stage_names(
            {"stage": "SORT", "inputStage": {"stage": "IXSCAN"}})

    def test_an_index_scan_without_a_sort_stage_is_clean(self):
        stages = _stage_names(
            {"stage": "LIMIT", "inputStage": {"stage": "FETCH",
                                              "inputStage": {"stage": "IXSCAN"}}})
        assert "COLLSCAN" not in stages and "SORT" not in stages

    def test_the_examined_budget_is_defined_and_tight(self):
        assert 1 <= _MAX_EXAMINED_PER_RETURNED <= 100

    def test_stage_names_walks_branching_plans(self):
        stages = _stage_names({
            "stage": "SUBPLAN",
            "inputStages": [{"stage": "IXSCAN"}, {"stage": "COLLSCAN"}],
        })
        assert "COLLSCAN" in stages, (
            "a COLLSCAN hiding in one branch of an $or plan must still be found"
        )
