"""No two index names may request the same key pattern.

MongoDB rejects a second index over an existing key pattern under a
different name with `IndexOptionsConflict` (code 85). Because that check
happens server-side, and the test doubles used elsewhere in this repo record
`create_index` without enforcing anything, a duplicate definition can pass
every existing test and then crash real startup.

A previous change added `aff_batch_pool_window` on
`(pool_id, starts_at, ends_at)` — the exact key pattern of the pre-existing
`batch_pool_window`. These tests exist so that cannot recur.
"""
from __future__ import annotations

import collections

import pytest
from pymongo.errors import OperationFailure

import affiliate_rewards
import affiliate_voucher_batches


def _normalize(keys):
    """A key pattern as Mongo compares it: ordered (field, direction)."""
    if isinstance(keys, dict):
        return tuple((k, int(v)) for k, v in keys.items())
    return tuple((k, int(v)) for k, v in keys)


class _RecordingCollection:
    """Records every requested index. Enforces nothing — used to inspect the
    catalogue this application asks for."""

    def __init__(self, name, registry, existing=None):
        self.name = name
        self._registry = registry
        self._existing = list(existing or [])

    def create_index(self, keys, **kwargs):
        name = kwargs.get("name")
        self._registry[self.name].append({
            "name": name,
            "keys": _normalize(keys),
            "unique": bool(kwargs.get("unique", False)),
            "partial": repr(kwargs.get("partialFilterExpression")),
        })
        return name

    def list_indexes(self):
        return list(self._existing)

    def __getattr__(self, item):
        def _noop(*a, **k):
            return None
        return _noop


class _RecordingDb:
    def __init__(self, existing_by_collection=None):
        self.registry = collections.defaultdict(list)
        self._existing = existing_by_collection or {}
        self._cols = {}

    def _col(self, name):
        if name not in self._cols:
            self._cols[name] = _RecordingCollection(
                name, self.registry, self._existing.get(name)
            )
        return self._cols[name]

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._col(name)

    def __getitem__(self, name):
        return self._col(name)


class _ConflictEnforcingCollection(_RecordingCollection):
    """Behaves like real MongoDB for the one rule that matters here: a second
    index over an already-indexed key pattern, under a different name, raises
    IndexOptionsConflict (code 85)."""

    def create_index(self, keys, **kwargs):
        norm = _normalize(keys)
        name = kwargs.get("name")
        unique = bool(kwargs.get("unique", False))
        partial = repr(kwargs.get("partialFilterExpression"))
        for prior in self._registry[self.name]:
            if prior["keys"] != norm:
                continue
            if prior["name"] == name:
                # Same name + same keys: idempotent no-op, as Mongo allows.
                if prior["unique"] == unique and prior["partial"] == partial:
                    return name
            raise OperationFailure(
                f"Index with pattern {list(norm)} already exists with a "
                f"different name ({prior['name']} vs {name})",
                85,
            )
        return super().create_index(keys, **kwargs)


class _ConflictEnforcingDb(_RecordingDb):
    def _col(self, name):
        if name not in self._cols:
            self._cols[name] = _ConflictEnforcingCollection(
                name, self.registry, self._existing.get(name)
            )
        return self._cols[name]


def _build_full_catalogue(db):
    """Every index this application requests at startup."""
    affiliate_rewards.ensure_affiliate_indexes(db)
    affiliate_voucher_batches.ensure_affiliate_voucher_batch_indexes(db)
    return db.registry


class TestNoEquivalentKeyPatterns:
    def test_no_collection_requests_one_key_pattern_under_two_names(self):
        registry = _build_full_catalogue(_RecordingDb())
        offenders = []
        for collection, indexes in registry.items():
            by_pattern = collections.defaultdict(set)
            for idx in indexes:
                by_pattern[(idx["keys"], idx["unique"], idx["partial"])].add(idx["name"])
            for pattern, names in by_pattern.items():
                if len(names) > 1:
                    offenders.append(f"{collection}: {list(pattern[0])} -> {sorted(names)}")
        assert not offenders, (
            "these key patterns are requested under more than one name, which real "
            "MongoDB rejects with IndexOptionsConflict (code 85) at startup:\n  "
            + "\n  ".join(offenders)
        )

    def test_startup_survives_a_conflict_enforcing_backend(self):
        # The recording double above cannot catch a conflict; this one can.
        db = _ConflictEnforcingDb()
        _build_full_catalogue(db)  # must not raise

    def test_the_conflict_enforcing_double_actually_enforces(self):
        """If this double were permissive, the test above would be worthless."""
        db = _ConflictEnforcingDb()
        db.some_collection.create_index([("a", 1), ("b", 1)], name="first")
        with pytest.raises(OperationFailure) as exc:
            db.some_collection.create_index([("a", 1), ("b", 1)], name="second")
        assert exc.value.code == 85

    def test_batch_window_pattern_is_owned_by_the_pre_existing_index(self):
        registry = _build_full_catalogue(_RecordingDb())
        pattern = (("pool_id", 1), ("starts_at", 1), ("ends_at", 1))
        names = [i["name"] for i in registry["affiliate_voucher_batches"]
                 if i["keys"] == pattern]
        assert names == ["batch_pool_window"], (
            f"(pool_id, starts_at, ends_at) must be served only by the pre-existing "
            f"batch_pool_window index; found {names}"
        )

    def test_no_redundant_pool_id_only_index_on_batches(self):
        registry = _build_full_catalogue(_RecordingDb())
        names = [i["name"] for i in registry["affiliate_voucher_batches"]
                 if i["keys"] == (("pool_id", 1),)]
        assert names == [], (
            f"a pool_id-only index is a redundant prefix of batch_pool_window; found {names}"
        )


class TestIdempotentAgainstRealWorldStates:
    """ensure_indexes() must be safe on every partially-applied state."""

    PRE_EXISTING_BATCH = [
        {"name": "batch_pool_window",
         "key": {"pool_id": 1, "starts_at": 1, "ends_at": 1}},
        {"name": "batch_ends_at", "key": {"ends_at": 1}},
        {"name": "batch_distribution_disabled", "key": {"distribution_disabled": 1}},
    ]

    def _run(self, existing=None):
        db = _ConflictEnforcingDb(existing_by_collection=existing or {})
        _build_full_catalogue(db)
        return db

    def test_empty_collection(self):
        self._run()

    def test_with_the_pre_existing_production_index_catalogue(self):
        self._run({"affiliate_voucher_batches": self.PRE_EXISTING_BATCH})

    def test_with_the_new_indexes_already_present(self):
        existing = {"affiliate_voucher_batches": self.PRE_EXISTING_BATCH + [
            {"name": "aff_batch_pool_window",
             "key": {"pool_id": 1, "starts_at": 1, "ends_at": 1}},
        ]}
        # Even if a partially-applied environment already carries the old
        # duplicate name, startup must not try to create it again.
        self._run(existing)

    def test_with_both_names_present_from_a_partial_rollout(self):
        existing = {"affiliate_voucher_batches": self.PRE_EXISTING_BATCH + [
            {"name": "aff_batch_pool", "key": {"pool_id": 1}},
            {"name": "aff_batch_pool_window",
             "key": {"pool_id": 1, "starts_at": 1, "ends_at": 1}},
        ]}
        self._run(existing)

    def test_repeated_process_startup(self):
        db = _ConflictEnforcingDb(
            existing_by_collection={"affiliate_voucher_batches": self.PRE_EXISTING_BATCH}
        )
        for _ in range(3):
            db._cols.clear()  # a fresh process, same underlying database
            _build_full_catalogue(db)

    def test_no_existing_index_is_dropped_at_startup(self):
        dropped = []

        class _DropSpy(_ConflictEnforcingCollection):
            def drop_index(self, *a, **k):
                dropped.append((self.name, a, k))

            def drop_indexes(self, *a, **k):
                dropped.append((self.name, "drop_indexes", k))

        class _DropSpyDb(_ConflictEnforcingDb):
            def _col(self, name):
                if name not in self._cols:
                    self._cols[name] = _DropSpy(
                        name, self.registry, self._existing.get(name)
                    )
                return self._cols[name]

        db = _DropSpyDb(
            existing_by_collection={"affiliate_voucher_batches": self.PRE_EXISTING_BATCH}
        )
        _build_full_catalogue(db)
        assert dropped == [], f"startup dropped an existing index: {dropped}"
