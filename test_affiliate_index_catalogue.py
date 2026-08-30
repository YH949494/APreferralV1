"""No two index names may request the same key pattern — and startup must
survive every partially-migrated database this migration can land on.

MongoDB rejects a second index over an already-indexed key pattern under a
different name with ``IndexOptionsConflict`` (code 85), and rejects reusing a
name over a different key spec with ``IndexKeySpecsConflict`` (code 86). Both
checks happen server-side, so a test double that merely *records*
``create_index`` cannot see either: a duplicate definition passes every such
test and then kills real startup.

Two concrete states make this more than theoretical. An intermediate commit of
this migration created ``aff_batch_pool_window`` over
``(pool_id, starts_at, ends_at)`` — the exact key pattern of the pre-existing
``batch_pool_window`` — and ``aff_batch_pool`` over ``(pool_id,)``. Any
database that ran that commit still carries those names.

So the doubles here enforce, against BOTH the indexes a run creates and the
``_existing`` catalogue the database started with. `_existing` is the half
that was missing: without it, a raw ``create_index`` colliding with a
pre-existing index looked perfectly fine.
"""
from __future__ import annotations

import collections

import pytest
from pymongo.errors import OperationFailure

import affiliate_rewards
import affiliate_voucher_batches

BATCH = "affiliate_voucher_batches"
WINDOW_KEYS = [("pool_id", 1), ("starts_at", 1), ("ends_at", 1)]


def _normalize(keys):
    """A key pattern as Mongo compares it: ordered (field, direction)."""
    if isinstance(keys, dict):
        return tuple((k, int(v)) for k, v in keys.items())
    return tuple((k, int(v)) for k, v in keys)


def _idx(name, keys, **extra):
    """One row as ``list_indexes()`` would report it."""
    doc = {"name": name, "key": dict(keys)}
    doc.update(extra)
    return doc


class _RecordingCollection:
    """Records every requested index. Enforces nothing — used to inspect the
    catalogue this application asks for."""

    def __init__(self, name, registry, existing=None):
        self.name = name
        self._registry = registry
        self._existing = [dict(i) for i in (existing or [])]

    def create_index(self, keys, **kwargs):
        name = kwargs.get("name")
        self._registry[self.name].append({
            "name": name,
            "keys": _normalize(keys),
            "unique": bool(kwargs.get("unique", False)),
            "partial": repr(kwargs.get("partialFilterExpression")),
            "kwargs": dict(kwargs),
        })
        return name

    def _all_indexes(self):
        """Everything the database would now report: the catalogue it started
        with, plus anything created during this run. Without the second half,
        a repeated startup would not see its own first pass."""
        out = [dict(i) for i in self._existing]
        seen = {i.get("name") for i in out}
        for made in self._registry[self.name]:
            if made["name"] in seen:
                continue
            row = {"name": made["name"], "key": dict(made["keys"])}
            if made["unique"]:
                row["unique"] = True
            partial = made["kwargs"].get("partialFilterExpression")
            if partial is not None:
                row["partialFilterExpression"] = partial
            out.append(row)
        return out

    def list_indexes(self):
        return self._all_indexes()

    def __getattr__(self, item):
        def _noop(*a, **k):
            return None
        return _noop


class _RecordingDb:
    def __init__(self, existing_by_collection=None):
        self.registry = collections.defaultdict(list)
        self._existing = existing_by_collection or {}
        self._cols = {}

    def _make(self, name):
        return _RecordingCollection(name, self.registry, self._existing.get(name))

    def _col(self, name):
        if name not in self._cols:
            self._cols[name] = self._make(name)
        return self._cols[name]

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._col(name)

    def __getitem__(self, name):
        return self._col(name)


class _ConflictEnforcingCollection(_RecordingCollection):
    """Behaves like real MongoDB for the two rules that matter here.

    Enforced against ``_all_indexes()`` — the pre-existing catalogue AND
    anything created during this run — because a conflict with an index the
    database already had is the failure mode that actually reached
    production. Checking only what the current run created is exactly the
    blind spot that let a raw ``create_index`` look safe.

      * equivalent key pattern under a DIFFERENT name -> code 85
      * same name, same keys, incompatible options    -> code 85
      * same name over DIFFERENT keys                 -> code 86
      * same name, same keys, same options            -> idempotent no-op
    """

    def create_index(self, keys, **kwargs):
        norm = _normalize(keys)
        name = kwargs.get("name")
        unique = bool(kwargs.get("unique", False))
        partial = kwargs.get("partialFilterExpression")
        for prior in self._all_indexes():
            prior_name = prior.get("name")
            prior_keys = _normalize(prior.get("key") or {})
            if prior_keys == norm:
                if prior_name != name:
                    raise OperationFailure(
                        f"Index with pattern {list(norm)} already exists with a "
                        f"different name ({prior_name} vs {name})", 85,
                    )
                same_opts = (
                    bool(prior.get("unique", False)) == unique
                    and prior.get("partialFilterExpression") == partial
                )
                if same_opts:
                    return name  # idempotent no-op, as Mongo allows
                raise OperationFailure(
                    f"Index '{name}' already exists with different options", 85,
                )
            if prior_name == name:
                raise OperationFailure(
                    f"Index '{name}' already exists with a different key spec "
                    f"({list(prior_keys)} vs {list(norm)})", 86,
                )
        return super().create_index(keys, **kwargs)


class _ConflictEnforcingDb(_RecordingDb):
    def _make(self, name):
        return _ConflictEnforcingCollection(name, self.registry, self._existing.get(name))


class _DropSpyCollection(_ConflictEnforcingCollection):
    """Fails the test loudly if startup ever drops or renames an index."""

    def __init__(self, name, registry, existing=None, dropped=None):
        super().__init__(name, registry, existing)
        self.dropped = dropped if dropped is not None else []

    def drop_index(self, *a, **k):
        self.dropped.append((self.name, "drop_index", a, k))

    def drop_indexes(self, *a, **k):
        self.dropped.append((self.name, "drop_indexes", a, k))


class _DropSpyDb(_ConflictEnforcingDb):
    def __init__(self, existing_by_collection=None):
        super().__init__(existing_by_collection)
        self.dropped = []

    def _make(self, name):
        return _DropSpyCollection(
            name, self.registry, self._existing.get(name), dropped=self.dropped
        )


def _build_full_catalogue(db):
    """Every index this application requests at startup."""
    affiliate_rewards.ensure_affiliate_indexes(db)
    affiliate_voucher_batches.ensure_affiliate_voucher_batch_indexes(db)
    return db.registry


# ---------------------------------------------------------------------------
# The double itself must be trustworthy before anything it proves counts.
# ---------------------------------------------------------------------------

class TestTheDoubleEnforces:
    def test_it_rejects_a_second_name_created_in_this_run(self):
        db = _ConflictEnforcingDb()
        db.some_collection.create_index([("a", 1), ("b", 1)], name="first")
        with pytest.raises(OperationFailure) as exc:
            db.some_collection.create_index([("a", 1), ("b", 1)], name="second")
        assert exc.value.code == 85

    def test_it_rejects_a_second_name_over_a_PRE_EXISTING_index(self):
        """The half that was missing. Without ``_existing`` in the check, this
        collision — the one that actually reaches production — is invisible."""
        db = _ConflictEnforcingDb(existing_by_collection={
            "some_collection": [_idx("already_there", [("a", 1), ("b", 1)])],
        })
        with pytest.raises(OperationFailure) as exc:
            db.some_collection.create_index([("a", 1), ("b", 1)], name="second")
        assert exc.value.code == 85
        assert "already_there" in str(exc.value)

    def test_it_rejects_reusing_a_name_over_different_keys(self):
        db = _ConflictEnforcingDb(existing_by_collection={
            "some_collection": [_idx("dup", [("a", 1)])],
        })
        with pytest.raises(OperationFailure) as exc:
            db.some_collection.create_index([("b", 1)], name="dup")
        assert exc.value.code == 86

    def test_it_rejects_the_same_name_and_keys_with_different_options(self):
        db = _ConflictEnforcingDb(existing_by_collection={
            "some_collection": [_idx("dup", [("a", 1)], unique=True)],
        })
        with pytest.raises(OperationFailure) as exc:
            db.some_collection.create_index([("a", 1)], name="dup", unique=False)
        assert exc.value.code == 85

    def test_an_identical_request_is_an_idempotent_no_op(self):
        db = _ConflictEnforcingDb(existing_by_collection={
            "some_collection": [_idx("dup", [("a", 1)])],
        })
        assert db.some_collection.create_index([("a", 1)], name="dup") == "dup"
        assert db.registry["some_collection"] == []


# ---------------------------------------------------------------------------
# The catalogue this application asks for
# ---------------------------------------------------------------------------

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
        _build_full_catalogue(_ConflictEnforcingDb())  # must not raise

    def test_batch_window_pattern_is_owned_by_one_name_only(self):
        registry = _build_full_catalogue(_RecordingDb())
        names = [i["name"] for i in registry[BATCH] if i["keys"] == _normalize(WINDOW_KEYS)]
        assert names == ["batch_pool_window"], (
            f"(pool_id, starts_at, ends_at) must be served by exactly one name; found {names}"
        )

    def test_no_redundant_pool_id_only_index_on_batches(self):
        registry = _build_full_catalogue(_RecordingDb())
        names = [i["name"] for i in registry[BATCH] if i["keys"] == (("pool_id", 1),)]
        assert names == [], (
            f"a pool_id-only index is a redundant prefix of batch_pool_window; found {names}"
        )


# ---------------------------------------------------------------------------
# Partial-rollout states. This is the matrix the reviewer asked for.
# ---------------------------------------------------------------------------

PRODUCTION = [
    _idx("batch_pool_window", WINDOW_KEYS),
    _idx("batch_ends_at", [("ends_at", 1)]),
    _idx("batch_distribution_disabled", [("distribution_disabled", 1)]),
]
STALE_POOL = _idx("aff_batch_pool", [("pool_id", 1)])
STALE_WINDOW = _idx("aff_batch_pool_window", WINDOW_KEYS)

#: name -> the ``affiliate_voucher_batches`` catalogue the database starts with
SCENARIOS = {
    "empty database": [],
    "expected production catalogue": PRODUCTION,
    "only batch_pool_window": [PRODUCTION[0]],
    "only batch_ends_at": [PRODUCTION[1]],
    "stale aff_batch_pool only": [STALE_POOL],
    "stale aff_batch_pool_window only": [STALE_WINDOW],
    "legacy + intermediate names together": PRODUCTION + [STALE_POOL, STALE_WINDOW],
    "equivalent keys under a different name": [
        STALE_WINDOW, PRODUCTION[1], PRODUCTION[2],
    ],
}

#: What the module asks for, in order.
REQUESTED = [(name, _normalize(keys))
             for keys, name in affiliate_voucher_batches.AFFILIATE_VOUCHER_BATCH_INDEXES]


def _run_batch_indexes(existing):
    """Run ONLY the batch catalogue against ``existing``; return the spy db."""
    db = _DropSpyDb(existing_by_collection={BATCH: list(existing)})
    affiliate_voucher_batches.ensure_affiliate_voucher_batch_indexes(db)
    return db


def _final_catalogue(db):
    return sorted(
        (i["name"], _normalize(i["key"])) for i in db[BATCH]._all_indexes()
    )


class TestPartialRolloutStates:
    """``ensure_affiliate_voucher_batch_indexes`` on every state this
    migration can land on. In all of them: nothing is dropped, nothing is
    renamed, and no key pattern ends up under two names."""

    @pytest.mark.parametrize("scenario", list(SCENARIOS))
    def test_startup_succeeds_and_never_drops(self, scenario):
        existing = SCENARIOS[scenario]
        db = _run_batch_indexes(existing)

        assert db.dropped == [], f"{scenario}: startup dropped an index: {db.dropped}"

        final = _final_catalogue(db)
        # Every requested key pattern ends up served by something.
        for _name, keys in REQUESTED:
            serving = [n for n, k in final if k == keys]
            assert serving, (
                f"{scenario}: key pattern {list(keys)} is served by nothing"
            )

        def _dupes(rows):
            by_pattern = collections.defaultdict(list)
            for name, keys in rows:
                by_pattern[keys].append(name)
            return {k: sorted(v) for k, v in by_pattern.items() if len(v) > 1}

        # This run must introduce no duplication of its own. A database that
        # ALREADY carries two names over one pattern (a partial rollout) keeps
        # them: the extra index is dead weight, and dropping it here would be
        # exactly the destructive behaviour this code must never have.
        before = _dupes((i["name"], _normalize(i["key"])) for i in existing)
        after = _dupes(final)
        assert after == before, (
            f"{scenario}: this run changed the duplicate set from {before} to {after}"
        )
        # Every index present beforehand is still present afterwards.
        for row in existing:
            assert any(n == row["name"] for n, _ in final), (
                f"{scenario}: pre-existing index {row['name']} disappeared"
            )

    def test_a_fresh_database_gets_the_canonical_names(self):
        db = _run_batch_indexes([])
        assert _final_catalogue(db) == sorted(REQUESTED)

    def test_an_equivalent_index_is_reused_and_nothing_is_created(self):
        db = _run_batch_indexes(SCENARIOS["equivalent keys under a different name"])
        assert db.registry[BATCH] == [], (
            "an equivalent index already existed under another name; nothing "
            f"should have been created, but got {db.registry[BATCH]}"
        )
        assert ("aff_batch_pool_window", _normalize(WINDOW_KEYS)) in _final_catalogue(db)
        assert "batch_pool_window" not in [n for n, _ in _final_catalogue(db)]

    def test_only_the_missing_indexes_are_created(self):
        db = _run_batch_indexes([PRODUCTION[0]])
        created = sorted(i["name"] for i in db.registry[BATCH])
        assert created == ["batch_distribution_disabled", "batch_ends_at"]

    def test_stale_names_are_left_in_place_untouched(self):
        db = _run_batch_indexes(SCENARIOS["legacy + intermediate names together"])
        assert db.registry[BATCH] == []
        names = [n for n, _ in _final_catalogue(db)]
        assert "aff_batch_pool" in names and "aff_batch_pool_window" in names

    def test_three_consecutive_startups_are_idempotent(self):
        db = _DropSpyDb(existing_by_collection={BATCH: list(PRODUCTION)})
        snapshots = []
        for _ in range(3):
            db._cols.clear()  # a fresh process against the same database
            affiliate_voucher_batches.ensure_affiliate_voucher_batch_indexes(db)
            snapshots.append(_final_catalogue(db))
        assert snapshots[0] == snapshots[1] == snapshots[2] == sorted(REQUESTED)
        assert db.registry[BATCH] == []
        assert db.dropped == []

    def test_three_consecutive_startups_from_empty_create_once(self):
        db = _DropSpyDb(existing_by_collection={BATCH: []})
        for _ in range(3):
            db._cols.clear()
            affiliate_voucher_batches.ensure_affiliate_voucher_batch_indexes(db)
        created = sorted(i["name"] for i in db.registry[BATCH])
        assert created == sorted(n for n, _ in REQUESTED), (
            f"repeated startup created indexes more than once: {created}"
        )
        assert db.dropped == []

    @pytest.mark.parametrize("existing,expected_code", [
        # Same name, different key spec.
        ([_idx("batch_ends_at", [("ends_at", -1)])], 86),
        # Same name, same keys, incompatible options.
        ([_idx("batch_pool_window", WINDOW_KEYS, unique=True)], 85),
    ])
    def test_an_incompatible_same_name_index_raises_and_drops_nothing(
        self, existing, expected_code
    ):
        """A name whose index does not do what this code believes it does is a
        real conflict. The helper refuses rather than dropping it: removing an
        index is an operator decision, not a deploy side effect."""
        db = _DropSpyDb(existing_by_collection={BATCH: list(existing)})
        with pytest.raises(OperationFailure) as exc:
            affiliate_voucher_batches.ensure_affiliate_voucher_batch_indexes(db)
        assert exc.value.code in (expected_code, 85)
        assert db.dropped == [], "a conflict must never be resolved by dropping"


class TestRawCreateIndexIsTheBug:
    """The regression itself: a raw ``create_index`` fails where the helper
    succeeds, on the exact state an intermediate migration commit leaves."""

    EXISTING = [STALE_WINDOW, PRODUCTION[1], PRODUCTION[2]]

    def test_raw_create_index_reproduces_mongodb_code_85(self):
        db = _ConflictEnforcingDb(existing_by_collection={BATCH: list(self.EXISTING)})
        with pytest.raises(OperationFailure) as exc:
            db.affiliate_voucher_batches.create_index(
                WINDOW_KEYS, name="batch_pool_window"
            )
        assert exc.value.code == 85
        assert "aff_batch_pool_window" in str(exc.value)

    def test_the_production_helper_reuses_it_instead(self):
        db = _run_batch_indexes(self.EXISTING)
        assert db.registry[BATCH] == [], "nothing should have been created"
        assert db.dropped == [], "nothing should have been dropped"
        assert _final_catalogue(db) == sorted(
            (i["name"], _normalize(i["key"])) for i in self.EXISTING
        ), "the catalogue must be exactly what it was before"

    def test_full_startup_survives_that_state(self):
        db = _ConflictEnforcingDb(existing_by_collection={BATCH: list(self.EXISTING)})
        _build_full_catalogue(db)  # must not raise


def test_report_the_index_matrix(capsys):
    """Prints requested / existing / final for every scenario, so the matrix
    in the review is generated from the code rather than asserted from memory."""
    lines = ["", "scenario | existing -> final (created)"]
    for scenario, existing in SCENARIOS.items():
        db = _run_batch_indexes(existing)
        created = sorted(i["name"] for i in db.registry[BATCH]) or ["-"]
        lines.append(
            f"  {scenario}\n"
            f"    requested: {[n for n, _ in REQUESTED]}\n"
            f"    existing : {[i['name'] for i in existing] or ['-']}\n"
            f"    final    : {[n for n, _ in _final_catalogue(db)]}\n"
            f"    created  : {created}   dropped: {db.dropped or ['-']}"
        )
    with capsys.disabled():
        print("\n".join(lines))
