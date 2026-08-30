"""The stuck-ledger retry sweep must be bounded, fair, and lease-aware.

Three defects, all in `_retry_stuck_pending_manual_affiliate_ledgers`:

  * it materialized `list(find(query))` for BOTH classes with no limit and no
    order, then sliced afterwards — so on a production-sized collection it
    pulled the whole eligible set into memory every tick;
  * PENDING_MANUAL rows were concatenated ahead of SETTLING rows, so a large
    PENDING_MANUAL population (a pool that has run dry leaves one row per
    affected user, and they never resolve on their own) consumed the entire
    slice and stale SETTLING recovery never ran at all;
  * eligibility for SETTLING was decided by `updated_at`, contradicting the
    lease-based liveness the surplus sweep and the allocator already use. An
    allocator that has been renewing its lease can carry a stale
    `updated_at`, so that row could be retried out from under a bundle still
    in flight.

`batch_limit` bounds the ledgers PROCESSED per tick across both classes. Each
of the two queries is independently bounded by the same number, so at most
`2 * batch_limit` documents are materialized and never the whole eligible set.
"""
from __future__ import annotations

import textwrap
from datetime import datetime, timedelta, timezone

import pytest

import affiliate_rewards as ar
import affiliate_reward_plans as arp
from fake_mongo import FakeDb

SEP = datetime(2026, 9, 10, 4, 0, tzinfo=timezone.utc)
TTL = timedelta(seconds=ar._ALLOCATION_LEASE_TTL_SECONDS)
#: comfortably past the lease TTL, so these rows are genuinely abandoned
OLD = SEP - TTL - timedelta(hours=1)
MONTH = "202609"
UNIQUE = {"affiliate_ledger": [("dedup_key",)], "voucher_pools": [("pool_id", "code")]}


class _QuerySpy:
    """Records every `affiliate_ledger.find` call and what it returned."""

    def __init__(self, inner):
        self._inner = inner
        self.calls = []

    def __getattr__(self, name):
        col = getattr(self._inner, name)
        if name != "affiliate_ledger":
            return col
        spy = self

        class _Col:
            def find(self, query=None, **kwargs):
                rows = col.find(query, **kwargs)
                spy.calls.append({"query": query, "kwargs": kwargs, "returned": len(rows)})
                return rows

            def __getattr__(self, item):
                return getattr(col, item)

        return _Col()

    def __getitem__(self, name):
        return getattr(self, name)

    def reset(self):
        self.calls = []


def _db():
    return FakeDb(UNIQUE)


def _base(idx: int, uid: int) -> dict:
    return {
        "ledger_type": "AFFILIATE_MONTHLY",
        "tier": "T1",
        "user_id": uid,
        "year_month": MONTH,
        "entitlement_month": MONTH,
        "reward_plan": arp.DENOMINATION_PLAN_ID,
        "bundle_recipe": arp.tier_recipe(MONTH, "T1"),
        "dedup_key": f"AFF:{uid}:{MONTH}:T1",
    }


def _seed_pending(db, n, *, prefix="P"):
    """PENDING_MANUAL rows flagged pool_empty — eligible, but permanently
    unsatisfiable while the pool has no stock."""
    for i in range(n):
        uid = 5000 + i
        db.affiliate_ledger.insert_one({
            "_id": f"{prefix}{i:04d}", **_base(i, uid),
            "status": "PENDING_MANUAL",
            "risk_flags": ["pool_empty"],
            "updated_at": OLD,
        })


def _seed_settling(db, n, *, prefix="S", lease_at=OLD, count_from=0):
    """SETTLING rows whose allocation lease expired long ago."""
    for i in range(count_from, count_from + n):
        uid = 9000 + i
        doc = {
            "_id": f"{prefix}{i:04d}", **_base(i, uid),
            "status": ar.SETTLING_STATUS,
            "updated_at": OLD,
        }
        if lease_at is not None:
            doc["allocation_lease_at"] = lease_at
        db.affiliate_ledger.insert_one(doc)


@pytest.fixture
def stub_issue(monkeypatch):
    """Replace the issuance call so these tests measure SCHEDULING only.

    Default behaviour is "still no stock": the ledger stays in its class, so
    a class that is never scheduled stays visibly un-drained.
    """
    seen = []

    def _stub(db, *, ledger, now_utc):
        # Faithful to "the pool is still empty": the real call puts a
        # PENDING_MANUAL ledger back to PENDING_MANUAL rather than leaving it
        # parked in SETTLING. A crashed allocator (an S row) stays SETTLING.
        seen.append(ledger["_id"])
        original = ("PENDING_MANUAL" if str(ledger["_id"]).startswith("P")
                    else ar.SETTLING_STATUS)
        db.affiliate_ledger.update_one(
            {"_id": ledger["_id"]}, {"$set": {"status": original}}
        )
        return db.affiliate_ledger.find_one({"_id": ledger["_id"]})

    monkeypatch.setattr(ar, "_issue_affiliate_ledger_from_pool", _stub)
    return seen


def _touched(db) -> set:
    return {r["_id"] for r in db.affiliate_ledger.find({"retry_attempts": {"$gte": 1}})}


def _cls(ids, prefix):
    return {i for i in ids if i.startswith(prefix)}


# ---------------------------------------------------------------------------
# Boundedness
# ---------------------------------------------------------------------------

class TestBounded:
    def test_every_query_is_bounded_and_sorted_at_the_database(self, stub_issue):
        db = _db()
        _seed_pending(db, 60)
        _seed_settling(db, 60)
        spy = _QuerySpy(db)

        ar._retry_stuck_pending_manual_affiliate_ledgers(spy, now_utc=SEP, batch_limit=5)

        # Identify selection queries by their SORT, not by whether they happen
        # to carry a limit — keying on `limit` would make this assertion pass
        # vacuously the moment the limit is dropped, which is the exact bug.
        selection = [c for c in spy.calls if c["kwargs"].get("sort")]
        assert len(selection) == 2, "one bounded query per class, no more"
        for call in selection:
            assert call["kwargs"].get("limit") == 5, (
                f"an unbounded selection query would materialize the whole "
                f"eligible set: {call['kwargs']}"
            )
            assert call["kwargs"]["sort"] == ar._RETRY_SCAN_ORDER, (
                "without a database-side sort the same head is re-read every tick"
            )
            assert call["returned"] <= 5

    def test_total_work_per_tick_respects_the_documented_bound(self, stub_issue):
        db = _db()
        _seed_pending(db, 60)
        _seed_settling(db, 60)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP, batch_limit=5)
        assert len(_touched(db)) == 5, (
            "batch_limit bounds ledgers PROCESSED per tick across both classes"
        )

    def test_it_never_materializes_the_whole_eligible_set(self, stub_issue):
        db = _db()
        _seed_pending(db, 60)
        _seed_settling(db, 60)
        spy = _QuerySpy(db)
        ar._retry_stuck_pending_manual_affiliate_ledgers(spy, now_utc=SEP, batch_limit=5)
        selection = [c for c in spy.calls if c["kwargs"].get("sort")]
        assert selection, "no selection query was issued at all"
        returned = sum(c["returned"] for c in selection)
        assert returned <= 2 * 5, (
            f"the selection queries materialized {returned} documents for a "
            f"batch_limit of 5 — on a production-sized collection that is the "
            f"whole eligible set"
        )


# ---------------------------------------------------------------------------
# Fairness
# ---------------------------------------------------------------------------

class TestFairness:
    def test_both_classes_make_progress_in_a_single_tick(self, stub_issue):
        db = _db()
        _seed_pending(db, 60)
        _seed_settling(db, 60)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP, batch_limit=5)
        touched = _touched(db)
        assert _cls(touched, "P"), "PENDING_MANUAL made no progress"
        assert _cls(touched, "S"), "stale SETTLING made no progress"

    def test_a_permanently_stuck_pending_population_cannot_starve_settling(self, stub_issue):
        """The original failure: 60 unsatisfiable PENDING_MANUAL rows were
        concatenated ahead of SETTLING and consumed every slice forever."""
        db = _db()
        _seed_pending(db, 60)
        _seed_settling(db, 60)
        for tick in range(12):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=5
            )
        settling_seen = _cls(_touched(db), "S")
        assert len(settling_seen) >= 12, (
            f"stale SETTLING recovery was starved: only {len(settling_seen)} of 60 "
            f"were ever considered across 12 ticks"
        )

    def test_the_quota_is_explicit_and_guarantees_both_classes_a_slot(self):
        for limit in range(2, 12):
            settling, pending = ar._retry_class_quotas(limit)
            assert settling >= 1 and pending >= 1, limit
            assert settling + pending == limit, limit

    def test_batch_limit_of_one_is_defined_and_serves_the_oldest_head(self, stub_issue):
        """At a limit of one there is no way to serve both classes in a tick.
        The slot goes to the least-recently-considered ledger, so the classes
        alternate rather than one being locked out."""
        db = _db()
        _seed_pending(db, 3)
        _seed_settling(db, 3)
        for tick in range(6):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=1
            )
        touched = _touched(db)
        assert len(touched) == 6, "exactly one ledger per tick"
        assert _cls(touched, "P") and _cls(touched, "S"), (
            f"one class was locked out entirely at batch_limit=1: {sorted(touched)}"
        )

    def test_unused_capacity_spills_to_the_other_class(self, stub_issue):
        db = _db()
        _seed_pending(db, 20)
        _seed_settling(db, 1)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP, batch_limit=6)
        touched = _touched(db)
        assert len(touched) == 6, "a quiet class must not waste the tick's capacity"
        assert len(_cls(touched, "S")) == 1
        assert len(_cls(touched, "P")) == 5


# ---------------------------------------------------------------------------
# Every eligible record is eventually considered
# ---------------------------------------------------------------------------

class TestNoStarvation:
    def test_repeated_ticks_walk_the_whole_eligible_set(self, stub_issue):
        db = _db()
        _seed_pending(db, 60)
        _seed_settling(db, 60)
        for tick in range(40):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=5
            )
        touched = _touched(db)
        assert len(_cls(touched, "P")) == 60 and len(_cls(touched, "S")) == 60, (
            f"records were never considered: "
            f"{60 - len(_cls(touched, 'P'))} pending, {60 - len(_cls(touched, 'S'))} settling"
        )

    def test_a_no_stock_record_does_not_get_picked_over_and_over(self, stub_issue):
        db = _db()
        _seed_pending(db, 10)
        for tick in range(2):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=4
            )
        attempts = {r["_id"]: r.get("retry_attempts", 0)
                    for r in db.affiliate_ledger.find({"status": "PENDING_MANUAL"})}
        assert max(attempts.values()) == 1, (
            f"a record was re-selected before later records had their turn: {attempts}"
        )
        assert sum(1 for v in attempts.values() if v == 1) == 8

    def test_new_stale_settling_work_is_eventually_selected(self, stub_issue):
        db = _db()
        _seed_pending(db, 40)
        for tick in range(4):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=4
            )
        # A worker crashes now, leaving a fresh stranded row.
        _seed_settling(db, 1, count_from=99)
        ar._retry_stuck_pending_manual_affiliate_ledgers(
            db, now_utc=SEP + timedelta(minutes=30), batch_limit=4
        )
        assert "S0099" in _touched(db), (
            "a newly stranded SETTLING ledger must jump the queue: it has never "
            "been considered, and a missing retry_checked_at sorts first"
        )

    def test_a_malformed_record_does_not_block_later_records(self, stub_issue):
        db = _db()
        _seed_pending(db, 6)
        # A genuinely corrupt row: risk_flags is not iterable, so the sweep
        # raises while inspecting it.
        db.affiliate_ledger.update_one({"_id": "P0000"}, {"$set": {"risk_flags": 12345}})

        first = ar._retry_stuck_pending_manual_affiliate_ledgers(
            db, now_utc=SEP, batch_limit=2
        )
        assert first["errors"] == 1
        assert len(_touched(db)) == 2, "the batch continued past the malformed row"

        for tick in range(1, 3):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=2
            )
        assert len(_touched(db)) == 6, (
            "the malformed row must go to the BACK of the queue, not wedge it"
        )

    def test_repeated_calls_are_safe(self, stub_issue):
        db = _db()
        _seed_pending(db, 5)
        _seed_settling(db, 5)
        for tick in range(6):
            out = ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=5
            )
            assert out["errors"] == 0
        assert db.affiliate_ledger.count_documents({}) == 10, "no row created or lost"


# ---------------------------------------------------------------------------
# Lease liveness — the canonical helpers, not updated_at
# ---------------------------------------------------------------------------

class TestLeaseLiveness:
    """Controlled-time boundary tests. `_allocation_lease_is_live` is live iff
    `allocation_lease_at >= _allocation_lease_expiry_cutoff(ref)`, and the
    selection query is eligible iff `allocation_lease_at < cutoff` — the two
    are exact complements, so no ledger is both live and eligible."""

    def _one_settling(self, db, **over):
        doc = {
            "_id": "S0000", **_base(0, 9000),
            "status": ar.SETTLING_STATUS,
            "updated_at": OLD,          # deliberately stale in every case
        }
        doc.update(over)
        db.affiliate_ledger.insert_one(doc)
        return db.affiliate_ledger.find_one({"_id": "S0000"})

    def test_a_live_lease_beats_a_stale_updated_at(self, stub_issue):
        db = _db()
        self._one_settling(db, allocation_lease_at=SEP)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP, batch_limit=5)
        assert _touched(db) == set(), (
            "updated_at is not a liveness signal: a worker renewing its lease "
            "can carry a stale updated_at, and retrying it would fight a bundle "
            "still in flight"
        )

    def test_one_microsecond_before_expiry_is_still_live(self):
        cutoff = ar._allocation_lease_expiry_cutoff(SEP)
        led = {"status": ar.SETTLING_STATUS,
               "allocation_lease_at": cutoff + timedelta(microseconds=1)}
        assert ar._allocation_lease_is_live(led, reference=SEP) is True

    def test_the_exact_expiry_boundary_is_live(self):
        cutoff = ar._allocation_lease_expiry_cutoff(SEP)
        led = {"status": ar.SETTLING_STATUS, "allocation_lease_at": cutoff}
        assert ar._allocation_lease_is_live(led, reference=SEP) is True

    def test_one_microsecond_after_expiry_is_not_live(self):
        cutoff = ar._allocation_lease_expiry_cutoff(SEP)
        led = {"status": ar.SETTLING_STATUS,
               "allocation_lease_at": cutoff - timedelta(microseconds=1)}
        assert ar._allocation_lease_is_live(led, reference=SEP) is False

    def test_the_boundary_matches_the_surplus_sweep_exactly(self, stub_issue):
        """Both sweeps must agree on the instant a lease dies — they call the
        same helper, so the retry sweep's selection is the exact complement of
        `_allocation_lease_is_live`."""
        cutoff = ar._allocation_lease_expiry_cutoff(SEP)
        for delta, expect_retried in ((timedelta(microseconds=1), False),
                                      (timedelta(0), False),
                                      (timedelta(microseconds=-1), True)):
            db = _db()
            lease_at = cutoff + delta
            self._one_settling(db, allocation_lease_at=lease_at)
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP, batch_limit=5
            )
            retried = bool(_touched(db))
            live = ar._allocation_lease_is_live(
                {"status": ar.SETTLING_STATUS, "allocation_lease_at": lease_at},
                reference=SEP,
            )
            assert retried == expect_retried, f"delta={delta}"
            assert retried is not live, (
                f"delta={delta}: a ledger must be either live or eligible, never both"
            )

    def test_a_renewed_lease_is_protected_even_after_selection(self, monkeypatch):
        """Selection is only the first gate: a worker that acquires the lease
        between the query and processing is still protected."""
        db = _db()
        self._one_settling(db, allocation_lease_at=OLD)
        real_find_one = db.affiliate_ledger.find_one
        state = {"renewed": False}

        def _renew_then_read(*args, **kw):
            row = real_find_one(*args, **kw)
            if row is not None and not state["renewed"]:
                state["renewed"] = True
                db.affiliate_ledger.update_one(
                    {"_id": row["_id"]}, {"$set": {"allocation_lease_at": SEP}}
                )
                return real_find_one(*args, **kw)
            return row

        monkeypatch.setattr(db.affiliate_ledger, "find_one", _renew_then_read)
        out = ar._retry_stuck_pending_manual_affiliate_ledgers(
            db, now_utc=SEP, batch_limit=5
        )
        assert out["skipped_live"] == 1
        assert out["scanned"] == 0
        assert _touched(db) == set(), (
            "a ledger skipped for a live lease must NOT be stamped — it stays at "
            "the front of the queue and is revisited once the lease lapses"
        )

    def test_a_missing_lease_falls_back_to_updated_at_for_eligibility(self, stub_issue):
        db = _db()
        self._one_settling(db)  # no allocation_lease_at at all
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP, batch_limit=5)
        assert _touched(db) == {"S0000"}

    def test_a_null_lease_falls_back_to_updated_at_for_eligibility(self, stub_issue):
        db = _db()
        self._one_settling(db, allocation_lease_at=None)
        ar._retry_stuck_pending_manual_affiliate_ledgers(db, now_utc=SEP, batch_limit=5)
        assert _touched(db) == {"S0000"}

    def test_a_missing_lease_is_never_treated_as_live(self):
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS}, reference=SEP) is False
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": None},
            reference=SEP) is False

    @pytest.mark.parametrize("bad", ["2026-09-10T04:00:00Z", 1757476800, "", [], {}, object()])
    def test_a_malformed_lease_is_not_live(self, bad):
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": bad},
            reference=SEP) is False

    def test_a_naive_lease_datetime_is_read_as_utc(self):
        cutoff = ar._allocation_lease_expiry_cutoff(SEP)
        naive_live = (cutoff + timedelta(seconds=1)).replace(tzinfo=None)
        naive_dead = (cutoff - timedelta(seconds=1)).replace(tzinfo=None)
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": naive_live},
            reference=SEP) is True
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": naive_dead},
            reference=SEP) is False

    def test_a_naive_reference_is_read_as_utc(self):
        assert ar._allocation_lease_expiry_cutoff(SEP.replace(tzinfo=None)) == \
            ar._allocation_lease_expiry_cutoff(SEP)

    def test_the_ttl_arithmetic_is_not_duplicated_in_the_retry_sweep(self):
        """One definition of the cutoff, shared. A second copy is how the two
        sweeps drift apart and start disagreeing about who is alive.

        Analysed as an AST rather than as source text: the function's own
        docstring names both helpers, so a substring search would keep passing
        after the calls were removed — exactly the vacuous assertion this
        review is about."""
        import ast
        import inspect

        tree = ast.parse(
            textwrap.dedent(
                inspect.getsource(ar._retry_stuck_pending_manual_affiliate_ledgers)
            )
        )
        called = {
            n.func.id for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
        names = {
            n.id for n in ast.walk(tree) if isinstance(n, ast.Name)
        }
        assert "_allocation_lease_expiry_cutoff" in called, (
            f"the retry sweep does not call the shared cutoff helper; it calls {sorted(called)}"
        )
        assert "_allocation_lease_is_live" in called, (
            f"the retry sweep does not call the shared liveness helper; it calls {sorted(called)}"
        )
        assert "_ALLOCATION_LEASE_TTL_SECONDS" not in names, (
            "the retry sweep must not re-derive the TTL boundary itself"
        )


# ---------------------------------------------------------------------------
# One clock, one boundary, across BOTH sweeps
# ---------------------------------------------------------------------------

class TestBothSweepsShareTheSameClockAndBoundary:
    """The surplus sweep took `now_utc` but then asked the lease helpers about
    the WALL clock, so a simulated-time test exercised whatever
    `datetime.now()` happened to be rather than the boundary it named. Both
    sweeps now pass the injected reference through, so they classify the same
    lease identically."""

    def _settling_ledger(self, db, lease_at, *, _id="S0000"):
        db.affiliate_ledger.insert_one({
            "_id": _id, **_base(0, 9000),
            "status": ar.SETTLING_STATUS,
            "updated_at": OLD,
            "allocation_lease_at": lease_at,
        })

    def _denomination_settling(self, db, lease_at, *, _id="S0000"):
        doc = {
            "_id": _id, **_base(0, 9000),
            "status": ar.SETTLING_STATUS,
            "updated_at": OLD,
            "allocation_lease_at": lease_at,
            "reward_plan": arp.DENOMINATION_PLAN_ID,
            "entitlement_month": MONTH,
        }
        db.affiliate_ledger.insert_one(doc)

    def _surplus_considered(self, db, lease_at, reference):
        """Did the surplus sweep CONSIDER this ledger at `reference`?

        A live allocator is excluded by the selection query itself, so it is
        never stamped; `skipped_live` only counts the race where a lease is
        acquired between the query and processing. Stamping is therefore the
        observable that both sweeps share.
        """
        self._denomination_settling(db, lease_at)
        ar.reconcile_surplus_denomination_allocations(
            db, now_utc=reference, batch_limit=5
        )
        row = db.affiliate_ledger.find_one({"_id": "S0000"})
        return row.get("surplus_checked_at") is not None

    def _retry_considered(self, db, lease_at, reference):
        self._settling_ledger(db, lease_at)
        ar._retry_stuck_pending_manual_affiliate_ledgers(
            db, now_utc=reference, batch_limit=5
        )
        return bool(_touched(db))

    @pytest.mark.parametrize("offset,label,expect_live", [
        (timedelta(microseconds=1), "before expiry", True),
        (timedelta(0), "exact expiry boundary", True),
        (timedelta(microseconds=-1), "after expiry", False),
    ])
    def test_both_sweeps_classify_the_same_lease_identically(
        self, offset, label, expect_live, stub_issue
    ):
        cutoff = ar._allocation_lease_expiry_cutoff(SEP)
        lease_at = cutoff + offset

        live = ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": lease_at},
            reference=SEP,
        )
        assert live is expect_live, label

        retry_considered = self._retry_considered(_db(), lease_at, SEP)
        surplus_considered = self._surplus_considered(_db(), lease_at, SEP)

        assert retry_considered is (not expect_live), (
            f"{label}: the retry sweep disagreed with _allocation_lease_is_live"
        )
        assert surplus_considered is (not expect_live), (
            f"{label}: the surplus sweep disagreed with _allocation_lease_is_live"
        )
        assert retry_considered is surplus_considered, (
            f"{label}: the two sweeps classified the same lease differently"
        )

    def test_both_sweeps_honour_the_injected_clock_not_the_wall_clock(self, stub_issue):
        """A lease live relative to the INJECTED reference but long dead by
        wall-clock time. Reading `datetime.now()` instead of `now_utc` flips
        the answer, so this fails against the wall clock in either direction
        and cannot pass by accident of when it runs.

        The reference is fixed in 2020, so it is unambiguously in the past
        whenever this suite runs.
        """
        past_ref = datetime(2020, 1, 1, 12, 0, tzinfo=timezone.utc)
        cutoff = ar._allocation_lease_expiry_cutoff(past_ref)
        lease_at = cutoff + timedelta(seconds=30)   # live vs past_ref

        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": lease_at},
            reference=past_ref) is True
        # ...and unambiguously dead against the wall clock.
        assert ar._allocation_lease_is_live(
            {"status": ar.SETTLING_STATUS, "allocation_lease_at": lease_at}) is False

        assert self._surplus_considered(_db(), lease_at, past_ref) is False, (
            "the surplus sweep read the wall clock: this lease is live relative "
            "to the injected reference and must not be touched"
        )
        assert self._retry_considered(_db(), lease_at, past_ref) is False, (
            "the retry sweep read the wall clock: this lease is live relative "
            "to the injected reference and must not be touched"
        )

    def test_neither_sweep_re_derives_the_ttl(self):
        import ast
        import inspect

        for fn in (ar.reconcile_surplus_denomination_allocations,
                   ar._retry_stuck_pending_manual_affiliate_ledgers):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            called = {n.func.id for n in ast.walk(tree)
                      if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
            names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
            assert "_allocation_lease_expiry_cutoff" in called, fn.__name__
            assert "_ALLOCATION_LEASE_TTL_SECONDS" not in names, (
                f"{fn.__name__} re-derives the TTL boundary itself"
            )

    def test_the_surplus_sweep_stamps_with_the_injected_clock(self, stub_issue):
        db = _db()
        db.affiliate_ledger.insert_one({
            "_id": "S0001", **_base(1, 9001),
            "status": "ISSUED",
            "reward_plan": arp.DENOMINATION_PLAN_ID,
            "entitlement_month": MONTH,
            "updated_at": OLD,
        })
        ar.reconcile_surplus_denomination_allocations(db, now_utc=SEP, batch_limit=5)
        row = db.affiliate_ledger.find_one({"_id": "S0001"})
        assert ar._as_aware_utc(row.get("surplus_checked_at")) == SEP, (
            "the checkpoint must be stamped with the pass's reference clock, not "
            f"the wall clock; got {row.get('surplus_checked_at')!r}"
        )


# ---------------------------------------------------------------------------
# batch_limit == 1: the ACTUAL behaviour, not a flattering description of it
# ---------------------------------------------------------------------------

class TestBatchLimitOfOne:
    """At a limit of one, the slot goes to the least-recently-considered
    ledger. Among never-considered rows that is decided by `_id`, so one class
    drains before the other is reached — a losing class's head does NOT age
    into winning, because losing does not stamp it. Documented rather than
    engineered around: production runs batch_limit=500."""

    def test_production_uses_a_limit_where_both_classes_get_capacity(self):
        import pathlib
        import re as _re

        main_src = pathlib.Path(__file__).resolve().parent / "main.py"
        text = main_src.read_text()
        m = _re.search(
            r'AFFILIATE_CURRENT_MONTH_BATCH_LIMIT = int\(os\.getenv\('
            r'"AFFILIATE_CURRENT_MONTH_BATCH_LIMIT", "(\d+)"\)\)', text)
        assert m, "the production batch limit is no longer where the docs say it is"
        production_limit = int(m.group(1))
        assert production_limit == 500

        settling, pending = ar._retry_class_quotas(production_limit)
        assert (settling, pending) == (250, 250), (
            "at the production limit both classes must get capacity every tick"
        )
        assert 'retry_current_month_pending_manual_ledgers(' in text
        assert 'batch_limit=AFFILIATE_CURRENT_MONTH_BATCH_LIMIT' in text

    def test_at_limit_one_exactly_one_ledger_is_processed_per_tick(self, stub_issue):
        db = _db()
        _seed_pending(db, 5)
        _seed_settling(db, 5)
        for tick in range(4):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=1
            )
            assert len(_touched(db)) == tick + 1

    def test_at_limit_one_one_class_drains_before_the_other_is_reached(self, stub_issue):
        """The honest behaviour. Asserting the opposite — that the classes
        alternate — would be asserting a guarantee this code does not make."""
        db = _db()
        _seed_pending(db, 3)
        _seed_settling(db, 3)
        order = []
        for tick in range(6):
            before = _touched(db)
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=1
            )
            order.append(sorted(_touched(db) - before)[0])
        assert order == ["P0000", "P0001", "P0002", "S0000", "S0001", "S0002"], (
            f"observed selection order at batch_limit=1: {order}"
        )

    def test_at_limit_one_every_record_is_still_eventually_considered(self, stub_issue):
        db = _db()
        _seed_pending(db, 4)
        _seed_settling(db, 4)
        for tick in range(8):
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=1
            )
        assert len(_touched(db)) == 8, "no record is permanently unreachable"

    def test_at_the_production_limit_both_classes_progress_every_tick(self, stub_issue):
        db = _db()
        _seed_pending(db, 60)
        _seed_settling(db, 60)
        for tick in range(3):
            before = _touched(db)
            ar._retry_stuck_pending_manual_affiliate_ledgers(
                db, now_utc=SEP + timedelta(minutes=5 * tick), batch_limit=500
            )
            new = _touched(db) - before
            if not new:
                break
            assert _cls(new, "P") and _cls(new, "S"), (
                f"tick {tick}: only one class progressed at the production limit"
            )
