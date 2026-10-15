"""Dedupe weekly_leaderboard_history so uniq_weekly_history_week_start can be created.

Root cause: the deprecated `save_weekly_snapshot()` writer (database.py) used a
bare `insert_one()` with no key check, so any misfire replay / boot catch-up /
second worker invocation could insert another document for the same
`week_start`. The current writer (`main._archive_week_upsert`) is already an
atomic upsert keyed by `week_start`, but a unique index still can't be built
on top of documents that were already duplicated before that fix shipped.

Canonical selection (see `pick_canonical`)
------------------------------------------
Persisted schema (main._archive_week_upsert / database.save_weekly_snapshot):
  checkin_leaderboard:  [{user_id, username, weekly_xp}, ...]        sorted desc
  referral_leaderboard: [{user_id, username, weekly_referrals}, ...] sorted desc

Duplicates for one week were written while the Monday reset was running, so
the group mixes the real pre-reset standings with partially/fully zeroed
snapshots. Neither "latest" nor "earliest" is trusted. Instead the canonical
snapshot must be a *verified pre-reset* snapshot:

  * structurally valid (week_end + well-formed, correctly sorted boards),
  * not reset (at least one positive weekly_xp / weekly_referrals value), and
  * dominant: for every user on both boards its value is >= the value in
    every other snapshot of the week (a user absent from a top-N board is only
    allowed if the other value is <= that board's lowest entry). Weekly
    counters only grow during the week and the reset only lowers them, so the
    pre-reset state dominates every later reset-in-progress snapshot.

If no snapshot satisfies this, or several non-identical ones do, or any
snapshot is unparseable, selection is AMBIGUOUS and the whole run aborts
before touching the database. An operator who has reviewed the dry-run can
force a choice with `--canonical WEEK=ID`; the pinned snapshot must still be
valid, non-reset and correctly sorted.

Safety order of `--apply`
-------------------------
  1. Select a canonical snapshot for EVERY duplicated week (abort on any
     ambiguity -> nothing is written).
  2. Back up EVERY document of those weeks, canonical included (upsert keyed
     by the original _id; an existing identical backup is a no-op, a differing
     one aborts).
  3. Read every backup back and compare with the live document.
  4. Only then delete the non-canonical documents, in batches, never the
     canonical. A crash at any point leaves backup + originals intact; a
     rerun resumes safely.
  5. `--create-index` only runs when no duplicate week_start remains.

Usage:
  MONGO_URL='mongodb://...' python scripts/dedupe_weekly_leaderboard_history.py            # dry-run (default)
  MONGO_URL='mongodb://...' python scripts/dedupe_weekly_leaderboard_history.py --apply --create-index

Exit codes: 0 ok, 1 index creation failed, 2 aborted (ambiguous / unsafe).
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure

HISTORY_COLLECTION_NAME = "weekly_leaderboard_history"
BACKUP_COLLECTION_NAME = "weekly_leaderboard_history_dedupe_backup"
INDEX_NAME = "uniq_weekly_history_week_start"
DELETE_BATCH_SIZE = 100

CHECKIN_FIELD = "weekly_xp"
REFERRAL_FIELD = "weekly_referrals"
LEGACY_KEYS = ("checkin", "referral")  # read-only API fallbacks; never written by archive writers


class AmbiguousCanonicalError(Exception):
    """Canonical snapshot cannot be verified; nothing may be deleted."""


class UnsafeCleanupError(Exception):
    """A safety invariant failed mid-run (backup mismatch, canonical missing...)."""


@dataclass
class Board:
    values: dict = field(default_factory=dict)  # user_id -> value
    ordered: list = field(default_factory=list)  # values in stored order

    @property
    def floor(self):
        return min(self.values.values()) if self.values else 0

    @property
    def top(self):
        return max(self.values.values()) if self.values else 0

    @property
    def sorted_ok(self) -> bool:
        return all(a >= b for a, b in zip(self.ordered, self.ordered[1:]))


@dataclass
class Analysis:
    doc: dict
    checkin: Board | None = None
    referral: Board | None = None
    error: str | None = None

    @property
    def structurally_valid(self) -> bool:
        return self.error is None and bool(self.doc.get("week_end")) and bool(
            self.doc.get("checkin_leaderboard") or self.doc.get("referral_leaderboard")
        )

    @property
    def zeroed(self) -> bool:
        return self.error is None and self.checkin.top <= 0 and self.referral.top <= 0

    @property
    def sorted_ok(self) -> bool:
        return self.error is None and self.checkin.sorted_ok and self.referral.sorted_ok

    @property
    def eligible(self) -> bool:
        return self.structurally_valid and not self.zeroed and self.sorted_ok

    def state(self) -> str:
        if self.error:
            return f"MALFORMED({self.error})"
        if not self.structurally_valid:
            return "INVALID"
        if self.zeroed:
            return "ZEROED"
        if not self.sorted_ok:
            return "UNSORTED"
        return "ELIGIBLE"


def _parse_board(entries, value_field: str) -> Board:
    if entries is None:
        entries = []
    if not isinstance(entries, list):
        raise ValueError("board is not a list")
    board = Board()
    for e in entries:
        if not isinstance(e, dict) or e.get("user_id") is None:
            raise ValueError("entry without user_id")
        v = e.get(value_field)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
            raise ValueError(f"bad {value_field}")
        if e["user_id"] in board.values:
            raise ValueError("duplicate user_id in board")
        board.values[e["user_id"]] = v
        board.ordered.append(v)
    return board


def analyze(doc: dict) -> Analysis:
    a = Analysis(doc=doc)
    if any(k in doc for k in LEGACY_KEYS):
        a.error = "unsupported legacy keys"
        return a
    try:
        a.checkin = _parse_board(doc.get("checkin_leaderboard"), CHECKIN_FIELD)
        a.referral = _parse_board(doc.get("referral_leaderboard"), REFERRAL_FIELD)
    except ValueError as exc:
        a.error = str(exc)
    return a


def _board_dominates(s: Board, o: Board) -> bool:
    for uid, ov in o.values.items():
        sv = s.values.get(uid)
        if sv is None:
            # absent from a top-N board: only plausible if it would rank below S's last entry
            if ov > s.floor:
                return False
        elif sv < ov:
            return False
    return True


def dominates(s: Analysis, o: Analysis) -> bool:
    return _board_dominates(s.checkin, o.checkin) and _board_dominates(s.referral, o.referral)


def _same_standings(a: Analysis, b: Analysis) -> bool:
    return a.checkin.values == b.checkin.values and a.referral.values == b.referral.values


def _archived_at_sort_value(doc: dict):
    archived_at = doc.get("archived_at")
    if not isinstance(archived_at, datetime):
        return datetime.max.replace(tzinfo=timezone.utc)
    if archived_at.tzinfo is None:
        return archived_at.replace(tzinfo=timezone.utc)
    return archived_at


def pick_canonical(docs: list[dict], pinned_id: str | None = None) -> dict:
    """Return the verified pre-reset snapshot or raise AmbiguousCanonicalError.

    Never selects by archived_at/_id alone; those only break ties between
    snapshots whose standings are identical.
    """
    analyses = [analyze(d) for d in docs]
    bad = [f"{a.doc.get('_id')}: {a.error}" for a in analyses if a.error]
    if bad:
        raise AmbiguousCanonicalError(f"unparseable snapshot(s): {bad}")

    eligible = [a for a in analyses if a.eligible]
    if pinned_id is not None:
        for a in eligible:
            if str(a.doc["_id"]) == str(pinned_id):
                return a.doc
        raise AmbiguousCanonicalError(
            f"pinned _id={pinned_id} is not an eligible (valid, non-reset, sorted) snapshot of this week"
        )
    if not eligible:
        raise AmbiguousCanonicalError("no valid non-reset snapshot exists")

    dominant = [
        c for c in eligible if all(dominates(c, o) for o in analyses if o is not c)
    ]
    if not dominant:
        raise AmbiguousCanonicalError(
            "conflicting rankings: no snapshot dominates every other snapshot of the week"
        )
    if not all(_same_standings(dominant[0], d) for d in dominant[1:]):
        raise AmbiguousCanonicalError("multiple non-identical snapshots dominate each other")
    return min(
        (d.doc for d in dominant),
        key=lambda d: (_archived_at_sort_value(d), str(d.get("_id"))),
    )


def find_duplicate_groups(history_collection) -> list[dict]:
    """Group by the raw week_start value, including null/missing.

    A single-field unique index treats a missing field the same as an
    explicit null, so two documents that both lack week_start collide on
    that index exactly like two documents sharing a string value — they
    must be reported and deduped too, not silently skipped.
    """
    pipeline = [
        {"$group": {"_id": "$week_start", "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    return list(history_collection.aggregate(pipeline, allowDiskUse=True))


def build_report(history_collection, pinned: dict | None = None) -> list[dict]:
    """Fetch full documents for every duplicated week_start and decide the
    canonical one, without mutating anything. A group whose canonical snapshot
    cannot be verified carries `error` and `canonical_id=None`."""
    pinned = pinned or {}
    report = []
    for group in find_duplicate_groups(history_collection):
        week_start = group["_id"]
        docs = list(history_collection.find({"week_start": week_start}))
        error = None
        canonical = None
        try:
            canonical = pick_canonical(docs, pinned.get(week_start))
        except AmbiguousCanonicalError as exc:
            error = str(exc)
        canonical_id = canonical["_id"] if canonical else None
        report.append({
            "week_start": week_start,
            "doc_count": len(docs),
            "docs": [_describe(d, canonical_id) for d in docs],
            "canonical_id": canonical_id,
            "all_ids": [d["_id"] for d in docs],
            "remove_ids": [d["_id"] for d in docs if d["_id"] != canonical_id] if canonical else [],
            "error": error,
        })
    return report


def _describe(doc: dict, canonical_id) -> dict:
    a = analyze(doc)
    return {
        "_id": doc["_id"],
        "source": doc.get("source", "unknown"),
        "archived_at": doc.get("archived_at"),
        "week_end": doc.get("week_end"),
        "entry_counts": {
            "checkin": len(doc.get("checkin_leaderboard") or []),
            "referral": len(doc.get("referral_leaderboard") or []),
        },
        "rank1": {
            "weekly_xp": a.checkin.top if a.checkin else None,
            "weekly_referrals": a.referral.top if a.referral else None,
        },
        "state": a.state(),
        "valid": a.structurally_valid,
        "is_canonical": canonical_id is not None and doc["_id"] == canonical_id,
    }


def print_report(report: list[dict], *, mode: str) -> None:
    if not report:
        print(f"[{mode}] no duplicate week_start values found — nothing to do.")
        return
    print(f"[{mode}] {len(report)} duplicated week_start value(s) found:")
    for group in report:
        docs = group["docs"]
        versions = {(d["rank1"]["weekly_xp"], d["rank1"]["weekly_referrals"], d["state"]) for d in docs}
        print(f"\n  week_start={group['week_start']} doc_count={group['doc_count']} "
              f"distinct(rank1_xp, rank1_refs, state)={len(versions)}")
        if group["error"]:
            print(f"    !! AMBIGUOUS — {group['error']}")
        for d in sorted(docs, key=lambda d: (str(d["archived_at"]), str(d["_id"]))):
            marker = "KEEP (canonical)" if d["is_canonical"] else (
                "BACKUP + DELETE" if group["canonical_id"] is not None else "UNDECIDED")
            print(
                f"    - _id={d['_id']} archived_at={d['archived_at']} source={d['source']} "
                f"rank1={d['rank1']} entries={d['entry_counts']} state={d['state']} -> {marker}"
            )


def _strip_meta(doc: dict) -> dict:
    return {k: v for k, v in doc.items() if k != "_dedupe_meta"}


def _backup_documents(history_collection, backup_collection, group: dict) -> int:
    """Idempotently back up EVERY document of the group (canonical included)."""
    docs = list(history_collection.find({"_id": {"$in": group["all_ids"]}}))
    if len(docs) != len(group["all_ids"]):
        raise UnsafeCleanupError(
            f"week_start={group['week_start']}: documents changed since report; rerun"
        )
    written = 0
    for doc in docs:
        existing = backup_collection.find_one({"_id": doc["_id"]})
        if existing is not None:
            if _strip_meta(existing) != doc:
                raise UnsafeCleanupError(
                    f"backup _id={doc['_id']!r} exists but differs from the live document"
                )
            continue
        backup_doc = dict(doc)
        backup_doc["_dedupe_meta"] = {
            "week_start": group["week_start"],
            "canonical_id": group["canonical_id"],
            "role": "canonical" if doc["_id"] == group["canonical_id"] else "removed",
            "backed_up_at": datetime.now(timezone.utc),
        }
        # Upsert keyed by the original _id: a crash + rerun can never double-back-up.
        result = backup_collection.replace_one({"_id": doc["_id"]}, backup_doc, upsert=True)
        if getattr(result, "acknowledged", True) is False:
            raise UnsafeCleanupError(f"backup write not acknowledged for _id={doc['_id']!r}")
        written += 1
    return written


def _verify_backups(history_collection, backup_collection, group: dict) -> None:
    live = {d["_id"]: d for d in history_collection.find({"_id": {"$in": group["all_ids"]}})}
    stored = {d["_id"]: d for d in backup_collection.find({"_id": {"$in": group["all_ids"]}})}
    for _id in group["remove_ids"]:
        # A doc already deleted by an interrupted earlier run is fine as long as its backup exists.
        if _id not in stored:
            raise UnsafeCleanupError(f"backup missing for _id={_id!r}")
        if _id in live and _strip_meta(stored[_id]) != live[_id]:
            raise UnsafeCleanupError(f"backup for _id={_id!r} does not match live document")


def dedupe(
    *,
    history_collection,
    backup_collection,
    dry_run: bool = True,
    pinned: dict | None = None,
    delete_batch_size: int = DELETE_BATCH_SIZE,
) -> dict[str, Any]:
    """Run one dedupe pass. Dry-run by default. Returns a summary dict.

    mode == "aborted": at least one week has no verifiable canonical snapshot;
    NOTHING was written or deleted (for any week). Expected Mongo/safety errors
    during apply raise; the originals are never deleted before their backup is
    verified, so an exception at any point loses no history.
    """
    report = build_report(history_collection, pinned)
    print_report(report, mode="DRY-RUN" if dry_run else "APPLY")

    summary = {"duplicate_groups": len(report), "backed_up": 0, "deleted": 0, "report": report}
    ambiguous = [g["week_start"] for g in report if g["error"]]
    if ambiguous:
        print(f"\n[ABORT] canonical snapshot ambiguous for week_start={ambiguous}; nothing modified.")
        return {**summary, "mode": "aborted", "ambiguous_weeks": ambiguous}
    if dry_run:
        return {**summary, "mode": "dry_run"}

    groups = [g for g in report if g["remove_ids"]]

    # Phase 1+2: back up and verify everything BEFORE the first delete.
    for group in groups:
        summary["backed_up"] += _backup_documents(history_collection, backup_collection, group)
    for group in groups:
        _verify_backups(history_collection, backup_collection, group)

    # Phase 3: delete non-canonical documents only, in batches.
    for group in groups:
        ids = group["remove_ids"]
        for i in range(0, len(ids), delete_batch_size):
            if history_collection.count_documents({"_id": group["canonical_id"]}) != 1:
                raise UnsafeCleanupError(
                    f"canonical _id={group['canonical_id']!r} missing; refusing to delete"
                )
            batch = ids[i:i + delete_batch_size]
            res = history_collection.delete_many({"_id": {"$in": batch}})
            summary["deleted"] += int(getattr(res, "deleted_count", 0) or 0)
        if history_collection.count_documents({"_id": group["canonical_id"]}) != 1:
            raise UnsafeCleanupError(f"canonical _id={group['canonical_id']!r} missing after cleanup")

    return {**summary, "mode": "apply"}


def create_unique_index(history_collection) -> dict[str, Any]:
    """Attempt the unique index after cleanup. Never raises — this mirrors
    the non-fatal safe_create_index behavior used at app startup, so this
    script's exit code reflects the dedupe result, not index creation."""
    try:
        history_collection.create_index(
            [("week_start", ASCENDING)], unique=True, name=INDEX_NAME
        )
        return {"status": "created_or_exists", "name": INDEX_NAME}
    except (OperationFailure, DuplicateKeyError) as exc:
        return {"status": "failed", "name": INDEX_NAME, "error": str(exc)}


def _parse_pins(values: list[str]) -> dict:
    pins = {}
    for item in values or []:
        week, sep, _id = item.partition("=")
        if not sep or not week or not _id:
            raise SystemExit(f"--canonical expects WEEK=ID, got {item!r}")
        pins[week] = _id
    return pins


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default="referral_bot")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report only (default).")
    mode.add_argument("--apply", action="store_true", help="Back up and delete duplicates.")
    parser.add_argument(
        "--create-index",
        action="store_true",
        help="With --apply, create uniq_weekly_history_week_start once no duplicates remain.",
    )
    parser.add_argument(
        "--canonical", action="append", default=[], metavar="WEEK=ID",
        help="Operator override for an ambiguous week after reviewing the dry-run. "
             "The _id must still be a valid, non-reset, sorted snapshot of that week.",
    )
    args = parser.parse_args()
    if args.create_index and not args.apply:
        parser.error("--create-index requires --apply")

    mongo_url = os.environ.get("MONGO_URL")
    if not mongo_url:
        raise RuntimeError("MONGO_URL is required")

    client = MongoClient(mongo_url)
    db = client[args.db]
    history_collection = db[HISTORY_COLLECTION_NAME]
    backup_collection = db[BACKUP_COLLECTION_NAME]

    result = dedupe(
        history_collection=history_collection,
        backup_collection=backup_collection,
        dry_run=not args.apply,
        pinned=_parse_pins(args.canonical),
    )
    print(
        f"\n[{result['mode'].upper()}] duplicate_groups={result['duplicate_groups']} "
        f"backed_up={result['backed_up']} deleted={result['deleted']}"
    )
    if result["mode"] == "aborted":
        return 2

    if args.apply and args.create_index:
        remaining = find_duplicate_groups(history_collection)
        if remaining:
            print(f"[INDEX] refusing: {len(remaining)} duplicate week_start group(s) remain")
            return 1
        idx_result = create_unique_index(history_collection)
        print(f"[INDEX] {idx_result}")
        if idx_result["status"] != "created_or_exists":
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
