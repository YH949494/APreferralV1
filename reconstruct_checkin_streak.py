"""Dry-run diagnostic/repair tool for a single user's check-in streak.

Reconstructs a user's daily-check-in history from the immutable event
sources (``xp_events``/``xp_ledger`` "checkin:*" entries, and — once the
canonical claim/audit collection exists — ``checkin_events``), converts
every timestamp to its Asia/Kuala_Lumpur calendar date, and compares the
reconstructed streak against the stored ``users.streak`` /
``users.longest_streak`` fields.

Read-only by default. Nothing in production is modified unless ``--apply``
is passed explicitly, and even then only ``streak``, ``longest_streak``, and
``last_checkin`` are touched, and only after the same reconstruction this
script prints.

Usage:
    python reconstruct_checkin_streak.py --user-id 123456789
    python reconstruct_checkin_streak.py --user-id 123456789 --json
    python reconstruct_checkin_streak.py --user-id 123456789 --apply   # writes the fix
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from config import KL_TZ
from database import get_collection, init_db
from time_utils import as_aware_utc


@dataclass
class Reconstruction:
    user_id: int
    stored_streak: int
    stored_longest_streak: int
    stored_last_checkin: datetime | None
    checkin_dates: list[date] = field(default_factory=list)
    duplicate_attempts: list[dict] = field(default_factory=list)
    naive_timestamp_events: list[dict] = field(default_factory=list)
    reconstructed_streak: int = 0
    reconstructed_longest_streak: int = 0
    missing_dates: list[date] = field(default_factory=list)
    inconsistent: bool = False
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "stored": {
                "streak": self.stored_streak,
                "longest_streak": self.stored_longest_streak,
                "last_checkin": self.stored_last_checkin.isoformat() if self.stored_last_checkin else None,
            },
            "reconstructed": {
                "streak": self.reconstructed_streak,
                "longest_streak": self.reconstructed_longest_streak,
                "checkin_dates_kl": [d.isoformat() for d in self.checkin_dates],
                "checkin_count": len(self.checkin_dates),
            },
            "missing_dates_kl": [d.isoformat() for d in self.missing_dates],
            "duplicate_checkin_attempts": self.duplicate_attempts,
            "naive_timestamp_events": self.naive_timestamp_events,
            "inconsistent": self.inconsistent,
            "notes": self.notes,
        }


def _kl_date_of(dt) -> date | None:
    """Convert any stored timestamp (aware or naive) to its KL calendar date.

    Naive datetimes are assumed UTC, matching every writer currently found in
    this codebase (main.process_checkin's now_utc(), database.checkin_user's
    datetime.now(timezone.utc)). If a genuinely naive-KL-local legacy row is
    found, this assumption would misdate it by up to 8 hours — that is what
    ``naive_timestamp_events`` in the report is for; treat any user flagged
    there as needing manual confirmation before trusting the reconstruction.
    """
    if dt is None:
        return None
    aware = as_aware_utc(dt)
    if aware is None:
        return None
    return aware.astimezone(KL_TZ).date()


def _collect_checkin_events(user_id: int) -> list[dict]:
    """Gather every distinct check-in attempt for user_id from all known sources."""
    events: list[dict] = []

    xp_events_col = get_collection("xp_events")
    for doc in xp_events_col.find({"user_id": user_id, "type": "checkin"}):
        events.append({
            "source": "xp_events",
            "unique_key": doc.get("unique_key"),
            "created_at": doc.get("created_at"),
            "raw": doc,
        })

    xp_ledger_col = get_collection("xp_ledger")
    for doc in xp_ledger_col.find({"user_id": user_id, "source": "checkin"}):
        events.append({
            "source": "xp_ledger",
            "unique_key": doc.get("source_id"),
            "created_at": doc.get("created_at"),
            "raw": doc,
        })

    # checkin_events is written by main.process_checkin() strictly *after*
    # the atomic streak/last_checkin compare-and-swap on `users` has already
    # committed (see process_checkin's docstring) — there is no pending or
    # in-flight state for this collection to be in. Every row here reflects
    # already-committed streak state, never an intended-but-not-yet-applied
    # one. A row missing new_streak/reset_reason would indicate a schema
    # anomaly (e.g. a manually inserted or corrupted record), not a normal
    # in-progress claim, so flag rather than silently trust it.
    checkin_events_col = get_collection("checkin_events")
    for doc in checkin_events_col.find({"user_id": user_id}):
        if "new_streak" not in doc or "reset_reason" not in doc:
            result_note = f"checkin_events doc {doc.get('_id')!r} missing new_streak/reset_reason — anomalous row, not a normal committed check-in; excluded from reconstruction."
            events.append({
                "source": "checkin_events",
                "unique_key": doc.get("_id"),
                "created_at": None,  # excluded: see note above
                "raw": doc,
                "anomaly_note": result_note,
            })
            continue
        events.append({
            "source": "checkin_events",
            "unique_key": doc.get("_id"),
            "created_at": doc.get("checked_in_at_utc"),
            "raw": doc,
        })

    return events


def reconstruct(user_id: int) -> Reconstruction:
    users_col = get_collection("users")
    user = users_col.find_one({"user_id": user_id}) or {}

    result = Reconstruction(
        user_id=user_id,
        stored_streak=int(user.get("streak", 0)),
        stored_longest_streak=int(user.get("longest_streak", 0)),
        stored_last_checkin=user.get("last_checkin"),
    )

    if not user:
        result.notes.append("User not found in users collection.")

    events = _collect_checkin_events(user_id)

    # dedupe by local KL calendar date, tracking any date seen more than once
    # (a genuine duplicate check-in attempt for the same day — grant_xp's
    # unique_key dedup should prevent duplicate xp_events/xp_ledger rows, so
    # any that surface here indicate the idempotency guard was bypassed).
    seen_by_date: dict[date, list[dict]] = {}
    for ev in events:
        created_at = ev["created_at"]
        if created_at is not None and getattr(created_at, "tzinfo", "unset") is None:
            result.naive_timestamp_events.append({
                "source": ev["source"],
                "unique_key": ev["unique_key"],
                "raw_value": str(created_at),
            })
        d = _kl_date_of(created_at)
        if d is None:
            note = ev.get("anomaly_note") or f"Unparseable timestamp in {ev['source']} unique_key={ev['unique_key']!r}"
            result.notes.append(note)
            continue
        seen_by_date.setdefault(d, []).append(ev)

    for d, evs in seen_by_date.items():
        sources = {e["source"] for e in evs}
        # More than one *independent* source-family (xp_events vs xp_ledger vs
        # checkin_events) for the same date is expected (same check-in logged
        # in multiple ledgers). Flag only when the SAME source has >1 doc for
        # the date, since grant_xp's unique_key means that should never happen.
        by_source: dict[str, int] = {}
        for e in evs:
            by_source[e["source"]] = by_source.get(e["source"], 0) + 1
        for source, count in by_source.items():
            if count > 1:
                result.duplicate_attempts.append({
                    "date_kl": d.isoformat(),
                    "source": source,
                    "count": count,
                })

    result.checkin_dates = sorted(seen_by_date.keys())

    # reconstructed longest streak = longest run of consecutive calendar dates
    longest = 0
    current_run = 0
    prev_date: date | None = None
    for d in result.checkin_dates:
        if prev_date is not None and d == prev_date + timedelta(days=1):
            current_run += 1
        else:
            current_run = 1
        longest = max(longest, current_run)
        prev_date = d
    result.reconstructed_longest_streak = longest

    # reconstructed CURRENT streak = consecutive run ending at the most
    # recent check-in date (streak is "as of stored_last_checkin", not "as of
    # today" — a user who stopped checking in N days ago still had a real
    # streak at the time of their last check-in; whether that streak is
    # still "current" today is a display-time concern, not a data-integrity
    # one).
    if result.checkin_dates:
        run = 1
        for i in range(len(result.checkin_dates) - 1, 0, -1):
            if result.checkin_dates[i] == result.checkin_dates[i - 1] + timedelta(days=1):
                run += 1
            else:
                break
        result.reconstructed_streak = run

        # missing dates = gaps between first and last check-in that have no
        # check-in event (useful for spotting exactly which day(s) look
        # "missed" from the event log, whether or not that matches what the
        # stored streak implies).
        span_start, span_end = result.checkin_dates[0], result.checkin_dates[-1]
        all_days = {span_start + timedelta(days=i) for i in range((span_end - span_start).days + 1)}
        result.missing_dates = sorted(all_days - set(result.checkin_dates))

    result.inconsistent = (
        result.reconstructed_streak != result.stored_streak
        or result.reconstructed_longest_streak > result.stored_longest_streak
    )
    if result.inconsistent:
        result.notes.append(
            "Stored streak/longest_streak disagrees with the reconstructed "
            "check-in history — do not trust users.streak for this user "
            "without further review."
        )
    if result.duplicate_attempts:
        result.notes.append(
            "Duplicate same-day event documents found in a single source — "
            "grant_xp's idempotency key should have prevented this; investigate "
            "before assuming the ledger is authoritative."
        )
    if result.naive_timestamp_events:
        result.notes.append(
            "Naive (tz-less) timestamps found. NOTE: pymongo returns naive "
            "UTC datetimes by default (MongoClient is not tz_aware=True), so "
            "this fires for essentially every event read back from Mongo and "
            "is NOT by itself evidence of a bug — every current writer in "
            "this codebase constructs UTC-aware datetimes before insert. It "
            "only matters if a row's *original* value (before the BSON "
            "round-trip) was itself naive-KL-local rather than "
            "naive/aware-UTC — which cannot be determined from the value "
            "alone. Treat this as a prompt for manual spot-checking very old "
            "rows, not a standalone inconsistency signal."
        )

    return result


def apply_fix(user_id: int, result: Reconstruction) -> None:
    users_col = get_collection("users")
    new_longest = max(result.reconstructed_longest_streak, result.stored_longest_streak)
    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"streak": result.reconstructed_streak}, "$max": {"longest_streak": new_longest}},
    )
    print(f"[APPLY] uid={user_id} streak set to {result.reconstructed_streak}, longest_streak $max'd to {new_longest}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of a text report.")
    parser.add_argument("--apply", action="store_true", help="Write the reconstructed streak/longest_streak to production. Off by default.")
    args = parser.parse_args()

    init_db()
    result = reconstruct(args.user_id)

    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(f"=== Check-in streak reconstruction for user_id={result.user_id} ===")
        print(f"Stored:        streak={result.stored_streak}  longest_streak={result.stored_longest_streak}  "
              f"last_checkin={result.stored_last_checkin}")
        print(f"Reconstructed: streak={result.reconstructed_streak}  longest_streak={result.reconstructed_longest_streak}")
        print(f"Check-in dates (KL, {len(result.checkin_dates)} total): "
              f"{[d.isoformat() for d in result.checkin_dates]}")
        print(f"Missing dates within span: {[d.isoformat() for d in result.missing_dates]}")
        print(f"Duplicate same-source check-in attempts: {result.duplicate_attempts}")
        print(f"Naive-timestamp events: {result.naive_timestamp_events}")
        print(f"INCONSISTENT: {result.inconsistent}")
        for note in result.notes:
            print(f"  - {note}")

    if args.apply:
        if not result.inconsistent:
            print("No inconsistency detected; nothing to apply.")
        else:
            apply_fix(args.user_id, result)

    return 1 if result.inconsistent and not args.apply else 0


if __name__ == "__main__":
    sys.exit(main())
