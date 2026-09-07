"""Shared helpers for reading/writing the referral_events ledger.

referral_events documents use "invalidated" to mark referral_revoked rows
that were written in error (see repair_referral_ledger.py) without deleting
the audit record. Every aggregation that nets referral_settled/
referral_revoked events must exclude invalidated rows.

Why raw event-row subtraction (sum of +1 per referral_settled, -1 per
referral_revoked, filtered through with_not_invalidated) is equivalent to
tracking each referral's effective lifecycle state, rather than a distinct
computation that happens to agree by luck:

  1. `uniq_referral_event` (unique index on (event, inviter_id, invitee_id),
     main.py) makes it impossible to insert a second referral_settled, or a
     second referral_revoked, for the same (inviter_id, invitee_id) pair --
     a retried write raises DuplicateKeyError and is a no-op.
  2. scheduler.revoke_settled_referral() is the only code path allowed to
     write referral_revoked, and it refuses when no referral_settled exists
     yet for the pair, or when a referral_revoked already exists for it.
  3. repair_referral_ledger.py marks any pre-existing (legacy, written
     before #1/#2 existed) row that violates either guarantee as
     `invalidated`, and with_not_invalidated excludes those rows from every
     aggregation below.

Given 1-3, for every (inviter_id, invitee_id) pair counted by an aggregation
using with_not_invalidated, there is at most one referral_settled row and at
most one referral_revoked row, and a referral_revoked row can only exist
alongside a referral_settled row for the same pair. So each pair's raw net
(+1/-1 summed) can only ever be 0 or 1 -- exactly its effective lifecycle
state (not-yet-or-never-a-referral vs. settled-and-not-revoked) -- and
summing that net across an inviter's pairs equals summing the lifecycle
states directly. A per-pair $lookup/state machine would compute the same
number at a much higher aggregation cost; the raw sum is retained as the
equivalent, cheaper form and the invariant is asserted at write time (1-3)
rather than re-derived from event rows on every snapshot run.
"""

NOT_INVALIDATED_OR = [{"invalidated": {"$exists": False}}, {"invalidated": False}]


def not_invalidated_filter() -> dict:
    """A Mongo filter clause matching events that are not invalidated."""
    return {"$or": list(NOT_INVALIDATED_OR)}


def with_not_invalidated(match: dict) -> dict:
    """Return a copy of ``match`` with a not-invalidated clause merged in.

    ``match`` must not already contain a top-level "$or" key.
    """
    merged = dict(match)
    merged["$or"] = list(NOT_INVALIDATED_OR)
    return merged
