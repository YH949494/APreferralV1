"""Shared helpers for reading/writing the referral_events ledger.

referral_events documents use "invalidated" to mark referral_revoked rows
that were written in error (see repair_referral_ledger.py) without deleting
the audit record. Every aggregation that nets referral_settled/
referral_revoked events must exclude invalidated rows.
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
