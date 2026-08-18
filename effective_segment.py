"""Canonical resolver for a user's EFFECTIVE operational eligibility segment.

Canonical *behavioral* segmentation (``users.for_bot_segment`` /
``users.for_bot_segment_normalized``) and Telegram-level multi-account *risk*
classification (``users.multi_account_voucher_hunter``, written by
``multi_account_risk_sync.py`` / Databot's segment_sync_job) are two
independent, separately-authoritative fields. Neither this module nor any of
its callers may write to the canonical segment fields, and this module never
reads or writes ``segment_snapshots`` -- historical behavioral snapshots are
untouched by this override.

What this module adds is a third, *derived*, read-only concept: the segment a
user should be treated as for OPERATIONAL eligibility/gating purposes (which
campaign audience they resolve into, which voucher-hunter restrictions apply)
-- as opposed to the BEHAVIORAL segment shown on analytics/reporting surfaces.
A user who is behaviorally ``high_value`` but flagged
``multi_account_voucher_hunter=True`` keeps ``high_value`` as their canonical
behavioral segment forever (nothing here changes that), but resolves as
``voucher_hunter`` for eligibility purposes, and must not simultaneously
qualify for ``high_value``-gated operational treatment.

Business rule (see resolve_effective_segment):
    if user.multi_account_voucher_hunter is True:
        effective segment = "voucher_hunter"
    else:
        effective segment = normalize_for_bot_segment(user.for_bot_segment / bot_segment)

Use ``resolve_effective_segment`` for in-Python decisions on an already-loaded
user doc, and ``effective_segment_query`` / ``effective_segment_query_for_segments``
for Mongo-side filtering (e.g. campaign audience resolution) so the same rule
never has to be hand-duplicated as a raw query elsewhere.
"""

from __future__ import annotations

from typing import Any

from config import normalize_for_bot_segment


def resolve_effective_segment(user: dict[str, Any] | None) -> str:
    """Return the OPERATIONAL eligibility segment for an already-loaded user doc.

    Never mutates the doc, never touches for_bot_segment/for_bot_segment_normalized.
    """
    user = user or {}
    if user.get("multi_account_voucher_hunter") is True:
        return "voucher_hunter"
    raw = user.get("for_bot_segment")
    if raw is None or str(raw).strip() == "":
        raw = user.get("bot_segment")
    return normalize_for_bot_segment(raw)


def effective_segment_query(segment: str) -> dict:
    """Mongo predicate matching users whose EFFECTIVE segment is ``segment``.

    voucher_hunter is inclusive: canonical voucher_hunter OR flagged
    multi_account_voucher_hunter. Every other segment is exclusive of
    multi-account voucher hunters: a multi-account voucher hunter must never
    also match another segment's operational eligibility predicate, even if
    their canonical for_bot_segment_normalized says otherwise.
    """
    if segment == "voucher_hunter":
        return {
            "$or": [
                {"for_bot_segment_normalized": "voucher_hunter"},
                {"multi_account_voucher_hunter": True},
            ]
        }
    return {
        "for_bot_segment_normalized": segment,
        "multi_account_voucher_hunter": {"$ne": True},
    }


def effective_segment_query_for_segments(segments: list[str]) -> dict:
    """Mongo predicate matching users whose EFFECTIVE segment is any of ``segments``.

    Returns a query that matches nothing for an empty list (callers that want
    "no filter" should not call this with an empty list).
    """
    clauses = [effective_segment_query(s) for s in segments]
    if not clauses:
        return {"_id": {"$exists": False}}
    if len(clauses) == 1:
        return clauses[0]
    return {"$or": clauses}
