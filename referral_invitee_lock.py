"""Cross-destination duplicate-referral guard.

One invitee must never accumulate more than one active/awarded referral
across the community-group and official-channel destinations at once
(P0-4 in the migration audit). The ``referral_invitee_locks`` collection
(unique index on ``invitee_user_id``) is an atomic claim document:
``claim()`` performs a single ``find_one_and_update`` with ``upsert=True``
whose filter excludes already-blocking statuses, so a second concurrent
claim for the same invitee either updates the *same* claim (if it is not
currently blocking) or fails the unique index with ``DuplicateKeyError``
(if it is) — there is no separate pre-check-then-insert race window.

Both ``main.py`` (pending-referral creation) and ``scheduler.py``
(settlement award/revoke) call into this module, passing their own
module-level ``db`` reference so test monkeypatching of ``db`` keeps
working exactly as it does today.
"""

from __future__ import annotations

import logging
from datetime import datetime

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)

COLLECTION_NAME = "referral_invitee_locks"

# Actual pending_referrals.status values that represent an active or
# already-successful referral. "qualified"/"settled" are event names on
# other collections (qualified_events, referral_events) rather than
# pending_referrals statuses, so they are intentionally not listed here.
BLOCKING_STATUSES = ("pending", "pending_channel", "processing", "awarded")

# Sentinel returned by claim() when the lock's state could not be
# determined because the database call itself failed. Deliberately not
# True/False so callers cannot accidentally treat a database outage as
# either "acquired" (fail-open) or "duplicate" (wrong audit reason) by a
# careless `if not lock_claimed` check.
LOCK_ERROR = "lock_error"


def claim(
    db,
    *,
    invitee_user_id: int,
    inviter_user_id: int,
    chat_id: int,
    destination_type: str,
    now_utc_ts: datetime,
):
    """Attempt to claim the invitee-scoped referral lock.

    Returns True if the caller may proceed to create a pending referral,
    False if this invitee already has an active/awarded referral (in any
    destination) and the caller must skip creation, or LOCK_ERROR if the
    database call failed and ownership could not be proven either way —
    callers must fail closed (no pending referral) on LOCK_ERROR, not
    treat it as a successful claim.
    """
    try:
        collection = db[COLLECTION_NAME]
        collection.find_one_and_update(
            {
                "invitee_user_id": invitee_user_id,
                "status": {"$nin": list(BLOCKING_STATUSES)},
            },
            {
                "$set": {
                    "invitee_user_id": invitee_user_id,
                    "inviter_user_id": inviter_user_id,
                    "chat_id": chat_id,
                    "destination_type": destination_type,
                    "status": "pending",
                    "updated_at_utc": now_utc_ts,
                },
                "$setOnInsert": {"created_at_utc": now_utc_ts},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return True
    except DuplicateKeyError:
        return False
    except Exception:
        logger.exception(
            "[REFERRAL][LOCK] claim_failed invitee=%s inviter=%s", invitee_user_id, inviter_user_id
        )
        return LOCK_ERROR


def release(
    db,
    *,
    invitee_user_id: int,
    status: str,
    now_utc_ts: datetime,
    expected_inviter_user_id: int | None = None,
) -> None:
    """Move the invitee's lock to a terminal/non-blocking status (e.g.
    "awarded" or "revoked") so a future genuinely-new referral attempt for
    the same invitee is not blocked forever.

    When ``expected_inviter_user_id`` is given, the update only applies if
    the lock is still owned by that inviter (its `inviter_user_id` still
    matches) — this is an ownership-token check so a partial-failure
    cleanup from one attribution attempt can never release a lock a
    *different* attempt has since legitimately claimed for the same
    invitee.
    """
    filt = {"invitee_user_id": invitee_user_id}
    if expected_inviter_user_id is not None:
        filt["inviter_user_id"] = expected_inviter_user_id
    try:
        db[COLLECTION_NAME].update_one(
            filt,
            {"$set": {"status": status, "updated_at_utc": now_utc_ts}},
        )
    except Exception:
        logger.exception(
            "[REFERRAL][LOCK] release_failed invitee=%s status=%s", invitee_user_id, status
        )


def ensure_indexes(db) -> None:
    try:
        db[COLLECTION_NAME].create_index(
            [("invitee_user_id", 1)],
            unique=True,
            name="uniq_referral_invitee_lock",
        )
    except Exception:
        logger.exception("[REFERRAL][LOCK] index_create_failed name=uniq_referral_invitee_lock")
