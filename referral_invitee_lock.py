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

import referral_destination

logger = logging.getLogger(__name__)

# Chat ids a legacy destination-scoped award key ("ref:<chat_id>:<invitee_id>")
# could plausibly have been minted under: the currently configured community
# group / official channel ids, plus their historical hardcoded defaults (in
# case an env var override post-dates the legacy rows). Kept as a fixed,
# known set rather than scanning every award_key with a regex, since that
# regex can only use the "ref:" index prefix and would degrade to a full
# collection scan as referral_award_events grows.
_LEGACY_AWARD_KEY_CHAT_IDS = tuple(
    {
        chat_id
        for chat_id in (
            referral_destination.COMMUNITY_GROUP_ID,
            referral_destination.OFFICIAL_CHANNEL_ID,
            referral_destination._DEFAULT_COMMUNITY_GROUP_ID,
            referral_destination._DEFAULT_OFFICIAL_CHANNEL_ID,
        )
        if chat_id is not None
    }
)

COLLECTION_NAME = "referral_invitee_locks"

# Actual pending_referrals.status values that represent an active or
# already-successful referral. "qualified"/"settled" are event names on
# other collections (qualified_events, referral_events) rather than
# pending_referrals statuses, so they are intentionally not listed here.
BLOCKING_STATUSES = ("pending", "pending_channel", "processing", "awarded")


def has_historical_success(db, *, invitee_user_id: int) -> bool:
    """Return True if this invitee has ANY prior evidence of a successful
    referral, checked across every collection/key format that has ever
    recorded one — not just ``qualified_events``.

    ``referral_invitee_locks`` only exists going forward (created by this
    migration), so an invitee who was qualified/settled/awarded before the
    lock collection existed has no lock row and ``claim()`` alone would let
    a brand-new referral through for them. This check closes that gap by
    consulting every historical source directly:

      1. ``qualified_events.invitee_id``
      2. ``referral_events`` with ``event="referral_settled"`` for this invitee
      3. ``referral_award_events`` with a structured ``invitee_user_id`` field
      4. legacy destination-scoped award key ``ref:<chat_id>:<invitee_id>``
      5. new invitee-scoped award key ``ref:<invitee_id>``

    (4) and (5) are matched on the ``award_key`` string itself, independent
    of (3), because pre-migration award rows are not guaranteed to carry a
    structured ``invitee_user_id`` field. (4) is matched by exact equality
    against ``award_key`` for every known destination chat id (see
    ``_LEGACY_AWARD_KEY_CHAT_IDS``) rather than a regex scan, so the lookup
    stays an index seek instead of degrading into a full collection scan as
    ``referral_award_events`` grows.
    """
    invitee = int(invitee_user_id)

    if db.qualified_events.find_one({"invitee_id": invitee}, {"_id": 1}):
        return True

    if db.referral_events.find_one(
        {"event": "referral_settled", "invitee_id": invitee}, {"_id": 1}
    ):
        return True

    if db.referral_award_events.find_one({"invitee_user_id": invitee}, {"_id": 1}):
        return True

    # Exact-match candidate keys (new format + legacy format under every
    # known destination chat id) so this hits the unique award_key index
    # instead of scanning every award row.
    candidate_keys = [f"ref:{invitee}"] + [
        f"ref:{chat_id}:{invitee}" for chat_id in _LEGACY_AWARD_KEY_CHAT_IDS
    ]
    if db.referral_award_events.find_one(
        {"award_key": {"$in": candidate_keys}}, {"_id": 1}
    ):
        return True

    return False


def claim(
    db,
    *,
    invitee_user_id: int,
    inviter_user_id: int,
    chat_id: int,
    destination_type: str,
    now_utc_ts: datetime,
) -> bool:
    """Attempt to claim the invitee-scoped referral lock.

    Returns True if the caller may proceed to create a pending referral,
    False if this invitee already has an active/awarded referral (in any
    destination) and the caller must skip creation.
    """
    collection = db[COLLECTION_NAME]
    try:
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


def release(db, *, invitee_user_id: int, status: str, now_utc_ts: datetime) -> None:
    """Move the invitee's lock to a terminal/non-blocking status (e.g.
    "awarded" or "revoked") so a future genuinely-new referral attempt for
    the same invitee is not blocked forever.
    """
    try:
        db[COLLECTION_NAME].update_one(
            {"invitee_user_id": invitee_user_id},
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
