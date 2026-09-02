"""Mission Reward Pool — worker-side campaign finalization.

Runs entirely off the request path (§17/§28). One bounded, resumable,
fenced state machine per campaign:

    closed
      -> processing_eligibility     identity resolve + dedupe + abuse filter
      -> qualified_snapshot_ready   qualified set frozen
      -> selecting_winners          seed drawn once, never redrawn
      -> winners_selected
      -> allocating_rewards         atomic voucher claim from db.voucher_pools
      -> notifying                  Telegram, retryable, never touches vouchers
      -> completed

CONCURRENCY MODEL (§19)
-----------------------
The repo's only distributed lock (``main.acquire_scheduler_lock``) is a TTL
lease on ``scheduler_locks`` with an ``owner`` field and **no fencing token**:
a worker that stalls past its TTL still believes it owns the lease and can
keep mutating. That is not strong enough to guard voucher allocation, so
Mission Pool adds a monotonic fence of its own:

    gc_campaigns.mission_pool.processing_generation

``_claim_campaign`` atomically ``$inc``s it and records the new value as the
owning worker's fence. Every subsequent mutation — stage transitions, entry
qualification, winner marking, allocation bookkeeping — carries
``mission_pool.processing_generation: <my fence>`` in its *filter*. The moment
another worker claims the campaign the generation moves on and a stale
worker's writes match nothing and silently no-op. The scheduler lock is still
used, as a cheap first-line "don't even try" gate.

WHAT IS DELIBERATELY NOT HERE
-----------------------------
No new voucher inventory: allocation goes through
``voucher_pool_service.allocate_voucher`` (the same ``find_one_and_update`` on
``db.voucher_pools`` that Campaign Centre already uses). No new anti-abuse
model: the flags read below are the ones Databot/UIM already writes onto
``users``. No new Telegram sender: ``telegram_utils.send_telegram_http_message``.
"""

from __future__ import annotations

import logging
import os
import random
import secrets
import time
from datetime import datetime, timedelta, timezone

import database
import mission_pool as mp

logger = logging.getLogger(__name__)

CAMPAIGNS_COLLECTION = "gc_campaigns"
REWARDS_COLLECTION = "campaign_rewards"
REWARD_CATEGORY = "mission_pool"


def _int_env(name: str, default: int, *, lo: int, hi: int) -> int:
    try:
        val = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, val))


# Bounded batches (§29). Kept to a handful of knobs, not dozens.
def eligibility_batch_size() -> int:
    return _int_env("MISSION_ELIGIBILITY_BATCH_SIZE", 200, lo=1, hi=1000)


def allocation_batch_size() -> int:
    return _int_env("MISSION_ALLOCATION_BATCH_SIZE", 50, lo=1, hi=500)


def notify_batch_size() -> int:
    return _int_env("MISSION_NOTIFY_BATCH_SIZE", 25, lo=1, hi=200)


def time_budget_seconds() -> int:
    return _int_env("MISSION_PROCESSOR_TIME_BUDGET_SECONDS", 60, lo=5, hi=600)


def max_campaigns_per_tick() -> int:
    return _int_env("MISSION_PROCESSOR_MAX_CAMPAIGNS", 3, lo=1, hi=20)


def lease_seconds() -> int:
    return _int_env("MISSION_PROCESSOR_LEASE_SECONDS", 300, lo=30, hi=3600)


# Upper bound on how many qualified entries a single selection run will pull
# ids for. Only ``_id`` is projected (12-byte ObjectIds), so 200k ids is a few
# MB — measured, bounded, and logged if a campaign ever exceeds it.
def max_qualified_for_selection() -> int:
    return _int_env("MISSION_MAX_QUALIFIED_FOR_SELECTION", 200000, lo=1000, hi=2000000)


NOTIFY_MAX_ATTEMPTS = 5
NOTIFY_BACKOFF_BASE_SECONDS = 60

WINNER_MESSAGE_TEMPLATE = (
    "🎉 Congratulations!\n\n"
    "You've been selected as a winner of {campaign_name}!\n\n"
    "Your reward is now available in Campaign Rewards.\n\n"
    "Open the bot and redeem your code now."
)


# ---------------------------------------------------------------------------
# Fenced ownership (§19)
# ---------------------------------------------------------------------------

class _Fence:
    """One worker's claim on one campaign: the generation token plus the
    lease expiry it must keep renewing."""

    __slots__ = ("campaign_id", "generation", "owner", "expires_at")

    def __init__(self, campaign_id: str, generation: int, owner: str, expires_at: datetime):
        self.campaign_id = campaign_id
        self.generation = generation
        self.owner = owner
        self.expires_at = expires_at


def _instance_id() -> str:
    try:
        import main  # pragma: no cover - not importable in unit tests

        return str(getattr(main, "INSTANCE_ID", "")) or f"pid-{os.getpid()}"
    except Exception:
        return f"pid-{os.getpid()}"


def _campaigns():
    return database.db[CAMPAIGNS_COLLECTION]


def _entries():
    return database.db[mp.ENTRIES_COLLECTION]


def _claims():
    return database.db[mp.IDENTITY_CLAIMS_COLLECTION]


def _rewards():
    return database.db[REWARDS_COLLECTION]


def _claim_campaign(campaign_id: str, now: datetime) -> _Fence | None:
    """Atomically take ownership. Succeeds only when nobody holds a live
    lease; the ``$inc`` makes the returned generation strictly greater than
    any previous owner's, which is what invalidates a stale worker."""
    owner = _instance_id()
    expires_at = now + timedelta(seconds=lease_seconds())
    doc = _campaigns().find_one_and_update(
        {
            "campaign_id": campaign_id,
            "$or": [
                {"mission_pool.processing_lease_expires_at": {"$lte": now}},
                {"mission_pool.processing_lease_expires_at": None},
                {"mission_pool.processing_lease_expires_at": {"$exists": False}},
            ],
        },
        {
            "$inc": {"mission_pool.processing_generation": 1},
            "$set": {
                "mission_pool.processing_owner": owner,
                "mission_pool.processing_lease_expires_at": expires_at,
                "mission_pool.processing_claimed_at": now,
                "mission_pool.updated_at": now,
            },
        },
        return_document=True,
    )
    if not doc:
        return None
    generation = int(((doc.get("mission_pool") or {}).get("processing_generation")) or 0)
    return _Fence(campaign_id, generation, owner, expires_at)


def _fenced_filter(fence: _Fence) -> dict:
    """Every critical campaign mutation must go through this filter."""
    return {
        "campaign_id": fence.campaign_id,
        "mission_pool.processing_generation": fence.generation,
    }


def _still_owner(fence: _Fence) -> bool:
    return _campaigns().count_documents(_fenced_filter(fence)) == 1


def _renew(fence: _Fence, now: datetime) -> bool:
    """Extend the lease under the fence. A failure here means ownership was
    lost, and the caller must stop mutating immediately (§19)."""
    expires_at = now + timedelta(seconds=lease_seconds())
    res = _campaigns().update_one(
        _fenced_filter(fence),
        {"$set": {
            "mission_pool.processing_lease_expires_at": expires_at,
            "mission_pool.updated_at": now,
        }},
    )
    if res.matched_count != 1:
        return False
    fence.expires_at = expires_at
    return True


def _release(fence: _Fence, now: datetime) -> None:
    _campaigns().update_one(
        _fenced_filter(fence),
        {"$set": {"mission_pool.processing_lease_expires_at": now, "mission_pool.updated_at": now}},
    )


def _set_stage(fence: _Fence, stage: str, now: datetime, **extra) -> bool:
    updates = {"mission_pool.processing_stage": stage, "mission_pool.updated_at": now}
    for key, value in extra.items():
        updates[f"mission_pool.{key}"] = value
    res = _campaigns().update_one(_fenced_filter(fence), {"$set": updates})
    return res.matched_count == 1


def _campaign_doc(campaign_id: str) -> dict | None:
    return _campaigns().find_one({"campaign_id": campaign_id})


def _stage_of(campaign: dict) -> str:
    return (campaign.get("mission_pool") or {}).get("processing_stage") or mp.STAGE_PENDING


def _log(event: str, **fields) -> None:
    """Structured, greppable operational logging (§52). Identity keys are
    masked and voucher codes are never included."""
    parts = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
    logger.info("[MISSION_POOL][%s] %s", event, parts)


def _emit(event: str, **kwargs) -> None:
    try:
        from campaign_events import emit_campaign_event

        emit_campaign_event(event_type=event, **kwargs)
    except Exception:
        logger.warning("[MISSION_POOL] event emit failed event=%s", event, exc_info=True)


# ---------------------------------------------------------------------------
# Identity resolution (§13)
# ---------------------------------------------------------------------------

def resolve_identity(user_doc: dict | None, telegram_user_id: int) -> dict:
    """Strongest reliable identity available in THIS repository.

    Precedence actually implemented:

        1. gaming account  — ``users.linked_gaming_accounts``, the materialized
           UIM mapping written verbatim by ``multi_account_risk_sync.py`` from
           the ``user_profile_summary`` sheet. Consumed as a snapshot; no
           account-linkage graph is recomputed here.
        2. telegram        — fallback when the mapping is absent.

    There is deliberately NO canonical ``identity_cluster_id`` tier: this repo
    has no such materialized field (see the gap analysis in the PR), and
    inventing one would be a fake equivalent.

    Freshness: ``linked_gaming_accounts`` is as fresh as the last
    multi_account_risk_sync run; ``multi_account_risk_synced_at`` on the user
    document records when. A campaign closing between syncs sees the previous
    snapshot — acceptable because the alternative (live graph traversal) is
    exactly what §37 forbids.

    ``account_keys`` carries *every* linked account so a shared account still
    collides even when two Telegram identities report differently ordered
    lists; ``identity_key`` is the deterministic primary (lexicographically
    smallest account id) used for reporting and the reward uniqueness index.
    """
    accounts = []
    for raw in ((user_doc or {}).get("linked_gaming_accounts") or []):
        if isinstance(raw, str) and raw.strip():
            accounts.append(raw.strip())
    accounts = sorted(set(accounts))

    if accounts:
        return {
            "identity_type": mp.IDENTITY_TYPE_GAMING_ACCOUNT,
            "identity_key": f"acct:{accounts[0]}",
            "account_keys": [f"acct:{a}" for a in accounts],
        }
    return {
        "identity_type": mp.IDENTITY_TYPE_TELEGRAM,
        "identity_key": f"tg:{telegram_user_id}",
        "account_keys": [f"tg:{telegram_user_id}"],
    }


def _claim_identity(campaign_id: str, entry_id, identity: dict, now: datetime) -> tuple[bool, str | None]:
    """Reserve every identity key an entry maps to, for this entry only.

    Ordering is by (submitted_at, _id) ascending, so the **earliest valid
    submission wins** an identity contest (§14) — deterministic and
    auditable. The unique index ``ux_mission_identity_claims_campaign_key``
    is the actual guarantee; the read below is just to produce the right
    reason code.

    Idempotent: a re-run of the same entry re-owns its own claims and
    returns success, so a crash mid-claim resumes cleanly.
    """
    keys = identity.get("account_keys") or [identity["identity_key"]]
    claims = _claims()

    # Check-all-then-claim-all, so a losing entry never leaves a key reserved
    # that a later legitimate entry would need.
    for key in keys:
        existing = claims.find_one({"campaign_id": campaign_id, "identity_key": key})
        if existing and existing.get("entry_id") != entry_id:
            reason = (mp.REASON_DUPLICATE_GAMING_ACCOUNT
                      if identity["identity_type"] == mp.IDENTITY_TYPE_GAMING_ACCOUNT
                      else mp.REASON_DUPLICATE_IDENTITY)
            return False, reason

    for key in keys:
        try:
            claims.insert_one({
                "campaign_id": campaign_id,
                "identity_key": key,
                "entry_id": entry_id,
                "identity_type": identity["identity_type"],
                "claimed_at": now,
            })
        except Exception as exc:
            if "duplicate" not in str(exc).lower():
                raise
            owner = claims.find_one({"campaign_id": campaign_id, "identity_key": key})
            if owner and owner.get("entry_id") != entry_id:
                # Lost the contest between the check above and this insert.
                # Give back the keys this entry did manage to claim, so a
                # disqualified entry never sits on an identity nobody won.
                # Safe because the quality/abuse gate already ran and passed
                # before any claiming started: releasing here can only ever
                # re-open a key to another entry of the SAME identity, never
                # to one that failed an abuse check.
                for owned in keys:
                    claims.delete_one({
                        "campaign_id": campaign_id,
                        "identity_key": owned,
                        "entry_id": entry_id,
                    })
                reason = (mp.REASON_DUPLICATE_GAMING_ACCOUNT
                          if identity["identity_type"] == mp.IDENTITY_TYPE_GAMING_ACCOUNT
                          else mp.REASON_DUPLICATE_IDENTITY)
                return False, reason
    return True, None


# ---------------------------------------------------------------------------
# Anti-abuse eligibility (§15, §16)
# ---------------------------------------------------------------------------

# Only fields this module actually evaluates are projected — a bulk load of
# 200 small documents, not an N+1 walk (§37).
_USER_PROJECTION = {
    "user_id": 1,
    "blocked": 1,
    "linked_gaming_accounts": 1,
    "multi_account_risk": 1,
    "multi_account_voucher_hunter": 1,
    "for_bot_segment": 1,
    "for_bot_segment_normalized": 1,
    "bot_segment": 1,
}


def _load_users(user_ids: list[int]) -> dict:
    """One bulk query per batch."""
    if not user_ids:
        return {}
    docs = database.db["users"].find({"user_id": {"$in": user_ids}}, projection=_USER_PROJECTION)
    return {int(d["user_id"]): d for d in docs if d.get("user_id") is not None}


def evaluate_quality_eligibility(entry: dict, user_doc: dict | None, policy: dict) -> str | None:
    """Quality/abuse gate ONLY (§16). Mission completion is the participation
    gate and winner selection is the reward gate — no segment probability is
    consulted anywhere in Mission Pool. The Standard Drop's probabilistic
    admission (``vouchers.assign_public_pool_access_once``) is not called and
    not replicated.

    Returns a machine-readable disqualification reason, or None to qualify.
    Flags are read verbatim from the canonical writers:
      * ``users.blocked``                        (bot user lifecycle)
      * ``users.multi_account_voucher_hunter``   (multi_account_risk_sync.py,
        from UIM's pre-computed behavioural+cluster flag) — also what
        ``effective_segment.resolve_effective_segment`` treats as an
        operational voucher_hunter override
      * ``users.multi_account_risk``             (multi_account_risk_sync.py,
        the same flag ``voucher_risk_eligibility`` reads)
      * canonical behavioural segment == voucher_hunter, resolved through
        ``effective_segment.resolve_effective_segment`` so this module never
        re-implements the segment normalisation rule
    """
    if policy.get("require_correct_answer", True) and entry.get("is_correct") is False:
        return mp.REASON_INCORRECT_ANSWER

    user_doc = user_doc or {}

    if policy.get("exclude_blocked", True) and user_doc.get("blocked"):
        return mp.REASON_BLOCKED

    if policy.get("exclude_voucher_hunter", True):
        from effective_segment import resolve_effective_segment

        if resolve_effective_segment(user_doc) == "voucher_hunter":
            return mp.REASON_VOUCHER_HUNTER

    if policy.get("exclude_multi_account_risk", True) and user_doc.get("multi_account_risk"):
        return mp.REASON_MULTI_ACCOUNT_RISK

    if policy.get("require_gaming_account", False) and not (user_doc.get("linked_gaming_accounts") or []):
        return mp.REASON_MISSING_GAMING_ACCOUNT

    return None


# ---------------------------------------------------------------------------
# Stage 1 — eligibility (§17)
# ---------------------------------------------------------------------------

def _eligibility_pass(fence: _Fence, campaign: dict, deadline: float) -> dict:
    """Process ``submitted`` entries in bounded batches, oldest first.

    Resumable without a stored cursor: an entry leaves ``submitted`` the
    moment it is decided, so the *next* batch is simply the next page of
    remaining ``submitted`` entries. A crash mid-batch re-reads the
    undecided remainder and nothing else."""
    campaign_id = fence.campaign_id
    policy = (campaign.get("mission_pool") or {}).get("eligibility_policy") or mp.DEFAULT_ELIGIBILITY_POLICY
    batch = eligibility_batch_size()
    qualified = disqualified = 0

    while True:
        if time.monotonic() > deadline:
            _log("eligibility_time_budget", campaign_id=campaign_id, qualified=qualified, disqualified=disqualified)
            return {"done": False, "qualified": qualified, "disqualified": disqualified}

        now = datetime.now(timezone.utc)
        if not _renew(fence, now):
            _log("ownership_lost", campaign_id=campaign_id, stage="eligibility", generation=fence.generation)
            return {"done": False, "ownership_lost": True, "qualified": qualified, "disqualified": disqualified}

        rows = list(_entries().find(
            {"campaign_id": campaign_id, "status": mp.ENTRY_STATUS_SUBMITTED},
            sort=[("submitted_at", 1), ("_id", 1)],
            limit=batch,
        ))
        if not rows:
            return {"done": True, "qualified": qualified, "disqualified": disqualified}

        users = _load_users([int(r["telegram_user_id"]) for r in rows])

        for entry in rows:
            uid = int(entry["telegram_user_id"])
            user_doc = users.get(uid)
            identity = resolve_identity(user_doc, uid)

            reason = evaluate_quality_eligibility(entry, user_doc, policy)
            if reason is None:
                ok, dup_reason = _claim_identity(campaign_id, entry["_id"], identity, now)
                if not ok:
                    reason = dup_reason

            new_status = mp.ENTRY_STATUS_QUALIFIED if reason is None else mp.ENTRY_STATUS_DISQUALIFIED
            _entries().update_one(
                {"_id": entry["_id"], "status": mp.ENTRY_STATUS_SUBMITTED},
                {"$set": {
                    "status": new_status,
                    "identity_key": identity["identity_key"],
                    "identity_type": identity["identity_type"],
                    "disqualification_reason": reason,
                    "eligibility_generation": fence.generation,
                    "updated_at": now,
                }},
            )
            if reason is None:
                qualified += 1
            else:
                disqualified += 1
                _emit("mission_entry_disqualified", campaign_id=campaign_id, telegram_user_id=uid,
                      status="fail", reason=reason, source="worker")

        _log("eligibility_batch", campaign_id=campaign_id, generation=fence.generation,
             batch=len(rows), qualified=qualified, disqualified=disqualified)


# ---------------------------------------------------------------------------
# Stage 2 — winner selection (§20, §21)
# ---------------------------------------------------------------------------

def _select_winners(fence: _Fence, campaign: dict, now: datetime) -> dict:
    """Draw the winner set exactly once.

    Auditability: the qualified set is frozen before this runs, ordered
    deterministically by (submitted_at, _id), and shuffled with a stored
    ``selection_seed``. Given the seed and the ordered id list the selection
    is exactly reproducible internally. The seed is generated *after* the
    campaign closes, so it is never exploitable material while submissions
    are still open.

    Retry safety: the seed is written under the fence with a filter that
    matches only when no seed exists yet. Once set, every retry — including a
    retry by a *different* worker with a newer generation — recomputes the
    identical winner set from the identical inputs rather than reshuffling.
    """
    campaign_id = fence.campaign_id
    block = campaign.get("mission_pool") or {}
    requested = int(block.get("winner_count") or 0)
    method = block.get("allocation_method") or mp.ALLOCATION_RANDOM_QUALIFIED

    seed = block.get("selection_seed")
    if not seed:
        seed = secrets.token_hex(16)
        res = _campaigns().update_one(
            {**_fenced_filter(fence), "$or": [
                {"mission_pool.selection_seed": {"$exists": False}},
                {"mission_pool.selection_seed": None},
                {"mission_pool.selection_seed": ""},
            ]},
            {"$set": {
                "mission_pool.selection_seed": seed,
                "mission_pool.selection_started_at": now,
                "mission_pool.updated_at": now,
            }},
        )
        if res.matched_count != 1:
            refreshed = _campaign_doc(campaign_id) or {}
            seed = (refreshed.get("mission_pool") or {}).get("selection_seed")
            if not seed:
                _log("selection_seed_write_failed", campaign_id=campaign_id, generation=fence.generation)
                return {"ok": False, "reason": "seed_write_failed"}

    # Ids only — a 12-byte ObjectId per qualified entry, deliberately not the
    # full documents (§18).
    cap = max_qualified_for_selection()
    rows = list(_entries().find(
        {"campaign_id": campaign_id, "status": {"$in": [
            mp.ENTRY_STATUS_QUALIFIED, mp.ENTRY_STATUS_WINNER, mp.ENTRY_STATUS_NON_WINNER,
            mp.ENTRY_STATUS_REWARD_ALLOCATING, mp.ENTRY_STATUS_REWARD_ALLOCATED,
        ]}},
        sort=[("submitted_at", 1), ("_id", 1)],
        limit=cap + 1,
        projection={"_id": 1},
    ))
    if len(rows) > cap:
        _log("qualified_set_capped", campaign_id=campaign_id, cap=cap)
        rows = rows[:cap]

    ordered_ids = [r["_id"] for r in rows]
    qualified_count = len(ordered_ids)

    if method == mp.ALLOCATION_FIRST_QUALIFIED:
        winner_ids = ordered_ids[:requested]
    else:
        shuffled = list(ordered_ids)
        random.Random(seed).shuffle(shuffled)
        winner_ids = shuffled[:requested]

    # qualified_count < winner_count -> award all qualified (§20).
    winner_set = set(winner_ids)
    actual = len(winner_set)

    # Mark winners in bounded chunks (one indexed multi-update per chunk
    # rather than one write per entry), then sweep every entry still sitting
    # at `qualified` into `non_winner` with a single update. Both halves are
    # idempotent, so a crash between them resumes correctly: re-running
    # recomputes the same winner_ids from the same seed, re-marks them
    # (a no-op for those already `winner` or further along) and re-sweeps.
    chunk_size = 500
    for start in range(0, len(winner_ids), chunk_size):
        chunk = winner_ids[start:start + chunk_size]
        _entries().update_many(
            {"_id": {"$in": chunk},
             "status": {"$in": [mp.ENTRY_STATUS_QUALIFIED, mp.ENTRY_STATUS_NON_WINNER]}},
            {"$set": {"status": mp.ENTRY_STATUS_WINNER, "updated_at": now}},
        )
    _entries().update_many(
        {"campaign_id": campaign_id, "status": mp.ENTRY_STATUS_QUALIFIED},
        {"$set": {"status": mp.ENTRY_STATUS_NON_WINNER, "updated_at": now}},
    )

    ok = _set_stage(
        fence, mp.STAGE_WINNERS_SELECTED, now,
        qualified_count=qualified_count,
        winner_count_requested=requested,
        winner_count_actual=actual,
        selection_completed_at=now,
    )
    _log("winners_selected", campaign_id=campaign_id, generation=fence.generation,
         qualified=qualified_count, requested=requested, actual=actual, method=method)
    return {"ok": ok, "qualified_count": qualified_count, "winner_count_actual": actual}


# ---------------------------------------------------------------------------
# Stage 3 — reward allocation (§22-§25)
# ---------------------------------------------------------------------------

def ensure_mission_reward_indexes() -> None:
    """Additive, partial-filtered indexes on the shared ``campaign_rewards``
    collection. ``partialFilterExpression`` scopes both uniques to
    ``category="mission_pool"`` rows, so existing tournament rewards are
    completely unaffected.

      ux_campaign_rewards_mission_entry
          UNIQUE (campaign_id, mission_entry_id) — one reward per entry, the
          DB-level backing for the reward idempotency key (§25).
      ux_campaign_rewards_mission_identity
          UNIQUE (campaign_id, identity_key) — one reward per deduplicated
          identity per campaign, even if two entries somehow reached winner
          state for the same identity.
      ix_campaign_rewards_mission_notify
          Notification worker scan.
    """
    try:
        rewards = _rewards()
        rewards.create_index(
            [("campaign_id", 1), ("mission_entry_id", 1)],
            name="ux_campaign_rewards_mission_entry",
            unique=True,
            partialFilterExpression={"category": REWARD_CATEGORY},
        )
        rewards.create_index(
            [("campaign_id", 1), ("identity_key", 1)],
            name="ux_campaign_rewards_mission_identity",
            unique=True,
            partialFilterExpression={"category": REWARD_CATEGORY},
        )
        rewards.create_index(
            [("category", 1), ("notification_status", 1), ("notification_next_attempt_at", 1)],
            name="ix_campaign_rewards_mission_notify",
        )
    except Exception:
        logger.warning("[MISSION_POOL] reward index creation failed", exc_info=True)


ensure_mission_reward_indexes()


def _release_losing_draw(code_doc: dict, reward_id: str, campaign_id: str) -> None:
    """Return a code that lost the binding race to ``available``.

    Only ever called for a draw that was NOT written onto any reward
    document, so no winner loses anything and no user-visible reward is
    revoked. The filter is deliberately narrow — same row, still ``issued``,
    still stamped with this reward_id, and NOT the code that actually won the
    binding — so it can never touch a delivered voucher."""
    winner_code = (_rewards().find_one({"reward_id": reward_id}) or {}).get("voucher_code")
    if winner_code == code_doc.get("code"):
        return
    try:
        database.db["voucher_pools"].update_one(
            {
                "_id": code_doc["_id"],
                "status": "issued",
                "issued_for_reward_id": reward_id,
                "code": {"$ne": winner_code},
            },
            {"$set": {
                "status": "available",
                "issued_to": None,
                "issued_to_user_id": None,
                "issued_at": None,
                "issued_for_reward_id": None,
            }},
        )
        _log("losing_draw_released", campaign_id=campaign_id, reward_id=reward_id)
    except Exception:
        logger.exception("[MISSION_POOL] losing_draw_release_failed reward=%s", reward_id)


def _allocate_for_entry(campaign: dict, entry: dict, now: datetime, generation: int) -> dict:
    """Allocate exactly one voucher to one winner, idempotently.

    THREE independent guarantees stack here, because ``allocate_voucher``
    alone is not enough:

    1. **Atomic code claim.** ``voucher_pool_service.allocate_voucher`` is a
       single ``find_one_and_update`` matching ``status="available"`` and
       flipping it to ``issued`` with ``issued_for_reward_id``. Two callers
       can never claim the SAME code, and inventory can never go negative
       because the filter itself is the decrement.

    2. **Compare-and-set binding, with compensation.** (1) does not stop two
       callers claiming two DIFFERENT codes for the same reward: both could
       read "no code yet" and each draw one. That window is reachable in
       production — a worker whose lease expires mid-batch keeps iterating
       while the new owner starts the same entries, and a claim check alone
       cannot stop a racer that already passed it. So the code is bound to
       the reward with a conditional update that only matches while
       ``voucher_code`` is still unset. Exactly one draw can win that update.
       A draw that loses is returned to ``available`` immediately — and that
       is safe precisely because it lost: it was never written onto a reward
       document, never notified, never shown in Campaign Rewards, never
       observable by any user. This is the one and only place a Mission Pool
       code goes back to inventory; a code that has been bound to a winner is
       never released, for any reason (§26).

    3. **Reward idempotency.** ``reward_id`` is a pure function of
       (campaign_id, entry_id), so any retry targets the same reward document
       and returns the code already bound there rather than drawing a second
       one. The (campaign_id, mission_entry_id) and (campaign_id, identity_key)
       partial unique indexes are the DB-level backstop.

    The fence ``generation`` is still stamped on the reward row: it keeps a
    demonstrably stale worker from even attempting a draw (cheap first line)
    and records which processing run produced the allocation, for forensics.

    A voucher assigned here belongs to the winner permanently. Nothing in the
    notification path can release it, reassign it, or cause a second
    allocation (§26).
    """
    import voucher_pool_service

    campaign_id = campaign["campaign_id"]
    block = campaign.get("mission_pool") or {}
    pool_id = block.get("pool_id")
    pool_type = block.get("pool_type") or "voucher_drop"
    entry_id = entry["_id"]
    reward_id = mp.mission_reward_id(campaign_id, entry_id)
    uid = int(entry["telegram_user_id"])

    existing = _rewards().find_one({"reward_id": reward_id})
    if existing and existing.get("status") == "assigned":
        return {"state": "already_allocated", "reward_id": reward_id}

    if not existing:
        doc = {
            "reward_id": reward_id,
            "category": REWARD_CATEGORY,
            "campaign_id": campaign_id,
            "mission_entry_id": entry_id,
            "identity_key": entry.get("identity_key"),
            "identity_type": entry.get("identity_type"),
            "telegram_user_id": uid,
            "idempotency_key": mp.reward_idempotency_key(campaign_id, entry_id),
            "reward_label": block.get("reward_label") or campaign.get("name", ""),
            "pool_id": pool_id,
            "pool_type": pool_type,
            "voucher_code": None,
            "status": "allocating",
            "assigned_at": None,
            "first_viewed_at": None,
            "copied_at": None,
            "notification_status": "pending",
            "notification_attempts": 0,
            "notification_next_attempt_at": now,
            "notification_last_error": None,
            "allocation_generation": generation,
            "winner_popup_pending": True,
            "winner_popup_shown_at": None,
            "winner_popup_acknowledged_at": None,
            "created_at": now,
            "updated_at": now,
        }
        try:
            _rewards().insert_one(doc)
        except Exception as exc:
            if "duplicate" not in str(exc).lower():
                raise
            # Another worker (or a racing retry) already created it.
            existing = _rewards().find_one({"reward_id": reward_id})
            if existing and existing.get("status") == "assigned":
                return {"state": "already_allocated", "reward_id": reward_id}
            if not existing:
                # Uniqueness tripped on (campaign_id, identity_key): this
                # identity already holds a Mission reward for this campaign.
                return {"state": "duplicate_identity", "reward_id": None}

    # Guarantee (2): take the reward row under this worker's fence before
    # touching inventory. A stale worker's generation is strictly lower, so
    # it can never win here and can never draw a second code.
    claimed = _rewards().find_one_and_update(
        {
            "reward_id": reward_id,
            "status": {"$ne": "assigned"},
            "$or": [
                {"allocation_generation": {"$exists": False}},
                {"allocation_generation": None},
                {"allocation_generation": {"$lte": generation}},
            ],
        },
        {"$set": {"allocation_generation": generation, "status": "allocating", "updated_at": now}},
        return_document=True,
    )
    if not claimed:
        current = _rewards().find_one({"reward_id": reward_id})
        if current and current.get("status") == "assigned":
            return {"state": "already_allocated", "reward_id": reward_id}
        # A newer owner holds this reward; stand down rather than racing it.
        _log("allocation_claim_lost", campaign_id=campaign_id, reward_id=reward_id,
             generation=generation)
        return {"state": "claim_lost", "reward_id": reward_id}

    # The reward document is authoritative for "which code is this winner's".
    if claimed.get("voucher_code"):
        return {"state": "already_allocated", "reward_id": reward_id}

    # A code may already be issued against this reward_id by a run that
    # crashed between drawing and binding — reuse it rather than draw again.
    code_doc = voucher_pool_service.voucher_already_allocated_for_reward(reward_id)
    if not code_doc:
        code_doc = voucher_pool_service.allocate_voucher(
            pool_id, reward_id=reward_id, telegram_user_id=uid,
            expected_pool_type=pool_type, now=now,
        )

    if not code_doc:
        _rewards().update_one(
            {"reward_id": reward_id, "status": {"$in": ["allocating", "approved", "pending_review"]},
             "$or": [{"voucher_code": None}, {"voucher_code": {"$exists": False}}]},
            {"$set": {"status": "out_of_stock", "updated_at": now}},
        )
        _emit("voucher_out_of_stock", campaign_id=campaign_id, telegram_user_id=uid,
              reward_id=reward_id, pool_id=pool_id, source="worker", status="fail")
        return {"state": "out_of_stock", "reward_id": reward_id}

    # Guarantee (2): bind the code only while none is bound yet.
    bound = _rewards().update_one(
        {"reward_id": reward_id,
         "$or": [{"voucher_code": None}, {"voucher_code": {"$exists": False}}]},
        {"$set": {
            "voucher_code": code_doc["code"],
            "status": "assigned",
            "assigned_at": now,
            "updated_at": now,
        }},
    )
    if bound.matched_count != 1:
        _release_losing_draw(code_doc, reward_id, campaign_id)
        return {"state": "already_allocated", "reward_id": reward_id}

    _emit("voucher_assigned", campaign_id=campaign_id, telegram_user_id=uid,
          reward_id=reward_id, pool_id=pool_id, source="worker")
    return {"state": "allocated", "reward_id": reward_id}


def _allocation_pass(fence: _Fence, campaign: dict, deadline: float) -> dict:
    campaign_id = fence.campaign_id
    batch = allocation_batch_size()
    allocated = out_of_stock = 0

    while True:
        if time.monotonic() > deadline:
            return {"done": False, "allocated": allocated, "out_of_stock": out_of_stock}

        now = datetime.now(timezone.utc)
        if not _renew(fence, now):
            _log("ownership_lost", campaign_id=campaign_id, stage="allocation", generation=fence.generation)
            return {"done": False, "ownership_lost": True, "allocated": allocated, "out_of_stock": out_of_stock}

        # A cancel landing mid-run stops new allocations immediately; codes
        # already assigned stay assigned.
        fresh = _campaign_doc(campaign_id) or {}
        if (fresh.get("mission_pool") or {}).get("cancelled"):
            _log("allocation_halted_cancelled", campaign_id=campaign_id, allocated=allocated)
            return {"done": False, "cancelled": True, "allocated": allocated, "out_of_stock": out_of_stock}

        rows = list(_entries().find(
            {"campaign_id": campaign_id, "status": mp.ENTRY_STATUS_WINNER},
            sort=[("submitted_at", 1), ("_id", 1)],
            limit=batch,
        ))
        if not rows:
            return {"done": True, "allocated": allocated, "out_of_stock": out_of_stock}

        for entry in rows:
            _entries().update_one(
                {"_id": entry["_id"], "status": mp.ENTRY_STATUS_WINNER},
                {"$set": {"status": mp.ENTRY_STATUS_REWARD_ALLOCATING, "updated_at": now}},
            )
            try:
                result = _allocate_for_entry(campaign, entry, now, fence.generation)
            except Exception:
                logger.exception("[MISSION_POOL] allocation_failed campaign=%s entry=%s",
                                 campaign_id, entry["_id"])
                _entries().update_one(
                    {"_id": entry["_id"], "status": mp.ENTRY_STATUS_REWARD_ALLOCATING},
                    {"$set": {"status": mp.ENTRY_STATUS_WINNER, "updated_at": now}},
                )
                return {"done": False, "error": "allocation_error",
                        "allocated": allocated, "out_of_stock": out_of_stock}

            state = result["state"]
            if state in ("allocated", "already_allocated"):
                allocated += 1
                _entries().update_one(
                    {"_id": entry["_id"]},
                    {"$set": {
                        "status": mp.ENTRY_STATUS_REWARD_ALLOCATED,
                        "reward_id": result["reward_id"],
                        "updated_at": now,
                    }},
                )
            elif state == "claim_lost":
                # A newer owner has this reward. Put the entry back so
                # whoever owns the campaign now picks it up.
                _entries().update_one(
                    {"_id": entry["_id"], "status": mp.ENTRY_STATUS_REWARD_ALLOCATING},
                    {"$set": {"status": mp.ENTRY_STATUS_WINNER, "updated_at": now}},
                )
                return {"done": False, "ownership_lost": True,
                        "allocated": allocated, "out_of_stock": out_of_stock}
            elif state == "duplicate_identity":
                _entries().update_one(
                    {"_id": entry["_id"]},
                    {"$set": {
                        "status": mp.ENTRY_STATUS_DISQUALIFIED,
                        "disqualification_reason": mp.REASON_ALREADY_REWARDED,
                        "updated_at": now,
                    }},
                )
            else:  # out_of_stock
                out_of_stock += 1
                _entries().update_one(
                    {"_id": entry["_id"]},
                    {"$set": {
                        "status": mp.ENTRY_STATUS_DISQUALIFIED,
                        "disqualification_reason": mp.REASON_OUT_OF_STOCK,
                        "reward_id": result["reward_id"],
                        "updated_at": now,
                    }},
                )

        _log("allocation_batch", campaign_id=campaign_id, generation=fence.generation,
             batch=len(rows), allocated=allocated, out_of_stock=out_of_stock)


# ---------------------------------------------------------------------------
# Stage 4 — notification (§26, §27)
# ---------------------------------------------------------------------------

def _notification_pass(fence: _Fence, campaign: dict, deadline: float) -> dict:
    """Deliver winner notifications. Completely decoupled from voucher
    ownership: a Telegram failure only ever moves ``notification_status``,
    never ``status``/``voucher_code``, so the reward stays visible in
    Campaign Rewards and can never be handed to a different winner (§26).
    """
    from telegram_utils import send_telegram_http_message

    campaign_id = fence.campaign_id
    campaign_name = campaign.get("name", "")
    batch = notify_batch_size()
    sent = failed = 0

    while True:
        if time.monotonic() > deadline:
            return {"done": False, "sent": sent, "failed": failed}

        now = datetime.now(timezone.utc)
        if not _renew(fence, now):
            _log("ownership_lost", campaign_id=campaign_id, stage="notify", generation=fence.generation)
            return {"done": False, "ownership_lost": True, "sent": sent, "failed": failed}

        fresh = _campaign_doc(campaign_id) or {}
        if (fresh.get("mission_pool") or {}).get("cancelled"):
            return {"done": False, "cancelled": True, "sent": sent, "failed": failed}

        rows = list(_rewards().find(
            {
                "campaign_id": campaign_id,
                "category": REWARD_CATEGORY,
                "status": "assigned",
                "notification_status": {"$in": ["pending", "failed_retryable"]},
                "notification_next_attempt_at": {"$lte": now},
            },
            sort=[("notification_next_attempt_at", 1), ("_id", 1)],
            limit=batch,
        ))
        if not rows:
            return {"done": True, "sent": sent, "failed": failed}

        for reward in rows:
            uid = int(reward["telegram_user_id"])
            attempts = int(reward.get("notification_attempts") or 0) + 1
            text = WINNER_MESSAGE_TEMPLATE.format(campaign_name=campaign_name)

            # Claim BEFORE sending: atomically bump the attempt counter and
            # push the next-attempt time out, so a second worker scanning the
            # same window cannot send this winner a duplicate congratulation.
            # If the send then fails, the states below correct the schedule.
            claimed = _rewards().find_one_and_update(
                {
                    "reward_id": reward["reward_id"],
                    "notification_attempts": int(reward.get("notification_attempts") or 0),
                    "notification_status": {"$in": ["pending", "failed_retryable"]},
                },
                {"$set": {
                    "notification_attempts": attempts,
                    "notification_next_attempt_at": now + timedelta(
                        seconds=NOTIFY_BACKOFF_BASE_SECONDS * (2 ** (attempts - 1))),
                    "updated_at": now,
                }},
                return_document=True,
            )
            if not claimed:
                continue

            try:
                ok, err, blocked = send_telegram_http_message(uid, text)
            except Exception as exc:  # never let one send abort the batch
                ok, err, blocked = False, f"{exc.__class__.__name__}", False

            if ok:
                sent += 1
                _rewards().update_one(
                    {"reward_id": reward["reward_id"]},
                    {"$set": {
                        "notification_status": "sent",
                        "notification_attempts": attempts,
                        "notification_sent_at": now,
                        "notification_last_error": None,
                        "updated_at": now,
                    }},
                )
                _emit("mission_notification_sent", campaign_id=campaign_id, telegram_user_id=uid,
                      reward_id=reward["reward_id"], source="worker")
                continue

            failed += 1
            terminal = blocked or err in ("bot_blocked", "chat_not_found", "user_deactivated") \
                or attempts >= NOTIFY_MAX_ATTEMPTS
            next_at = now + timedelta(seconds=NOTIFY_BACKOFF_BASE_SECONDS * (2 ** (attempts - 1)))
            _rewards().update_one(
                {"reward_id": reward["reward_id"]},
                {"$set": {
                    "notification_status": "failed_terminal" if terminal else "failed_retryable",
                    "notification_attempts": attempts,
                    "notification_next_attempt_at": next_at,
                    "notification_last_error": err,
                    "updated_at": now,
                }},
            )
            _emit("mission_notification_failed", campaign_id=campaign_id, telegram_user_id=uid,
                  reward_id=reward["reward_id"], source="worker", status="fail", reason=err)

        _log("notify_batch", campaign_id=campaign_id, generation=fence.generation,
             batch=len(rows), sent=sent, failed=failed)


def _notifications_outstanding(campaign_id: str) -> int:
    return _rewards().count_documents({
        "campaign_id": campaign_id,
        "category": REWARD_CATEGORY,
        "status": "assigned",
        "notification_status": {"$in": ["pending", "failed_retryable"]},
    })


# ---------------------------------------------------------------------------
# State machine driver (§18, §34)
# ---------------------------------------------------------------------------

def process_campaign(campaign_id: str, *, source: str = "worker") -> dict:
    """Advance one campaign as far as the time budget and its own state
    allow. Safe to call repeatedly, concurrently, and after a crash at any
    point: every stage is idempotent and every mutation is fenced."""
    if not mp.mission_pool_enabled():
        return {"skipped": "mission_pool_disabled"}

    campaign = _campaign_doc(campaign_id)
    if not campaign or not mp.is_mission_pool(campaign):
        return {"skipped": "not_a_mission_campaign"}
    if not mp.is_closed_for_processing(campaign):
        return {"skipped": "not_closed"}

    now = datetime.now(timezone.utc)
    fence = _claim_campaign(campaign_id, now)
    if not fence:
        return {"skipped": "not_owner"}

    deadline = time.monotonic() + time_budget_seconds()
    stages: list[str] = []
    result: dict = {"campaign_id": campaign_id, "generation": fence.generation, "source": source}

    try:
        campaign = _campaign_doc(campaign_id) or campaign
        stage = _stage_of(campaign)

        if stage == mp.STAGE_PENDING:
            if not _set_stage(fence, mp.STAGE_PROCESSING_ELIGIBILITY, datetime.now(timezone.utc)):
                return {**result, "skipped": "ownership_lost"}
            stage = mp.STAGE_PROCESSING_ELIGIBILITY

        if stage == mp.STAGE_PROCESSING_ELIGIBILITY:
            stages.append(stage)
            out = _eligibility_pass(fence, campaign, deadline)
            result["eligibility"] = out
            if not out.get("done"):
                return result
            if not _set_stage(fence, mp.STAGE_QUALIFIED_SNAPSHOT_READY, datetime.now(timezone.utc)):
                return {**result, "skipped": "ownership_lost"}
            stage = mp.STAGE_QUALIFIED_SNAPSHOT_READY

        if stage == mp.STAGE_QUALIFIED_SNAPSHOT_READY:
            stages.append(stage)
            if not _set_stage(fence, mp.STAGE_SELECTING_WINNERS, datetime.now(timezone.utc)):
                return {**result, "skipped": "ownership_lost"}
            stage = mp.STAGE_SELECTING_WINNERS

        if stage == mp.STAGE_SELECTING_WINNERS:
            stages.append(stage)
            campaign = _campaign_doc(campaign_id) or campaign
            out = _select_winners(fence, campaign, datetime.now(timezone.utc))
            result["selection"] = out
            if not out.get("ok"):
                return result
            stage = mp.STAGE_WINNERS_SELECTED

        if stage == mp.STAGE_WINNERS_SELECTED:
            stages.append(stage)
            if not _set_stage(fence, mp.STAGE_ALLOCATING_REWARDS, datetime.now(timezone.utc)):
                return {**result, "skipped": "ownership_lost"}
            stage = mp.STAGE_ALLOCATING_REWARDS

        if stage == mp.STAGE_ALLOCATING_REWARDS:
            stages.append(stage)
            campaign = _campaign_doc(campaign_id) or campaign
            out = _allocation_pass(fence, campaign, deadline)
            result["allocation"] = out
            if not out.get("done"):
                return result
            if not _set_stage(fence, mp.STAGE_NOTIFYING, datetime.now(timezone.utc),
                              allocation_count=out.get("allocated", 0)):
                return {**result, "skipped": "ownership_lost"}
            stage = mp.STAGE_NOTIFYING

        if stage == mp.STAGE_NOTIFYING:
            stages.append(stage)
            campaign = _campaign_doc(campaign_id) or campaign
            out = _notification_pass(fence, campaign, deadline)
            result["notification"] = out
            if not out.get("done"):
                return result
            final_now = datetime.now(timezone.utc)
            if _notifications_outstanding(campaign_id):
                # Retryable sends still queued: stay in `notifying` so the
                # next tick picks them up. Vouchers are already owned.
                return result
            _set_stage(fence, mp.STAGE_COMPLETED, final_now,
                       notification_sent_count=out.get("sent", 0),
                       completed_at=final_now)
            result["completed"] = True
            _emit("mission_campaign_completed", campaign_id=campaign_id, source=source)

        result["stages"] = stages
        return result
    finally:
        _release(fence, datetime.now(timezone.utc))


def find_due_campaigns(now: datetime | None = None, limit: int | None = None) -> list[str]:
    """Campaigns whose submissions have closed and whose processing has not
    reached ``completed``. Indexed by ``ix_gc_campaigns_status`` /
    ``ix_gc_campaigns_ends_at`` and bounded by ``limit``."""
    now = now or datetime.now(timezone.utc)
    limit = limit or max_campaigns_per_tick()
    docs = _campaigns().find(
        {
            "mechanic": mp.MECHANIC_MISSION_POOL,
            "mission_pool.processing_stage": {"$ne": mp.STAGE_COMPLETED},
            "mission_pool.cancelled": {"$ne": True},
        },
        sort=[("schedule.ends_at", 1)],
        limit=limit * 4,
    )
    out = []
    for doc in docs:
        if mp.is_closed_for_processing(doc, now):
            out.append(doc["campaign_id"])
        if len(out) >= limit:
            break
    return out


def run_mission_pool_processor() -> dict:
    """Scheduler entry point. Bounded, independently locked, resumable,
    time-budgeted and observable (§28) — never an unbounded
    ``process_all_missions()`` bolted onto the shared 5-minute tick."""
    if not mp.mission_pool_enabled():
        _log("processor_skipped", reason="disabled")
        return {"skipped": "disabled"}

    started = time.monotonic()
    campaign_ids = find_due_campaigns()
    results = []
    for campaign_id in campaign_ids:
        try:
            results.append(process_campaign(campaign_id))
        except Exception:
            logger.exception("[MISSION_POOL] processor_failed campaign=%s", campaign_id)
            results.append({"campaign_id": campaign_id, "error": "exception"})
    summary = {
        "campaigns": len(campaign_ids),
        "duration_ms": int((time.monotonic() - started) * 1000),
        "results": results,
    }
    _log("processor_tick", campaigns=len(campaign_ids), duration_ms=summary["duration_ms"])
    return summary
