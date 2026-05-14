"""Pure referral reward rules."""

REFERRAL_XP_PER_SUCCESS = 60
REFERRAL_BONUS_INTERVAL = 3
REFERRAL_BONUS_XP = 400
BASE_REFERRAL_XP = REFERRAL_XP_PER_SUCCESS
REFERRAL_SUCCESS_EVENT = "ref_success"
REFERRAL_BONUS_EVENT = "ref_bonus"

def calc_referral_progress(total_referrals: int, milestone_size: int = 3) -> dict[str, float | int | bool]:
    """
    Returns referral milestone progress details.
    progress: referrals toward next milestone (total % milestone_size)
    remaining: referrals remaining to hit next milestone
    near_miss: True only when one referral away from milestone
    progress_pct: progress as percent of milestone
    """
    progress = total_referrals % milestone_size
    remaining = milestone_size if progress == 0 else milestone_size - progress
    near_miss = progress == milestone_size - 1
    progress_pct = 0 if progress == 0 else (progress / milestone_size) * 100
    return {
        "progress": progress,
        "remaining": remaining,
        "near_miss": near_miss,
        "progress_pct": progress_pct,
    }
    
def calc_referral_award(new_ref_count_total: int) -> tuple[int, int]:
    """
    Returns (xp_added_total, bonus_added) for a successful referral.
    xp_added_total includes base 30 plus bonus when applicable.
    bonus_added is 200 only if new_ref_count_total % 3 == 0 else 0.
    """
    bonus_added = REFERRAL_BONUS_XP if new_ref_count_total % REFERRAL_BONUS_INTERVAL == 0 else 0
    return REFERRAL_XP_PER_SUCCESS + bonus_added, bonus_added


def upsert_referral_and_update_user_count(referrals_col, users_col, referrer_user_id: int, referred_user_id: int) -> dict:
    result = referrals_col.update_one(
        {"referrer_user_id": referrer_user_id, "referred_user_id": referred_user_id},
        {"$setOnInsert": {"status": "confirmed"}},
        upsert=True,
    )
    counted = bool(getattr(result, "upserted_id", None))
    if counted:
        users_col.update_one({"user_id": referrer_user_id}, {"$inc": {"total_referrals": 1}}, upsert=True)
    return {"counted": counted}


def grant_referral_rewards(db, users_col, referrer_user_id: int, referred_user_id: int) -> dict:
    user_doc = users_col.find_one({"user_id": referrer_user_id}) or {}
    total_referrals = int(user_doc.get("total_referrals", 0))
    xp_added, bonus_added = calc_referral_award(total_referrals)
    db.xp_events.update_one(
        {"user_id": referrer_user_id, "unique_key": f"{REFERRAL_SUCCESS_EVENT}:{referred_user_id}"},
        {"$setOnInsert": {"type": REFERRAL_SUCCESS_EVENT, "xp": REFERRAL_XP_PER_SUCCESS}},
        upsert=True,
    )
    if bonus_added:
        bonus_idx = max(1, total_referrals // REFERRAL_BONUS_INTERVAL)
        db.xp_events.update_one(
            {"user_id": referrer_user_id, "unique_key": f"{REFERRAL_BONUS_EVENT}:{bonus_idx}"},
            {"$setOnInsert": {"type": REFERRAL_BONUS_EVENT, "xp": REFERRAL_BONUS_XP}},
            upsert=True,
        )
    return {"xp_added_total": xp_added, "bonus_added": bonus_added}


def reconcile_referrals(referrals: list[dict], xp_events: list[dict]) -> list[dict]:
    confirmed_invitees = sorted(
        {
            int(r.get("invitee_user_id") or r.get("referred_user_id"))
            for r in referrals
            if str(r.get("status", "")).lower() in {"confirmed", "qualified", "settled", "success"}
            and (r.get("invitee_user_id") is not None or r.get("referred_user_id") is not None)
        }
    )
    success_keys = {
        str(ev.get("unique_key"))
        for ev in xp_events
        if ev.get("type") == REFERRAL_SUCCESS_EVENT and ev.get("unique_key")
    }
    expected_success = {f"{REFERRAL_SUCCESS_EVENT}:{iid}" for iid in confirmed_invitees}
    if success_keys != expected_success:
        return [{"kind": "success_mismatch", "expected": sorted(expected_success), "actual": sorted(success_keys)}]
    return []


def build_public_referral_status(row: dict | None, logger=None) -> dict[str, str]:
    raw_status = str((row or {}).get("status") or "").strip().lower()
    raw_reason = str((row or {}).get("revoked_reason") or "").strip().lower()
    reason = raw_reason or raw_status
    if reason in {"qualified", "success", "settled", "awarded"}:
        return {"status": "qualified", "label": "Qualified", "icon": "🟢", "tone": "success"}
    if reason in {"pending", "pending_channel", "processing"}:
        return {"status": "pending", "label": "Checking", "icon": "🟡", "tone": "warning"}
    if reason == "not_subscribed_channel":
        return {"status": "failed", "label": "Not subscribed", "icon": "🔴", "tone": "danger"}
    if reason == "left_channel":
        return {"status": "failed", "label": "Left channel", "icon": "🔴", "tone": "danger"}
    if reason == "left_group":
        return {"status": "failed", "label": "Left group", "icon": "🔴", "tone": "danger"}
    if reason in {"duplicate", "blocked", "abuse", "deny_severe", "unknown", "revoked", "failed", "rejected", "expired"}:
        return {"status": "failed", "label": "Not eligible", "icon": "🔴", "tone": "danger"}
    if logger is not None:
        logger.info("[REFERRAL_PROGRESS][UNKNOWN_REASON] raw_status=%s raw_reason=%s", raw_status, raw_reason)
    return {"status": "failed", "label": "Not eligible", "icon": "🔴", "tone": "danger"}
