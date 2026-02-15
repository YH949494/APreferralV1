"""Pure referral reward rules."""

REFERRAL_XP_PER_SUCCESS = 60
REFERRAL_BONUS_INTERVAL = 3
REFERRAL_BONUS_XP = 400

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
