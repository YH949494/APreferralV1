"""Pure referral reward rules."""

REFERRAL_XP_PER_SUCCESS = 30
REFERRAL_BONUS_INTERVAL = 3
REFERRAL_BONUS_XP = 200

def calc_referral_award(new_ref_count: int) -> tuple[int, int]:
    """
    Returns (xp_added_total, bonus_added) for a successful referral.
    xp_added_total includes base 30 plus bonus when applicable.
    bonus_added is 200 only if new_ref_count % 3 == 0 else 0.
    """
    bonus_added = REFERRAL_BONUS_XP if new_ref_count % REFERRAL_BONUS_INTERVAL == 0 else 0
    return REFERRAL_XP_PER_SUCCESS + bonus_added, bonus_added
