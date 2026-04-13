from __future__ import annotations

import logging
from datetime import datetime, timezone

from app_context import get_bot, run_bot_coroutine
from pymongo import ReturnDocument
from config import (
    AFFILIATE_GROUP_DM_ENABLED,
    AFFILIATE_GROUP_INVITE_URL,
    AFFILIATE_GROUP_UNLOCK_REFERRALS,
)
from telegram_utils import safe_send_message, send_telegram_http_message

logger = logging.getLogger(__name__)


def _short_error(err: str | None) -> str:
    if not err:
        return "unknown_error"
    return str(err)[:160]


def _send_affiliate_group_dm(*, user_id: int, invite_url: str, threshold: int) -> tuple[bool, str | None]:
    text = (
        f"🎉 You unlocked affiliate group access after {int(threshold)} valid referrals.\n\n"
        f"Join here:\n{invite_url}"
    )

    bot = get_bot()
    if bot is not None:
        try:
            result = run_bot_coroutine(
                safe_send_message(
                    bot,
                    int(user_id),
                    text,
                    send_type="affiliate_group_unlock",
                    return_error=True,
                    logger=logger,
                )
            )
            if isinstance(result, tuple):
                ok, err = result
                if ok:
                    return True, None
                return False, _short_error(err)
            if result:
                return True, None
            return False, "send_failed"
        except Exception as exc:
            logger.warning("[AFFILIATE_GROUP][DM_FAIL] uid=%s via=bot_loop err=%s", user_id, exc)

    ok, err, _ = send_telegram_http_message(int(user_id), text, log=logger)
    if ok:
        return True, None
    return False, _short_error(err)


def maybe_unlock_affiliate_group(
    *,
    db,
    user_id: int,
    current_ref_total: int,
    new_ref_total: int,
    now_utc: datetime | None = None,
) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    threshold = int(AFFILIATE_GROUP_UNLOCK_REFERRALS)
    invite_url = str(AFFILIATE_GROUP_INVITE_URL or "").strip()

    if not AFFILIATE_GROUP_DM_ENABLED:
        logger.info("[AFFILIATE_GROUP][SKIP] uid=%s reason=disabled", user_id)
        return {"status": "skipped", "reason": "disabled", "dm_sent": False, "unlocked": False}

    if not invite_url:
        logger.info("[AFFILIATE_GROUP][SKIP] uid=%s reason=missing_invite_url", user_id)
        return {"status": "skipped", "reason": "missing_invite_url", "dm_sent": False, "unlocked": False}

    if int(current_ref_total) >= threshold:
        logger.info("[AFFILIATE_GROUP][SKIP] uid=%s reason=already_at_or_above_threshold current=%s", user_id, current_ref_total)
        return {"status": "skipped", "reason": "already_at_or_above_threshold", "dm_sent": False, "unlocked": False}

    if int(new_ref_total) < threshold:
        logger.info("[AFFILIATE_GROUP][SKIP] uid=%s reason=below_threshold new_total=%s", user_id, new_ref_total)
        return {"status": "skipped", "reason": "below_threshold", "dm_sent": False, "unlocked": False}

    unlocked_doc = db.users.find_one_and_update(
        {"user_id": int(user_id), "affiliate_group_unlocked_at": {"$exists": False}},
        {
            "$set": {
                "affiliate_group_unlocked_at": now_utc,
                "affiliate_group_unlock_referrals": threshold,
                "affiliate_group_invite_url": invite_url,
            },
            "$unset": {"affiliate_group_dm_error": ""},
        },
        return_document=ReturnDocument.AFTER,
    )

    if not unlocked_doc:
        logger.info("[AFFILIATE_GROUP][SKIP] uid=%s reason=already_unlocked", user_id)
        return {"status": "skipped", "reason": "already_unlocked", "dm_sent": False, "unlocked": False}

    logger.info(
        "[AFFILIATE_GROUP][UNLOCK] uid=%s current=%s new=%s threshold=%s",
        user_id,
        current_ref_total,
        new_ref_total,
        threshold,
    )

    ok, err = _send_affiliate_group_dm(user_id=int(user_id), invite_url=invite_url, threshold=threshold)
    if ok:
        db.users.update_one(
            {"user_id": int(user_id)},
            {
                "$set": {
                    "affiliate_group_dm_sent_at": now_utc,
                    "affiliate_group_invite_url": invite_url,
                    "affiliate_group_unlock_referrals": threshold,
                },
                "$unset": {"affiliate_group_dm_error": ""},
            },
        )
        logger.info("[AFFILIATE_GROUP][DM_OK] uid=%s", user_id)
        return {"status": "ok", "reason": "dm_sent", "dm_sent": True, "unlocked": True}

    db.users.update_one(
        {"user_id": int(user_id)},
        {
            "$inc": {"affiliate_group_dm_attempts": 1},
            "$set": {
                "affiliate_group_dm_error": _short_error(err),
                "affiliate_group_invite_url": invite_url,
                "affiliate_group_unlock_referrals": threshold,
            },
        },
    )
    logger.error("[AFFILIATE_GROUP][DM_FAIL] uid=%s err=%s", user_id, err)
    return {"status": "error", "reason": "dm_failed", "dm_sent": False, "unlocked": True}
