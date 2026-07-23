"""Canonical referral-destination resolver.

Single source of truth for which chat (community group or official
channel) newly generated referral invite links should point to.

``main.py`` and ``scheduler.py`` both import ``COMMUNITY_GROUP_ID`` /
``OFFICIAL_CHANNEL_ID`` from here instead of independently re-parsing
environment variables, so the two runtimes can never disagree about chat
identity.

Environment contract
---------------------
COMMUNITY_GROUP_ID          - community chatroom chat id (falls back to the
                               legacy MAIN_GROUP_ID / GROUP_ID env vars, then
                               the historical hardcoded chat id).
OFFICIAL_CHANNEL_ID         - official channel chat id.
REFERRAL_DESTINATION_MODE   - "community_group" (default) or
                               "official_channel".
REFERRAL_DESTINATION_CHAT_ID- optional explicit chat id override, only
                               consulted when mode == "official_channel".

Rollback to group-link generation only requires setting
``REFERRAL_DESTINATION_MODE=community_group`` (or unsetting it) — no code
change is needed.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# Historical hardcoded chat ids (pre-migration defaults), preserved as the
# ultimate fallback so an unset/blank env var never breaks production.
_DEFAULT_COMMUNITY_GROUP_ID = -1002304653063
_DEFAULT_OFFICIAL_CHANNEL_ID = -1002396761021

COMMUNITY_GROUP = "community_group"
OFFICIAL_CHANNEL = "official_channel"
VALID_DESTINATION_TYPES = {COMMUNITY_GROUP, OFFICIAL_CHANNEL}


def _parse_chat_id(raw, default, *, label: str) -> int:
    if raw in (None, ""):
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        logger.error(
            "[REFERRAL][CONFIG] invalid_chat_id label=%s raw=%s fallback=%s",
            label,
            raw,
            default,
        )
        return default


_raw_community_group_id = (
    os.getenv("COMMUNITY_GROUP_ID")
    or os.getenv("MAIN_GROUP_ID")
    or os.getenv("GROUP_ID")
)
COMMUNITY_GROUP_ID = _parse_chat_id(
    _raw_community_group_id, _DEFAULT_COMMUNITY_GROUP_ID, label="COMMUNITY_GROUP_ID"
)

_raw_official_channel_id = os.getenv("OFFICIAL_CHANNEL_ID")
OFFICIAL_CHANNEL_ID = _parse_chat_id(
    _raw_official_channel_id, _DEFAULT_OFFICIAL_CHANNEL_ID, label="OFFICIAL_CHANNEL_ID"
)


def get_referral_destination() -> tuple[int, str]:
    """Resolve the chat id + destination type new referral links must target.

    Returns ``(chat_id, destination_type)`` where ``destination_type`` is
    exactly ``"community_group"`` or ``"official_channel"``. Never raises —
    an invalid mode or chat id override fails safely back to
    ``community_group`` with an error log rather than silently generating
    links for an unknown destination.
    """
    raw_mode = (os.getenv("REFERRAL_DESTINATION_MODE") or COMMUNITY_GROUP).strip().lower()
    if raw_mode not in VALID_DESTINATION_TYPES:
        logger.error(
            "[REFERRAL][CONFIG] invalid_destination_mode mode=%s fallback=%s",
            raw_mode,
            COMMUNITY_GROUP,
        )
        raw_mode = COMMUNITY_GROUP

    if raw_mode == COMMUNITY_GROUP:
        return COMMUNITY_GROUP_ID, COMMUNITY_GROUP

    override_raw = os.getenv("REFERRAL_DESTINATION_CHAT_ID")
    if override_raw not in (None, ""):
        try:
            override_chat_id = int(str(override_raw).strip())
        except (TypeError, ValueError):
            logger.error(
                "[REFERRAL][CONFIG] invalid_destination_chat_id_override raw=%s "
                "fallback=official_channel_default",
                override_raw,
            )
            override_chat_id = None
        if override_chat_id is not None:
            return override_chat_id, OFFICIAL_CHANNEL

    if OFFICIAL_CHANNEL_ID is None:
        logger.error(
            "[REFERRAL][CONFIG] official_channel_unset fallback=%s",
            COMMUNITY_GROUP,
        )
        return COMMUNITY_GROUP_ID, COMMUNITY_GROUP

    return OFFICIAL_CHANNEL_ID, OFFICIAL_CHANNEL


def destination_type_for_chat_id(chat_id: int | None) -> str:
    """Best-effort classification of a chat id into a destination_type.

    Used to derive destination metadata for events/chats that were not
    generated through :func:`get_referral_destination` (e.g. legacy rows,
    or an event chat id read straight off a Telegram update).

    Also recognizes the *currently live* resolved destination so a
    REFERRAL_DESTINATION_CHAT_ID override (a channel id that differs from
    OFFICIAL_CHANNEL_ID) is classified as "official_channel" rather than
    falling through to "community_group".
    """
    if chat_id == OFFICIAL_CHANNEL_ID:
        return OFFICIAL_CHANNEL
    try:
        live_chat_id, live_type = get_referral_destination()
    except Exception:
        return COMMUNITY_GROUP
    if live_type == OFFICIAL_CHANNEL and chat_id == live_chat_id:
        return OFFICIAL_CHANNEL
    return COMMUNITY_GROUP
