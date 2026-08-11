from collections import Counter
from datetime import datetime, timedelta, timezone
import logging
import os
import pytz
import requests
import socket
import time
from html import escape as html_escape
from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError
from pm_preferences import pm_allowed
from requests import RequestException
from database import db
from referral_rules import calc_referral_award
from xp import grant_xp, now_utc, now_kl
from affiliate_rewards import mark_invitee_qualified
from affiliate_group_access import maybe_unlock_affiliate_group
import referral_invitee_lock
from referral_ledger import with_not_invalidated

from telegram_utils import send_telegram_http_message

try:
    from settings_service import get_setting as _get_setting
except Exception:  # pragma: no cover
    _get_setting = None


def _journey_setting(field: str, fallback):
    if _get_setting is None:
        return fallback
    try:
        value = _get_setting("welcome_journey", field)
        return value if value is not None else fallback
    except Exception:
        return fallback


def _referral_setting(field: str, fallback):
    if _get_setting is None:
        return fallback
    try:
        value = _get_setting("referral_config", field)
        return value if value is not None else fallback
    except Exception:
        return fallback


# ---------------------------------------------------------------------------
# Welcome reminder flow (consolidated, Phase 2)
#
# Single source of truth per lifecycle stage, split across two cron jobs that
# never nudge the same user for the same thing:
#
#   1. process_welcome_voucher_lifecycle (*/30 min) - Day-0 safety net only.
#      Owns users who joined but have not completed a single check-in yet
#      (no "welcome_reminders" doc exists for them, since that doc is only
#      created on first check-in - see vouchers.record_welcome_checkin_progress).
#      Sends the generic "voucher waiting" / "last chance" nudge and performs
#      the hard expiry/final-warning bookkeeping on `welcome_eligibility`.
#      As soon as a user has any check-in, this job stops sending them
#      reminders (see the `welcome_reminders` ownership guard below) so it
#      never overlaps with job #2's check-in-aware copy.
#
#   2. process_welcome_reminders (hourly) - Owns every user who has started
#      checking in (i.e. has a `welcome_reminders` doc). Reads live progress
#      from vouchers.get_welcome_progress and sends, at most once each:
#        - reminder_20h  (stuck on 1/3, ~20h after Day 1)
#        - reminder_28h  (stuck on 1/3, ~28h after Day 1 - more urgency)
#        - day2_reminder (stuck on 2/3, ~20h after Day 2)
#        - recovery      (Smart Recovery Journey: still stuck well past the
#          normal nudge window - one last "still waiting" message before the
#          7-day Welcome window lapses)
#      All four stages share the same anti-abuse gate
#      (_welcome_reminder_anti_abuse_blocked), are personalized with the
#      user's first name when available (vouchers.resolve_welcome_display_name)
#      with a graceful generic fallback, are localized via
#      vouchers.resolve_welcome_locale, and use lightweight adaptive timing
#      (_preferred_send_hour_kl) to land close to the user's usual check-in
#      hour instead of firing the instant the elapsed-time threshold is hit.
#
# Both jobs write to `welcome_analytics_events` via vouchers.log_welcome_event
# so the funnel dashboard (dashboard_panels.build_welcome_journey_panel) can
# report reminder volume without a separate tracking system.
# ---------------------------------------------------------------------------

WELCOME_REMINDER_AFTER_HOURS = int(os.getenv("WELCOME_REMINDER_AFTER_HOURS", "12"))
WELCOME_FINAL_WARNING_HOURS = int(os.getenv("WELCOME_FINAL_WARNING_HOURS", "36"))
WELCOME_EXPIRY_HOURS = int(os.getenv("WELCOME_EXPIRY_HOURS", "48"))
WELCOME_REMINDER_BATCH_LIMIT = int(os.getenv("WELCOME_REMINDER_BATCH_LIMIT", "200"))
WELCOME_REMINDER_LINK = os.getenv("WELCOME_REMINDER_LINK", "https://apreferralv1.fly.dev/miniapp")


def _welcome_reminder_link() -> str:
    try:
        from settings_service import get_setting as _gs
        link = _gs("urls", "miniapp_url")
        if link:
            return link
    except Exception:
        pass
    return WELCOME_REMINDER_LINK


def _welcome_reminder_markup() -> dict:
    """Raw Telegram Bot API ``reply_markup`` JSON for the Mini-App check-in
    button, used by the plain-HTTP send path so the button survives even
    when the live-bot (python-telegram-bot) send path is unavailable."""
    return {
        "inline_keyboard": [[
            {"text": "🎁 Open Mini-App", "web_app": {"url": _welcome_reminder_link()}},
        ]]
    }


def _welcome_http_send_fn(uid: int, text: str) -> tuple[bool, str | None, bool]:
    """Default HTTP fallback for Welcome reminders — includes the Mini-App
    button so a failed/absent live-bot send does not silently strip it."""
    return send_telegram_http_message(uid, text, reply_markup=_welcome_reminder_markup())


def _welcome_reminder_text(*, final_warning: bool) -> str:
    link = _welcome_reminder_link()
    if final_warning:
        try:
            from settings_service import get_setting as _gs
            template = _gs("message_templates", "day3_reminder")
        except Exception:
            template = None
        template = template or (
            "⏳ Last chance!\n\n"
            "Your AdvantPlay Welcome Voucher is about to expire — don't miss out.\n"
            "{link}"
        )
        return template.format(link=link)
    try:
        from settings_service import get_setting as _gs
        template = _gs("message_templates", "checkin_reminder")
    except Exception:
        template = None
    template = template or (
        "🎁 Your AdvantPlay Welcome Voucher is waiting.\n\n"
        "Finish your check-ins to claim it before it expires.\n"
        "{link}"
    )
    return template.format(link=link)


def process_welcome_voucher_lifecycle(*, now_ref: datetime | None = None, batch_limit: int | None = None, db_ref=None, send_fn=None, bot_send_fn=None) -> dict:
    """Hourly/30-min sweep for the legacy Welcome eligibility window.

    ``bot_send_fn(uid, text) -> bool`` is an optional hook (mirrors
    ``process_welcome_reminders``) used to deliver the reminder with a
    Mini-App button via the live bot. When it is absent, raises, or returns a
    falsy result, the reminder falls back to plain-text ``send_fn`` (HTTP).
    """
    db_ref = db_ref or db
    send_fn = send_fn or _welcome_http_send_fn
    now_ts = _coerce_utc(now_ref) or now_utc()
    limit = int(batch_limit or WELCOME_REMINDER_BATCH_LIMIT)
    expiry_hours = int(_journey_setting("welcome_window_hours", WELCOME_EXPIRY_HOURS))
    final_warning_hours = int(_journey_setting("final_reminder_hours", WELCOME_FINAL_WARNING_HOURS))
    reminder_after_hours = int(_journey_setting("reminder_after_hours", WELCOME_REMINDER_AFTER_HOURS))
    scanned = reminder_sent = final_warning_sent = expired = send_failed = 0

    cursor = db_ref.welcome_eligibility.find(
        {"claimed": {"$ne": True}, "expired_at": {"$exists": False}},
        {"_id": 1, "uid": 1, "user_id": 1, "first_seen_at": 1, "eligible_until": 1, "claimed": 1, "claimed_at": 1, "lifecycle_state": 1, "reminder_sent_at": 1, "final_warning_sent_at": 1, "expired_at": 1},
    ).limit(limit)

    for doc in cursor:
        scanned += 1
        uid = doc.get("uid") or doc.get("user_id")
        if not uid:
            continue
        if doc.get("claimed") or doc.get("claimed_at"):
            db_ref.welcome_eligibility.update_one({"_id": doc["_id"], "claimed": {"$ne": True}}, {"$set": {"claimed": True, "lifecycle_state": "claimed", "updated_at": now_ts}})
            continue

        first_seen = _coerce_utc(doc.get("first_seen_at"))
        eligible_until = _coerce_utc(doc.get("eligible_until"))
        created_ref = first_seen or (eligible_until - timedelta(hours=expiry_hours) if eligible_until else None)
        if not created_ref:
            continue
        expiry_at = eligible_until or (created_ref + timedelta(hours=expiry_hours))

        if now_ts >= expiry_at:
            claim_doc = db_ref.new_joiner_claims.find_one({"uid": int(uid)}, {"_id": 1})
            if claim_doc:
                db_ref.welcome_eligibility.update_one({"_id": doc["_id"]}, {"$set": {"claimed": True, "lifecycle_state": "claimed", "updated_at": now_ts}})
                continue
            res = db_ref.welcome_eligibility.update_one(
                {"_id": doc["_id"], "expired_at": {"$exists": False}, "claimed": {"$ne": True}},
                {"$set": {"expired_at": now_ts, "lifecycle_state": "expired", "updated_at": now_ts}},
            )
            if res.modified_count:
                expired += 1
                logger.info("[WELCOME_LIFECYCLE] welcome_expired uid=%s", uid)
                try:
                    from vouchers import log_welcome_event

                    log_welcome_event("welcome_abandoned", uid, {"reason": "window_expired"}, now=now_ts)
                except Exception:
                    logger.warning("[WELCOME_LIFECYCLE] failed to log welcome_abandoned uid=%s", uid)
            continue

        claim_doc = db_ref.new_joiner_claims.find_one({"uid": int(uid)}, {"_id": 1})
        if claim_doc:
            db_ref.welcome_eligibility.update_one({"_id": doc["_id"]}, {"$set": {"claimed": True, "lifecycle_state": "claimed", "updated_at": now_ts}})
            continue

        age_hours = (now_ts - created_ref).total_seconds() / 3600.0
        needs_final = age_hours >= final_warning_hours and not doc.get("final_warning_sent_at")
        needs_reminder = age_hours >= reminder_after_hours and not doc.get("reminder_sent_at") and not needs_final

        if not (needs_reminder or needs_final):
            continue

        # Ownership handoff: once a user has completed at least one check-in,
        # process_welcome_reminders (V2) owns their reminders end-to-end. Skip
        # here to avoid sending a duplicate/generic nudge on top of the V2
        # check-in-aware copy. This is the only change needed to consolidate
        # the two pipelines into a single source of truth per user.
        try:
            owned_by_v2 = bool(db_ref.welcome_reminders.find_one({"user_id": int(uid)}, {"_id": 1}))
        except Exception:  # noqa: BLE001
            owned_by_v2 = False
        if owned_by_v2:
            continue

        text = _welcome_reminder_text(final_warning=needs_final)
        ok, err = False, None
        if bot_send_fn is not None:
            try:
                ok = bool(bot_send_fn(int(uid), text))
            except Exception as exc:  # noqa: BLE001
                logger.warning("[WELCOME_LIFECYCLE] bot_send_failed uid=%s err=%s", uid, exc)
                ok = False
        if not ok:
            ok, err, _blocked = send_fn(int(uid), text)
        if not ok:
            send_failed += 1
            logger.warning("[WELCOME_LIFECYCLE] send_failed uid=%s final_warning=%s err=%s", uid, needs_final, err)
            continue

        if needs_final:
            res = db_ref.welcome_eligibility.update_one(
                {"_id": doc["_id"], "final_warning_sent_at": {"$exists": False}, "claimed": {"$ne": True}, "expired_at": {"$exists": False}},
                {"$set": {"final_warning_sent_at": now_ts, "lifecycle_state": "final_warning", "updated_at": now_ts}},
            )
            if res.modified_count:
                final_warning_sent += 1
                logger.info("[WELCOME_LIFECYCLE] final_warning_sent uid=%s", uid)
        else:
            res = db_ref.welcome_eligibility.update_one(
                {"_id": doc["_id"], "reminder_sent_at": {"$exists": False}, "claimed": {"$ne": True}, "expired_at": {"$exists": False}},
                {"$set": {"reminder_sent_at": now_ts, "lifecycle_state": "reminded", "updated_at": now_ts}},
            )
            if res.modified_count:
                reminder_sent += 1
                logger.info("[WELCOME_LIFECYCLE] reminder_sent uid=%s", uid)

    return {"scanned": scanned, "reminder_sent": reminder_sent, "final_warning_sent": final_warning_sent, "expired": expired, "send_failed": send_failed}


WELCOME_PROGRESS_REMINDER_BATCH_LIMIT = int(os.getenv("WELCOME_PROGRESS_REMINDER_BATCH_LIMIT", "500"))
WELCOME_RECOVERY_AFTER_HOURS = int(os.getenv("WELCOME_RECOVERY_AFTER_HOURS", "48"))
WELCOME_ADAPTIVE_MAX_DELAY_HOURS = int(os.getenv("WELCOME_ADAPTIVE_MAX_DELAY_HOURS", "6"))
WELCOME_ADAPTIVE_MIN_HISTORY = int(os.getenv("WELCOME_ADAPTIVE_MIN_HISTORY", "3"))

# Personalized, localized Welcome reminder copy (Phase 2). ``{greeting}`` is
# rendered as "Hi <first name> \U0001F44B\n\n" when a first name is on file,
# and collapses to "" otherwise (graceful fallback - see
# _welcome_reminder_greeting). Locale falls back to "en" whenever no
# recognized language field is present on the user doc (see
# vouchers.resolve_welcome_locale) - today that is effectively everyone, since
# no write path populates a language field yet, but the templates and
# selection logic are ready for when one does.
_WELCOME_REMINDER_TEMPLATES = {
    "en": {
        # Stage 1 (friendly): a gentle nudge shortly after Day 1.
        "day1_20h": (
            "{greeting}Great job completing Day 1!\n\n"
            "Only TWO more check-ins remain.\n\n"
            "Your Welcome Voucher is waiting."
        ),
        # Stage 2 (more urgency): still stuck on 1/3, reward window closing in.
        "day1_28h": (
            "⚠️ Don't lose your Welcome Voucher\n\n"
            "🟩⬜⬜ 1/3\n\n"
            "Your Day 2 check-in is still waiting — complete it now to stay eligible.\n\n"
            "Your reward expires 7 days after joining."
        ),
        # Stage 3 (high excitement): one check-in away from unlocking.
        "day2_20h": (
            "🔥 You're almost there!\n\n"
            "Just ONE more check-in unlocks your FREE Welcome Voucher.\n\n"
            "Don't stop now."
        ),
        # Stage 4 (Smart Recovery Journey): well past the normal nudge window.
        "recovery": (
            "{greeting}Still waiting 👀\n\n"
            "You're only one step away from unlocking your reward."
        ),
    },
    "th": {
        "day1_20h": (
            "{greeting}เยี่ยมมากที่เช็คอินวันที่ 1 สำเร็จ!\n\n"
            "เหลืออีกแค่ 2 ครั้งเท่านั้น\n\n"
            "บัตรกำนัลต้อนรับของคุณรออยู่แล้ว"
        ),
        "day1_28h": (
            "⚠️ อย่าพลาดบัตรกำนัลต้อนรับ\n\n"
            "🟩⬜⬜ 1/3\n\n"
            "การเช็คอินวันที่ 2 ของคุณยังรออยู่ — ทำตอนนี้เพื่อรักษาสิทธิ์\n\n"
            "รางวัลของคุณหมดอายุภายใน 7 วันหลังจากเข้าร่วม"
        ),
        "day2_20h": (
            "🔥 ใกล้จะสำเร็จแล้ว!\n\n"
            "อีกแค่ 1 ครั้งเท่านั้นก็จะได้บัตรกำนัลต้อนรับฟรี\n\n"
            "อย่าเพิ่งหยุดตอนนี้"
        ),
        "recovery": (
            "{greeting}ยังรออยู่นะ 👀\n\n"
            "คุณเหลืออีกแค่ก้าวเดียวก็จะปลดล็อกรางวัลได้แล้ว"
        ),
    },
    "id": {
        "day1_20h": (
            "{greeting}Kerja bagus menyelesaikan Hari 1!\n\n"
            "Tinggal DUA check-in lagi.\n\n"
            "Voucher Selamat Datang kamu sudah menunggu."
        ),
        "day1_28h": (
            "⚠️ Jangan lewatkan Voucher Selamat Datang kamu\n\n"
            "🟩⬜⬜ 1/3\n\n"
            "Check-in Hari 2 kamu masih menunggu — selesaikan sekarang agar tetap memenuhi syarat.\n\n"
            "Hadiah kamu berakhir 7 hari setelah bergabung."
        ),
        "day2_20h": (
            "🔥 Sedikit lagi!\n\n"
            "Tinggal SATU check-in lagi untuk membuka Voucher Selamat Datang GRATIS kamu.\n\n"
            "Jangan berhenti sekarang."
        ),
        "recovery": (
            "{greeting}Masih menunggu 👀\n\n"
            "Kamu tinggal satu langkah lagi untuk membuka hadiahmu."
        ),
    },
}


def _welcome_reminder_greeting(display_name: str | None) -> str:
    return f"Hi {display_name} 👋\n\n" if display_name else ""


def _render_welcome_reminder(stage: str, *, display_name: str | None, locale: str = "en") -> str:
    templates = _WELCOME_REMINDER_TEMPLATES.get(locale) or _WELCOME_REMINDER_TEMPLATES["en"]
    template = templates.get(stage) or _WELCOME_REMINDER_TEMPLATES["en"][stage]
    return template.format(greeting=_welcome_reminder_greeting(display_name))


# Backward-compatible module-level aliases: the generic (no first name, "en")
# rendering of each stage - kept so existing callers/tests that reference
# these constants directly keep working. process_welcome_reminders below
# renders the personalized/localized text per-user instead of using these.
_WELCOME_PROGRESS_REMINDER_20H = _render_welcome_reminder("day1_20h", display_name=None, locale="en")
_WELCOME_PROGRESS_REMINDER_28H = _render_welcome_reminder("day1_28h", display_name=None, locale="en")
_WELCOME_PROGRESS_REMINDER_DAY2 = _render_welcome_reminder("day2_20h", display_name=None, locale="en")
_WELCOME_PROGRESS_REMINDER_RECOVERY = _render_welcome_reminder("recovery", display_name=None, locale="en")


def _preferred_send_hour_kl(uid: int, *, db_ref) -> int | None:
    """Lightweight heuristic: the user's most common local check-in hour.

    Reuses the existing ``xp_events`` check-in ledger (no new tracking, no
    ML) - looks at the last 14 check-ins and returns the most frequent local
    hour. Returns ``None`` when there isn't enough history yet, in which case
    callers must fall back to the fixed elapsed-time schedule.
    """
    try:
        docs = list(
            db_ref.xp_events.find({"user_id": uid, "type": "checkin"}, {"created_at": 1})
            .sort("created_at", -1)
            .limit(14)
        )
    except Exception:  # noqa: BLE001
        return None
    hours = []
    for entry in docs:
        ts = entry.get("created_at")
        if not isinstance(ts, datetime):
            continue
        aware = ts if ts.tzinfo else KL_TZ.localize(ts)
        hours.append(aware.astimezone(KL_TZ).hour)
    if len(hours) < WELCOME_ADAPTIVE_MIN_HISTORY:
        return None
    return Counter(hours).most_common(1)[0][0]


def _welcome_adaptive_send_ready(uid: int, *, db_ref, now_ts: datetime, elapsed: timedelta, threshold_hours: int) -> bool:
    """Decide whether *now* is a good time to send, once the elapsed-time gate is met.

    Sends immediately when there isn't enough check-in history (fixed
    schedule, unchanged behaviour). Otherwise waits for the hour window
    ending at the user's usual check-in hour (e.g. usual check-in ~8PM ->
    send between 7-8PM), bounded by ``WELCOME_ADAPTIVE_MAX_DELAY_HOURS`` so a
    reminder is never silently skipped - this keeps volume flat (still one
    send per stage) while nudging delivery closer to when the user is
    actually likely to check in.
    """
    preferred_hour = _preferred_send_hour_kl(uid, db_ref=db_ref)
    if preferred_hour is None:
        return True
    if elapsed >= timedelta(hours=threshold_hours + WELCOME_ADAPTIVE_MAX_DELAY_HOURS):
        return True
    now_hour_kl = now_ts.astimezone(KL_TZ).hour
    return now_hour_kl in (preferred_hour, (preferred_hour - 1) % 24)


def _send_welcome_reminder(uid: int, text: str, *, send_fn, bot_send_fn, stage: str) -> tuple[bool, str | None]:
    """Send a Welcome reminder, preferring the Mini-App-button bot path.

    Falls back to plain-text ``send_fn`` (HTTP) when ``bot_send_fn`` is
    absent, raises, or returns a falsy result.
    """
    if bot_send_fn is not None:
        try:
            if bool(bot_send_fn(uid, text)):
                return True, None
        except Exception as exc:  # noqa: BLE001
            logger.warning("[WELCOME_PROGRESS_REMINDER] bot_send_failed uid=%s stage=%s err=%s", uid, stage, exc)
    ok, err, _blocked = send_fn(uid, text)
    return ok, err


def _welcome_reminder_anti_abuse_blocked(uid: int, *, db_ref, progress: dict) -> str | None:
    """Return a reason code if reminders must be suppressed for this user, else None."""
    if progress.get("claimed"):
        return "welcome_claimed"
    if progress.get("expired"):
        return "welcome_expired"
    user_doc = db_ref.users.find_one(
        {"user_id": uid},
        {"pm_blocked": 1, "claim_risk_level": 1, "multi_account_flag": 1, "welcome_abuse_flag": 1, "left_channel": 1},
    ) or {}
    if user_doc.get("pm_blocked"):
        return "telegram_blocked"
    risk_level = str(user_doc.get("claim_risk_level") or "").lower()
    if risk_level in ("high", "blocked", "risk_blocked"):
        return "risk_blocked"
    if user_doc.get("multi_account_flag"):
        return "multi_account"
    if user_doc.get("welcome_abuse_flag"):
        return "welcome_abuse"
    if user_doc.get("left_channel"):
        return "user_left_channel"
    return None


# Internal stage code -> normalized stage identifier persisted on analytics
# events. Only "day2" differs (the D3/final reminder, keyed internally off
# ``day2_at`` since it fires 20h after the Day-2 check-in).
_STAGE_NORMALIZED = {"20h": "20h", "28h": "28h", "day2": "day3", "recovery": "recovery"}


def _welcome_reminder_candidate_stages(*, completed: int, day1_at, day2_at, doc: dict, now_ts: datetime) -> list[str]:
    """Which reminder stage(s) this user is currently eligible for, ignoring
    the anti-abuse check. Used only to attribute a skip event to the right
    stage(s) — never to decide whether to actually send."""
    stages: list[str] = []
    if (
        completed == 1 and day1_at and not doc.get("reminder_20h_sent")
        and (now_ts - day1_at) >= timedelta(hours=20)
    ):
        stages.append("20h")
    if (
        completed == 1 and day1_at and not doc.get("reminder_28h_sent")
        and (now_ts - day1_at) >= timedelta(hours=28)
    ):
        stages.append("28h")
    if (
        completed == 2 and day2_at and not doc.get("day2_reminder_sent")
        and (now_ts - day2_at) >= timedelta(hours=20)
    ):
        stages.append("day2")
    if not doc.get("recovery_sent"):
        recovery_anchor = None
        if completed == 1 and day1_at and doc.get("reminder_28h_sent"):
            recovery_anchor = day1_at
        elif completed == 2 and day2_at and doc.get("day2_reminder_sent"):
            recovery_anchor = day2_at
        if recovery_anchor and (now_ts - recovery_anchor) >= timedelta(hours=WELCOME_RECOVERY_AFTER_HOURS):
            stages.append("recovery")
    return stages


def process_welcome_reminders(*, now_ref: datetime | None = None, batch_limit: int | None = None, db_ref=None, send_fn=None, bot_send_fn=None) -> dict:
    """Hourly reminder job for the Welcome Voucher Progress journey (V2).

    Sends four check-in nudges, each at most once (state tracked on the
    ``welcome_reminders`` collection):
      - 20h after Day 1 (stuck on 1/3)
      - 28h after Day 1 (still stuck on 1/3, more urgency)
      - 20h after Day 2 (stuck on 2/3)
      - Smart Recovery: well past the above (``WELCOME_RECOVERY_AFTER_HOURS``,
        default 48h since the last relevant check-in), one final nudge before
        the 7-day Welcome window lapses.

    Each stage is personalized with the user's first name when available
    (falls back to generic copy otherwise), localized via
    ``vouchers.resolve_welcome_locale``, and adaptively timed to land close to
    the user's usual check-in hour (``_welcome_adaptive_send_ready`` - falls
    back to sending as soon as the elapsed-time threshold is hit when there
    isn't enough check-in history).

    ``bot_send_fn(uid, text) -> bool`` is an optional hook used only for the
    Day 2 and recovery reminders so they can include a Mini-App button via the
    live bot (InlineKeyboardButton/WebAppInfo). When it is absent, raises, or
    returns a falsy result, the reminder falls back to plain-text ``send_fn``
    (HTTP).
    """
    import uuid

    from vouchers import get_welcome_progress, log_welcome_event, resolve_welcome_display_name, resolve_welcome_locale

    db_ref = db_ref or db
    send_fn = send_fn or _welcome_http_send_fn
    now_ts = _coerce_utc(now_ref) or now_utc()
    limit = int(batch_limit or WELCOME_PROGRESS_REMINDER_BATCH_LIMIT)
    run_id = uuid.uuid4().hex

    scanned = reminder_20h_sent = reminder_28h_sent = day2_reminder_sent = recovery_sent = skipped_abuse = send_failed = 0
    eligible_20h = eligible_28h = eligible_day3 = eligible_recovery = 0
    failed_count = 0
    failed_users: list[dict] = []
    skip_breakdown = {
        "already_claimed": 0,
        "expired": 0,
        "bot_blocked": 0,
        "risk_blocked": 0,
        "multi_account": 0,
        "welcome_abuse": 0,
        "left_channel": 0,
        "missing_data": 0,
    }
    _SKIP_REASON_BUCKET = {
        "welcome_claimed": "already_claimed",
        "welcome_expired": "expired",
        "telegram_blocked": "bot_blocked",
        "risk_blocked": "risk_blocked",
        "multi_account": "multi_account",
        "welcome_abuse": "welcome_abuse",
        "user_left_channel": "left_channel",
    }

    cursor = db_ref.welcome_reminders.find(
        {
            "$or": [
                {"reminder_20h_sent": {"$ne": True}},
                {"reminder_28h_sent": {"$ne": True}},
                {"day2_reminder_sent": {"$ne": True}},
                {"recovery_sent": {"$ne": True}},
            ]
        }
    ).limit(limit)

    for doc in cursor:
        scanned += 1
        uid = doc.get("user_id")
        if not uid:
            skip_breakdown["missing_data"] += 1
            continue
        uid = int(uid)
        current_stage = None

        # Per-user isolation: a single user's malformed data or a transient
        # failure in progress lookup / send / bookkeeping must not abort the
        # rest of the hourly batch (and, with it, the run-wrapper's stats
        # write that keeps the dashboard heartbeat alive). Errors raised
        # while iterating ``cursor`` itself (e.g. a lost DB connection) are
        # NOT caught here and are left to propagate, since those are global
        # failures the run should surface rather than silently continue.
        try:
            progress = get_welcome_progress(uid, now=now_ts)
            completed = int(progress.get("completed") or 0)
            day1_at = _coerce_utc(doc.get("day1_at"))
            day2_at = _coerce_utc(doc.get("day2_at"))

            blocked_reason = _welcome_reminder_anti_abuse_blocked(uid, db_ref=db_ref, progress=progress)
            if blocked_reason:
                skipped_abuse += 1
                skip_breakdown[_SKIP_REASON_BUCKET.get(blocked_reason, "missing_data")] += 1
                logger.info("[WELCOME_PROGRESS_REMINDER] skip uid=%s reason=%s", uid, blocked_reason)
                candidate_stages = _welcome_reminder_candidate_stages(
                    completed=completed, day1_at=day1_at, day2_at=day2_at, doc=doc, now_ts=now_ts,
                )
                for internal_stage in (candidate_stages or [None]):
                    normalized_stage = _STAGE_NORMALIZED.get(internal_stage) if internal_stage else None
                    log_welcome_event(
                        "welcome_reminder_skipped", uid, {"reason": blocked_reason},
                        stage=normalized_stage, status="skipped", reason=blocked_reason,
                        run_id=run_id, dedupe=True, now=now_ts,
                    )
                continue

            user_doc = db_ref.users.find_one(
                {"user_id": uid}, {"first_name": 1, "display_name": 1, "name": 1, "username": 1, "language_code": 1, "locale": 1, "lang": 1}
            ) or {}
            display_name = resolve_welcome_display_name(uid, user_doc=user_doc)
            locale = resolve_welcome_locale(user_doc)

            # Reminder #1: completed == 1, ~20h elapsed since Day 1, not yet sent.
            if completed == 1 and day1_at and not doc.get("reminder_20h_sent"):
                elapsed = now_ts - day1_at
                if elapsed >= timedelta(hours=20):
                    current_stage = "20h"
                    eligible_20h += 1
                    if _welcome_adaptive_send_ready(uid, db_ref=db_ref, now_ts=now_ts, elapsed=elapsed, threshold_hours=20):
                        text = _render_welcome_reminder("day1_20h", display_name=display_name, locale=locale)
                        ok, err = _send_welcome_reminder(uid, text, send_fn=send_fn, bot_send_fn=bot_send_fn, stage="20h")
                        if ok:
                            db_ref.welcome_reminders.update_one({"_id": doc["_id"]}, {"$set": {"reminder_20h_sent": True, "updated_at": now_ts}})
                            log_welcome_event("welcome_reminder_20h_sent", uid, stage="20h", status="sent", run_id=run_id, now=now_ts)
                            reminder_20h_sent += 1
                        else:
                            send_failed += 1
                            logger.warning("[WELCOME_PROGRESS_REMINDER] send_failed uid=%s stage=20h err=%s", uid, err)
                            log_welcome_event(
                                "welcome_reminder_failed", uid, {"err": str(err)},
                                stage="20h", status="failed", reason=str(err), run_id=run_id, now=now_ts,
                            )

            # Reminder #2: completed == 1, ~28h elapsed since Day 1, still stuck, not yet sent.
            if completed == 1 and day1_at and not doc.get("reminder_28h_sent"):
                elapsed = now_ts - day1_at
                if elapsed >= timedelta(hours=28):
                    current_stage = "28h"
                    eligible_28h += 1
                    if _welcome_adaptive_send_ready(uid, db_ref=db_ref, now_ts=now_ts, elapsed=elapsed, threshold_hours=28):
                        text = _render_welcome_reminder("day1_28h", display_name=display_name, locale=locale)
                        ok, err = _send_welcome_reminder(uid, text, send_fn=send_fn, bot_send_fn=bot_send_fn, stage="28h")
                        if ok:
                            db_ref.welcome_reminders.update_one({"_id": doc["_id"]}, {"$set": {"reminder_28h_sent": True, "updated_at": now_ts}})
                            log_welcome_event("welcome_reminder_28h_sent", uid, stage="28h", status="sent", run_id=run_id, now=now_ts)
                            reminder_28h_sent += 1
                        else:
                            send_failed += 1
                            logger.warning("[WELCOME_PROGRESS_REMINDER] send_failed uid=%s stage=28h err=%s", uid, err)
                            log_welcome_event(
                                "welcome_reminder_failed", uid, {"err": str(err)},
                                stage="28h", status="failed", reason=str(err), run_id=run_id, now=now_ts,
                            )

            # Reminder #3: completed == 2, ~20h elapsed since Day 2, not yet sent.
            if completed == 2 and day2_at and not doc.get("day2_reminder_sent"):
                elapsed = now_ts - day2_at
                if elapsed >= timedelta(hours=20):
                    current_stage = "day2"
                    eligible_day3 += 1
                    if _welcome_adaptive_send_ready(uid, db_ref=db_ref, now_ts=now_ts, elapsed=elapsed, threshold_hours=20):
                        text = _render_welcome_reminder("day2_20h", display_name=display_name, locale=locale)
                        ok, err = _send_welcome_reminder(uid, text, send_fn=send_fn, bot_send_fn=bot_send_fn, stage="day2")
                        if ok:
                            db_ref.welcome_reminders.update_one({"_id": doc["_id"]}, {"$set": {"day2_reminder_sent": True, "updated_at": now_ts}})
                            log_welcome_event("welcome_reminder_day2_sent", uid, stage="day3", status="sent", run_id=run_id, now=now_ts)
                            day2_reminder_sent += 1
                        else:
                            send_failed += 1
                            logger.warning("[WELCOME_PROGRESS_REMINDER] send_failed uid=%s stage=day2 err=%s", uid, err)
                            log_welcome_event(
                                "welcome_reminder_failed", uid, {"err": str(err)},
                                stage="day3", status="failed", reason=str(err), run_id=run_id, now=now_ts,
                            )

            # Reminder #4 (Smart Recovery Journey): user stalled on Day 1 or Day 2
            # well past the normal nudge window - one last "still waiting" message,
            # gated on the earlier stage reminder already having fired so this
            # never overtakes/duplicates reminders #1-3. Recovery timing is
            # anchored on the last relevant check-in, so it can never fire
            # after the user has already unlocked/claimed (both cases are
            # already filtered out above by the anti-abuse gate) and never
            # exceeds the 7-day Welcome window since the anchor timestamps
            # themselves are bounded by that window.
            if not doc.get("recovery_sent"):
                recovery_anchor = None
                if completed == 1 and day1_at and doc.get("reminder_28h_sent"):
                    recovery_anchor = day1_at
                elif completed == 2 and day2_at and doc.get("day2_reminder_sent"):
                    recovery_anchor = day2_at
                if recovery_anchor and (now_ts - recovery_anchor) >= timedelta(hours=WELCOME_RECOVERY_AFTER_HOURS):
                    current_stage = "recovery"
                    eligible_recovery += 1
                    text = _render_welcome_reminder("recovery", display_name=display_name, locale=locale)
                    ok, err = _send_welcome_reminder(uid, text, send_fn=send_fn, bot_send_fn=bot_send_fn, stage="recovery")
                    if ok:
                        db_ref.welcome_reminders.update_one({"_id": doc["_id"]}, {"$set": {"recovery_sent": True, "updated_at": now_ts}})
                        log_welcome_event("welcome_recovery_sent", uid, stage="recovery", status="sent", run_id=run_id, now=now_ts)
                        recovery_sent += 1
                    else:
                        send_failed += 1
                        logger.warning("[WELCOME_PROGRESS_REMINDER] send_failed uid=%s stage=recovery err=%s", uid, err)
                        log_welcome_event(
                            "welcome_reminder_failed", uid, {"err": str(err)},
                            stage="recovery", status="failed", reason=str(err), run_id=run_id, now=now_ts,
                        )
        except Exception as exc:  # noqa: BLE001 - per-user isolation, see docstring above
            failed_count += 1
            failed_users.append({
                "user_id": uid,
                "stage": current_stage,
                "run_id": run_id,
                "error": f"{type(exc).__name__}: {exc}",
            })
            logger.exception(
                "[WELCOME_PROGRESS_REMINDER] user_processing_failed uid=%s stage=%s run_id=%s",
                uid, current_stage, run_id,
            )
            log_welcome_event(
                "welcome_reminder_failed", uid, {"err": f"{type(exc).__name__}: {exc}"},
                stage=_STAGE_NORMALIZED.get(current_stage) if current_stage else None,
                status="failed", reason="exception", run_id=run_id, now=now_ts,
            )
            # Nothing on this user was marked sent, so the next hourly run
            # will pick them back up from the same query — retryable.
            continue

    blocked_users = skip_breakdown["bot_blocked"] + skip_breakdown["risk_blocked"]

    return {
        "run_id": run_id,
        "scanned": scanned,
        "eligible_20h": eligible_20h,
        "eligible_28h": eligible_28h,
        "eligible_day3": eligible_day3,
        "eligible_recovery": eligible_recovery,
        "reminder_20h_sent": reminder_20h_sent,
        "reminder_28h_sent": reminder_28h_sent,
        "day2_reminder_sent": day2_reminder_sent,
        "recovery_sent": recovery_sent,
        "skipped_abuse": skipped_abuse,
        "skip_breakdown": skip_breakdown,
        "blocked_users": blocked_users,
        "send_failed": send_failed,
        "failed_count": failed_count,
        "failed_users": failed_users,
        "status": "partial_failure" if failed_count else "ok",
    }

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
# GROUP_ID / OFFICIAL_CHANNEL_ID are resolved once in referral_destination.py so
# main.py and scheduler.py can never disagree on chat identity.
from referral_destination import (
    COMMUNITY_GROUP_ID as GROUP_ID,
    OFFICIAL_CHANNEL_ID,
    COMMUNITY_GROUP,
    OFFICIAL_CHANNEL,
    VALID_DESTINATION_TYPES,
)
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
REFERRAL_HOLD_HOURS = int(os.getenv("REFERRAL_QUALIFY_HOURS", "48"))


def _referral_hold_hours() -> int:
    return int(_referral_setting("qualify_hold_hours", REFERRAL_HOLD_HOURS))

AFFILIATE_CONGRATS_CHANNEL_ID = int(os.getenv("AFFILIATE_CONGRATS_CHANNEL_ID", "-1003820861717"))
REFERRAL_CONGRATS_TIERS = [
    (10, 10),
    (25, 15),
    (50, 50),
    (150, 125),
    (250, 250),
]
REFERRAL_CHANNEL_RETRY_HOURS = 12
REFERRAL_CHANNEL_EXPIRE_DAYS = 7
INVITEE_SUB_AUDIT_ENABLED = os.getenv("INVITEE_SUB_AUDIT_ENABLED", "1") == "1"
MAX_INVITEE_SUB_CHECKS_PER_RUN = int(os.getenv("MAX_INVITEE_SUB_CHECKS_PER_RUN", "800"))
SUB_CACHE_TTL_DAYS = int(os.getenv("SUB_CACHE_TTL_DAYS", "14"))
RECENT_CHECK_SKIP_HOURS = int(os.getenv("RECENT_CHECK_SKIP_HOURS", "6"))
TG_GETCHATMEMBER_TIMEOUT_SEC = int(os.getenv("TG_GETCHATMEMBER_TIMEOUT_SEC", "5"))
TG_REQUEST_SLEEP_MS = int(os.getenv("TG_REQUEST_SLEEP_MS", "80"))

KL_TZ = pytz.timezone("Asia/Kuala_Lumpur")

logger = logging.getLogger(__name__)
INSTANCE_ID = os.getenv("FLY_ALLOC_ID") or f"{socket.gethostname()}:{os.getpid()}"
PROCESSING_TIMEOUT = timedelta(minutes=10)
RETRY_RELEASE_DELAY = timedelta(minutes=2)
# SNAPSHOT FIELDS — ONLY WRITTEN BY WORKER
# weekly_xp, monthly_xp, total_xp, weekly_referrals, monthly_referrals, total_referrals, vip_tier, vip_month
# DEPRECATED — DO NOT USE (ledger-based referrals only)
# weekly_referral_count, total_referral_count, ref_count_total, monthly_referral_count

class ReferralRetryableError(RuntimeError):
    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class ReferralTelegramError(RuntimeError):
    """A parsed, non-2xx/non-429/non-5xx getChatMember response that is
    NOT a definitive membership verdict (definitive verdicts — member,
    left, kicked, restricted — are returned as a normal status string, not
    raised).

    ``kind`` is "config" for a permanent configuration/permission problem
    (wrong chat id, bot not an admin/not a member of the chat, bad/expired
    bot token) that will fail identically for every invitee until an
    operator fixes it, "malformed" for a non-JSON/unparseable body, or
    "user" for a response that is specific to the target invitee (e.g. an
    unknown/invalid user id). All three are operational uncertainty, not
    proof of anything about the invitee's membership — callers must never
    treat any of them as a confirmed negative verdict.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: int | None = None,
        description: str | None = None,
        kind: str = "user",
    ):
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.description = description
        self.kind = kind


# Substrings (lowercased) of Telegram's getChatMember error descriptions
# that indicate a permanent destination/permission misconfiguration rather
# than anything specific to the invitee being checked.
_TELEGRAM_CONFIG_ERROR_MARKERS = (
    "chat not found",
    "not enough rights",
    "have no rights",
    "chat_admin_required",
    "member list is inaccessible",
    "bot is not a member",
    "kicked from",
    "bot was kicked",
    "bot was blocked",
    "group chat was upgraded",
    "unauthorized",
    "forbidden",
)

# HTTP status codes that are always a configuration/permission problem
# (bad/expired bot token, bot forbidden from the chat) regardless of what
# Telegram's description text says — these must never fall through to the
# generic "user-specific" bucket, which would eventually terminate as a
# per-invitee failure for what is actually a global outage.
_TELEGRAM_CONFIG_ERROR_STATUS_CODES = (401, 403)


def _classify_telegram_getchatmember_error(status_code: int | None, description: str | None) -> str:
    if status_code in _TELEGRAM_CONFIG_ERROR_STATUS_CODES:
        return "config"
    text = (description or "").lower()
    for marker in _TELEGRAM_CONFIG_ERROR_MARKERS:
        if marker in text:
            return "config"
    return "user"


# Both "config" (permanent, affects every invitee) and "user" (ambiguous,
# invitee-specific) 400s are operational uncertainty, not a confirmed
# negative membership verdict — neither may ever revoke a referral. Both
# retry with bounded attempts and then land on the same terminal
# status="error" operational state; only the retry budget/backoff differs
# (config errors get a longer backoff since retrying sooner cannot help).
MAX_TELEGRAM_CONFIG_RETRIES = int(os.getenv("MAX_TELEGRAM_CONFIG_RETRIES", "5"))
MAX_TELEGRAM_USER_RETRIES = int(os.getenv("MAX_TELEGRAM_USER_RETRIES", "3"))
TELEGRAM_CONFIG_ERROR_BACKOFF_SEC = int(os.getenv("TELEGRAM_CONFIG_ERROR_BACKOFF_SEC", "1800"))


def _get_chat_member_status(user_id: int) -> str | None:
    if not BOT_TOKEN:
        raise RuntimeError("missing_bot_token")
    resp = requests.get(
        f"{API_BASE}/getChatMember",
        params={"chat_id": GROUP_ID, "user_id": user_id},
        timeout=10,
    )
    if resp.status_code == 429:
        retry_after = None
        try:
            payload = resp.json()
            retry_after = (payload.get("parameters") or {}).get("retry_after")
        except Exception:
            retry_after = None
        raise ReferralRetryableError("telegram_rate_limited", retry_after=retry_after)    
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"getChatMember_not_ok:{data.get('description')}")
    return (data.get("result") or {}).get("status")


def _get_official_channel_member_status(user_id: int, chat_id: int | None = None) -> str | None:
    if not BOT_TOKEN:
        raise RuntimeError("missing_bot_token")
    # Defaults to OFFICIAL_CHANNEL_ID, but a caller settling a channel-origin
    # pending row created under a REFERRAL_DESTINATION_CHAT_ID override
    # passes that row's own destination_chat_id, so membership is checked
    # against the chat the invitee actually joined rather than always the
    # static official channel id.
    target_chat_id = chat_id if chat_id is not None else OFFICIAL_CHANNEL_ID
    if target_chat_id is None:
        raise RuntimeError("official_channel_unset")
    resp = requests.get(
        f"{API_BASE}/getChatMember",
        params={"chat_id": target_chat_id, "user_id": user_id},
        timeout=TG_GETCHATMEMBER_TIMEOUT_SEC,
    )
    try:
        data = resp.json()
        parse_failed = False
    except ValueError:
        data = {}
        parse_failed = True
    error_code = data.get("error_code") if isinstance(data, dict) else None
    if resp.status_code == 429 or error_code == 429:
        retry_after = None
        try:
            retry_after = (data.get("parameters") or {}).get("retry_after")
        except Exception:
            retry_after = None
        raise ReferralRetryableError("telegram_rate_limited", retry_after=retry_after)
    if resp.status_code >= 500:
        raise ReferralRetryableError(f"telegram_server_error_{resp.status_code}")
    if parse_failed:
        # Malformed/transient body: never a definitive verdict, and never
        # attributable to this specific invitee -- treat like any other
        # transient Telegram hiccup.
        raise ReferralRetryableError(f"telegram_malformed_response_{resp.status_code}")
    if not (isinstance(data, dict) and data.get("ok")):
        description = data.get("description") if isinstance(data, dict) else None
        raise ReferralTelegramError(
            "getChatMember_not_ok",
            status_code=resp.status_code,
            error_code=error_code,
            description=description,
            kind=_classify_telegram_getchatmember_error(resp.status_code, description),
        )
    result = data.get("result") or {}
    status = result.get("status")
    if status == "restricted":
        # A restricted supergroup member can still be a member in good
        # standing (Telegram sets is_member=true and only limits specific
        # permissions) -- only a restricted row with is_member explicitly
        # not true is a definitive "not a member" verdict.
        return "member" if result.get("is_member") is True else "kicked"
    return status


def _check_official_channel_subscribed_sync(uid: int) -> tuple[bool, str]:
    if not uid:
        return False, "missing_uid"
    if OFFICIAL_CHANNEL_ID is None:
        return False, "channel_unset"
    if not BOT_TOKEN:
        return False, "missing_token"

    def _fetch_once():
        try:
            resp = requests.get(
                f"{API_BASE}/getChatMember",
                params={"chat_id": OFFICIAL_CHANNEL_ID, "user_id": uid},
                timeout=10,
            )
        except requests.RequestException as exc:
            return None, None, str(exc)
        try:
            payload = resp.json()
        except ValueError:
            return resp.status_code, None, "bad_json"
        return resp.status_code, payload, None

    status_code, payload, err = _fetch_once()
    if err:
        return False, err

    for attempt in range(2):
        error_code = (payload or {}).get("error_code")
        if status_code == 429 or ((payload or {}).get("ok") is False and error_code == 429):
            if attempt == 1:
                return False, "rate_limited"
            retry_after = ((payload or {}).get("parameters") or {}).get("retry_after", 1)
            try:
                retry_after = int(retry_after)
            except (TypeError, ValueError):
                retry_after = 1
            time.sleep(max(0, min(retry_after, 5)))
            status_code, payload, err = _fetch_once()
            if err:
                return False, err
            continue
        break

    if not isinstance(payload, dict):
        return False, "bad_json"
    if not payload.get("ok"):
        return False, f"tg_not_ok:{payload.get('description', 'unknown')}"
    status = (payload.get("result") or {}).get("status")
    if status in {"member", "administrator", "creator"}:
        return True, f"status:{status}"
    return False, f"status:{status}"

def _coerce_utc(dt_value) -> datetime | None:
    if not dt_value:
        return None
    if isinstance(dt_value, datetime):
        if dt_value.tzinfo:
            return dt_value.astimezone(timezone.utc)
        return dt_value.replace(tzinfo=timezone.utc)
    if isinstance(dt_value, str):
        try:
            parsed = datetime.fromisoformat(dt_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc)
        return parsed.replace(tzinfo=timezone.utc)
    return None


def evaluate_referral_engagement(
    *,
    invitee_user_id: int,
    invitee_doc: dict | None,
    window_start: datetime,
    window_end: datetime,
    db_ref=None,
) -> dict:
    db_ref = db_ref or db
    first_checkin_at = _coerce_utc((invitee_doc or {}).get("first_checkin_at"))
    last_visible_at = _coerce_utc((invitee_doc or {}).get("last_visible_at"))
    claim_attempt_doc = db_ref.voucher_claims.find_one(
        {
            "user_id": invitee_user_id,
            "created_at": {"$gte": window_start, "$lte": window_end},
        },
        {"created_at": 1},
    ) or {}
    claim_attempt_at = _coerce_utc(claim_attempt_doc.get("created_at"))

    signals = {
        "first_checkin": bool(first_checkin_at and window_start <= first_checkin_at <= window_end),
        "miniapp_open": bool(last_visible_at and window_start <= last_visible_at <= window_end),
        "claim_attempt": bool(claim_attempt_at and window_start <= claim_attempt_at <= window_end),
    }
    points = {
        "first_checkin": 2 if signals["first_checkin"] else 0,
        "miniapp_open": 1 if signals["miniapp_open"] else 0,
        "claim_attempt": 2 if signals["claim_attempt"] else 0,
    }
    score = int(sum(points.values()))
    checkin_required = signals["first_checkin"]

    return {
        "score": score,
        "qualified": bool(checkin_required and score >= 3),
        "signals": signals,
        "points": points,
        "window_start": window_start,
        "window_end": window_end,
    }


def _round_rate(value: float) -> float:
    return round(float(value), 4)


def _utc_day_bounds(day_utc: str) -> tuple[datetime, datetime]:
    day_start = datetime.strptime(day_utc, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return day_start, day_start + timedelta(days=1)


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return _round_rate(float(numerator) / float(denominator))


def ensure_affiliate_daily_kpi_indexes(db_ref=None) -> None:
    db_ref = db_ref or db
    db_ref.affiliate_daily_kpis.create_index([("day_utc", 1)], unique=True, name="uniq_affiliate_daily_kpi_day")


def compute_affiliate_daily_kpi(day_utc: str, *, db_ref=None, now_utc_ts: datetime | None = None) -> dict:
    db_ref = db_ref or db
    now_utc_ts = now_utc_ts or now_utc()
    day_start, day_end = _utc_day_bounds(day_utc)

    referrals = list(
        db_ref.pending_referrals.find(
            {"created_at_utc": {"$gte": day_start, "$lt": day_end}},
            {"invitee_user_id": 1, "created_at_utc": 1},
        )
    )
    new_referrals = int(len(referrals))
    qualified = int(
        db_ref.qualified_events.count_documents(
            {"qualified_at": {"$gte": day_start, "$lt": day_end}}
        )
    )

    checkin_hits = 0
    claim_hits = 0
    invitee_first_referral_at: dict[int, datetime] = {}
    for row in referrals:
        invitee_user_id = row.get("invitee_user_id")
        created_at_utc = _coerce_utc(row.get("created_at_utc"))
        if invitee_user_id is None or not created_at_utc:
            continue
        prev_created_at = invitee_first_referral_at.get(invitee_user_id)
        if prev_created_at is None or created_at_utc < prev_created_at:
            invitee_first_referral_at[invitee_user_id] = created_at_utc
        cutoff = created_at_utc + timedelta(hours=72)

        user_doc = db_ref.users.find_one({"user_id": invitee_user_id}, {"first_checkin_at": 1}) or {}
        first_checkin_at = _coerce_utc(user_doc.get("first_checkin_at"))
        if first_checkin_at and first_checkin_at <= cutoff:
            checkin_hits += 1

        claim_doc = db_ref.new_joiner_claims.find_one({"uid": invitee_user_id}, {"claimed_at": 1}) or {}
        claimed_at = _coerce_utc(claim_doc.get("claimed_at"))
        if claimed_at and claimed_at <= cutoff:
            claim_hits += 1

    checkin_72h_rate = _safe_rate(checkin_hits, new_referrals)
    claim_proxy_72h_rate = _safe_rate(claim_hits, new_referrals)

    subscribed_72h_hits = 0
    invitee_ids = list(invitee_first_referral_at.keys())
    subscription_by_uid: dict[int, datetime | None] = {}

    if invitee_ids:
        try:
            cursor = db_ref.subscription_cache.find(
                {
                    "user_id": {"$in": invitee_ids},
                    "subscribed": {"$ne": False},
                },
                {
                    "user_id": 1,
                    "first_subscribed_at_utc": 1,
                    "checked_at": 1,
                    "updated_at": 1,
                },
            )
            for doc in cursor:
                uid = doc.get("user_id")
                if uid is None:
                    continue
                first_seen = _coerce_utc(doc.get("first_subscribed_at_utc"))
                if not first_seen:
                    first_seen = _coerce_utc(doc.get("checked_at"))
                subscription_by_uid[int(uid)] = first_seen
        except Exception:
            subscription_by_uid = {}

    for uid, referral_created_at in invitee_first_referral_at.items():
        first_seen = subscription_by_uid.get(uid)
        if first_seen and first_seen <= (referral_created_at + timedelta(hours=72)):
            subscribed_72h_hits += 1

    invitee_channel_sub_72h_rate = _safe_rate(subscribed_72h_hits, max(new_referrals, 1))

    window_7d_start = day_start - timedelta(days=6)
    window_filter = {"day_utc": {"$gte": window_7d_start.date().isoformat(), "$lte": day_utc}}
    snapshot_rows = list(
        db_ref.affiliate_daily_kpis.find(
            window_filter,
            {"new_referrals": 1, "qualified": 1},
        )
    )
    if len(snapshot_rows) == 7:
        new_referrals_7d = int(sum(int(r.get("new_referrals", 0) or 0) for r in snapshot_rows))
        qualified_7d = int(sum(int(r.get("qualified", 0) or 0) for r in snapshot_rows))
    else:
        new_referrals_7d = int(
            db_ref.pending_referrals.count_documents(
                {"created_at_utc": {"$gte": window_7d_start, "$lt": day_end}}
            )
        )
        qualified_7d = int(
            db_ref.qualified_events.count_documents(
                {"qualified_at": {"$gte": window_7d_start, "$lt": day_end}}
            )
        )

    # Keep this 7-day window aligned with qualified_7d/active_referrers_7d: [window_7d_start, day_end) in UTC.
    joins_7d_rows = list(
        db_ref.pending_referrals.aggregate(
            [
                {
                    "$match": {
                        "created_at_utc": {"$gte": window_7d_start, "$lt": day_end},
                        "inviter_user_id": {"$ne": None},
                    }
                },
                {"$group": {"_id": "$inviter_user_id", "joins_7d": {"$sum": 1}}},
            ]
        )
    )
    qualified_7d_rows = list(
        db_ref.qualified_events.aggregate(
            [
                {
                    "$match": {
                        "qualified_at": {"$gte": window_7d_start, "$lt": day_end},
                        "referrer_id": {"$ne": None},
                    }
                },
                {"$group": {"_id": "$referrer_id", "qualified_7d": {"$sum": 1}}},
            ]
        )
    )

    top_referrers_map: dict[str, dict] = {}
    for row in joins_7d_rows:
        referrer_id_raw = row.get("_id")
        if referrer_id_raw is None:
            continue
        referrer_key = str(referrer_id_raw)
        top_referrers_map[referrer_key] = {
            "referrer_id": referrer_key,
            "joins_7d": int(row.get("joins_7d", 0) or 0),
            "qualified_7d": 0,
        }
    for row in qualified_7d_rows:
        referrer_id_raw = row.get("_id")
        if referrer_id_raw is None:
            continue
        referrer_key = str(referrer_id_raw)
        if referrer_key not in top_referrers_map:
            top_referrers_map[referrer_key] = {
                "referrer_id": referrer_key,
                "joins_7d": 0,
                "qualified_7d": 0,
            }
        top_referrers_map[referrer_key]["qualified_7d"] = int(row.get("qualified_7d", 0) or 0)

    top_referrers_7d = []
    for item in top_referrers_map.values():
        joins_count = int(item.get("joins_7d", 0) or 0)
        qualified_count = int(item.get("qualified_7d", 0) or 0)
        # conversion_7d = qualified_7d / joins_7d; safe division returns 0 when joins_7d is 0.
        conversion_7d = float(qualified_count / joins_count) if joins_count > 0 else 0.0
        top_referrers_7d.append(
            {
                "referrer_id": item["referrer_id"],
                "joins_7d": joins_count,
                "qualified_7d": qualified_count,
                "conversion_7d": conversion_7d,
            }
        )
    top_referrers_7d.sort(key=lambda r: (-int(r["qualified_7d"]), -int(r["joins_7d"])))
    top_referrers_7d = top_referrers_7d[:20]

    quality_rate_7d = _safe_rate(qualified_7d, new_referrals_7d)
    active_referrers_7d = int(
        len(
            db_ref.qualified_events.distinct(
                "referrer_id",
                {
                    "qualified_at": {"$gte": window_7d_start, "$lt": day_end},
                    "referrer_id": {"$ne": None},
                },
            )
        )
    )

    payload = {
        "day_utc": day_utc,
        "new_referrals": new_referrals,
        "qualified": qualified,
        "checkin_72h_rate": checkin_72h_rate,
        "claim_proxy_72h_rate": claim_proxy_72h_rate,
        "invitee_channel_sub_72h_rate": invitee_channel_sub_72h_rate,
        "new_referrals_7d": new_referrals_7d,
        "qualified_7d": qualified_7d,
        "quality_rate_7d": quality_rate_7d,
        "active_referrers_7d": active_referrers_7d,
        "top_referrers_7d": top_referrers_7d,
        "computed_at_utc": now_utc_ts,
    }
    db_ref.affiliate_daily_kpis.update_one({"day_utc": day_utc}, {"$set": payload}, upsert=True)
    logger.info(
        "[AFF_KPI] day=%s new=%s qualified=%s checkin72=%s claim72=%s q7=%s qr7=%s",
        day_utc,
        new_referrals,
        qualified,
        checkin_72h_rate,
        claim_proxy_72h_rate,
        qualified_7d,
        quality_rate_7d,
    )
    return payload


# DEPRECATED: this legacy publisher ranks by a raw `qualified_events`
# aggregation, not the authoritative `users.weekly_referrals` settlement-ledger
# snapshot. It must NOT be used for the public Sunday "Top 5 Growth Leaders"
# channel post — `publish_weekly_referral_post()` (job id `weekly_referral_post`)
# is the authoritative implementation for that post. Kept only for backward
# compatibility where GROWTH_LEADERBOARD_ENABLED was already relied on and
# WEEKLY_REF_POST_CHAT_ID is not configured; see the startup conflict guard in
# main.py's run_worker() scheduler registration.
def post_growth_leaderboard_weekly(*, db_ref=None, now_utc_ts: datetime | None = None) -> bool:
    db_ref = db_ref or db
    now_utc_ts = now_utc_ts or now_utc()
    tz_name = os.getenv("GROWTH_LEADERBOARD_TIMEZONE", "Asia/Kuala_Lumpur")
    local_tz = pytz.timezone(tz_name)
    channel_id_raw = (os.getenv("GROWTH_LEADERBOARD_CHANNEL_ID", "") or "").strip()
    if not channel_id_raw:
        logger.warning("[GROWTH_LEADERBOARD] skip reason=missing_channel_id")
        return False

    try:
        channel_id = int(channel_id_raw)
    except (TypeError, ValueError):
        logger.warning("[GROWTH_LEADERBOARD] skip reason=invalid_channel_id value=%s", channel_id_raw)
        return False

    now_local = now_utc_ts.astimezone(local_tz)
    week_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now_local.weekday())
    week_end_local = week_start_local + timedelta(days=7)
    week_start_utc = week_start_local.astimezone(timezone.utc)
    week_end_utc = week_end_local.astimezone(timezone.utc)
    week_key = week_start_local.date().isoformat()

    existing = db_ref.growth_leaderboard_posts.find_one({"week_key": week_key}, {"_id": 1})
    if existing:
        logger.info("[GROWTH_LEADERBOARD] skip reason=already_posted week_key=%s", week_key)
        return False

    rows = list(
        db_ref.qualified_events.aggregate(
            [
                {"$match": {"qualified_at": {"$gte": week_start_utc, "$lt": week_end_utc}, "referrer_id": {"$ne": None}}},
                {"$group": {"_id": "$referrer_id", "qualified_count": {"$sum": 1}}},
                {"$match": {"qualified_count": {"$gt": 0}}},
                {"$sort": {"qualified_count": -1, "_id": 1}},
                {"$limit": 5},
            ]
        )
    )
    if not rows:
        logger.info("[GROWTH_LEADERBOARD] skip reason=no_qualified_referrals week_key=%s", week_key)
        return False

    medals = ["🥇", "🥈", "🥉"]
    lines = ["<b>🏆 Top 5 Growth Leaders This Week</b>", ""]
    for idx, row in enumerate(rows, start=1):
        uid = int(row.get("_id"))
        qcount = int(row.get("qualified_count", 0) or 0)
        user = db_ref.users.find_one({"user_id": uid}, {"username": 1, "first_name": 1}) or {}
        display_name = user.get("username") or user.get("first_name") or f"User {uid}"
        display_name = html_escape(str(display_name))
        prefix = medals[idx - 1] if idx <= 3 else f"#{idx}"
        lines.append(f"{prefix} {display_name} — {qcount} qualified invites")
    lines.extend(
        [
            "",
            "<i>Invite more qualified members, join our affiliate program, and earn up to <b>$450/month</b>.</i>",
        ]
    )
    text = "\n".join(lines)

    lock = db_ref.growth_leaderboard_posts.update_one(
        {"week_key": week_key},
        {
            "$setOnInsert": {
                "week_key": week_key,
                "week_start_utc": week_start_utc,
                "week_end_utc": week_end_utc,
                "created_at": now_utc_ts,
                "status": "posting",
            }
        },
        upsert=True,
    )
    if not getattr(lock, "upserted_id", None):
        logger.info("[GROWTH_LEADERBOARD] skip reason=already_posted_race week_key=%s", week_key)
        return False

    resp = requests.post(
        f"{API_BASE}/sendMessage",
        json={
            "chat_id": channel_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description") or "telegram_not_ok")
    message_id = ((payload.get("result") or {}).get("message_id"))
    db_ref.growth_leaderboard_posts.update_one(
        {"week_key": week_key},
        {"$set": {"status": "posted", "posted_at": now_utc_ts, "message_id": message_id}},
    )
    logger.info("[GROWTH_LEADERBOARD] posted week_key=%s message_id=%s", week_key, message_id)
    return True


# ---------------------------------------------------------------------------
# Sunday weekly "Top 5 Growth Leaders" referral post.
#
# Ranking source is the authoritative users.weekly_referrals snapshot (kept
# in sync with the referral settlement ledger) — NOT a raw join, NOT pending
# referrals, and NOT the affiliate raw-invite aggregation used elsewhere.
# The ranking is frozen into `weekly_referral_posts` (idempotency key
# "weekly_referral_post:{week_start_local}") before any Telegram delivery is
# attempted, so retries/reruns always resend the same frozen entries and never
# duplicate the channel post once a message_id is recorded.
# ---------------------------------------------------------------------------
WEEKLY_REF_POST_TZ = pytz.timezone("Asia/Kuala_Lumpur")


def _weekly_referral_post_week_bounds(week_key: str | None, now_utc_ts: datetime):
    if week_key:
        week_start_local = WEEKLY_REF_POST_TZ.localize(datetime.strptime(week_key, "%Y-%m-%d"))
    else:
        now_local = now_utc_ts.astimezone(WEEKLY_REF_POST_TZ)
        week_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=now_local.weekday()
        )
    week_end_local = week_start_local + timedelta(days=7)
    return week_start_local, week_end_local


def _weekly_referral_display_name(user_id: int, username: str | None, first_name: str | None) -> str:
    if username:
        cleaned = str(username).lstrip("@").strip()
        if cleaned:
            return f"@{cleaned}"
    if first_name and str(first_name).strip():
        return str(first_name).strip()
    return f"Member #{str(int(user_id))[-4:]}"


def _weekly_referral_entries_from_users(db_ref) -> list[dict]:
    """Authoritative source: current users.weekly_referrals snapshot."""
    proj = {"user_id": 1, "username": 1, "first_name": 1, "weekly_referrals": 1}
    rows = db_ref.users.find({"weekly_referrals": {"$gt": 0}}, proj).sort(
        [("weekly_referrals", -1), ("user_id", 1)]
    ).limit(5)
    entries = []
    for row in rows:
        uid = int(row["user_id"])
        entries.append(
            {
                "user_id": uid,
                "display_name": _weekly_referral_display_name(uid, row.get("username"), row.get("first_name")),
                "weekly_referrals": int(row.get("weekly_referrals", 0) or 0),
            }
        )
    return entries


def weekly_referral_entries_from_history_archive(db_ref, week_start_local: datetime) -> list[dict]:
    """Fallback source for a completed (already-reset) week: the pre-reset
    archive written by reset_weekly_xp() into weekly_leaderboard_history."""
    week_start_date = week_start_local.date().isoformat()
    archive = db_ref.weekly_leaderboard_history.find_one({"week_start": week_start_date})
    if not archive:
        return []
    rows = archive.get("referral_leaderboard") or []
    filtered = [r for r in rows if int(r.get("weekly_referrals", 0) or 0) > 0]
    filtered.sort(key=lambda r: (-int(r.get("weekly_referrals", 0) or 0), int(r.get("user_id"))))
    entries = []
    for row in filtered[:5]:
        uid = int(row["user_id"])
        entries.append(
            {
                "user_id": uid,
                "display_name": _weekly_referral_display_name(uid, row.get("username"), None),
                "weekly_referrals": int(row.get("weekly_referrals", 0) or 0),
            }
        )
    return entries


def render_weekly_referral_post_text(entries: list[dict]) -> str:
    medals = ["🥇", "🥈", "🥉"]
    lines = ["<b>🏆 Top 5 Growth Leaders This Week</b>", ""]
    for idx, entry in enumerate(entries, start=1):
        prefix = medals[idx - 1] if idx <= 3 else f"#{idx}"
        name = html_escape(str(entry["display_name"]))
        count = int(entry["weekly_referrals"])
        lines.append(f"{prefix} {name} — {count} qualified invites")
    lines.extend(
        [
            "",
            "Invite more qualified members, join our affiliate program, and earn up to <b>$450/month</b>.",
        ]
    )
    return "\n".join(lines)


def publish_weekly_referral_post(
    *,
    db_ref=None,
    now_utc_ts: datetime | None = None,
    run_id: str | None = None,
    week_key: str | None = None,
    entries_override: list[dict] | None = None,
    dry_run: bool = False,
    source: str = "live",
) -> dict:
    """Freeze (or reuse) the Top-5 weekly_referrals ranking and deliver the
    historical "Top 5 Growth Leaders This Week" post to WEEKLY_REF_POST_CHAT_ID.

    Idempotent on `weekly_referral_post:{week_start_local}`. Safe to call
    repeatedly (scheduler retry, worker restart, manual rerun, or the
    `scripts.publish_weekly_referral_post` repair script) — a frozen ranking
    is only computed once per week and a channel post only sent once a
    Telegram message_id has not already been recorded.
    """
    db_ref = db_ref or db
    now_utc_ts = now_utc_ts or now_utc()
    run_id = run_id or f"wrp_{int(now_utc_ts.timestamp() * 1000)}"

    week_start_local, week_end_local = _weekly_referral_post_week_bounds(week_key, now_utc_ts)
    resolved_week_key = week_start_local.date().isoformat()
    doc_id = f"weekly_referral_post:{resolved_week_key}"

    logger.info("[WEEKLY_REF_POST][START] week_key=%s run_id=%s", resolved_week_key, run_id)

    doc = db_ref.weekly_referral_posts.find_one({"_id": doc_id})

    if doc is None:
        if entries_override is not None:
            entries = entries_override
        elif source == "archive":
            # Historical/manual repair of a completed week: pull from the
            # pre-reset archive, never from the live (already-reset) counters.
            entries = weekly_referral_entries_from_history_archive(db_ref, week_start_local)
        else:
            entries = _weekly_referral_entries_from_users(db_ref)

        new_doc = {
            "_id": doc_id,
            "week_key": resolved_week_key,
            "week_start_local": week_start_local.isoformat(),
            "week_end_local": week_end_local.isoformat(),
            "entries": entries,
            "status": "frozen" if entries else "empty",
            "attempted_at": None,
            "sent_at": None,
            "message_id": None,
            "failure_reason": None,
            "created_at": now_utc_ts,
            "run_id": run_id,
        }
        try:
            db_ref.weekly_referral_posts.insert_one(new_doc)
            doc = new_doc
        except DuplicateKeyError:
            doc = db_ref.weekly_referral_posts.find_one({"_id": doc_id})
        logger.info(
            "[WEEKLY_REF_POST][FROZEN] week_key=%s entry_count=%s run_id=%s",
            resolved_week_key,
            len(doc.get("entries") or []),
            run_id,
        )

    entries = doc.get("entries") or []

    if doc.get("status") == "sent":
        logger.info(
            "[WEEKLY_REF_POST][SKIP_ALREADY_SENT] week_key=%s message_id=%s run_id=%s",
            resolved_week_key,
            doc.get("message_id"),
            run_id,
        )
        return doc

    if not entries:
        logger.info("[WEEKLY_REF_POST][SKIP_EMPTY] week_key=%s run_id=%s", resolved_week_key, run_id)
        return doc

    text = render_weekly_referral_post_text(entries)

    if dry_run:
        return {**doc, "preview_text": text}

    if doc.get("status") == "failed":
        logger.info(
            "[WEEKLY_REF_POST][RETRY] week_key=%s entry_count=%s run_id=%s prior_error=%s",
            resolved_week_key,
            len(entries),
            run_id,
            doc.get("failure_reason"),
        )

    chat_id_raw = (os.getenv("WEEKLY_REF_POST_CHAT_ID", "") or "").strip()
    if not chat_id_raw:
        logger.error(
            "[WEEKLY_REF_POST][FAILED] week_key=%s entry_count=%s run_id=%s error=missing_chat_id",
            resolved_week_key,
            len(entries),
            run_id,
        )
        db_ref.weekly_referral_posts.update_one(
            {"_id": doc_id},
            {"$set": {"status": "failed", "failure_reason": "missing_chat_id", "attempted_at": now_utc_ts}},
        )
        return db_ref.weekly_referral_posts.find_one({"_id": doc_id})

    try:
        chat_id = int(chat_id_raw)
    except (TypeError, ValueError):
        logger.error(
            "[WEEKLY_REF_POST][FAILED] week_key=%s run_id=%s error=invalid_chat_id chat_id=%s",
            resolved_week_key,
            run_id,
            chat_id_raw,
        )
        db_ref.weekly_referral_posts.update_one(
            {"_id": doc_id},
            {"$set": {"status": "failed", "failure_reason": "invalid_chat_id", "attempted_at": now_utc_ts}},
        )
        return db_ref.weekly_referral_posts.find_one({"_id": doc_id})

    # Atomically claim the send slot: only an invocation that actually flips
    # status frozen/failed -> sending is allowed to call Telegram. This is what
    # prevents a concurrent scheduler retry, worker restart, or manual repair
    # run from double-posting even though max_instances=1 only serializes
    # executions within a single APScheduler instance. A "sending" claim older
    # than the lease window is treated as abandoned (crashed mid-send) and can
    # be reclaimed by a later retry.
    claim_lease_cutoff = now_utc_ts - timedelta(minutes=10)
    claim = db_ref.weekly_referral_posts.update_one(
        {
            "_id": doc_id,
            "$or": [
                {"status": {"$in": ["frozen", "failed"]}},
                {"status": "sending", "attempted_at": {"$lt": claim_lease_cutoff}},
            ],
        },
        {"$set": {"status": "sending", "attempted_at": now_utc_ts}},
    )
    if getattr(claim, "modified_count", 0) != 1:
        current = db_ref.weekly_referral_posts.find_one({"_id": doc_id})
        logger.info(
            "[WEEKLY_REF_POST][SKIP_ALREADY_SENT] week_key=%s status=%s run_id=%s reason=claim_lost",
            resolved_week_key,
            (current or {}).get("status"),
            run_id,
        )
        return current

    try:
        resp = requests.post(
            f"{API_BASE}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
        if not payload.get("ok"):
            raise RuntimeError(payload.get("description") or "telegram_not_ok")
        message_id = (payload.get("result") or {}).get("message_id")
    except Exception as exc:
        logger.error(
            "[WEEKLY_REF_POST][FAILED] week_key=%s chat_id=%s run_id=%s error=%s",
            resolved_week_key,
            chat_id,
            run_id,
            exc,
        )
        db_ref.weekly_referral_posts.update_one(
            {"_id": doc_id},
            {"$set": {"status": "failed", "failure_reason": f"{exc.__class__.__name__}: {exc}"}},
        )
        return db_ref.weekly_referral_posts.find_one({"_id": doc_id})

    db_ref.weekly_referral_posts.update_one(
        {"_id": doc_id},
        {"$set": {"status": "sent", "sent_at": now_utc_ts, "message_id": message_id, "failure_reason": None}},
    )
    logger.info(
        "[WEEKLY_REF_POST][SENT] week_key=%s entry_count=%s chat_id=%s message_id=%s run_id=%s",
        resolved_week_key,
        len(entries),
        chat_id,
        message_id,
        run_id,
    )
    return db_ref.weekly_referral_posts.find_one({"_id": doc_id})


def compute_affiliate_daily_kpi_yesterday() -> dict:
    ensure_affiliate_daily_kpi_indexes()
    target_day = (now_utc() - timedelta(days=1)).date().isoformat()
    return compute_affiliate_daily_kpi(target_day)


def _channel_subscribe_verdict(result_dict) -> tuple[bool, str | None, bool | None]:
    status = (result_dict.get("status") or "").lower() or None
    is_member = result_dict.get("is_member")
    subscribed = (
        status in ("member", "administrator", "creator")
        or (status == "restricted" and is_member is True)
    )
    if status in ("left", "kicked"):
        subscribed = False
    return subscribed, status, is_member


def run_invitee_subscription_audit(now_utc_ts=None, db_ref=None) -> dict:
    db_ref = db_ref or db
    now_utc_ts = now_utc_ts or now_utc()
    started = time.monotonic()
    audit_enabled = INVITEE_SUB_AUDIT_ENABLED
    try:
        job_cfg = _get_setting("scheduler", "invite_subscription_audit") if _get_setting else None
        if isinstance(job_cfg, dict) and "enabled" in job_cfg:
            audit_enabled = bool(job_cfg["enabled"])
    except Exception:
        pass
    if not audit_enabled:
        logger.info("[SUB_AUDIT] disabled")
        return {"disabled": True}
    if OFFICIAL_CHANNEL_ID is None:
        logger.warning("[SUB_AUDIT] skip reason=channel_unset")
        return {"skipped": "channel_unset"}
    if not BOT_TOKEN:
        logger.warning("[SUB_AUDIT] skip reason=missing_token")
        return {"skipped": "missing_token"}

    scan_start = now_utc_ts - timedelta(days=3)
    logger.info("[SUB_AUDIT] start scan_start=%s limit=%s", scan_start.isoformat(), MAX_INVITEE_SUB_CHECKS_PER_RUN)

    scanned = checked = subscribed_true = subscribed_false = skipped_recent = errors = 0
    recent_cutoff = now_utc_ts - timedelta(hours=RECENT_CHECK_SKIP_HOURS)
    ttl_expire_at = now_utc_ts + timedelta(days=SUB_CACHE_TTL_DAYS)
    cursor = db_ref.pending_referrals.find(
        {"created_at_utc": {"$gte": scan_start}},
        {"invitee_user_id": 1, "created_at_utc": 1},
    ).sort("created_at_utc", -1).limit(MAX_INVITEE_SUB_CHECKS_PER_RUN)

    for row in cursor:
        scanned += 1
        uid = row.get("invitee_user_id")
        if uid is None or not isinstance(uid, int):
            continue

        cache_id = f"sub:{uid}"
        cache_doc = db_ref.subscription_cache.find_one(
            {"_id": cache_id},
            {"checked_at": 1, "first_subscribed_at_utc": 1},
        ) or {}
        checked_at = _coerce_utc(cache_doc.get("checked_at"))
        if checked_at and checked_at >= recent_cutoff:
            skipped_recent += 1
            continue

        checked += 1
        subscribed = False
        tg_member_status = None
        tg_is_member = None
        tg_error = None
        try:
            resp = requests.get(
                f"{API_BASE}/getChatMember",
                params={"chat_id": OFFICIAL_CHANNEL_ID, "user_id": uid},
                timeout=TG_GETCHATMEMBER_TIMEOUT_SEC,
            )
            if resp.status_code != 200:
                tg_error = f"http_{resp.status_code}"
            else:
                try:
                    payload = resp.json()
                except ValueError:
                    tg_error = "bad_json"
                else:
                    if not payload.get("ok"):
                        desc = payload.get("description")
                        tg_error = f"not_ok:{desc}" if desc else "not_ok"
                    else:
                        subscribed, tg_member_status, tg_is_member = _channel_subscribe_verdict(payload.get("result") or {})
        except RequestException as exc:
            tg_error = str(exc) or exc.__class__.__name__

        if tg_error:
            errors += 1
            subscribed = False

        if subscribed:
            subscribed_true += 1
        else:
            subscribed_false += 1

        update_doc = {
            "$set": {
                "user_id": uid,
                "subscribed": subscribed,
                "tg_member_status": tg_member_status,
                "tg_is_member": tg_is_member,
                "tg_error": tg_error,
                "checked_at": now_utc_ts,
                "updated_at": now_utc_ts,
                "expireAt": ttl_expire_at,
            }
        }
        if subscribed:
            update_doc["$setOnInsert"] = {"first_subscribed_at_utc": now_utc_ts}
            if not _coerce_utc(cache_doc.get("first_subscribed_at_utc")):
                update_doc["$set"]["first_subscribed_at_utc"] = now_utc_ts

        db_ref.subscription_cache.update_one({"_id": cache_id}, update_doc, upsert=True)
        time.sleep(max(TG_REQUEST_SLEEP_MS, 0) / 1000.0)

    duration_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "[SUB_AUDIT] done scanned=%s checked=%s subscribed_true=%s subscribed_false=%s skipped_recent=%s errors=%s duration_ms=%s",
        scanned,
        checked,
        subscribed_true,
        subscribed_false,
        skipped_recent,
        errors,
        duration_ms,
    )
    return {
        "scanned": scanned,
        "checked": checked,
        "subscribed_true": subscribed_true,
        "subscribed_false": subscribed_false,
        "skipped_recent": skipped_recent,
        "errors": errors,
        "duration_ms": duration_ms,
    }



def _compute_backoff_seconds(retry_count: int, *, base: int, cap: int) -> int:
    try:
        retry_count = int(retry_count)
    except (TypeError, ValueError):
        retry_count = 0
    return min(cap, base * (2**retry_count))


def _release_for_retry(pending_id, now_utc_ts: datetime, retry_after_seconds: int, reason: str) -> None:
    next_retry = now_utc_ts + timedelta(seconds=retry_after_seconds)
    db.pending_referrals.update_one(
        {"_id": pending_id},
        {
            "$set": {
                "status": "pending",
                "next_retry_at_utc": next_retry,
                "retry_last_reason": reason,
            },
            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
            "$inc": {"retry_count": 1},
        },
    )

def _week_start_kl(reference: datetime | None = None) -> datetime:
    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    return (ref_local - timedelta(days=ref_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

def _week_end_kl(reference: datetime | None = None) -> datetime:
    return _week_start_kl(reference) + timedelta(days=7)

def _month_start_kl(reference: datetime | None = None) -> datetime:
    ref_local = reference.astimezone(KL_TZ) if reference else datetime.now(KL_TZ)
    return ref_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def _month_end_kl(reference: datetime | None = None) -> datetime:
    start_local = _month_start_kl(reference)
    if start_local.month == 12:
        return start_local.replace(year=start_local.year + 1, month=1)
    return start_local.replace(month=start_local.month + 1)

def _maybe_send_near_miss_dm_web(inviter_user_id: int, total_referrals_after: int) -> None:
    if os.getenv("RUNNER_MODE") != "web":
        return
    try:
        from main import _maybe_send_near_miss_dm
    except Exception:
        logger.exception(
            "[SCHED][REFERRAL] near_miss_import_failed inviter=%s",
            inviter_user_id,
        )
        return
    try:
        _maybe_send_near_miss_dm(inviter_user_id, total_referrals_after)
    except Exception:
        logger.exception(
            "[SCHED][REFERRAL] near_miss_failed inviter=%s",
            inviter_user_id,
        )
        
def _week_window_utc(reference: datetime | None = None) -> tuple[datetime, datetime]:
    start_local = _week_start_kl(reference)
    end_local = _week_end_kl(reference)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def _month_window_utc(reference: datetime | None = None) -> tuple[datetime, datetime]:
    start_local = _month_start_kl(reference)
    end_local = _month_end_kl(reference)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)
    
def _referral_event_doc(inviter_id: int, invitee_id: int, event: str, occurred_at: datetime) -> dict:
    week_key = _week_start_kl(occurred_at).date().isoformat()
    month_key = _month_start_kl(occurred_at).date().isoformat()
    return {
        "inviter_id": inviter_id,
        "invitee_id": invitee_id,
        "event": event,
        "occurred_at": occurred_at,
        "week_key": week_key,
        "month_key": month_key,
    }

def _record_referral_event(inviter_id: int, invitee_id: int, event: str, occurred_at: datetime) -> bool:
    if inviter_id is None or invitee_id is None:
        return False
    try:
        event_doc = _referral_event_doc(inviter_id, invitee_id, event, occurred_at)
        db.referral_events.insert_one(event_doc)
    except DuplicateKeyError:
        logger.info(
            "[SCHED][REFERRAL_LEDGER] duplicate inviter=%s invitee=%s action=%s",
            inviter_id,
            invitee_id,
            event,
        )
        return False

    logger.info(
        "[SCHED][REFERRAL_LEDGER] inviter=%s invitee=%s action=%s",
        inviter_id,
        invitee_id,
        "settled" if event == "referral_settled" else "revoked",
    )
    try:
        from affiliate_leaderboard import emit_referral_flow_event
        emit_referral_flow_event(
            db,
            event=event,
            referrer_id=int(inviter_id),
            invitee_id=int(invitee_id),
            ts_utc=occurred_at,
            meta={},
            idempotency_key=f"rf|{event}|{int(inviter_id)}|{int(invitee_id)}|{occurred_at.isoformat()}",
        )
    except Exception:
        logger.exception(
            "[SCHED][REFERRAL_LEDGER] flow_event_emit_failed inviter=%s invitee=%s event=%s",
            inviter_id,
            invitee_id,
            event,
        )
    return True


def revoke_settled_referral(
    db,
    *,
    inviter_id: int,
    invitee_id: int,
    reason: str,
    occurred_at: datetime,
) -> bool:
    """Reverse a *previously settled* referral.

    This is the only path that may write a referral_revoked event. It
    requires a matching referral_settled event for the same inviter/invitee
    pair to already exist, so a referral that never settled can never be
    driven negative by a revocation. Returns True only when a new
    revocation event was written; False (with no ledger change) if there
    was no prior settlement, or a revocation for this pair already exists.
    """
    if inviter_id is None or invitee_id is None:
        return False
    inviter_id = int(inviter_id)
    invitee_id = int(invitee_id)

    settled_event = db.referral_events.find_one(
        {"inviter_id": inviter_id, "invitee_id": invitee_id, "event": "referral_settled"}
    )
    if not settled_event:
        logger.warning(
            "[SCHED][REFERRAL_LEDGER][REVOKE_WITHOUT_SETTLEMENT] inviter=%s invitee=%s reason=%s",
            inviter_id,
            invitee_id,
            reason,
        )
        return False

    existing_revoke = db.referral_events.find_one(
        {"inviter_id": inviter_id, "invitee_id": invitee_id, "event": "referral_revoked"}
    )
    if existing_revoke:
        logger.info(
            "[SCHED][REFERRAL_LEDGER][REVOKE_ALREADY_APPLIED] inviter=%s invitee=%s reason=%s",
            inviter_id,
            invitee_id,
            reason,
        )
        return False

    event_doc = _referral_event_doc(inviter_id, invitee_id, "referral_revoked", occurred_at)
    event_doc["reason"] = reason
    event_doc["reverses_settled_at"] = settled_event.get("occurred_at")
    try:
        db.referral_events.insert_one(event_doc)
    except DuplicateKeyError:
        logger.info(
            "[SCHED][REFERRAL_LEDGER] duplicate inviter=%s invitee=%s action=referral_revoked",
            inviter_id,
            invitee_id,
        )
        return False

    logger.info(
        "[SCHED][REFERRAL_LEDGER] inviter=%s invitee=%s action=revoked reason=%s",
        inviter_id,
        invitee_id,
        reason,
    )
    try:
        from affiliate_leaderboard import emit_referral_flow_event
        emit_referral_flow_event(
            db,
            event="referral_revoked",
            referrer_id=inviter_id,
            invitee_id=invitee_id,
            ts_utc=occurred_at,
            meta={"reason": reason},
            idempotency_key=f"rf|referral_revoked|{inviter_id}|{invitee_id}|{occurred_at.isoformat()}",
        )
    except Exception:
        logger.exception(
            "[SCHED][REFERRAL_LEDGER] flow_event_emit_failed inviter=%s invitee=%s event=referral_revoked",
            inviter_id,
            invitee_id,
        )
    return True


def maybe_handle_first_referral(uid: int, old_total: int, new_total: int, now_utc_ts: datetime) -> None:
    if old_total != 0 or new_total < 1:
        return
    try:
        from onboarding import record_first_referral, maybe_unlock_vip1
    except Exception:
        logger.exception("[FIRST_REFERRAL] import_failed uid=%s", uid)
        return
    created = record_first_referral(uid, ref=now_utc_ts)
    if created:
        maybe_unlock_vip1(uid)
        
def _xp_time_expr():
    return {"$ifNull": ["$created_at", "$ts"]}

def _write_snapshot_heartbeat(source: str, now_utc_ts: datetime) -> None:
    try:
        db.admin_cache.update_one(
            {"_id": "snapshot_heartbeat"},
            {
                "$set": {
                    "ts_utc": now_utc_ts,
                    "ts_kl": now_utc_ts.astimezone(KL_TZ),
                    "source": source,
                }
            },
            upsert=True,
        )
        logger.info(
            "[SNAPSHOT][HEARTBEAT] type=%s ts=%s",
            source,
            now_utc_ts.isoformat(),
        )
    except Exception:
        logger.exception("[SNAPSHOT][HEARTBEAT] failed type=%s", source)
        
def settle_xp_snapshots() -> None:
    """Entry point used by tick_5min. Dispatches to the incremental settler
    (default) or the legacy full-history rebuild (rollback path via
    XP_SNAPSHOT_INCREMENTAL=0).

    See xp_snapshot.py for the incremental implementation and
    docs/xp_snapshot_incremental.md for the design rationale.
    """
    if os.getenv("XP_SNAPSHOT_INCREMENTAL", "1") == "1":
        from xp_snapshot import settle_xp_snapshots_incremental

        settle_xp_snapshots_incremental(db)
    else:
        _settle_xp_snapshots_full_rebuild()


def _settle_xp_snapshots_full_rebuild(now_utc_ts: datetime | None = None) -> None:
    """Legacy behavior: re-aggregate the entire xp_events history every run.

    Kept only as the migration bootstrap step (xp_snapshot.py runs this
    exactly once to establish known-correct totals before the incremental
    cursor takes over) and as a manual rollback path.
    """
    run_started = time.monotonic()
    verbose_snapshot_logs = os.getenv("SNAPSHOT_VERBOSE_LOGS", "").strip() == "1"
    now_utc_ts = now_utc_ts or now_utc()
    week_start_utc, week_end_utc = _week_window_utc(now_utc_ts)
    month_start_utc, month_end_utc = _month_window_utc(now_utc_ts)
    logger.info(
        "[SNAPSHOT] rebuild_start kind=xp week_start=%s month_start=%s",
        week_start_utc.isoformat(),
        month_start_utc.isoformat(),
    )
    
    week_cond = {
        "$and": [
            {"$gte": [_xp_time_expr(), week_start_utc]},
            {"$lt": [_xp_time_expr(), week_end_utc]},
        ]
    }

    month_cond = {
        "$and": [
            {"$gte": [_xp_time_expr(), month_start_utc]},
            {"$lt": [_xp_time_expr(), month_end_utc]},
        ]
    }

    db.users.update_many(
        {},
        {
            "$set": {
                "weekly_xp_next": 0,
                "monthly_xp_next": 0,
                "total_xp_next": 0,
            }
        },
    )

    pipeline = [
        {
            "$match": {
                "user_id": {"$ne": None},
                "$or": [{"invalidated": {"$exists": False}}, {"invalidated": False}],
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "total_xp": {"$sum": "$xp"},
                "weekly_xp": {"$sum": {"$cond": [week_cond, "$xp", 0]}},
                "monthly_xp": {"$sum": {"$cond": [month_cond, "$xp", 0]}},
            }
        },
    ]
    results = list(db.xp_events.aggregate(pipeline))
    processed_count = len(results)
    skipped_count = 0
    written_count = 0
    if results:
        updates = []
        for row in results:
            uid = row.get("_id")
            if uid is None:
                skipped_count += 1
                continue
            total_xp = int(row.get("total_xp", 0))
            weekly_xp = int(row.get("weekly_xp", 0))
            monthly_xp = int(row.get("monthly_xp", 0))
            updates.append(
                UpdateOne(
                    {"user_id": uid},
                    {
                        "$set": {
                            "total_xp_next": total_xp,
                            "weekly_xp_next": weekly_xp,
                            "monthly_xp_next": monthly_xp,
                        }
                    },
                    upsert=True,
                )
            )
        if updates:
            db.users.bulk_write(updates, ordered=False)
            written_count = len(updates)

    publish_result = db.users.update_many(
        {},
        [
            {
                "$set": {
                    "total_xp": "$total_xp_next",
                    "weekly_xp": "$weekly_xp_next",
                    "monthly_xp": "$monthly_xp_next",
                    "xp": "$total_xp_next",
                    "snapshot_published_at": now_utc_ts,
                    "snapshot_updated_at": now_utc_ts,                    
                }
            }
        ],
    )
    db.users.update_many({}, {"$inc": {"snapshot_version": 1}})
    logger.info(
        "[SNAPSHOT] publish_done users=%s version_inc=1",
        publish_result.modified_count,
    )    
    _write_snapshot_heartbeat("xp", now_utc_ts)
    elapsed_ms = int((time.monotonic() - run_started) * 1000)
    logger.info(
        "[SCHED][SNAPSHOT_SUMMARY] kind=xp processed=%s written=%s skipped=%s elapsed_ms=%s",
        processed_count,
        written_count,
        skipped_count,
        elapsed_ms,
    )
    for row in results:
        uid = row.get("_id")
        if uid is None:
            continue

        if verbose_snapshot_logs:
            logger.debug(
                "[SCHED][SNAPSHOT_WRITE] uid=%s weekly_xp=%s",
                uid,
                int(row.get("weekly_xp", 0)),
            )
        if int(row.get("monthly_xp", 0)) >= 800:
            try:
                from onboarding import maybe_unlock_vip1
            except Exception:
                logger.exception("[VIP][CHECK] import_failed uid=%s", uid)
            else:
                maybe_unlock_vip1(uid)        

def _referral_sign_expr():
    return {
        "$cond": [
            {"$eq": ["$event", "referral_settled"]},
            1,
            {
                "$cond": [
                    {"$eq": ["$event", "referral_revoked"]},
                    -1,
                    0,
                ]
            },
        ]
    }

def settle_referral_snapshots() -> dict:
    start_perf = time.perf_counter()
    now_utc_ts = now_utc()
    week_start_utc, week_end_utc = _week_window_utc(now_utc_ts)
    month_start_utc, month_end_utc = _month_window_utc(now_utc_ts)
    logger.info(
        "[SNAPSHOT] rebuild_start kind=referral week_start=%s month_start=%s",
        week_start_utc.isoformat(),
        month_start_utc.isoformat(),
    )
    
    week_cond = {
        "$and": [
            {"$gte": ["$occurred_at", week_start_utc]},
            {"$lt": ["$occurred_at", week_end_utc]},
        ]
    }
    month_cond = {
        "$and": [
            {"$gte": ["$occurred_at", month_start_utc]},
            {"$lt": ["$occurred_at", month_end_utc]},
        ]
    }

    db.users.update_many(
        {},
        {
            "$set": {
                "weekly_referrals_next": 0,
                "monthly_referrals_next": 0,
                "total_referrals_next": 0,
            }
        },
    )

    pipeline = [
        {
            "$match": with_not_invalidated(
                {
                    "inviter_id": {"$ne": None},
                    "event": {"$in": ["referral_settled", "referral_revoked"]},
                }
            )
        },
        {
            "$group": {
                "_id": "$inviter_id",
                "total_referrals": {"$sum": _referral_sign_expr()},
                "weekly_referrals": {"$sum": {"$cond": [week_cond, _referral_sign_expr(), 0]}},
                "monthly_referrals": {"$sum": {"$cond": [month_cond, _referral_sign_expr(), 0]}},
                "settled_total": {
                    "$sum": {"$cond": [{"$eq": ["$event", "referral_settled"]}, 1, 0]}
                },
                "revoked_total": {
                    "$sum": {"$cond": [{"$eq": ["$event", "referral_revoked"]}, 1, 0]}
                },
            }
        },
    ]
    results = list(db.referral_events.aggregate(pipeline))

    scanned = 0
    updated = 0
    unchanged = 0
    errors = 0
    negative_rows = 0
    negative_examples_logged = 0
    weekly_sum = 0
    monthly_sum = 0
    total_sum = 0
    min_weekly = 0
    min_monthly = 0
    min_total = 0
    weekly_negative_count = 0
    monthly_negative_count = 0
    total_negative_count = 0
    invariant_examples_logged = 0
    top_affected_inviters: list[dict] = []
    MAX_NEGATIVE_EXAMPLES = 20

    if results:
        updates = []
        for row in results:
            scanned += 1
            uid = row.get("_id")
            if uid is None:
                errors += 1
                logger.warning(
                    "[SCHED][REFERRAL_SNAPSHOT][MALFORMED] row=%r",
                    row,
                )
                continue
            total_referrals = int(row.get("total_referrals", 0))
            weekly_referrals = int(row.get("weekly_referrals", 0))
            monthly_referrals = int(row.get("monthly_referrals", 0))
            settled_total = int(row.get("settled_total", 0))
            revoked_total = int(row.get("revoked_total", 0))

            weekly_sum += weekly_referrals
            monthly_sum += monthly_referrals
            total_sum += total_referrals
            min_weekly = min(min_weekly, weekly_referrals)
            min_monthly = min(min_monthly, monthly_referrals)
            min_total = min(min_total, total_referrals)

            # Final invariant guard: a referral count must never be negative.
            # The not-invalidated filter above should already prevent this,
            # but corrupted legacy events (revocations without a prior valid
            # settlement, not yet repaired by repair_referral_ledger.py) can
            # still net negative here. Clamp what is written, and report the
            # clamp with the underlying settled/revoked counts rather than
            # silently swallowing it.
            clamped_weekly = max(0, weekly_referrals)
            clamped_monthly = max(0, monthly_referrals)
            clamped_total = max(0, total_referrals)

            if weekly_referrals < 0 or monthly_referrals < 0 or total_referrals < 0:
                negative_rows += 1
                if weekly_referrals < 0:
                    weekly_negative_count += 1
                if monthly_referrals < 0:
                    monthly_negative_count += 1
                if total_referrals < 0:
                    total_negative_count += 1
                top_affected_inviters.append(
                    {
                        "inviter_id": uid,
                        "weekly": weekly_referrals,
                        "monthly": monthly_referrals,
                        "total": total_referrals,
                        "settled_total": settled_total,
                        "revoked_total": revoked_total,
                    }
                )
                if negative_examples_logged < MAX_NEGATIVE_EXAMPLES:
                    negative_examples_logged += 1
                    logger.warning(
                        "[SCHED][REFERRAL_SNAPSHOT][NEGATIVE] uid=%s weekly=%s monthly=%s total=%s "
                        "settled_total=%s revoked_total=%s clamped_to=weekly=%s,monthly=%s,total=%s",
                        uid,
                        weekly_referrals,
                        monthly_referrals,
                        total_referrals,
                        settled_total,
                        revoked_total,
                        clamped_weekly,
                        clamped_monthly,
                        clamped_total,
                    )
                # A distinct, explicitly-named invariant-violation log (in
                # addition to the [NEGATIVE] line above) with one line per
                # negative window, so an operator grepping for this exact
                # tag sees the full settled/revoked/raw_net context per
                # window without having to parse the combined line. Clamping
                # to zero is a safety guard, not a fix -- this line exists so
                # the underlying corruption is reported, never hidden.
                if invariant_examples_logged < MAX_NEGATIVE_EXAMPLES:
                    for window, raw_net in (
                        ("weekly", weekly_referrals),
                        ("monthly", monthly_referrals),
                        ("total", total_referrals),
                    ):
                        if raw_net >= 0:
                            continue
                        invariant_examples_logged += 1
                        logger.warning(
                            "[REFERRAL][LEDGER_INVARIANT_VIOLATION] inviter=%s window=%s "
                            "settled=%s revoked=%s raw_net=%s stored_net=0",
                            uid,
                            window,
                            settled_total,
                            revoked_total,
                            raw_net,
                        )

            updates.append(
                UpdateOne(
                    {"user_id": uid},
                    {
                        "$set": {
                            "weekly_referrals_next": clamped_weekly,
                            "monthly_referrals_next": clamped_monthly,
                            "total_referrals_next": clamped_total,
                        }
                    },
                    upsert=True,
                )
            )
        if updates:
            try:
                db.users.bulk_write(updates, ordered=False)
                updated = len(updates)
            except Exception:
                logger.exception(
                    "[SCHED][REFERRAL_SNAPSHOT][WRITE_FAILED] batch_size=%s aborting_publish=true",
                    len(updates),
                )
                raise

    publish_result = db.users.update_many(
        {},
        [
            {
                "$set": {
                    "weekly_referrals": "$weekly_referrals_next",
                    "monthly_referrals": "$monthly_referrals_next",
                    "total_referrals": "$total_referrals_next",
                    "snapshot_published_at": now_utc_ts,
                    "snapshot_updated_at": now_utc_ts,
                }
            }
        ],
    )
    version_result = db.users.update_many({}, {"$inc": {"snapshot_version": 1}})
    logger.info(
        "[SNAPSHOT] publish_done users=%s version_inc=1",
        publish_result.modified_count,
    )
    _write_snapshot_heartbeat("referral", now_utc_ts)

    duration_ms = int((time.perf_counter() - start_perf) * 1000)
    logger.info(
        "[SCHED][REFERRAL_SNAPSHOT][DONE] scanned=%s updated=%s unchanged=%s errors=%s "
        "negative_rows=%s negative_examples_logged=%s weekly_sum=%s monthly_sum=%s total_sum=%s "
        "min_weekly=%s min_monthly=%s min_total=%s duration_ms=%s version_inc=%s",
        scanned,
        updated,
        unchanged,
        errors,
        negative_rows,
        negative_examples_logged,
        weekly_sum,
        monthly_sum,
        total_sum,
        min_weekly,
        min_monthly,
        min_total,
        duration_ms,
        version_result.modified_count,
    )

    top_affected_inviters.sort(key=lambda r: abs(r["total"]) + abs(r["weekly"]) + abs(r["monthly"]), reverse=True)

    return {
        "users_scanned": scanned,
        "users_modified": updated,
        "negative_raw_totals_detected": weekly_negative_count + monthly_negative_count + total_negative_count,
        "negative_users_clamped": negative_rows,
        "weekly_negative_count": weekly_negative_count,
        "monthly_negative_count": monthly_negative_count,
        "total_negative_count": total_negative_count,
        "top_affected_inviters": top_affected_inviters[:20],
        "duration_seconds": round((time.perf_counter() - start_perf), 3),
    }

def _recover_stale_processing(now_utc_ts: datetime) -> int:
    cutoff = now_utc_ts - PROCESSING_TIMEOUT
    result = db.pending_referrals.update_many(
        {
            "status": "processing",
            "$or": [
                {"processing_at_utc": {"$lte": cutoff}},
                {"processing_at": {"$lte": cutoff}},
            ],
        },
        {
            "$set": {"status": "pending", "next_retry_at_utc": now_utc_ts + RETRY_RELEASE_DELAY},
            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
            "$inc": {"retry_count": 1},
        },
    )
    return result.modified_count
def reconcile_drop_statuses(ref_now: datetime | None = None) -> dict:
    """
    Reconcile drop status transitions by time:
      - upcoming/live/active -> active when now is within [startsAt, endsAt)
      - any non-expired -> expired when endsAt <= now
    """
    now = ref_now or datetime.now(timezone.utc)

    activated = db.drops.update_many(
        {
            "status": {"$in": ["upcoming", "live", "active"]},
            "startsAt": {"$lte": now},
            "endsAt": {"$gt": now},
        },
        {"$set": {"status": "active"}},
    ).modified_count

    expired = db.drops.update_many(
        {
            "status": {"$ne": "expired"},
            "endsAt": {"$lte": now},
        },
        {"$set": {"status": "expired"}},
    ).modified_count

    if activated or expired:
        logger.info(
            "[DROP_STATUS] reconciled activated=%s expired=%s now=%s",
            activated,
            expired,
            now.isoformat(),
        )
    return {"activated": int(activated), "expired": int(expired)}


def sweep_expired_drops():
    """
    Backward-compatible wrapper.
    """
    reconcile_drop_statuses()

def archive_weekly_leaderboard():
    """
    Snapshots weekly leaderboards and resets weekly counters.
    Trigger this at Monday 00:00 KL (schedule in main.py).
    """
    now_utc = datetime.now(timezone.utc)
    now_kl = now_utc.astimezone(KL_TZ)
    week_key = now_kl.strftime("%Y-%W")  # e.g., "2025-42"

    checkin_list = list(db.users.find({}, {"_id": 0, "username": 1, "weekly_xp": 1}))
    referral_list = list(db.users.find({}, {"_id": 0, "username": 1, "weekly_referrals": 1}))

    snapshot = {
        "week": week_key,
        "timestampUtc": now_utc,
        "timestampKl": now_kl.isoformat(),
        "checkin": checkin_list,
        "referral": referral_list,
    }

    db.leaderboard_weekly.insert_one(snapshot)
    logger.info(
        "[LEADERBOARD] weekly_snapshot week=%s checkin_count=%s referral_count=%s",
        week_key,
        len(checkin_list),
        len(referral_list),
    )    
    db.users.update_many({}, {"$set": {"weekly_xp": 0, "weekly_referrals": 0}})
    logger.info("[RESET][WEEKLY] weekly_xp_ref_reset ok")



def _has_severe_deny(invitee_user_id: int) -> bool:
    reason = db.pending_referrals.find_one(
        {"invitee_user_id": int(invitee_user_id), "revoked_reason": {"$in": ["blocked", "abuse", "deny_severe"]}},
        {"_id": 1},
    )
    return bool(reason)


def _resolve_referrer_id(invitee_user_id: int) -> int | None:
    row = db.pending_referrals.find_one(
        {"invitee_user_id": int(invitee_user_id), "status": "awarded"},
        {"inviter_user_id": 1},
    )
    inviter = (row or {}).get("inviter_user_id")
    return int(inviter) if inviter is not None else None


def _simulate_pool_for_gate_day(gate_day: int) -> str:
    if gate_day == 3:
        return "T1"
    if gate_day == 7:
        return "T2"
    if gate_day == 15:
        return "T3"
    return "T4"


def _simulated_ledger_dedup_key(*, inviter_user_id: int, invitee_user_id: int, gate_day: int, tier: str) -> str:
    return f"SIM_GATE:{int(inviter_user_id)}:{int(invitee_user_id)}:{int(gate_day)}:{tier}"


def _simulated_ledger_natural_key(
    *, inviter_user_id: int, invitee_user_id: int, gate_day: int, tier: str, cohort_start_utc: datetime
) -> dict:
    return {
        "simulate": True,
        "ledger_type": "AFFILIATE_SIMULATION",
        "user_id": int(inviter_user_id),
        "invitee_user_id": int(invitee_user_id),
        "gate_day": int(gate_day),
        "tier": tier,
        "created_at": cohort_start_utc,
    }


def _derive_abuse_flags_for_invitee(invitee_user_id: int, now_utc_ts: datetime) -> list[str]:
    flags = []
    try:
        deny_count = db.referral_audit.count_documents(
            {
                "invitee_user_id": int(invitee_user_id),
                "created_at": {"$gte": now_utc_ts - timedelta(days=7), "$lt": now_utc_ts},
                "reason": {"$in": ["deny", "deny_severe", "blocked", "abuse"]},
            }
        )
        if int(deny_count or 0) > 0:
            flags.append("referral_audit_deny_7d")
    except Exception:
        pass

    try:
        cooldown_key = f"cooldown:uid:{int(invitee_user_id)}"
        if db.claim_rate_limits.find_one({"key": cooldown_key}, {"_id": 1}):
            flags.append("cooldown_uid")
    except Exception:
        pass

    try:
        kill_key = f"kill:uid:{int(invitee_user_id)}"
        if db.claim_rate_limits.find_one({"key": kill_key}, {"_id": 1}):
            flags.append("kill_uid")
    except Exception:
        pass

    return flags


def evaluate_affiliate_simulated_ledgers(batch_limit: int = 500) -> int:
    if str(os.getenv("AFFILIATE_SIMULATE", "0")).strip() != "1":
        return 0

    now_utc_ts = now_utc()
    now_kl_date = now_utc_ts.astimezone(KL_TZ).date()
    created_or_updated = 0
    final_statuses = {"ISSUED", "OUT_OF_STOCK", "REJECTED"}

    rows = db.pending_referrals.find(
        {"status": "awarded", "invitee_user_id": {"$exists": True}, "inviter_user_id": {"$exists": True}},
        {"invitee_user_id": 1, "inviter_user_id": 1},
    ).limit(batch_limit)

    for row in rows:
        invitee_user_id = row.get("invitee_user_id")
        inviter_user_id = row.get("inviter_user_id")
        if not isinstance(invitee_user_id, int) or not isinstance(inviter_user_id, int):
            continue

        user_doc = db.users.find_one({"user_id": invitee_user_id}, {"joined_main_at": 1, "total_xp": 1, "monthly_xp": 1}) or {}
        joined_main_at = _coerce_utc(user_doc.get("joined_main_at"))
        if not joined_main_at:
            continue
        age_days = (now_kl_date - joined_main_at.astimezone(KL_TZ).date()).days
        for gate_day in (3, 7, 15, 30):
            if age_days < gate_day:
                continue

            tier = _simulate_pool_for_gate_day(gate_day)

            try:
                status = _get_chat_member_status(invitee_user_id)
                still_in_group = status in {"member", "administrator", "creator"}
            except Exception:
                still_in_group = False

            dedup_key = _simulated_ledger_dedup_key(
                inviter_user_id=inviter_user_id,
                invitee_user_id=invitee_user_id,
                gate_day=gate_day,
                tier=tier,
            )
            existing = db.affiliate_ledger.find_one({"dedup_key": dedup_key}, {"_id": 1, "status": 1})
            if existing and existing.get("status") in final_statuses:
                continue

            try:
                res = db.affiliate_ledger.update_one(
                    {"dedup_key": dedup_key},
                    {
                        "$set": {
                            "evaluated_at_utc": now_utc_ts,
                            "xp_total": user_doc.get("total_xp"),
                            "monthly_xp": user_doc.get("monthly_xp"),
                            "still_in_group": bool(still_in_group),
                            "risk_flags": [],
                            "abuse_flags": _derive_abuse_flags_for_invitee(invitee_user_id, now_utc_ts),
                            "would_issue_pool": tier,
                            "year_month": None,
                            "updated_at": now_utc_ts,
                            "status": "SIMULATED_PENDING",
                            "simulate": True,
                        },
                        "$setOnInsert": {
                            "created_at": joined_main_at,
                            "pool_id": tier,
                            "tier": tier,
                            "user_id": int(inviter_user_id),
                            "invitee_user_id": int(invitee_user_id),
                            "gate_day": int(gate_day),
                            "ledger_type": "AFFILIATE_SIMULATION",
                            "dedup_key": dedup_key,
                            "voucher_code": None,
                            "first_seen_at": now_utc_ts,
                        },
                    },
                    upsert=True,
                )
            except DuplicateKeyError:
                logger.info("[SCHED][AFF_SIM] action=dedup_exists dedup_key=%s", dedup_key)
                continue
            if res.upserted_id is not None or int(res.modified_count or 0) > 0:
                created_or_updated += 1

    if created_or_updated:
        logger.info("[SCHED][AFF_SIM] action=processed_ledgers count=%s", created_or_updated)
    return created_or_updated


def confirm_qualified_invitees(batch_limit: int = 200) -> int:
    # Qualification is handled inline by mark_invitee_qualified(...) inside
    # settle_pending_referrals; keep this no-op for backward call compatibility.
    logger.info("[SCHED][QUALIFIED] skipped reason=mark_invitee_qualified_is_source_of_truth")
    return 0

def current_month_window_utc(now_utc_ts: datetime | None = None) -> tuple[datetime, datetime]:
    """UTC [start, end) bounds of the current reward month (Asia/Kuala_Lumpur
    calendar month) -- the same boundary current_month_qualified_referral_count()
    and REFERRAL_CONGRATS_TIERS evaluation use, exposed for callers that need
    a datetime range instead of a month_key string (e.g. windowing a
    collection that has no month_key field, like pending_referrals)."""
    return _month_window_utc(now_utc_ts or now_utc())


def current_month_qualified_referral_count(inviter_user_id: int, now_utc_ts: datetime | None = None) -> int:
    """Net qualified (settled minus revoked) referral_events for
    inviter_user_id within the current reward month (Asia/Kuala_Lumpur
    calendar month, matching REFERRAL_CONGRATS_TIERS' own boundary).

    This is the canonical month-scoped count backing the reward tiers --
    any other surface that shows "progress toward the next reward tier"
    (e.g. the Creator Share Centre) must call this instead of recomputing
    its own monthly window, so it can never drift from what
    maybe_shout_referral_congrats actually evaluates.
    """
    now_utc_ts = now_utc_ts or now_utc()
    month_key = _month_start_kl(now_utc_ts).date().isoformat()
    settled = db.referral_events.count_documents({
        "inviter_id": inviter_user_id,
        "event": "referral_settled",
        "month_key": month_key,
    })
    revoked = db.referral_events.count_documents(
        with_not_invalidated(
            {
                "inviter_id": inviter_user_id,
                "event": "referral_revoked",
                "month_key": month_key,
            }
        )
    )
    return max(0, settled - revoked)


def maybe_shout_referral_congrats(inviter_user_id: int, now_utc_ts: datetime) -> None:
    from html import escape as html_escape
    month_key = _month_start_kl(now_utc_ts).date().isoformat()
    monthly_count = current_month_qualified_referral_count(inviter_user_id, now_utc_ts)

    hit_tier = None
    for threshold, voucher in REFERRAL_CONGRATS_TIERS:
        if monthly_count >= threshold > (monthly_count - 1):
            hit_tier = (threshold, voucher)
            break

    if not hit_tier:
        return

    threshold, voucher = hit_tier

    # Check dedup before sending but do NOT record yet — record only after confirmed send
    already_sent = db.referral_tier_congrats.find_one(
        {"user_id": inviter_user_id, "month_key": month_key, "tier": threshold},
        {"_id": 1},
    )
    if already_sent:
        return

    tier_idx = next(i for i, (t, _) in enumerate(REFERRAL_CONGRATS_TIERS) if t == threshold)
    is_last = tier_idx == len(REFERRAL_CONGRATS_TIERS) - 1
    if not is_last:
        next_tier, next_voucher = REFERRAL_CONGRATS_TIERS[tier_idx + 1]
        tail = f"Next: {next_tier} refs = ${next_voucher}! 💪"
    else:
        tail = "Absolute legend! 🏆"

    user_doc = db.users.find_one({"user_id": inviter_user_id}, {"user_id": 1, "username": 1, "first_name": 1})
    if user_doc and user_doc.get("username"):
        mention = f'<a href="tg://user?id={inviter_user_id}">@{html_escape(user_doc["username"])}</a>'
    elif user_doc and user_doc.get("first_name"):
        mention = f'<a href="tg://user?id={inviter_user_id}">{html_escape(user_doc["first_name"])}</a>'
    else:
        mention = f'<a href="tg://user?id={inviter_user_id}">user</a>'

    text = (
        f"🎉 {mention} just hit <b>{threshold} valid referrals</b> this month "
        f"— <b>${voucher} voucher</b> unlocked! {tail}"
    )
    try:
        resp = requests.post(
            f"{API_BASE}/sendMessage",
            json={"chat_id": AFFILIATE_CONGRATS_CHANNEL_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if not resp.ok:
            logger.warning(
                "[REFERRAL][CONGRATS] send_failed inviter=%s tier=%s status=%s body=%s",
                inviter_user_id, threshold, resp.status_code, resp.text[:200],
            )
            return
    except Exception:
        logger.exception("[REFERRAL][CONGRATS] send_error inviter=%s tier=%s", inviter_user_id, threshold)
        return

    # Record dedup only after confirmed send to keep the tier retryable on failure.
    # username/first_name/reward_amount are stored alongside the existing
    # dedup key purely for display (e.g. the Money Room "Recent Win" card) --
    # they don't change the dedup semantics, which remain the unique index
    # on (user_id, month_key, tier).
    display_name = (user_doc or {}).get("username") or (user_doc or {}).get("first_name")
    try:
        db.referral_tier_congrats.insert_one({
            "user_id": inviter_user_id,
            "month_key": month_key,
            "tier": threshold,
            "sent_at": now_utc_ts,
            "username": (user_doc or {}).get("username"),
            "display_name": display_name,
            "qualified_referrals": threshold,
            "reward_amount": voucher,
        })
    except DuplicateKeyError:
        pass




def _maybe_send_referral_qualified_dm(
    inviter_user_id: int | None,
    invitee_user_id: int | None,
    invitee_username: str | None = None,
) -> None:
    if not inviter_user_id or not invitee_user_id:
        return
    dedupe_key = f"ref_qualified:{int(inviter_user_id)}:{int(invitee_user_id)}"
    now_ts = now_utc()
    pref_allowed = pm_allowed(
        int(inviter_user_id),
        "referral_updates",
        default=True,
        users_collection=db.users,
        logger=logger,
    )
    set_on_insert = {
        "key": dedupe_key,
        "type": "ref_qualified",
        "inviter_user_id": int(inviter_user_id),
        "invitee_user_id": int(invitee_user_id),
        "invitee_username": invitee_username,
        "created_at": now_ts,
    }
    if not pref_allowed:
        set_on_insert["suppressed"] = True
        set_on_insert["suppressed_reason"] = "pm_preference"
    result = db.referral_notifications.update_one(
        {"key": dedupe_key},
        {
            "$setOnInsert": set_on_insert
        },
        upsert=True,
    )
    if not getattr(result, "upserted_id", None):
        return
    if not pref_allowed:
        logger.info(
            "[PM_PREF][SUPPRESSED] uid=%s key=%s type=%s",
            inviter_user_id,
            "referral_updates",
            "ref_qualified",
        )
        return
    if invitee_username:
        text = (
            f"🎉 @{invitee_username} qualified from your invite!\n"
            "+60 XP has been credited."
        )
    else:
        text = "🎉 Your referral qualified!\n+60 XP has been credited."
    try:
        resp = requests.post(
            f"{API_BASE}/sendMessage",
            json={"chat_id": int(inviter_user_id), "text": text},
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
        if not payload.get("ok", True):
            raise RuntimeError(payload.get("description") or "telegram_not_ok")
    except Exception:
        logger.exception(
            "[REFERRAL_QUALIFIED_DM_FAILED] inviter=%s invitee=%s",
            inviter_user_id,
            invitee_user_id,
        )

def _resolve_pending_destination(pending: dict) -> tuple[int, str, str]:
    """Resolve (destination_chat_id, destination_type, resolution_source)
    for a pending_referrals row, in order of authority:

      1. the row's own explicit destination_type (schema_version >= 2 rows
         always have this — it is never guessed at)
      2. destination_chat_id/group_id matched against the known chat ids
      3. the row's stored invite_link resolved through invite_link_map,
         for legacy rows whose chat id alone doesn't disambiguate (e.g. a
         REFERRAL_DESTINATION_CHAT_ID override that has since changed)
      4. a safe legacy fallback of community_group — a row with no
         evidence pointing at official_channel is never guessed into the
         no-checkin-required rule.
    """
    destination_chat_id = (
        pending.get("destination_chat_id")
        or pending.get("group_id")
        or GROUP_ID
    )

    explicit_type = pending.get("destination_type")
    if explicit_type in VALID_DESTINATION_TYPES:
        return destination_chat_id, explicit_type, "explicit_field"

    if destination_chat_id == OFFICIAL_CHANNEL_ID:
        return destination_chat_id, OFFICIAL_CHANNEL, "chat_id_match"
    if destination_chat_id == GROUP_ID:
        return destination_chat_id, COMMUNITY_GROUP, "chat_id_match"

    invite_link = pending.get("invite_link")
    if invite_link:
        mapping = db.invite_link_map.find_one(
            {"invite_link": invite_link}, {"destination_type": 1, "chat_id": 1}
        )
        mapped_type = (mapping or {}).get("destination_type")
        if mapped_type in VALID_DESTINATION_TYPES:
            return (mapping.get("chat_id") or destination_chat_id, mapped_type, "invite_link_map")

    return destination_chat_id, COMMUNITY_GROUP, "legacy_fallback"


def settle_pending_referrals(batch_limit: int = 200) -> None:
    now_utc_ts = now_utc()
    cutoff = now_utc_ts - timedelta(hours=_referral_hold_hours())
    recovered = _recover_stale_processing(now_utc_ts)
    if recovered:
        logger.info("[SCHED][REFERRAL] recovered_stale_processing=%s", recovered)

    scanned = 0
    awarded = 0
    revoked = 0
    
    while scanned < batch_limit:
        pending = db.pending_referrals.find_one_and_update(
            {
                "status": {"$in": ["pending", "pending_channel"]},
                "created_at_utc": {"$lte": cutoff},
                "$or": [
                    {"next_retry_at_utc": {"$exists": False}},
                    {"next_retry_at_utc": {"$lte": now_utc_ts}},
                ],
            },
            {
                "$set": {
                    "status": "processing",
                    "processing_by": INSTANCE_ID,
                    "processing_at_utc": now_utc_ts,
                }
            },
            sort=[("created_at_utc", 1)],
            return_document=ReturnDocument.BEFORE,
        )
        if not pending:
            break
        scanned += 1
        pending_id = pending.get("_id")
        invitee_user_id = pending.get("invitee_user_id")
        inviter_user_id = pending.get("inviter_user_id")
        step = "validate"
        retry_count = pending.get("retry_count", 0) or 0
        destination_chat_id, destination_type, destination_resolution_source = (
            _resolve_pending_destination(pending)
        )
        logger.info(
            "[SCHED][REFERRAL][DESTINATION] pending_id=%s destination_type_resolved=%s destination_resolution_source=%s",
            pending_id,
            destination_type,
            destination_resolution_source,
        )
        try:
            if not invitee_user_id or not inviter_user_id:
                step = "validate_ids"
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "invalid_ids",
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                referral_invitee_lock.release(
                    db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                )                
                revoked += 1
                continue
            if invitee_user_id == inviter_user_id:
                step = "self_invite"
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "self_invite",
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                referral_invitee_lock.release(
                    db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                )                
                revoked += 1
                continue

            step = "check_channel"
            try:
                # destination_chat_id is already the correctly resolved chat
                # for this row's destination_type (community_group -> GROUP_ID,
                # official_channel -> OFFICIAL_CHANNEL_ID / override) — always
                # pass it through rather than defaulting to OFFICIAL_CHANNEL_ID
                # for non-channel rows.
                status = _get_official_channel_member_status(invitee_user_id, destination_chat_id)
            except ReferralRetryableError as exc:
                retry_after = exc.retry_after
                backoff = (
                    int(retry_after)
                    if retry_after is not None
                    else _compute_backoff_seconds(retry_count, base=5, cap=300)
                )
                logger.warning(
                    "[SCHED][REFERRAL] retryable=telegram_rate_limited inviter=%s invitee=%s retry_after=%s",
                    inviter_user_id,
                    invitee_user_id,
                    backoff,
                )
                _release_for_retry(pending_id, now_utc_ts, backoff, "telegram_429")
                continue
            except ReferralTelegramError as exc:
                # Neither "config" nor "user" is a definitive Telegram
                # membership verdict -- an unresolved check is operational
                # uncertainty, never proof the invitee left or was never
                # subscribed, so this can only ever retry (bounded) or land
                # on the terminal operational status="error" state. It must
                # never write status="revoked" -- no XP, no referral_settled,
                # no referral_revoked ledger event either way.
                is_config = exc.kind == "config"
                max_retries = MAX_TELEGRAM_CONFIG_RETRIES if is_config else MAX_TELEGRAM_USER_RETRIES
                backoff = (
                    TELEGRAM_CONFIG_ERROR_BACKOFF_SEC
                    if is_config
                    else _compute_backoff_seconds(retry_count, base=30, cap=300)
                )
                attempt = retry_count + 1
                decision = "error_terminal" if retry_count >= max_retries else "retry_bounded"
                logger.error(
                    "[REFERRAL][MEMBERSHIP_CHECK_ERROR] pending_id=%s invitee=%s inviter=%s "
                    "destination_type=%s destination_chat_id=%s http_status=%s tg_error_code=%s "
                    "tg_description=%s error_kind=%s attempt=%s decision=%s",
                    pending_id,
                    invitee_user_id,
                    inviter_user_id,
                    destination_type,
                    destination_chat_id,
                    exc.status_code,
                    exc.error_code,
                    exc.description,
                    exc.kind,
                    attempt,
                    decision,
                )
                if decision == "error_terminal":
                    db.pending_referrals.update_one(
                        {"_id": pending_id},
                        {
                            "$set": {
                                "status": "error",
                                "error_reason": "membership_check_unresolvable",
                                "tg_error_code": exc.error_code,
                                "tg_description": exc.description,
                                "membership_check_attempts": attempt,
                                "membership_last_checked_at": now_utc_ts,
                                "destination_type": destination_type,
                                "destination_chat_id": destination_chat_id,
                                "error_at_utc": now_utc_ts,
                            },
                            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                        },
                    )
                    referral_invitee_lock.release(
                        db, invitee_user_id=invitee_user_id, status="error", now_utc_ts=now_utc_ts
                    )
                    logger.error(
                        "[REFERRAL][OPERATIONAL_ERROR] pending_id=%s reason=membership_check_unresolvable "
                        "attempts=%s destination_type=%s destination_chat_id=%s",
                        pending_id,
                        attempt,
                        destination_type,
                        destination_chat_id,
                    )
                else:
                    _release_for_retry(
                        pending_id,
                        now_utc_ts,
                        backoff,
                        "telegram_config_error" if is_config else "telegram_bad_request",
                    )
                continue
            except RequestException as exc:
                backoff = _compute_backoff_seconds(retry_count, base=30, cap=120)
                logger.warning(
                    "[SCHED][REFERRAL] retryable=telegram_request_failed inviter=%s invitee=%s err=%s",
                    inviter_user_id,
                    invitee_user_id,
                    exc,
                )
                _release_for_retry(pending_id, now_utc_ts, backoff, "telegram_request_failed")
                continue
            if status not in {"member", "administrator", "creator"}:
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "not_in_official_channel",
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                referral_invitee_lock.release(
                    db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                )                
                revoked += 1
                logger.info(
                    "[SCHED][REFERRAL] revoked inviter=%s invitee=%s reason=not_in_official_channel",
                    inviter_user_id,
                    invitee_user_id,
                )
                continue

            step = "check_new_user"
            invitee_doc = db.users.find_one(
                {"user_id": invitee_user_id},
                {
                    "created_at": 1,
                    "joined_main_at": 1,
                    "first_checkin_at": 1,
                    "last_visible_at": 1,
                    "left_official_channel_at": 1,
                },
            )
            if not invitee_doc:
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "revoked",
                            "revoked_reason": "no_user_doc",
                            "revoked_at": now_utc_ts,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                referral_invitee_lock.release(
                    db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                )                
                revoked += 1
                continue
            join_seen = pending.get("created_at_utc")
            join_seen_utc = _coerce_utc(join_seen)

            if destination_type == OFFICIAL_CHANNEL:
                # Channel-origin referrals: the canonical join-time is the
                # attribution event itself (referral_join_seen_at_utc, falling
                # back to the pending row's own created_at_utc). Do NOT require
                # users.joined_main_at (channel joins never set it — that field
                # is group-only) and do NOT reject merely because the invitee
                # already has a users doc: existing bot users / existing
                # chatroom users can still be genuinely new channel subscribers.
                referral_join_seen_at_utc = _coerce_utc(pending.get("referral_join_seen_at_utc"))
                reference_time = referral_join_seen_at_utc or join_seen_utc
                if not reference_time:
                    db.pending_referrals.update_one(
                        {"_id": pending_id},
                        {
                            "$set": {
                                "status": "revoked",
                                "revoked_reason": "missing_join_time",
                                "revoked_at": now_utc_ts,
                            },
                            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                        },
                    )
                    referral_invitee_lock.release(
                        db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                    )
                    revoked += 1
                    continue
            else:
                joined_main_at = _coerce_utc(invitee_doc.get("joined_main_at"))
                created_at = _coerce_utc(invitee_doc.get("created_at"))
                reference_time = joined_main_at or created_at
                if not reference_time or not join_seen_utc:
                    db.pending_referrals.update_one(
                        {"_id": pending_id},
                        {
                            "$set": {
                                "status": "revoked",
                                "revoked_reason": "missing_join_time",
                                "revoked_at": now_utc_ts,
                            },
                            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                        },
                    )
                    referral_invitee_lock.release(
                        db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                    )
                    revoked += 1
                    continue
                if reference_time < (join_seen_utc - timedelta(minutes=10)):
                    db.pending_referrals.update_one(
                        {"_id": pending_id},
                        {
                            "$set": {
                                "status": "revoked",
                                "revoked_reason": "already_in_db",
                                "revoked_at": now_utc_ts,
                            },
                            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                        },
                    )
                    referral_invitee_lock.release(
                        db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                    )
                    revoked += 1
                    continue

            step = "check_qualification"
            # official_channel referrals qualify on retained channel
            # subscription through the hold period (membership was already
            # re-verified fresh, above, at settlement time) — a channel
            # subscriber may legitimately never start the bot, open the Mini
            # App, or check in, so first_checkin/engagement scoring must not
            # gate settlement for this destination. community_group referrals
            # keep the existing engagement/check-in requirement.
            #
            # "Retained through the hold" means continuously subscribed, not
            # just subscribed-right-now: main.py's member_update_handler
            # already stamps left_official_channel_at on every channel-leave
            # event (independent of this referral), so a leave-then-rejoin
            # inside the hold window would otherwise pass the fresh
            # getChatMember check above and settle despite the gap.
            qualification_metadata = {}
            if destination_type == OFFICIAL_CHANNEL:
                # Scoped to the actual hold window (join .. join+hold_hours),
                # not "any leave before this settlement run" — a leave that
                # happens long after the hold already completed (e.g. right
                # before a delayed retry, or when a historical row is
                # reopened for re-settlement much later) is unrelated to
                # whether this referral was retained through its own hold,
                # and is already covered by the fresh subscription check
                # above if the invitee is still gone at settlement time.
                left_at = _coerce_utc((invitee_doc or {}).get("left_official_channel_at"))
                hold_end = (
                    reference_time + timedelta(hours=_referral_hold_hours())
                    if reference_time is not None
                    else None
                )
                left_during_hold = bool(
                    left_at is not None
                    and reference_time is not None
                    and hold_end is not None
                    and reference_time <= left_at <= hold_end
                )
                if left_during_hold:
                    db.pending_referrals.update_one(
                        {"_id": pending_id},
                        {
                            "$set": {
                                "status": "revoked",
                                "revoked_reason": "left_before_hold",
                                "revoked_at": now_utc_ts,
                            },
                            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                        },
                    )
                    referral_invitee_lock.release(
                        db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                    )
                    revoked += 1
                    logger.info(
                        "[REFERRAL][QUALIFY_RULE] pending_id=%s invitee=%s inviter=%s destination_type=%s "
                        "rule=official_channel_retained hold_elapsed=true subscription_status=%s decision=revoke reason=left_before_hold",
                        pending_id,
                        invitee_user_id,
                        inviter_user_id,
                        destination_type,
                        status,
                    )
                    continue
                qualification_metadata = {
                    "qualification_rule": "official_channel_retained",
                    "subscription_status": status,
                    "subscription_checked_at": now_utc_ts,
                    "hold_hours": _referral_hold_hours(),
                }
                logger.info(
                    "[REFERRAL][QUALIFY_RULE] pending_id=%s invitee=%s inviter=%s destination_type=%s "
                    "rule=official_channel_retained hold_elapsed=true subscription_status=%s decision=qualify reason=",
                    pending_id,
                    invitee_user_id,
                    inviter_user_id,
                    destination_type,
                    status,
                )
            else:
                step = "check_engagement"
                engagement = evaluate_referral_engagement(
                    invitee_user_id=invitee_user_id,
                    invitee_doc=invitee_doc,
                    window_start=join_seen_utc,
                    window_end=now_utc_ts,
                    db_ref=db,
                )
                if not engagement.get("qualified"):
                    db.pending_referrals.update_one(
                        {"_id": pending_id},
                        {
                            "$set": {
                                "status": "revoked",
                                "revoked_reason": "insufficient_engagement",
                                "qualification_failure_reason": "insufficient_engagement",
                                "engagement_score": int(engagement.get("score", 0) or 0),
                                "engagement_signals": engagement.get("signals") or {},
                                "engagement_points": engagement.get("points") or {},
                                "engagement_window_start_utc": engagement.get("window_start"),
                                "engagement_window_end_utc": engagement.get("window_end"),
                                "engagement_evaluated_at_utc": now_utc_ts,
                                "revoked_at": now_utc_ts,
                            },
                            "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                        },
                    )
                    referral_invitee_lock.release(
                        db, invitee_user_id=invitee_user_id, status="revoked", now_utc_ts=now_utc_ts
                    )
                    revoked += 1
                    logger.info(
                        "[SCHED][REFERRAL][ENGAGEMENT] revoked inviter=%s invitee=%s reason=insufficient_engagement score=%s signals=%s points=%s window_start=%s window_end=%s",
                        inviter_user_id,
                        invitee_user_id,
                        engagement.get("score"),
                        engagement.get("signals"),
                        engagement.get("points"),
                        engagement.get("window_start"),
                        engagement.get("window_end"),
                    )
                    logger.info(
                        "[REFERRAL][QUALIFY_RULE] pending_id=%s invitee=%s inviter=%s destination_type=%s "
                        "rule=engagement_score hold_elapsed=true subscription_status= decision=revoke reason=insufficient_engagement",
                        pending_id,
                        invitee_user_id,
                        inviter_user_id,
                        destination_type,
                    )
                    continue
                logger.info(
                    "[REFERRAL][QUALIFY_RULE] pending_id=%s invitee=%s inviter=%s destination_type=%s "
                    "rule=engagement_score hold_elapsed=true subscription_status= decision=qualify reason=",
                    pending_id,
                    invitee_user_id,
                    inviter_user_id,
                    destination_type,
                )

            step = "award"
            # Award key is invitee-scoped (not group_id/chat_id-scoped) so the
            # same invitee cannot be awarded twice across a group-origin and a
            # channel-origin referral (P0-4: cross-destination duplicate XP).
            award_key = f"ref:{invitee_user_id}"

            # Guard against a pre-migration award under the legacy
            # destination-scoped key format ("ref:<group_id>:<invitee_id>"),
            # which would not collide with the new invitee-scoped award_key
            # and is not covered by referral_invitee_locks (that collection
            # did not exist before this migration, so historical invitees
            # have no lock row). Any prior award row for this invitee — old
            # key format or new — means XP was already granted; recover
            # qualification without granting XP again.
            existing_award = db.referral_award_events.find_one(
                {"invitee_user_id": invitee_user_id}, {"award_key": 1}
            )
            if existing_award:
                recovered_award_key = existing_award.get("award_key") or award_key
                mark_invitee_qualified(
                    db,
                    invitee_id=invitee_user_id,
                    referrer_id=inviter_user_id,
                    now_utc=now_utc_ts,
                )
                _maybe_send_referral_qualified_dm(
                    inviter_user_id,
                    invitee_user_id,
                    (invitee_doc or {}).get("username"),
                )
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "awarded",
                            "awarded_at_utc": now_utc_ts,
                            "awarded_at_kl": now_kl().isoformat(),
                            "award_key": recovered_award_key,
                            **qualification_metadata,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                referral_invitee_lock.release(
                    db, invitee_user_id=invitee_user_id, status="awarded", now_utc_ts=now_utc_ts
                )
                logger.info(
                    "[SCHED][REFERRAL] duplicate_award_legacy_key inviter=%s invitee=%s existing_award_key=%s",
                    inviter_user_id,
                    invitee_user_id,
                    recovered_award_key,
                )
                continue

            award_doc = {
                "award_key": award_key,
                "group_id": destination_chat_id,
                "destination_chat_id": destination_chat_id,
                "destination_type": destination_type,
                "inviter_user_id": inviter_user_id,
                "invitee_user_id": invitee_user_id,
                "pending_id": pending_id,
                "created_at_utc": now_utc_ts,
                "awarded_at_utc": now_utc_ts,
                "status": "awarded",
                **qualification_metadata,
            }
            try:
                db.referral_award_events.insert_one(award_doc)
            except DuplicateKeyError:
                mark_invitee_qualified(
                    db,
                    invitee_id=invitee_user_id,
                    referrer_id=inviter_user_id,
                    now_utc=now_utc_ts,
                )
                _maybe_send_referral_qualified_dm(
                    inviter_user_id,
                    invitee_user_id,
                    (invitee_doc or {}).get("username"),
                )
                db.pending_referrals.update_one(
                    {"_id": pending_id},
                    {
                        "$set": {
                            "status": "awarded",
                            "awarded_at_utc": now_utc_ts,
                            "awarded_at_kl": now_kl().isoformat(),
                            "award_key": award_key,
                            **qualification_metadata,
                        },
                        "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                    },
                )
                referral_invitee_lock.release(
                    db, invitee_user_id=invitee_user_id, status="awarded", now_utc_ts=now_utc_ts
                )
                logger.info(
                    "[SCHED][REFERRAL] duplicate_award inviter=%s invitee=%s award_key=%s",
                    inviter_user_id,
                    invitee_user_id,
                    award_key,
                )
                logger.info(
                    "[REFERRAL][DUP_AWARD_RECOVER_QUALIFY] inviter_id=%s referrer_id=%s invitee_id=%s pending_referral_id=%s",
                    inviter_user_id,
                    inviter_user_id,
                    invitee_user_id,
                    pending_id,
                )
                continue

            total_pipeline = [
                {
                    "$match": with_not_invalidated(
                        {
                            "inviter_id": inviter_user_id,
                            "event": {"$in": ["referral_settled", "referral_revoked"]},
                        }
                    )
                },
                {"$group": {"_id": None, "total": {"$sum": _referral_sign_expr()}}},
            ]
            total_rows = list(db.referral_events.aggregate(total_pipeline))
            # Clamp: a referral count must never be negative. Corrupted
            # legacy ledger rows (not yet repaired by
            # repair_referral_ledger.py) could otherwise depress the tier
            # calculation below the inviter's true settled count.
            current_ref_total = max(0, int((total_rows[0]["total"] if total_rows else 0) or 0))
            new_ref_total = current_ref_total + 1
            xp_added, bonus_added = calc_referral_award(new_ref_total)
            xp_granted = grant_xp(db, inviter_user_id, "referral_award", award_key, xp_added)
            ref_total = current_ref_total
            actual_xp_added = 0
            actual_bonus_added = 0

            if xp_granted:
                actual_xp_added = xp_added
                actual_bonus_added = bonus_added
            _record_referral_event(inviter_user_id, invitee_user_id, "referral_settled", now_utc_ts)
            mark_invitee_qualified(
                db,
                invitee_id=invitee_user_id,
                referrer_id=inviter_user_id,
                now_utc=now_utc_ts,
            )
            _maybe_send_referral_qualified_dm(
                inviter_user_id,
                invitee_user_id,
                (invitee_doc or {}).get("username"),
            )
            ref_total = new_ref_total
            maybe_handle_first_referral(inviter_user_id, current_ref_total, new_ref_total, now_utc_ts)
            maybe_unlock_affiliate_group(
                db=db,
                user_id=inviter_user_id,
                current_ref_total=current_ref_total,
                new_ref_total=new_ref_total,
                now_utc=now_utc_ts,
            )
            maybe_shout_referral_congrats(inviter_user_id, now_utc_ts)

            db.pending_referrals.update_one(
                {"_id": pending_id},
                {
                    "$set": {
                        "status": "awarded",
                        "awarded_at_utc": now_utc_ts,
                        "awarded_at_kl": now_kl().isoformat(),
                        "xp_added": actual_xp_added,
                        "bonus_added": actual_bonus_added,
                        "total_referrals_after": ref_total,
                        "award_key": award_key,
                        **qualification_metadata,
                    },
                    "$unset": {"processing_by": "", "processing_at_utc": "", "processing_at": ""},
                },
            )
            referral_invitee_lock.release(
                db, invitee_user_id=invitee_user_id, status="awarded", now_utc_ts=now_utc_ts
            )
            awarded += 1
            logger.info(
                "[SCHED][REFERRAL] awarded inviter=%s invitee=%s qualify_hours=%s destination_type=%s rule=%s",
                inviter_user_id,
                invitee_user_id,
                _referral_hold_hours(),
                destination_type,
                qualification_metadata.get("qualification_rule", "engagement_score"),
            )
            logger.info(
                "[SCHED][REFERRAL] award_ok inviter=%s invitee=%s ref_total=%s xp_added=%s bonus_added=%s hold_hours=%s users_counter_update_attempted=%s",
                inviter_user_id,
                invitee_user_id,
                ref_total,
                actual_xp_added,
                actual_bonus_added,
                _referral_hold_hours(),
                False,
            )
        except Exception as exc:
            logger.exception(
                "[SCHED][REFERRAL] error step=%s inviter=%s invitee=%s err=%s",
                step,
                inviter_user_id,
                invitee_user_id,
                exc,
            )
            backoff = _compute_backoff_seconds(retry_count, base=30, cap=120)
            _release_for_retry(pending_id, now_utc_ts, backoff, f"exception:{step}")

    confirm_qualified_invitees()
    logger.info(
        "[SCHED][REFERRAL] settle scanned=%s awarded=%s revoked=%s batch_limit=%s",
        scanned,
        awarded,
        revoked,
        batch_limit,        
    )
