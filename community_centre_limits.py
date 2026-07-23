"""Single source of truth for Community Centre content/poll/quiz/button limits
and Telegram-HTML sanitisation.

Every backend validator in ``community_centre.py`` imports from here, and the
values are also exposed verbatim to the frontend via
``GET /api/admin/community/limits`` so the Composer never hardcodes a Telegram
limit independently of the backend that will enforce it.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Telegram Bot API limits (Phase 1: stable text-based content only — see
# python-telegram-bot==20.8 compatibility note in the module docstring of
# community_centre.py).
# ---------------------------------------------------------------------------

TEXT_MAX_LEN = 4096
CAPTION_MAX_LEN = 1024
INTERNAL_TITLE_MAX_LEN = 200
INTERNAL_NOTES_MAX_LEN = 4000
CAMPAIGN_TAG_MAX_LEN = 40
CAMPAIGN_TAGS_MAX_COUNT = 20

POLL_QUESTION_MAX_LEN = 300
POLL_OPTION_MAX_LEN = 100
POLL_OPTIONS_MIN = 2
POLL_OPTIONS_MAX = 10
QUIZ_EXPLANATION_MAX_LEN = 200

POLL_OPEN_PERIOD_MIN_SECONDS = 5
# Telegram's native open_period/close_date parameters only accept 5-600
# seconds (Poll.MAX_OPEN_PERIOD in PTB 20.8) — NOT 600000. Anything the
# admin configures above TELEGRAM_NATIVE_POLL_DURATION_MAX_SECONDS is still
# accepted here (product-level cap below), but is sent to Telegram as an
# open-ended poll and auto-stopped by the restart-safe worker at the
# configured time instead of via Telegram's native parameter — see
# community_centre.py's run_due_poll_closures / _do_send.
TELEGRAM_NATIVE_POLL_DURATION_MAX_SECONDS = 600
POLL_OPEN_PERIOD_MAX_SECONDS = 2592000  # 30 days — our own scheduling ceiling, not Telegram's
POLL_CLOSE_DATE_MIN_LEAD_SECONDS = 5
POLL_CLOSE_DATE_MAX_LEAD_SECONDS = 2592000

MEDIA_GROUP_MIN_ITEMS = 2
MEDIA_GROUP_MAX_ITEMS = 10
MEDIA_MAX_SIZE_BYTES = {
    "photo": 10 * 1024 * 1024,
    "animation": 50 * 1024 * 1024,
    "video": 50 * 1024 * 1024,
}
MEDIA_ALLOWED_MIME_PREFIXES = {
    "photo": ("image/jpeg", "image/png", "image/webp"),
    "animation": ("video/mp4", "image/gif"),
    "video": ("video/mp4",),
}

BUTTON_TEXT_MAX_LEN = 64
BUTTON_MAX_ROWS = 8
BUTTON_MAX_PER_ROW = 4
BUTTON_MAX_TOTAL = 20

BUTTON_TYPES = ("url", "telegram_link", "webapp", "callback")

# Admin-defined allowlist of callback_data actions a button may reference.
# Arbitrary callback payloads are never accepted from the frontend.
ALLOWED_CALLBACK_ACTIONS = {
    "noop",
}

CONTENT_TYPES = ("text", "photo", "animation", "video", "media_group", "poll", "quiz")

POLL_CLOSE_MODES = ("manual", "duration", "date")

POST_STATUSES = (
    "draft",
    "pending_approval",
    "approved",
    "scheduled",
    "processing",
    "published",
    "partially_published",
    "failed",
    "cancelled",
)

POLL_STATUSES = ("not_applicable", "scheduled", "open", "closed", "failed")

RUN_STATUSES = ("pending", "processing", "published", "partially_published", "failed", "cancelled")

EDITABLE_STATUSES = ("draft", "scheduled")

# ---------------------------------------------------------------------------
# Error categorisation (spec section 28)
# ---------------------------------------------------------------------------

RETRYABLE_ERROR_CODES = {
    "network_timeout",
    "connection_reset",
    "telegram_rate_limited",
    "telegram_server_error",
    "dns_failure",
}

PERMANENT_ERROR_CODES = {
    "telegram_forbidden",
    "bot_lacks_permission",
    "bot_lacks_pin_permission",
    "invalid_chat_id",
    "invalid_media",
    "invalid_media_file_id",
    "invalid_poll",
    "invalid_url",
    "message_too_long",
    "chat_not_found",
    "bot_loop_not_running",
    "local_type_error",
    "unknown_error",
}

MAX_ATTEMPTS = 5
RETRY_BACKOFF_BASE_SECONDS = 30
RETRY_BACKOFF_MAX_SECONDS = 1800
PROCESSING_TIMEOUT_SECONDS = 120


def compute_backoff_seconds(attempt_count: int) -> int:
    """Capped exponential backoff: 30s, 60s, 120s, 240s ... capped at 30m."""
    seconds = RETRY_BACKOFF_BASE_SECONDS * (2 ** max(0, attempt_count - 1))
    return min(seconds, RETRY_BACKOFF_MAX_SECONDS)


# ---------------------------------------------------------------------------
# Validation helpers. Each returns None on success or a short machine-
# readable error code string on failure — never a raw Telegram error, never
# a stack trace.
# ---------------------------------------------------------------------------

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def has_control_chars(value: str) -> bool:
    return bool(_CONTROL_CHAR_RE.search(value or ""))


def validate_text_len(value: str, max_len: int) -> str | None:
    if not isinstance(value, str):
        return "bad_type"
    if has_control_chars(value):
        return "control_characters"
    if len(value) > max_len:
        return "too_long"
    return None


def validate_poll_options(options: list[str]) -> str | None:
    if not isinstance(options, list):
        return "bad_options"
    if len(options) < POLL_OPTIONS_MIN:
        return "too_few_options"
    if len(options) > POLL_OPTIONS_MAX:
        return "too_many_options"
    seen = set()
    for opt in options:
        if not isinstance(opt, str) or not opt.strip():
            return "empty_option"
        if has_control_chars(opt):
            return "control_characters"
        if len(opt) > POLL_OPTION_MAX_LEN:
            return "option_too_long"
        key = opt.strip().lower()
        if key in seen:
            return "duplicate_option"
        seen.add(key)
    return None


ALLOWED_URL_SCHEMES = {"https"}
ALLOWED_TG_LINK_PREFIXES = ("https://t.me/", "https://telegram.me/")
ALLOWED_TG_SCHEME_ACTIONS = {"tg://resolve", "tg://join", "tg://user"}


def validate_button_url(url: str, button_type: str) -> str | None:
    if not isinstance(url, str) or not url.strip():
        return "missing_url"
    if has_control_chars(url):
        return "control_characters"
    lowered = url.strip().lower()
    if lowered.startswith("javascript:") or lowered.startswith("data:"):
        return "disallowed_protocol"
    parsed = urlparse(url.strip())
    if button_type in ("url", "webapp"):
        if parsed.scheme != "https":
            return "https_required"
        return None
    if button_type == "telegram_link":
        if not url.startswith(ALLOWED_TG_LINK_PREFIXES) and parsed.scheme != "https":
            return "invalid_telegram_link"
        return None
    return "unknown_button_type"


def validate_buttons(buttons: list[dict]) -> str | None:
    if not isinstance(buttons, list):
        return "bad_buttons"
    if len(buttons) > BUTTON_MAX_TOTAL:
        return "too_many_buttons"
    rows: dict[int, int] = {}
    for btn in buttons:
        if not isinstance(btn, dict):
            return "bad_button"
        text = btn.get("text")
        if not isinstance(text, str) or not text.strip():
            return "missing_button_text"
        if has_control_chars(text):
            return "control_characters"
        if len(text) > BUTTON_TEXT_MAX_LEN:
            return "button_text_too_long"
        btype = btn.get("type")
        if btype not in BUTTON_TYPES:
            return "invalid_button_type"
        if btype == "callback":
            action = btn.get("value")
            if action not in ALLOWED_CALLBACK_ACTIONS:
                return "unapproved_callback"
        else:
            err = validate_button_url(btn.get("value", ""), btype)
            if err:
                return err
        row = btn.get("row")
        if not isinstance(row, int) or row < 0 or row >= BUTTON_MAX_ROWS:
            return "invalid_row"
        rows[row] = rows.get(row, 0) + 1
        if rows[row] > BUTTON_MAX_PER_ROW:
            return "too_many_buttons_in_row"
    return None


def validate_campaign_tags(tags: list[str]) -> str | None:
    if tags is None:
        return None
    if not isinstance(tags, list):
        return "bad_tags"
    if len(tags) > CAMPAIGN_TAGS_MAX_COUNT:
        return "too_many_tags"
    for tag in tags:
        if not isinstance(tag, str) or not tag.strip():
            return "bad_tag"
        if has_control_chars(tag) or len(tag) > CAMPAIGN_TAG_MAX_LEN:
            return "tag_too_long"
    return None


def limits_payload() -> dict:
    """Everything the frontend needs to mirror backend validation without
    duplicating numbers — call this from the /limits endpoint."""
    return {
        "text_max_len": TEXT_MAX_LEN,
        "caption_max_len": CAPTION_MAX_LEN,
        "internal_title_max_len": INTERNAL_TITLE_MAX_LEN,
        "internal_notes_max_len": INTERNAL_NOTES_MAX_LEN,
        "campaign_tag_max_len": CAMPAIGN_TAG_MAX_LEN,
        "campaign_tags_max_count": CAMPAIGN_TAGS_MAX_COUNT,
        "poll_question_max_len": POLL_QUESTION_MAX_LEN,
        "poll_option_max_len": POLL_OPTION_MAX_LEN,
        "poll_options_min": POLL_OPTIONS_MIN,
        "poll_options_max": POLL_OPTIONS_MAX,
        "quiz_explanation_max_len": QUIZ_EXPLANATION_MAX_LEN,
        "poll_open_period_min_seconds": POLL_OPEN_PERIOD_MIN_SECONDS,
        "poll_open_period_max_seconds": POLL_OPEN_PERIOD_MAX_SECONDS,
        "telegram_native_poll_duration_max_seconds": TELEGRAM_NATIVE_POLL_DURATION_MAX_SECONDS,
        "media_group_min_items": MEDIA_GROUP_MIN_ITEMS,
        "media_group_max_items": MEDIA_GROUP_MAX_ITEMS,
        "media_max_size_bytes": MEDIA_MAX_SIZE_BYTES,
        "button_text_max_len": BUTTON_TEXT_MAX_LEN,
        "button_max_rows": BUTTON_MAX_ROWS,
        "button_max_per_row": BUTTON_MAX_PER_ROW,
        "button_max_total": BUTTON_MAX_TOTAL,
        "button_types": list(BUTTON_TYPES),
        "content_types": list(CONTENT_TYPES),
        "poll_close_modes": list(POLL_CLOSE_MODES),
        "allowed_callback_actions": sorted(ALLOWED_CALLBACK_ACTIONS),
    }


# ---------------------------------------------------------------------------
# Telegram HTML sanitiser — strict allowlist. Anything not on the list is
# dropped (tag stripped, text content kept) rather than passed through.
# ---------------------------------------------------------------------------

_ALLOWED_TAGS = {
    "b", "strong", "i", "em", "u", "ins", "s", "strike", "del",
    "span", "tg-spoiler", "a", "code", "pre", "blockquote",
}
_ALLOWED_ATTRS = {
    "a": {"href"},
    "span": {"class"},
    "blockquote": {"expandable"},
    "code": {"class"},
}


class _TelegramHTMLSanitizer(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.out: list[str] = []
        self._stack: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag not in _ALLOWED_TAGS:
            return
        allowed_attrs = _ALLOWED_ATTRS.get(tag, set())
        kept = []
        for name, value in attrs:
            if name not in allowed_attrs:
                continue
            if name == "href":
                v = (value or "").strip()
                low = v.lower()
                if low.startswith("javascript:") or low.startswith("data:"):
                    continue
                if not (low.startswith("https://") or low.startswith("tg://") or low.startswith("http://")):
                    continue
            kept.append(f'{name}="{value}"' if value is not None else name)
        attr_str = (" " + " ".join(kept)) if kept else ""
        self.out.append(f"<{tag}{attr_str}>")
        self._stack.append(tag)

    def handle_endtag(self, tag):
        if tag not in _ALLOWED_TAGS:
            return
        if tag in self._stack:
            # Close any accidentally-unclosed inner tags first (best effort).
            while self._stack and self._stack[-1] != tag:
                self.out.append(f"</{self._stack.pop()}>")
            if self._stack:
                self._stack.pop()
            self.out.append(f"</{tag}>")

    def handle_data(self, data):
        self.out.append(
            data.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )

    def handle_entityref(self, name):
        self.out.append(f"&{name};")

    def handle_charref(self, name):
        self.out.append(f"&#{name};")

    def close_all(self):
        while self._stack:
            self.out.append(f"</{self._stack.pop()}>")

    def result(self) -> str:
        return "".join(self.out)


def sanitize_telegram_html(raw: str | None) -> tuple[str, str | None]:
    """Return (sanitized_html, error_code). error_code is set only for
    inputs that cannot be safely processed (control chars); tag stripping
    itself never errors, it just silently drops disallowed markup."""
    if raw is None:
        return "", None
    if has_control_chars(raw):
        return "", "control_characters"
    parser = _TelegramHTMLSanitizer()
    try:
        parser.feed(raw)
        parser.close_all()
    except Exception:
        return "", "invalid_html"
    return parser.result(), None
