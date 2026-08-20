"""Public affiliate / Money Room milestone announcements must never publish a
member's full Telegram username, and must never render a leading '@' before the
masked value (the masked value is not a resolvable handle).

Masking is applied at the public rendering layer only -- users.username, the
affiliate ledger/snapshot rows and admin-facing rendering are untouched.
"""

from datetime import datetime, timezone
from unittest.mock import patch

import scheduler

import affiliate_leaderboard


NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


class _OkResp:
    ok = True
    status_code = 200
    text = "ok"


class _FakeUsers:
    def __init__(self, doc):
        self.doc = doc

    def find_one(self, filt, proj=None):
        return dict(self.doc) if self.doc is not None else None


class _FakeReferralEvents:
    def __init__(self, settled):
        self.settled = settled

    def count_documents(self, filt):
        return self.settled if filt.get("event") == "referral_settled" else 0


class _FakeCongrats:
    def __init__(self):
        self.inserted = []
        self._next_id = 1

    def find_one(self, filt, proj=None):
        return None

    def insert_one(self, doc):
        doc = dict(doc)
        doc["_id"] = self._next_id
        self._next_id += 1
        self.inserted.append(doc)
        return type("Result", (), {"inserted_id": doc["_id"]})()

    def update_one(self, filt, update):
        for doc in self.inserted:
            if doc.get("_id") == filt.get("_id"):
                doc.update(update.get("$set") or {})
        return None

    def delete_one(self, filt):
        self.inserted = [d for d in self.inserted if d.get("_id") != filt.get("_id")]
        return None


class _FakeAffiliateLedger:
    """Defaults to a durably ISSUED voucher for whatever (user, year_month,
    tier) is looked up, so tests focused on username masking don't need to
    care about the gating -- tests that DO care override via ``rows``."""

    def __init__(self, status="ISSUED", voucher_code="AFFCODE123"):
        self.status = status
        self.voucher_code = voucher_code

    def find_one(self, filt, proj=None):
        return {"status": self.status, "voucher_code": self.voucher_code}


class _FakeDb:
    def __init__(self, user_doc, settled=25, ledger_status="ISSUED", ledger_voucher_code="AFFCODE123"):
        self.users = _FakeUsers(user_doc)
        self.referral_events = _FakeReferralEvents(settled)
        self.referral_tier_congrats = _FakeCongrats()
        self.affiliate_ledger = _FakeAffiliateLedger(status=ledger_status, voucher_code=ledger_voucher_code)


def _announce(user_doc, settled=25):
    """Run the real announcement path and return (sent_text, fake_db)."""
    fake_db = _FakeDb(user_doc, settled=settled)
    with patch.object(scheduler, "db", fake_db), patch.object(
        scheduler.requests, "post", return_value=_OkResp()
    ) as post_mock:
        scheduler.maybe_shout_referral_congrats(777, NOW)
    return post_mock.call_args.kwargs["json"]["text"], fake_db


# ---------------------------------------------------------------------------
# public_affiliate_announcement_name -- the single public rendering helper
# ---------------------------------------------------------------------------

def test_public_name_masks_normal_username():
    assert scheduler.public_affiliate_announcement_name({"username": "kamilszs"}) == "kami****"
    assert scheduler.public_affiliate_announcement_name({"username": "NOOMI1402"}) == "NOOM****"


def test_public_name_strips_leading_at_sign():
    assert scheduler.public_affiliate_announcement_name({"username": "@kamilszs"}) == "kami****"
    assert not scheduler.public_affiliate_announcement_name({"username": "@kamilszs"}).startswith("@")


def test_public_name_short_username():
    assert scheduler.public_affiliate_announcement_name({"username": "abcd"}) == "abcd****"
    assert scheduler.public_affiliate_announcement_name({"username": "abc"}) == "abc****"
    assert scheduler.public_affiliate_announcement_name({"username": "a"}) == "a****"


def test_public_name_empty_or_missing_username_falls_back():
    # No identity at all -> the same anonymous fallback the public leaderboard uses.
    assert scheduler.public_affiliate_announcement_name(None) == "Anonymous"
    assert scheduler.public_affiliate_announcement_name({}) == "Anonymous"
    assert scheduler.public_affiliate_announcement_name({"username": ""}) == "Anonymous"
    assert scheduler.public_affiliate_announcement_name({"username": "   "}) == "Anonymous"
    assert scheduler.public_affiliate_announcement_name({"username": "@"}) == "Anonymous"
    # first_name is not a Telegram handle; it stays the fallback display, same
    # as the public weekly leaderboard post.
    assert scheduler.public_affiliate_announcement_name({"username": None, "first_name": "Kamil"}) == "Kamil"


def test_public_name_reuses_the_public_leaderboard_masking_rule():
    for name in ("kamilszs", "@NOOMI1402", "Valivan", "abc"):
        assert scheduler.public_affiliate_announcement_name(
            {"username": name}
        ) == scheduler.mask_public_username(name)


# ---------------------------------------------------------------------------
# The rendered/sent public announcement
# ---------------------------------------------------------------------------

def test_announcement_does_not_contain_full_username():
    text, _ = _announce({"user_id": 777, "username": "kamilszs"})
    assert "kamilszs" not in text
    assert "kami****" in text


def test_announcement_has_no_leading_at_before_masked_name():
    text, _ = _announce({"user_id": 777, "username": "kamilszs"})
    assert "@" not in text
    assert not text.split("🎉 ", 1)[1].startswith("@")


def test_announcement_does_not_deep_link_to_the_profile():
    # A tg://user?id= mention would still expose the full username on tap,
    # defeating the mask -- the public post is plain masked text.
    text, _ = _announce({"user_id": 777, "username": "kamilszs"})
    assert "tg://user" not in text


def test_announcement_keeps_tier_counts_and_reflects_actual_issuance():
    text, _ = _announce({"user_id": 777, "username": "kamilszs"}, settled=25)
    assert text == (
        "🎉 kami**** just hit <b>25 valid referrals</b> this month "
        "— <b>$15 voucher issued!</b> Next: 50 refs = $50! 💪"
    )


def test_announcement_masks_username_with_stored_leading_at():
    text, _ = _announce({"user_id": 777, "username": "@NOOMI1402"})
    assert "NOOMI1402" not in text
    assert "NOOM****" in text
    assert "@" not in text


def test_announcement_with_missing_username_uses_first_name():
    text, _ = _announce({"user_id": 777, "username": None, "first_name": "Kamil"})
    assert "🎉 Kamil just hit" in text
    assert "@" not in text


def test_announcement_with_no_identity_at_all():
    text, _ = _announce({"user_id": 777})
    assert "🎉 Anonymous just hit" in text


# ---------------------------------------------------------------------------
# Storage and admin/internal surfaces are untouched
# ---------------------------------------------------------------------------

def test_stored_username_remains_unmasked():
    _text, fake_db = _announce({"user_id": 777, "username": "kamilszs"})
    stored = fake_db.referral_tier_congrats.inserted[0]
    assert stored["username"] == "kamilszs"
    assert stored["display_name"] == "kamilszs"
    # The user document itself is never rewritten by the announcement path.
    assert fake_db.users.doc["username"] == "kamilszs"


def _main_mask_username(username: str) -> str:
    """Mirror of main.mask_username (main.py imports connect to Mongo at import
    time, so the admin-surface rule is reproduced here, as the existing
    affiliate-leaderboard tests do)."""
    if not username:
        return "********"
    u = str(username).lstrip("@")
    if len(u) <= 2:
        return u[0] + "*" * (len(u) - 1)
    if len(u) <= 6:
        return u[:2] + "***"
    return f"{u[:4]}***{u[-2:]}"


def _main_format_username(u, current_user_id, is_admin):
    name = str(u.get("username")).lstrip("@") if u.get("username") else (u.get("first_name") or None)
    if not name:
        return None
    if (not is_admin) and int(u.get("user_id") or 0) != int(current_user_id or 0):
        return _main_mask_username(name)
    return name


def test_admin_affiliate_leaderboard_rendering_unchanged():
    entries = [{"user_id": 777, "username": "kamilszs", "display_name": "Kamil"}]
    rows = affiliate_leaderboard.serialize_affiliate_snapshot_entries_for_viewer(
        entries,
        current_user_id=1,
        is_admin=True,
        format_username_fn=_main_format_username,
        mask_username_fn=_main_mask_username,
    )
    assert rows[0]["username"] == "kamilszs"
    assert rows[0]["display_name"] == "kamilszs"
    assert rows[0]["user_id"] == 777


def test_own_private_view_still_sees_full_username():
    entries = [{"user_id": 777, "username": "kamilszs", "display_name": "Kamil"}]
    rows = affiliate_leaderboard.serialize_affiliate_snapshot_entries_for_viewer(
        entries,
        current_user_id=777,
        is_admin=False,
        format_username_fn=_main_format_username,
        mask_username_fn=_main_mask_username,
    )
    assert rows[0]["username"] == "kamilszs"
    assert rows[0]["display_name"] == "kamilszs"
