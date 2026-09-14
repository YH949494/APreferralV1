"""Public weekly XP / weekly referral milestone announcements (main.py) must
never publish a member's full clickable Telegram username.

main._format_mention() is the shared identity formatter used by both
_announce_text(u, "weekly_xp", ...) and _announce_text(u, "weekly_ref", ...).
It must reuse main.mask_username() and must never emit a tg://user?id=...
deep link, since a masked label wrapped in a profile link still exposes the
full username on tap. Leaderboard/admin/private-user rendering (format_username)
is untouched by this patch and is not exercised here.
"""

import os
import unittest.mock as mock

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
os.environ.setdefault("BOT_TOKEN", "123:ABC")
os.environ.setdefault("FLASK_SECRET_KEY", "test-secret")

import mongomock

import database

if database._db is None:
    with mock.patch.object(database, "MongoClient", lambda url: mongomock.MongoClient()):
        import main  # noqa: E402
else:  # pragma: no cover
    import main  # noqa: E402


def test_format_mention_masks_username_via_mask_username():
    u = {"user_id": 555, "username": "kamiiszs"}
    mention = main._format_mention(u)
    assert mention == f"@{main.mask_username('kamiiszs')}"
    assert "kamiiszs" not in mention


def test_format_mention_has_no_tg_deep_link():
    u = {"user_id": 555, "username": "kamiiszs"}
    mention = main._format_mention(u)
    assert "tg://user" not in mention
    assert "<a " not in mention


def test_format_mention_falls_back_to_first_name_when_no_username():
    u = {"user_id": 555, "first_name": "Kamil"}
    mention = main._format_mention(u)
    assert mention == "Kamil"
    assert "tg://user" not in mention


def test_format_mention_falls_back_to_player_when_no_identity():
    u = {"user_id": 555}
    mention = main._format_mention(u)
    assert mention == "player"


def test_weekly_xp_announcement_uses_masked_identity_no_link():
    u = {"user_id": 555, "username": "kamiiszs"}
    text = main._announce_text(u, "weekly_xp", 4000)
    assert "kamiiszs" not in text
    assert f"@{main.mask_username('kamiiszs')}" in text
    assert "tg://user=" not in text
    assert "tg://user?id=" not in text
    assert text == f"🎉 @{main.mask_username('kamiiszs')} just hit <b>4,000 weekly XP</b>! On a streak! ⚡"


def test_weekly_referral_announcement_uses_masked_identity_no_link():
    u = {"user_id": 555, "username": "kamiiszs"}
    text = main._announce_text(u, "weekly_ref", 20)
    assert "kamiiszs" not in text
    assert f"@{main.mask_username('kamiiszs')}" in text
    assert "tg://user?id=" not in text
    assert text == f"🚀 @{main.mask_username('kamiiszs')} reached <b>20 weekly referrals</b>! Absolute legend! 🏆"


def test_both_announcement_paths_share_the_same_masked_identity():
    u = {"user_id": 555, "username": "kamiiszs"}
    xp_text = main._announce_text(u, "weekly_xp", 4000)
    ref_text = main._announce_text(u, "weekly_ref", 20)
    masked = main.mask_username("kamiiszs")
    assert masked in xp_text
    assert masked in ref_text


def test_announcements_fall_back_to_first_name_when_username_missing():
    u = {"user_id": 555, "first_name": "Kamil"}
    xp_text = main._announce_text(u, "weekly_xp", 4000)
    ref_text = main._announce_text(u, "weekly_ref", 20)
    assert "🎉 Kamil just hit" in xp_text
    assert "🚀 Kamil reached" in ref_text
    assert "tg://user" not in xp_text
    assert "tg://user" not in ref_text


def test_announcements_fall_back_to_player_when_no_identity_at_all():
    u = {"user_id": 555}
    xp_text = main._announce_text(u, "weekly_xp", 4000)
    ref_text = main._announce_text(u, "weekly_ref", 20)
    assert "🎉 player just hit" in xp_text
    assert "🚀 player reached" in ref_text
