"""Tests for the Lucky Game card's click/impression analytics
(lucky_games.py: record_lucky_game_event, /api/lucky-game/track,
/api/admin/lucky-game/analytics).

Covers: authenticated writes, impression dedup (one per user+tracking_key),
raw clicks never deduped, distinct viewers/clickers across users, malformed
event rejection, spoofed-identity rejection (user_id always comes from
verified Telegram initData, never the request body), strict tracking_key
validation against the server's own persisted daily selection (a missing or
mismatched tracking_key is rejected outright, never silently reattributed to
a different game), same-day-reselection analytics separation, KL-day
attribution, admin analytics math (including the zero-viewer CTR case), and
admin-auth + bounds on the analytics endpoint's ``days`` parameter.

The client-side half of attribution correctness (always revalidating a
cached daily-game slot against GET /v2/miniapp/daily-game before treating
any tracking_key as confirmed) is covered by
test_lucky_game_click_tracking.test.js, not here -- this file only exercises
the backend's own defense-in-depth strict-equality check.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from flask import Flask

import database
import lucky_games as lg
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(
        unique_keys_by_collection={
            lg.EVENTS_COLLECTION: [
                (("user_id", "tracking_key", "event_type"), {"event_type": "impression"}),
            ],
        }
    )
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(lg, "database", database)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(lg.lucky_games_admin_bp)
    app.register_blueprint(lg.lucky_games_public_bp)
    return app


def _mock_admin():
    return patch("vouchers.require_admin", return_value=({"id": 1, "usernameLower": "admin"}, None))


def _mock_verified_user(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _seed_single_game(fake_db, *, name="Fighting Bull", **overrides):
    """Inserts exactly one published game, so get_daily_game_selection's
    weighted pick is deterministic (only one eligible candidate)."""
    doc = {
        "name": name,
        "label": "Lucky Game",
        "volatility": "Medium",
        "max_win": "8000x",
        "image_url": "https://cdn.example.com/fighting-bull.webp",
        "game_url": "https://games.example.com/fighting-bull",
        "provider": "PG Soft",
        "sort_order": 10,
        "selection_weight": 10,
        "is_published": True,
    }
    doc.update(overrides)
    result = fake_db[lg.COLLECTION].insert_one(doc)
    return str(result.inserted_id)


def _current_selection(fake_db):
    """Forces today's selection so tests don't depend on wall-clock KL date
    for setup, while the endpoint itself still resolves "today" for real."""
    return lg.get_daily_game_selection()


# ---------------------------------------------------------------------------
# 1-2. Authenticated impression + dedup
# ---------------------------------------------------------------------------


def test_valid_authenticated_impression_is_recorded(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    with _mock_verified_user(111):
        resp = client.post(
            "/api/lucky-game/track?init_data=raw",
            json={"event": "impression", "tracking_key": selection["tracking_key"]},
        )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["success"] is True
    assert not body.get("duplicate")

    docs = list(fake_db[lg.EVENTS_COLLECTION].find({}))
    assert len(docs) == 1
    doc = docs[0]
    assert doc["event_type"] == "impression"
    assert doc["user_id"] == 111
    assert doc["game_id"] == selection["slot"]["id"]
    assert doc["game_name"] == selection["slot"]["name"]
    assert doc["selection_date_kl"] == selection["date_kl"]
    assert doc["tracking_key"] == selection["tracking_key"]
    assert doc["surface"] == lg.TRACKING_SURFACE
    assert doc["destination"] == lg.LUCKY_GAME_DESTINATION_URL
    assert doc["created_at_utc"] is not None


def test_duplicate_impression_stays_one_unique_impression(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    with _mock_verified_user(111):
        first = client.post(
            "/api/lucky-game/track?init_data=raw", json={"event": "impression", "tracking_key": selection["tracking_key"]}
        )
        second = client.post(
            "/api/lucky-game/track?init_data=raw", json={"event": "impression", "tracking_key": selection["tracking_key"]}
        )

    assert first.status_code == 200 and first.get_json()["success"] is True
    assert second.status_code == 200
    second_body = second.get_json()
    assert second_body["success"] is True
    assert second_body["duplicate"] is True

    assert fake_db[lg.EVENTS_COLLECTION].count_documents({"event_type": "impression"}) == 1


# ---------------------------------------------------------------------------
# 3-5. Clicks are never deduped; unique clicker vs. raw clicks; distinct users
# ---------------------------------------------------------------------------


def test_same_user_clicking_twice_creates_two_raw_clicks_but_one_unique_clicker(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    with _mock_verified_user(222):
        client.post("/api/lucky-game/track?init_data=raw", json={"event": "click", "tracking_key": selection["tracking_key"]})
        client.post("/api/lucky-game/track?init_data=raw", json={"event": "click", "tracking_key": selection["tracking_key"]})

    click_docs = list(fake_db[lg.EVENTS_COLLECTION].find({"event_type": "click"}))
    assert len(click_docs) == 2
    assert {d["user_id"] for d in click_docs} == {222}


def test_different_users_count_as_distinct_clickers_and_viewers(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    for uid in (1, 2, 3):
        with _mock_verified_user(uid):
            client.post("/api/lucky-game/track?init_data=raw", json={"event": "impression", "tracking_key": selection["tracking_key"]})
            client.post("/api/lucky-game/track?init_data=raw", json={"event": "click", "tracking_key": selection["tracking_key"]})
    # One of them clicks again -- must not inflate unique clickers.
    with _mock_verified_user(1):
        client.post("/api/lucky-game/track?init_data=raw", json={"event": "click", "tracking_key": selection["tracking_key"]})

    events = list(fake_db[lg.EVENTS_COLLECTION].find({}))
    viewers = {d["user_id"] for d in events if d["event_type"] == "impression"}
    clickers = {d["user_id"] for d in events if d["event_type"] == "click"}
    clicks = [d for d in events if d["event_type"] == "click"]
    assert viewers == {1, 2, 3}
    assert clickers == {1, 2, 3}
    assert len(clicks) == 4  # 3 first clicks + 1 repeat


# ---------------------------------------------------------------------------
# 6. Malformed event rejected
# ---------------------------------------------------------------------------


def test_unknown_event_type_is_rejected(fake_db):
    _seed_single_game(fake_db)
    app = _app()
    client = app.test_client()

    with _mock_verified_user(1):
        resp = client.post("/api/lucky-game/track?init_data=raw", json={"event": "purchase"})

    assert resp.status_code == 400
    assert resp.get_json()["success"] is False
    assert fake_db[lg.EVENTS_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# 7. Spoofed identity cannot override verified Telegram auth
# ---------------------------------------------------------------------------


def test_client_supplied_user_id_is_ignored(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    with _mock_verified_user(555):
        resp = client.post(
            "/api/lucky-game/track?init_data=raw",
            json={"event": "click", "tracking_key": selection["tracking_key"], "user_id": 999999, "uid": 999999},
        )

    assert resp.status_code == 200
    doc = fake_db[lg.EVENTS_COLLECTION].find_one({})
    assert doc["user_id"] == 555


def test_unauthenticated_event_is_discarded_not_written(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    with patch("vouchers.verify_telegram_init_data", return_value=(False, {}, "hash_mismatch")):
        resp = client.post(
            "/api/lucky-game/track?init_data=raw",
            json={"event": "click", "tracking_key": selection["tracking_key"]},
        )

    # Never blocks/errors the caller -- but nothing is written under an
    # unverifiable identity.
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True
    assert fake_db[lg.EVENTS_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# 8 & 10. tracking_key validation / spoofing, and correct daily-selection tie
# ---------------------------------------------------------------------------


def test_spoofed_tracking_key_is_rejected_never_reattributed(fake_db):
    # A client-supplied tracking_key that doesn't match today's canonical
    # selection must be rejected outright -- never silently rewritten to
    # record the event under whichever game is currently canonical. (Client
    # cache staleness is handled upstream by the frontend always
    # revalidating against GET /v2/miniapp/daily-game before it ever
    # activates tracking -- see loadDailyGame() in static/index.html -- so a
    # legitimate stale-cache mismatch should never actually reach here.)
    other_game_id = _seed_single_game(fake_db, name="Other Game")
    selection = _current_selection(fake_db)
    assert selection["slot"]["id"] == other_game_id
    app = _app()
    client = app.test_client()

    with _mock_verified_user(1):
        resp = client.post(
            "/api/lucky-game/track?init_data=raw",
            json={"event": "click", "tracking_key": "daily_game:2099-01-01:not-a-real-game-id"},
        )

    assert resp.status_code == 400
    assert resp.get_json()["error"] == "invalid_tracking_key"
    assert fake_db[lg.EVENTS_COLLECTION].count_documents({}) == 0


def test_missing_tracking_key_is_rejected(fake_db):
    _seed_single_game(fake_db)
    _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    with _mock_verified_user(1):
        resp = client.post("/api/lucky-game/track?init_data=raw", json={"event": "click"})

    assert resp.status_code == 400
    assert resp.get_json()["error"] == "invalid_tracking_key"
    assert fake_db[lg.EVENTS_COLLECTION].count_documents({}) == 0


def test_event_tied_to_persisted_daily_selection_not_client_claims(fake_db):
    game_id = _seed_single_game(fake_db, name="Infinity Ocean")
    selection = _current_selection(fake_db)
    assert selection["slot"]["id"] == game_id
    app = _app()
    client = app.test_client()

    with _mock_verified_user(1):
        # The correct (server-issued) tracking_key is supplied, but a
        # spoofed game_id/game_name is also sent -- those fields are never
        # read by the endpoint, so game identity must come exclusively from
        # the persisted selection, not from anything else in the body.
        client.post(
            "/api/lucky-game/track?init_data=raw",
            json={
                "event": "click",
                "tracking_key": selection["tracking_key"],
                "game_id": "spoofed-game-id",
                "game_name": "Spoofed Game",
            },
        )

    doc = fake_db[lg.EVENTS_COLLECTION].find_one({})
    assert doc["game_id"] == game_id
    assert doc["game_name"] == "Infinity Ocean"
    assert doc["selection_date_kl"] == selection["date_kl"]
    assert doc["tracking_key"] == selection["tracking_key"]


def test_no_eligible_game_rejects_tracking_without_error_to_caller(fake_db):
    # No games seeded at all -- get_daily_game_selection returns ok=False.
    # A plausible-looking tracking_key is still supplied so the request
    # passes the endpoint's format check and actually reaches
    # record_lucky_game_event(), which is what this test exercises.
    app = _app()
    client = app.test_client()

    with _mock_verified_user(1):
        resp = client.post(
            "/api/lucky-game/track?init_data=raw",
            json={"event": "click", "tracking_key": "daily_game:2026-01-01:nonexistent"},
        )

    assert resp.status_code == 200
    assert resp.get_json()["success"] is True
    assert fake_db[lg.EVENTS_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# 9. KL date boundary: different selection_date_kl => different tracking_key,
# and analytics groups them into separate days.
# ---------------------------------------------------------------------------


def test_kl_date_boundary_produces_distinct_tracking_keys_and_day_buckets(fake_db):
    game_id = "aaaaaaaaaaaaaaaaaaaaaaaa"
    now = None  # unused; we insert events directly to control both days deterministically
    doc_day1 = {
        "event_type": "impression", "user_id": 1, "game_id": game_id, "game_name": "A",
        "selection_date_kl": "2026-09-21", "tracking_key": lg._build_tracking_key("2026-09-21", game_id),
        "surface": lg.TRACKING_SURFACE, "destination": lg.LUCKY_GAME_DESTINATION_URL,
        "created_at_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
    }
    doc_day2 = dict(doc_day1)
    doc_day2["selection_date_kl"] = "2026-09-22"
    doc_day2["tracking_key"] = lg._build_tracking_key("2026-09-22", game_id)

    assert doc_day1["tracking_key"] != doc_day2["tracking_key"]

    fake_db[lg.EVENTS_COLLECTION].insert_one(doc_day1)
    fake_db[lg.EVENTS_COLLECTION].insert_one(doc_day2)

    by_day = lg._build_daily_breakdown(list(fake_db[lg.EVENTS_COLLECTION].find({})))
    assert {row["date"] for row in by_day} == {"2026-09-21", "2026-09-22"}
    for row in by_day:
        assert row["impressions"] == 1
        assert row["unique_viewers"] == 1


def test_same_day_reselection_keeps_games_in_separate_daily_rows(fake_db):
    # Two different games recorded under the *same* selection_date_kl -- the
    # scenario a same-day reselection produces (today's persisted game got
    # unpublished/deleted mid-day and get_daily_game_selection() picked a
    # replacement). by_day must not conflate their counts into one row.
    game_a, game_b = "aaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbb"
    now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
    doc_a = {
        "event_type": "impression", "user_id": 1, "game_id": game_a, "game_name": "A",
        "selection_date_kl": "2026-09-21", "tracking_key": lg._build_tracking_key("2026-09-21", game_a),
        "surface": lg.TRACKING_SURFACE, "destination": lg.LUCKY_GAME_DESTINATION_URL, "created_at_utc": now,
    }
    doc_b = {
        "event_type": "impression", "user_id": 2, "game_id": game_b, "game_name": "B",
        "selection_date_kl": "2026-09-21", "tracking_key": lg._build_tracking_key("2026-09-21", game_b),
        "surface": lg.TRACKING_SURFACE, "destination": lg.LUCKY_GAME_DESTINATION_URL, "created_at_utc": now,
    }
    fake_db[lg.EVENTS_COLLECTION].insert_one(doc_a)
    fake_db[lg.EVENTS_COLLECTION].insert_one(doc_b)

    by_day = lg._build_daily_breakdown(list(fake_db[lg.EVENTS_COLLECTION].find({})))
    assert len(by_day) == 2
    rows_by_game = {row["game_id"]: row for row in by_day}
    assert rows_by_game[game_a]["date"] == "2026-09-21"
    assert rows_by_game[game_a]["unique_viewers"] == 1
    assert rows_by_game[game_b]["date"] == "2026-09-21"
    assert rows_by_game[game_b]["unique_viewers"] == 1

    # by_game still aggregates each game across every day it appeared.
    by_game = lg._build_game_breakdown(list(fake_db[lg.EVENTS_COLLECTION].find({})))
    assert {row["game_id"] for row in by_game} == {game_a, game_b}


# ---------------------------------------------------------------------------
# 11-12. Admin analytics summary math, including zero-viewer CTR
# ---------------------------------------------------------------------------


def test_admin_analytics_summary_and_ctr_math(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    tk = selection["tracking_key"]

    # 3 unique viewers (one reopens -> still 1 impression row thanks to
    # dedup), 2 unique clickers, one of whom clicks twice (raw clicks = 3).
    for uid in (1, 2, 3):
        with _mock_verified_user(uid):
            client.post("/api/lucky-game/track?init_data=raw", json={"event": "impression", "tracking_key": tk})
    with _mock_verified_user(1):
        client.post("/api/lucky-game/track?init_data=raw", json={"event": "impression", "tracking_key": tk})  # duplicate, ignored
        client.post("/api/lucky-game/track?init_data=raw", json={"event": "click", "tracking_key": tk})
        client.post("/api/lucky-game/track?init_data=raw", json={"event": "click", "tracking_key": tk})
    with _mock_verified_user(2):
        client.post("/api/lucky-game/track?init_data=raw", json={"event": "click", "tracking_key": tk})

    with _mock_admin():
        resp = client.get("/api/admin/lucky-game/analytics?days=7")

    assert resp.status_code == 200
    body = resp.get_json()
    summary = body["summary"]
    assert summary["impressions"] == 3
    assert summary["unique_viewers"] == 3
    assert summary["clicks"] == 3
    assert summary["unique_clickers"] == 2
    assert summary["unique_ctr"] == round(2 / 3, 4)

    assert len(body["by_day"]) == 1
    day_row = body["by_day"][0]
    assert day_row["date"] == selection["date_kl"]
    assert day_row["game_id"] == selection["slot"]["id"]
    assert day_row["unique_clickers"] == 2
    assert day_row["unique_viewers"] == 3

    assert len(body["by_game"]) == 1
    assert body["by_game"][0]["game_id"] == selection["slot"]["id"]


def test_admin_analytics_zero_viewers_ctr_is_zero_not_error(fake_db):
    _seed_single_game(fake_db)
    selection = _current_selection(fake_db)
    app = _app()
    client = app.test_client()

    with _mock_verified_user(1):
        client.post(
            "/api/lucky-game/track?init_data=raw",
            json={"event": "click", "tracking_key": selection["tracking_key"]},
        )

    with _mock_admin():
        resp = client.get("/api/admin/lucky-game/analytics")

    assert resp.status_code == 200
    summary = resp.get_json()["summary"]
    assert summary["unique_viewers"] == 0
    assert summary["clicks"] == 1
    assert summary["unique_ctr"] == 0


# ---------------------------------------------------------------------------
# 13-14. Admin auth required; days parameter bounded
# ---------------------------------------------------------------------------


def test_admin_analytics_requires_admin_auth(fake_db):
    app = _app()
    client = app.test_client()

    with patch("vouchers.require_admin", return_value=(None, ({"status": "error"}, 401))):
        resp = client.get("/api/admin/lucky-game/analytics")

    assert resp.status_code == 401


def test_admin_analytics_days_parameter_is_bounded(fake_db):
    app = _app()
    client = app.test_client()

    with _mock_admin():
        too_many = client.get("/api/admin/lucky-game/analytics?days=99999")
        too_few = client.get("/api/admin/lucky-game/analytics?days=-5")
        garbage = client.get("/api/admin/lucky-game/analytics?days=not-a-number")

    assert too_many.get_json()["period"]["days"] == lg.ANALYTICS_MAX_DAYS
    assert too_few.get_json()["period"]["days"] == lg.ANALYTICS_MIN_DAYS
    assert garbage.get_json()["period"]["days"] == lg.ANALYTICS_DEFAULT_DAYS
