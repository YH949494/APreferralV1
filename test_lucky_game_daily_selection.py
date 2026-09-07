"""Tests for lucky_games.get_daily_game_selection: the weighted-random,
one-winner-per-Kuala-Lumpur-day selection mechanism that backs the Mini
App's single "Lucky Game" tile (main.py's /v2/miniapp/daily-game).

Covers: exactly one selection per KL day, persistence/stability across
repeated calls, a new KL day producing a new selection, publish-eligibility
filtering, weighted-probability skew (seeded RNG), weight changes not
retroactively affecting an already-selected day, concurrency-safety of the
first-of-day selection, the no-eligible-games safe response, and controlled
reselection when today's previously-selected game is later
unpublished/deleted.
"""

from __future__ import annotations

import random
import threading
from datetime import datetime

import pytest

import database
import lucky_games as lg
from fake_mongo import FakeDb


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb()
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(lg, "database", database)
    return fdb


def _game_doc(**overrides):
    base = {
        "name": "Infinity Ocean",
        "label": "Lucky Game",
        "volatility": "High-Med",
        "max_win": "25000x",
        "image_url": "https://cdn.example.com/infinity-ocean.webp",
        "game_url": "https://games.example.com/infinity-ocean",
        "provider": "PG Soft",
        "sort_order": 10,
        "selection_weight": 10,
        "is_published": True,
    }
    base.update(overrides)
    return base


def _utc_for_kl_date(y, m, d, hour=12):
    from datetime import timezone

    # Construct directly as UTC noon on that date; KL is UTC+8 with no DST,
    # so UTC noon is always within the same KL calendar date.
    return datetime(y, m, d, hour, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 1-2. Exactly one selection per KL day; stable across repeated calls
# ---------------------------------------------------------------------------


def test_exactly_one_game_selected_and_persisted(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(name="A"))
    fake_db["lucky_games"].insert_one(_game_doc(name="B"))
    now = _utc_for_kl_date(2026, 9, 7)

    result = lg.get_daily_game_selection(now=now)

    assert result["ok"] is True
    assert result["date_kl"] == "2026-09-07"
    assert fake_db["lucky_game_daily_selection"].count_documents({}) == 1
    doc = fake_db["lucky_game_daily_selection"].find_one({"_id": "2026-09-07"})
    assert doc["game_name"] == result["slot"]["name"]


def test_same_game_returned_repeatedly_same_day(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(name="A"))
    fake_db["lucky_games"].insert_one(_game_doc(name="B"))
    fake_db["lucky_games"].insert_one(_game_doc(name="C"))
    now = _utc_for_kl_date(2026, 9, 7)

    first = lg.get_daily_game_selection(now=now)
    for _ in range(10):
        again = lg.get_daily_game_selection(now=now)
        assert again["slot"]["id"] == first["slot"]["id"]
        assert again["slot"]["name"] == first["slot"]["name"]
    assert fake_db["lucky_game_daily_selection"].count_documents({}) == 1


# ---------------------------------------------------------------------------
# 3. A new KL day can produce a new selection (new persisted row)
# ---------------------------------------------------------------------------


def test_new_kl_day_creates_new_selection_row(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(name="A"))
    fake_db["lucky_games"].insert_one(_game_doc(name="B"))

    day1 = lg.get_daily_game_selection(now=_utc_for_kl_date(2026, 9, 7))
    day2 = lg.get_daily_game_selection(now=_utc_for_kl_date(2026, 9, 8))

    assert day1["date_kl"] == "2026-09-07"
    assert day2["date_kl"] == "2026-09-08"
    assert fake_db["lucky_game_daily_selection"].count_documents({}) == 2
    assert fake_db["lucky_game_daily_selection"].find_one({"_id": "2026-09-07"}) is not None
    assert fake_db["lucky_game_daily_selection"].find_one({"_id": "2026-09-08"}) is not None


# ---------------------------------------------------------------------------
# 4. Only published/active games are eligible
# ---------------------------------------------------------------------------


def test_only_published_games_are_eligible(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(name="Published", is_published=True))
    fake_db["lucky_games"].insert_one(_game_doc(name="Draft", is_published=False))
    now = _utc_for_kl_date(2026, 9, 7)

    for _ in range(20):
        fake_db["lucky_game_daily_selection"].delete_many({})
        result = lg.get_daily_game_selection(now=now)
        assert result["slot"]["name"] == "Published"


# ---------------------------------------------------------------------------
# 5. Higher weight has higher selection likelihood (seeded RNG)
# ---------------------------------------------------------------------------


def test_weighted_pick_skews_towards_higher_weight():
    games = [
        {"name": "A", "selection_weight": 30},
        {"name": "B", "selection_weight": 10},
        {"name": "C", "selection_weight": 10},
        {"name": "D", "selection_weight": 5},
    ]
    rng = random.Random(1234)
    counts = {"A": 0, "B": 0, "C": 0, "D": 0}
    trials = 6000
    for _ in range(trials):
        counts[lg._weighted_pick(games, rng=rng)["name"]] += 1

    # A (weight 30) should dominate; D (weight 5) should be picked least.
    assert counts["A"] > counts["B"]
    assert counts["A"] > counts["C"]
    assert counts["A"] > counts["D"]
    assert counts["D"] < counts["B"]
    assert counts["D"] < counts["C"]
    # Loose sanity check against the expected 30/10/10/5 ratio (total 55).
    expected_a_share = 30 / 55
    assert abs(counts["A"] / trials - expected_a_share) < 0.05


# ---------------------------------------------------------------------------
# 6. Weight changed mid-day does not affect today's already-selected game
# ---------------------------------------------------------------------------


def test_weight_change_does_not_affect_todays_selection(fake_db):
    id_a = fake_db["lucky_games"].insert_one(_game_doc(name="A", selection_weight=30)).inserted_id
    fake_db["lucky_games"].insert_one(_game_doc(name="B", selection_weight=1))
    now = _utc_for_kl_date(2026, 9, 7)

    # Force A to win deterministically via a stub rng, then lower its weight
    # to (near) zero for the rest of the day.
    class _StubRng:
        def choices(self, population, weights, k):
            return [next(g for g in population if g["name"] == "A")]

    first = lg.get_daily_game_selection(now=now, rng=_StubRng())
    assert first["slot"]["name"] == "A"

    fake_db["lucky_games"].update_one({"_id": id_a}, {"$set": {"selection_weight": 1}})

    again = lg.get_daily_game_selection(now=now)
    assert again["slot"]["id"] == first["slot"]["id"]
    assert again["slot"]["name"] == "A"


# ---------------------------------------------------------------------------
# 7. Concurrent first requests produce one stored daily selection
# ---------------------------------------------------------------------------


def test_concurrent_first_requests_resolve_to_one_winner(fake_db):
    for i in range(5):
        fake_db["lucky_games"].insert_one(_game_doc(name=f"Game{i}", selection_weight=10))
    now = _utc_for_kl_date(2026, 9, 7)

    results = []
    lock = threading.Lock()

    def worker():
        r = lg.get_daily_game_selection(now=now)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert fake_db["lucky_game_daily_selection"].count_documents({}) == 1
    winner_ids = {r["slot"]["id"] for r in results if r.get("ok")}
    assert len(winner_ids) == 1


# ---------------------------------------------------------------------------
# 8. No published/active games -> safe, non-crashing empty response
# ---------------------------------------------------------------------------


def test_no_eligible_games_returns_safe_response(fake_db):
    now = _utc_for_kl_date(2026, 9, 7)
    result = lg.get_daily_game_selection(now=now)
    assert result == {"ok": False, "date_kl": "2026-09-07", "error": "no_eligible_games"}
    assert fake_db["lucky_game_daily_selection"].count_documents({}) == 0


def test_no_eligible_games_when_only_unpublished(fake_db):
    fake_db["lucky_games"].insert_one(_game_doc(is_published=False))
    now = _utc_for_kl_date(2026, 9, 7)
    result = lg.get_daily_game_selection(now=now)
    assert result["ok"] is False
    assert result["error"] == "no_eligible_games"


# ---------------------------------------------------------------------------
# 10. Today's selected game later deleted/unpublished -> safe reselection
# ---------------------------------------------------------------------------


def test_unpublished_selected_game_triggers_reselection(fake_db):
    id_a = fake_db["lucky_games"].insert_one(_game_doc(name="A")).inserted_id
    id_b = fake_db["lucky_games"].insert_one(_game_doc(name="B")).inserted_id
    now = _utc_for_kl_date(2026, 9, 7)

    class _PickA:
        def choices(self, population, weights, k):
            return [next(g for g in population if g["id"] == str(id_a))]

    first = lg.get_daily_game_selection(now=now, rng=_PickA())
    assert first["slot"]["name"] == "A"

    fake_db["lucky_games"].update_one({"_id": id_a}, {"$set": {"is_published": False}})

    again = lg.get_daily_game_selection(now=now)
    assert again["ok"] is True
    assert again["slot"]["name"] == "B"
    doc = fake_db["lucky_game_daily_selection"].find_one({"_id": "2026-09-07"})
    assert doc["game_id"] == str(id_b)


def test_deleted_selected_game_with_no_replacement_returns_safe_response(fake_db):
    id_a = fake_db["lucky_games"].insert_one(_game_doc(name="A")).inserted_id
    now = _utc_for_kl_date(2026, 9, 7)

    first = lg.get_daily_game_selection(now=now)
    assert first["ok"] is True

    fake_db["lucky_games"].delete_one({"_id": id_a})

    again = lg.get_daily_game_selection(now=now)
    assert again == {"ok": False, "date_kl": "2026-09-07", "error": "no_eligible_games"}


def test_deleted_selected_game_reselects_when_replacement_available(fake_db):
    id_a = fake_db["lucky_games"].insert_one(_game_doc(name="A")).inserted_id
    now = _utc_for_kl_date(2026, 9, 7)

    first = lg.get_daily_game_selection(now=now)
    assert first["slot"]["name"] == "A"

    fake_db["lucky_games"].delete_one({"_id": id_a})
    id_b = fake_db["lucky_games"].insert_one(_game_doc(name="B")).inserted_id

    again = lg.get_daily_game_selection(now=now)
    assert again["ok"] is True
    assert again["slot"]["name"] == "B"
    doc = fake_db["lucky_game_daily_selection"].find_one({"_id": "2026-09-07"})
    assert doc["game_id"] == str(id_b)
