"""Unit tests for the correct-answer retry flow on the Mission Pool submit
hot path: a wrong answer is a retryable attempt, never a final entry.

Mirrors test_mission_pool.py's fixtures/style so the retry behaviour is
exercised against the SAME in-memory FakeDb + unique-index setup as the rest
of the submission hot path, rather than a separate mock world.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import database
import mission_pool as mp
from fake_mongo import FakeDb

CAMPAIGN_ID = "mission-retry-1"
UID = 555002


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
        mp.ATTEMPTS_COLLECTION: [("campaign_id", "telegram_user_id")],
        "gc_campaigns": [("campaign_id",)],
    }


@pytest.fixture
def fake_db(monkeypatch):
    fdb = FakeDb(_unique_keys())
    monkeypatch.setattr(database, "db", fdb)
    monkeypatch.setattr(mp, "database", database)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(mp.mission_pool_bp)
    app.register_blueprint(mp.mission_pool_admin_bp)
    return app


def _verified(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _campaign(mission_config, **overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Retry Mission",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": mission_config,
        "mission_pool": {
            "pool_id": "MISSION-RETRY",
            "pool_type": "voucher_drop",
            "winner_count": 3,
            "allocation_method": "random_qualified",
            "eligibility_policy": dict(mp.DEFAULT_ELIGIBILITY_POLICY),
            "cancelled": False,
            "processing_stage": mp.STAGE_PENDING,
            "processing_generation": 0,
        },
    }
    doc.update(overrides)
    return doc


def _keyword_config(correct="DRAGON", case_insensitive=True):
    return {
        "mission_type": "keyword",
        "prompt": "Guess the word",
        "correct_answer": correct,
        "keyword_case_insensitive": case_insensitive,
    }


def _choice_config(correct=None):
    cfg = {
        "mission_type": "single_choice",
        "prompt": "Pick one",
        "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}, {"id": "c", "label": "C"}],
    }
    if correct is not None:
        cfg["correct_answer"] = correct
    return cfg


def _feedback_config():
    return {"mission_type": "feedback", "prompt": "Tell us", "min_chars": 1, "max_chars": 200}


def _seed(fake_db, mission_config, **overrides):
    doc = _campaign(mission_config, **overrides)
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _submit(client, uid=UID, answer="x", campaign_id=CAMPAIGN_ID):
    with _verified(uid):
        return client.post(
            f"/api/mission-pool/{campaign_id}/submit?init_data=stub",
            json={"answer": answer},
        )


# ---------------------------------------------------------------------------
# Keyword retry
# ---------------------------------------------------------------------------

def test_keyword_wrong_answer_creates_no_entry_and_is_retryable(fake_db):
    _seed(fake_db, _keyword_config())
    with _app().test_client() as client:
        resp = _submit(client, answer="TIGER")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["submitted"] is False
    assert body["state"] == "incorrect_retry"
    assert body["retry_allowed"] is True
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_keyword_correct_retry_creates_final_entry(fake_db):
    _seed(fake_db, _keyword_config())
    with _app().test_client() as client:
        wrong = _submit(client, answer="TIGER")
        right = _submit(client, answer="DRAGON")
    assert wrong.get_json()["state"] == "incorrect_retry"
    assert right.status_code == 200
    right_body = right.get_json()
    assert right_body == {"status": "ok", "submitted": True, "state": "submitted"}
    entries = fake_db[mp.ENTRIES_COLLECTION].find({})
    assert len(entries) == 1
    assert entries[0]["is_correct"] is True
    assert entries[0]["answer_normalized"] == "dragon"


def test_keyword_correct_answer_case_insensitive_after_retry(fake_db):
    """Correct-answer comparison after a retry still uses the existing
    normalisation/case-fold rules -- no separate comparison path."""
    _seed(fake_db, _keyword_config(case_insensitive=True))
    with _app().test_client() as client:
        _submit(client, answer="tiger")
        resp = _submit(client, answer="  dragon ")
    assert resp.get_json()["state"] == "submitted"


def test_final_correct_entry_is_unique_after_retries(fake_db):
    _seed(fake_db, _keyword_config())
    with _app().test_client() as client:
        _submit(client, answer="wrong1")
        _submit(client, answer="wrong2")
        _submit(client, answer="DRAGON")
        again = _submit(client, answer="DRAGON")
    assert again.get_json()["state"] == "already_submitted"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1


def test_already_submitted_final_entry_is_immutable_to_further_wrong_guesses(fake_db):
    """Once a final (correct) entry exists, any further submit -- right or
    wrong -- is already_submitted, never a retry, and never touches the
    attempt counter."""
    _seed(fake_db, _keyword_config())
    with _app().test_client() as client:
        _submit(client, answer="DRAGON")
        resp = _submit(client, answer="totally-wrong")
    assert resp.get_json() == {"status": "ok", "submitted": True, "state": "already_submitted"}
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1
    assert fake_db[mp.ENTRIES_COLLECTION].find({})[0]["answer_normalized"] == "dragon"
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


def test_attempts_exhausted_after_configured_limit(fake_db, monkeypatch):
    # No cooldown here -- this test is exercising the attempt-count limit in
    # isolation; the cooldown itself is covered separately below.
    monkeypatch.setattr(mp, "RETRY_COOLDOWN_SECONDS", 0)
    _seed(fake_db, _keyword_config())
    with _app().test_client() as client:
        results = [_submit(client, answer="wrong").get_json() for _ in range(mp.MAX_INCORRECT_ATTEMPTS + 2)]
    states = [r["state"] for r in results]
    assert states[: mp.MAX_INCORRECT_ATTEMPTS] == ["incorrect_retry"] * mp.MAX_INCORRECT_ATTEMPTS
    assert states[mp.MAX_INCORRECT_ATTEMPTS] == "attempts_exhausted"
    assert results[mp.MAX_INCORRECT_ATTEMPTS]["retry_allowed"] is False
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_attempts_exhausted_blocks_even_a_correct_answer(fake_db):
    """§ success criteria: exhausted means excluded from the reward pool --
    a correct guess after the limit must not slip through."""
    _seed(fake_db, _keyword_config())
    fake_db[mp.ATTEMPTS_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID,
        "incorrect_attempts": mp.MAX_INCORRECT_ATTEMPTS,
        "last_attempt_at": datetime.now(timezone.utc) - timedelta(seconds=mp.RETRY_COOLDOWN_SECONDS + 5),
        "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc),
    })
    with _app().test_client() as client:
        resp = _submit(client, answer="wrong-again")
    assert resp.get_json()["state"] == "attempts_exhausted"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_cooldown_enforced_between_rapid_wrong_attempts(fake_db):
    _seed(fake_db, _keyword_config())
    with _app().test_client() as client:
        first = _submit(client, answer="wrong1")
        second = _submit(client, answer="wrong2")
    assert first.get_json()["state"] == "incorrect_retry"
    assert second.get_json()["state"] == "retry_cooldown"
    assert second.get_json()["retry_allowed"] is True
    # Cooldown-blocked attempts must not consume the attempt budget.
    stored = fake_db[mp.ATTEMPTS_COLLECTION].find({})[0]
    assert stored["incorrect_attempts"] == 1


def test_incorrect_retry_leaks_no_answer_details(fake_db):
    _seed(fake_db, _keyword_config())
    with _app().test_client() as client:
        resp = _submit(client, answer="TIGER")
    body = resp.get_json()
    serialized = str(body)
    for leak in ("DRAGON", "dragon", "correct_answer", "TIGER"):
        assert leak not in serialized, f"response leaked {leak!r}: {body}"
    assert set(body.keys()) <= {"status", "submitted", "state", "retry_allowed", "attempts_remaining"}


# ---------------------------------------------------------------------------
# Single choice retry / opinion poll
# ---------------------------------------------------------------------------

def test_single_choice_wrong_option_is_retryable(fake_db):
    _seed(fake_db, _choice_config(correct="a"))
    with _app().test_client() as client:
        resp = _submit(client, answer="b")
    assert resp.get_json()["state"] == "incorrect_retry"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_single_choice_correct_option_creates_final_entry(fake_db):
    _seed(fake_db, _choice_config(correct="a"))
    with _app().test_client() as client:
        _submit(client, answer="b")
        resp = _submit(client, answer="a")
    assert resp.get_json()["state"] == "submitted"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1


def test_opinion_poll_without_correct_answer_is_one_submit_only(fake_db):
    _seed(fake_db, _choice_config(correct=None))
    with _app().test_client() as client:
        first = _submit(client, answer="b")
        second = _submit(client, answer="c")
    assert first.get_json()["state"] == "submitted"
    assert second.get_json()["state"] == "already_submitted"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1
    assert fake_db[mp.ENTRIES_COLLECTION].find({})[0]["answer_normalized"] == "b"
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


def test_invalid_option_remains_a_validation_error_not_a_wrong_answer(fake_db):
    _seed(fake_db, _choice_config(correct="a"))
    with _app().test_client() as client:
        resp = _submit(client, answer="not-an-option")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_option"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# Feedback — never retryable
# ---------------------------------------------------------------------------

def test_feedback_first_valid_submission_is_final(fake_db):
    _seed(fake_db, _feedback_config())
    with _app().test_client() as client:
        first = _submit(client, answer="Great mission!")
        second = _submit(client, answer="Different feedback")
    assert first.get_json()["state"] == "submitted"
    assert second.get_json()["state"] == "already_submitted"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# Lifecycle: retry must still respect campaign state
# ---------------------------------------------------------------------------

def test_retry_after_campaign_closed_is_rejected(fake_db):
    now = datetime.now(timezone.utc)
    _seed(fake_db, _keyword_config(),
          schedule={"starts_at": now - timedelta(hours=2), "ends_at": now - timedelta(minutes=1)})
    with _app().test_client() as client:
        resp = _submit(client, answer="wrong")
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_closed"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


def test_retry_while_paused_is_rejected(fake_db):
    _seed(fake_db, _keyword_config(), status="paused")
    with _app().test_client() as client:
        resp = _submit(client, answer="wrong")
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_paused"
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


def test_retry_before_start_is_rejected(fake_db):
    now = datetime.now(timezone.utc)
    _seed(fake_db, _keyword_config(),
          schedule={"starts_at": now + timedelta(hours=1), "ends_at": now + timedelta(hours=2)})
    with _app().test_client() as client:
        resp = _submit(client, answer="wrong")
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_not_started"
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


def test_retry_blocked_when_campaign_is_closed_mid_flight(fake_db):
    """A close landing between the first check and the write must also
    block a wrong-answer retry attempt -- the second state re-check is not
    bypassed by the retry branch."""
    _seed(fake_db, _keyword_config())
    real_validate = mp.validate_submission

    def close_then_validate(cfg, answer):
        out = real_validate(cfg, answer)
        fake_db["gc_campaigns"].update_one({"campaign_id": CAMPAIGN_ID}, {"$set": {"status": "ended"}})
        return out

    with _app().test_client() as client:
        with patch.object(mp, "validate_submission", side_effect=close_then_validate):
            resp = _submit(client, answer="wrong")
    assert resp.status_code == 409
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0
    assert fake_db[mp.ATTEMPTS_COLLECTION].count_documents({}) == 0


def test_retry_rejected_when_feature_flag_off(fake_db):
    fake_db["gc_campaigns"].insert_one(_campaign(_keyword_config()))
    with _app().test_client() as client:
        with patch.object(mp, "mission_pool_enabled", lambda: False):
            resp = _submit(client, answer="wrong")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Legacy entries
# ---------------------------------------------------------------------------

def test_legacy_final_wrong_entry_remains_already_submitted(fake_db):
    """An entry created before this feature existed (is_correct=False,
    final row present) keeps its existing behaviour -- already_submitted,
    never reopened or migrated."""
    _seed(fake_db, _keyword_config())
    now = datetime.now(timezone.utc)
    fake_db[mp.ENTRIES_COLLECTION].insert_one({
        "campaign_id": CAMPAIGN_ID, "telegram_user_id": UID,
        "answer": "wrongword", "answer_normalized": "wrongword", "is_correct": False,
        "status": mp.ENTRY_STATUS_SUBMITTED, "identity_key": None, "identity_type": None,
        "disqualification_reason": None, "reward_id": None,
        "submitted_at": now, "created_at": now, "updated_at": now,
    })
    with _app().test_client() as client:
        resp = _submit(client, answer="DRAGON")
    assert resp.get_json() == {"status": "ok", "submitted": True, "state": "already_submitted"}
    entries = fake_db[mp.ENTRIES_COLLECTION].find({})
    assert len(entries) == 1
    assert entries[0]["is_correct"] is False
    assert entries[0]["answer_normalized"] == "wrongword"


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

def test_two_simultaneous_correct_submissions_create_one_final_entry(fake_db):
    import threading

    _seed(fake_db, _keyword_config())
    app = _app()
    results = []
    lock = threading.Lock()

    def worker():
        with app.test_client() as client:
            resp = _submit(client, answer="DRAGON")
        with lock:
            results.append(resp.get_json()["state"])

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count("submitted") == 1
    assert results.count("already_submitted") == 5
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1
