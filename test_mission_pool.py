"""Unit tests for mission_pool.py — mechanic routing, config/answer validation,
the submission hot path, kill switch and admin controls (spec §48).

Every test uses the in-memory FakeDb with the SAME unique indexes
ensure_mission_indexes() creates in production, so duplicate protection is
exercised against a real unique constraint rather than a mock.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import database
import mission_pool as mp
from fake_mongo import FakeDb

CAMPAIGN_ID = "mission-pilot-1"
UID = 555001


def _unique_keys():
    return {
        mp.ENTRIES_COLLECTION: [("campaign_id", "telegram_user_id")],
        mp.IDENTITY_CLAIMS_COLLECTION: [("campaign_id", "identity_key")],
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


def _campaign(**overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Pilot Mission",
        "type": "mission_pool",
        "mechanic": "mission_pool",
        "status": "live",
        "schedule": {"starts_at": now - timedelta(hours=1), "ends_at": now + timedelta(hours=1)},
        "mission_config": {
            "mission_type": "multiple_choice",
            "prompt": "Which game?",
            "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "correct_answer": "a",
        },
        "mission_pool": {
            "pool_id": "MISSION-PILOT",
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


def _seed(fake_db, **overrides):
    doc = _campaign(**overrides)
    fake_db["gc_campaigns"].insert_one(doc)
    return doc


def _submit(client, uid=UID, answer="a", campaign_id=CAMPAIGN_ID):
    with _verified(uid):
        return client.post(
            f"/api/mission-pool/{campaign_id}/submit?init_data=stub",
            json={"answer": answer},
        )


# ---------------------------------------------------------------------------
# §2 mechanic routing / backward compatibility
# ---------------------------------------------------------------------------

def test_missing_mechanic_field_defaults_to_standard_drop():
    """A campaign document written before Mission Pool existed has no
    ``mechanic`` field and must keep behaving exactly as a standard drop."""
    assert mp.resolve_mechanic({"campaign_id": "legacy", "type": "tournament"}) == "standard_drop"
    assert mp.resolve_mechanic({}) == "standard_drop"
    assert mp.resolve_mechanic(None) == "standard_drop"
    assert mp.is_mission_pool({"type": "tournament"}) is False


def test_unknown_mechanic_value_falls_back_to_standard_drop():
    assert mp.resolve_mechanic({"mechanic": "something_new"}) == "standard_drop"


def test_mechanic_is_derived_from_type_server_side():
    assert mp.mechanic_for_type("mission_pool") == "mission_pool"
    assert mp.mechanic_for_type("tournament") == "standard_drop"
    assert mp.mechanic_for_type(None) == "standard_drop"


def test_mission_routes_reject_a_non_mission_campaign(fake_db):
    fake_db["gc_campaigns"].insert_one({"campaign_id": "tourney", "type": "tournament", "status": "live"})
    with _app().test_client() as client:
        resp = _submit(client, campaign_id="tourney")
    assert resp.status_code == 404
    assert resp.get_json()["code"] == "campaign_not_found"


# ---------------------------------------------------------------------------
# §33 answer validation + normalisation
# ---------------------------------------------------------------------------

def test_choice_answer_must_match_a_configured_option():
    cfg = {"mission_type": "multiple_choice", "options": [{"id": "a"}, {"id": "b"}], "correct_answer": "a"}
    assert mp.validate_submission(cfg, "a")["is_correct"] is True
    assert mp.validate_submission(cfg, "b")["is_correct"] is False
    with pytest.raises(mp.MissionValidationError) as exc:
        mp.validate_submission(cfg, "c")
    assert exc.value.code == "invalid_option"


def test_keyword_normalisation_is_conservative():
    cfg = {"mission_type": "keyword", "correct_answer": "Lucky  Spin", "keyword_case_insensitive": True}
    # NFKC + control-char strip + whitespace collapse + configured casefold.
    assert mp.validate_submission(cfg, "  lucky   spin ")["is_correct"] is True
    assert mp.validate_submission(cfg, "LUCKY SPIN")["is_correct"] is True
    # Punctuation is NOT stripped: two genuinely different answers stay different.
    assert mp.validate_submission(cfg, "lucky-spin")["is_correct"] is False


def test_keyword_case_sensitivity_is_opt_out():
    cfg = {"mission_type": "keyword", "correct_answer": "Lucky", "keyword_case_insensitive": False}
    assert mp.validate_submission(cfg, "Lucky")["is_correct"] is True
    assert mp.validate_submission(cfg, "lucky")["is_correct"] is False


def test_feedback_length_bounds_enforced_server_side():
    cfg = {"mission_type": "feedback", "min_chars": 5, "max_chars": 20}
    assert mp.validate_submission(cfg, "good enough")["answer_normalized"] == "good enough"
    with pytest.raises(mp.MissionValidationError) as short:
        mp.validate_submission(cfg, "hi")
    assert short.value.code == "answer_too_short"
    with pytest.raises(mp.MissionValidationError) as long:
        mp.validate_submission(cfg, "x" * 50)
    assert long.value.code == "answer_too_long"


def test_oversized_payload_rejected_before_any_normalisation():
    cfg = {"mission_type": "feedback"}
    with pytest.raises(mp.MissionValidationError) as exc:
        mp.validate_submission(cfg, "x" * (mp.MAX_ANSWER_CHARS + 1))
    assert exc.value.code == "answer_too_long"


@pytest.mark.parametrize("payload", [
    {"$ne": None},
    {"$gt": ""},
    [{"$ne": None}],
    ["a"],
    None,
    True,
])
def test_mongo_operator_shaped_payloads_are_rejected(payload):
    """A dict/list answer can never reach a query or a stored document."""
    cfg = {"mission_type": "keyword", "correct_answer": "x"}
    with pytest.raises(mp.MissionValidationError) as exc:
        mp.validate_submission(cfg, payload)
    assert exc.value.code == "invalid_answer_type"


def test_control_characters_are_stripped():
    cfg = {"mission_type": "feedback", "min_chars": 1, "max_chars": 100}
    out = mp.validate_submission(cfg, "hel\x00lo\x07 there")
    assert "\x00" not in out["answer_normalized"]
    assert out["answer_normalized"] == "hello there"


def test_invalid_mission_config_is_rejected_not_silently_accepted():
    with pytest.raises(mp.MissionValidationError) as exc:
        mp.validate_submission({"mission_type": "image_upload"}, "x")
    assert exc.value.code == "invalid_mission_config"
    with pytest.raises(mp.MissionValidationError):
        mp.validate_submission({"mission_type": "multiple_choice", "options": []}, "x")


# ---------------------------------------------------------------------------
# Admin-side campaign config validation
# ---------------------------------------------------------------------------

def test_validate_mission_config_rejects_bad_shapes():
    assert mp.validate_mission_config({"mission_type": "nope", "prompt": "x"})[1] == "invalid_mission_type"
    assert mp.validate_mission_config({"mission_type": "keyword"})[1] == "missing_mission_prompt"
    assert mp.validate_mission_config(
        {"mission_type": "multiple_choice", "prompt": "p", "options": [{"id": "a"}]}
    )[1] == "not_enough_options"
    assert mp.validate_mission_config(
        {"mission_type": "multiple_choice", "prompt": "p",
         "options": [{"id": "a"}, {"id": "a"}]}
    )[1] == "duplicate_option"
    assert mp.validate_mission_config(
        {"mission_type": "multiple_choice", "prompt": "p",
         "options": [{"id": "a"}, {"id": "b"}], "correct_answer": "z"}
    )[1] == "correct_answer_not_an_option"


def test_validate_mission_pool_config_bounds():
    assert mp.validate_mission_pool_config({"winner_count": 5})[1] == "missing_pool_id"
    assert mp.validate_mission_pool_config({"pool_id": "P", "winner_count": 0})[1] == "invalid_winner_count"
    assert mp.validate_mission_pool_config(
        {"pool_id": "P", "winner_count": 5, "allocation_method": "bribe"}
    )[1] == "invalid_allocation_method"
    cfg, err = mp.validate_mission_pool_config({"pool_id": "P", "winner_count": 5})
    assert err is None
    assert cfg["allocation_method"] == "random_qualified"
    assert cfg["eligibility_policy"]["exclude_voucher_hunter"] is True


def test_merge_preserves_worker_owned_processing_state():
    """An admin edit must never reset the fence or drop the selection seed."""
    existing = {
        "pool_id": "OLD", "winner_count": 1,
        "processing_generation": 7, "processing_stage": mp.STAGE_WINNERS_SELECTED,
        "selection_seed": "deadbeef", "qualified_count": 42,
    }
    validated, _ = mp.validate_mission_pool_config({"pool_id": "NEW", "winner_count": 9})
    merged = mp.merge_mission_pool_config(existing, validated)
    assert merged["pool_id"] == "NEW"
    assert merged["winner_count"] == 9
    assert merged["processing_generation"] == 7
    assert merged["processing_stage"] == mp.STAGE_WINNERS_SELECTED
    assert merged["selection_seed"] == "deadbeef"
    assert merged["qualified_count"] == 42


# ---------------------------------------------------------------------------
# §31 time window / campaign state
# ---------------------------------------------------------------------------

def test_submission_window_is_start_inclusive_end_exclusive():
    now = datetime.now(timezone.utc)
    campaign = _campaign(schedule={"starts_at": now, "ends_at": now + timedelta(hours=1)})
    assert mp.submission_state(campaign, now)[0] is True
    assert mp.submission_state(campaign, now - timedelta(seconds=1)) == (False, "campaign_not_started")
    # Exactly at end_at is CLOSED.
    assert mp.submission_state(campaign, now + timedelta(hours=1)) == (False, "campaign_closed")


def test_paused_and_cancelled_block_submissions():
    now = datetime.now(timezone.utc)
    paused = _campaign(status="paused")
    assert mp.submission_state(paused, now) == (False, "campaign_paused")
    block = dict(_campaign()["mission_pool"], cancelled=True)
    cancelled = _campaign(mission_pool=block)
    assert mp.submission_state(cancelled, now) == (False, "campaign_cancelled")


def test_paused_campaign_is_not_eligible_for_processing():
    """Pausing must halt the processor too, not just submissions."""
    now = datetime.now(timezone.utc)
    ended_window = {"starts_at": now - timedelta(hours=2), "ends_at": now - timedelta(hours=1)}
    assert mp.is_closed_for_processing(_campaign(status="live", schedule=ended_window), now) is True
    assert mp.is_closed_for_processing(_campaign(status="paused", schedule=ended_window), now) is False
    block = dict(_campaign()["mission_pool"], cancelled=True)
    assert mp.is_closed_for_processing(
        _campaign(status="ended", schedule=ended_window, mission_pool=block), now
    ) is False


# ---------------------------------------------------------------------------
# §9/§10/§32 submission hot path
# ---------------------------------------------------------------------------

def test_submission_creates_one_entry(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        resp = _submit(client)
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok", "submitted": True, "state": "submitted"}
    entries = fake_db[mp.ENTRIES_COLLECTION].find({})
    assert len(entries) == 1
    assert entries[0]["telegram_user_id"] == UID
    assert entries[0]["status"] == mp.ENTRY_STATUS_SUBMITTED
    assert entries[0]["is_correct"] is True
    # Identity resolution is deliberately deferred to the worker.
    assert entries[0]["identity_key"] is None


def test_repeated_submission_is_idempotent_not_a_duplicate(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        first = _submit(client)
        second = _submit(client, answer="b")
    assert first.get_json()["state"] == "submitted"
    assert second.status_code == 200
    assert second.get_json()["state"] == "already_submitted"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 1
    # The first answer stands; a retry never overwrites it.
    assert fake_db[mp.ENTRIES_COLLECTION].find({})[0]["answer_normalized"] == "a"


def test_unauthenticated_submission_rejected(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        resp = client.post(f"/api/mission-pool/{CAMPAIGN_ID}/submit", json={"answer": "a"})
    assert resp.status_code == 401
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_client_supplied_user_id_is_never_trusted(fake_db):
    """The submission owner is the verified initData user, even when the body
    and query string both claim someone else."""
    _seed(fake_db)
    with _app().test_client() as client:
        with _verified(UID):
            resp = client.post(
                f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=stub&user_id=999999",
                json={"answer": "a", "telegram_user_id": 999999, "uid": 999999},
            )
    assert resp.status_code == 200
    stored = fake_db[mp.ENTRIES_COLLECTION].find({})
    assert len(stored) == 1
    assert stored[0]["telegram_user_id"] == UID


def test_forged_initdata_rejected(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        with patch("vouchers.verify_telegram_init_data", return_value=(False, {}, "bad_hash")):
            resp = client.post(
                f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=forged", json={"answer": "a"}
            )
    assert resp.status_code == 401
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_submission_after_close_is_rejected(fake_db):
    now = datetime.now(timezone.utc)
    _seed(fake_db, schedule={"starts_at": now - timedelta(hours=2), "ends_at": now - timedelta(minutes=1)})
    with _app().test_client() as client:
        resp = _submit(client)
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "campaign_closed"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_admin_close_while_request_in_flight_blocks_the_write(fake_db):
    """The state re-check happens immediately before the write, so a close
    that lands mid-request still wins."""
    _seed(fake_db)
    real_validate = mp.validate_submission

    def close_then_validate(cfg, answer):
        out = real_validate(cfg, answer)
        fake_db["gc_campaigns"].update_one({"campaign_id": CAMPAIGN_ID}, {"$set": {"status": "ended"}})
        return out

    with _app().test_client() as client:
        with patch.object(mp, "validate_submission", side_effect=close_then_validate):
            resp = _submit(client)
    assert resp.status_code == 409
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_invalid_answer_rejected_without_writing(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        resp = _submit(client, answer="not-an-option")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_option"
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_malformed_json_body_is_rejected_not_crashed(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        with _verified(UID):
            resp = client.post(
                f"/api/mission-pool/{CAMPAIGN_ID}/submit?init_data=stub",
                data="{not json", content_type="application/json",
            )
    assert resp.status_code == 400


def test_submission_hot_path_touches_only_two_collections(fake_db, monkeypatch):
    """§9: the synchronous request may read gc_campaigns and write
    mission_entries — nothing else. In particular it must never read `users`,
    `vouchers`, `voucher_pools` or any risk snapshot."""
    _seed(fake_db)
    touched = set()

    original_getitem = type(fake_db).__getitem__

    def tracking_getitem(self, name):
        touched.add(name)
        return original_getitem(self, name)

    monkeypatch.setattr(type(fake_db), "__getitem__", tracking_getitem)
    with _app().test_client() as client:
        resp = _submit(client)
    assert resp.status_code == 200
    forbidden = {"users", "vouchers", "voucher_pools", "drops", "voucher_claims", "segment_snapshots"}
    assert not (touched & forbidden), f"hot path touched {touched & forbidden}"


# ---------------------------------------------------------------------------
# §30/§42 kill switch
# ---------------------------------------------------------------------------

def test_kill_switch_blocks_every_public_route(fake_db, monkeypatch):
    _seed(fake_db)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: False)
    with _app().test_client() as client:
        assert _submit(client).status_code == 503
        with _verified(UID):
            assert client.get(f"/api/mission-pool/{CAMPAIGN_ID}?init_data=stub").status_code == 503
    assert fake_db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


def test_env_kill_switch_hard_off_beats_settings(monkeypatch):
    monkeypatch.setenv("MISSION_POOL_ENABLED", "0")
    monkeypatch.setattr("settings_service.get_setting", lambda *a, **k: True)
    assert mp.mission_pool_enabled() is False


def test_feature_flag_off_disables_even_with_env_on(monkeypatch):
    monkeypatch.setenv("MISSION_POOL_ENABLED", "1")
    monkeypatch.setattr("settings_service.get_setting", lambda *a, **k: False)
    assert mp.mission_pool_enabled() is False


def test_settings_outage_fails_closed(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("settings down")

    monkeypatch.setattr("settings_service.get_setting", boom)
    monkeypatch.delenv("MISSION_POOL_ENABLED", raising=False)
    assert mp.mission_pool_enabled() is False
    monkeypatch.setenv("MISSION_POOL_ENABLED", "1")
    assert mp.mission_pool_enabled() is True


# ---------------------------------------------------------------------------
# Discovery endpoint
# ---------------------------------------------------------------------------

def test_get_mission_never_leaks_the_correct_answer(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        with _verified(UID):
            resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}?init_data=stub")
    body = resp.get_json()
    assert resp.status_code == 200
    assert "correct_answer" not in body["mission"]
    assert body["mission"]["options"] == [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}]
    assert body["submissions_open"] is True
    assert body["already_submitted"] is False


# ---------------------------------------------------------------------------
# Admin controls (§30/§40)
# ---------------------------------------------------------------------------

def _admin():
    return patch("vouchers.require_admin", return_value=({"usernameLower": "gracy_ap"}, None))


def test_admin_routes_require_admin(fake_db):
    _seed(fake_db)
    with _app().test_client() as client:
        with patch("vouchers.require_admin", return_value=(None, ({"error": "no"}, 401))):
            assert client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/cancel").status_code == 401
            assert client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/close").status_code == 401
            assert client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/summary").status_code == 401


def test_admin_close_then_cancel_semantics(fake_db):
    _seed(fake_db)
    with _app().test_client() as client, _admin():
        assert client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/close").status_code == 200
        doc = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
        assert doc["status"] == "ended"
        assert mp.is_closed_for_processing(doc) is True

        assert client.post(f"/api/admin/mission-pool/{CAMPAIGN_ID}/cancel").status_code == 200
        doc = fake_db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
        assert doc["mission_pool"]["cancelled"] is True
        # Cancelled campaigns stop processing entirely.
        assert mp.is_closed_for_processing(doc) is False


def test_admin_routes_reject_a_standard_drop_campaign(fake_db):
    fake_db["gc_campaigns"].insert_one({"campaign_id": "t1", "type": "tournament", "status": "live"})
    with _app().test_client() as client, _admin():
        assert client.post("/api/admin/mission-pool/t1/cancel").status_code == 404


def test_reward_idempotency_key_is_stable_and_leaks_no_identity():
    key = mp.reward_idempotency_key("camp", "entry123")
    assert key == "MISSION:camp:entry123"
    assert mp.mission_reward_id("camp", "entry123") == mp.mission_reward_id("camp", "entry123")
    assert mp.mission_reward_id("camp", "entry123") != mp.mission_reward_id("camp", "entry124")


def test_identity_key_masking():
    assert mp.mask_identity_key("acct:ABC12345") == "acct:***345"
    assert mp.mask_identity_key("tg:1") == "tg:***"
    assert mp.mask_identity_key(None) is None
