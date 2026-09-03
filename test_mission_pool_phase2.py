"""Phase 2 backend contracts — mission_pool_ux.py.

Covers the Phase 2 presentation layer only. Phase 1's engine
(test_mission_pool*.py, 121 tests) is unchanged and asserted separately.

What these tests exist to protect:
  * the Mission UI can only ever activate for a real Mission campaign (§5)
  * a deep link is a navigation reference and nothing more (§6, §42)
  * a disqualified participant is indistinguishable from a non-winner, in
    both content AND timing (§44)
  * the admin editor's freeze/lifecycle/pool answers come from the same
    rules the write paths enforce (§26, §27, §29)
  * the winner CTA navigates and never allocates (§14)
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from flask import Flask

import campaign_centre as cc
import database
import mission_pool as mp
import mission_pool_ux as ux
from fake_mongo import FakeDb

CAMPAIGN_ID = "mission-phase2"
UID = 777001
OTHER_UID = 777002


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
    monkeypatch.setattr(ux, "database", database)
    monkeypatch.setattr(cc, "database", database)
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: True)
    return fdb


def _app():
    app = Flask(__name__)
    app.register_blueprint(ux.mission_pool_ux_bp)
    app.register_blueprint(ux.mission_pool_ux_admin_bp)
    return app


def _verified(uid: int):
    return patch(
        "vouchers.verify_telegram_init_data",
        return_value=(True, {"user": f'{{"id": {uid}}}'}, "ok"),
    )


def _admin_ok():
    return patch("vouchers.require_admin", return_value=({"usernameLower": "ops"}, None))


def _campaign(**overrides):
    now = datetime.now(timezone.utc)
    doc = {
        "campaign_id": CAMPAIGN_ID,
        "name": "Phase 2 Mission",
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


def _insert(fake_db, doc):
    database.db["gc_campaigns"].insert_one(doc)


def _entry(status=mp.ENTRY_STATUS_SUBMITTED, uid=UID, campaign_id=CAMPAIGN_ID):
    now = datetime.now(timezone.utc)
    return {
        "campaign_id": campaign_id, "telegram_user_id": uid, "status": status,
        "answer": "a", "answer_normalized": "a", "is_correct": True,
        "submitted_at": now, "created_at": now, "updated_at": now,
    }


# ---------------------------------------------------------------------------
# Deep link (§6, §41)
# ---------------------------------------------------------------------------

def test_mission_deep_link_is_built_in_one_place(monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "AdvantPlayBot")
    assert ux.mission_deep_link("summer-quiz") == \
        "https://t.me/AdvantPlayBot?startapp=mission_summer-quiz"


def test_mission_deep_link_is_none_without_bot_username(monkeypatch):
    monkeypatch.delenv("BOT_USERNAME", raising=False)
    assert ux.mission_deep_link("summer-quiz") is None


def test_mission_deep_link_refuses_an_unsafe_campaign_id(monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "AdvantPlayBot")
    # Telegram start params allow only [A-Za-z0-9_-]; a half-built link that
    # silently drops characters would open the wrong campaign.
    assert ux.mission_deep_link("summer quiz") is None
    assert ux.mission_deep_link("summer/quiz") is None
    assert ux.mission_deep_link("x" * 100) is None


def test_parse_mission_start_param_roundtrips():
    assert ux.parse_mission_start_param(ux.mission_start_param("abc-1")) == "abc-1"


def test_parse_mission_start_param_ignores_the_existing_ad_attribution_param():
    # main.py /go already issues ?startapp=attr_<token>; it must keep working.
    assert ux.parse_mission_start_param("attr_deadbeef01") is None
    assert ux.parse_mission_start_param("") is None
    assert ux.parse_mission_start_param(None) is None
    assert ux.parse_mission_start_param("mission_") is None


def test_winner_cta_is_navigation_only(monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "AdvantPlayBot")
    markup = ux.winner_cta_reply_markup(CAMPAIGN_ID)
    button = markup["inline_keyboard"][0][0]
    assert button["text"] == "🎁 Redeem Reward"
    # A url button navigates. No voucher code, no reward id, no allocation
    # trigger is carried in the CTA (§14).
    assert button["url"].endswith("?startapp=mission_" + CAMPAIGN_ID)
    assert set(button.keys()) == {"text", "url"}


def test_winner_cta_absent_rather_than_broken(monkeypatch):
    monkeypatch.delenv("BOT_USERNAME", raising=False)
    assert ux.winner_cta_reply_markup(CAMPAIGN_ID) is None


def test_winner_notification_falls_back_to_text_without_a_cta(monkeypatch):
    import mission_pool_processor as proc

    monkeypatch.delenv("BOT_USERNAME", raising=False)
    text, markup = proc._winner_message(CAMPAIGN_ID, "Phase 2 Mission")
    assert markup is None
    # Never instructs the user to tap a button that is not there.
    assert "Tap below" not in text
    assert "Phase 2 Mission" in text


def test_winner_notification_uses_the_cta_copy_when_a_link_exists(monkeypatch):
    import mission_pool_processor as proc

    monkeypatch.setenv("BOT_USERNAME", "AdvantPlayBot")
    text, markup = proc._winner_message(CAMPAIGN_ID, "Phase 2 Mission")
    assert markup is not None
    assert text == (
        "🎉 Congratulations!\n\n"
        "You've been selected as a winner of Phase 2 Mission!\n\n"
        "Your reward is now available in Campaign Rewards.\n\n"
        "Tap below to redeem your code."
    )


# ---------------------------------------------------------------------------
# User-facing state machine (§13, §44)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status,entry_status,stage,expected", [
    ("live", None, mp.STAGE_PENDING, ux.STATE_LIVE),
    ("live", mp.ENTRY_STATUS_SUBMITTED, mp.STAGE_PENDING, ux.STATE_SUBMITTED),
    ("paused", None, mp.STAGE_PENDING, ux.STATE_PAUSED),
    ("paused", mp.ENTRY_STATUS_SUBMITTED, mp.STAGE_PENDING, ux.STATE_PAUSED),
    ("ended", mp.ENTRY_STATUS_QUALIFIED, mp.STAGE_SELECTING_WINNERS, ux.STATE_CLOSED_PROCESSING),
    ("ended", None, mp.STAGE_COMPLETED, ux.STATE_ENDED),
    ("ended", mp.ENTRY_STATUS_WINNER, mp.STAGE_COMPLETED, ux.STATE_WON),
    ("ended", mp.ENTRY_STATUS_REWARD_ALLOCATED, mp.STAGE_COMPLETED, ux.STATE_WON),
    ("ended", mp.ENTRY_STATUS_NON_WINNER, mp.STAGE_COMPLETED, ux.STATE_NOT_WON),
])
def test_user_state_matrix(status, entry_status, stage, expected):
    campaign = _campaign(status=status)
    campaign["mission_pool"]["processing_stage"] = stage
    entry = _entry(entry_status) if entry_status else None
    assert ux.user_state(campaign, entry) == expected


def test_scheduled_campaign_reports_scheduled():
    now = datetime.now(timezone.utc)
    campaign = _campaign(schedule={"starts_at": now + timedelta(days=1), "ends_at": now + timedelta(days=2)})
    assert ux.user_state(campaign, None) == ux.STATE_SCHEDULED


def test_cancelled_wins_over_every_other_state():
    campaign = _campaign(status="ended")
    campaign["mission_pool"]["cancelled"] = True
    campaign["mission_pool"]["processing_stage"] = mp.STAGE_COMPLETED
    assert ux.user_state(campaign, _entry(mp.ENTRY_STATUS_WINNER)) == ux.STATE_CANCELLED


def test_disqualified_is_indistinguishable_from_a_non_winner():
    """§44: no state, and no field, may reveal an anti-abuse exclusion."""
    campaign = _campaign(status="ended")
    campaign["mission_pool"]["processing_stage"] = mp.STAGE_COMPLETED
    dq = _entry(mp.ENTRY_STATUS_DISQUALIFIED)
    dq["disqualification_reason"] = mp.REASON_VOUCHER_HUNTER
    assert ux.user_state(campaign, dq) == ux.STATE_NOT_WON
    assert ux.user_state(campaign, _entry(mp.ENTRY_STATUS_NON_WINNER)) == ux.STATE_NOT_WON


def test_disqualification_is_not_revealed_early_by_timing():
    """An entry is stamped `disqualified` during the eligibility pass, well
    before selection. If the result state were derived from the entry status
    alone, an excluded participant would learn their outcome before anyone
    else did — a timing side channel. Result states wait for `completed`."""
    campaign = _campaign(status="ended")
    campaign["mission_pool"]["processing_stage"] = mp.STAGE_PROCESSING_ELIGIBILITY
    dq = _entry(mp.ENTRY_STATUS_DISQUALIFIED)
    dq["disqualification_reason"] = mp.REASON_MULTI_ACCOUNT_RISK
    assert ux.user_state(campaign, dq) == ux.STATE_CLOSED_PROCESSING
    assert ux.user_state(campaign, _entry(mp.ENTRY_STATUS_QUALIFIED)) == ux.STATE_CLOSED_PROCESSING


# ---------------------------------------------------------------------------
# /view endpoint (§5, §22, §42)
# ---------------------------------------------------------------------------

def test_view_returns_everything_the_card_needs_in_one_call(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _verified(UID):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["mechanic"] == "mission_pool"
    assert data["user_state"] == ux.STATE_LIVE
    assert data["campaign_name"] == "Phase 2 Mission"
    assert data["winner_count"] == 3
    assert data["schedule"]["ends_at"]
    assert [o["id"] for o in data["mission"]["options"]] == ["a", "b"]


def test_view_never_exposes_the_correct_answer(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _verified(UID):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    assert "correct_answer" not in resp.get_json()["mission"]


def test_view_404s_for_a_standard_drop_campaign(fake_db):
    """§5: Mission UI must never activate for a non-Mission campaign, even
    when the caller names one explicitly."""
    _insert(fake_db, {
        "campaign_id": "tourney-1", "name": "Tournament", "type": "tournament",
        "mechanic": "standard_drop", "status": "live",
    })
    with _app().test_client() as client, _verified(UID):
        resp = client.get("/api/mission-pool/tourney-1/view?init_data=x")
    assert resp.status_code == 404


def test_view_404s_for_a_legacy_campaign_with_no_mechanic_field(fake_db):
    _insert(fake_db, {"campaign_id": "legacy-1", "name": "Legacy", "type": "tournament", "status": "live"})
    with _app().test_client() as client, _verified(UID):
        resp = client.get("/api/mission-pool/legacy-1/view?init_data=x")
    assert resp.status_code == 404


def test_view_404s_for_a_forged_campaign_id(fake_db):
    with _app().test_client() as client, _verified(UID):
        resp = client.get("/api/mission-pool/does-not-exist/view?init_data=x")
    assert resp.status_code == 404


def test_view_requires_authentication(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, patch(
        "vouchers.verify_telegram_init_data", return_value=(False, None, "bad_signature")
    ):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=forged")
    assert resp.status_code in (401, 403)


def test_view_is_off_when_the_kill_switch_is_off(fake_db, monkeypatch):
    monkeypatch.setattr(mp, "mission_pool_enabled", lambda: False)
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _verified(UID):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    assert resp.status_code == 503


def test_view_reports_another_users_entry_as_not_submitted(fake_db):
    """Submission state is per authenticated identity, never global."""
    _insert(fake_db, _campaign())
    database.db[mp.ENTRIES_COLLECTION].insert_one(_entry(uid=OTHER_UID))
    with _app().test_client() as client, _verified(UID):
        resp = client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    data = resp.get_json()
    assert data["already_submitted"] is False
    assert data["user_state"] == ux.STATE_LIVE


def test_view_is_read_only(fake_db):
    """Phase 2 must not mutate Phase 1 state. Nothing this endpoint touches
    may gain, lose or change a document."""
    _insert(fake_db, _campaign())
    before = database.db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    with _app().test_client() as client, _verified(UID):
        client.get(f"/api/mission-pool/{CAMPAIGN_ID}/view?init_data=x")
    after = database.db["gc_campaigns"].find_one({"campaign_id": CAMPAIGN_ID})
    assert before == after
    assert database.db[mp.ENTRIES_COLLECTION].count_documents({}) == 0


# ---------------------------------------------------------------------------
# Admin edit-state (§26, §27, §41)
# ---------------------------------------------------------------------------

def test_edit_state_reports_unlocked_before_any_entry(fake_db, monkeypatch):
    monkeypatch.setenv("BOT_USERNAME", "AdvantPlayBot")
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _admin_ok():
        data = client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/edit-state").get_json()
    assert data["mission_config_locked"] is False
    assert data["entries"] == 0
    assert data["locked_fields"] == []
    assert data["mission_link"].endswith("?startapp=mission_" + CAMPAIGN_ID)


def test_edit_state_locks_mission_config_once_an_entry_exists(fake_db):
    """§26: the UI is told to disable the fields BEFORE the operator types,
    using the same 'does an entry exist' rule campaign_centre enforces."""
    _insert(fake_db, _campaign())
    database.db[mp.ENTRIES_COLLECTION].insert_one(_entry())
    with _app().test_client() as client, _admin_ok():
        data = client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/edit-state").get_json()
    assert data["mission_config_locked"] is True
    assert data["entries"] == 1
    assert set(data["locked_fields"]) == set(ux.MISSION_CONFIG_FIELDS)


def test_edit_state_freeze_agrees_with_the_backend_write_path(fake_db):
    """The advisory flag and the authoritative rejection must never diverge:
    if edit-state says locked, the PUT must actually refuse the change."""
    _insert(fake_db, _campaign())
    database.db[mp.ENTRIES_COLLECTION].insert_one(_entry())

    app = Flask(__name__)
    app.register_blueprint(ux.mission_pool_ux_admin_bp)
    app.register_blueprint(cc.campaign_centre_bp)
    with app.test_client() as client, _admin_ok():
        state = client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/edit-state").get_json()
        resp = client.put(f"/api/admin/gc-campaigns/{CAMPAIGN_ID}", json={
            "mission_config": {
                "mission_type": "keyword", "prompt": "Changed", "correct_answer": "x",
            },
        })
    assert state["mission_config_locked"] is True
    assert resp.status_code == 409
    assert resp.get_json()["code"] == "mission_config_locked"


def test_edit_state_reports_schedule_editability_independently(fake_db):
    """§27: mission_config being frozen does NOT imply the schedule is."""
    _insert(fake_db, _campaign())
    database.db[mp.ENTRIES_COLLECTION].insert_one(_entry())
    with _app().test_client() as client, _admin_ok():
        data = client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/edit-state").get_json()
    assert data["mission_config_locked"] is True
    assert data["schedule_editable"] is True


def _admin_denied():
    return patch("vouchers.require_admin", return_value=(None, ("unauthorized", 401)))


def test_edit_state_requires_admin(fake_db):
    _insert(fake_db, _campaign())
    with _app().test_client() as client, _admin_denied():
        resp = client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/edit-state")
    assert resp.status_code == 401


def test_edit_state_404s_for_a_non_mission_campaign(fake_db):
    _insert(fake_db, {"campaign_id": "t1", "type": "tournament", "mechanic": "standard_drop", "status": "live"})
    with _app().test_client() as client, _admin_ok():
        resp = client.get("/api/admin/mission-pool/t1/edit-state")
    assert resp.status_code == 404


def test_edit_state_surfaces_closed_at_without_ever_moving_it(fake_db):
    closed = datetime.now(timezone.utc) - timedelta(minutes=5)
    campaign = _campaign(status="ended")
    campaign["mission_pool"]["closed_at"] = closed
    _insert(fake_db, campaign)
    with _app().test_client() as client, _admin_ok():
        first = client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/edit-state").get_json()
        second = client.get(f"/api/admin/mission-pool/{CAMPAIGN_ID}/edit-state").get_json()
    assert first["closed_at"] == second["closed_at"] == closed.isoformat()


def test_admin_pools_only_lists_mission_compatible_pools(fake_db):
    """§29: the filter is the backend's own rule, so a protected affiliate or
    welcome pool can never be offered to a Mission campaign."""
    import voucher_pool_service

    registry = database.db["voucher_pool_registry"]
    now = datetime.now(timezone.utc)
    for pool_id, scope in [
        ("MISSION-OK", "campaign_rewards"),
        ("SHARED-OK", "shared"),
        ("AFFILIATE-NO", "affiliate_rewards"),
        ("DROPS-NO", "voucher_drops"),
        ("WELCOME", "campaign_rewards"),  # reserved legacy id
        ("T1", "shared"),                 # reserved legacy id
    ]:
        registry.insert_one({
            "pool_id": pool_id, "name": pool_id, "pool_type": "voucher_drop",
            "allocation_scope": scope, "status": "active", "created_at": now,
        })

    with _app().test_client() as client, _admin_ok():
        data = client.get("/api/admin/mission-pool/pools").get_json()
    listed = {p["pool_id"] for p in data["pools"]}
    assert listed == {"MISSION-OK", "SHARED-OK"}
    for reserved in voucher_pool_service.RESERVED_LEGACY_POOL_IDS:
        assert reserved not in listed


def test_admin_pools_requires_admin(fake_db):
    with _app().test_client() as client, _admin_denied():
        assert client.get("/api/admin/mission-pool/pools").status_code == 401


# ---------------------------------------------------------------------------
# Phase 1 boundary (§4)
# ---------------------------------------------------------------------------

def test_phase2_module_performs_no_writes():
    """Every Phase 2 route is a GET, and the module must not contain a single
    write call against a Phase 1 collection. This is an AST-level assertion so
    it cannot be satisfied by a comment."""
    import ast

    source = open("mission_pool_ux.py").read()
    tree = ast.parse(source)
    write_calls = {"insert_one", "insert_many", "update_one", "update_many",
                   "delete_one", "delete_many", "find_one_and_update",
                   "replace_one", "bulk_write"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in write_calls, f"Phase 2 must not write: {node.func.attr}"


def _executable_source(path: str) -> str:
    """Module source with every docstring removed, so a prose reference in a
    comment/docstring never satisfies (or fails) a code-level assertion."""
    import ast

    tree = ast.parse(open(path).read())
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(ast.fix_missing_locations(tree))


def test_phase2_does_not_reimplement_phase1_eligibility():
    """Phase 2 must consume Phase 1's decisions, not recompute them. It may
    not reference the anti-abuse flags, the selection seed or the identity
    machinery in executable code at all."""
    source = _executable_source("mission_pool_ux.py")
    for forbidden in ("linked_gaming_accounts", "multi_account_risk",
                      "voucher_hunter", "selection_seed", "identity_key",
                      "resolve_effective_segment", "allocate_voucher"):
        assert forbidden not in source, f"Phase 2 must not touch {forbidden}"
