"""Phase 1 regression coverage: public-voucher eligibility must be enforced
for every region at BOTH the claimability pre-check and the allocation point
(claim_pooled), and multi-account-risk players outside the New Player
exception must be refused.

Probabilities are deterministic: ``random.random`` is pinned per test, so a
roll of 0.05 always passes the 10% Voucher-Hunter gate and 0.50 always fails
it. Collections are real mongomock collections (unique (user_id,
public_pool_id) index included) so allocation, fall-through and the
one-roll-per-(user,drop) idempotency behave as in production.
"""
from datetime import datetime, timedelta, timezone

import mongomock
import pytest
from bson import ObjectId
from flask import Flask
from pymongo.errors import DuplicateKeyError  # noqa: F401  (documents the index contract)

import vouchers as m

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
VH_PASS_ROLL = 0.05   # < 0.10 -> VH gate passes
VH_FAIL_ROLL = 0.50   # > 0.10 -> VH gate denies


class _Env:
    def __init__(self, monkeypatch):
        self.mp = monkeypatch
        client = mongomock.MongoClient()
        self.db = client["referral_bot"]
        self.db.public_pool_access_assignments.create_index(
            [("user_id", 1), ("public_pool_id", 1)], unique=True
        )
        self.roll = VH_PASS_ROLL
        self.rolls = 0
        monkeypatch.setattr(m, "db", self.db)
        monkeypatch.setattr(m, "users_collection", self.db.users)
        monkeypatch.setattr(m, "public_pool_access_assignments_col", self.db.public_pool_access_assignments)
        monkeypatch.setattr(m, "_is_retained_days", lambda uid, min_days: False)
        monkeypatch.setattr(m, "now_utc", lambda: NOW)
        monkeypatch.setattr(m.random, "randint", lambda a, b: 0)

        def _roll():
            self.rolls += 1
            return self.roll

        monkeypatch.setattr(m.random, "random", _roll)
        monkeypatch.delenv(m.MULTI_ACCOUNT_PUBLIC_BLOCK_ENV, raising=False)
        monkeypatch.delenv(m.MULTI_ACCOUNT_PUBLIC_BLOCK_INCLUDE_VH_ENV, raising=False)

    def user(self, uid, *, segment="high_value", region=None, multi=False, age=None, backend=None):
        doc = {"user_id": uid, "for_bot_segment": segment, "multi_account_risk": multi}
        if region:
            doc["region"] = region
        if age:
            doc["player_age_type"] = age
        if backend:
            doc["backend_segment"] = backend
        self.db.users.insert_one(doc)
        return doc

    def drop(self, *, public=3, my=0, audience="public", dtype="pooled"):
        drop_id = ObjectId()
        self.db.drops.insert_one({
            "_id": drop_id,
            "type": dtype,
            "audience": {"type": audience},
            "eligibility": {"mode": "public"},
            "status": "active",
            "startsAt": NOW - timedelta(hours=1),
            "endsAt": NOW + timedelta(hours=1),
            "public_remaining": public,
            "my_remaining": my,
            "reserve_enabled": False,
        })
        rows = [
            {"type": "pooled", "dropId": str(drop_id), "status": "free", "pool": "public",
             "code": f"PUB{i}", "createdAt": NOW}
            for i in range(public)
        ] + [
            {"type": "pooled", "dropId": str(drop_id), "status": "free", "pool": "my",
             "code": f"MY{i}", "createdAt": NOW}
            for i in range(my)
        ]
        if rows:
            self.db.vouchers.insert_many(rows)
        return self.db.drops.find_one({"_id": drop_id})

    def claimability(self, drop, uid, *, region=None):
        return m._pooled_claimability_state(
            drop=drop, drop_id=str(drop["_id"]), user_region=region, uid=uid,
            is_my_user=m._is_my_region(region), ref=NOW,
        )

    def claim(self, drop, uid):
        return m.claim_voucher_for_user(user_id=str(uid), drop_id=str(drop["_id"]), username=f"u{uid}")

    def assignment(self, drop, uid):
        return self.db.public_pool_access_assignments.find_one(
            {"user_id": uid, "public_pool_id": str(drop["_id"])}
        )

    def free(self, drop, pool):
        return self.db.vouchers.count_documents(
            {"dropId": str(drop["_id"]), "status": "free", "pool": pool}
        )


@pytest.fixture
def env(monkeypatch):
    with Flask(__name__).app_context():
        yield _Env(monkeypatch)


# --- 1/2. VH probability is unchanged and region-independent -----------------

@pytest.mark.parametrize("region", ["my", None, "sg"])
def test_vh_probability_is_ten_percent_in_every_region(env, region):
    env.user(1, segment="voucher_hunter", region=region)
    drop = env.drop(public=2, my=0)
    env.roll = VH_PASS_ROLL
    assert env.claimability(drop, 1, region=region)["claimable"] is True
    assignment = env.assignment(drop, 1)
    assert assignment["probability"] == 0.10
    assert assignment["probability_source"] == "for_bot_segment"
    assert assignment["multi_account_hard_block_applied"] is False


@pytest.mark.parametrize("region", ["my", None])
def test_vh_failing_roll_is_denied_in_every_region(env, region):
    env.user(2, segment="voucher_hunter", region=region)
    drop = env.drop(public=2, my=0)
    env.roll = VH_FAIL_ROLL
    state = env.claimability(drop, 2, region=region)
    assert state["claimable"] is False and state["reason"] == "shaping_denied"
    with pytest.raises(m.NotEligible):
        env.claim(drop, 2)
    assert env.free(drop, "public") == 2


def test_other_segment_probabilities_unchanged(env):
    # HV 50%, normal_actual 70%, ghost 5% -- the model is untouched.
    for uid, seg, expected in ((10, "high_value", 0.5), (11, "normal_actual", 0.7), (12, "ghost", 0.05)):
        env.user(uid, segment=seg, region="my")
        drop = env.drop(public=1, my=0)
        env.claimability(drop, uid, region="my")
        assert env.assignment(drop, uid)["probability"] == expected


# --- 3/4. Multi-account outside the New Player exception is blocked ----------

@pytest.mark.parametrize("region", ["my", None])
def test_multi_account_player_is_blocked_even_with_a_passing_roll(env, region):
    env.user(20, segment="high_value", region=region, multi=True)
    drop = env.drop(public=2, my=0)
    env.roll = 0.0  # would pass any non-zero probability
    state = env.claimability(drop, 20, region=region)
    assert state["claimable"] is False and state["reason"] == "multi_account_blocked"
    assignment = env.assignment(drop, 20)
    assert assignment["probability"] == 0.0
    assert assignment["multi_account_hard_block_applied"] is True
    # Base segment probability and the configured risk modifier are recorded unchanged.
    assert assignment["base_probability_pre_risk"] == 0.5
    assert assignment["risk_category"] == "multi_account_only"
    with pytest.raises(m.NotEligible):
        env.claim(drop, 20)
    assert env.free(drop, "public") == 2


def test_multi_account_block_applies_to_ghost_and_unclassified_too(env):
    for uid, seg in ((21, "ghost"), (22, None)):
        env.user(uid, segment=seg, region="my", multi=True)
        drop = env.drop(public=1, my=0)
        env.roll = 0.0
        assert env.claimability(drop, uid, region="my")["reason"] == "multi_account_blocked"


def test_flag_set_after_assignment_still_blocks_at_allocation(env):
    env.user(23, segment="high_value", region="my", multi=False)
    drop = env.drop(public=1, my=0)
    env.roll = 0.0
    assert env.claimability(drop, 23, region="my")["claimable"] is True  # roll persisted: allowed
    env.db.users.update_one({"user_id": 23}, {"$set": {"multi_account_risk": True}})
    assert env.claimability(drop, 23, region="my")["reason"] == "multi_account_blocked"
    with pytest.raises(m.NotEligible):
        env.claim(drop, 23)
    assert env.free(drop, "public") == 1


def test_kill_switch_restores_probability_modifier_only(env, monkeypatch):
    monkeypatch.setenv(m.MULTI_ACCOUNT_PUBLIC_BLOCK_ENV, "false")
    env.user(24, segment="high_value", region="my", multi=True)
    drop = env.drop(public=1, my=0)
    env.roll = 0.0
    assert env.claimability(drop, 24, region="my")["claimable"] is True
    assignment = env.assignment(drop, 24)
    assert assignment["multi_account_hard_block_applied"] is False
    assert assignment["probability"] == pytest.approx(0.5 * 0.25)


# --- VH + multi-account keeps its configured band (no hard block for VH) ----

def test_vh_plus_multi_account_keeps_configured_band_not_hard_block(env):
    env.user(30, segment="voucher_hunter", region="my", multi=True)
    drop = env.drop(public=1, my=0)
    env.roll = 0.0
    env.claimability(drop, 30, region="my")
    assignment = env.assignment(drop, 30)
    assert assignment["risk_category"] == "behavioral_and_multi_account"
    assert 0.05 <= assignment["probability"] <= 0.10
    assert assignment["multi_account_hard_block_applied"] is False


def test_vh_plus_multi_account_hard_block_is_opt_in(env, monkeypatch):
    monkeypatch.setenv(m.MULTI_ACCOUNT_PUBLIC_BLOCK_INCLUDE_VH_ENV, "true")
    env.user(31, segment="voucher_hunter", region="my", multi=True)
    drop = env.drop(public=1, my=0)
    env.roll = 0.0
    assert env.claimability(drop, 31, region="my")["reason"] == "multi_account_blocked"


# --- 5/6. New Player exception and boost are untouched -----------------------
#
# What is live today: check_public_pool_access() calls
# assign_public_pool_access_once() WITHOUT user_doc, so player_age_type /
# backend_segment are not read on the claim path and the effective New Player
# exception is the legacy for_bot_segment new_user/new_joiner boost. The
# player_age_type == "new_player" override is exercised only when a caller
# passes user_doc. Both are pinned here exactly as implemented.

@pytest.mark.parametrize("region", ["my", None])
@pytest.mark.parametrize("segment", ["new_user", "new_joiner"])
def test_live_new_player_boost_unchanged(env, region, segment):
    env.user(40, segment=segment, region=region)
    drop = env.drop(public=1, my=0)
    env.roll = 0.99
    assert env.claimability(drop, 40, region=region)["claimable"] is True
    assignment = env.assignment(drop, 40)
    assert assignment["probability"] == 1.0
    assert assignment["probability_source"] == "for_bot_segment_new_user_svd_boost"
    assert env.claim(drop, 40)["code"].startswith("PUB")


@pytest.mark.parametrize("region", ["my", None])
def test_live_new_player_with_multi_account_risk_keeps_exception(env, region):
    env.user(41, segment="new_user", region=region, multi=True)
    drop = env.drop(public=1, my=0)
    env.roll = 0.99
    assert env.claimability(drop, 41, region=region)["claimable"] is True
    assignment = env.assignment(drop, 41)
    assert assignment["probability"] == 1.0
    assert assignment["multi_account_hard_block_applied"] is False
    assert env.claim(drop, 41)["code"].startswith("PUB")


def test_new_player_override_via_user_doc_unchanged_and_wins_over_multi_account(env):
    for uid, multi in ((44, False), (45, True)):
        assignment = m.assign_public_pool_access_once(
            uid, "drop-direct", NOW,
            user_doc={"for_bot_segment": "ghost", "player_age_type": "new_player", "multi_account_risk": multi},
        )
        assert assignment["probability"] == 1.0
        assert assignment["probability_source"] == "new_player_override"
        assert assignment["new_player_override_applied"] is True
        assert assignment["multi_account_hard_block_applied"] is False


def test_live_gate_still_does_not_read_player_age_type(env):
    """Characterisation: wiring player_age_type into the live gate would
    change the New Player definition, which this patch must not do."""
    env.user(46, segment="ghost", region="my", age="new_player")
    drop = env.drop(public=1, my=0)
    env.roll = 0.99
    env.claimability(drop, 46, region="my")
    assignment = env.assignment(drop, 46)
    assert assignment["new_player_override_applied"] is False
    assert assignment["probability"] == 0.05   # ghost, unboosted -- as before


def test_new_player_exception_expires_after_three_opportunities_then_blocks(env):
    env.user(43, segment="new_user", region="my", multi=True)
    env.roll = 0.0
    for _ in range(3):
        drop = env.drop(public=1, my=0)
        assert env.claimability(drop, 43, region="my")["claimable"] is True
    drop = env.drop(public=1, my=0)
    assert env.claimability(drop, 43, region="my")["reason"] == "multi_account_blocked"


# --- 7. MY reserve available: unchanged allocation, no public restriction ----

def test_my_reserve_available_allocates_from_my_pool_without_public_gate(env):
    env.user(50, segment="voucher_hunter", region="my", multi=True)
    drop = env.drop(public=2, my=2)
    env.roll = VH_FAIL_ROLL
    state = env.claimability(drop, 50, region="my")
    assert state["claimable"] is True and state["remaining"] == 4
    assert env.claim(drop, 50)["code"] == "MY0"
    assert env.free(drop, "my") == 1 and env.free(drop, "public") == 2
    assert env.assignment(drop, 50) is None   # no public roll was consumed
    assert env.rolls == 0


def test_multi_account_my_user_still_served_from_my_reserve(env):
    env.user(51, segment="high_value", region="my", multi=True)
    drop = env.drop(public=1, my=1)
    assert env.claim(drop, 51)["code"] == "MY0"


# --- 8. MY reserve empty: public fallback enforces eligibility ---------------

def test_my_reserve_empty_public_fallback_is_gated(env):
    env.user(60, segment="voucher_hunter", region="my")
    drop = env.drop(public=2, my=0)
    env.roll = VH_FAIL_ROLL
    state = env.claimability(drop, 60, region="my")
    assert state["claimable"] is False and state["reason"] == "shaping_denied"
    with pytest.raises(m.NotEligible):
        env.claim(drop, 60)
    assert env.free(drop, "public") == 2


def test_my_reserve_empty_public_fallback_allows_passing_roll(env):
    env.user(61, segment="voucher_hunter", region="my")
    drop = env.drop(public=2, my=0)
    env.roll = VH_PASS_ROLL
    assert env.claim(drop, 61)["code"].startswith("PUB")


# --- 9. Concurrent claims during reserve depletion ---------------------------

def _drain_my_pool_before_first_my_attempt(env, monkeypatch, thief_uid):
    """Simulate a concurrent claimant taking the last MY voucher between this
    user's pre-check and this user's allocation attempt."""
    real = m._atomic_claim_pooled_voucher
    state = {"done": False}

    def racing(**kwargs):
        if kwargs["pool"] == "my" and not state["done"]:
            state["done"] = True
            env.db.vouchers.update_one(
                {"dropId": kwargs["drop_id"], "pool": "my", "status": "free"},
                {"$set": {"status": "claimed", "claimedByKey": f"uid:{thief_uid}"}},
            )
        return real(**kwargs)

    monkeypatch.setattr(m, "_atomic_claim_pooled_voucher", racing)


def test_reserve_drained_between_precheck_and_allocation_still_gates_public(env, monkeypatch):
    env.user(70, segment="voucher_hunter", region="my")
    drop = env.drop(public=2, my=1)
    env.roll = VH_FAIL_ROLL
    assert env.claimability(drop, 70, region="my")["claimable"] is True   # MY stock visible
    _drain_my_pool_before_first_my_attempt(env, monkeypatch, thief_uid=999)
    with pytest.raises(m.NotEligible):
        env.claim(drop, 70)
    assert env.free(drop, "public") == 2   # public inventory not leaked


def test_reserve_drained_concurrently_blocks_multi_account_user(env, monkeypatch):
    env.user(71, segment="high_value", region="my", multi=True)
    drop = env.drop(public=2, my=1)
    env.roll = 0.0
    assert env.claimability(drop, 71, region="my")["claimable"] is True
    _drain_my_pool_before_first_my_attempt(env, monkeypatch, thief_uid=999)
    with pytest.raises(m.NotEligible):
        env.claim(drop, 71)
    assert env.free(drop, "public") == 2


def test_reserve_drained_concurrently_eligible_user_falls_through_to_public(env, monkeypatch):
    env.user(72, segment="voucher_hunter", region="my")
    drop = env.drop(public=2, my=1)
    env.roll = VH_PASS_ROLL
    assert env.claimability(drop, 72, region="my")["claimable"] is True
    _drain_my_pool_before_first_my_attempt(env, monkeypatch, thief_uid=999)
    assert env.claim(drop, 72)["code"].startswith("PUB")


def test_precheck_then_allocation_never_rerolls(env):
    env.user(73, segment="voucher_hunter", region=None)
    drop = env.drop(public=1, my=0)
    env.roll = VH_PASS_ROLL
    env.claimability(drop, 73)
    rolls_after_precheck = env.rolls
    env.claim(drop, 73)
    assert env.rolls == rolls_after_precheck == 1


def test_claim_pooled_without_gate_keeps_legacy_signature(env):
    drop = env.drop(public=1, my=0)
    res = m.claim_pooled(drop_id=str(drop["_id"]), claim_key="uid:5", ref=NOW, pools=["public"])
    assert res["ok"] is True


# --- 10. Other voucher categories are unaffected -----------------------------

def test_welcome_vip_and_personalised_drops_are_never_probability_shaped(env):
    welcome = env.drop(public=1, audience="new_joiner")
    vip = env.drop(public=1, audience="vip1")
    personalised = env.drop(public=0, dtype="personalised")
    public = env.drop(public=1)
    assert m._is_probability_shaped_pooled_drop(public) is True
    for d in (welcome, vip, personalised):
        assert m._is_probability_shaped_pooled_drop(d) is False


@pytest.mark.parametrize("audience", ["new_joiner", "vip1"])
def test_welcome_and_vip_claims_ignore_multi_account_and_rolls(env, audience):
    env.user(80, segment="voucher_hunter", region="my", multi=True)
    drop = env.drop(public=1, audience=audience)
    env.roll = 0.99
    assert env.claimability(drop, 80, region="my")["claimable"] is True
    assert env.claim(drop, 80)["code"].startswith("PUB")
    assert env.rolls == 0 and env.assignment(drop, 80) is None


def test_personalised_claim_unaffected_by_multi_account_flag(env):
    env.user(81, segment="voucher_hunter", region="my", multi=True)
    drop_id = ObjectId()
    env.db.drops.insert_one({"_id": drop_id, "type": "personalised", "status": "active",
                             "startsAt": NOW - timedelta(hours=1), "endsAt": NOW + timedelta(hours=1)})
    env.db.vouchers.insert_one({"type": "personalised", "dropId": str(drop_id), "status": "unclaimed",
                                "code": "PERS1", "usernameLower": "u81", "createdAt": NOW})
    env.roll = 0.99
    res = m.claim_voucher_for_user(user_id="81", drop_id=str(drop_id), username="u81")
    assert res["code"] == "PERS1"
    assert env.rolls == 0


def test_affiliate_and_admin_flows_do_not_use_the_public_gate():
    import pathlib

    root = pathlib.Path(m.__file__).parent
    for name in ("affiliate_rewards.py", "affiliate_voucher_batches.py", "voucher_pool_service.py"):
        text = (root / name).read_text()
        assert "evaluate_public_pool_gate" not in text
        assert "multi_account_risk_hard_block_applies" not in text


# --- Review follow-up: the New Player definition is exactly what it was before this PR -----------------

@pytest.mark.parametrize("label", ["new_user", "new_joiner"])
@pytest.mark.parametrize("region", ["my", None])
def test_both_existing_new_player_labels_keep_the_boost_and_the_exception_with_multi_account(env, label, region):
    env.user(90, segment=label, region=region, multi=True)
    drop = env.drop(public=1, my=0)
    env.roll = 0.99
    assert env.claimability(drop, 90, region=region)["claimable"] is True
    a = env.assignment(drop, 90)
    assert a["probability"] == 1.0 and a["probability_source"] == "for_bot_segment_new_user_svd_boost"
    assert a["multi_account_hard_block_applied"] is False and a["new_user_svd_boost_applied"] is True
    assert env.claim(drop, 90)["code"].startswith("PUB")


def test_player_age_type_new_player_with_high_value_and_multi_account_is_restricted_as_before_the_boost_rule(env):
    env.user(91, segment="high_value", region="my", age="new_player", multi=True)
    drop = env.drop(public=1, my=0)
    env.roll = 0.0
    state = env.claimability(drop, 91, region="my")
    assert state["claimable"] is False and state["reason"] == "multi_account_blocked"
    a = env.assignment(drop, 91)
    assert a["new_player_override_applied"] is False and a["new_user_svd_boost_applied"] is False
    assert a["player_age_type_at_assignment"] is None            # not read on the live path, exactly as before the PR
    # and without the multi-account flag the same user gets exactly the unboosted high_value probability
    env.user(92, segment="high_value", region="my", age="new_player")
    drop2 = env.drop(public=1, my=0)
    env.claimability(drop2, 92, region="my")
    assert env.assignment(drop2, 92)["probability"] == 0.5


def test_new_player_boost_and_vh_probabilities_are_numerically_unchanged(env):
    expected = {"new_user": 1.0, "new_joiner": 1.0, "voucher_hunter": 0.10, "high_value": 0.5, "ghost": 0.05,
                "normal_actual": 0.7, "low_value": 0.1, "unclassified": 0.1}
    for i, (seg, prob) in enumerate(expected.items()):
        env.user(100 + i, segment=seg, region="my")
        drop = env.drop(public=1, my=0)
        env.claimability(drop, 100 + i, region="my")
        assert env.assignment(drop, 100 + i)["probability"] == prob, seg


# --- Review follow-up: true multi-claimant race on one MY voucher -------------------------------------

def test_concurrent_claimants_on_the_last_my_voucher_each_get_the_right_pool_or_denial(env, monkeypatch):
    """Three users pass the pre-check while one MY voucher is free. Whoever reaches allocation first takes it (the MY pool
    is not restricted); the others fall through to public, where each is judged by the same gate."""
    env.user(110, segment="high_value", region="my", multi=True)       # would be blocked on public
    env.user(111, segment="new_user", region="my")                      # New Player exception
    env.user(112, segment="voucher_hunter", region="my")               # VH, failing roll
    drop = env.drop(public=3, my=1)
    env.roll = VH_FAIL_ROLL
    assert all(env.claimability(drop, u, region="my")["claimable"] for u in (110, 111, 112))
    assert env.rolls == 0                                               # nobody was rolled: all were served-from-reserve candidates
    assert env.claim(drop, 110)["code"] == "MY0"                        # first claimant takes the reserve voucher
    assert env.claim(drop, 111)["code"].startswith("PUB")               # New Player: boost, passes the public gate
    with pytest.raises(m.NotEligible):
        env.claim(drop, 112)                                            # VH, failing roll
    assert env.free(drop, "my") == 0 and env.free(drop, "public") == 2
    assert env.assignment(drop, 110) is None                            # the reserve claimant was never gated or rolled
    assert env.rolls == 1                                               # exactly one roll in total (VH), nothing re-rolled


def test_fall_through_rolls_at_most_once_per_user_and_drop(env, monkeypatch):
    env.user(113, segment="voucher_hunter", region="my")
    drop = env.drop(public=2, my=1)
    env.roll = VH_PASS_ROLL
    assert env.claimability(drop, 113, region="my")["claimable"] is True      # reserve visible -> no roll yet
    _drain_my_pool_before_first_my_attempt(env, monkeypatch, thief_uid=999)
    assert env.claim(drop, 113)["code"].startswith("PUB")                      # gate evaluated once at allocation
    assert env.rolls == 1
    env.claimability(drop, 113, region="my")                                   # later look-ups reuse the stored decision
    assert env.rolls == 1
