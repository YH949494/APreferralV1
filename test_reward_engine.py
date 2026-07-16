"""Tests for reward_engine.py: the generic rule-based reward engine that
lets Campaign Centre support new campaign types without redesign."""

import reward_engine as re_


def test_rank_condition_matches_within_range():
    rule = {"rule_id": "r1", "condition_type": "rank", "params": {"min_rank": 2, "max_rank": 3}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {"rank": 2}) is True
    assert re_.evaluate_rule(rule, {"rank": 3}) is True
    assert re_.evaluate_rule(rule, {"rank": 1}) is False
    assert re_.evaluate_rule(rule, {"rank": 4}) is False


def test_participation_condition_always_matches():
    rule = {"rule_id": "r1", "condition_type": "participation", "params": {}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {}) is True


def test_score_threshold_condition():
    rule = {"rule_id": "r1", "condition_type": "score_threshold", "params": {"min_score": 100}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {"score": 150}) is True
    assert re_.evaluate_rule(rule, {"score": 50}) is False
    assert re_.evaluate_rule(rule, {}) is False


def test_referral_count_condition():
    rule = {"rule_id": "r1", "condition_type": "referral_count", "params": {"min_referrals": 5}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {"referral_count": 5}) is True
    assert re_.evaluate_rule(rule, {"referral_count": 4}) is False


def test_first_play_condition():
    rule = {"rule_id": "r1", "condition_type": "first_play", "params": {}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {"first_play": True}) is True
    assert re_.evaluate_rule(rule, {"first_play": False}) is False


def test_vip_condition():
    rule = {"rule_id": "r1", "condition_type": "vip", "params": {}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {"is_vip": True}) is True
    assert re_.evaluate_rule(rule, {}) is False


def test_campaign_tag_condition():
    rule = {"rule_id": "r1", "condition_type": "campaign_tag", "params": {"tag": "vip_event"}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {"tags": ["vip_event", "other"]}) is True
    assert re_.evaluate_rule(rule, {"tags": ["other"]}) is False


def test_unknown_condition_type_never_matches():
    rule = {"rule_id": "r1", "condition_type": "not_real", "params": {}, "pool_id": "p"}
    assert re_.evaluate_rule(rule, {"rank": 1}) is False


def test_match_rule_returns_first_matching_rule_in_order():
    rules = [
        {"rule_id": "specific", "condition_type": "rank", "params": {"min_rank": 1, "max_rank": 1}, "pool_id": "gold"},
        {"rule_id": "fallback", "condition_type": "participation", "params": {}, "pool_id": "consolation"},
    ]
    assert re_.match_rule(rules, {"rank": 1})["rule_id"] == "specific"
    assert re_.match_rule(rules, {"rank": 99})["rule_id"] == "fallback"


def test_match_rule_no_match_returns_none():
    rules = [{"rule_id": "r1", "condition_type": "rank", "params": {"min_rank": 1, "max_rank": 1}, "pool_id": "p"}]
    assert re_.match_rule(rules, {"rank": 5}) is None


def test_validate_reward_rules_missing_min_score():
    rules = [{"rule_id": "r1", "condition_type": "score_threshold", "params": {}, "pool_id": "p"}]
    assert re_.validate_reward_rules(rules) == "missing_min_score"


def test_validate_reward_rules_missing_min_referrals():
    rules = [{"rule_id": "r1", "condition_type": "referral_count", "params": {}, "pool_id": "p"}]
    assert re_.validate_reward_rules(rules) == "missing_min_referrals"


def test_validate_reward_rules_missing_tag():
    rules = [{"rule_id": "r1", "condition_type": "campaign_tag", "params": {}, "pool_id": "p"}]
    assert re_.validate_reward_rules(rules) == "missing_tag"


def test_validate_reward_rules_not_a_list():
    assert re_.validate_reward_rules("not-a-list") == "invalid_rules"


def test_rank_ranges_collects_only_rank_rules():
    rules = [
        {"rule_id": "r1", "condition_type": "rank", "params": {"min_rank": 1, "max_rank": 2}, "pool_id": "p"},
        {"rule_id": "r2", "condition_type": "participation", "params": {}, "pool_id": "p2"},
    ]
    assert re_.rank_ranges(rules) == {1, 2}
