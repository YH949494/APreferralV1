"""Generic, rule-based reward-rule evaluation for Campaign Centre.

A campaign's ``reward_config.rules`` is a list of rules, each mapping a
condition against a participation ``context`` to a voucher pool. Adding a
new *condition type* still needs one small evaluator function here, but
adding a new *rule instance* (e.g. a new rank bracket, a new score
threshold, a new pool) never requires a backend code change — it's just
admin-entered data.

Rule shape::

    {
      "rule_id": "rank-1",
      "condition_type": "rank",       # see CONDITION_TYPES
      "params": {"min_rank": 1, "max_rank": 1},
      "pool_id": "july-tournament-gold",
      "reward_label": "Champion Reward",
    }

Context shape depends on the campaign type submitting it, e.g. a tournament
winner row supplies ``{"rank": 1, "score": 18500}``.
"""

from __future__ import annotations

CONDITION_TYPES = [
    "rank",             # params: min_rank, max_rank (inclusive)
    "participation",    # params: {} — matches any participant (e.g. consolation pool)
    "score_threshold",  # params: min_score
    "referral_count",   # params: min_referrals
    "first_play",       # params: {} — matches context["first_play"] truthy
    "vip",              # params: {} — matches context["is_vip"] truthy
    "campaign_tag",     # params: tag — matches tag in context["tags"]
]


def _evaluate_rank(params: dict, context: dict) -> bool:
    rank = context.get("rank")
    if rank is None:
        return False
    try:
        rank = int(rank)
        min_rank = int(params["min_rank"])
        max_rank = int(params["max_rank"])
    except (KeyError, TypeError, ValueError):
        return False
    return min_rank <= rank <= max_rank


def _evaluate_participation(params: dict, context: dict) -> bool:
    return True


def _evaluate_score_threshold(params: dict, context: dict) -> bool:
    score = context.get("score")
    if score is None:
        return False
    try:
        return float(score) >= float(params["min_score"])
    except (KeyError, TypeError, ValueError):
        return False


def _evaluate_referral_count(params: dict, context: dict) -> bool:
    count = context.get("referral_count")
    if count is None:
        return False
    try:
        return int(count) >= int(params["min_referrals"])
    except (KeyError, TypeError, ValueError):
        return False


def _evaluate_first_play(params: dict, context: dict) -> bool:
    return bool(context.get("first_play"))


def _evaluate_vip(params: dict, context: dict) -> bool:
    return bool(context.get("is_vip"))


def _evaluate_campaign_tag(params: dict, context: dict) -> bool:
    tag = params.get("tag")
    tags = context.get("tags") or []
    return bool(tag) and tag in tags


_EVALUATORS = {
    "rank": _evaluate_rank,
    "participation": _evaluate_participation,
    "score_threshold": _evaluate_score_threshold,
    "referral_count": _evaluate_referral_count,
    "first_play": _evaluate_first_play,
    "vip": _evaluate_vip,
    "campaign_tag": _evaluate_campaign_tag,
}


def evaluate_rule(rule: dict, context: dict) -> bool:
    evaluator = _EVALUATORS.get(rule.get("condition_type"))
    if not evaluator:
        return False
    return evaluator(rule.get("params") or {}, context)


def match_rule(rules: list, context: dict) -> dict | None:
    """First rule (in list order) whose condition matches the context."""
    for rule in rules or []:
        if evaluate_rule(rule, context):
            return rule
    return None


def _rank_ranges_overlap(rules: list) -> bool:
    ranges = []
    for rule in rules:
        if rule.get("condition_type") != "rank":
            continue
        params = rule.get("params") or {}
        try:
            ranges.append((int(params["min_rank"]), int(params["max_rank"])))
        except (KeyError, TypeError, ValueError):
            continue
    ranges.sort()
    return any(ranges[i][0] <= ranges[i - 1][1] for i in range(1, len(ranges)))


def validate_reward_rules(rules) -> str | None:
    """Structural validation shared by every campaign type. Returns an error
    code, or None if the rules are well-formed."""
    if not isinstance(rules, list):
        return "invalid_rules"

    seen_ids = set()
    for rule in rules:
        if not isinstance(rule, dict):
            return "invalid_rule"
        rule_id = rule.get("rule_id")
        if not rule_id or rule_id in seen_ids:
            return "duplicate_or_missing_rule_id"
        seen_ids.add(rule_id)

        if rule.get("condition_type") not in CONDITION_TYPES:
            return "invalid_condition_type"
        if not rule.get("pool_id"):
            return "missing_pool_id"

        params = rule.get("params") or {}
        if rule["condition_type"] == "rank":
            try:
                min_rank = int(params["min_rank"])
                max_rank = int(params["max_rank"])
            except (KeyError, TypeError, ValueError):
                return "invalid_rank_range"
            if min_rank < 1 or max_rank < min_rank:
                return "invalid_rank_range"
        elif rule["condition_type"] == "score_threshold" and "min_score" not in params:
            return "missing_min_score"
        elif rule["condition_type"] == "referral_count" and "min_referrals" not in params:
            return "missing_min_referrals"
        elif rule["condition_type"] == "campaign_tag" and not params.get("tag"):
            return "missing_tag"

    if _rank_ranges_overlap(rules):
        return "overlapping_rank_ranges"
    return None


def rank_ranges(rules: list) -> set:
    """Every rank covered by any rank-type rule — used by the tournament
    result validator to reject winners outside configured reward ranks."""
    ranks: set = set()
    for rule in rules or []:
        if rule.get("condition_type") != "rank":
            continue
        params = rule.get("params") or {}
        try:
            ranks.update(range(int(params["min_rank"]), int(params["max_rank"]) + 1))
        except (KeyError, TypeError, ValueError):
            continue
    return ranks
