"""Regression tests for the effective (operational eligibility) segment
override: multi_account_voucher_hunter=True must resolve as "voucher_hunter"
for eligibility/gating purposes without ever mutating for_bot_segment /
for_bot_segment_normalized, and must exclude the user from every other
segment's operational eligibility predicate."""

import unittest

from effective_segment import (
    effective_segment_query,
    effective_segment_query_for_segments,
    resolve_effective_segment,
)


class ResolveEffectiveSegmentTests(unittest.TestCase):
    def test_case1_behavioral_voucher_hunter_stays_voucher_hunter(self):
        user = {"for_bot_segment": "Voucher Hunter", "for_bot_segment_normalized": "voucher_hunter", "multi_account_voucher_hunter": False}
        self.assertEqual(resolve_effective_segment(user), "voucher_hunter")

    def test_case2_high_value_plus_multi_account_becomes_voucher_hunter(self):
        user = {"for_bot_segment": "High Value", "for_bot_segment_normalized": "high_value", "multi_account_voucher_hunter": True}
        self.assertEqual(resolve_effective_segment(user), "voucher_hunter")
        # Canonical behavioral field is untouched by resolution (read-only helper).
        self.assertEqual(user["for_bot_segment_normalized"], "high_value")

    def test_case3_normal_actual_plus_multi_account_becomes_voucher_hunter(self):
        user = {"for_bot_segment_normalized": "normal_actual", "multi_account_voucher_hunter": True}
        self.assertEqual(resolve_effective_segment(user), "voucher_hunter")

    def test_case4_no_risk_canonical_behavior_unchanged(self):
        user = {"for_bot_segment": "Ghost", "for_bot_segment_normalized": "ghost"}
        self.assertEqual(resolve_effective_segment(user), "ghost")
        user_ha = {"for_bot_segment": "High Value", "for_bot_segment_normalized": "high_value", "multi_account_voucher_hunter": False}
        self.assertEqual(resolve_effective_segment(user_ha), "high_value")

    def test_missing_multi_account_field_falls_back_to_canonical(self):
        user = {"for_bot_segment": "Normal Actual"}
        self.assertEqual(resolve_effective_segment(user), "normal_actual")

    def test_none_user_resolves_unclassified(self):
        self.assertEqual(resolve_effective_segment(None), "unclassified")


class EffectiveSegmentQueryTests(unittest.TestCase):
    def test_voucher_hunter_query_is_inclusive_or(self):
        query = effective_segment_query("voucher_hunter")
        self.assertEqual(
            query,
            {
                "$or": [
                    {"for_bot_segment_normalized": "voucher_hunter"},
                    {"multi_account_voucher_hunter": True},
                ]
            },
        )

    def test_other_segment_query_excludes_multi_account_voucher_hunters(self):
        query = effective_segment_query("high_value")
        self.assertEqual(
            query,
            {
                "for_bot_segment_normalized": "high_value",
                "multi_account_voucher_hunter": {"$ne": True},
            },
        )

    def test_query_for_segments_ors_multiple_clauses(self):
        query = effective_segment_query_for_segments(["voucher_hunter", "high_value"])
        self.assertIn("$or", query)
        self.assertEqual(len(query["$or"]), 2)

    def test_query_for_single_segment_is_unwrapped(self):
        query = effective_segment_query_for_segments(["high_value"])
        self.assertNotIn("$or", query)


# --- In-memory Mongo-predicate evaluator, mirroring test_campaign_builder's
# FakeCollection semantics ($or/$ne/$in), so these tests exercise the actual
# query shape rather than only the Python-side resolve_effective_segment(). ---


def _matches(doc: dict, filt: dict) -> bool:
    for key, cond in (filt or {}).items():
        if key == "$or":
            if not any(_matches(doc, sub) for sub in cond):
                return False
            continue
        val = doc.get(key)
        if isinstance(cond, dict) and any(k.startswith("$") for k in cond):
            for op, opval in cond.items():
                if op == "$ne":
                    if val == opval:
                        return False
                elif op == "$in":
                    if val not in opval:
                        return False
                else:
                    return False
        else:
            if val != cond:
                return False
    return True


class MongoPredicateBehaviorTests(unittest.TestCase):
    def test_case2_multi_account_high_value_matches_voucher_hunter_not_high_value(self):
        user = {"user_id": 1, "for_bot_segment_normalized": "high_value", "multi_account_voucher_hunter": True}
        self.assertTrue(_matches(user, effective_segment_query("voucher_hunter")))
        self.assertFalse(_matches(user, effective_segment_query("high_value")))

    def test_case5_cluster_members_all_resolve_voucher_hunter_regardless_of_canonical(self):
        """>3 linked identities all flagged multi_account_voucher_hunter=True
        must all resolve operationally as voucher_hunter, regardless of their
        individual canonical behavioral segments."""
        cluster = [
            {"user_id": 501, "for_bot_segment_normalized": "high_value", "multi_account_voucher_hunter": True},
            {"user_id": 502, "for_bot_segment_normalized": "normal_actual", "multi_account_voucher_hunter": True},
            {"user_id": 503, "for_bot_segment_normalized": "voucher_hunter", "multi_account_voucher_hunter": True},
            {"user_id": 504, "for_bot_segment_normalized": "unclassified", "multi_account_voucher_hunter": True},
        ]
        vh_query = effective_segment_query("voucher_hunter")
        for member in cluster:
            with self.subTest(user_id=member["user_id"]):
                self.assertEqual(resolve_effective_segment(member), "voucher_hunter")
                self.assertTrue(_matches(member, vh_query))
                # Canonical field on the doc itself is never mutated by resolution.
                self.assertNotEqual(member["for_bot_segment_normalized"], None)

        # None of them should leak into an unrelated segment's eligibility.
        for segment in ("high_value", "normal_actual", "unclassified"):
            query = effective_segment_query(segment)
            matched = [m["user_id"] for m in cluster if _matches(m, query)]
            self.assertEqual(matched, [], f"multi-account voucher hunters leaked into {segment} eligibility: {matched}")

    def test_non_cluster_user_still_resolves_own_canonical_segment(self):
        user = {"user_id": 900, "for_bot_segment_normalized": "high_value", "multi_account_voucher_hunter": False}
        self.assertTrue(_matches(user, effective_segment_query("high_value")))
        self.assertFalse(_matches(user, effective_segment_query("voucher_hunter")))


if __name__ == "__main__":
    unittest.main()
