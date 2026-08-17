"""Regression coverage for voucher_risk_eligibility.apply_risk_modifier and
its wiring into vouchers.assign_public_pool_access_once -- the single,
real production claim-time probability gate (task:
wire-risk-eligibility-into-production-gating).

Proves:
  1. high_value, no risk -> normal HV probability (unchanged).
  2. high_value + multi_account_risk -> HV segment preserved
     (for_bot_segment_normalized stays "high_value"), risk-adjusted
     probability actually used by the real claim gate.
  3. behavioral Voucher Hunter only -> existing VH probability, unchanged.
  4. behavioral Voucher Hunter + multi-account risk -> strongest
     configured restriction (clamped 5-10% band).
  5. clearing multi_account_risk restores the canonical segment base
     probability on the next evaluation.
  6. invalid/missing risk configuration fails safely to the documented
     default -- a malformed value (250, -1, "banana") never reaches the
     random gate.
  7. dashboard parity: apply_risk_modifier is a pure function of
     (base_probability, behavioral_voucher_hunter, multi_account_risk) --
     the exact same inputs always produce the exact same
     final_probability regardless of caller, which is what lets
     Databot's dashboard and this live gate agree for the same user.
  8. unrelated claim eligibility (new-player override, idempotent
     assignment reuse) is unchanged by the risk modifier.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from pymongo.errors import DuplicateKeyError

from vouchers import assign_public_pool_access_once
from voucher_risk_eligibility import (
    RISK_CATEGORY_BEHAVIORAL_AND_MULTI_ACCOUNT,
    RISK_CATEGORY_MULTI_ACCOUNT_ONLY,
    RISK_CATEGORY_NONE,
    apply_risk_modifier,
)


class _SimpleCollection:
    """Mirrors test_vouchers.py's own fake assignment collection so this
    file can drive assign_public_pool_access_once identically."""

    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find_one(self, filt, projection=None):  # noqa: ARG002
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in filt.items()):
                return dict(doc)
        return None

    def insert_one(self, doc):
        for existing in self.docs:
            if (
                existing.get("user_id") == doc.get("user_id")
                and existing.get("public_pool_id") == doc.get("public_pool_id")
            ):
                raise DuplicateKeyError("duplicate key")
        self.docs.append(dict(doc))

        class R:
            inserted_id = 1
        return R()


class ApplyRiskModifierTests(unittest.TestCase):
    """Pure-function coverage -- no Mongo/Flask context needed."""

    def test_high_value_no_risk_unchanged(self):
        result = apply_risk_modifier(0.5, behavioral_voucher_hunter=False, multi_account_risk=False)
        self.assertEqual(result["risk_category"], RISK_CATEGORY_NONE)
        self.assertEqual(result["final_probability"], 0.5)
        self.assertEqual(result["base_probability"], 0.5)

    def test_high_value_with_multi_account_risk_reduces_probability(self):
        result = apply_risk_modifier(0.5, behavioral_voucher_hunter=False, multi_account_risk=True)
        self.assertEqual(result["risk_category"], RISK_CATEGORY_MULTI_ACCOUNT_ONLY)
        self.assertLess(result["final_probability"], 0.5)
        self.assertEqual(result["final_probability"], round(0.5 * result["risk_modifier"], 6))

    def test_behavioral_voucher_hunter_only_unchanged(self):
        result = apply_risk_modifier(0.10, behavioral_voucher_hunter=True, multi_account_risk=False)
        self.assertEqual(result["risk_category"], RISK_CATEGORY_NONE)
        self.assertEqual(result["final_probability"], 0.10)

    def test_behavioral_and_multi_account_gets_strongest_restriction(self):
        result = apply_risk_modifier(0.10, behavioral_voucher_hunter=True, multi_account_risk=True)
        self.assertEqual(result["risk_category"], RISK_CATEGORY_BEHAVIORAL_AND_MULTI_ACCOUNT)
        self.assertGreaterEqual(result["final_probability"], 0.05)
        self.assertLessEqual(result["final_probability"], 0.10)

    def test_clearing_multi_account_risk_restores_base(self):
        with_risk = apply_risk_modifier(0.5, behavioral_voucher_hunter=False, multi_account_risk=True)
        without_risk = apply_risk_modifier(0.5, behavioral_voucher_hunter=False, multi_account_risk=False)
        self.assertLess(with_risk["final_probability"], without_risk["final_probability"])
        self.assertEqual(without_risk["final_probability"], 0.5)

    def test_pure_function_gives_identical_result_for_identical_inputs(self):
        """Dashboard parity: no hidden state -- same call, same answer."""
        a = apply_risk_modifier(0.3, behavioral_voucher_hunter=False, multi_account_risk=True)
        b = apply_risk_modifier(0.3, behavioral_voucher_hunter=False, multi_account_risk=True)
        self.assertEqual(a, b)

    def test_invalid_configured_modifier_falls_back_safely(self):
        # An out-of-range (>100) or negative or non-numeric stored setting
        # must never leak through and change claim behaviour.
        for bad_value in (250, -1, "banana", float("nan")):
            with patch(
                "voucher_risk_eligibility.get_setting",
                return_value=bad_value,
            ):
                result = apply_risk_modifier(0.5, behavioral_voucher_hunter=False, multi_account_risk=True)
                # Falls back to the documented default (25%) instead of
                # producing an out-of-bounds or crashing probability.
                self.assertAlmostEqual(result["risk_modifier"], 0.25)
                self.assertGreaterEqual(result["final_probability"], 0.0)
                self.assertLessEqual(result["final_probability"], 1.0)

    def test_settings_lookup_exception_falls_back_safely(self):
        with patch("voucher_risk_eligibility.get_setting", side_effect=RuntimeError("mongo down")):
            result = apply_risk_modifier(0.5, behavioral_voucher_hunter=False, multi_account_risk=True)
            self.assertAlmostEqual(result["risk_modifier"], 0.25)

    def test_dashboard_parity_reference_values(self):
        """Hand-computed reference values for a known pool_probabilities /
        voucher_risk_modifiers config -- mirrored by Databot's
        tests/test_voucher_eligibility.py::
        test_dashboard_parity_reference_values so both repos independently
        prove they implement the identical base -> modifier -> final
        formula for the same configuration and inputs. Base probabilities
        (0.50 high_value, 0.10 voucher_hunter) and risk-modifier defaults
        (25% multi-account-only, 5-10% band for both) here are this
        module's own schema defaults -- see settings_service.SETTINGS_SCHEMA.
        """
        hv_multi_account_only = apply_risk_modifier(
            0.50, behavioral_voucher_hunter=False, multi_account_risk=True,
        )
        self.assertEqual(hv_multi_account_only["final_probability"], 0.125)  # 0.50 * 0.25

        vh_both = apply_risk_modifier(
            0.10, behavioral_voucher_hunter=True, multi_account_risk=True,
        )
        self.assertEqual(vh_both["final_probability"], 0.10)  # 0.10 * 1.00, clamped into [0.05, 0.10]


class AssignPublicPoolAccessRiskWiringTests(unittest.TestCase):
    """Integration coverage against the real claim-time gate."""

    def _run(self, user_id, drop_id, user_doc, *, roll=0.01):
        import vouchers as m

        orig_assignments = m.public_pool_access_assignments_col
        orig_random = m.random.random
        orig_randint = m.random.randint
        m.public_pool_access_assignments_col = _SimpleCollection()
        m.random.random = lambda: roll
        m.random.randint = lambda a, b: 20  # noqa: ARG005
        drop_open = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
        try:
            return assign_public_pool_access_once(user_id, drop_id, drop_open, user_doc=user_doc)
        finally:
            m.public_pool_access_assignments_col = orig_assignments
            m.random.random = orig_random
            m.random.randint = orig_randint

    def test_high_value_no_risk_uses_normal_hv_probability(self):
        assignment = self._run(201, "drop-1", {"for_bot_segment": "high_value"})
        self.assertEqual(assignment["for_bot_segment_normalized"], "high_value")
        self.assertEqual(assignment["probability"], 0.5)
        self.assertEqual(assignment["risk_category"], RISK_CATEGORY_NONE)

    def test_high_value_with_multi_account_risk_preserves_segment_reduces_probability(self):
        assignment = self._run(
            202, "drop-1", {"for_bot_segment": "high_value", "multi_account_risk": True},
        )
        # Canonical segment must remain high_value -- multi_account_risk
        # never mutates for_bot_segment / for_bot_segment_normalized.
        self.assertEqual(assignment["for_bot_segment_normalized"], "high_value")
        self.assertEqual(assignment["risk_category"], RISK_CATEGORY_MULTI_ACCOUNT_ONLY)
        self.assertLess(assignment["probability"], 0.5)
        self.assertEqual(assignment["base_probability_pre_risk"], 0.5)
        self.assertTrue(assignment["multi_account_risk_at_assignment"])

    def test_behavioral_voucher_hunter_only_keeps_existing_gating(self):
        assignment = self._run(203, "drop-1", {"for_bot_segment": "voucher_hunter"})
        self.assertEqual(assignment["probability"], 0.10)
        self.assertEqual(assignment["risk_category"], RISK_CATEGORY_NONE)

    def test_behavioral_and_multi_account_gets_strongest_restriction(self):
        assignment = self._run(
            204, "drop-1", {"for_bot_segment": "voucher_hunter", "multi_account_risk": True},
        )
        self.assertEqual(assignment["for_bot_segment_normalized"], "voucher_hunter")
        self.assertEqual(assignment["risk_category"], RISK_CATEGORY_BEHAVIORAL_AND_MULTI_ACCOUNT)
        self.assertGreaterEqual(assignment["probability"], 0.05)
        self.assertLessEqual(assignment["probability"], 0.10)

    def test_clearing_multi_account_risk_restores_segment_probability(self):
        risky = self._run(205, "drop-1", {"for_bot_segment": "high_value", "multi_account_risk": True})
        clean = self._run(206, "drop-1", {"for_bot_segment": "high_value", "multi_account_risk": False})
        self.assertLess(risky["probability"], clean["probability"])
        self.assertEqual(clean["probability"], 0.5)

    def test_behavioral_flag_uses_the_same_segment_source_as_the_base_probability(self):
        """backend_segment and for_bot_segment are independent taxonomies
        and can disagree. When backend_segment drives the base probability
        (backend_seg in SEGMENT_PROBABILITY_CONFIG), the behavioral-VH flag
        must come from backend_seg too -- not from for_bot_segment, which
        would apply the wrong risk band."""
        assignment = self._run(
            209, "drop-1",
            {
                "for_bot_segment": "high_value",
                "backend_segment": "voucher_hunter",
                "multi_account_risk": True,
            },
        )
        # base_probability_pre_risk came from backend_segment_probability("voucher_hunter") = 0.10.
        self.assertEqual(assignment["base_probability_pre_risk"], 0.10)
        # backend_seg == "voucher_hunter" -> behavioral+multi-account band (5-10%),
        # NOT the multi-account-only 25% modifier (which would give 2.5%).
        self.assertEqual(assignment["risk_category"], RISK_CATEGORY_BEHAVIORAL_AND_MULTI_ACCOUNT)
        self.assertGreaterEqual(assignment["probability"], 0.05)
        self.assertLessEqual(assignment["probability"], 0.10)

    def test_new_player_override_bypasses_risk_modifier_entirely(self):
        """Unrelated eligibility rule (new-player onboarding boost) must be
        untouched by this feature -- still an absolute 100%, even with
        multi_account_risk set."""
        assignment = self._run(
            207, "drop-1",
            {"for_bot_segment": "high_value", "multi_account_risk": True, "player_age_type": "new_player"},
        )
        self.assertEqual(assignment["probability"], 1.0)
        self.assertTrue(assignment["new_player_override_applied"])

    def test_idempotent_reuse_unaffected_by_risk_fields(self):
        import vouchers as m

        orig_assignments = m.public_pool_access_assignments_col
        m.public_pool_access_assignments_col = _SimpleCollection()
        drop_open = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
        try:
            first = assign_public_pool_access_once(
                208, "drop-1", drop_open, user_doc={"for_bot_segment": "high_value", "multi_account_risk": True},
            )
            second = assign_public_pool_access_once(
                208, "drop-1", drop_open, user_doc={"for_bot_segment": "high_value", "multi_account_risk": False},
            )
        finally:
            m.public_pool_access_assignments_col = orig_assignments
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
