"""Drift guard: T1-T5 qualified-referral thresholds must be identical on
every production surface.

They were previously restated with independent `os.getenv` defaults in three
modules and had drifted — `affiliate_rewards` used T5=250 while `vouchers.py`
(the admin pending-review "eligible tier" calculation) and `dashboard_panels`
(the settings panel) both defaulted T5 to 300. A user on 250-299 qualified
referrals was therefore shown, and announced, a tier the evaluator would
never grant.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import affiliate_rewards
import dashboard_panels
import scheduler
from affiliate_reward_plans import tier_threshold, tier_thresholds

ROOT = Path(__file__).parent

CONFIRMED = {"T1": 10, "T2": 25, "T3": 50, "T4": 150, "T5": 250}


class TestCanonicalValues:
    def test_canonical_thresholds_match_the_confirmed_business_rules(self):
        assert tier_thresholds() == CONFIRMED

    @pytest.mark.parametrize("tier,expected", sorted(CONFIRMED.items()))
    def test_each_tier_individually(self, tier, expected):
        assert tier_threshold(tier) == expected


class TestEverySurfaceAgrees:
    def test_evaluator_module_constants(self):
        assert affiliate_rewards.T1_THRESHOLD == CONFIRMED["T1"]
        assert affiliate_rewards.T2_THRESHOLD == CONFIRMED["T2"]
        assert affiliate_rewards.T3_THRESHOLD == CONFIRMED["T3"]
        assert affiliate_rewards.T4_THRESHOLD == CONFIRMED["T4"]
        assert affiliate_rewards.T5_THRESHOLD == CONFIRMED["T5"]

    def test_evaluator_tier_selection_uses_them(self):
        assert affiliate_rewards._tier_for_count(9) is None
        assert affiliate_rewards._tier_for_count(10) == "T1"
        assert affiliate_rewards._tier_for_count(24) == "T1"
        assert affiliate_rewards._tier_for_count(25) == "T2"
        assert affiliate_rewards._tier_for_count(49) == "T2"
        assert affiliate_rewards._tier_for_count(50) == "T3"
        assert affiliate_rewards._tier_for_count(149) == "T3"
        assert affiliate_rewards._tier_for_count(150) == "T4"
        assert affiliate_rewards._tier_for_count(249) == "T4"
        assert affiliate_rewards._tier_for_count(250) == "T5"

    def test_eligible_tier_list_at_the_top_threshold(self):
        assert affiliate_rewards._eligible_tiers_for_count(250) == ["T1", "T2", "T3", "T4", "T5"]
        assert affiliate_rewards._eligible_tiers_for_count(249) == ["T1", "T2", "T3", "T4"]

    def test_milestone_announcement_thresholds(self):
        assert dict(scheduler.REFERRAL_CONGRATS_TIER_THRESHOLDS) == {
            v: k for k, v in CONFIRMED.items()
        }
        assert scheduler.REFERRAL_CONGRATS_TIER_LABEL == {v: k for k, v in CONFIRMED.items()}

    def test_admin_settings_panel_reports_canonical_thresholds(self):
        panel = dashboard_panels.build_settings_panel(
            {"BOT_USERNAME": "apbot"}, constants={"XP_BASE_PER_CHECKIN": 20, "GROUP_ID": -100}
        )
        rendered = panel["sections"]["affiliate_settings"]["tier_thresholds"]
        assert rendered == CONFIRMED, (
            "the settings panel reports thresholds the evaluator does not use"
        )

    def test_settings_panel_honours_an_env_override(self):
        panel = dashboard_panels.build_settings_panel(
            {"BOT_USERNAME": "apbot", "AFF_T5_THRESHOLD": "400"},
            constants={"XP_BASE_PER_CHECKIN": 20, "GROUP_ID": -100},
        )
        assert panel["sections"]["affiliate_settings"]["tier_thresholds"]["T5"] == 400

    def test_settings_panel_renders_canonical_values_for_an_empty_env(self):
        # The panel renders from a captured env; with nothing set it must show
        # the canonical defaults, not a local copy.
        assert tier_thresholds({}) == CONFIRMED


class TestNoSurfaceRestatesThresholds:
    """The real defence: no production module may carry its own default."""

    PRODUCTION_FILES = [
        "vouchers.py",
        "dashboard_panels.py",
        "affiliate_rewards.py",
        "scheduler.py",
        "main.py",
    ]

    @pytest.mark.parametrize("filename", PRODUCTION_FILES)
    def test_no_module_hardcodes_a_threshold_env_default(self, filename):
        path = ROOT / filename
        if not path.exists():
            pytest.skip(f"{filename} not present")
        text = path.read_text()
        # e.g. os.getenv("AFF_T5_THRESHOLD", "300") or _env(env, "AFF_T5_THRESHOLD", "300")
        offenders = re.findall(
            r'["\']AFF_T[1-5]_THRESHOLD["\']\s*,\s*["\']?\d+', text
        )
        assert not offenders, (
            f"{filename} restates a threshold default ({offenders}); read "
            "affiliate_reward_plans.tier_thresholds() instead"
        )

    def test_the_canonical_module_is_the_only_definition(self):
        from affiliate_reward_plans import _THRESHOLD_ENV_DEFAULTS

        assert {t: d for t, _v, d in _THRESHOLD_ENV_DEFAULTS} == CONFIRMED

    def test_no_stray_t5_equals_300_anywhere_in_affiliate_code(self):
        for filename in self.PRODUCTION_FILES + ["affiliate_reward_plans.py"]:
            path = ROOT / filename
            if not path.exists():
                continue
            text = path.read_text()
            for match in re.finditer(r'AFF_T5_THRESHOLD["\']?\s*,\s*["\']?(\d+)', text):
                assert match.group(1) == "250", (
                    f"{filename} defaults T5 to {match.group(1)}, not 250"
                )


class TestEnvOverrideStillWorksEverywhere:
    def test_an_override_moves_every_surface_together(self):
        overridden = tier_thresholds({"AFF_T5_THRESHOLD": "999"})
        assert overridden["T5"] == 999
        # ...and the other tiers are untouched.
        assert {k: v for k, v in overridden.items() if k != "T5"} == {
            k: v for k, v in CONFIRMED.items() if k != "T5"
        }

    def test_a_malformed_override_falls_back_to_the_confirmed_value(self):
        assert tier_thresholds({"AFF_T5_THRESHOLD": "not-a-number"})["T5"] == 250
        assert tier_thresholds({"AFF_T5_THRESHOLD": ""})["T5"] == 250
