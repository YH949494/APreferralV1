"""Unit tests for the read-only Phase 5B UIM KPI mapping/gap report."""

from __future__ import annotations

import uim_kpi_mapping as km

_REQUIRED_FIELDS = {
    "uim_metric_key",
    "uim_display_label",
    "source_tab",
    "source_columns",
    "confirmed",
    "uim_rule_plain_english",
    "backend_current_metric_used",
    "backend_gap",
    "implementation_status",
    "recommended_backend_implementation",
}

_VALID_STATUSES = {"exact_available", "backend_missing", "definition_mismatch", "source_missing"}


def test_get_kpi_mapping_covers_all_seven_focus_kpis():
    mapping = km.get_kpi_mapping()
    keys = {entry["uim_metric_key"] for entry in mapping}
    assert keys == {
        "total_campaign_players",
        "voucher_claimer_accounts",
        "actual_players",
        "high_value_players",
        "new_player_total",
        "old_player_total",
        "claim_risk",
    }


def test_every_entry_has_required_fields_and_valid_status():
    for entry in km.get_kpi_mapping():
        missing = _REQUIRED_FIELDS - set(entry.keys())
        assert not missing, f"{entry.get('uim_metric_key')} missing fields: {missing}"
        assert entry["implementation_status"] in _VALID_STATUSES


def test_get_kpi_mapping_returns_copies_not_live_references():
    mapping = km.get_kpi_mapping()
    mapping[0]["uim_metric_key"] = "mutated"
    assert km.get_kpi_mapping()[0]["uim_metric_key"] != "mutated"


def test_total_campaign_players_flags_wrong_universe():
    entry = km.get_kpi_mapping_by_key("total_campaign_players")
    assert entry["implementation_status"] == "definition_mismatch"
    assert "count_documents({})" in entry["backend_current_metric_used"]
    assert "all registered users" in entry["backend_current_metric_used"].lower() or "ALL registered users" in entry["backend_current_metric_used"]


def test_new_player_total_documents_is_new_player_flag():
    entry = km.get_kpi_mapping_by_key("new_player_total")
    assert "is_new_player" in entry["uim_rule_plain_english"]
    assert "Marketing" in entry["uim_rule_plain_english"]


def test_high_value_players_documents_after_bet_withdrawal_rule():
    entry = km.get_kpi_mapping_by_key("high_value_players")
    assert "after-bet" in entry["uim_rule_plain_english"]
    assert "withdrawal" in entry["uim_rule_plain_english"]


def test_claim_risk_documents_claim_count_thresholds():
    entry = km.get_kpi_mapping_by_key("claim_risk")
    assert "claim" in entry["uim_rule_plain_english"].lower()
    assert "threshold" in entry["uim_rule_plain_english"].lower() or "cutoff" in entry["backend_gap"].lower()


def test_claim_risk_tier_aliases_resolve_to_shared_entry():
    base = km.get_kpi_mapping_by_key("claim_risk")
    for alias in ("medium_risk_claim_accounts", "high_risk_claim_accounts", "abuse_freeze_claim_accounts"):
        aliased = km.get_kpi_mapping_by_key(alias)
        assert aliased is not None
        assert aliased["uim_metric_key"] == base["uim_metric_key"]


def test_unmapped_metric_key_returns_none_not_invented_data():
    assert km.get_kpi_mapping_by_key("welcome_abuse_invitees") is None
    assert km.get_kpi_mapping_by_key("totally_unknown_key") is None
