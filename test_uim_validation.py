"""Unit tests for the read-only UIM "dashboard" KPI tab parsing helpers."""

from __future__ import annotations

import uim_validation as uv


def _kpi_row(label, value, note=""):
    return ["KPI", label, value, note]


def test_parse_uim_kpi_rows_only_kpi_rows_with_four_columns():
    rows = [
        ["TYPE", "LABEL", "VALUE", "NOTES"],
        _kpi_row("Total Campaign Players", "1,300", "weekly export"),
        _kpi_row("New Users", "150"),
        _kpi_row("Voucher Claimer Accounts", "410"),
        _kpi_row("Total Claims", "500"),
        _kpi_row("Medium Risk Claim Accounts", "30"),
        _kpi_row("High Risk Claim Accounts", "10"),
        _kpi_row("Abuse / Freeze Claim Accounts", "5"),
        _kpi_row("Actual Players", "950"),
        _kpi_row("High Value Players", "120"),
        _kpi_row("New Player Total", "300"),
        _kpi_row("Old Player Total", "900"),
        _kpi_row("Welcome Abuse Invitees", "8"),
        _kpi_row("High Risk Welcome Abuse", "2"),
        _kpi_row("Self/Farming Risk Invitees", "4"),
        ["SECTION", "Not a KPI row", "999", ""],  # row type != KPI -> ignored
    ]
    values, notes = uv.parse_uim_kpi_rows(rows)
    assert values["total_campaign_players"] == 1300.0
    assert values["abuse_freeze_claim_accounts"] == 5.0
    assert values["self_farming_risk_invitees"] == 4.0
    assert set(values.keys()) == set(uv.METRIC_KEYS)
    assert notes["total_campaign_players"] == "weekly export"
    assert "new_users" not in notes  # no note column value supplied


def test_parse_uim_kpi_rows_ignores_non_kpi_row_type():
    rows = [
        ["NOTE", "High Value Players", "999", ""],
        _kpi_row("High Value Players", "120"),
    ]
    values, notes = uv.parse_uim_kpi_rows(rows)
    assert values == {"high_value_players": 120.0}


def test_parse_uim_kpi_rows_skips_unrecognized_and_non_numeric_rows():
    rows = [
        _kpi_row("Some Other Row", "n/a"),
        _kpi_row("High Value Players", "not_a_number"),
        ["KPI", "", "", ""],
        [],
        _kpi_row("Voucher Claimer Accounts", "40"),
    ]
    values, notes = uv.parse_uim_kpi_rows(rows)
    assert values == {"voucher_claimer_accounts": 40.0}


def test_fetch_uim_validation_metrics_uses_injected_rows():
    rows = [_kpi_row("Total Campaign Players", "500")]
    result = uv.fetch_uim_validation_metrics(rows=rows, spreadsheet_id="sheet1", worksheet_title="dashboard")
    assert result["ok"] is True
    assert result["error"] is None
    assert result["values"] == {"total_campaign_players": 500.0}
    assert result["spreadsheet_id"] == "sheet1"
    assert result["worksheet_title"] == "dashboard"


def test_fetch_uim_validation_metrics_defaults_to_dashboard_tab_not_campaign_roi():
    result = uv.fetch_uim_validation_metrics(rows=[])
    assert result["worksheet_title"] == "dashboard"
    assert result["worksheet_title"] != uv.CAMPAIGN_ROI_SHEET_GID


def test_fetch_uim_validation_metrics_missing_credentials_degrades_gracefully():
    # No rows injected and no credentials configured in this environment ->
    # fetch_sheet_rows_by_title raises, and we must degrade rather than crash.
    result = uv.fetch_uim_validation_metrics(spreadsheet_id="sheet1", worksheet_title="dashboard")
    assert result["ok"] is False
    assert result["error"]
    assert result["values"] == {}
    assert result["notes"] == {}
