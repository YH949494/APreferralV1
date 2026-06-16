"""Unit tests for the read-only UIM KPI sheet parsing helpers."""

from __future__ import annotations

import uim_validation as uv


def test_parse_uim_kpi_rows_two_column_table():
    rows = [
        ["Metric", "Value"],
        ["Total Campaign Players", "1,300"],
        ["Voucher Claimers", "410"],
        ["High Value Players", "120"],
        ["Normal Actual Players", "300"],
        ["Low Value Players", "200"],
        ["Voucher Hunters", "40"],
        ["New Players", "150"],
        ["Old Players", "900"],
        ["Claim Risk", "12.5%"],
        ["Campaign Quality", "88"],
        ["Affiliate Quality", "91"],
        ["Actual Players", "950"],
    ]
    values = uv.parse_uim_kpi_rows(rows)
    assert values["total_campaign_players"] == 1300.0
    assert values["voucher_claimers"] == 410.0
    assert values["claim_risk"] == 12.5
    assert set(values.keys()) == set(uv.METRIC_KEYS)


def test_parse_uim_kpi_rows_skips_unrecognized_and_non_numeric_rows():
    rows = [
        ["Some Other Row", "n/a"],
        ["High Value Players", "not_a_number"],
        ["", ""],
        [],
        ["Voucher Hunters", "40"],
    ]
    values = uv.parse_uim_kpi_rows(rows)
    assert values == {"voucher_hunters": 40.0}


def test_fetch_uim_validation_metrics_uses_injected_rows():
    rows = [["Total Campaign Players", "500"]]
    result = uv.fetch_uim_validation_metrics(rows=rows, spreadsheet_id="sheet1", worksheet_gid="999")
    assert result["ok"] is True
    assert result["error"] is None
    assert result["values"] == {"total_campaign_players": 500.0}
    assert result["spreadsheet_id"] == "sheet1"
    assert result["worksheet_gid"] == "999"


def test_fetch_uim_validation_metrics_missing_credentials_degrades_gracefully():
    # No rows injected and no credentials configured in this environment ->
    # fetch_sheet_rows raises, and we must degrade rather than crash.
    result = uv.fetch_uim_validation_metrics(spreadsheet_id="sheet1", worksheet_gid="999")
    assert result["ok"] is False
    assert result["error"]
    assert result["values"] == {}
