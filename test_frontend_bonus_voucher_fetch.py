from pathlib import Path


def test_bonus_voucher_fetch_uses_init_data_path_not_raw_user_id_query():
    html = Path("static/index.html").read_text(encoding="utf-8")
    assert "fetch(`${API_BASE}/api/bonus_voucher?user_id=${userId}`)" not in html
    assert "withInitDataInQuery(`${API_BASE}/api/bonus_voucher`, initDataRaw)" in html
    assert "withInitDataInQuery(`${API_BASE}/api/campaign_bonus_voucher`, initDataRaw)" in html


def test_admin_bonus_paths_wait_for_init_data_and_do_not_use_raw_user_id_query():
    html = Path("static/index.html").read_text(encoding="utf-8")
    assert "fetch(`${API_BASE}/api/is_admin?user_id=${userId}`)" not in html
    assert "const initDataRaw = await waitForInitData();" in html
    assert "const adminUrl = withInitDataInQuery(`${API_BASE}/api/is_admin`, initDataRaw);" in html
    assert "const bonusVoucherUrl = withInitDataInQuery(`${API_BASE}/api/bonus_voucher`, initDataRaw);" in html
    assert "const campaignBonusVoucherUrl = withInitDataInQuery(`${API_BASE}/api/campaign_bonus_voucher`, initDataRaw);" in html
    assert "const authHeaders = v2Headers({}, initDataRaw);" in html
