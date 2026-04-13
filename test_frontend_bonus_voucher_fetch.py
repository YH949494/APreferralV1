from pathlib import Path


def test_bonus_voucher_fetch_uses_init_data_path_not_raw_user_id_query():
    html = Path("static/index.html").read_text(encoding="utf-8")
    assert "fetch(`${API_BASE}/api/bonus_voucher?user_id=${userId}`)" not in html
    assert "withInitDataInQuery(`${API_BASE}/api/bonus_voucher`, initDataRaw)" in html
