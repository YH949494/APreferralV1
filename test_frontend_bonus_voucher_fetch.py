from pathlib import Path


def test_bonus_voucher_fetch_uses_init_data_path_not_raw_user_id_query():
    html = Path("static/index.html").read_text(encoding="utf-8")
    assert "fetch(`${API_BASE}/api/bonus_voucher?user_id=${userId}`)" not in html
    assert "withInitDataInQuery(`${API_BASE}/api/affiliate_bonus_vouchers`, initDataRaw)" in html
    assert "withInitDataInQuery(`${API_BASE}/api/campaign_bonus_voucher`, initDataRaw)" in html


def test_admin_bonus_paths_wait_for_init_data_and_do_not_use_raw_user_id_query():
    html = Path("static/index.html").read_text(encoding="utf-8")
    assert "fetch(`${API_BASE}/api/is_admin?user_id=${userId}`)" not in html
    assert "const initDataRaw = await waitForInitData();" in html
    assert "const adminUrl = withInitDataInQuery(`${API_BASE}/api/is_admin`, initDataRaw);" in html
    assert "const bonusVoucherUrl = withInitDataInQuery(`${API_BASE}/api/affiliate_bonus_vouchers`, initDataRaw);" in html
    assert "const campaignBonusVoucherUrl = withInitDataInQuery(`${API_BASE}/api/campaign_bonus_voucher`, initDataRaw);" in html
    assert "const authHeaders = v2Headers({}, initDataRaw);" in html


def test_affiliate_rewards_block_hides_when_empty_and_renders_multiple():
    html = Path("static/index.html").read_text(encoding="utf-8")
    assert "function renderAffiliateRewards(rewards)" in html
    assert "if (!Array.isArray(rewards) || rewards.length === 0)" in html
    assert "bonusSection.style.display = \"none\";" in html
    assert "rewards.forEach((reward) => {" in html
    assert "🎁 Your Affiliate Rewards" in html


def test_claim_error_state_rendering_hooks_present():
    html = Path("static/index.html").read_text(encoding="utf-8")
    assert 'displayState === "fully_redeemed"' in html
    assert 'displayState === "high_traffic"' in html
    assert 'hideRetry: true' in html
    assert 'displayState === "cooldown"' in html
    assert 'Retry available in ${remaining}s' in html
    assert 'Fully Claimed' in html
