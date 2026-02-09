from database import normalize_affiliate_role, normalize_affiliate_status


def test_normalize_affiliate_status_defaults_to_none():
    assert normalize_affiliate_status(None) == "none"
    assert normalize_affiliate_status("unknown") == "none"


def test_normalize_affiliate_status_accepts_allowed_values():
    for status in ("none", "pending", "active", "suspended"):
        assert normalize_affiliate_status(status) == status


def test_normalize_affiliate_role_defaults_to_member():
    assert normalize_affiliate_role(None) == "member"
    assert normalize_affiliate_role("unknown") == "member"


def test_normalize_affiliate_role_accepts_allowed_values():
    for role in ("member", "affiliate"):
        assert normalize_affiliate_role(role) == role
