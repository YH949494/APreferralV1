import pytest

from vouchers import (
    _drop_audience_type,
    _is_new_joiner_audience,
    _normalize_audience_marker,
)
import vouchers as vouchers_module


# ---- _normalize_audience_marker / _drop_audience_type ----

def test_audience_type_new_joiner_recognized():
    drop = {"audience": {"type": "new_joiner"}}
    assert _drop_audience_type(drop) == "new_joiner"
    assert _is_new_joiner_audience(_drop_audience_type(drop))


def test_campaign_type_welcome_voucher_recognized():
    drop = {"campaign_type": "welcome_voucher", "audience": {}}
    assert _drop_audience_type(drop) == "new_joiner"


def test_category_welcome_voucher_recognized():
    drop = {"category": "welcome_voucher"}
    assert _drop_audience_type(drop) == "new_joiner"


def test_tags_include_welcome_voucher_recognized():
    drop = {"tags": ["seasonal", "welcome_voucher"]}
    assert _drop_audience_type(drop) == "new_joiner"


def test_legacy_whitelist_username_new_joiner_recognized():
    drop = {"whitelistUsernames": ["new_joiner"]}
    assert _drop_audience_type(drop) == "new_joiner"


def test_legacy_whitelist_username_new_joiner_48h_recognized():
    drop = {"whitelistUsernames": ["new_joiner_48h"]}
    assert _drop_audience_type(drop) == "new_joiner_48h"


def test_normal_public_drop_remains_public():
    assert _drop_audience_type({}) == "public"
    assert _drop_audience_type({"audience": {}}) == "public"
    assert _drop_audience_type({"whitelistUsernames": ["@someone"]}) == "public"


def test_vip_audience_unaffected():
    assert _drop_audience_type({"audience": {"type": "vip1"}}) == "vip1"


def test_welcome_marker_overrides_explicit_public_audience_type():
    drop = {"audience": {"type": "public"}, "campaign_type": "welcome_voucher"}
    assert _drop_audience_type(drop) == "new_joiner"


def test_various_explicit_welcome_marker_fields_normalize_to_new_joiner():
    variants = [
        {"audience_type": "welcome"},
        {"audienceType": "welcome_bonus"},
        {"campaignType": "welcome_voucher"},
        {"reward_type": "new member"},
        {"rewardType": "newjoiner"},
        {"audience": {"kind": "new joiner"}},
        {"audience": {"segment": "welcome"}},
    ]
    for drop in variants:
        assert _drop_audience_type(drop) == "new_joiner", drop


def test_explicit_new_joiner_48h_variants_map_correctly():
    variants = [
        {"campaign_type": "new_joiner_48h"},
        {"category": "new joiner 48h"},
        {"tags": ["newjoiner48h"]},
    ]
    for drop in variants:
        assert _drop_audience_type(drop) == "new_joiner_48h", drop


def test_normalize_audience_marker_rejects_unknown_values():
    assert _normalize_audience_marker("vip1") is None
    assert _normalize_audience_marker("") is None
    assert _normalize_audience_marker(None) is None
    assert _normalize_audience_marker(123) is None
