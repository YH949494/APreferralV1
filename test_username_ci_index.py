"""
Regression guard: verify that:

1. database.py still creates the username_ci_idx collation index (kept for
   any ad-hoc username lookups / future rules).

2. backend_segment_engine.py uses the coupon-code → voucher_claims identity
   join (NOT the old account → users.username join which had 0 % match rate).

Run with:  python3 test_username_ci_index.py
"""
import sys

EXPECTED_INDEX_NAME = "username_ci_idx"
EXPECTED_LOCALE = '"locale": "en"'
EXPECTED_STRENGTH = '"strength": 2'


def _read(path):
    with open(path) as f:
        return f.read()


def test_database_index():
    """database.py must still create the username_ci_idx collation index."""
    src = _read("database.py")
    assert EXPECTED_INDEX_NAME in src, \
        f"database.py: missing index name '{EXPECTED_INDEX_NAME}'"
    assert EXPECTED_LOCALE in src, \
        f"database.py: missing {EXPECTED_LOCALE} in collation spec"
    assert EXPECTED_STRENGTH in src, \
        f"database.py: missing {EXPECTED_STRENGTH} in collation spec"


def test_engine_uses_coupon_join():
    """Engine must resolve identity via coupon_code → voucher_claims, not account → username."""
    src = _read("backend_segment_engine.py")
    assert "coupon_to_uid" in src, \
        "backend_segment_engine.py: missing coupon_to_uid map (coupon→voucher_claims join)"
    assert "_doc_coupon" in src, \
        "backend_segment_engine.py: missing _doc_coupon helper"
    assert "voucher_code" in src, \
        "backend_segment_engine.py: missing voucher_code field reference"
    # Old account→username join must NOT be the identity lookup
    assert 'users.find({"username"' not in src and '"username": {"$in"' not in src, \
        "backend_segment_engine.py: still using username $in query for identity — should use coupon_code join"


def test_engine_summary_includes_identity_fields():
    """Engine summary must expose matched_rows / unmatched_rows / identity_match_rate."""
    src = _read("backend_segment_engine.py")
    for field in ("matched_rows", "unmatched_rows", "identity_match_rate"):
        assert f'"{field}"' in src, \
            f"backend_segment_engine.py: missing summary field '{field}'"


# ---------------------------------------------------------------------------

_TESTS = [
    test_database_index,
    test_engine_uses_coupon_join,
    test_engine_summary_includes_identity_fields,
]

if __name__ == "__main__":
    passed = failed = 0
    print("\nIdentity resolution + index consistency tests\n")
    for fn in _TESTS:
        try:
            fn()
            print("  PASS", fn.__name__)
            passed += 1
        except AssertionError as exc:
            print("  FAIL", fn.__name__, "—", exc)
            failed += 1
    print(f"\n{passed} passed, {failed} failed\n")
    if failed:
        sys.exit(1)
