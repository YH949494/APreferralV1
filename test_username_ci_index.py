"""
Regression guard: verify that the case-insensitive collation used by the
users.username $in query (backend_segment_engine.py) matches the collation
index created in database.ensure_indexes(), so MongoDB can use the index
for each batch instead of falling back to a full collection scan.

The query (backend_segment_engine.py) and the index (database.py) must use:
  collation: { locale: "en", strength: 2 }

Run with:  python3 test_username_ci_index.py
"""
import sys

EXPECTED_NAME = "username_ci_idx"
EXPECTED_LOCALE = '"locale": "en"'
EXPECTED_STRENGTH = '"strength": 2'


def _read(path):
    with open(path) as f:
        return f.read()


def test_database_index():
    src = _read("database.py")
    assert EXPECTED_NAME in src, \
        f"database.py: missing index name '{EXPECTED_NAME}'"
    assert EXPECTED_LOCALE in src, \
        f"database.py: missing {EXPECTED_LOCALE} in collation spec"
    assert EXPECTED_STRENGTH in src, \
        f"database.py: missing {EXPECTED_STRENGTH} in collation spec"


def test_engine_query_collation():
    src = _read("backend_segment_engine.py")
    assert ".collation(" in src, \
        "backend_segment_engine.py: missing .collation() call on username $in query"
    assert EXPECTED_LOCALE in src, \
        f"backend_segment_engine.py: missing {EXPECTED_LOCALE} in query collation"
    assert EXPECTED_STRENGTH in src, \
        f"backend_segment_engine.py: missing {EXPECTED_STRENGTH} in query collation"


def test_collation_specs_match():
    """Both files must reference identical locale/strength so MongoDB picks the index."""
    db_src = _read("database.py")
    bse_src = _read("backend_segment_engine.py")

    for spec_fragment in (EXPECTED_LOCALE, EXPECTED_STRENGTH):
        assert spec_fragment in db_src, \
            f"database.py missing '{spec_fragment}'"
        assert spec_fragment in bse_src, \
            f"backend_segment_engine.py missing '{spec_fragment}'"


# ---------------------------------------------------------------------------

_TESTS = [
    test_database_index,
    test_engine_query_collation,
    test_collation_specs_match,
]

if __name__ == "__main__":
    passed = failed = 0
    print("\nusers.username collation-index consistency tests\n")
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
