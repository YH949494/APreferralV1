"""Unit tests for funnel_dashboard.py — covers the 4 review-comment fixes.

Tests:
  1. date_to=YYYY-MM-DD includes the entire selected day (exclusive bound)
  2. Stage activity after custom date_to is excluded
  3. after_total_bet_amount stored as string is counted as first bet
  4. after_total_bet_amount empty string is not counted as first bet
  5. is_new_player missing/blank → unknown, not returning
  6. Explicit false values → returning
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from funnel_dashboard import (
    _coerce_float,
    _is_explicit_false,
    _is_explicit_true,
    compute_funnel,
)

# ---------------------------------------------------------------------------
# Helper: minimal MongoDB stub
# ---------------------------------------------------------------------------

class _Cursor:
    def __init__(self, docs: list[dict], projection: dict | None = None):
        self._docs = docs

    def __iter__(self):
        return iter(self._docs)


def _matches(doc: dict, query: dict) -> bool:
    """Recursively evaluate a simple MongoDB query against a document."""
    for key, cond in query.items():
        if key == "$or":
            if not any(_matches(doc, sub) for sub in cond):
                return False
            continue
        if key == "$and":
            if not all(_matches(doc, sub) for sub in cond):
                return False
            continue
        val = doc.get(key)
        if isinstance(cond, dict):
            for op, operand in cond.items():
                if op == "$gte":
                    if val is None or val < operand:
                        return False
                elif op == "$gt":
                    if val is None or val <= operand:
                        return False
                elif op == "$lt":
                    if val is None or val >= operand:
                        return False
                elif op == "$lte":
                    if val is None or val > operand:
                        return False
                elif op == "$eq":
                    if val != operand:
                        return False
                elif op == "$ne":
                    if val == operand:
                        return False
                elif op == "$in":
                    if val not in operand:
                        return False
                elif op == "$exists":
                    if operand and val is None:
                        return False
                    if not operand and val is not None:
                        return False
                elif op == "$not":
                    # {"$not": {"$gte": x}} → field < x OR field missing
                    inner = operand
                    matched_inner = True
                    for iop, ioperand in inner.items():
                        if iop == "$gte":
                            if val is None or val < ioperand:
                                matched_inner = False
                        elif iop == "$gt":
                            if val is None or val <= ioperand:
                                matched_inner = False
                    if matched_inner:
                        return False
                elif op == "$regex":
                    import re
                    if not (isinstance(val, str) and re.search(operand, val)):
                        return False
        else:
            if val != cond:
                return False
    return True


class _Collection:
    def __init__(self, docs: list[dict]):
        self._docs = docs

    def find(self, query: dict, projection: dict | None = None) -> _Cursor:
        return _Cursor([d for d in self._docs if _matches(d, query)])


class _MockDb:
    def __init__(self, **collections: list[dict]):
        for name, docs in collections.items():
            setattr(self, name, _Collection(docs))

    def __getattr__(self, name: str) -> _Collection:
        return _Collection([])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 6, 19, 12, 0, 0, tzinfo=timezone.utc)

# A user who joined on 2026-06-18 (well within any window ending 2026-06-19)
_USER_A = {
    "user_id": 1001,
    "joined_main_at": datetime(2026, 6, 18, 10, 0, 0, tzinfo=timezone.utc),
    "first_private_interaction_at": datetime(2026, 6, 18, 11, 0, 0, tzinfo=timezone.utc),
}

# ---------------------------------------------------------------------------
# Unit tests: helper functions
# ---------------------------------------------------------------------------

class TestCoerceFloat:
    def test_numeric_int(self):
        assert _coerce_float(100) == 100.0

    def test_numeric_float(self):
        assert _coerce_float(99.5) == 99.5

    def test_string_number(self):
        assert _coerce_float("100") == 100.0

    def test_string_with_spaces(self):
        assert _coerce_float("  42.5  ") == 42.5

    def test_empty_string(self):
        assert _coerce_float("") == 0.0

    def test_none(self):
        assert _coerce_float(None) == 0.0

    def test_non_numeric_string(self):
        assert _coerce_float("N/A") == 0.0


class TestIsExplicitTrue:
    def test_bool_true(self):
        assert _is_explicit_true(True) is True

    def test_int_1(self):
        assert _is_explicit_true(1) is True

    def test_string_true(self):
        assert _is_explicit_true("true") is True

    def test_string_1(self):
        assert _is_explicit_true("1") is True

    def test_string_yes(self):
        assert _is_explicit_true("yes") is True

    def test_string_y(self):
        assert _is_explicit_true("y") is True

    def test_none_is_not_true(self):
        assert _is_explicit_true(None) is False

    def test_empty_string_is_not_true(self):
        assert _is_explicit_true("") is False

    def test_false_is_not_true(self):
        assert _is_explicit_true(False) is False


class TestIsExplicitFalse:
    def test_bool_false(self):
        assert _is_explicit_false(False) is True

    def test_int_0(self):
        assert _is_explicit_false(0) is True

    def test_string_false(self):
        assert _is_explicit_false("false") is True

    def test_string_0(self):
        assert _is_explicit_false("0") is True

    def test_string_no(self):
        assert _is_explicit_false("no") is True

    def test_string_n(self):
        assert _is_explicit_false("n") is True

    def test_none_is_not_false(self):
        assert _is_explicit_false(None) is False

    def test_empty_string_is_not_false(self):
        assert _is_explicit_false("") is False

    def test_true_is_not_false(self):
        assert _is_explicit_false(True) is False


# ---------------------------------------------------------------------------
# Integration tests: compute_funnel
# ---------------------------------------------------------------------------

class TestCustomEndDateInclusion:
    """Issue 1: date_to=YYYY-MM-DD should include the entire selected day.

    The route in main.py now normalises a date-only date_to to midnight of the
    *next* day, then uses $lt so that the entire selected day is captured.
    """

    def test_user_joined_late_on_selected_day_is_included(self):
        """A user who joined at 23:59 on date_to must appear in the cohort."""
        user = {
            "user_id": 2001,
            "joined_main_at": datetime(2026, 6, 19, 23, 59, 59, tzinfo=timezone.utc),
        }
        # end = midnight of next day (2026-06-20 00:00:00 UTC)
        end = datetime(2026, 6, 20, 0, 0, 0, tzinfo=timezone.utc)
        db = _MockDb(users=[user])
        result = compute_funnel(db, start=None, end=end, now=_NOW)
        assert result["cohort_size"] == 1, (
            "User who joined at 23:59:59 on selected day must be in cohort"
        )

    def test_user_joined_at_exact_next_midnight_is_excluded(self):
        """A user joining exactly at the next-day midnight must NOT be in cohort."""
        user = {
            "user_id": 2002,
            "joined_main_at": datetime(2026, 6, 20, 0, 0, 0, tzinfo=timezone.utc),
        }
        end = datetime(2026, 6, 20, 0, 0, 0, tzinfo=timezone.utc)
        db = _MockDb(users=[user])
        result = compute_funnel(db, start=None, end=end, now=_NOW)
        assert result["cohort_size"] == 0, (
            "User joining at exactly next-day midnight must not be included"
        )


class TestStageActivityBoundedByEnd:
    """Issue 2: stage activity after custom date_to must be excluded."""

    def test_checkin_after_end_is_excluded(self):
        """Check-in event timestamped after end must not advance the user to check-in stage."""
        end = datetime(2026, 6, 19, 0, 0, 0, tzinfo=timezone.utc)  # midnight of Jun 19
        checkin_after_end = {
            "user_id": _USER_A["user_id"],
            "type": "checkin",
            "created_at": datetime(2026, 6, 19, 1, 0, 0, tzinfo=timezone.utc),  # after end
        }
        db = _MockDb(users=[_USER_A], xp_events=[checkin_after_end])
        result = compute_funnel(db, start=None, end=end, now=_NOW)
        first_checkin = next(s for s in result["stages"] if s["id"] == "first_checkin")
        assert first_checkin["count"] == 0, (
            "Check-in after custom end date must not be counted"
        )

    def test_checkin_before_end_is_included(self):
        """Check-in event before end must advance the user."""
        end = datetime(2026, 6, 19, 0, 0, 0, tzinfo=timezone.utc)
        checkin_before_end = {
            "user_id": _USER_A["user_id"],
            "type": "checkin",
            "created_at": datetime(2026, 6, 18, 15, 0, 0, tzinfo=timezone.utc),
        }
        db = _MockDb(users=[_USER_A], xp_events=[checkin_before_end])
        result = compute_funnel(db, start=None, end=end, now=_NOW)
        first_checkin = next(s for s in result["stages"] if s["id"] == "first_checkin")
        assert first_checkin["count"] == 1, (
            "Check-in before custom end date must be counted"
        )


class TestBetAmountStringCoercion:
    """Issue 3: after_total_bet_amount may be a string from CSV upload."""

    def _run_with_bet(self, bet_value: Any) -> dict:
        end = datetime(2026, 6, 20, 0, 0, 0, tzinfo=timezone.utc)
        coupon = "TESTCODE"
        voucher_claim = {"user_id": _USER_A["user_id"], "coupon_code": coupon}
        mkt_row = {
            "coupon_code": coupon,
            "after_total_bet_amount": bet_value,
            # created_at needed so the date-range $or filter in compute_funnel matches
            "created_at": datetime(2026, 6, 18, 12, 0, 0, tzinfo=timezone.utc),
        }
        db = _MockDb(
            users=[_USER_A],
            voucher_claims=[voucher_claim],
            marketing_raw_data=[mkt_row],
        )
        return compute_funnel(db, start=None, end=end, now=_NOW)

    def test_string_amount_counted(self):
        """after_total_bet_amount='100' must be treated as a valid first bet."""
        result = self._run_with_bet("100")
        first_bet = next(s for s in result["stages"] if s["id"] == "first_bet")
        assert first_bet["count"] == 1, (
            "String bet amount '100' must be coerced and counted as first bet"
        )

    def test_empty_string_not_counted(self):
        """after_total_bet_amount='' must be treated as no bet."""
        result = self._run_with_bet("")
        first_bet = next(s for s in result["stages"] if s["id"] == "first_bet")
        assert first_bet["count"] is None, (
            "Empty string bet amount must not count as first bet"
        )

    def test_zero_string_not_counted(self):
        """after_total_bet_amount='0' must not count as a first bet."""
        result = self._run_with_bet("0")
        first_bet = next(s for s in result["stages"] if s["id"] == "first_bet")
        assert first_bet["count"] is None, (
            "String '0' bet amount must not count as first bet"
        )


class TestPlayerSplitUnknown:
    """Issue 4: missing/blank is_new_player → unknown, not returning."""

    def _run_with_is_new(self, is_new_value: Any) -> dict:
        end = datetime(2026, 6, 20, 0, 0, 0, tzinfo=timezone.utc)
        coupon = "SPLITCODE"
        voucher_claim = {"user_id": _USER_A["user_id"], "coupon_code": coupon}
        # The player-split query doesn't use a date filter on marketing_raw_data,
        # so no created_at needed here.
        mkt_row = {"coupon_code": coupon, "is_new_player": is_new_value}
        db = _MockDb(
            users=[_USER_A],
            voucher_claims=[voucher_claim],
            marketing_raw_data=[mkt_row],
        )
        return compute_funnel(db, start=None, end=end, now=_NOW)

    def test_missing_is_new_is_unknown(self):
        """is_new_player absent from document → unknown."""
        end = datetime(2026, 6, 20, 0, 0, 0, tzinfo=timezone.utc)
        coupon = "SPLITCODE"
        voucher_claim = {"user_id": _USER_A["user_id"], "coupon_code": coupon}
        mkt_row = {"coupon_code": coupon}  # no is_new_player key
        db = _MockDb(
            users=[_USER_A],
            voucher_claims=[voucher_claim],
            marketing_raw_data=[mkt_row],
        )
        result = compute_funnel(db, start=None, end=end, now=_NOW)
        ps = result["player_split"]
        assert ps["returning_player_count"] == 0, "Missing is_new_player must not go to returning"
        assert ps["unknown_count"] == 1, "Missing is_new_player must go to unknown"

    def test_none_is_unknown(self):
        result = self._run_with_is_new(None)
        ps = result["player_split"]
        assert ps["returning_player_count"] == 0
        assert ps["unknown_count"] == 1

    def test_empty_string_is_unknown(self):
        result = self._run_with_is_new("")
        ps = result["player_split"]
        assert ps["returning_player_count"] == 0
        assert ps["unknown_count"] == 1

    def test_explicit_false_is_returning(self):
        """is_new_player=False → returning_player_count."""
        result = self._run_with_is_new(False)
        ps = result["player_split"]
        assert ps["returning_player_count"] == 1
        assert ps["unknown_count"] == 0

    def test_string_false_is_returning(self):
        result = self._run_with_is_new("false")
        ps = result["player_split"]
        assert ps["returning_player_count"] == 1
        assert ps["unknown_count"] == 0

    def test_string_0_is_returning(self):
        result = self._run_with_is_new("0")
        ps = result["player_split"]
        assert ps["returning_player_count"] == 1
        assert ps["unknown_count"] == 0

    def test_string_no_is_returning(self):
        result = self._run_with_is_new("no")
        ps = result["player_split"]
        assert ps["returning_player_count"] == 1
        assert ps["unknown_count"] == 0

    def test_explicit_true_is_new(self):
        """is_new_player=True → new_player_count."""
        result = self._run_with_is_new(True)
        ps = result["player_split"]
        assert ps["new_player_count"] == 1
        assert ps["returning_player_count"] == 0
        assert ps["unknown_count"] == 0
