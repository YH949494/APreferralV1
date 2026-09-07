"""Tests for the campaign-scoped manual referral leaderboard display
override (campaign_display_override.py).

Covers: schedule/enabled gating (incl. UTC boundary + naive-datetime
handling), participant validation (type/range/visibility rejection rules),
combined sort/rank/aggregate derivation, campaign isolation, malformed-doc
and lookup-failure fail-closed behavior, and that manual rows never touch
any genuine referral/reward/KPI collection.

Uses the same mongomock-per-test pattern as test_affiliate_monthly_leaderboard.py
so the genuine leaderboard side (compute_affiliate_monthly_kpis_live) runs
its real aggregation pipeline rather than a hand-rolled fake.
"""

from datetime import datetime, timedelta, timezone

import mongomock

from campaign_display_override import (
    CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION,
    MAX_DISPLAY_NAME_LENGTH,
    MAX_OVERRIDE_PARTICIPANTS,
    build_public_campaign_activity,
    load_active_campaign_override,
    public_campaign_activity_view,
    render_campaign_activity_announcement_text,
)

CAMPAIGN_ID = "referral_sep_2026"
NOW = datetime(2026, 9, 15, 12, 0, 0, tzinfo=timezone.utc)


def _fresh_db():
    return mongomock.MongoClient().db


def _override_doc(**overrides):
    base = {
        "_id": CAMPAIGN_ID,
        "campaign_id": CAMPAIGN_ID,
        "enabled": True,
        "starts_at": NOW - timedelta(days=5),
        "ends_at": NOW + timedelta(days=5),
        "participants": [
            {"entry_id": "seed-001", "display_name": "A***n", "qualified_count": 3, "visible": True},
            {"entry_id": "seed-002", "display_name": "J***8", "qualified_count": 2, "visible": True},
        ],
        "created_at": NOW,
        "updated_at": NOW,
    }
    base.update(overrides)
    return base


def _insert_override(db, **overrides):
    doc = _override_doc(**overrides)
    db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].insert_one(doc)
    return doc


def _seed_genuine_referrer(db, referrer_id: int, qualified_count: int, *, username: str | None = None):
    db.users.insert_one({"user_id": referrer_id, "username": username, "first_name": None})
    for i in range(qualified_count):
        db.qualified_events.insert_one(
            {
                "invitee_id": referrer_id * 1000 + i,
                "referrer_id": referrer_id,
                "qualified_at": NOW,
            }
        )


# ---------------------------------------------------------------------------
# 1-5: enabled/schedule gating, incl. boundaries
# ---------------------------------------------------------------------------

def test_active_enabled_override_is_included():
    db = _fresh_db()
    _insert_override(db)
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["active"] is True
    assert len(result["participants"]) == 2


def test_disabled_override_is_ignored():
    db = _fresh_db()
    _insert_override(db, enabled=False)
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["active"] is False
    assert result["participants"] == []


def test_not_yet_started_override_is_ignored():
    db = _fresh_db()
    _insert_override(db, starts_at=NOW + timedelta(days=1), ends_at=NOW + timedelta(days=10))
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["active"] is False


def test_expired_override_is_ignored_but_document_still_stored():
    db = _fresh_db()
    _insert_override(db, starts_at=NOW - timedelta(days=10), ends_at=NOW - timedelta(days=1))
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["active"] is False
    # Document itself must remain in Mongo (no TTL delete of expired campaigns).
    assert db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID}) is not None


def test_included_exactly_at_starts_at_boundary():
    db = _fresh_db()
    starts_at = NOW
    _insert_override(db, starts_at=starts_at, ends_at=NOW + timedelta(days=1))
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=starts_at)
    assert result["active"] is True


def test_excluded_exactly_at_ends_at_boundary():
    db = _fresh_db()
    ends_at = NOW
    _insert_override(db, starts_at=NOW - timedelta(days=1), ends_at=ends_at)
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=ends_at)
    assert result["active"] is False


# ---------------------------------------------------------------------------
# 6-7: timezone-aware UTC handling, naive/malformed datetimes
# ---------------------------------------------------------------------------

def test_non_utc_aware_datetime_is_normalized_to_utc():
    from zoneinfo import ZoneInfo

    db = _fresh_db()
    kl_tz = ZoneInfo("Asia/Kuala_Lumpur")  # GMT+8
    # 2026-09-07T08:00:00+08:00 == 2026-09-07T00:00:00Z
    starts_at_kl = datetime(2026, 9, 7, 8, 0, 0, tzinfo=kl_tz)
    ends_at_kl = datetime(2026, 9, 30, 23, 59, 59, tzinfo=kl_tz)
    _insert_override(db, starts_at=starts_at_kl, ends_at=ends_at_kl)
    reference = datetime(2026, 9, 7, 0, 0, 1, tzinfo=timezone.utc)
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=reference)
    assert result["active"] is True


def test_naive_datetime_is_treated_as_utc_per_project_convention():
    db = _fresh_db()
    # Simulates PyMongo handing back a naive datetime for a value written as
    # UTC-aware -- the same convention affiliate_leaderboard.py and
    # campaign_centre.py already rely on (naive == UTC, not rejected).
    _insert_override(
        db,
        starts_at=(NOW - timedelta(days=1)).replace(tzinfo=None),
        ends_at=(NOW + timedelta(days=1)).replace(tzinfo=None),
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["active"] is True


def test_malformed_schedule_value_fails_closed():
    db = _fresh_db()
    _insert_override(db, starts_at="not-a-date", ends_at=NOW + timedelta(days=1))
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["active"] is False
    assert result["participants"] == []


# ---------------------------------------------------------------------------
# 8-9: manual edits don't touch genuine data; totals derive from visible rows
# ---------------------------------------------------------------------------

def test_manual_count_increase_never_touches_genuine_collections():
    db = _fresh_db()
    _seed_genuine_referrer(db, 501, 4, username="genuineuser")
    _insert_override(db)

    before_qualified = db.qualified_events.count_documents({})
    before_users_total_referrals = list(db.users.find({}, {"total_referrals": 1}))

    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})
    participants = doc["participants"]
    participants[0]["qualified_count"] = 5
    db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID}, {"$set": {"participants": participants}})

    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    manual_row = next(r for r in activity["leaderboard"] if r["display_name"] == "A***n")
    assert manual_row["qualified_count"] == 5

    assert db.qualified_events.count_documents({}) == before_qualified
    assert list(db.users.find({}, {"total_referrals": 1})) == before_users_total_referrals


def test_qualified_total_is_derived_from_visible_rows_only():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[
            {"entry_id": "v1", "display_name": "Visible1", "qualified_count": 3, "visible": True},
            {"entry_id": "v2", "display_name": "Hidden1", "qualified_count": 100, "visible": False},
        ],
    )
    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    assert activity["qualified_total"] == 3
    assert activity["participant_count"] == 1


# ---------------------------------------------------------------------------
# 10-11: combined sort + rank
# ---------------------------------------------------------------------------

def test_manual_and_genuine_entries_sort_and_rank_correctly():
    db = _fresh_db()
    _seed_genuine_referrer(db, 501, 4, username="genuineuser")
    _insert_override(
        db,
        participants=[
            {"entry_id": "m1", "display_name": "TopManual", "qualified_count": 10, "visible": True},
            {"entry_id": "m2", "display_name": "LowManual", "qualified_count": 1, "visible": True},
        ],
    )
    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    board = activity["leaderboard"]
    counts = [row["qualified_count"] for row in board]
    assert counts == sorted(counts, reverse=True)
    assert board[0]["display_name"] == "TopManual"
    assert board[0]["rank"] == 1
    assert board[-1]["display_name"] == "LowManual"
    assert [row["rank"] for row in board] == list(range(1, len(board) + 1))


# ---------------------------------------------------------------------------
# 12-15: participant validation rejection rules
# ---------------------------------------------------------------------------

def test_hidden_entries_are_excluded_from_leaderboard():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[
            {"entry_id": "v1", "display_name": "Shown", "qualified_count": 5, "visible": True},
            {"entry_id": "v2", "display_name": "Hidden", "qualified_count": 5, "visible": False},
        ],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    names = [p["display_name"] for p in result["participants"]]
    assert names == ["Shown"]


def test_negative_qualified_count_is_excluded():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[{"entry_id": "v1", "display_name": "Bad", "qualified_count": -1, "visible": True}],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["participants"] == []


def test_boolean_qualified_count_is_excluded_even_though_bool_is_an_int_subclass():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[{"entry_id": "v1", "display_name": "Bad", "qualified_count": True, "visible": True}],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["participants"] == []


def test_float_and_numeric_string_qualified_counts_are_excluded():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[
            {"entry_id": "v1", "display_name": "FloatBad", "qualified_count": 3.5, "visible": True},
            {"entry_id": "v2", "display_name": "StringBad", "qualified_count": "4", "visible": True},
        ],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["participants"] == []


def test_unreasonably_large_qualified_count_is_excluded():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[{"entry_id": "v1", "display_name": "TooBig", "qualified_count": 10 ** 9, "visible": True}],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["participants"] == []


def test_empty_or_missing_display_name_is_excluded():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[
            {"entry_id": "v1", "display_name": "   ", "qualified_count": 1, "visible": True},
            {"entry_id": "v2", "qualified_count": 1, "visible": True},
        ],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["participants"] == []


def test_display_name_over_max_length_is_excluded():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[
            {
                "entry_id": "v1",
                "display_name": "X" * (MAX_DISPLAY_NAME_LENGTH + 1),
                "qualified_count": 1,
                "visible": True,
            }
        ],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert result["participants"] == []


def test_display_name_at_max_length_is_accepted():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[
            {
                "entry_id": "v1",
                "display_name": "X" * MAX_DISPLAY_NAME_LENGTH,
                "qualified_count": 1,
                "visible": True,
            }
        ],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert len(result["participants"]) == 1


def test_participant_list_bounded_to_max_entries():
    db = _fresh_db()
    participants = [
        {"entry_id": f"p{i}", "display_name": f"Name{i}", "qualified_count": 1, "visible": True}
        for i in range(MAX_OVERRIDE_PARTICIPANTS + 20)
    ]
    _insert_override(db, participants=participants)
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert len(result["participants"]) <= MAX_OVERRIDE_PARTICIPANTS


# ---------------------------------------------------------------------------
# 16-17: duplicate entry_id + masked-name collision handling
# ---------------------------------------------------------------------------

def test_duplicate_manual_entry_id_keeps_first_and_ignores_rest():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[
            {"entry_id": "dup", "display_name": "First", "qualified_count": 1, "visible": True},
            {"entry_id": "dup", "display_name": "Second", "qualified_count": 99, "visible": True},
        ],
    )
    result = load_active_campaign_override(db, CAMPAIGN_ID, reference_utc=NOW)
    assert len(result["participants"]) == 1
    assert result["participants"][0]["display_name"] == "First"


def test_masked_display_name_collision_does_not_merge_genuine_and_manual_rows():
    db = _fresh_db()
    # No username (so no re-masking applies) -- first_name passes through
    # verbatim and happens to collide with the manual alias below.
    db.users.insert_one({"user_id": 501, "username": None, "first_name": "A***n"})
    for i in range(3):
        db.qualified_events.insert_one({"invitee_id": 501000 + i, "referrer_id": 501, "qualified_at": NOW})
    _insert_override(
        db,
        participants=[{"entry_id": "seed-001", "display_name": "A***n", "qualified_count": 3, "visible": True}],
    )
    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    matching = [r for r in activity["leaderboard"] if r["display_name"] == "A***n"]
    # Both rows survive independently -- never merged into one combined count.
    assert len(matching) == 2
    assert activity["qualified_total"] == 6
    assert activity["diagnostics"]["genuine_rows"] == 1
    assert activity["diagnostics"]["manual_rows"] == 1


# ---------------------------------------------------------------------------
# 18-20: malformed doc / lookup failure / campaign isolation
# ---------------------------------------------------------------------------

def test_malformed_override_document_falls_back_to_genuine_leaderboard():
    db = _fresh_db()
    _seed_genuine_referrer(db, 501, 2, username="genuineuser")
    _insert_override(db, participants="not-a-list")  # structurally malformed
    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    assert activity["state"] == "genuine_only"
    assert activity["diagnostics"]["manual_rows"] == 0
    assert activity["diagnostics"]["genuine_rows"] == 1
    assert activity["qualified_total"] == 2


def test_override_lookup_failure_does_not_break_public_activity(monkeypatch):
    db = _fresh_db()
    _seed_genuine_referrer(db, 501, 2, username="genuineuser")
    _insert_override(db)

    def _boom(*args, **kwargs):
        raise RuntimeError("mongo temporarily unavailable")

    monkeypatch.setattr(db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION], "find_one", _boom)
    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    assert activity["state"] == "genuine_only"
    assert activity["qualified_total"] == 2


def test_campaign_isolation_one_override_never_leaks_into_another_campaign():
    db = _fresh_db()
    _insert_override(db)  # campaign_id = referral_sep_2026
    other_result = load_active_campaign_override(db, "referral_oct_2026", reference_utc=NOW)
    assert other_result["active"] is False
    assert other_result["participants"] == []


# ---------------------------------------------------------------------------
# 21-24: manual rows never touch genuine referral/reward/KPI collections
# ---------------------------------------------------------------------------

def test_no_manual_writes_to_genuine_or_abuse_collections():
    db = _fresh_db()
    _seed_genuine_referrer(db, 501, 2, username="genuineuser")
    guarded_collections = [
        "qualified_events",
        "referral_events",
        "referral_flow_events",
        "affiliate_ledger",
        "voucher_pools",
    ]
    before_counts = {name: db[name].count_documents({}) for name in guarded_collections}

    _insert_override(db)
    build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW + timedelta(days=1))

    after_counts = {name: db[name].count_documents({}) for name in guarded_collections}
    assert after_counts == before_counts


# ---------------------------------------------------------------------------
# 25: public API view omits internal/diagnostic fields
# ---------------------------------------------------------------------------

def test_public_view_omits_diagnostics_and_internal_fields():
    db = _fresh_db()
    _insert_override(db)
    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    public_view = public_campaign_activity_view(activity)
    assert "diagnostics" not in public_view
    assert "_combined_rows" not in public_view
    assert set(public_view.keys()) == {"campaign_id", "state", "participant_count", "qualified_total", "leaderboard"}
    for row in public_view["leaderboard"]:
        assert set(row.keys()) == {"rank", "display_name", "qualified_count"}


# ---------------------------------------------------------------------------
# 26: editing the Mongo document changes the next read, no restart needed
# ---------------------------------------------------------------------------

def test_editing_override_document_changes_next_read_immediately():
    db = _fresh_db()
    _insert_override(db)
    first = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    first_total = first["qualified_total"]

    doc = db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].find_one({"_id": CAMPAIGN_ID})
    participants = doc["participants"]
    participants[0]["qualified_count"] = 999
    db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID}, {"$set": {"participants": participants}})

    second = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    assert second["qualified_total"] != first_total
    assert second["qualified_total"] == first_total - 3 + 999


def test_disabling_campaign_removes_override_on_next_read():
    db = _fresh_db()
    _insert_override(db)
    active = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    assert active["state"] == "active"

    db[CAMPAIGN_DISPLAY_OVERRIDE_COLLECTION].update_one({"_id": CAMPAIGN_ID}, {"$set": {"enabled": False}})
    disabled = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    assert disabled["state"] == "genuine_only"
    assert disabled["diagnostics"]["manual_rows"] == 0


# ---------------------------------------------------------------------------
# Announcement preview helper
# ---------------------------------------------------------------------------

def test_render_campaign_activity_announcement_text_escapes_and_uses_shared_totals():
    db = _fresh_db()
    _insert_override(
        db,
        participants=[{"entry_id": "v1", "display_name": "<script>alert(1)</script>", "qualified_count": 5, "visible": True}],
    )
    activity = build_public_campaign_activity(db, CAMPAIGN_ID, reference_utc=NOW)
    text = render_campaign_activity_announcement_text(activity)
    assert "<script>" not in text
    assert "5 qualified invites" in text
