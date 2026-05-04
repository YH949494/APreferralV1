from datetime import datetime, timedelta, timezone

from retention_kpis import compute_retention_for_month


class _FakeCollection:
    def __init__(self, docs):
        self.docs = list(docs)
        self.find_calls = 0
        self.queries = []

    def find(self, query=None, projection=None):
        self.find_calls += 1
        query = query or {}
        out = []
        self.last_query = query
        self.queries.append(query)
        def _match(doc, q):
            for k, v in q.items():
                if k == "$or":
                    if not any(_match(doc, cond) for cond in v):
                        return False
                elif k == "$and":
                    if not all(_match(doc, cond) for cond in v):
                        return False
                elif isinstance(v, dict) and "$in" in v:
                    if doc.get(k) not in v["$in"]:
                        return False
                elif isinstance(v, dict) and "$gte" in v and "$lt" in v:
                    val = doc.get(k)
                    if isinstance(val, str):
                        try:
                            val = datetime.fromisoformat(val.replace("Z", "+00:00"))
                        except Exception:
                            pass
                    if not (isinstance(val, datetime) and v["$gte"] <= val < v["$lt"]):
                        return False
                elif isinstance(v, dict) and "$regex" in v:
                    import re
                    if not re.search(v["$regex"], str(doc.get(k, ""))):
                        return False
                else:
                    if doc.get(k) != v:
                        return False
            return True

        for d in self.docs:
            if _match(d, query):
                out.append(d)
        return out


class _DB:
    def __init__(self, users, xp_events=None, voucher_claims=None, new_joiner_claims=None, miniapp_sessions_daily=None):
        self.users = _FakeCollection(users)
        self.xp_events = _FakeCollection(xp_events or [])
        self.voucher_claims = _FakeCollection(voucher_claims or [])
        self.new_joiner_claims = _FakeCollection(new_joiner_claims or [])
        self.miniapp_sessions_daily = _FakeCollection(miniapp_sessions_daily or [])


def test_d7_checkin_from_xp_events_counts_retained():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(
        users=[{"user_id": 1, "joined_main_at": join}],
        xp_events=[{"user_id": 1, "type": "checkin", "created_at": join + timedelta(days=7, hours=1)}],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=40))
    assert out["d7_eligible"] == 1
    assert out["d7_retained"] == 1


def test_first_checkin_outside_d7_not_counted():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(users=[{"user_id": 1, "joined_main_at": join, "first_checkin_at": join + timedelta(days=1)}])
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=40))
    assert out["d7_retained"] == 0


def test_last_visible_after_d30_not_counted_for_d7_or_d14():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(users=[{"user_id": 1, "joined_main_at": join, "last_visible_at": join + timedelta(days=31, hours=1)}])
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=60))
    assert out["d7_retained"] == 0
    assert out["d14_retained"] == 0


def test_voucher_claim_timestamp_fallback_fields():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(
        users=[{"user_id": 1, "joined_main_at": join}],
        voucher_claims=[{"user_id": 1, "claimedAt": join + timedelta(days=14, hours=2)}],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=60))
    assert out["d14_retained"] == 1


def test_no_per_user_queries_bulk_pattern():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(users=[{"user_id": 1, "joined_main_at": join}, {"user_id": 2, "joined_main_at": join}])
    compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=40))
    assert db.users.find_calls == 1
    assert db.xp_events.find_calls <= 3
    assert db.voucher_claims.find_calls <= 1
    assert db.new_joiner_claims.find_calls <= 1


def test_activity_outside_bounds_ignored_and_query_bounded():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    inside = join + timedelta(days=7, hours=1)
    outside = join + timedelta(days=70)
    db = _DB(
        users=[{"user_id": 1, "joined_main_at": join}],
        xp_events=[
            {"user_id": 1, "type": "checkin", "created_at": inside},
            {"user_id": 1, "type": "checkin", "created_at": outside},
        ],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=90))
    assert out["d7_retained"] == 1
    queries = db.xp_events.queries
    assert len(queries) == 3
    for q0 in queries:
        assert any(k in q0 for k in ("created_at", "createdAt", "ts"))


def test_claim_string_and_day_string_are_parsed_and_counted():
    join = datetime(2026, 1, 1, 20, 0, tzinfo=timezone.utc)
    db = _DB(
        users=[{"user_id": 1, "joined_main_at": join}],
        voucher_claims=[{"user_id": 1, "createdAt": "2026-01-15T21:00:00+00:00"}],
        miniapp_sessions_daily=[{"uid": 1, "day": datetime(2026, 1, 9, 0, 0, tzinfo=timezone.utc)}],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=60))
    assert out["d7_retained"] == 1
    assert out["d14_retained"] == 1


def test_claim_only_d7_does_not_count_xp_checkin():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(
        users=[{"user_id": 1, "joined_main_at": join}],
        xp_events=[{"user_id": 1, "type": "checkin", "created_at": join + timedelta(days=7, hours=1)}],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=40))
    assert out["d7_retained"] == 1
    assert out["d7_claim_retained"] == 0


def test_claim_only_d14_counts_voucher_claims():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(
        users=[{"user_id": 1, "joined_main_at": join}],
        voucher_claims=[{"user_id": 1, "claimed_at": join + timedelta(days=14, hours=1)}],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=60))
    assert out["d14_claim_retained"] == 1


def test_claim_only_d30_counts_new_joiner_claims_and_rate_eligible_denominator():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(
        users=[
            {"user_id": 1, "joined_main_at": join},
            {"user_id": 2, "joined_main_at": join},
        ],
        new_joiner_claims=[{"uid": 1, "claimed_at": join + timedelta(days=30, hours=1)}],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=90))
    assert out["d30_eligible"] == 2
    assert out["d30_claim_retained"] == 1
    assert out["d30_claim_retention_rate"] == 0.5


def test_general_retention_fields_unchanged_with_claim_metrics():
    join = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = _DB(
        users=[{"user_id": 1, "joined_main_at": join}],
        voucher_claims=[{"user_id": 1, "claimed_at": join + timedelta(days=7, hours=1)}],
    )
    out = compute_retention_for_month(db, datetime(2026, 1, 1, tzinfo=timezone.utc), join + timedelta(days=40))
    assert "d7_retained" in out
    assert "d7_retention_rate" in out
