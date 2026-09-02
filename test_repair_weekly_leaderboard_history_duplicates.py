import datetime

from bson import ObjectId

from scripts.repair_weekly_leaderboard_history_duplicates import plan_repair


class _DeleteResult:
    def __init__(self, deleted_count):
        self.deleted_count = deleted_count


class _FakeCollection:
    def __init__(self, docs):
        self.docs = list(docs)

    def aggregate(self, pipeline, allowDiskUse=False):
        counts: dict = {}
        ids: dict = {}
        for doc in self.docs:
            ws = doc.get("week_start")  # missing and explicit None group together
            counts[ws] = counts.get(ws, 0) + 1
            ids.setdefault(ws, []).append(doc["_id"])
        for ws, count in counts.items():
            if count > 1:
                yield {"_id": ws, "count": count, "doc_ids": ids[ws]}

    def find(self, query):
        ws = query.get("week_start")
        return [d for d in self.docs if d.get("week_start") == ws]

    def delete_many(self, query):
        ids = set(query["_id"]["$in"])
        before = len(self.docs)
        self.docs = [d for d in self.docs if d["_id"] not in ids]
        return _DeleteResult(before - len(self.docs))


def _doc(**kwargs):
    base = {
        "_id": ObjectId(),
        "week_start": "2025-08-25",
        "week_end": "2025-08-31",
        "checkin_leaderboard": [{"user_id": 1}],
        "referral_leaderboard": [{"user_id": 1}],
        "archived_at": datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc),
    }
    base.update(kwargs)
    return base


def test_no_duplicates_yields_no_plan():
    col = _FakeCollection([_doc(week_start="2025-08-25"), _doc(week_start="2025-09-01")])
    assert plan_repair(col) == []


def test_prefers_valid_schema_over_malformed():
    good = _doc(archived_at=datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc))
    malformed = _doc(checkin_leaderboard="not-a-list", archived_at=datetime.datetime(2025, 9, 2, tzinfo=datetime.timezone.utc))
    col = _FakeCollection([good, malformed])

    plans = plan_repair(col)
    assert len(plans) == 1
    plan = plans[0]
    assert plan["keeper_id"] == good["_id"]
    assert plan["delete_ids"] == [malformed["_id"]]
    assert "valid_schema" in plan["reason_keeper_selected"]


def test_prefers_more_complete_leaderboard():
    sparse = _doc(checkin_leaderboard=[{"user_id": 1}], referral_leaderboard=[])
    complete = _doc(
        checkin_leaderboard=[{"user_id": 1}, {"user_id": 2}],
        referral_leaderboard=[{"user_id": 1}],
    )
    col = _FakeCollection([sparse, complete])

    plans = plan_repair(col)
    assert plans[0]["keeper_id"] == complete["_id"]
    assert "leaderboard_completeness" in plans[0]["reason_keeper_selected"]


def test_prefers_newest_archived_at_when_schema_and_completeness_tie():
    older = _doc(archived_at=datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc))
    newer = _doc(archived_at=datetime.datetime(2025, 9, 2, tzinfo=datetime.timezone.utc))
    col = _FakeCollection([older, newer])

    plans = plan_repair(col)
    assert plans[0]["keeper_id"] == newer["_id"]
    assert "newest_archived_at" in plans[0]["reason_keeper_selected"]


def test_repair_is_idempotent_across_two_runs():
    older = _doc(archived_at=datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc))
    newer = _doc(archived_at=datetime.datetime(2025, 9, 2, tzinfo=datetime.timezone.utc))
    col = _FakeCollection([older, newer])

    plans = plan_repair(col)
    assert len(plans) == 1
    col.delete_many({"_id": {"$in": plans[0]["delete_ids"]}})

    assert plan_repair(col) == []
    assert len(col.docs) == 1
    assert col.docs[0]["_id"] == newer["_id"]


def test_null_and_missing_week_start_are_treated_as_one_duplicate_group():
    # A non-sparse unique index treats a missing field and an explicit null
    # as the same key, so these two documents would collide just as much as
    # a real duplicate week_start would.
    explicit_null = _doc(week_start=None)
    missing = _doc()
    del missing["week_start"]
    col = _FakeCollection([explicit_null, missing])

    plans = plan_repair(col)
    assert len(plans) == 1
    assert plans[0]["week_start"] is None
    assert plans[0]["document_count"] == 2
    assert set(plans[0]["delete_ids"]) == {explicit_null["_id"], missing["_id"]} - {plans[0]["keeper_id"]}

    col.delete_many({"_id": {"$in": plans[0]["delete_ids"]}})
    assert plan_repair(col) == []
    assert len(col.docs) == 1
