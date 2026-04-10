from datetime import datetime, timedelta, timezone

from pymongo.errors import DuplicateKeyError

from affiliate_leaderboard import (
    MAX_COUNTED_JOINS_PER_DAY,
    MIN_SECONDS_BETWEEN_COUNTED_JOINS,
    compute_affiliate_weekly_kpis_live,
    _build_affiliate_weekly_payload,
    affiliate_week_window_utc_from_reference,
    affiliate_previous_completed_week_window_kl,
    affiliate_week_window_from_week_key_kl,
    should_count_referral_join,
    serialize_affiliate_snapshot_entries_for_viewer,
)


class _FakeCooldownCollection:
    def __init__(self):
        self.docs = {}

    def find_one_and_update(self, filt, update, upsert=False, return_document=None):
        doc_id = filt["_id"]
        doc = self.docs.get(doc_id)
        cap_limit = filt["$and"][0]["$or"][1]["counted_joins_today"]["$lt"]
        cap_ok = doc is None or int(doc.get("counted_joins_today", 0)) < cap_limit
        ts = None if doc is None else doc.get("last_counted_join_ts")
        cooldown_cutoff = filt["$and"][1]["$or"][1]["last_counted_join_ts"]["$lte"]
        cooldown_ok = ts is None or ts <= cooldown_cutoff
        if not (cap_ok and cooldown_ok):
            return None
        if doc is None and upsert:
            doc = {"_id": doc_id, **update.get("$setOnInsert", {})}
            self.docs[doc_id] = doc
        if doc is None:
            return None
        doc["counted_joins_today"] = int(doc.get("counted_joins_today", 0)) + int(update["$inc"]["counted_joins_today"])
        doc["last_counted_join_ts"] = update["$set"]["last_counted_join_ts"]
        doc["updated_at"] = update["$set"]["updated_at"]
        return dict(doc)

    def find_one(self, filt, projection=None):
        doc = self.docs.get(filt["_id"])
        return dict(doc) if doc else None


class _FakeDB:
    def __init__(self):
        self.affiliate_referral_cooldown = _FakeCooldownCollection()


class _RaceCooldownCollection(_FakeCooldownCollection):
    def __init__(self):
        super().__init__()
        self._first = True

    def find_one_and_update(self, filt, update, upsert=False, return_document=None):
        if self._first and upsert:
            self._first = False
            doc_id = filt["_id"]
            self.docs[doc_id] = {"_id": doc_id, "counted_joins_today": 0, "last_counted_join_ts": 0}
            raise DuplicateKeyError("duplicate")
        return super().find_one_and_update(filt, update, upsert=upsert, return_document=return_document)


class _RaceDB:
    def __init__(self):
        self.affiliate_referral_cooldown = _RaceCooldownCollection()


class _FakeAggregateCollection:
    def aggregate(self, pipeline):
        return []


class _FakeQualifiedAggregateCollection:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.pipelines = []

    def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        return list(self.rows)


class _FakeLiveCollection:
    def __init__(self, doc=None):
        self.doc = doc
        self.updated = None
        self.update_calls = []

    def find_one(self, filt, projection=None):
        if self.doc and self.doc.get("week_start_utc") == filt.get("week_start_utc"):
            return dict(self.doc)
        return None

    def update_one(self, filt, update, upsert=False):
        payload = dict(update.get("$set", {}))
        self.updated = {"filt": dict(filt), "payload": payload, "upsert": upsert}
        self.update_calls.append(self.updated)
        self.doc = payload


class _FakeWeeklyDb:
    def __init__(self, live_doc=None):
        self.pending_referrals = _FakeAggregateCollection()
        self.referral_flow_events = _FakeAggregateCollection()
        self.referral_events = _FakeAggregateCollection()
        self.qualified_events = _FakeAggregateCollection()
        self.affiliate_weekly_kpis_live = _FakeLiveCollection(live_doc)


class _FakeQualifiedWeeklyDb:
    def __init__(self, *, flow_rows=None, ledger_rows=None):
        self.pending_referrals = _FakeAggregateCollection()
        self.referral_flow_events = _FakeQualifiedAggregateCollection(flow_rows)
        self.referral_events = _FakeQualifiedAggregateCollection(ledger_rows)


def test_should_count_referral_join_cooldown_and_cap():
    db = _FakeDB()
    referrer_id = 42
    now = datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc)

    counted, reason = should_count_referral_join(db, referrer_id, now)
    assert counted is True
    assert reason is None

    counted, reason = should_count_referral_join(db, referrer_id, now + timedelta(seconds=MIN_SECONDS_BETWEEN_COUNTED_JOINS - 1))
    assert counted is False
    assert reason == "cooldown"

    for i in range(MAX_COUNTED_JOINS_PER_DAY - 1):
        counted, reason = should_count_referral_join(
            db,
            referrer_id,
            now + timedelta(seconds=MIN_SECONDS_BETWEEN_COUNTED_JOINS * (i + 1)),
        )
        assert counted is True
        assert reason is None

    counted, reason = should_count_referral_join(
        db,
        referrer_id,
        now + timedelta(seconds=MIN_SECONDS_BETWEEN_COUNTED_JOINS * (MAX_COUNTED_JOINS_PER_DAY + 1)),
    )
    assert counted is False
    assert reason == "daily_cap"


def test_should_count_referral_join_duplicate_key_race_retries():
    db = _RaceDB()
    referrer_id = 99
    now = datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc)

    counted, reason = should_count_referral_join(db, referrer_id, now)

    assert counted is True
    assert reason is None


def test_build_weekly_payload_contains_required_window_fields():
    db = _FakeWeeklyDb()
    ref = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    week_start, payload = _build_affiliate_weekly_payload(db, reference_utc=ref)
    assert payload["week_start_utc"] == week_start
    assert payload["week_start_utc"].tzinfo is not None
    assert payload["week_end_utc"].tzinfo is not None
    assert payload["generated_at"].tzinfo is not None


def test_live_kpi_ttl_returns_cached_without_build(monkeypatch):
    now = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    week_start, _, _ = affiliate_week_window_utc_from_reference(now)
    db = _FakeWeeklyDb(
        live_doc={
            "week_start_utc": week_start,
            "week_end_utc": datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc),
            "generated_at": now,
            "affiliate_leaderboard_week": [],
        }
    )

    import affiliate_leaderboard as mod

    def _fail_build(*args, **kwargs):
        raise AssertionError("builder_should_not_be_called")

    monkeypatch.setattr(mod, "_build_affiliate_weekly_payload", _fail_build)
    out = compute_affiliate_weekly_kpis_live(db, reference_utc=now)
    assert out["week_start_utc"] == week_start


def test_live_kpi_week_mismatch_forces_build_and_current_week_key(monkeypatch):
    now = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    current_week_start, current_week_end, _ = affiliate_week_window_utc_from_reference(now)
    db = _FakeWeeklyDb(
        live_doc={
            "week_start_utc": datetime(2025, 12, 29, 0, 0, 0, tzinfo=timezone.utc),
            "week_end_utc": datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc),
            "generated_at": now,
            "affiliate_leaderboard_week": [],
        }
    )

    import affiliate_leaderboard as mod

    called = {"ok": False}

    def _build(*args, **kwargs):
        called["ok"] = True
        return current_week_start, {
            "week_start_utc": current_week_start,
            "week_end_utc": current_week_end,
            "generated_at": now,
            "affiliate_leaderboard_week": [],
        }

    monkeypatch.setattr(mod, "_build_affiliate_weekly_payload", _build)
    out = compute_affiliate_weekly_kpis_live(db, reference_utc=now)
    assert called["ok"] is True
    assert db.affiliate_weekly_kpis_live.update_calls
    assert db.affiliate_weekly_kpis_live.update_calls[-1]["filt"]["week_start_utc"] == current_week_start
    assert out["week_start_utc"] == current_week_start


def test_live_kpi_string_generated_at_iso_short_circuits(monkeypatch):
    now = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    week_start, _, _ = affiliate_week_window_utc_from_reference(now)
    db = _FakeWeeklyDb(
        live_doc={
            "week_start_utc": week_start,
            "week_end_utc": datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc),
            "generated_at": now.isoformat(),
            "affiliate_leaderboard_week": [],
        }
    )

    import affiliate_leaderboard as mod

    def _fail_build(*args, **kwargs):
        raise AssertionError("builder_should_not_be_called")

    monkeypatch.setattr(mod, "_build_affiliate_weekly_payload", _fail_build)
    out = compute_affiliate_weekly_kpis_live(db, reference_utc=now)
    assert out["week_start_utc"] == week_start


def test_live_kpi_string_generated_at_z_short_circuits(monkeypatch):
    now = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    week_start, _, _ = affiliate_week_window_utc_from_reference(now)
    db = _FakeWeeklyDb(
        live_doc={
            "week_start_utc": week_start,
            "week_end_utc": datetime(2026, 1, 12, 0, 0, 0, tzinfo=timezone.utc),
            "generated_at": now.replace(tzinfo=None).isoformat() + "Z",
            "affiliate_leaderboard_week": [],
        }
    )

    import affiliate_leaderboard as mod

    def _fail_build(*args, **kwargs):
        raise AssertionError("builder_should_not_be_called")

    monkeypatch.setattr(mod, "_build_affiliate_weekly_payload", _fail_build)
    out = compute_affiliate_weekly_kpis_live(db, reference_utc=now)
    assert out["week_start_utc"] == week_start


def test_weekly_payload_qualified_ignores_qualified_events_collection():
    ref = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    db = _FakeQualifiedWeeklyDb(flow_rows=[], ledger_rows=[])

    _, payload = _build_affiliate_weekly_payload(db, reference_utc=ref)

    assert payload["affiliate_weekly_by_referrer"] == {}


def test_weekly_payload_qualified_counts_referral_settled_from_flow_events():
    ref = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    db = _FakeQualifiedWeeklyDb(flow_rows=[{"_id": 123, "qualified_week": 2}], ledger_rows=[])

    _, payload = _build_affiliate_weekly_payload(db, reference_utc=ref)

    assert payload["affiliate_weekly_by_referrer"]["123"]["qualified_week"] == 2


def test_weekly_payload_qualified_falls_back_to_referral_events_when_no_flow_settled():
    ref = datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
    db = _FakeQualifiedWeeklyDb(flow_rows=[], ledger_rows=[{"_id": 456, "qualified_week": 3}])

    _, payload = _build_affiliate_weekly_payload(db, reference_utc=ref)

    assert payload["affiliate_weekly_by_referrer"]["456"]["qualified_week"] == 3


class _FakeSnapshotCollection:
    def __init__(self):
        self.docs = {}

    def create_index(self, *args, **kwargs):
        return None

    def find_one(self, filt, projection=None):
        doc = self.docs.get(filt.get("week_key"))
        return dict(doc) if doc else None

    def update_one(self, filt, update, upsert=False):
        payload = dict(update.get("$set", {}))
        self.docs[filt.get("week_key")] = payload


class _FakeSnapshotDb:
    def __init__(self):
        self.pending_referrals = _FakeAggregateCollection()
        self.referral_flow_events = _FakeAggregateCollection()
        self.referral_events = _FakeAggregateCollection()
        self.affiliate_leaderboard_snapshots = _FakeSnapshotCollection()


def test_week_window_from_week_key_kl_requires_monday():
    assert affiliate_week_window_from_week_key_kl("2026-01-06") is None
    valid = affiliate_week_window_from_week_key_kl("2026-01-05")
    assert valid is not None
    assert valid["week_start_local"] == "2026-01-05"


def test_snapshot_builder_skip_and_force_modes():
    from affiliate_leaderboard import build_affiliate_leaderboard_snapshot

    db = _FakeSnapshotDb()
    week_window = affiliate_week_window_from_week_key_kl("2026-01-05")

    created = build_affiliate_leaderboard_snapshot(db, week_window=week_window, mode="scheduler")
    assert created["status"] == "created"
    skipped = build_affiliate_leaderboard_snapshot(db, week_window=week_window, mode="scheduler")
    assert skipped["status"] == "skipped_exists"
    forced = build_affiliate_leaderboard_snapshot(db, week_window=week_window, mode="admin_manual", force=True)
    assert forced["status"] == "regenerated"


def test_affiliate_week_window_uses_kl_boundary():
    ref = datetime(2026, 1, 4, 16, 5, 0, tzinfo=timezone.utc)
    week_start_utc, week_end_utc, week_start_local = affiliate_week_window_utc_from_reference(ref)
    assert week_start_local.isoformat() == "2026-01-05T00:00:00+08:00"
    assert week_start_utc.isoformat() == "2026-01-04T16:00:00+00:00"
    assert week_end_utc.isoformat() == "2026-01-11T16:00:00+00:00"


def test_previous_completed_week_window_kl_correctness():
    ref = datetime(2026, 1, 6, 2, 0, 0, tzinfo=timezone.utc)
    prev = affiliate_previous_completed_week_window_kl(ref)
    assert prev["week_key"] == "2025-12-29"
    assert prev["week_start_local"] == "2025-12-29"
    assert prev["week_end_local"] == "2026-01-04"


def test_snapshot_builder_empty_week_creates_empty_entries():
    from affiliate_leaderboard import build_affiliate_leaderboard_snapshot

    db = _FakeSnapshotDb()
    week_window = affiliate_week_window_from_week_key_kl("2026-01-05")
    out = build_affiliate_leaderboard_snapshot(db, week_window=week_window, mode="scheduler", top_n=0)
    assert out["status"] == "created"
    doc = db.affiliate_leaderboard_snapshots.find_one({"week_key": "2026-01-05"})
    assert doc["entry_count"] == 0
    assert doc["entries"] == []


def test_snapshot_builder_uses_identity_loader_and_window_type():
    from affiliate_leaderboard import build_affiliate_leaderboard_snapshot

    class _RowsCollection:
        def aggregate(self, pipeline):
            if any(step.get("$group", {}).get("qualified_week") for step in pipeline if isinstance(step, dict)):
                return [{"_id": 99, "qualified_week": 4}]
            if any(step.get("$group", {}).get("joins_week_raw") for step in pipeline if isinstance(step, dict)):
                return [{"_id": 99, "joins_week_raw": 9}]
            if any(step.get("$group", {}).get("joins_week_counted") for step in pipeline if isinstance(step, dict)):
                return [{"_id": 99, "joins_week_counted": 3}]
            return []

    db = _FakeSnapshotDb()
    db.pending_referrals = _RowsCollection()
    db.referral_flow_events = _RowsCollection()
    db.referral_events = _RowsCollection()
    week_window = affiliate_week_window_from_week_key_kl("2026-01-05")

    out = build_affiliate_leaderboard_snapshot(
        db,
        week_window=week_window,
        mode="admin_manual",
        user_identity_loader=lambda ids: {"99": {"username": "alice", "display_name": "alice"}},
    )
    assert out["status"] == "created"
    doc = db.affiliate_leaderboard_snapshots.find_one({"week_key": "2026-01-05"})
    assert doc["window_type"] == "explicit_week_key"
    assert doc["entries"][0]["username"] == "alice"


def _test_mask_username(name: str) -> str:
    v = str(name)
    if len(v) <= 2:
        return "*" * len(v)
    return v[:2] + "***"


def _test_format_username(u, current_user_id, is_admin):
    name = (u.get("username") or u.get("first_name") or "").strip()
    try:
        uid = int(u.get("user_id") or 0)
    except Exception:
        uid = 0
    if is_admin or uid == int(current_user_id or 0):
        return name or None
    return _test_mask_username(name) if name else None


def test_snapshot_entries_non_admin_masks_other_user_identity_and_quality_flag():
    rows = [
        {
            "user_id": 100,
            "username": "alice",
            "display_name": "alice",
            "extra": {"quality_flag": "ok", "qualified_week": 2},
        }
    ]
    out = serialize_affiliate_snapshot_entries_for_viewer(
        rows,
        current_user_id=200,
        is_admin=False,
        format_username_fn=_test_format_username,
        mask_username_fn=_test_mask_username,
    )
    assert out[0]["username"] != "alice"
    assert out[0]["display_name"] != "alice"
    assert "user_id" not in out[0]
    assert "quality_flag" not in out[0]["extra"]


def test_snapshot_entries_non_admin_keeps_own_identity_visible():
    rows = [
        {
            "user_id": 200,
            "username": "selfuser",
            "display_name": "selfuser",
            "extra": {"quality_flag": "ok", "qualified_week": 2},
        }
    ]
    out = serialize_affiliate_snapshot_entries_for_viewer(
        rows,
        current_user_id=200,
        is_admin=False,
        format_username_fn=_test_format_username,
        mask_username_fn=_test_mask_username,
    )
    assert out[0]["display_name"] == "selfuser"
    assert out[0]["user_id"] == 200


def test_snapshot_entries_admin_sees_full_identity_and_quality_flag():
    rows = [
        {
            "user_id": 100,
            "username": "alice",
            "display_name": "alice",
            "extra": {"quality_flag": "ok", "qualified_week": 2},
        }
    ]
    out = serialize_affiliate_snapshot_entries_for_viewer(
        rows,
        current_user_id=999,
        is_admin=True,
        format_username_fn=_test_format_username,
        mask_username_fn=_test_mask_username,
    )
    assert out[0]["username"] == "alice"
    assert out[0]["display_name"] == "alice"
    assert out[0]["user_id"] == 100
    assert out[0]["extra"].get("quality_flag") == "ok"
