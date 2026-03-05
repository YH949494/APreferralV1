from datetime import datetime, timedelta, timezone

from affiliate_leaderboard import (
    MAX_COUNTED_JOINS_PER_DAY,
    MIN_SECONDS_BETWEEN_COUNTED_JOINS,
    compute_affiliate_weekly_kpis_live,
    _build_affiliate_weekly_payload,
    week_window_utc,
    should_count_referral_join,
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


class _FakeAggregateCollection:
    def aggregate(self, pipeline):
        return []


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
        self.qualified_events = _FakeAggregateCollection()
        self.affiliate_weekly_kpis_live = _FakeLiveCollection(live_doc)


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
    week_start = datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
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
    current_week_start, current_week_end = week_window_utc(now)
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
    week_start = datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
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
    week_start = datetime(2026, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
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
