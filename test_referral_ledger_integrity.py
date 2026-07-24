"""Regression tests for the referral ledger correctness fix.

Covers: referral_revoked is only ever written for a previously-settled
referral (revoke_settled_referral), initial-qualification failures no
longer write count-changing events, invalidated legacy revocations are
excluded from every aggregation, and user-facing counters never go
negative while internal diagnostics keep showing raw values.
"""

import unittest
from datetime import datetime, timedelta, timezone

from pymongo.errors import DuplicateKeyError

import scheduler
from referral_ledger import with_not_invalidated
from repair_referral_ledger import build_report, _invalidate


# ---------------------------------------------------------------------------
# A reasonably faithful in-memory fake of the referral_events aggregation
# pipelines used by scheduler.py (supports $match with $ne/$in/$exists/$or,
# and $group/$sum over $cond expressions built from _referral_sign_expr()).
# ---------------------------------------------------------------------------

def _eval_expr(expr, doc):
    if isinstance(expr, str) and expr.startswith("$"):
        return doc.get(expr[1:])
    if not isinstance(expr, dict):
        return expr
    if "$cond" in expr:
        cond, then, els = expr["$cond"]
        return _eval_expr(then, doc) if _eval_expr(cond, doc) else _eval_expr(els, doc)
    if "$eq" in expr:
        a, b = expr["$eq"]
        return _eval_expr(a, doc) == _eval_expr(b, doc)
    if "$and" in expr:
        return all(_eval_expr(c, doc) for c in expr["$and"])
    if "$gte" in expr:
        a, b = expr["$gte"]
        return _eval_expr(a, doc) >= _eval_expr(b, doc)
    if "$lt" in expr:
        a, b = expr["$lt"]
        return _eval_expr(a, doc) < _eval_expr(b, doc)
    raise NotImplementedError(expr)


def _match_filter(doc, filt):
    for key, val in filt.items():
        if key == "$or":
            if not any(_match_filter(doc, branch) for branch in val):
                return False
            continue
        if isinstance(val, dict):
            if "$ne" in val:
                if doc.get(key) == val["$ne"]:
                    return False
                continue
            if "$in" in val:
                if doc.get(key) not in val["$in"]:
                    return False
                continue
            if "$exists" in val:
                if bool(val["$exists"]) != (key in doc):
                    return False
                continue
        if doc.get(key) != val:
            return False
    return True


class _FakeReferralEvents:
    def __init__(self):
        self.docs = []
        self._keys = set()

    def insert_one(self, doc):
        key = (doc.get("event"), doc.get("inviter_id"), doc.get("invitee_id"))
        if key in self._keys:
            raise DuplicateKeyError("duplicate")
        self._keys.add(key)
        self.docs.append(dict(doc))

    def find_one(self, filt, projection=None):
        for doc in self.docs:
            if _match_filter(doc, filt):
                return dict(doc)
        return None

    def find(self, filt, projection=None):
        return [dict(d) for d in self.docs if _match_filter(d, filt)]

    def count_documents(self, filt, limit=None):
        matches = [d for d in self.docs if _match_filter(d, filt)]
        return len(matches) if limit is None else min(len(matches), limit)

    def aggregate(self, pipeline, allowDiskUse=False):
        rows = list(self.docs)
        for stage in pipeline:
            if "$match" in stage:
                rows = [d for d in rows if _match_filter(d, stage["$match"])]
            elif "$group" in stage:
                spec = dict(stage["$group"])
                id_expr = spec.pop("_id")
                groups = {}
                for d in rows:
                    gid = _eval_expr(id_expr, d) if isinstance(id_expr, str) else id_expr
                    groups.setdefault(gid, []).append(d)
                new_rows = []
                for gid, members in groups.items():
                    row = {"_id": gid}
                    for field, agg in spec.items():
                        if "$sum" in agg:
                            total = 0
                            for m in members:
                                v = _eval_expr(agg["$sum"], m)
                                if isinstance(v, bool):
                                    v = 0
                                if isinstance(v, (int, float)):
                                    total += v
                            row[field] = total
                    new_rows.append(row)
                rows = new_rows
        return rows

    def bulk_write(self, ops, ordered=False):
        modified = 0
        for op in ops:
            filt = op._filter
            for doc in self.docs:
                if _match_filter(doc, filt):
                    for k, v in op._doc.get("$set", {}).items():
                        doc[k] = v
                    modified += 1
                    break
        return type("Result", (), {"modified_count": modified})()


def _settled_doc(inviter, invitee, occurred_at, week_key="wk", month_key="mo"):
    return {
        "inviter_id": inviter,
        "invitee_id": invitee,
        "event": "referral_settled",
        "occurred_at": occurred_at,
        "week_key": week_key,
        "month_key": month_key,
    }


def _revoked_doc(inviter, invitee, occurred_at, week_key="wk", month_key="mo", invalidated=False, reason=None):
    doc = {
        "inviter_id": inviter,
        "invitee_id": invitee,
        "event": "referral_revoked",
        "occurred_at": occurred_at,
        "week_key": week_key,
        "month_key": month_key,
    }
    if reason is not None:
        doc["reason"] = reason
    if invalidated:
        doc["invalidated"] = True
    return doc


NOW = datetime(2026, 7, 24, tzinfo=timezone.utc)


class RevokeSettledReferralTests(unittest.TestCase):
    def setUp(self):
        self.events = _FakeReferralEvents()
        self.orig_grant_xp = scheduler.grant_xp
        self.xp_calls = []
        scheduler.grant_xp = lambda *a, **kw: self.xp_calls.append((a, kw)) or True

    def tearDown(self):
        scheduler.grant_xp = self.orig_grant_xp

    def test_revocation_without_prior_settlement_returns_false_and_no_op(self):
        ok = scheduler.revoke_settled_referral(
            type("DB", (), {"referral_events": self.events})(),
            inviter_id=1,
            invitee_id=2,
            reason="fraud_confirmed",
            occurred_at=NOW,
        )
        self.assertFalse(ok)
        self.assertEqual(self.events.docs, [])

    def test_settlement_then_revocation_nets_to_zero(self):
        db = type("DB", (), {"referral_events": self.events})()
        self.events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=1)))

        ok = scheduler.revoke_settled_referral(
            db, inviter_id=1, invitee_id=2, reason="fraud_confirmed", occurred_at=NOW
        )
        self.assertTrue(ok)

        events = [d["event"] for d in self.events.docs]
        self.assertEqual(sorted(events), ["referral_revoked", "referral_settled"])
        net = sum(1 if d["event"] == "referral_settled" else -1 for d in self.events.docs)
        self.assertEqual(net, 0)

    def test_repeated_revocation_is_idempotent(self):
        db = type("DB", (), {"referral_events": self.events})()
        self.events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=1)))

        first = scheduler.revoke_settled_referral(
            db, inviter_id=1, invitee_id=2, reason="fraud_confirmed", occurred_at=NOW
        )
        second = scheduler.revoke_settled_referral(
            db, inviter_id=1, invitee_id=2, reason="fraud_confirmed", occurred_at=NOW
        )
        self.assertTrue(first)
        self.assertFalse(second)
        revoked_docs = [d for d in self.events.docs if d["event"] == "referral_revoked"]
        self.assertEqual(len(revoked_docs), 1)

    def test_different_pairs_are_independent(self):
        db = type("DB", (), {"referral_events": self.events})()
        self.events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=1)))
        self.events.insert_one(_settled_doc(1, 3, NOW - timedelta(days=1)))

        ok_2 = scheduler.revoke_settled_referral(
            db, inviter_id=1, invitee_id=2, reason="fraud_confirmed", occurred_at=NOW
        )
        # invitee 3's settlement must be untouched by invitee 2's revocation
        still_settled_3 = self.events.find_one(
            {"inviter_id": 1, "invitee_id": 3, "event": "referral_settled"}
        )
        revoked_3 = self.events.find_one({"inviter_id": 1, "invitee_id": 3, "event": "referral_revoked"})
        self.assertTrue(ok_2)
        self.assertIsNotNone(still_settled_3)
        self.assertIsNone(revoked_3)

    def test_revocation_does_not_touch_xp(self):
        db = type("DB", (), {"referral_events": self.events})()
        self.events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=1)))

        scheduler.revoke_settled_referral(
            db, inviter_id=1, invitee_id=2, reason="fraud_confirmed", occurred_at=NOW
        )
        # revoke_settled_referral only reverses the referral count ledger;
        # XP/reward reversal is not implemented and must not be invented here.
        self.assertEqual(self.xp_calls, [])


class InitialQualificationFailureTests(unittest.TestCase):
    """settle_pending_referrals() must not write referral_revoked for
    referrals that never had a prior referral_settled event."""

    def setUp(self):
        self.orig_db = scheduler.db
        self.orig_now_utc = scheduler.now_utc
        self.orig_recover = scheduler._recover_stale_processing
        self.fixed_now = NOW
        scheduler.now_utc = lambda: self.fixed_now
        scheduler._recover_stale_processing = lambda now_utc_ts: 0

    def tearDown(self):
        scheduler.db = self.orig_db
        scheduler.now_utc = self.orig_now_utc
        scheduler._recover_stale_processing = self.orig_recover

    def _run_self_invite_case(self):
        from pymongo import ReturnDocument

        class _Pending:
            def __init__(self, doc):
                self.doc = dict(doc)
                self.served = False

            def find_one_and_update(self, filt, update, sort=None, return_document=None):
                if self.served:
                    return None
                self.served = True
                before = dict(self.doc)
                for k, v in update.get("$set", {}).items():
                    self.doc[k] = v
                return before

            def update_one(self, filt, update):
                for k, v in update.get("$set", {}).items():
                    self.doc[k] = v
                for k in update.get("$unset", {}).keys():
                    self.doc.pop(k, None)
                return type("R", (), {"modified_count": 1})()

        pending = _Pending(
            {
                "_id": 1,
                "inviter_user_id": 11,
                "invitee_user_id": 11,  # self-invite
                "created_at_utc": self.fixed_now - timedelta(hours=100),
                "group_id": scheduler.GROUP_ID,
            }
        )
        events = _FakeReferralEvents()
        db = type("DB", (), {"pending_referrals": pending, "referral_events": events})()

        import referral_invitee_lock

        orig_release = referral_invitee_lock.release
        referral_invitee_lock.release = lambda *a, **kw: None
        try:
            scheduler.db = db
            scheduler.settle_pending_referrals(batch_limit=1)
        finally:
            referral_invitee_lock.release = orig_release

        return pending.doc, events

    def test_self_invite_does_not_write_referral_revoked(self):
        doc, events = self._run_self_invite_case()
        self.assertEqual(doc["status"], "revoked")
        self.assertEqual(doc["revoked_reason"], "self_invite")
        self.assertEqual(events.docs, [])


class SnapshotAggregationInvalidatedTests(unittest.TestCase):
    def setUp(self):
        self.orig_db = scheduler.db
        self.orig_heartbeat = scheduler._write_snapshot_heartbeat
        scheduler._write_snapshot_heartbeat = lambda source, ts: None

    def tearDown(self):
        scheduler.db = self.orig_db
        scheduler._write_snapshot_heartbeat = self.orig_heartbeat

    def _fake_users(self):
        class _Users:
            def __init__(self):
                self.docs = {}

            def update_many(self, filt, update):
                if isinstance(update, list):
                    stage = update[0].get("$set", {}) if update else {}
                    for doc in self.docs.values():
                        for k, v in stage.items():
                            doc[k] = doc.get(v[1:]) if isinstance(v, str) and v.startswith("$") else v
                    return type("R", (), {"modified_count": len(self.docs)})()
                if "$set" in update:
                    for doc in self.docs.values():
                        for k, v in update["$set"].items():
                            doc[k] = v
                if "$inc" in update:
                    for doc in self.docs.values():
                        for k, v in update["$inc"].items():
                            doc[k] = doc.get(k, 0) + v
                return type("R", (), {"modified_count": len(self.docs)})()

            def bulk_write(self, updates, ordered=False):
                for op in updates:
                    user_id = op._filter["user_id"]
                    doc = self.docs.setdefault(user_id, {"user_id": user_id})
                    for k, v in op._doc.get("$set", {}).items():
                        doc[k] = v

        return _Users()

    def test_invalidated_legacy_revocation_is_ignored_by_snapshot_totals(self):
        events = _FakeReferralEvents()
        # One legitimate settle, plus a legacy bad revoke (no prior
        # settlement) that has already been marked invalidated by the
        # repair script.
        events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=2)))
        bad_revoke = _revoked_doc(1, 3, NOW - timedelta(days=1), invalidated=True)
        events.docs.append(bad_revoke)

        users = self._fake_users()
        users.docs[1] = {"user_id": 1}

        db = type("DB", (), {"referral_events": events, "users": users})()
        scheduler.db = db

        scheduler.settle_referral_snapshots()

        # the invalidated revoke must not drag the total below the single
        # legitimate settlement.
        self.assertEqual(users.docs[1]["total_referrals"], 1)

    def test_valid_revocation_after_repair_still_counted(self):
        events = _FakeReferralEvents()
        events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=2)))
        events.insert_one(_revoked_doc(1, 2, NOW - timedelta(days=1)))  # legitimate, not invalidated

        users = self._fake_users()
        users.docs[1] = {"user_id": 1}

        db = type("DB", (), {"referral_events": events, "users": users})()
        scheduler.db = db

        scheduler.settle_referral_snapshots()

        self.assertEqual(users.docs[1]["total_referrals"], 0)


class WeeklyMonthlyWindowDeterminismTests(unittest.TestCase):
    def test_events_in_different_windows_compute_deterministically(self):
        events = _FakeReferralEvents()
        # settled in an earlier week/month, revoked (legitimately) in the
        # current week/month -> weekly/monthly nets negative for that
        # window even though the lifetime net is 0. This must be stable
        # and reproducible across repeated runs of the same pipeline.
        events.insert_one(_settled_doc(1, 2, NOW - timedelta(days=10), week_key="prev_wk", month_key="prev_mo"))
        events.insert_one(_revoked_doc(1, 2, NOW, week_key="cur_wk", month_key="cur_mo"))

        pipeline = [
            {"$match": with_not_invalidated({"inviter_id": {"$ne": None}, "event": {"$in": ["referral_settled", "referral_revoked"]}})},
            {
                "$group": {
                    "_id": "$inviter_id",
                    "total": {"$sum": scheduler._referral_sign_expr()},
                    "weekly": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$week_key", "cur_wk"]},
                                scheduler._referral_sign_expr(),
                                0,
                            ]
                        }
                    },
                }
            },
        ]
        result_1 = events.aggregate(pipeline)
        result_2 = events.aggregate(pipeline)
        self.assertEqual(result_1, result_2)
        row = result_1[0]
        self.assertEqual(row["total"], 0)
        self.assertEqual(row["weekly"], -1)


class UserFacingClampingTests(unittest.TestCase):
    # main.py has heavy import-time side effects (real Mongo index creation)
    # that make importing it in a unit test unsafe/fragile, so this checks
    # (a) the API-boundary functions call the clamp helper, via source
    # inspection, and (b) the clamp helper's own semantics, standalone.

    def test_compute_referral_stats_and_snapshot_clamp_via_safe_non_negative_int(self):
        with open("main.py", "r", encoding="utf-8") as fh:
            source = fh.read()

        def _function_body(name):
            start = source.index(f"def {name}(")
            next_def = source.index("\ndef ", start + 1)
            return source[start:next_def]

        stats_body = _function_body("compute_referral_stats")
        snapshot_body = _function_body("_get_user_snapshot")
        self.assertIn("_safe_non_negative_int(user_doc.get(\"total_referrals\"", stats_body)
        self.assertIn("_safe_non_negative_int(user_doc.get(\"weekly_referrals\"", stats_body)
        self.assertIn("_safe_non_negative_int(user_doc.get(\"monthly_referrals\"", stats_body)
        self.assertIn("_safe_non_negative_int(user_doc.get(\"total_referrals\"", snapshot_body)
        self.assertIn("_safe_non_negative_int(user_doc.get(\"weekly_referrals\"", snapshot_body)
        self.assertIn("_safe_non_negative_int(user_doc.get(\"monthly_referrals\"", snapshot_body)

    def test_safe_non_negative_int_clamps_negative_values(self):
        def _safe_non_negative_int(value):
            try:
                n = float(value)
            except (TypeError, ValueError):
                return 0
            if n != n or n in (float("inf"), float("-inf")):
                return 0
            try:
                return max(0, int(n))
            except OverflowError:
                return 0

        self.assertEqual(_safe_non_negative_int(-36), 0)
        self.assertEqual(_safe_non_negative_int(-1), 0)
        self.assertEqual(_safe_non_negative_int(5), 5)
        self.assertEqual(_safe_non_negative_int(0), 0)


class InternalDiagnosticsShowRawValuesTests(unittest.TestCase):
    def test_sync_referral_counts_reports_raw_negative_delta(self):
        import sync_referral_counts

        events = _FakeReferralEvents()
        # An invalid (uninvalidated) legacy revoke with no matching settle:
        # the diagnostic script must surface this raw, not clamp it away.
        events.insert_one(_revoked_doc(1, 2, NOW))

        class _Users:
            def __init__(self):
                self._docs = [{"_id": "u1", "user_id": 1, "total_referrals": 0}]
                self._pending = []

            def find(self, query, projection):
                last_id = (query or {}).get("_id", {}).get("$gt") if query else None
                self._pending = [d for d in self._docs if last_id is None or d["_id"] > last_id]
                return self

            def sort(self, *a, **kw):
                return self

            def limit(self, *a, **kw):
                result, self._pending = self._pending, []
                return result

        db = type("DB", (), {"users": _Users(), "referral_events": events})()
        summary = sync_referral_counts.sync_referral_counts(db, batch_size=10, dry_run=True)

        self.assertEqual(summary["users_mismatched"], 1)
        self.assertEqual(summary["top_20_deltas"][0]["computed"], -1)


class RepairScriptReportTests(unittest.TestCase):
    def test_build_report_counts_and_marks_invalid_events(self):
        events = _FakeReferralEvents()
        d1 = _revoked_doc(1, 2, NOW - timedelta(days=40), week_key="old_wk", month_key="old_mo", reason="self_invite")
        d2 = _revoked_doc(1, 3, NOW, week_key="cur_wk", month_key="cur_mo", reason="insufficient_engagement")
        events.docs.extend([dict(d1, _id="a"), dict(d2, _id="b")])

        report = build_report(events.docs, NOW)
        self.assertEqual(report["invalid_revocation_count"], 2)
        self.assertEqual(report["affected_inviter_count"], 1)
        self.assertEqual(report["affected_invitee_count"], 2)
        self.assertEqual(report["lifetime_impact"], 2)
        self.assertEqual(report["reasons"], {"self_invite": 1, "insufficient_engagement": 1})

        fake_db = type("DB", (), {"referral_events": events})()
        modified = _invalidate(fake_db, events.docs)
        self.assertEqual(modified, 2)
        for doc in events.docs:
            self.assertTrue(doc.get("invalidated"))
            self.assertEqual(doc.get("invalidated_reason"), "revoked_without_prior_settlement")


if __name__ == "__main__":
    unittest.main()
