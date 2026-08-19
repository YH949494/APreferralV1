import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pymongo.errors import DuplicateKeyError, OperationFailure

from affiliate_rewards import (
    affiliate_bundle_visible_cards,
    ensure_affiliate_indexes,
    approve_affiliate_ledger,
    evaluate_monthly_affiliate_reward,
    issue_current_week_affiliate_rewards,
    issue_previous_week_affiliate_rewards,
    issue_current_month_affiliate_rewards,
    issue_welcome_bonus_if_eligible,
    mark_invitee_qualified,
    retry_current_month_pending_manual_ledgers,
    settle_previous_month_affiliate_rewards,
)


class _UpdateResult:
    def __init__(self, modified_count):
        self.modified_count = modified_count


class FakeCollection:
    def __init__(self, unique_fields=None):
        self.docs = []
        self._id = 1
        self.unique_fields = unique_fields or []

    def create_index(self, *args, **kwargs):
        return None

    def list_indexes(self):
        return []

    def _match_value(self, value, cond):
        if isinstance(cond, dict):
            for op, expected in cond.items():
                if op == "$gte" and not (value is not None and value >= expected):
                    return False
                if op == "$lt" and not (value is not None and value < expected):
                    return False
                if op == "$in" and value not in expected:
                    return False
                if op == "$nin" and value in expected:
                    return False
                if op == "$ne" and value == expected:
                    return False
                if op == "$exists":
                    exists = value is not None
                    if bool(expected) != exists:
                        return False
            return True
        return value == cond

    def _match(self, doc, filt):
        for k, v in (filt or {}).items():
            if k == "$or":
                if not any(self._match(doc, sub) for sub in v):
                    return False
                continue
            if not self._match_value(doc.get(k), v):
                return False
        return True

    def insert_one(self, doc):
        for fields in self.unique_fields:
            for existing in self.docs:
                if all(existing.get(f) == doc.get(f) for f in fields):
                    raise DuplicateKeyError("duplicate")
        row = dict(doc)
        row.setdefault("_id", self._id)
        self._id += 1
        self.docs.append(row)
        return row

    def find_one(self, filt=None, proj=None):
        for d in self.docs:
            if self._match(d, filt or {}):
                if not proj:
                    return dict(d)
                return {k: d.get(k) for k in proj.keys()}
        return None

    def update_one(self, filt, update, upsert=False):
        for d in self.docs:
            if self._match(d, filt):
                for k, v in update.get("$set", {}).items():
                    d[k] = v
                for k, v in update.get("$inc", {}).items():
                    d[k] = d.get(k, 0) + v
                for k, v in update.get("$addToSet", {}).items():
                    current = d.get(k) or []
                    if v not in current:
                        d[k] = list(current) + [v]
                for k, cond in update.get("$pull", {}).items():
                    current = d.get(k) or []
                    if isinstance(cond, dict) and "$in" in cond:
                        removed = set(cond["$in"])
                        d[k] = [item for item in current if item not in removed]
                    else:
                        d[k] = [item for item in current if item != cond]
                for k in update.get("$unset", {}):
                    d.pop(k, None)
                return _UpdateResult(1)
        if upsert:
            row = dict(filt)
            for k, v in update.get("$setOnInsert", {}).items():
                row.setdefault(k, v)
            for k, v in update.get("$set", {}).items():
                row[k] = v
            self.insert_one(row)
            return _UpdateResult(1)
        return _UpdateResult(0)

    def find_one_and_update(self, filt, update, sort=None, return_document=None):
        matches = [d for d in self.docs if self._match(d, filt)]
        if not matches:
            return None
        if sort:
            key, direction = sort[0]
            matches.sort(key=lambda x: x.get(key, 0), reverse=(direction < 0))
        d = matches[0]
        for k, v in update.get("$set", {}).items():
            d[k] = v
        return dict(d)

    def count_documents(self, filt):
        return sum(1 for d in self.docs if self._match(d, filt))

    def aggregate(self, pipeline):
        rows = list(self.docs)
        for stage in pipeline:
            if "$match" in stage:
                rows = [r for r in rows if self._match(r, stage["$match"])]
            elif "$group" in stage:
                key_expr = stage["$group"]["_id"]
                key_field = key_expr.lstrip("$") if isinstance(key_expr, str) else None
                grouped = {}
                for r in rows:
                    key = r.get(key_field) if key_field else None
                    bucket = grouped.setdefault(key, {"_id": key})
                    for out_key, out_expr in stage["$group"].items():
                        if out_key == "_id":
                            continue
                        if isinstance(out_expr, dict) and "$sum" in out_expr:
                            inc = out_expr["$sum"]
                            if isinstance(inc, int):
                                bucket[out_key] = int(bucket.get(out_key, 0)) + int(inc)
                            elif isinstance(inc, str) and inc.startswith("$"):
                                bucket[out_key] = int(bucket.get(out_key, 0)) + int(r.get(inc.lstrip("$"), 0) or 0)
                    if not any(isinstance(expr, dict) and "$sum" in expr for expr in stage["$group"].values() if expr != key_expr):
                        bucket.setdefault("invitees", set()).add(r.get("invitee_id"))
                normalized = []
                for b in grouped.values():
                    out_row = dict(b)
                    if isinstance(out_row.get("invitees"), set):
                        out_row["invitees"] = list(out_row["invitees"])
                    normalized.append(out_row)
                rows = normalized
            elif "$project" in stage:
                rows = [{"count": len(r.get("invitees") or [])} for r in rows]
            elif "$sort" in stage:
                sort_items = list(stage["$sort"].items())
                for field, direction in reversed(sort_items):
                    rows.sort(key=lambda x: x.get(field), reverse=(int(direction) < 0))
            elif "$limit" in stage:
                rows = rows[: int(stage["$limit"])]
        return rows

    def find(self, filt=None, proj=None):
        for d in self.docs:
            if self._match(d, filt or {}):
                if proj:
                    yield {k: d.get(k) for k in proj.keys()}
                else:
                    yield dict(d)


class FakeDb:
    def __init__(self):
        self.users = FakeCollection()
        self.voucher_pools = FakeCollection(unique_fields=[("pool_id", "code")])
        self.affiliate_ledger = FakeCollection(unique_fields=[("dedup_key",)])
        self.affiliate_voucher_batches = FakeCollection()
        self.qualified_events = FakeCollection(unique_fields=[("invitee_id",)])
        self.user_last_seen = FakeCollection(unique_fields=[("user_id",)])
        self.affiliate_group_invites = FakeCollection(unique_fields=[("user_id", "week_key")])
        self.referral_audit = FakeCollection()
        self.referral_flow_events = FakeCollection()


BUNDLE_COUNTS = {"T1": 2, "T2": 3, "T3": 5, "T4": 3, "T5": 5}


class AffiliateRewardTests(unittest.TestCase):
    def add_pool_bundle(self, db, tier, prefix):
        for idx in range(1, BUNDLE_COUNTS[tier] + 1):
            db.voucher_pools.insert_one({"pool_id": tier, "code": f"{prefix}-{idx}", "status": "available"})

    def assert_bundle(self, row, tier):
        self.assertEqual(row["status"], "ISSUED")
        self.assertEqual(row["reward_type"], "affiliate_bundle")
        self.assertEqual(row["affiliate_tier"], tier)
        self.assertEqual(row["voucher_count"], BUNDLE_COUNTS[tier])
        self.assertEqual(len(row.get("vouchers") or []), BUNDLE_COUNTS[tier])

    def test_ensure_affiliate_indexes_duplicate_key_operation_failure_is_non_fatal(self):
        db = FakeDb()
        original = db.affiliate_ledger.create_index
        call_count = {"n": 0}

        def _create_index(*args, **kwargs):
            call_count["n"] += 1
            if kwargs.get("name") == "uniq_affiliate_monthly_user_month_tier":
                raise OperationFailure("duplicate key", code=11000)
            return original(*args, **kwargs)

        db.affiliate_ledger.create_index = _create_index
        ensure_affiliate_indexes(db)
        self.assertGreater(call_count["n"], 0)

    def test_ensure_affiliate_indexes_non_duplicate_operation_failure_raises(self):
        db = FakeDb()
        original = db.affiliate_ledger.create_index

        def _create_index(*args, **kwargs):
            if kwargs.get("name") == "uniq_affiliate_monthly_user_month_tier":
                raise OperationFailure("other failure", code=12345)
            return original(*args, **kwargs)

        db.affiliate_ledger.create_index = _create_index
        with self.assertRaises(OperationFailure):
            ensure_affiliate_indexes(db)

    def test_welcome_once_with_dedup(self):
        db = FakeDb()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "W1", "status": "available"})
        with patch("affiliate_rewards._is_official_channel_subscribed", return_value=True):
            out1 = issue_welcome_bonus_if_eligible(db, user_id=10, is_new_user=True)
            out2 = issue_welcome_bonus_if_eligible(db, user_id=10, is_new_user=True)
        self.assertEqual(out1["status"], "ISSUED")
        self.assertEqual(out2["status"], "ISSUED")
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "WELCOME:10"}), 1)

    def test_welcome_reconcile_reuses_existing_issued_pool_voucher(self):
        db = FakeDb()
        now = datetime.now(timezone.utc)
        ledger = {
            "dedup_key": "WELCOME:55",
            "user_id": 55,
            "ledger_type": "AFFILIATE_WELCOME",
            "status": "SETTLING",
            "voucher_code": None,
            "created_at": now,
            "updated_at": now,
        }
        db.affiliate_ledger.insert_one(ledger)
        ledger_doc = db.affiliate_ledger.find_one({"dedup_key": "WELCOME:55"})
        db.voucher_pools.insert_one(
            {
                "pool_id": "WELCOME",
                "code": "RECOVERED-W",
                "status": "issued",
                "issued_for_ledger_id": str(ledger_doc["_id"]),
                "ledger_id": ledger_doc["_id"],
            }
        )
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "NEXT-W", "status": "available"})

        with patch("affiliate_rewards._is_official_channel_subscribed", return_value=True):
            out = issue_welcome_bonus_if_eligible(db, user_id=55, is_new_user=True, now_utc=now)

        self.assertEqual(out["status"], "ISSUED")
        self.assertEqual(out["voucher_code"], "RECOVERED-W")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "WELCOME", "status": "issued"}), 1)
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "WELCOME", "status": "available"}), 1)

    def test_welcome_blocked_for_self_invite(self):
        db = FakeDb()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "SW1", "status": "available"})
        db.referral_audit.insert_one(
            {
                "invitee_user_id": 77,
                "inviter_user_id": 77,
                "status": "skipped",
                "reason": "self_invite",
            }
        )
        with patch("affiliate_rewards._is_official_channel_subscribed", return_value=True):
            out = issue_welcome_bonus_if_eligible(db, user_id=77, is_new_user=True)
        self.assertEqual(out["status"], "BLOCKED_SELF_INVITE")
        self.assertFalse(out["created"])
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "WELCOME:77"}), 0)

    def test_welcome_allowed_for_valid_referred_invitee(self):
        db = FakeDb()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "SW2", "status": "available"})
        db.referral_audit.insert_one(
            {
                "invitee_user_id": 88,
                "inviter_user_id": 5,
                "status": "confirmed",
                "reason": "qualified",
            }
        )
        with patch("affiliate_rewards._is_official_channel_subscribed", return_value=True):
            out = issue_welcome_bonus_if_eligible(db, user_id=88, is_new_user=True)
        self.assertEqual(out["status"], "ISSUED")
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "WELCOME:88"}), 1)

    def test_welcome_allowed_for_organic_new_user(self):
        db = FakeDb()
        db.voucher_pools.insert_one({"pool_id": "WELCOME", "code": "SW3", "status": "available"})
        with patch("affiliate_rewards._is_official_channel_subscribed", return_value=True):
            out = issue_welcome_bonus_if_eligible(db, user_id=99, is_new_user=True)
        self.assertEqual(out["status"], "ISSUED")
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "WELCOME:99"}), 1)

    def test_qualified_event_once_lifetime(self):
        db = FakeDb()
        first = mark_invitee_qualified(db, invitee_id=20, referrer_id=3)
        second = mark_invitee_qualified(db, invitee_id=20, referrer_id=3)
        self.assertTrue(first)
        self.assertFalse(second)


    def test_monthly_reward_ignores_non_qualified_join_data(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 88, "blocked": False})
        now = datetime(2026, 1, 12, tzinfo=timezone.utc)

        # Non-qualified join-like record should not affect reward counting.
        db.referral_audit.insert_one({"inviter_user_id": 88, "invitee_user_id": 501, "reason": "join"})

        row = evaluate_monthly_affiliate_reward(db, referrer_id=88, now_utc=now)
        self.assertIsNone(row)

    def test_monthly_tier_and_dedup_key(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 8, "blocked": False})
        now = datetime(2026, 1, 10, tzinfo=timezone.utc)
        for i in range(1, 26):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 8, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "A1")
        self.add_pool_bundle(db, "T2", "A")
        row = evaluate_monthly_affiliate_reward(db, referrer_id=8, now_utc=now)
        self.assertEqual(row["tier"], "T2")
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "AFF:8:202601:T1"}), 1)
        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "AFF:8:202601:T2"}), 1)

    def test_thresholds_issue_expected_tiers_once_up_to_t5(self):
        now = datetime(2026, 1, 10, tzinfo=timezone.utc)
        cases = [
            (10, ("T1",)),
            (25, ("T1", "T2")),
            (50, ("T1", "T2", "T3")),
            (150, ("T1", "T2", "T3", "T4")),
            (300, ("T1", "T2", "T3", "T4", "T5")),
        ]
        for idx, (qualified_total, expected_tiers) in enumerate(cases, start=1):
            db = FakeDb()
            user_id = 500 + idx
            db.users.insert_one({"user_id": user_id, "blocked": False})
            invitee_seed = idx * 10000
            for offset in range(qualified_total):
                db.qualified_events.insert_one(
                    {"invitee_id": invitee_seed + offset, "referrer_id": user_id, "qualified_at": now}
                )
            for tier in ("T1", "T2", "T3", "T4", "T5"):
                self.add_pool_bundle(db, tier, f"{tier}-{idx}")
            evaluate_monthly_affiliate_reward(db, referrer_id=user_id, now_utc=now)
            evaluate_monthly_affiliate_reward(db, referrer_id=user_id, now_utc=now)
            with self.subTest(qualified_total=qualified_total):
                for tier in expected_tiers:
                    dedup = f"AFF:{user_id}:202601:{tier}"
                    row = db.affiliate_ledger.find_one({"dedup_key": dedup})
                    self.assertIsNotNone(row)
                    self.assert_bundle(row, tier)
                    self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": dedup}), 1)
                for tier in ("T1", "T2", "T3", "T4", "T5"):
                    if tier not in expected_tiers:
                        self.assertIsNone(db.affiliate_ledger.find_one({"dedup_key": f"AFF:{user_id}:202601:{tier}"}))

    def test_affiliate_bundle_visible_card_payload(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 808, "blocked": False})
        now = datetime(2026, 1, 10, tzinfo=timezone.utc)
        for i in range(10):
            db.qualified_events.insert_one({"invitee_id": 80800 + i, "referrer_id": 808, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "VISIBLE-T1")

        evaluate_monthly_affiliate_reward(db, referrer_id=808, now_utc=now)
        cards = affiliate_bundle_visible_cards(db, user_id=808)

        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card["reward_type"], "affiliate_bundle")
        self.assertEqual(card["affiliate_tier"], "T1")
        self.assertEqual(card["voucher_count"], 2)
        self.assertEqual(card["total_value"], 10)
        self.assertEqual([v["code"] for v in card["vouchers"]], ["VISIBLE-T1-1", "VISIBLE-T1-2"])

    def test_t2_risk_auto_issue_and_t1_bypass(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 9, "blocked": False})
        now = datetime(2026, 1, 12, tzinfo=timezone.utc)
        for i in range(1, 26):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 9, "qualified_at": now, "ip": "1.1.1.1"})
        self.add_pool_bundle(db, "T1", "T1-A")
        self.add_pool_bundle(db, "T2", "T2-A")
        row = evaluate_monthly_affiliate_reward(db, referrer_id=9, now_utc=now)
        self.assertEqual(row["status"], "ISSUED")
        statuses = {d.get("status") for d in db.affiliate_ledger.docs}
        self.assertNotIn("PENDING_REVIEW", statuses)
        self.assertNotIn("PENDING_MANUAL", statuses)

        db2 = FakeDb()
        db2.users.insert_one({"user_id": 5, "blocked": False})
        for i in range(1, 11):
            db2.qualified_events.insert_one({"invitee_id": i, "referrer_id": 5, "qualified_at": now, "ip": "1.1.1.1"})
        self.add_pool_bundle(db2, "T1", "TT1")
        row2 = evaluate_monthly_affiliate_reward(db2, referrer_id=5, now_utc=now)
        self.assertEqual(row2["status"], "ISSUED")

    def test_out_of_stock_and_atomic_claim(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 7, "blocked": False})
        now = datetime.now(timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 7, "qualified_at": now})
        row = evaluate_monthly_affiliate_reward(db, referrer_id=7, now_utc=now)
        self.assertEqual(row["status"], "PENDING_MANUAL")

        self.add_pool_bundle(db, "T1", "ONLY1")
        db.users.insert_one({"user_id": 11, "blocked": False})
        for i in range(101, 111):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 11, "qualified_at": now})
        row_a = evaluate_monthly_affiliate_reward(db, referrer_id=11, now_utc=now)
        db.users.insert_one({"user_id": 12, "blocked": False})
        for i in range(201, 211):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 12, "qualified_at": now})
        row_b = evaluate_monthly_affiliate_reward(db, referrer_id=12, now_utc=now)
        issued = [r for r in (row_a, row_b) if r.get("status") == "ISSUED"]
        self.assertEqual(len(issued), 1)

    def test_previous_week_t1_issues_once_and_is_idempotent(self):
        db = FakeDb()
        uid = 7001
        db.users.insert_one({"user_id": uid, "blocked": False})
        now = datetime(2026, 1, 12, 1, 0, tzinfo=timezone.utc)
        event_time = datetime(2026, 1, 5, 1, 0, tzinfo=timezone.utc)
        for i in range(10):
            db.qualified_events.insert_one({"invitee_id": 700100 + i, "referrer_id": uid, "qualified_at": event_time})
        self.add_pool_bundle(db, "T1", "WEEK-T1")

        first = issue_previous_week_affiliate_rewards(db, now_utc=now)
        second = issue_previous_week_affiliate_rewards(db, now_utc=now)

        self.assertEqual(first["week_key"], "2026-01-05")
        self.assertEqual(first["issued_count"], 1)
        self.assertEqual(second["issued_count"], 0)
        ledger = db.affiliate_ledger.find_one({"dedup_key": f"AFFW:{uid}:2026-01-05:T1"})
        self.assertIsNotNone(ledger)
        self.assertEqual(ledger["ledger_type"], "AFFILIATE_WEEKLY")
        self.assert_bundle(ledger, "T1")
        self.assertEqual(ledger["voucher_code"], "WEEK-T1-1")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}), 2)

    def test_current_week_t1_issues_after_qualified_threshold_before_week_end(self):
        db = FakeDb()
        uid = 7011
        db.users.insert_one({"user_id": uid, "blocked": False})
        now = datetime(2026, 1, 7, 6, 0, tzinfo=timezone.utc)
        for i in range(10):
            db.qualified_events.insert_one({"invitee_id": 701100 + i, "referrer_id": uid, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "CUR-WEEK-T1")

        first = issue_current_week_affiliate_rewards(db, now_utc=now)
        second = issue_current_week_affiliate_rewards(db, now_utc=now + timedelta(minutes=30))

        self.assertEqual(first["week_key"], "2026-01-05")
        self.assertEqual(first["issued_count"], 1)
        self.assertEqual(second["issued_count"], 0)
        ledger = db.affiliate_ledger.find_one({"dedup_key": f"AFFW:{uid}:2026-01-05:T1"})
        self.assert_bundle(ledger, "T1")
        self.assertEqual(ledger["voucher_code"], "CUR-WEEK-T1-1")

    def test_previous_week_pool_empty_stays_pending_manual(self):
        db = FakeDb()
        uid = 7002
        db.users.insert_one({"user_id": uid, "blocked": False})
        now = datetime(2026, 1, 12, 1, 0, tzinfo=timezone.utc)
        event_time = datetime(2026, 1, 5, 1, 0, tzinfo=timezone.utc)
        for i in range(10):
            db.qualified_events.insert_one({"invitee_id": 700200 + i, "referrer_id": uid, "qualified_at": event_time})

        out = issue_previous_week_affiliate_rewards(db, now_utc=now)

        self.assertEqual(out["pending_manual"], 1)
        self.assertEqual(out["pool_empty"], 1)
        ledger = db.affiliate_ledger.find_one({"dedup_key": f"AFFW:{uid}:2026-01-05:T1"})
        self.assertEqual(ledger["status"], "PENDING_MANUAL")
        self.assertIn("pool_empty", ledger.get("risk_flags") or [])

    def test_previous_week_simulation_does_not_consume_voucher(self):
        db = FakeDb()
        uid = 7003
        db.users.insert_one({"user_id": uid, "blocked": False})
        now = datetime(2026, 1, 12, 1, 0, tzinfo=timezone.utc)
        event_time = datetime(2026, 1, 5, 1, 0, tzinfo=timezone.utc)
        for i in range(10):
            db.qualified_events.insert_one({"invitee_id": 700300 + i, "referrer_id": uid, "qualified_at": event_time})
        self.add_pool_bundle(db, "T1", "SIM-T1")

        with patch.dict(os.environ, {"AFFILIATE_SIMULATE": "1"}):
            out = issue_previous_week_affiliate_rewards(db, now_utc=now)

        self.assertEqual(out["issued_count"], 0)
        ledger = db.affiliate_ledger.find_one({"dedup_key": f"AFFW:{uid}:2026-01-05:T1"})
        self.assertEqual(ledger["status"], "SIMULATED_PENDING")
        self.assertEqual(ledger["would_issue_pool"], "T1")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}), 2)

    def test_simulate_mode_creates_ledger_without_pool_consumption(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 31, "blocked": False})
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 31, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "SIMT1")

        os.environ["AFFILIATE_SIMULATE"] = "1"
        try:
            row = evaluate_monthly_affiliate_reward(db, referrer_id=31, now_utc=now)
        finally:
            os.environ.pop("AFFILIATE_SIMULATE", None)

        self.assertEqual(row["status"], "SIMULATED_PENDING")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}), 2)

    def test_simulate_mode_dedup_safe_on_duplicate_evaluation(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 41, "blocked": False})
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 41, "qualified_at": now})

        os.environ["AFFILIATE_SIMULATE"] = "1"
        try:
            evaluate_monthly_affiliate_reward(db, referrer_id=41, now_utc=now)
            evaluate_monthly_affiliate_reward(db, referrer_id=41, now_utc=now)
        finally:
            os.environ.pop("AFFILIATE_SIMULATE", None)

        self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": "AFF:41:202601:T1"}), 1)

    def test_current_month_simulated_pending_not_auto_issued_when_sim_off(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 42, "blocked": False})
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": 4200 + i, "referrer_id": 42, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "REAL42")

        os.environ["AFFILIATE_SIMULATE"] = "1"
        try:
            simulated = evaluate_monthly_affiliate_reward(db, referrer_id=42, now_utc=now)
        finally:
            os.environ.pop("AFFILIATE_SIMULATE", None)
        self.assertEqual(simulated["status"], "SIMULATED_PENDING")

        row = evaluate_monthly_affiliate_reward(db, referrer_id=42, now_utc=now + timedelta(minutes=1))
        self.assertEqual(row["status"], "SIMULATED_PENDING")
        self.assertIsNone(row.get("voucher_code"))
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}), 0)

    def test_mark_invitee_qualified_triggers_monthly_evaluation(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 77, "blocked": False})
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        self.add_pool_bundle(db, "T1", "M77")
        for i in range(1, 10):
            db.qualified_events.insert_one({"invitee_id": 7700 + i, "referrer_id": 77, "qualified_at": now})

        out = mark_invitee_qualified(db, invitee_id=7799, referrer_id=77, now_utc=now)
        self.assertTrue(out)
        ledger = db.affiliate_ledger.find_one({"dedup_key": "AFF:77:202601:T1"})
        self.assertIsNotNone(ledger)
        self.assert_bundle(ledger, "T1")
        self.assertEqual(ledger["voucher_code"], "M77-1")

    def test_historical_simulated_pending_not_auto_issued(self):
        db = FakeDb()
        user_id = 143
        db.users.insert_one({"user_id": user_id, "blocked": False})
        now = datetime(2026, 2, 10, tzinfo=timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": 14300 + i, "referrer_id": user_id, "qualified_at": now})
        db.affiliate_ledger.insert_one(
            {
                "dedup_key": "AFF:143:202601:T1",
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": user_id,
                "year_month": "202601",
                "tier": "T1",
                "pool_id": "T1",
                "status": "SIMULATED_PENDING",
                "simulate": True,
                "voucher_code": None,
                "created_at": now - timedelta(days=30),
                "updated_at": now - timedelta(days=30),
            }
        )
        self.add_pool_bundle(db, "T1", "HIST-T1")
        out = evaluate_monthly_affiliate_reward(db, referrer_id=user_id, now_utc=now)
        jan = db.affiliate_ledger.find_one({"dedup_key": "AFF:143:202601:T1"})
        self.assertEqual(jan["status"], "SIMULATED_PENDING")
        self.assertEqual(out["status"], "ISSUED")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}), 2)

    def test_late_evaluation_issues_each_eligible_tier_once(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 55, "blocked": False})
        now = datetime(2026, 1, 20, tzinfo=timezone.utc)
        for i in range(1, 61):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 55, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "L1")
        self.add_pool_bundle(db, "T2", "L2")
        self.add_pool_bundle(db, "T3", "L3")

        evaluate_monthly_affiliate_reward(db, referrer_id=55, now_utc=now)
        evaluate_monthly_affiliate_reward(db, referrer_id=55, now_utc=now)

        for tier in ("T1", "T2", "T3"):
            dedup = f"AFF:55:202601:{tier}"
            self.assertEqual(db.affiliate_ledger.count_documents({"dedup_key": dedup}), 1)
            row = db.affiliate_ledger.find_one({"dedup_key": dedup})
            self.assert_bundle(row, tier)

    def test_settle_processes_stale_approved_previous_month(self):
        db = FakeDb()
        now = datetime(2026, 2, 20, 0, 0, tzinfo=timezone.utc)
        stale_updated_at = now - timedelta(minutes=16)
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 77,
                "year_month": "202601",
                "tier": "T1",
                "pool_id": "T1",
                "qualified_count": 10,
                "status": "APPROVED",
                "dedup_key": "AFF:77:202601:T1",
                "voucher_code": None,
                "risk_flags": [],
                "created_at": stale_updated_at,
                "updated_at": stale_updated_at,
            }
        )
        self.add_pool_bundle(db, "T1", "SETTLE1")

        out = settle_previous_month_affiliate_rewards(db, now_utc=now)

        row = db.affiliate_ledger.find_one({"dedup_key": "AFF:77:202601:T1"})
        self.assertEqual(out["prev_yyyymm"], "202601")
        self.assert_bundle(row, "T1")
        self.assertEqual(row["voucher_code"], "SETTLE1-1")

    def test_settle_skips_reclaim_when_voucher_already_present(self):
        db = FakeDb()
        now = datetime(2026, 2, 20, 0, 0, tzinfo=timezone.utc)
        stale_updated_at = now - timedelta(minutes=16)
        db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 91,
                "year_month": "202601",
                "tier": "T1",
                "pool_id": "T1",
                "qualified_count": 10,
                "status": "SETTLING",
                "dedup_key": "AFF:91:202601:T1",
                "voucher_code": "EXISTING-1",
                "risk_flags": [],
                "created_at": stale_updated_at,
                "updated_at": stale_updated_at,
            }
        )
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "NEXT1", "status": "available"})
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "NEXT2", "status": "available"})

        first = settle_previous_month_affiliate_rewards(db, now_utc=now)
        second = settle_previous_month_affiliate_rewards(db, now_utc=now + timedelta(minutes=20))

        row = db.affiliate_ledger.find_one({"dedup_key": "AFF:91:202601:T1"})
        self.assertEqual(row["voucher_code"], "EXISTING-1")
        self.assertEqual(row["status"], "ISSUED")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}), 0)
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}), 2)
        self.assertGreaterEqual(first["processed"], 1)
        self.assertEqual(second["processed"], 0)

    def test_settle_reconciles_from_issued_pool_without_second_claim(self):
        db = FakeDb()
        now = datetime(2026, 2, 20, 0, 0, tzinfo=timezone.utc)
        stale_updated_at = now - timedelta(minutes=16)
        ledger = db.affiliate_ledger.insert_one(
            {
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 92,
                "year_month": "202601",
                "tier": "T1",
                "pool_id": "T1",
                "qualified_count": 10,
                "status": "SETTLING",
                "dedup_key": "AFF:92:202601:T1",
                "voucher_code": None,
                "risk_flags": [],
                "created_at": stale_updated_at,
                "updated_at": stale_updated_at,
            }
        )
        db.voucher_pools.insert_one(
            {
                "pool_id": "T1",
                "code": "BOUND1-1",
                "status": "issued",
                "issued_for_ledger_id": str(ledger["_id"]),
                "issued_at": now - timedelta(minutes=20),
            }
        )
        db.voucher_pools.insert_one(
            {
                "pool_id": "T1",
                "code": "BOUND1-2",
                "status": "issued",
                "issued_for_ledger_id": str(ledger["_id"]),
                "issued_at": now - timedelta(minutes=20),
            }
        )
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "NEXT1", "status": "available"})
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "NEXT2", "status": "available"})

        settle_previous_month_affiliate_rewards(db, now_utc=now, batch_limit=10)

        row = db.affiliate_ledger.find_one({"dedup_key": "AFF:92:202601:T1"})
        self.assert_bundle(row, "T1")
        self.assertEqual(row["voucher_code"], "BOUND1-1")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}), 2)
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}), 2)

    def test_issue_path_cas_prevents_double_consumption(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 66, "blocked": False})
        now = datetime(2026, 1, 12, tzinfo=timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 66, "qualified_at": now})
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "ONE", "status": "available"})
        db.voucher_pools.insert_one({"pool_id": "T1", "code": "TWO", "status": "available"})

        first = evaluate_monthly_affiliate_reward(db, referrer_id=66, now_utc=now)
        second = evaluate_monthly_affiliate_reward(db, referrer_id=66, now_utc=now)

        self.assertEqual(first["status"], "ISSUED")
        self.assertEqual(second["status"], "ISSUED")
        self.assertEqual(first.get("voucher_code"), second.get("voucher_code"))
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}), 2)

    def test_monthly_upsert_no_set_conflict_creates_and_issues_t1(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 166, "blocked": False})
        now = datetime(2026, 1, 16, tzinfo=timezone.utc)
        for i in range(1, 15):
            db.qualified_events.insert_one({"invitee_id": 16600 + i, "referrer_id": 166, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "UPSERT-T1")

        row = evaluate_monthly_affiliate_reward(db, referrer_id=166, now_utc=now)

        self.assertEqual(row["dedup_key"], "AFF:166:202601:T1")
        self.assert_bundle(row, "T1")
        self.assertIsNotNone(row.get("voucher_code"))

    def test_existing_issued_ledger_rerun_keeps_status_and_voucher_code(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 167, "blocked": False})
        now = datetime(2026, 1, 16, tzinfo=timezone.utc)
        for i in range(1, 15):
            db.qualified_events.insert_one({"invitee_id": 16700 + i, "referrer_id": 167, "qualified_at": now})

        db.affiliate_ledger.insert_one({
            "dedup_key": "AFF:167:202601:T1",
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": 167,
            "year_month": "202601",
            "tier": "T1",
            "pool_id": "T1",
            "status": "ISSUED",
            "voucher_code": "LOCKED-T1",
            "risk_flags": [],
            "qualified_count": 14,
            "created_at": now,
            "updated_at": now,
        })

        row = evaluate_monthly_affiliate_reward(db, referrer_id=167, now_utc=now + timedelta(minutes=1))

        self.assertEqual(row["status"], "ISSUED")
        self.assertEqual(row.get("voucher_code"), "LOCKED-T1")

    def test_settling_retry_issues_voucher_when_pool_claim_incomplete(self):
        # Ledger stuck in SETTLING (e.g. process crashed before pool claim ran).
        # Next evaluate call must retry the pool claim and reach ISSUED.
        db = FakeDb()
        db.users.insert_one({"user_id": 77, "blocked": False})
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": i, "referrer_id": 77, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "RETRY1")

        # Simulate a crash mid-claim: ledger is SETTLING but no pool voucher was claimed.
        dedup_key = "AFF:77:202601:T1"
        db.affiliate_ledger.insert_one({
            "dedup_key": dedup_key,
            "ledger_type": "AFFILIATE_MONTHLY",
            "user_id": 77,
            "year_month": "202601",
            "tier": "T1",
            "pool_id": "T1",
            "status": "SETTLING",
            "voucher_code": None,
            "risk_flags": [],
            "qualified_count": 10,
            "created_at": now,
            "updated_at": now,
        })

        row = evaluate_monthly_affiliate_reward(db, referrer_id=77, now_utc=now)

        self.assert_bundle(row, "T1")
        self.assertIsNotNone(row.get("voucher_code"))
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "issued"}), 2)

    def test_pool_empty_pending_manual_retries_after_refill(self):
        db = FakeDb()
        db.users.insert_one({"user_id": 175, "blocked": False})
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": 17500 + i, "referrer_id": 175, "qualified_at": now})
        first = evaluate_monthly_affiliate_reward(db, referrer_id=175, now_utc=now)
        self.assertEqual(first["status"], "PENDING_MANUAL")
        self.assertIn("pool_empty", first.get("risk_flags") or [])
        self.add_pool_bundle(db, "T1", "REFILL-T1")
        summary = issue_current_month_affiliate_rewards(db, now_utc=now + timedelta(minutes=5), batch_limit=10)
        after = db.affiliate_ledger.find_one({"dedup_key": "AFF:175:202601:T1"})
        self.assert_bundle(after, "T1")
        self.assertGreaterEqual(summary["issued_count"], 1)

    def test_duplicate_monthly_tier_is_rejected_before_pool_claim(self):
        db = FakeDb()
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        db.voucher_pools.insert_one({"pool_id": "T2", "code": "T2-ONLY", "status": "available"})
        db.affiliate_ledger.insert_one(
            {
                "dedup_key": "AFF:501:202601:T2:1",
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 501,
                "year_month": "202601",
                "tier": "T2",
                "pool_id": "T2",
                "status": "ISSUED",
                "voucher_code": "ALREADY-T2",
                "risk_flags": [],
                "created_at": now,
                "updated_at": now,
            }
        )
        dupe = db.affiliate_ledger.insert_one(
            {
                "dedup_key": "AFF:501:202601:T2:2",
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 501,
                "year_month": "202601",
                "tier": "T2",
                "pool_id": "T2",
                "status": "APPROVED",
                "voucher_code": None,
                "risk_flags": [],
                "created_at": now,
                "updated_at": now,
            }
        )

        approve_affiliate_ledger(db, ledger_id=dupe["_id"], now_utc=now)

        rejected = db.affiliate_ledger.find_one({"_id": dupe["_id"]})
        self.assertEqual(rejected["status"], "REJECTED")
        self.assertEqual(rejected["review_reason"], "duplicate_monthly_tier")
        self.assertIsNotNone(rejected.get("duplicate_of"))
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T2", "status": "available"}), 1)
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T2", "status": "issued"}), 0)

    def test_non_duplicate_or_allowed_variants_still_issue_normally(self):
        db = FakeDb()
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        self.add_pool_bundle(db, "T2", "ISSUE-T2")
        self.add_pool_bundle(db, "T3", "ISSUE-T3")
        self.add_pool_bundle(db, "T2", "ISSUE-T2-NEXT")
        same_month_diff_tier = db.affiliate_ledger.insert_one(
            {
                "dedup_key": "AFF:601:202601:T3",
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 601,
                "year_month": "202601",
                "tier": "T3",
                "pool_id": "T3",
                "status": "APPROVED",
                "voucher_code": None,
                "risk_flags": [],
                "created_at": now,
                "updated_at": now,
            }
        )
        diff_month_same_tier = db.affiliate_ledger.insert_one(
            {
                "dedup_key": "AFF:601:202602:T2",
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 601,
                "year_month": "202602",
                "tier": "T2",
                "pool_id": "T2",
                "status": "APPROVED",
                "voucher_code": None,
                "risk_flags": [],
                "created_at": now,
                "updated_at": now,
            }
        )

        approve_affiliate_ledger(db, ledger_id=same_month_diff_tier["_id"], now_utc=now)
        approve_affiliate_ledger(db, ledger_id=diff_month_same_tier["_id"], now_utc=now)

        issued_t3 = db.affiliate_ledger.find_one({"_id": same_month_diff_tier["_id"]})
        issued_t2_other_month = db.affiliate_ledger.find_one({"_id": diff_month_same_tier["_id"]})
        self.assert_bundle(issued_t3, "T3")
        self.assert_bundle(issued_t2_other_month, "T2")

    def test_approve_ledger_empty_pool_keeps_pending_manual(self):
        db = FakeDb()
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        inserted = db.affiliate_ledger.insert_one(
            {
                "dedup_key": "AFF:201:202601:T1",
                "ledger_type": "AFFILIATE_MONTHLY",
                "user_id": 201,
                "year_month": "202601",
                "tier": "T1",
                "pool_id": "T1",
                "status": "PENDING_MANUAL",
                "voucher_code": None,
                "risk_flags": [],
                "created_at": now,
                "updated_at": now,
            }
        )
        out = approve_affiliate_ledger(db, ledger_id=inserted["_id"], now_utc=now)
        self.assertEqual(out["status"], "PENDING_MANUAL")

    def test_blocked_user_creates_review_ledger_and_retry_skips_issuance(self):
        db = FakeDb()
        now = datetime(2026, 1, 15, tzinfo=timezone.utc)
        db.users.insert_one({"user_id": 301, "blocked": True})
        for i in range(1, 11):
            db.qualified_events.insert_one({"invitee_id": 3000 + i, "referrer_id": 301, "qualified_at": now})
        self.add_pool_bundle(db, "T1", "B-T1")

        first = evaluate_monthly_affiliate_reward(db, referrer_id=301, now_utc=now)
        self.assertEqual(first["status"], "PENDING_REVIEW")
        self.assertIn("blocked_user", first.get("risk_flags") or [])

        retry_current_month_pending_manual_ledgers(db, now_utc=now, batch_limit=10)

        after = db.affiliate_ledger.find_one({"dedup_key": "AFF:301:202601:T1"})
        self.assertEqual(after["status"], "PENDING_REVIEW")
        self.assertEqual(db.voucher_pools.count_documents({"pool_id": "T1", "status": "available"}), 2)

    def test_stuck_legacy_t2_recovers_through_the_real_scheduled_retry_path(self):
        # Regression test: evaluate_monthly_affiliate_reward and
        # settle_previous_month_affiliate_rewards both recompute risk_flags
        # from scratch on every pass and used to blindly overwrite the
        # ledger's risk_flags with that fresh (abuse-only) list — silently
        # wiping out a "pool_empty" inventory marker a prior claim attempt
        # had set. That erased the only signal the inventory-retry
        # eligibility check relies on, so the production retry path
        # (retry_current_month_pending_manual_ledgers) never actually
        # re-resolved a stuck ledger even after stock was uploaded.
        db = FakeDb()
        now1 = datetime(2026, 8, 15, tzinfo=timezone.utc)
        for i in range(1, 26):
            db.qualified_events.insert_one({"invitee_id": 9000 + i, "referrer_id": 701, "qualified_at": now1})

        first = evaluate_monthly_affiliate_reward(db, referrer_id=701, now_utc=now1)
        t2_first = db.affiliate_ledger.find_one({"dedup_key": "AFF:701:202608:T2"})
        self.assertEqual(t2_first["status"], "PENDING_MANUAL")
        self.assertIn("pool_empty", t2_first.get("risk_flags") or [])
        self.assertEqual(t2_first.get("target_mode"), "legacy")

        # Ops uploads a T2 batch covering the whole entitlement month.
        batch = db.affiliate_voucher_batches.insert_one(
            {
                "pool_id": "T2",
                "starts_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
                "ends_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
                "upload_status": "ready",
                "distribution_disabled": False,
            }
        )
        for i in range(1, 4):
            db.voucher_pools.insert_one(
                {"pool_id": "T2", "code": f"T2-BATCH-{i}", "status": "available", "batch_id": batch["_id"]}
            )

        # Real scheduled retry entrypoint — not the internal helper directly.
        now2 = datetime(2026, 8, 20, tzinfo=timezone.utc)
        retry_current_month_pending_manual_ledgers(db, now_utc=now2, batch_limit=10)

        t2_after = db.affiliate_ledger.find_one({"dedup_key": "AFF:701:202608:T2"})
        self.assertEqual(t2_after["status"], "ISSUED")
        self.assertEqual(t2_after.get("target_mode"), "batch")
        self.assertNotIn("pool_empty", t2_after.get("risk_flags") or [])
        self.assertEqual(
            db.affiliate_ledger.count_documents({"tier": "T2", "year_month": "202608", "user_id": 701}), 1
        )


if __name__ == "__main__":
    unittest.main()
