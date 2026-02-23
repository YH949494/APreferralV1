import unittest
from datetime import datetime, timedelta, timezone
from flask import Flask

from pymongo.errors import DuplicateKeyError

from vouchers import (
    _acquire_claim_lock,
    _apply_kill_success,
    _build_idempotent_claim_response,
    _check_cooldown,
    _check_session_cooldown,
    _check_kill_switch,
    _compute_subnet_key,
    _compute_claimable_remaining,
    _compute_visible_remaining,
    _derive_session_key,
    _get_client_ip,
    _set_cooldown,
    _set_session_cooldown,
    _session_cooldown_payload,
    _should_enforce_session_cooldown,
    _check_ip_success_limit_for_welcome1,
    _welcome48h_gate,
    claim_pooled,
    get_claimable_pools,
    get_visible_pools,
    reconcile_pooled_remaining,
    user_visible_drops,
    get_active_drops,
)


class FakeInsertResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class FakeClaimsCollection:
    def __init__(self):
        self.docs = []
        self._id = 1

    def _match(self, doc, filt):
        for key, value in filt.items():
            if doc.get(key) != value:
                return False
        return True

    def insert_one(self, doc):
        for existing in self.docs:
            if (
                existing.get("drop_id") == doc.get("drop_id")
                and existing.get("user_id") == doc.get("user_id")
            ):
                raise DuplicateKeyError("duplicate key")
        new_doc = dict(doc)
        new_doc["_id"] = self._id
        self._id += 1
        self.docs.append(new_doc)
        return FakeInsertResult(new_doc["_id"])

    def find_one(self, filt):
        for doc in self.docs:
            if self._match(doc, filt):
                return dict(doc)
        return None

    def find_one_and_update(self, filt, update, return_document=None):  # noqa: ARG002
        for doc in self.docs:
            if self._match(doc, filt):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key in update.get("$unset", {}).keys():
                    doc.pop(key, None)
                return dict(doc)
        return None

    def update_one(self, filt, update, upsert=False):
        for doc in self.docs:
            if self._match(doc, filt):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key in update.get("$unset", {}).keys():
                    doc.pop(key, None)
                return
        if upsert:
            new_doc = dict(filt)
            for key, value in update.get("$set", {}).items():
                new_doc[key] = value
            for key in update.get("$unset", {}).keys():
                new_doc.pop(key, None)
            new_doc["_id"] = self._id
            self._id += 1
            self.docs.append(new_doc)


class FakeRateLimitCollection:
    def __init__(self):
        self.docs = []
        self._id = 1

    def _match(self, doc, filt):
        for key, value in filt.items():
            if doc.get(key) != value:
                return False
        return True

    def find_one(self, filt):
        for doc in self.docs:
            if self._match(doc, filt):
                return dict(doc)
        return None

    def update_one(self, filt, update, upsert=False):
        for doc in self.docs:
            if self._match(doc, filt):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key in update.get("$unset", {}).keys():
                    doc.pop(key, None)
                return
        if upsert:
            new_doc = dict(filt)
            for key, value in update.get("$set", {}).items():
                new_doc[key] = value
            for key in update.get("$unset", {}).keys():
                new_doc.pop(key, None)
            new_doc["_id"] = self._id
            self._id += 1
            self.docs.append(new_doc)

class FakeRequest:
    def __init__(self, headers=None, remote_addr=None):
        self.headers = headers or {}
        self.remote_addr = remote_addr


class FakeUpdateResult:
    def __init__(self, modified_count):
        self.modified_count = modified_count


class FakeVouchersCollection:
    def __init__(self, docs=None):
        self.docs = []
        for idx, doc in enumerate(docs or [], start=1):
            item = dict(doc)
            item.setdefault("_id", idx)
            self.docs.append(item)

    def _match(self, doc, filt):
        for key, value in filt.items():
            if key == "$or":
                if not any(self._match(doc, sub) for sub in value):
                    return False
                continue
            if isinstance(value, dict) and "$in" in value:
                if doc.get(key) not in value["$in"]:
                    return False
                continue
            if isinstance(value, dict) and "$exists" in value:
                exists = key in doc
                if value["$exists"] != exists:
                    return False
                continue
            if doc.get(key) != value:
                return False
        return True

    def find_one(self, filt):
        for doc in self.docs:
            if self._match(doc, filt):
                return dict(doc)
        return None

    def find_one_and_update(self, filt, update, sort=None, return_document=None):  # noqa: ARG002
        for doc in self.docs:
            if self._match(doc, filt):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                return dict(doc)
        return None

    def update_one(self, filt, update):
        for doc in self.docs:
            if self._match(doc, filt):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key in update.get("$unset", {}).keys():
                    doc.pop(key, None)
                return

    def count_documents(self, filt):
        return sum(1 for doc in self.docs if self._match(doc, filt))

class FakeDropsCollection:
    def __init__(self, docs=None):
        self.docs = {doc["_id"]: dict(doc) for doc in (docs or [])}

    def find_one(self, filt, projection=None):
        doc = self.docs.get(filt.get("_id"))
        if not doc:
            return {}
        if projection:
            return {key: doc.get(key) for key in projection.keys()}
        return dict(doc)

    def update_one(self, filt, update):
        doc = self.docs.get(filt.get("_id"))
        if not doc:
            return FakeUpdateResult(0)
        for key, value in filt.items():
            if key == "_id":
                continue
            if isinstance(value, dict) and "$gt" in value:
                if not doc.get(key, 0) > value["$gt"]:
                    return FakeUpdateResult(0)
        for key, value in update.get("$set", {}).items():
            doc[key] = value                    
        for key, value in update.get("$inc", {}).items():
            doc[key] = doc.get(key, 0) + value
        return FakeUpdateResult(1)


class FakeDb:
    def __init__(self, drops, vouchers):
        self.drops = FakeDropsCollection(drops)
        self.vouchers = FakeVouchersCollection(vouchers)
        
class VoucherAntiHunterTests(unittest.TestCase):
    def test_welcome48h_gate_denies_and_allows(self):
        import vouchers as m

        uid = 777
        fixed_now = datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc)
        orig_check_channel_subscribed = m.check_channel_subscribed
        orig_now_utc = m.now_utc
        try:
            m.now_utc = lambda: fixed_now

            m.check_channel_subscribed = lambda _uid: False
            allowed, code, _message = _welcome48h_gate(uid, {"last_checkin": fixed_now, "joined_main_at": fixed_now - timedelta(hours=72)})
            self.assertFalse(allowed)
            self.assertEqual(code, "not_subscribed")

            m.check_channel_subscribed = lambda _uid: True
            allowed, code, _message = _welcome48h_gate(uid, {"joined_main_at": fixed_now - timedelta(hours=72)})
            self.assertFalse(allowed)
            self.assertEqual(code, "no_checkin")

            allowed, code, _message = _welcome48h_gate(uid, {"last_checkin": fixed_now, "joined_main_at": fixed_now - timedelta(hours=47)})
            self.assertFalse(allowed)
            self.assertEqual(code, "stay_48h")

            allowed, code, _message = _welcome48h_gate(uid, {"last_checkin": fixed_now, "joined_main_at": fixed_now - timedelta(hours=72)})
            self.assertTrue(allowed)
            self.assertEqual(code, "ok")
        finally:
            m.check_channel_subscribed = orig_check_channel_subscribed
            m.now_utc = orig_now_utc

    def test_welcome1_ip_success_limit_helper(self):
        import vouchers as m

        now = datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc)
        day_key = now.strftime("%Y%m%d")
        fake_rate_limits = FakeRateLimitCollection()
        orig_claim_rate_limits_col = m.claim_rate_limits_col
        try:
            m.claim_rate_limits_col = fake_rate_limits
            app = Flask(__name__)
            with app.app_context():
                allowed, code, _message = _check_ip_success_limit_for_welcome1("1.2.3.4", now=now)
                self.assertTrue(allowed)
                self.assertEqual(code, "ok")

                fake_rate_limits.docs.append({
                    "_id": 99,
                    "key": f"ip:1.2.3.4:day:{day_key}",
                    "scope": "ip_claim",
                    "ip": "1.2.3.4",
                    "day": day_key,
                    "count": 1,
                })
                allowed, code, _message = _check_ip_success_limit_for_welcome1("1.2.3.4", now=now)
                self.assertFalse(allowed)
                self.assertEqual(code, "ip_success_limited")

                self.assertEqual(_check_ip_success_limit_for_welcome1("1.2.3.4", now=now)[0], False)

                class _IncCollection:
                    def __init__(self):
                        self.docs = {}

                    def find_one(self, filt):
                        key = filt.get("key")
                        doc = self.docs.get(key)
                        return dict(doc) if doc else None

                    def find_one_and_update(self, filt, update, upsert=False, return_document=None):
                        key = filt.get("key")
                        doc = self.docs.get(key)
                        if not doc:
                            if not upsert:
                                return None
                            doc = {"key": key}
                            doc.update(update.get("$setOnInsert", {}))
                            self.docs[key] = doc
                        for k, v in update.get("$inc", {}).items():
                            doc[k] = int(doc.get(k, 0) or 0) + int(v)
                        return dict(doc)

                m.claim_rate_limits_col = _IncCollection()
                count = m._increment_ip_claim_success(ip="5.6.7.8", now=now)
                self.assertEqual(count, 1)
                allowed, code, _message = _check_ip_success_limit_for_welcome1("5.6.7.8", now=now)
                self.assertFalse(allowed)
                self.assertEqual(code, "ip_success_limited")
        finally:
            m.claim_rate_limits_col = orig_claim_rate_limits_col

    def test_get_client_ip_prefers_fly_header_over_private_remote_addr(self):
        req = FakeRequest(
            headers={
                "Fly-Client-IP": "203.0.113.10",
                "X-Forwarded-For": "198.51.100.20",
                "X-Real-IP": "192.0.2.30",
            },
            remote_addr="172.16.0.5",
        )
        client_ip = _get_client_ip(req)
        self.assertEqual(client_ip, "203.0.113.10")
    
    def test_claim_lock_idempotent_duplicate(self):
        claims = FakeClaimsCollection()
        now = datetime.now(timezone.utc)
        claim_id, existing = _acquire_claim_lock(
            drop_id="drop-1",
            user_id="user-1",
            client_ip="1.1.1.1",
            user_agent="ua",
            now=now,
            claims_col=claims,
        )
        self.assertIsNotNone(claim_id)
        self.assertIsNone(existing)

        claims.update_one(
            {"_id": claim_id},
            {"$set": {"voucher_code": "CODE123", "claimed_at": now, "status": "claimed"}},
        )

        claim_id_2, existing_2 = _acquire_claim_lock(
            drop_id="drop-1",
            user_id="user-1",
            client_ip="1.1.1.1",
            user_agent="ua",
            now=now + timedelta(seconds=1),
            claims_col=claims,
        )
        self.assertIsNone(claim_id_2)
        payload = _build_idempotent_claim_response(existing_2)
        self.assertEqual(payload["voucher"]["code"], "CODE123")

        claim_id_3, existing_3 = _acquire_claim_lock(
            drop_id="drop-1",
            user_id="user-1",
            client_ip="1.1.1.1",
            user_agent="ua",
            now=now + timedelta(seconds=2),
            claims_col=claims,
        )
        self.assertIsNone(claim_id_3)
        payload = _build_idempotent_claim_response(existing_3)
        self.assertIsNotNone(payload)

        claim_id_no_code, existing_no_code = _acquire_claim_lock(
            drop_id="drop-2",
            user_id="user-2",
            client_ip="1.1.1.2",
            user_agent="ua",
            now=now,
            claims_col=claims,
        )
        self.assertIsNotNone(claim_id_no_code)
        self.assertIsNone(existing_no_code)

        claim_id_no_code_2, existing_no_code_2 = _acquire_claim_lock(
            drop_id="drop-2",
            user_id="user-2",
            client_ip="1.1.1.2",
            user_agent="ua",
            now=now + timedelta(seconds=1),
            claims_col=claims,
        )
        self.assertIsNone(claim_id_no_code_2)
        payload = _build_idempotent_claim_response(existing_no_code_2)
        self.assertIsNone(payload)

    def test_visible_remaining_excludes_reserved_pool_for_non_my_user(self):
        drop = {"public_remaining": 10, "my_remaining": 20}
        visible_pools = get_visible_pools("th", drop, uid=123)
        self.assertEqual(visible_pools, ["public"])
        self.assertEqual(_compute_visible_remaining("th", drop, uid=123), 10)
        self.assertEqual(_compute_claimable_remaining("th", drop), 10)

    def test_visible_remaining_includes_my_pool_for_my_user(self):
        drop = {"public_remaining": 10, "my_remaining": 20}
        visible_pools = get_visible_pools("MY", drop, uid=456)
        self.assertEqual(visible_pools, ["public", "my"])
        self.assertEqual(_compute_visible_remaining("MY", drop, uid=456), 30)
        self.assertEqual(_compute_claimable_remaining("MY", drop), 30)

    def test_claimable_pools_exclude_my_for_non_my_user(self):
        drop = {"public_remaining": 10, "my_remaining": 20}
        self.assertEqual(get_claimable_pools("th", drop), ["public"])
        self.assertEqual(get_claimable_pools("malaysia", drop), ["my", "public"])


    def test_claimable_remaining_blocks_non_my_when_public_empty(self):
        drop = {"public_remaining": 0, "my_remaining": 2}
        self.assertEqual(_compute_claimable_remaining("th", drop), 0)
        self.assertEqual(_compute_claimable_remaining("MY", drop), 2)

    def test_claim_pooled_prefers_my_pool_then_public(self):
        import vouchers as vouchers_module

        now = datetime.now(timezone.utc)
        fake_db = FakeDb(
            drops=[{"_id": "drop-1", "my_remaining": 1, "public_remaining": 1}],
            vouchers=[
                {
                    "type": "pooled",
                    "dropId": "drop-1",
                    "code": "MYCODE",
                    "status": "free",
                    "pool": "my",
                },
                {
                    "type": "pooled",
                    "dropId": "drop-1",
                    "code": "PUBCODE",
                    "status": "free",
                    "pool": "public",
                },
            ],
        )
        original_db = vouchers_module.db
        vouchers_module.db = fake_db
        try:
            first = claim_pooled(drop_id="drop-1", claim_key="uid:1", ref=now, pools=["my", "public"])
            self.assertEqual(first["code"], "MYCODE")
            self.assertEqual(fake_db.drops.docs["drop-1"]["my_remaining"], 0)

            second = claim_pooled(drop_id="drop-1", claim_key="uid:2", ref=now, pools=["my", "public"])
            self.assertEqual(second["code"], "PUBCODE")
            self.assertEqual(fake_db.drops.docs["drop-1"]["public_remaining"], 0)
        finally:
            vouchers_module.db = original_db

    def test_claim_pooled_non_my_uses_public_only(self):
        import vouchers as vouchers_module

        now = datetime.now(timezone.utc)
        fake_db = FakeDb(
            drops=[{"_id": "drop-2", "my_remaining": 1, "public_remaining": 1}],
            vouchers=[
                {
                    "type": "pooled",
                    "dropId": "drop-2",
                    "code": "MYCODE2",
                    "status": "free",
                    "pool": "my",
                },
                {
                    "type": "pooled",
                    "dropId": "drop-2",
                    "code": "PUBCODE2",
                    "status": "free",
                    "pool": "public",
                },
            ],
        )
        original_db = vouchers_module.db
        vouchers_module.db = fake_db
        try:
            res = claim_pooled(drop_id="drop-2", claim_key="uid:9", ref=now, pools=["public"])
            self.assertEqual(res["code"], "PUBCODE2")
            self.assertEqual(fake_db.drops.docs["drop-2"]["public_remaining"], 0)
            self.assertEqual(fake_db.drops.docs["drop-2"]["my_remaining"], 1)
        finally:
            vouchers_module.db = original_db


    def test_claim_pooled_reconciles_stale_my_remaining(self):
        import vouchers as vouchers_module

        now = datetime.now(timezone.utc)
        fake_db = FakeDb(
            drops=[{"_id": "drop-3", "my_remaining": 10, "public_remaining": 0}],
            vouchers=[],
        )
        original_db = vouchers_module.db
        vouchers_module.db = fake_db
        try:
            res = claim_pooled(drop_id="drop-3", claim_key="uid:11", ref=now, pools=["my", "public"])
            self.assertFalse(res["ok"])
            self.assertEqual(res["err"], "sold_out")
            self.assertEqual(fake_db.drops.docs["drop-3"]["my_remaining"], 0)
        finally:
            vouchers_module.db = original_db

    def test_claim_pooled_my_pool_decrements_when_available(self):
        import vouchers as vouchers_module

        now = datetime.now(timezone.utc)
        fake_db = FakeDb(
            drops=[{"_id": "drop-4", "my_remaining": 10, "public_remaining": 0}],
            vouchers=[
                {
                    "type": "pooled",
                    "dropId": "drop-4",
                    "code": "MYONLY",
                    "status": "free",
                    "pool": "my",
                }
            ],
        )
        original_db = vouchers_module.db
        vouchers_module.db = fake_db
        try:
            res = claim_pooled(drop_id="drop-4", claim_key="uid:22", ref=now, pools=["my", "public"])
            self.assertTrue(res["ok"])
            self.assertEqual(res["code"], "MYONLY")
            self.assertEqual(fake_db.drops.docs["drop-4"]["my_remaining"], 9)
        finally:
            vouchers_module.db = original_db

    def test_legacy_public_pool_counts_missing_pool_field(self):
        import vouchers as vouchers_module

        now = datetime.now(timezone.utc)
        fake_db = FakeDb(
            drops=[{"_id": "drop-5", "my_remaining": 0, "public_remaining": 1}],
            vouchers=[
                {
                    "type": "pooled",
                    "dropId": "drop-5",
                    "code": "LEGACY",
                    "status": "free",
                }
            ],
        )
        original_db = vouchers_module.db
        vouchers_module.db = fake_db
        try:
            reconcile = reconcile_pooled_remaining("drop-5")
            self.assertEqual(reconcile["actual_free_public"], 1)
            self.assertEqual(fake_db.drops.docs["drop-5"]["public_remaining"], 1)

            res = claim_pooled(drop_id="drop-5", claim_key="uid:33", ref=now, pools=["public"])
            self.assertTrue(res["ok"])
            self.assertEqual(res["code"], "LEGACY")
        finally:
            vouchers_module.db = original_db


    def test_internal_drop_not_in_visible_list(self):
        import vouchers as m

        class FakeFindResult:
            def __init__(self, docs):
                self.docs = docs

            def __iter__(self):
                return iter(self.docs)

        class FakeDropsForVisible:
            def __init__(self):
                self.last_query = None

            def find(self, query):
                self.last_query = query
                return FakeFindResult([])

        class FakeDbForVisible:
            def __init__(self):
                self.drops = FakeDropsForVisible()

        now = datetime.now(timezone.utc)
        orig_db = m.db
        fake_db = FakeDbForVisible()
        try:
            m.db = fake_db
            get_active_drops(now)
            self.assertEqual(fake_db.drops.last_query.get("internal_only"), {"$ne": True})
        finally:
            m.db = orig_db

    def test_claim_internal_drop_without_ledger_rejected(self):
        import vouchers as m
        from flask import Flask

        app = Flask(__name__)
        now = datetime.now(timezone.utc)
        drop_id = "drop-internal-claim"
        drop = {
            "_id": drop_id,
            "name": "Internal",
            "type": "pooled",
            "status": "active",
            "startsAt": now - timedelta(minutes=5),
            "endsAt": now + timedelta(minutes=5),
            "public_remaining": 1,
            "my_remaining": 0,
            "internal_only": True,
        }

        orig_extract = m.extract_raw_init_data_from_query
        orig_verify = m.verify_telegram_init_data
        orig_db = m.db
        orig_users = m.users_collection
        orig_claims = m.voucher_claims_col
        orig_load_user_context = m.load_user_context
        orig_is_drop_allowed = m.is_drop_allowed
        orig_is_user_eligible = m.is_user_eligible_for_drop
        orig_ledger = m.ugc_reward_ledger_col
        try:
            m.extract_raw_init_data_from_query = lambda req: "ok"
            m.verify_telegram_init_data = lambda init_data: (True, {"user": '{"id": 42, "username": "u42"}'}, "ok")
            m.db = FakeDb([drop], [])
            m.users_collection = FakeSimpleCollection([{"user_id": 42, "usernameLower": "u42", "region": "th"}])
            m.load_user_context = lambda **kwargs: {}
            m.is_drop_allowed = lambda *args, **kwargs: True
            m.is_user_eligible_for_drop = lambda *args, **kwargs: True
            m.ugc_reward_ledger_col = FakeSimpleCollection([])

            with app.test_request_context(
                "/vouchers/claim?init_data=ok",
                method="POST",
                json={"dropId": drop_id},
            ):
                resp, status = m.api_claim()
                self.assertEqual(status, 403)
                self.assertEqual(resp.get_json().get("code"), "not_eligible")
        finally:
            m.extract_raw_init_data_from_query = orig_extract
            m.verify_telegram_init_data = orig_verify
            m.db = orig_db
            m.users_collection = orig_users
            m.voucher_claims_col = orig_claims
            m.load_user_context = orig_load_user_context
            m.is_drop_allowed = orig_is_drop_allowed
            m.is_user_eligible_for_drop = orig_is_user_eligible
            m.ugc_reward_ledger_col = orig_ledger
    
    def test_kill_switch_blocks_after_threshold(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        ip = "2.3.4.5"
        subnet = _compute_subnet_key(ip)

        _apply_kill_success(
            ip=ip,
            subnet=subnet,
            now=now,
            rate_limits_col=rate_limits,
            ip_threshold=2,
            subnet_threshold=4,
            window_seconds=600,
            block_seconds=3600,
        )
        _apply_kill_success(
            ip=ip,
            subnet=subnet,
            now=now + timedelta(seconds=5),
            rate_limits_col=rate_limits,
            ip_threshold=2,
            subnet_threshold=4,
            window_seconds=600,
            block_seconds=3600,
        )

        allowed, reason, retry = _check_kill_switch(
            ip=ip,
            subnet=subnet,
            now=now + timedelta(seconds=6),
            rate_limits_col=rate_limits,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "ip_killed")
        self.assertGreater(retry, 0)

    def test_kill_switch_does_not_block_on_unknown_subnet(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        rate_limits.docs.append(
            {
                "_id": 1,
                "key": "kill:subnet:unknown",
                "blockedUntil": now + timedelta(seconds=300),
            }
        )
        allowed, reason, retry = _check_kill_switch(
            ip="8.8.8.8",
            subnet="unknown",
            now=now,
            rate_limits_col=rate_limits,
        )
        self.assertTrue(allowed)
        self.assertIsNone(reason)
        self.assertEqual(retry, 0)

    def test_kill_switch_blocks_when_ip_blocked(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        rate_limits.docs.append(
            {
                "_id": 1,
                "key": "kill:ip:1.2.3.4",
                "blockedUntil": now + timedelta(seconds=120),
            }
        )
        allowed, reason, retry = _check_kill_switch(
            ip="1.2.3.4",
            subnet=_compute_subnet_key("1.2.3.4"),
            now=now,
            rate_limits_col=rate_limits,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "ip_killed")
        self.assertGreater(retry, 0)
    
    def test_cooldown_blocks_subsequent_attempts(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        ip = "9.9.9.9"
        subnet = _compute_subnet_key(ip)

        _set_cooldown(
            ip=ip,
            subnet=subnet,
            uid="user-9",            
            now=now,
            rate_limits_col=rate_limits,
            cooldown_seconds=180,
        )

        allowed, reason, retry = _check_cooldown(
            ip=ip,
            subnet=subnet,
            uid="user-9",            
            now=now + timedelta(seconds=10),
            rate_limits_col=rate_limits,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "cooldown")
        self.assertGreater(retry, 0)

        payload = _build_idempotent_claim_response(
            {"voucher_code": "CODE999", "claimed_at": now}
        )
        self.assertEqual(payload["voucher"]["code"], "CODE999")

    def test_cooldown_ignores_unknown_subnet_bucket(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        rate_limits.docs.append(
            {
                "_id": 1,
                "key": "cooldown:subnet:unknown",
                "expiresAt": now + timedelta(seconds=300),
            }
        )
        allowed, reason, retry = _check_cooldown(
            ip="1.2.3.4",
            subnet="unknown",
            uid="user-1",
            now=now,
            rate_limits_col=rate_limits,
        )
        self.assertTrue(allowed)
        self.assertIsNone(reason)
        self.assertEqual(retry, 0)

    def test_cooldown_known_subnet_still_denies(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        ip = "4.5.6.7"
        subnet = _compute_subnet_key(ip)
        rate_limits.docs.append(
            {
                "_id": 1,
                "key": f"cooldown:subnet:{subnet}",
                "expiresAt": now + timedelta(seconds=300),
            }
        )
        allowed, reason, retry = _check_cooldown(
            ip=ip,
            subnet=subnet,
            uid="user-2",
            now=now,
            rate_limits_col=rate_limits,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "cooldown")
        self.assertGreater(retry, 0)

    def test_cooldown_unknown_subnet_falls_back_to_uid(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        _set_cooldown(
            ip="",
            subnet="unknown",
            uid="user-3",
            now=now,
            rate_limits_col=rate_limits,
            cooldown_seconds=180,
        )

        allowed, reason, retry = _check_cooldown(
            ip="",
            subnet="unknown",
            uid="user-3",
            now=now + timedelta(seconds=5),
            rate_limits_col=rate_limits,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "cooldown")
        self.assertGreater(retry, 0)

        allowed_other, reason_other, retry_other = _check_cooldown(
            ip="",
            subnet="unknown",
            uid="user-4",
            now=now + timedelta(seconds=5),
            rate_limits_col=rate_limits,
        )
        self.assertTrue(allowed_other)
        self.assertIsNone(reason_other)
        self.assertEqual(retry_other, 0)


    def test_session_cooldown_blocks_on_unknown_subnet(self):
        rate_limits = FakeRateLimitCollection()
        now = datetime.now(timezone.utc)
        init_data = "user=%7B%22id%22%3A1%7D&auth_date=1700000000"
        session_key = _derive_session_key(
            init_data_raw=init_data,
            uid=1,
            auth_date="1700000000",
            query_id="query-1",
        )
        self.assertTrue(_should_enforce_session_cooldown("unknown", ""))

        _set_session_cooldown(
            session_key=session_key,
            now=now,
            rate_limits_col=rate_limits,
            cooldown_seconds=30,
        )
        allowed, reason, retry = _check_session_cooldown(
            session_key=session_key,
            now=now + timedelta(seconds=5),
            rate_limits_col=rate_limits,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "session_cooldown")
        self.assertGreater(retry, 0)

    def test_session_cooldown_skips_known_subnet(self):
        subnet = _compute_subnet_key("203.0.113.10")
        self.assertFalse(_should_enforce_session_cooldown(subnet, "203.0.113.10"))

    def test_session_key_stability(self):
        init_data = "user=%7B%22id%22%3A1%7D&auth_date=1700000000"
        init_data_other = "user=%7B%22id%22%3A2%7D&auth_date=1700000000"
        key_a = _derive_session_key(
            init_data_raw=init_data,
            uid=1,
            auth_date="1700000000",
            query_id="query-1",
        )
        key_b = _derive_session_key(
            init_data_raw=init_data,
            uid=1,
            auth_date="1700000000",
            query_id="query-1",
        )
        key_c = _derive_session_key(
            init_data_raw=init_data_other,
            uid=2,
            auth_date="1700000000",
            query_id="query-2",
        )
        self.assertEqual(key_a, key_b)
        self.assertNotEqual(key_a, key_c)

    def test_session_cooldown_payload_shape(self):
        payload = _session_cooldown_payload(12)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["code"], "rate_limited")
        self.assertEqual(payload["reason"], "session_cooldown")
        self.assertEqual(payload["retry_after_sec"], 12)
        self.assertIn("Please try again in", payload["message"])
        
if __name__ == "__main__":
    unittest.main()


class FakeSimpleCollection:
    def __init__(self, docs=None):
        self.docs = []
        self._id = 1
        for doc in docs or []:
            d = dict(doc)
            d.setdefault("_id", self._id)
            self._id += 1
            self.docs.append(d)

    def _match(self, doc, filt):
        for k, v in filt.items():
            if isinstance(v, dict) and "$gte" in v:
                if doc.get(k) < v["$gte"]:
                    return False
                continue
            if isinstance(v, dict) and "$lte" in v:
                if doc.get(k) > v["$lte"]:
                    return False
                continue
            if isinstance(v, dict) and "$in" in v:
                if doc.get(k) not in v["$in"]:
                    return False
                continue
            if doc.get(k) != v:
                return False
        return True

    def find_one(self, filt):
        for d in self.docs:
            if self._match(d, filt):
                return dict(d)
        return None

    def insert_one(self, doc):
        d = dict(doc)
        d.setdefault("_id", self._id)
        self._id += 1
        self.docs.append(d)
        class R:
            inserted_id = d["_id"]
        return R()

    def update_one(self, filt, update, upsert=False):
        for d in self.docs:
            if self._match(d, filt):
                for k, v in update.get("$set", {}).items():
                    d[k] = v
                for k, v in update.get("$inc", {}).items():
                    d[k] = d.get(k, 0) + v
                for k, v in update.get("$unset", {}).items():
                    d.pop(k, None)
                return
        if upsert:
            nd = dict(filt)
            for k, v in update.get("$set", {}).items():
                nd[k] = v
            for k, v in update.get("$setOnInsert", {}).items():
                nd.setdefault(k, v)
            nd["_id"] = self._id
            self._id += 1
            self.docs.append(nd)

    def find_one_and_update(self, filt, update, return_document=None, upsert=False):
        found = None
        for d in self.docs:
            if self._match(d, filt):
                found = d
                break
        if not found and upsert:
            found = dict(filt)
            found["_id"] = self._id
            self._id += 1
            self.docs.append(found)
        if not found:
            return None
        for k, v in update.get("$setOnInsert", {}).items():
            found.setdefault(k, v)
        for k, v in update.get("$set", {}).items():
            found[k] = v
        return dict(found)

    def count_documents(self, filt):
        return sum(1 for d in self.docs if self._match(d, filt))


class UGCRewardTests(unittest.TestCase):
    def test_dedupe_submission_hash(self):
        import vouchers as m
        h1 = m._post_hash("https://instagram.com/p/abc")
        h2 = m._post_hash("https://instagram.com/p/abc")
        self.assertEqual(h1, h2)

    def test_t1_auto_issue_path_writes_ledger(self):
        import vouchers as m
        orig_ledger = m.ugc_reward_ledger_col
        orig_sub = m.ugc_submissions_col
        orig_claim = m.claim_voucher_for_user
        orig_t1 = m.UGC_T1_DROP_ID
        m.ugc_reward_ledger_col = FakeSimpleCollection()
        m.ugc_submissions_col = FakeSimpleCollection([{"_id": "s1", "reward": {}}])
        m.UGC_T1_DROP_ID = "drop-t1"
        m.claim_voucher_for_user = lambda **kwargs: {"code": "C1", "claimedAt": datetime.now(timezone.utc).isoformat()}
        try:
            out = m._issue_small_reward({"_id": "s1", "user_id": 1, "usernameLower": "u", "tier_claimed": "T1"})
            self.assertTrue(out["ok"])
            self.assertEqual(m.ugc_reward_ledger_col.docs[0]["status"], "issued")
        finally:
            m.ugc_reward_ledger_col = orig_ledger
            m.ugc_submissions_col = orig_sub
            m.claim_voucher_for_user = orig_claim
            m.UGC_T1_DROP_ID = orig_t1

    def test_no_codes_left_keeps_approved_not_issued(self):
        import vouchers as m
        orig_ledger = m.ugc_reward_ledger_col
        orig_sub = m.ugc_submissions_col
        orig_claim = m.claim_voucher_for_user
        orig_t1 = m.UGC_T1_DROP_ID
        m.ugc_reward_ledger_col = FakeSimpleCollection()
        m.ugc_submissions_col = FakeSimpleCollection([{"_id": "s2", "reward": {}}])
        m.UGC_T1_DROP_ID = "drop-t1"
        def _raise(**kwargs):
            raise m.NoCodesLeft("sold_out")
        m.claim_voucher_for_user = _raise
        try:
            out = m._issue_small_reward({"_id": "s2", "user_id": 1, "usernameLower": "u", "tier_claimed": "T1"})
            self.assertFalse(out["ok"])
            self.assertEqual(m.ugc_reward_ledger_col.docs[0]["status"], "approved_not_issued")
        finally:
            m.ugc_reward_ledger_col = orig_ledger
            m.ugc_submissions_col = orig_sub
            m.claim_voucher_for_user = orig_claim
            m.UGC_T1_DROP_ID = orig_t1

    def test_t4_gate_calculation(self):
        import vouchers as m
        now = datetime.now(timezone.utc)
        orig_sub = m.ugc_submissions_col
        orig_kpi = m.ugc_user_kpis_col
        docs = []
        for _ in range(15):
            docs.append({"user_id": 9, "status": "validated", "tier_claimed": "T2", "updated_at": now - timedelta(days=1)})
        for _ in range(25):
            docs.append({"user_id": 9, "status": "validated", "tier_claimed": "T1", "updated_at": now - timedelta(days=10)})
        m.ugc_submissions_col = FakeSimpleCollection(docs)
        m.ugc_user_kpis_col = FakeSimpleCollection()
        try:
            kpi = m._compute_user_kpis(9)
            self.assertEqual(kpi["count_validated_t2_last_30d"], 15)
            self.assertEqual(kpi["count_validated_all_last_60d"], 40)
            self.assertTrue(kpi["t4_candidate"])
        finally:
            m.ugc_submissions_col = orig_sub
            m.ugc_user_kpis_col = orig_kpi


class FakeUserCollection:
    def __init__(self, docs=None):
        self.docs = docs or []

    def _match(self, doc, filt):
        for key, value in filt.items():
            if key == "username" and isinstance(value, dict) and "$regex" in value:
                import re

                if not re.match(value["$regex"], str(doc.get("username", "")), re.IGNORECASE):
                    return False
                continue
            if doc.get(key) != value:
                return False
        return True

    def find_one(self, filt, projection=None, sort=None):  # noqa: ARG002
        for doc in self.docs:
            if self._match(doc, filt):
                return dict(doc)
        return None


class FakeReferralEventsCollection:
    def __init__(self, docs=None):
        self.docs = docs or []

    def count_documents(self, filt, limit=0):
        count = 0
        for doc in self.docs:
            ok = True
            for key, value in filt.items():
                if doc.get(key) != value:
                    ok = False
                    break
            if ok:
                count += 1
                if limit and count >= limit:
                    return count
        return count


class FakeDbForReferralProgress:
    def __init__(self, referral_events_col):
        self._referral_events_col = referral_events_col

    def __getitem__(self, name):
        if name == "referral_events":
            return self._referral_events_col
        raise KeyError(name)


class ReferralProgressTests(unittest.TestCase):
    def setUp(self):
        import vouchers as m

        self.m = m
        self.orig_users = m.users_collection
        self.orig_db = m.db
        self.orig_extract = m.extract_raw_init_data_from_query
        self.orig_verify = m.verify_telegram_init_data
        self.orig_ctx = m._user_ctx_or_preview
        self.orig_invite_link_map = m.invite_link_map_collection
        self.app = Flask(__name__)

        m.extract_raw_init_data_from_query = lambda req: "init"
        m.verify_telegram_init_data = lambda raw: (True, {}, None)
        m._user_ctx_or_preview = lambda req, init_data_raw, verification: (
            {"user": {"id": 123, "username": "alice"}},
            False,
        )
        m.invite_link_map_collection = FakeUserCollection([])

    def tearDown(self):
        self.m.users_collection = self.orig_users
        self.m.db = self.orig_db
        self.m.extract_raw_init_data_from_query = self.orig_extract
        self.m.verify_telegram_init_data = self.orig_verify
        self.m._user_ctx_or_preview = self.orig_ctx
        self.m.invite_link_map_collection = self.orig_invite_link_map

    def test_referral_progress_uses_ledger_when_snapshot_zero(self):
        self.m.users_collection = FakeUserCollection(
            [{"user_id": 123, "total_referrals": 0, "weekly_referrals": 0, "monthly_referrals": 0}]
        )
        now = datetime.now(timezone.utc)
        week_key = self.m._week_key_kl(now)
        month_key = self.m._month_key_kl(now)
        self.m.db = FakeDbForReferralProgress(
            FakeReferralEventsCollection(
            [
                {"inviter_id": 123, "event": "referral_settled", "week_key": week_key, "month_key": month_key},
                {"inviter_id": 123, "event": "referral_settled", "week_key": week_key, "month_key": month_key},
            ]
            )
        )
        with self.app.test_request_context("/v2/miniapp/referral/progress"):
            resp, status = self.m.api_referral_progress()
        self.assertEqual(status, 200)
        body = resp.get_json()
        self.assertEqual(body["total_referrals"], 2)

    def test_referral_progress_prefers_snapshot_when_present(self):
        self.m.users_collection = FakeUserCollection(
            [
                {
                    "user_id": 123,
                    "total_referrals": 4,
                    "weekly_referrals": 2,
                    "monthly_referrals": 3,
                    "snapshot_updated_at": datetime.now(timezone.utc),
                }
            ]
        )
        self.m.db = FakeDbForReferralProgress(FakeReferralEventsCollection([]))
        with self.app.test_request_context("/v2/miniapp/referral/progress"):
            resp, status = self.m.api_referral_progress()
        self.assertEqual(status, 200)
        body = resp.get_json()
        self.assertEqual(body["total_referrals"], 4)
