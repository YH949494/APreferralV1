import unittest
from datetime import datetime, timedelta, timezone

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
    claim_pooled,
    get_claimable_pools,
    get_visible_pools,
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
        for key, value in update.get("$inc", {}).items():
            doc[key] = doc.get(key, 0) + value
        return FakeUpdateResult(1)


class FakeDb:
    def __init__(self, drops, vouchers):
        self.drops = FakeDropsCollection(drops)
        self.vouchers = FakeVouchersCollection(vouchers)
        
class VoucherAntiHunterTests(unittest.TestCase):
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
