import unittest
from datetime import datetime, timedelta, timezone

from pymongo.errors import DuplicateKeyError

from vouchers import (
    _acquire_claim_lock,
    _apply_kill_success,
    _build_idempotent_claim_response,
    _check_cooldown,
    _check_kill_switch,
    _compute_subnet_key,
    _get_client_ip,
    _set_cooldown,
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
        
if __name__ == "__main__":
    unittest.main()
