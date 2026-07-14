import unittest
from flask import Flask

import vouchers as vouchers_module
from affiliate_rewards import is_user_blocked_for_self_invite


class FakeReferralAuditCollection:
    def __init__(self, docs):
        self.docs = docs

    def find(self, filt=None):
        uid = (filt or {}).get("invitee_user_id")
        return [d for d in self.docs if d.get("invitee_user_id") == uid]

    def find_one(self, filt, projection=None):
        uid = (filt or {}).get("invitee_user_id")
        or_clauses = (filt or {}).get("$or") or []
        for d in self.docs:
            if d.get("invitee_user_id") != uid:
                continue
            for clause in or_clauses:
                if all(d.get(k) == v for k, v in clause.items()):
                    return d
        return None


class FakeDb:
    def __init__(self, referral_audit_docs):
        self.referral_audit = FakeReferralAuditCollection(referral_audit_docs)


class SelfInviteGuardTests(unittest.TestCase):
    def test_is_user_blocked_for_self_invite_detects_matching_ids(self):
        db = FakeDb([{"invitee_user_id": 1, "inviter_user_id": 1, "status": "skipped", "reason": "self_invite"}])
        self.assertTrue(is_user_blocked_for_self_invite(db, 1))

    def test_is_user_blocked_for_self_invite_detects_reason_only(self):
        db = FakeDb([{"invitee_user_id": 2, "inviter_user_id": 9, "status": "skipped", "reason": "self_invite"}])
        self.assertTrue(is_user_blocked_for_self_invite(db, 2))

    def test_is_user_blocked_for_self_invite_false_for_valid_invitee(self):
        db = FakeDb([{"invitee_user_id": 3, "inviter_user_id": 5, "status": "confirmed", "reason": "qualified"}])
        self.assertFalse(is_user_blocked_for_self_invite(db, 3))

    def test_is_user_blocked_for_self_invite_false_for_organic_user(self):
        db = FakeDb([])
        self.assertFalse(is_user_blocked_for_self_invite(db, 4))

    def test_welcome_eligibility_denies_claim_for_self_invite_user(self):
        fake_db = FakeDb(
            [{"invitee_user_id": 501, "inviter_user_id": 501, "status": "skipped", "reason": "self_invite"}]
        )
        original_db = vouchers_module.db
        vouchers_module.db = fake_db
        app = Flask(__name__)
        try:
            with app.app_context():
                allowed, reason, ticket = vouchers_module.welcome_eligibility(501)
        finally:
            vouchers_module.db = original_db
        self.assertFalse(allowed)
        self.assertEqual(reason, "self_invite_blocked")
        self.assertIsNone(ticket)


if __name__ == "__main__":
    unittest.main()
