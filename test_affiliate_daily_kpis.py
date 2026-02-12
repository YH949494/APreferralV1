import unittest
from datetime import datetime, timedelta, timezone

from scheduler import compute_affiliate_daily_kpi


class FakeCollection:
    def __init__(self):
        self.docs = []

    def create_index(self, *args, **kwargs):
        return None

    def _match_value(self, value, cond):
        if isinstance(cond, dict):
            for op, expected in cond.items():
                if op == "$gte" and not (value is not None and value >= expected):
                    return False
                if op == "$lt" and not (value is not None and value < expected):
                    return False
                if op == "$lte" and not (value is not None and value <= expected):
                    return False
                if op == "$ne" and value == expected:
                    return False
            return True
        return value == cond

    def _match(self, doc, filt):
        for key, cond in (filt or {}).items():
            if not self._match_value(doc.get(key), cond):
                return False
        return True

    def find(self, filt=None, proj=None):
        rows = [d for d in self.docs if self._match(d, filt or {})]
        if proj:
            return [{k: r.get(k) for k in proj.keys()} for r in rows]
        return [dict(r) for r in rows]

    def find_one(self, filt=None, proj=None):
        for d in self.docs:
            if self._match(d, filt or {}):
                if proj:
                    return {k: d.get(k) for k in proj.keys()}
                return dict(d)
        return None

    def count_documents(self, filt):
        return sum(1 for d in self.docs if self._match(d, filt or {}))

    def update_one(self, filt, update, upsert=False):
        for d in self.docs:
            if self._match(d, filt):
                d.update(update.get("$set", {}))
                return
        if upsert:
            row = dict(filt)
            row.update(update.get("$set", {}))
            self.docs.append(row)

    def distinct(self, key, filt=None):
        vals = []
        for d in self.docs:
            if self._match(d, filt or {}):
                val = d.get(key)
                if val not in vals:
                    vals.append(val)
        return vals


class FakeDb:
    def __init__(self):
        self.pending_referrals = FakeCollection()
        self.qualified_events = FakeCollection()
        self.users = FakeCollection()
        self.new_joiner_claims = FakeCollection()
        self.affiliate_daily_kpis = FakeCollection()


class AffiliateDailyKpiTests(unittest.TestCase):
    def test_compute_daily_snapshot(self):
        db = FakeDb()
        day_start = datetime(2026, 1, 10, tzinfo=timezone.utc)
        day_utc = day_start.date().isoformat()

        db.pending_referrals.docs.append(
            {
                "invitee_user_id": 777,
                "inviter_user_id": 123,
                "status": "pending",
                "created_at_utc": day_start,
            }
        )
        db.users.docs.append(
            {
                "user_id": 777,
                "first_checkin_at": day_start + timedelta(hours=24),
            }
        )
        db.new_joiner_claims.docs.append(
            {
                "uid": 777,
                "claimed_at": day_start + timedelta(hours=12),
            }
        )
        db.qualified_events.docs.append(
            {
                "invitee_id": 777,
                "referrer_id": 123,
                "qualified_at": day_start + timedelta(hours=2),
            }
        )

        out = compute_affiliate_daily_kpi(day_utc, db_ref=db, now_utc_ts=day_start + timedelta(days=1))

        self.assertEqual(out["day_utc"], day_utc)
        self.assertEqual(out["new_referrals"], 1)
        self.assertEqual(out["qualified"], 1)
        self.assertEqual(out["checkin_72h_rate"], 1.0)
        self.assertEqual(out["claim_proxy_72h_rate"], 1.0)
        self.assertEqual(out["new_referrals_7d"], 1)
        self.assertEqual(out["qualified_7d"], 1)
        self.assertEqual(out["quality_rate_7d"], 1.0)
        self.assertEqual(out["active_referrers_7d"], 1)


if __name__ == "__main__":
    unittest.main()
