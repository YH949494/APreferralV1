from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch

import affiliate_dashboard_export as mod


class _Cursor(list):
    def sort(self, *args, **kwargs):
        return self

    def batch_size(self, n):
        self._batch = n
        return self


class _Collection:
    def __init__(self, docs):
        self.docs = docs
        self.find_args = None

    def find(self, q, p):
        self.find_args = (q, p)
        return _Cursor(self.docs)


class _DB(dict):
    def __init__(self, docs):
        super().__init__()
        self['affiliate_ledger'] = _Collection(docs)
        self.export_state = self
        self.saved = None

    def find_one(self, q):
        return {}

    def update_one(self, q, u, upsert=False):
        self.saved = (q, u, upsert)


class T(TestCase):
    def test_build_rows_and_code_canonical(self):
        row = {"_id": 1, "status": "ISSUED", "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc), "voucher": {"code": "ABC"}}
        out = mod.build_welcome_rows([row], export_run_id='r', exported_at=datetime(2026, 1, 3, tzinfo=timezone.utc))[0]
        self.assertEqual(out['voucher_code'], 'ABC')
        self.assertEqual(out['coupon_code'], 'ABC')
        self.assertEqual(out['code'], 'ABC')

    def test_referral_derive_and_dedup(self):
        w = [{"export_run_id": "r", "exported_at": "x", "ledger_id": "1", "ledger_status": "APPROVED", "inviter_user_id": 1, "invitee_user_id": 2, "voucher_code": "C", "code": "C", "year_month": "202601", "created_at": "a", "issued_at": "b"},
             {"export_run_id": "r", "exported_at": "x", "ledger_id": "2", "ledger_status": "APPROVED", "inviter_user_id": 1, "invitee_user_id": 2, "voucher_code": "C", "code": "C", "year_month": "202601", "created_at": "a", "issued_at": "b"}]
        rows = mod.build_referral_rows(w)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['referral_status'], 'qualified')

    def test_status_mapping(self):
        self.assertEqual(mod._referral_status('PENDING_REVIEW'), 'pending')
        self.assertEqual(mod._referral_status('FAILED'), 'rejected')
        self.assertEqual(mod._referral_status('X'), 'unknown')

    def test_year_month_derived(self):
        ym = mod._derive_year_month({"created_at": datetime(2026, 2, 1, tzinfo=timezone.utc)})
        self.assertEqual(ym, '202602')

    @patch('affiliate_dashboard_export.ensure_affiliate_dashboard_indexes')
    def test_query_projection_batch_and_skip_without_checkpoint(self, _idx):
        db = _DB([])
        with patch('affiliate_dashboard_export.os.getenv') as getv:
            getv.side_effect = lambda k, d=None: {'ALLOW_FULL_EXPORT_REBUILD': '0'}.get(k, d)
            out = mod.run_affiliate_dashboard_export(db_ref=db, webhook_url='')
        self.assertEqual(out['status'], 'skipped')

    @patch('affiliate_dashboard_export.ensure_affiliate_dashboard_indexes')
    def test_full_rebuild_limit_and_payload_tabs(self, _idx):
        docs = [{"_id": 1, "status": "ISSUED", "updated_at": datetime.now(timezone.utc), "created_at": datetime.now(timezone.utc), "voucher_code": "A"} for _ in range(3)]
        db = _DB(docs)
        with patch('affiliate_dashboard_export.os.getenv') as getv:
            getv.side_effect = lambda k, d=None: {'ALLOW_FULL_EXPORT_REBUILD': '1', 'REPORTING_MAX_MAJOR_READ_ROWS': '2'}.get(k, d)
            payload = mod.run_affiliate_dashboard_export(db_ref=db, webhook_url='')
        self.assertIn('raw_welcome_code_map', payload['tabs'])
        self.assertIn('raw_referral_map', payload['tabs'])
        self.assertNotIn('raw_coupon_data', payload['tabs'])

    def test_index_setup_safe(self):
        calls = []

        def _safe(coll, keys, name, unique=False, partialFilterExpression=None):
            calls.append((keys, name))
            return False

        with patch('affiliate_dashboard_export.safe_create_index', side_effect=_safe):
            mod.ensure_affiliate_dashboard_indexes(db_ref={'affiliate_ledger': object()})
        self.assertEqual(len(calls), 7)
        self.assertIn(([("status", 1), ("updated_at", 1)], "affiliate_ledger_status_updated_at_idx"), calls)

    def test_previous_completed_month_kl(self):
        now_utc = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(mod._previous_completed_year_month(now_utc, 'Asia/Kuala_Lumpur'), '202605')

    @patch('affiliate_dashboard_export.ensure_affiliate_dashboard_indexes')
    def test_monthly_snapshot_query_and_payload_target_year_month(self, _idx):
        docs = [{"_id": 1, "status": "ISSUED", "year_month": "202605", "updated_at": datetime.now(timezone.utc), "created_at": datetime.now(timezone.utc), "voucher_code": "A"}]
        fake_db = _DB(docs)
        with patch.object(mod, 'db', fake_db), patch('affiliate_dashboard_export.os.getenv') as getv:
            getv.side_effect = lambda k, d=None: {'SCHEDULER_CRON_TIMEZONE': 'Asia/Kuala_Lumpur', 'SHEETS_WEBHOOK_URL': ''}.get(k, d)
            payload = mod.run_affiliate_dashboard_export_monthly_scheduled(now_utc_ts=datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc))
        q, _proj = fake_db['affiliate_ledger'].find_args
        self.assertEqual(q['year_month'], '202605')
        self.assertIn('status', q)
        self.assertEqual(payload['target_year_month'], '202605')
        self.assertNotIn('raw_coupon_data', payload['tabs'])

