import unittest
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

import marketing_upload as mu


class FakeInsertManyResult:
    def __init__(self, inserted_ids):
        self.inserted_ids = inserted_ids


class FakeMarketingCollection:
    def __init__(self):
        self.docs = []
        self._next_id = 1

    def distinct(self, field, filt):
        keys = filt.get(field, {}).get("$in", [])
        existing = {d[field] for d in self.docs if field in d}
        return [k for k in keys if k in existing]

    def insert_many(self, docs, ordered=False):  # noqa: ARG002
        inserted_ids = []
        for doc in docs:
            existing_keys = {d.get("dedupe_key") for d in self.docs}
            if doc.get("dedupe_key") in existing_keys:
                raise DuplicateKeyError("duplicate key")
            doc = dict(doc)
            doc["_id"] = self._next_id
            self._next_id += 1
            self.docs.append(doc)
            inserted_ids.append(doc["_id"])
        return FakeInsertManyResult(inserted_ids)


class FakeBatchesCollection:
    def __init__(self):
        self.docs = []

    def insert_one(self, doc):
        self.docs.append(dict(doc))

    def find(self, filt):  # noqa: ARG002
        return list(self.docs)


def _sorted_history(docs):
    return sorted(docs, key=lambda d: d.get("uploaded_at"), reverse=True)


class FakeBatchesCollectionWithSort(FakeBatchesCollection):
    def find(self, filt):  # noqa: ARG002
        return self


def _csv_bytes(rows, headers=None):
    headers = headers or ["campaign_id", "campaign_name", "account", "coupon_code"]
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(str(row.get(h, "")) for h in headers))
    return ("\n".join(lines)).encode("utf-8")


NOW = datetime(2024, 5, 15, tzinfo=timezone.utc)  # ISO week 2024-W20


class CsvImportTests(unittest.TestCase):
    def test_basic_csv_import(self):
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1"},
            {"campaign_id": "c2", "campaign_name": "Camp 2", "account": "acc2", "coupon_code": "X2"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_total"], 2)
        self.assertEqual(summary["rows_imported"], 2)
        self.assertEqual(summary["rows_failed"], 0)
        self.assertEqual(summary["duplicate_rows"], 0)
        self.assertEqual(summary["snapshot_week"], "2024-W20")
        self.assertEqual(summary["snapshot_month"], "2024-05")
        self.assertEqual(len(marketing_col.docs), 2)
        self.assertEqual(len(batches_col.docs), 1)

    def test_redeem_time_drives_mixed_snapshot_periods(self):
        content = _csv_bytes(
            [
                {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1", "coupon_redeem_time": "2024-03-18 10:00:00"},
                {"campaign_id": "c2", "campaign_name": "Camp 2", "account": "acc2", "coupon_code": "X2", "coupon_redeem_time": "2024-04-02 11:00:00"},
                {"campaign_id": "c3", "campaign_name": "Camp 3", "account": "acc3", "coupon_code": "X3", "coupon_redeem_time": "2024-05-15 12:00:00"},
            ],
            headers=["campaign_id", "campaign_name", "account", "coupon_code", "coupon_redeem_time"],
        )
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="historical.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertIsNone(summary["snapshot_week"])
        self.assertIsNone(summary["snapshot_month"])
        self.assertEqual(summary["period_source"], "coupon_redeem_time")
        self.assertEqual(summary["rows_by_snapshot_month"], {"2024-03": 1, "2024-04": 1, "2024-05": 1})
        self.assertIn("2024-W12", summary["rows_by_snapshot_week"])
        self.assertEqual({d["period_source"] for d in marketing_col.docs}, {"coupon_redeem_time"})
        self.assertTrue(all(d.get("source_redeem_time") is not None for d in marketing_col.docs))

    def test_missing_redeem_time_uses_manual_period_before_upload_time(self):
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="manual.csv", uploaded_by="admin",
            now=NOW, manual_period="2024-04", marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["snapshot_month"], "2024-04")
        self.assertEqual(summary["period_source"], "manual_period")
        self.assertEqual(marketing_col.docs[0]["period_source"], "manual_period")

    def test_extra_columns_are_stored_verbatim(self):
        content = _csv_bytes(
            [{"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1", "withdraw_amount": "50"}],
            headers=["campaign_id", "campaign_name", "account", "coupon_code", "withdraw_amount"],
        )
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(marketing_col.docs[0]["withdraw_amount"], "50")

    def test_missing_required_columns_rejected(self):
        content = _csv_bytes([{"campaign_id": "c1"}], headers=["campaign_id"])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertFalse(summary["ok"])
        self.assertIn("missing required columns", summary["error"])
        self.assertEqual(len(marketing_col.docs), 0)
        self.assertEqual(len(batches_col.docs), 0)

    def test_rows_with_missing_required_values_are_failed(self):
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "", "coupon_code": "X1"},
            {"campaign_id": "c2", "campaign_name": "Camp 2", "account": "acc2", "coupon_code": "X2"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_imported"], 1)
        self.assertEqual(summary["rows_failed"], 1)

    def test_unsupported_file_type_rejected(self):
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=b"abc", file_name="weekly.txt", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertFalse(summary["ok"])
        self.assertIn("unsupported file type", summary["error"])

    def test_uppercase_headers_are_matched_case_insensitively(self):
        content = _csv_bytes(
            [{"CAMPAIGN_ID": "c1", "CAMPAIGN_NAME": "Camp 1", "ACCOUNT": "acc1", "COUPON_CODE": "X1"}],
            headers=["CAMPAIGN_ID", "CAMPAIGN_NAME", "ACCOUNT", "COUPON_CODE"],
        )
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_imported"], 1)
        self.assertEqual(summary["rows_failed"], 0)

    def test_oversized_file_rejected(self):
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        big_content = b"x" * (mu.MAX_FILE_SIZE_BYTES + 1)
        summary = mu.ingest_upload(
            content=big_content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertFalse(summary["ok"])
        self.assertIn("exceeds maximum size", summary["error"])


class XlsxImportTests(unittest.TestCase):
    def _xlsx_bytes(self, headers, rows):
        import io

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(headers)
        for row in rows:
            ws.append(row)
        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()

    def test_basic_xlsx_import(self):
        content = self._xlsx_bytes(
            ["campaign_id", "campaign_name", "account", "coupon_code"],
            [["c1", "Camp 1", "acc1", "X1"], ["c2", "Camp 2", "acc2", "X2"]],
        )
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="weekly.xlsx", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_imported"], 2)


class DuplicatePreventionTests(unittest.TestCase):
    def test_reuploading_same_file_same_week_does_not_duplicate(self):
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        first = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        second = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(first["rows_imported"], 1)
        self.assertEqual(second["rows_imported"], 0)
        self.assertEqual(second["duplicate_rows"], 1)
        self.assertEqual(len(marketing_col.docs), 1)

    def test_duplicate_rows_within_same_batch_are_not_imported_twice(self):
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1"},
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_imported"], 1)
        self.assertEqual(summary["duplicate_rows"], 1)

    def test_different_week_creates_separate_snapshot(self):
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        first = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        next_week = datetime(2024, 5, 22, tzinfo=timezone.utc)
        second = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=next_week, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(second["rows_imported"], 1)
        self.assertEqual(second["duplicate_rows"], 0)
        self.assertEqual(len(marketing_col.docs), 2)


class DedupeLookupChunkingTests(unittest.TestCase):
    def test_large_batch_chunks_existing_key_lookup(self):
        rows = [
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc%d" % i, "coupon_code": "X%d" % i}
            for i in range(mu._DEDUPE_LOOKUP_CHUNK_SIZE + 10)
        ]
        content = _csv_bytes(rows)
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        calls = []
        original_distinct = marketing_col.distinct

        def counting_distinct(field, filt):
            calls.append(len(filt.get(field, {}).get("$in", [])))
            return original_distinct(field, filt)

        marketing_col.distinct = counting_distinct
        summary = mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_imported"], mu._DEDUPE_LOOKUP_CHUNK_SIZE + 10)
        self.assertGreater(len(calls), 1)
        for n in calls:
            self.assertLessEqual(n, mu._DEDUPE_LOOKUP_CHUNK_SIZE)


class UploadBatchTests(unittest.TestCase):
    def test_upload_batch_doc_created_with_expected_fields(self):
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "Camp 1", "account": "acc1", "coupon_code": "X1"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        mu.ingest_upload(
            content=content, file_name="weekly.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertEqual(len(batches_col.docs), 1)
        batch = batches_col.docs[0]
        for field in ("upload_batch_id", "file_name", "snapshot_week", "snapshot_month",
                      "rows_total", "rows_imported", "rows_failed", "duplicate_rows",
                      "uploaded_by", "uploaded_at", "status"):
            self.assertIn(field, batch)
        self.assertEqual(batch["status"], "completed")


class UploadHistoryTests(unittest.TestCase):
    def test_get_upload_history_returns_batches(self):
        class SortableBatches:
            def __init__(self, docs):
                self.docs = docs

            def find(self, filt):  # noqa: ARG002
                return self

            def sort(self, field, direction):
                self.docs = sorted(self.docs, key=lambda d: d[field], reverse=(direction < 0))
                return self

            def limit(self, n):
                return self.docs[:n]

        docs = [
            {"upload_batch_id": "a", "uploaded_at": datetime(2024, 5, 1, tzinfo=timezone.utc)},
            {"upload_batch_id": "b", "uploaded_at": datetime(2024, 5, 8, tzinfo=timezone.utc)},
        ]
        batches_col = SortableBatches(docs)
        history = mu.get_upload_history(batches_col=batches_col, limit=10)
        self.assertEqual(history[0]["upload_batch_id"], "b")
        self.assertEqual(history[1]["upload_batch_id"], "a")


class HeaderNormalisationTests(unittest.TestCase):
    """Spaced, hyphenated, and mixed-case redeem-time headers must all be detected."""

    def _run(self, header_name: str, date_value: str = "2024-03-18 10:00:00"):
        """Upload a single row with the given header name for the redeem time column."""
        headers = ["campaign_id", "campaign_name", "account", "coupon_code", header_name]
        content = _csv_bytes(
            [{"campaign_id": "c1", "campaign_name": "Camp", "account": "u1",
              "coupon_code": "X1", header_name: date_value}],
            headers=headers,
        )
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="test.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        return summary, marketing_col

    def test_spaced_header_coupon_redeem_time(self):
        """'coupon redeem time' (spaces) must be treated as coupon_redeem_time."""
        summary, marketing_col = self._run("coupon redeem time")
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["period_source"], "coupon_redeem_time")
        self.assertEqual(summary["rows_by_snapshot_month"], {"2024-03": 1})
        self.assertEqual(summary["detected_redeem_time_column"], "coupon_redeem_time")
        self.assertIsNone(summary["redeem_time_column_warning"])
        self.assertEqual(marketing_col.docs[0]["period_source"], "coupon_redeem_time")

    def test_title_case_header(self):
        """'Coupon Redeem Time' (title case) must be detected."""
        summary, _ = self._run("Coupon Redeem Time")
        self.assertEqual(summary["period_source"], "coupon_redeem_time")
        self.assertEqual(summary["detected_redeem_time_column"], "coupon_redeem_time")

    def test_hyphenated_header(self):
        """'coupon-redeem-time' (hyphens) must be detected."""
        summary, _ = self._run("coupon-redeem-time")
        self.assertEqual(summary["period_source"], "coupon_redeem_time")
        self.assertEqual(summary["detected_redeem_time_column"], "coupon_redeem_time")

    def test_underscored_header_still_works(self):
        """Original 'coupon_redeem_time' (underscores) must still work."""
        summary, _ = self._run("coupon_redeem_time")
        self.assertEqual(summary["period_source"], "coupon_redeem_time")
        self.assertEqual(summary["detected_redeem_time_column"], "coupon_redeem_time")

    def test_march_april_may_june_spaced_header(self):
        """Full historical dataset with 'coupon redeem time' header must produce
        separate snapshot_month values for March, April, May, June."""
        headers = ["campaign_id", "campaign_name", "account", "coupon_code", "coupon redeem time"]
        rows = [
            {"campaign_id": "c1", "campaign_name": "C", "account": f"u{i}",
             "coupon_code": f"X{i}", "coupon redeem time": date}
            for i, date in enumerate([
                "2024-03-05 09:00:00",
                "2024-04-12 10:00:00",
                "2024-05-20 11:00:00",
                "2024-06-03 12:00:00",
            ])
        ]
        content = _csv_bytes(rows, headers=headers)
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="historical.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["period_source"], "coupon_redeem_time")
        months = summary["rows_by_snapshot_month"]
        self.assertIn("2024-03", months)
        self.assertIn("2024-04", months)
        self.assertIn("2024-05", months)
        self.assertIn("2024-06", months)
        # Must have multiple distinct weeks — not all in one week
        self.assertGreater(len(summary["rows_by_snapshot_week"]), 1)
        self.assertIsNone(summary["redeem_time_column_warning"])
        self.assertEqual(summary["detected_redeem_time_column"], "coupon_redeem_time")

    def test_no_redeem_time_column_shows_warning(self):
        """When no redeem time column is present, warning must be set."""
        content = _csv_bytes([
            {"campaign_id": "c1", "campaign_name": "C", "account": "u1", "coupon_code": "X1"},
        ])
        marketing_col = FakeMarketingCollection()
        batches_col = FakeBatchesCollection()
        summary = mu.ingest_upload(
            content=content, file_name="no_redeem.csv", uploaded_by="admin",
            now=NOW, marketing_col=marketing_col, batches_col=batches_col,
        )
        self.assertTrue(summary["ok"])
        self.assertIsNone(summary["detected_redeem_time_column"])
        self.assertIsNotNone(summary["redeem_time_column_warning"])
        self.assertIn("upload_time fallback", summary["redeem_time_column_warning"])
        self.assertEqual(summary["period_source"], "upload_time")


if __name__ == "__main__":
    unittest.main()
