import importlib
import unittest

import database


class ImportSideEffectsTests(unittest.TestCase):
    def setUp(self):
        # Simulate a process that never called init_db(), then restore the
        # handle: leaking ``_db = None`` breaks every later module that relies
        # on a db initialised at import time (seen as "Database not
        # initialized" in test_weekly_leaderboard_archive_recovery.py).
        self._saved_db = database._db
        self.addCleanup(setattr, database, "_db", self._saved_db)
        database._db = None

    def test_high_risk_modules_import_without_db_init(self):
        for module_name in ("referral", "checkin", "bot_joins", "patch_monthly_xp"):
            with self.subTest(module=module_name):
                if module_name in importlib.sys.modules:
                    importlib.reload(importlib.sys.modules[module_name])
                else:
                    importlib.import_module(module_name)


if __name__ == "__main__":
    unittest.main()
