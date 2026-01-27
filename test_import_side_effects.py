import importlib
import unittest

import database


class ImportSideEffectsTests(unittest.TestCase):
    def setUp(self):
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
