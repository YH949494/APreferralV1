import unittest

from vouchers import (
    PLATFORM_FINDER_CTA_LABEL,
    PLATFORM_FINDER_SEARCH_TERM,
    _platform_finder_payload,
    resolve_market_language,
)
from campaign_events import EVENT_TYPES


class PlatformFinderLanguageResolutionTests(unittest.TestCase):
    def test_my_region_resolves_to_english(self):
        self.assertEqual(resolve_market_language("my"), "en")
        self.assertEqual(resolve_market_language("MY"), "en")
        self.assertEqual(resolve_market_language("Malaysia"), "en")

    def test_th_region_resolves_to_thai(self):
        self.assertEqual(resolve_market_language("th"), "th")
        self.assertEqual(resolve_market_language("TH"), "th")
        self.assertEqual(resolve_market_language("Thailand"), "th")

    def test_id_region_resolves_to_bahasa_indonesia(self):
        self.assertEqual(resolve_market_language("id"), "id")
        self.assertEqual(resolve_market_language("ID"), "id")
        self.assertEqual(resolve_market_language("Indonesia"), "id")

    def test_unknown_or_missing_region_defaults_to_english(self):
        self.assertEqual(resolve_market_language(None), "en")
        self.assertEqual(resolve_market_language(""), "en")
        self.assertEqual(resolve_market_language("ph"), "en")
        self.assertEqual(resolve_market_language("unknown"), "en")


class PlatformFinderPayloadTests(unittest.TestCase):
    def test_search_term_is_identical_and_untranslated_across_regions(self):
        for region in ("my", "th", "id", None, "unknown"):
            payload = _platform_finder_payload(region)
            self.assertEqual(payload["search_term"], "AdvantPlay Slots")
            self.assertEqual(payload["search_term"], PLATFORM_FINDER_SEARCH_TERM)

    def test_cta_label_is_not_localized(self):
        for region in ("my", "th", "id", None):
            payload = _platform_finder_payload(region)
            self.assertEqual(payload["cta_label"], PLATFORM_FINDER_CTA_LABEL)
            self.assertEqual(payload["cta_label"], "🔎 Find Where To Play")

    def test_my_region_payload_is_english(self):
        payload = _platform_finder_payload("my")
        self.assertEqual(payload["language"], "en")
        self.assertEqual(payload["title"], "Voucher secured ✅")
        self.assertEqual(payload["subtitle"], "Not sure where to use it?")
        self.assertEqual(len(payload["steps"]), 3)
        self.assertIn("AdvantPlay Slots", payload["steps"][0]["text"])
        self.assertEqual(payload["copy_button_label"], '📋 Copy "AdvantPlay Slots"')
        self.assertEqual(payload["help_button_label"], "❓ Can't Find It?")

    def test_th_region_payload_is_thai(self):
        payload = _platform_finder_payload("th")
        self.assertEqual(payload["language"], "th")
        self.assertEqual(payload["title"], "รับคูปองเรียบร้อยแล้ว ✅")
        self.assertEqual(payload["subtitle"], "ไม่แน่ใจว่าต้องใช้ที่ไหน?")
        self.assertIn("AdvantPlay Slots", payload["steps"][0]["text"])
        self.assertEqual(payload["copy_button_label"], '📋 คัดลอก "AdvantPlay Slots"')
        self.assertEqual(payload["help_button_label"], "❓ หาไม่เจอ?")

    def test_id_region_payload_is_bahasa_indonesia(self):
        payload = _platform_finder_payload("id")
        self.assertEqual(payload["language"], "id")
        self.assertEqual(payload["title"], "Voucher berhasil diklaim ✅")
        self.assertEqual(payload["subtitle"], "Belum tahu harus digunakan di mana?")
        self.assertIn("AdvantPlay Slots", payload["steps"][0]["text"])
        self.assertEqual(payload["copy_button_label"], '📋 Salin "AdvantPlay Slots"')
        self.assertEqual(payload["help_button_label"], "❓ Tidak Ketemu?")

    def test_unknown_region_falls_back_to_english_payload(self):
        for region in (None, "", "ph", "unknown"):
            payload = _platform_finder_payload(region)
            self.assertEqual(payload["language"], "en")
            self.assertEqual(payload["title"], "Voucher secured ✅")


class PlatformFinderEventTypesTests(unittest.TestCase):
    def test_platform_finder_event_types_are_registered(self):
        for event_type in (
            "platform_finder_shown",
            "platform_finder_opened",
            "platform_search_copied",
            "platform_finder_help_clicked",
        ):
            self.assertIn(event_type, EVENT_TYPES)


if __name__ == "__main__":
    unittest.main()
