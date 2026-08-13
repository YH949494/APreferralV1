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
    """The backend payload is intentionally minimal -- {cta_label, language,
    search_term} -- since actual copy now lives in static/i18n.js's I18N
    table (see test_platform_finder_i18n.test.js) rather than being
    duplicated as a second, Python-side string table."""

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

    def test_my_region_payload_language_is_english(self):
        self.assertEqual(_platform_finder_payload("my")["language"], "en")

    def test_th_region_payload_language_is_thai(self):
        self.assertEqual(_platform_finder_payload("th")["language"], "th")

    def test_id_region_payload_language_is_bahasa_indonesia(self):
        self.assertEqual(_platform_finder_payload("id")["language"], "id")

    def test_unknown_region_payload_falls_back_to_english(self):
        for region in (None, "", "ph", "unknown"):
            self.assertEqual(_platform_finder_payload(region)["language"], "en")


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
