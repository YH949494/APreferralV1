/**
 * Regression tests for the Platform Finder ("Find Where To Play") strings
 * added to static/i18n.js's I18N dictionary, and for tFor()'s ability to
 * look up a caller-supplied language independent of window.currentLanguage.
 *
 * static/i18n.js is a standalone script (not inlined in index.html), so it
 * is loaded directly into a sandboxed vm context with minimal DOM/storage
 * stubs, mirroring test_event_banner_frontend.test.js's approach.
 *
 * Run with: node --test test_platform_finder_i18n.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const I18N_PATH = path.join(__dirname, "static", "i18n.js");

function loadI18nContext({ storedLang = null, tgLangCode = null } = {}) {
  const fakeEl = () => ({
    textContent: "",
    appendChild() {},
    setAttribute() {},
    classList: { toggle() {} },
  });

  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    document: {
      title: "",
      readyState: "complete",
      head: { appendChild() {} },
      createElement: () => fakeEl(),
      querySelectorAll: () => [],
      addEventListener() {},
    },
    window: {
      Telegram: tgLangCode ? { WebApp: { initDataUnsafe: { user: { language_code: tgLangCode } } } } : undefined,
    },
    localStorage: {
      getItem: (key) => (key === "ap_language" ? storedLang : null),
      setItem() {},
    },
    CustomEvent: class {
      constructor(name, opts) {
        this.name = name;
        this.detail = opts && opts.detail;
      }
    },
  };
  sandbox.window.dispatchEvent = () => {};

  const context = vm.createContext(sandbox);
  const source = fs.readFileSync(I18N_PATH, "utf8");
  vm.runInContext(source, context, { filename: "i18n.js" });
  return context;
}

test("platform_finder_* keys exist for en/th/id and are reachable via tFor()", () => {
  const ctx = loadI18nContext();
  const keys = [
    "platform_finder_cta",
    "platform_finder_title",
    "platform_finder_subtitle",
    "platform_finder_step1_label",
    "platform_finder_step1_text",
    "platform_finder_step2_label",
    "platform_finder_step2_text",
    "platform_finder_step3_label",
    "platform_finder_step3_text",
    "platform_finder_copy_btn",
    "platform_finder_help_btn",
    "platform_finder_fallback",
  ];
  for (const lang of ["en", "th", "id"]) {
    for (const key of keys) {
      const value = ctx.window.tFor(lang, key);
      assert.notEqual(value, key, `${lang}.${key} should resolve to real copy, not fall through to the key itself`);
    }
  }
});

test("expected localized copy for MY(en)/TH(th)/ID(id)", () => {
  const ctx = loadI18nContext();
  assert.equal(ctx.window.tFor("en", "platform_finder_title"), "Voucher secured ✅");
  assert.equal(ctx.window.tFor("th", "platform_finder_title"), "รับคูปองเรียบร้อยแล้ว ✅");
  assert.equal(ctx.window.tFor("id", "platform_finder_title"), "Voucher berhasil diklaim ✅");
});

test('"AdvantPlay Slots" search term is never translated across languages', () => {
  const ctx = loadI18nContext();
  for (const lang of ["en", "th", "id"]) {
    assert.match(ctx.window.tFor(lang, "platform_finder_step1_text"), /AdvantPlay Slots/);
    assert.match(ctx.window.tFor(lang, "platform_finder_copy_btn"), /AdvantPlay Slots/);
    assert.doesNotMatch(ctx.window.tFor(lang, "platform_finder_step1_text"), /AdvantPlay\s+(Slot Mesin|สล็อต|slot)\b/i);
  }
});

test("tFor(lang, key) ignores window.currentLanguage (pins to the caller's language)", () => {
  const ctx = loadI18nContext({ tgLangCode: "th" });
  assert.equal(ctx.window.currentLanguage, "th", "sanity: general Mini App language detected as Thai");
  assert.equal(ctx.window.tFor("en", "platform_finder_title"), "Voucher secured ✅");
  assert.equal(ctx.window.t("platform_finder_title"), "รับคูปองเรียบร้อยแล้ว ✅");
});

test("unmapped language falls back to English copy", () => {
  const ctx = loadI18nContext();
  assert.equal(ctx.window.tFor("fr", "platform_finder_title"), "Voucher secured ✅");
});
