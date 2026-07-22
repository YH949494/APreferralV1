/**
 * Tests for the Settings tab categorization fix in static/admin-dashboard.js
 * and static/settings_service.py equivalent (settings_service.py).
 *
 * The Settings module previously pointed 9 of its 12 tabs at the exact same
 * "settings" view, which rendered the complete SETTINGS_SCHEMA (and a static
 * Rejoin Buffer form) unfiltered on every one of them. This suite proves the
 * fix: each tab now filters the schema by a resolved category before
 * rendering, Rejoin Buffer moved under Voucher Rules, and saving still posts
 * the original backend field keys regardless of which subset was rendered.
 *
 * Run with: node --test test_settings_categorization.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { execFileSync } = require("node:child_process");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const JS_SOURCE = fs.readFileSync(JS_PATH, "utf8");

function extractBetween(source, startMarker, endMarker, label) {
  const start = source.indexOf(startMarker);
  assert.ok(start !== -1, `start marker not found for ${label}`);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(end !== -1, `end marker not found for ${label}`);
  return source.slice(start, end);
}

// The MODULES tab config (moduleKey -> tabs[] with settingsCategory).
const MODULES_SRC = extractBetween(
  JS_SOURCE,
  "var MODULES = [",
  "\n  var currentModuleKey = null;",
  "MODULES"
);

// The self-contained "Rejoin Buffer + Managed Settings" block: REJOIN_BUFFER_HTML,
// msFieldId/msRenderField/msRenderJobField/msCollectField/msSaveGroup,
// msFieldCategory, msRenderGroup, loadManagedSettings, LEGACY_SETTINGS_SECTION_*,
// renderSettingsShell, loadSettings.
const SETTINGS_BLOCK_SRC = extractBetween(
  JS_SOURCE,
  "// ---------- Rejoin Buffer admin toggle",
  "// -------------------------------------------------------------------------\n  // CAMPAIGNS VIEW",
  "settings block"
);

function makeFakeElement(id) {
  return {
    id,
    value: "",
    checked: false,
    disabled: false,
    textContent: "",
    innerHTML: "",
    classList: { toggle() {}, add() {}, remove() {} },
    addEventListener(evt, fn) {
      this._listeners = this._listeners || {};
      this._listeners[evt] = this._listeners[evt] || [];
      this._listeners[evt].push(fn);
    },
    click() {
      ((this._listeners && this._listeners.click) || []).forEach((fn) => fn());
    },
  };
}

// Builds a fresh sandbox exposing the extracted source plus stubbed globals
// ($ / api / fetch / toast / btnStart / btnStop / statePanel / emptyState /
// esc / setMeta / renderMeta) so the real filtering/render/save logic runs
// unmodified against a fake DOM + fake network.
function makeSandbox({ apiResponses = {}, fetchResponses = {} } = {}) {
  const dom = new Map();
  // No auto-vivification: only ids that actually exist ("rendered") resolve,
  // exactly like real getElementById — this is what lets msCollectField's
  // "field not present in DOM -> keep old value" fallback be exercised.
  function $(sel) {
    const id = sel.replace(/^#/, "");
    return dom.has(id) ? dom.get(id) : null;
  }
  function registerElement(id) {
    const el = makeFakeElement(id);
    dom.set(id, el);
    return el;
  }
  // Container ids that always exist in the static admin-dashboard.html shell
  // (or are (re)created by renderSettingsShell before any of this code runs).
  ["settings-category-body", "managed-settings-body", "settings-legacy-readonly"].forEach(registerElement);
  function esc(s) {
    return String(s === null || s === undefined ? "" : s).replace(/[&<>"']/g, (c) => (
      { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
    ));
  }
  function emptyState(msg) {
    return '<div class="empty-state-sub">' + esc(typeof msg === "string" ? msg : (msg && msg.sub) || "") + "</div>";
  }
  const fetchCalls = [];
  function fetchStub(url, opts) {
    fetchCalls.push({ url, opts });
    const body = fetchResponses[url] || { success: true, status: "ok" };
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve(body),
    });
  }
  function apiStub(url) {
    if (url in apiResponses) return Promise.resolve(apiResponses[url]);
    return Promise.resolve({});
  }

  const sandbox = {
    $,
    dom,
    registerElement,
    esc,
    emptyState,
    api: apiStub,
    fetch: fetchStub,
    fetchCalls,
    toast() {},
    btnStart() { return true; },
    btnStop() {},
    statePanel() {},
    setMeta() {},
    renderMeta() {},
    currentSettingsCategory: null,
    console,
    Promise,
    Object,
    Array,
    JSON,
    isNaN,
    parseInt,
    parseFloat,
    setTimeout,
  };
  vm.createContext(sandbox);
  vm.runInContext(SETTINGS_BLOCK_SRC, sandbox);
  return sandbox;
}

function loadModules() {
  const sandbox = { console };
  vm.createContext(sandbox);
  vm.runInContext(MODULES_SRC + "\nvar __RESULT__ = MODULES;", sandbox);
  return sandbox.__RESULT__;
}

function settingsTabs() {
  const modules = loadModules();
  const settingsModule = modules.find((m) => m.key === "settings");
  assert.ok(settingsModule, "settings module not found in MODULES");
  return settingsModule.tabs;
}

// ---------------------------------------------------------------------------
// Real production schema, sourced from settings_service.py, so tests exercise
// the actual mapping shipped to the backend rather than a hand-rolled fixture.
// ---------------------------------------------------------------------------
function loadRealSchema() {
  const out = execFileSync(
    "python3",
    ["-c", "import json, settings_service as s; print(json.dumps(s.SETTINGS_SCHEMA))"],
    { cwd: __dirname, encoding: "utf8" }
  );
  return JSON.parse(out);
}

function loadRealCategoryMap() {
  const out = execFileSync(
    "python3",
    ["-c", "import json, settings_service as s; print(json.dumps(s.category_map()))"],
    { cwd: __dirname, encoding: "utf8" }
  );
  return JSON.parse(out);
}

// ---------------------------------------------------------------------------
// Tab configuration
// ---------------------------------------------------------------------------

test("Settings module tabs each declare exactly one settingsCategory (or none for action-only tabs)", () => {
  const tabs = settingsTabs();
  const byLabel = {};
  tabs.forEach((t) => { byLabel[t.label] = t; });

  assert.equal(byLabel["General"].settingsCategory, "general");
  assert.equal(byLabel["Feature Flags"].settingsCategory, "feature_flags");
  assert.equal(byLabel["Rewards"].settingsCategory, "rewards");
  assert.equal(byLabel["Voucher Rules"].settingsCategory, "voucher_rules");
  assert.equal(byLabel["Referral"].settingsCategory, "referral");
  assert.equal(byLabel["Affiliate"].settingsCategory, "affiliate");
  assert.equal(byLabel["Welcome Journey"].settingsCategory, "welcome_journey");
  assert.equal(byLabel["Reactivation"].settingsCategory, "reactivation");
  assert.equal(byLabel["Security"].settingsCategory, "security");
  assert.equal(byLabel["Integrations"].settingsCategory, "integrations");
  assert.equal(byLabel["Segment Probability"].settingsCategory, "segment_probability");
});

test("no two Settings-module tabs share the same settingsCategory", () => {
  const tabs = settingsTabs();
  const categories = tabs.map((t) => t.settingsCategory).filter(Boolean);
  const unique = new Set(categories);
  assert.equal(unique.size, categories.length, "duplicate settingsCategory across Settings tabs");
});

// ---------------------------------------------------------------------------
// General does not contain Rejoin Buffer / Voucher Rules does
// ---------------------------------------------------------------------------

test("General tab shell does not render the Rejoin Buffer form", () => {
  const sandbox = makeSandbox();
  vm.runInContext('renderSettingsShell("general");', sandbox);
  const body = sandbox.$("#settings-category-body").innerHTML;
  assert.ok(!body.includes('id="rb-mode"'), "General tab must not contain Rejoin Buffer fields");
  assert.ok(!body.includes("Rejoin Buffer"));
});

test("Voucher Rules tab shell renders the Rejoin Buffer form", () => {
  const sandbox = makeSandbox();
  vm.runInContext('renderSettingsShell("voucher_rules");', sandbox);
  const body = sandbox.$("#settings-category-body").innerHTML;
  assert.ok(body.includes('id="rb-mode"'), "Voucher Rules tab must contain the Rejoin Buffer mode field");
  assert.ok(body.includes("Rejoin Buffer"));
});

// ---------------------------------------------------------------------------
// msFieldCategory resolution (group-level + per-field overrides)
// ---------------------------------------------------------------------------

test("msFieldCategory: group-level category applies when no override exists", () => {
  const sandbox = makeSandbox();
  const schema = { category: "security", fields: { foo: {} } };
  sandbox.__schema = schema;
  const result = vm.runInContext('msFieldCategory(__schema, "foo")', sandbox);
  assert.equal(result, "security");
});

test("msFieldCategory: per-field override wins over group category", () => {
  const sandbox = makeSandbox();
  const schema = {
    category: "general",
    field_categories: { affiliate_group_invite_url: "affiliate" },
    fields: { affiliate_group_invite_url: {}, official_channel_url: {} },
  };
  sandbox.__schema = schema;
  assert.equal(
    vm.runInContext('msFieldCategory(__schema, "affiliate_group_invite_url")', sandbox),
    "affiliate"
  );
  assert.equal(
    vm.runInContext('msFieldCategory(__schema, "official_channel_url")', sandbox),
    "general"
  );
});

// ---------------------------------------------------------------------------
// Changing tabs produces different category-specific content
// ---------------------------------------------------------------------------

const FIXTURE_SCHEMA = {
  abuse_protection: {
    label: "Abuse Protection", category: "security",
    fields: { claim_cooldown_seconds: { type: "int", label: "Claim Cooldown", default: 180 } },
  },
  feature_flags: {
    label: "Feature Flags", category: "feature_flags",
    fields: { affiliate: { type: "bool", label: "Affiliate", default: true } },
  },
  message_templates: {
    label: "Notification Templates",
    field_categories: { affiliate_unlock: "affiliate", referral_near_miss: "referral" },
    fields: {
      affiliate_unlock: { type: "str", label: "Affiliate Unlock", default: "x" },
      referral_near_miss: { type: "str", label: "Referral Near Miss", default: "y" },
    },
  },
};
const FIXTURE_VALUES = {
  abuse_protection: { claim_cooldown_seconds: 180 },
  feature_flags: { affiliate: true },
  message_templates: { affiliate_unlock: "x", referral_near_miss: "y" },
};

test("loadManagedSettings renders different content per category (General vs Security)", async () => {
  const sandbox = makeSandbox({
    apiResponses: { "/api/admin/settings": { schema: FIXTURE_SCHEMA, settings: FIXTURE_VALUES } },
  });
  await vm.runInContext('loadManagedSettings("security")', sandbox);
  const securityBody = sandbox.$("#managed-settings-body").innerHTML;
  assert.ok(securityBody.includes("Claim Cooldown"));
  assert.ok(!securityBody.includes("Affiliate Unlock"));

  await vm.runInContext('loadManagedSettings("general")', sandbox);
  const generalBody = sandbox.$("#managed-settings-body").innerHTML;
  assert.notEqual(generalBody, securityBody);
  assert.ok(!generalBody.includes("Claim Cooldown"));
});

test("loadManagedSettings shows the empty-category message when nothing matches (Rewards)", async () => {
  const sandbox = makeSandbox({
    apiResponses: { "/api/admin/settings": { schema: FIXTURE_SCHEMA, settings: FIXTURE_VALUES } },
  });
  await vm.runInContext('loadManagedSettings("rewards")', sandbox);
  const body = sandbox.$("#managed-settings-body").innerHTML;
  assert.ok(body.includes("No configurable settings are available in this category yet."));
});

test("loadManagedSettings splits a single group across categories via field_categories (Affiliate vs Referral)", async () => {
  const sandbox = makeSandbox({
    apiResponses: { "/api/admin/settings": { schema: FIXTURE_SCHEMA, settings: FIXTURE_VALUES } },
  });
  await vm.runInContext('loadManagedSettings("affiliate")', sandbox);
  const affiliateBody = sandbox.$("#managed-settings-body").innerHTML;
  assert.ok(affiliateBody.includes("Affiliate Unlock"));
  assert.ok(!affiliateBody.includes("Referral Near Miss"));

  await vm.runInContext('loadManagedSettings("referral")', sandbox);
  const referralBody = sandbox.$("#managed-settings-body").innerHTML;
  assert.ok(referralBody.includes("Referral Near Miss"));
  assert.ok(!referralBody.includes("Affiliate Unlock"));
});

// ---------------------------------------------------------------------------
// Saving still writes the original backend key, even when only a subset of a
// group's fields was rendered for the active category.
// ---------------------------------------------------------------------------

test("msSaveGroup posts every original field key to the unchanged backend endpoint, even for fields not rendered in the active tab", async () => {
  const sandbox = makeSandbox({
    fetchResponses: {
      "/api/admin/settings/message_templates": { success: true, settings: { affiliate_unlock: "edited", referral_near_miss: "y" } },
    },
  });
  // Simulate: only the "affiliate" category's field was rendered (its DOM input exists);
  // "referral_near_miss" was never rendered because it belongs to a different tab,
  // exactly as loadManagedSettings(category) would leave the DOM after filtering.
  vm.runInContext(`
    managedSettingsState.schema = ${JSON.stringify(FIXTURE_SCHEMA)};
    managedSettingsState.values = ${JSON.stringify(FIXTURE_VALUES)};
  `, sandbox);
  const renderedInput = sandbox.registerElement("ms-message_templates-affiliate_unlock");
  renderedInput.value = "edited";
  sandbox.registerElement("ms-save-message_templates"); // save button exists (group is always fully savable)

  await vm.runInContext('msSaveGroup("message_templates")', sandbox);

  assert.equal(sandbox.fetchCalls.length, 1);
  const call = sandbox.fetchCalls[0];
  assert.equal(call.url, "/api/admin/settings/message_templates", "must hit the original, unchanged backend endpoint");
  const posted = JSON.parse(call.opts.body).settings;
  assert.deepEqual(
    Object.keys(posted).sort(),
    ["affiliate_unlock", "referral_near_miss"],
    "save payload must still include every original field key for the group"
  );
  assert.equal(posted.affiliate_unlock, "edited", "edited field carries the new value");
  assert.equal(posted.referral_near_miss, "y", "un-rendered field falls back to its last known value, not dropped or renamed");
});

// ---------------------------------------------------------------------------
// Real production schema: exhaustive + unique category coverage, Affiliate
// isolation from unrelated categories.
// ---------------------------------------------------------------------------

test("every real SETTINGS_SCHEMA field has exactly one valid category (python <-> JS parity)", () => {
  const schema = loadRealSchema();
  const pyCategoryMap = loadRealCategoryMap();
  const sandbox = makeSandbox();
  const validCategories = new Set([
    "general", "feature_flags", "xp", "rewards", "voucher_rules", "referral",
    "affiliate", "welcome_journey", "reactivation", "security", "integrations",
    "segment_probability",
  ]);

  Object.keys(schema).forEach((group) => {
    Object.keys(schema[group].fields).forEach((field) => {
      sandbox.__schema = schema[group];
      const jsCategory = vm.runInContext(`msFieldCategory(__schema, ${JSON.stringify(field)})`, sandbox);
      assert.ok(jsCategory, `${group}.${field} has no category (JS resolution)`);
      assert.ok(validCategories.has(jsCategory), `${group}.${field} has unknown category ${jsCategory}`);
      assert.equal(
        jsCategory, pyCategoryMap[`${group}.${field}`],
        `${group}.${field}: JS category (${jsCategory}) must match backend category_map() (${pyCategoryMap[`${group}.${field}`]})`
      );
    });
  });
});

test("Affiliate category excludes unrelated General, Voucher Rules and Security settings (real schema)", () => {
  const pyCategoryMap = loadRealCategoryMap();
  const affiliateKeys = Object.keys(pyCategoryMap).filter((k) => pyCategoryMap[k] === "affiliate");
  assert.ok(affiliateKeys.length > 0, "expected at least one affiliate-categorised field");

  const forbiddenPrefixesByCategory = {
    "abuse_protection.": "security",
    "urls.official_channel_url": "general",
    "urls.community_url": "general",
    "urls.miniapp_url": "general",
    "message_templates.voucher_claimed": "voucher_rules",
  };
  affiliateKeys.forEach((key) => {
    Object.keys(forbiddenPrefixesByCategory).forEach((forbidden) => {
      assert.notEqual(key, forbidden, `${key} is categorised affiliate but should be ${forbiddenPrefixesByCategory[forbidden]}`);
    });
  });
  // And the converse: no security/general/voucher_rules field leaks into affiliate.
  assert.equal(pyCategoryMap["abuse_protection.claim_cooldown_seconds"], "security");
  assert.equal(pyCategoryMap["urls.official_channel_url"], "general");
  assert.equal(pyCategoryMap["message_templates.voucher_claimed"], "voucher_rules");
});

test("General category (real schema) does not include Rejoin Buffer or affiliate/security-only fields", () => {
  const pyCategoryMap = loadRealCategoryMap();
  const generalKeys = Object.keys(pyCategoryMap).filter((k) => pyCategoryMap[k] === "general");
  generalKeys.forEach((key) => {
    assert.notEqual(pyCategoryMap[key], "security");
    assert.notEqual(key, "urls.affiliate_group_invite_url", "affiliate invite URL must not be General");
  });
  assert.equal(pyCategoryMap["urls.affiliate_group_invite_url"], "affiliate");
});
