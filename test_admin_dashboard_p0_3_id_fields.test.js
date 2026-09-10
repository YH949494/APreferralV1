/**
 * P0.3: Campaign Centre "select/generate instead of typing a backend id"
 * fields in static/admin-dashboard.js.
 *
 *  - Create Campaign: campaign_id is auto-slugged from the campaign name
 *    (gcSlugify / gcSlugCandidate / gcFirstAvailableSuffix), with collision
 *    suffixing (-2, -3, ...) and a retry loop on the server's 409 responses
 *    (gcCreateCampaignAttempt) since a candidate id can collide with a
 *    tombstoned campaign the admin list never shows.
 *  - Provider select (#gc-c-provider) and Reward Pool campaign select
 *    (#gc-pool-campaign) are populated from the existing
 *    /api/admin/providers and /api/admin/gc-campaigns endpoints
 *    (fetchGcProviders / fetchGcCampaignsList / renderGcProviderSelect /
 *    renderGcPoolCampaignSelect) instead of free-text ids.
 *
 * Mirrors test_campaign_centre_delete_ui.test.js / test_admin_dashboard_p0_2_filters.test.js:
 * the dashboard is one large inline-script-free file with no build step and
 * no jsdom in this repo, so the relevant functions are extracted as text and
 * executed in a sandboxed vm context against a small hand-rolled DOM/fetch
 * stub.
 *
 * Run with: node --test test_admin_dashboard_p0_3_id_fields.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const HTML_PATH = path.join(__dirname, "static", "admin-dashboard.html");
const JS = fs.readFileSync(JS_PATH, "utf8");
const HTML = fs.readFileSync(HTML_PATH, "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

// The slug/select helpers plus the caches they close over, up to (but not
// including) loadGcCampaigns — pulling in the giant loader/renderer bodies
// isn't needed for these tests and they reach for globals ($, api, esc,
// statePanel, emptyState, ...) that aren't stubbed here.
const OPTIONS_SRC = slice(
  JS,
  "  var gcOptionsCache = {",
  "\n  function loadGcCampaigns(force) {"
);

// bindGcCampaigns's create-attempt helper, independent of the DOM-heavy
// bindGcCampaigns() wiring function itself.
const ATTEMPT_SRC = slice(
  JS,
  "  function gcCreateCampaignAttempt(",
  "\n  function bindGcCampaigns() {"
);

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign(
    { console, encodeURIComponent, Object },
    sandboxExtra
  );
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// ---------------------------------------------------------------------
// Minimal <select> stub for renderGcProviderSelect/renderGcPoolCampaignSelect
// ---------------------------------------------------------------------

class FakeSelect {
  constructor() { this._html = ""; this.value = ""; }
  get innerHTML() { return this._html; }
  set innerHTML(v) {
    this._html = v == null ? "" : String(v);
    // A plain regex "parse" of <option value="...">...</option> is enough
    // here — this is a stub, not a real DOM.
    this._options = [...this._html.matchAll(/<option value="([^"]*)">([^<]*)<\/option>/g)]
      .map((m) => ({ value: m[1], label: m[2] }));
    if (!this._options.some((o) => o.value === this.value)) this.value = this._options[0] ? this._options[0].value : "";
  }
  get options() { return this._options || []; }
}

function makeDomStub(elements) {
  return function $(sel) {
    const id = sel.replace(/^#/, "");
    return elements[id] || null;
  };
}

// ---------------------------------------------------------------------
// gcSlugify — deterministic URL-safe slug generation
// ---------------------------------------------------------------------

function loadSlugHelpers() {
  return runInSandbox(OPTIONS_SRC + "\nthis.gcSlugify = gcSlugify; this.gcSlugCandidate = gcSlugCandidate; " +
    "this.gcFirstAvailableSuffix = gcFirstAvailableSuffix; this.gcKnownCampaignIds = gcKnownCampaignIds;", {
    $: makeDomStub({}),
    api: () => ({ then: () => ({ catch: () => {} }) }),
    esc,
  });
}

test("gcSlugify: normal ASCII name", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcSlugify("October Lucky Draw"), "october-lucky-draw");
});

test("gcSlugify: collapses spaces to single hyphens", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcSlugify("Big   Summer   Promo"), "big-summer-promo");
});

test("gcSlugify: collapses repeated punctuation into one hyphen", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcSlugify("Flash!!! Sale -- July"), "flash-sale-july");
});

test("gcSlugify: lowercases mixed case", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcSlugify("VIP Tournament FINALS"), "vip-tournament-finals");
});

test("gcSlugify: strips URL-unsafe characters", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcSlugify("50% Cashback @ Launch!"), "50-cashback-launch");
});

test("gcSlugify: trims leading/trailing separators", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcSlugify("  -- Grand Opening -- "), "grand-opening");
});

test("gcSlugify: name with no valid characters yields an empty slug", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcSlugify("!!! *** ###"), "");
  assert.equal(s.gcSlugify(""), "");
});

// ---------------------------------------------------------------------
// Collision suffixing: base -> -2 -> -3, including tombstoned ids that
// still occupy a slug even though they're excluded from the active list.
// ---------------------------------------------------------------------

test("gcFirstAvailableSuffix: base slug available when nothing known", () => {
  const s = loadSlugHelpers();
  assert.equal(s.gcFirstAvailableSuffix("october-lucky-draw"), 1);
  assert.equal(s.gcSlugCandidate("october-lucky-draw", 1), "october-lucky-draw");
});

test("gcFirstAvailableSuffix: base taken -> suggests -2", () => {
  const s = loadSlugHelpers();
  s.gcKnownCampaignIds["october-lucky-draw"] = true;
  assert.equal(s.gcFirstAvailableSuffix("october-lucky-draw"), 2);
  assert.equal(s.gcSlugCandidate("october-lucky-draw", 2), "october-lucky-draw-2");
});

test("gcFirstAvailableSuffix: base and -2 taken -> suggests -3", () => {
  const s = loadSlugHelpers();
  s.gcKnownCampaignIds["october-lucky-draw"] = true;
  s.gcKnownCampaignIds["october-lucky-draw-2"] = true;
  assert.equal(s.gcFirstAvailableSuffix("october-lucky-draw"), 3);
});

// ---------------------------------------------------------------------
// gcCreateCampaignAttempt: server-side collision retry (covers a
// tombstoned id the client-side pre-check above can't see, since the
// admin list endpoint excludes deleted campaigns).
// ---------------------------------------------------------------------

function makeApiPostJsonStub(responses) {
  const calls = [];
  return {
    calls,
    fn: (urlArg, body) => {
      calls.push({ url: urlArg, body });
      const next = responses.shift();
      return Promise.resolve(next);
    },
  };
}

function loadAttemptSandbox(apiPostJson, toasts) {
  return runInSandbox(OPTIONS_SRC + "\n" + ATTEMPT_SRC + "\nthis.gcCreateCampaignAttempt = gcCreateCampaignAttempt;", {
    $: makeDomStub({}),
    apiPostJson,
    toast: (msg) => toasts.push(msg),
    loadGcCampaigns: () => {},
    Promise,
  });
}

test("gcCreateCampaignAttempt: base slug available on first try", async () => {
  const toasts = [];
  const { calls, fn } = makeApiPostJsonStub([{ ok: true, d: { status: "ok", campaign_id: "october-lucky-draw" } }]);
  const s = loadAttemptSandbox(fn, toasts);
  await s.gcCreateCampaignAttempt({ name: "October Lucky Draw", type: "tournament" }, "october-lucky-draw", 1, null, null);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].body.campaign_id, "october-lucky-draw");
  assert.ok(toasts[0].includes("✅"));
});

test("gcCreateCampaignAttempt: collision on base retries with -2", async () => {
  const toasts = [];
  const { calls, fn } = makeApiPostJsonStub([
    { ok: false, d: { status: "error", code: "duplicate_campaign_id" } },
    { ok: true, d: { status: "ok", campaign_id: "october-lucky-draw-2" } },
  ]);
  const s = loadAttemptSandbox(fn, toasts);
  await s.gcCreateCampaignAttempt({ name: "October Lucky Draw", type: "tournament" }, "october-lucky-draw", 1, null, null);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].body.campaign_id, "october-lucky-draw");
  assert.equal(calls[1].body.campaign_id, "october-lucky-draw-2");
  assert.ok(toasts[toasts.length - 1].includes("october-lucky-draw-2"));
});

test("gcCreateCampaignAttempt: -2 also collides -> retries to -3", async () => {
  const toasts = [];
  const { calls, fn } = makeApiPostJsonStub([
    { ok: false, d: { status: "error", code: "duplicate_campaign_id" } },
    { ok: false, d: { status: "error", code: "campaign_id_previously_deleted" } },
    { ok: true, d: { status: "ok", campaign_id: "october-lucky-draw-3" } },
  ]);
  const s = loadAttemptSandbox(fn, toasts);
  await s.gcCreateCampaignAttempt({ name: "October Lucky Draw", type: "tournament" }, "october-lucky-draw", 1, null, null);
  assert.equal(calls.length, 3);
  assert.deepEqual(calls.map((c) => c.body.campaign_id), [
    "october-lucky-draw", "october-lucky-draw-2", "october-lucky-draw-3",
  ]);
});

test("gcCreateCampaignAttempt: tombstoned id collision never surfaces the raw backend code to the admin", async () => {
  const toasts = [];
  const { fn } = makeApiPostJsonStub([
    { ok: false, d: { status: "error", code: "campaign_id_previously_deleted" } },
    { ok: true, d: { status: "ok", campaign_id: "october-lucky-draw-2" } },
  ]);
  const s = loadAttemptSandbox(fn, toasts);
  await s.gcCreateCampaignAttempt({ name: "October Lucky Draw", type: "tournament" }, "october-lucky-draw", 1, null, null);
  assert.ok(!toasts.some((t) => t.includes("campaign_id_previously_deleted")), "raw backend code must not reach the admin as the main message");
});

test("gcCreateCampaignAttempt: manually-entered id is never auto-suffixed on collision", async () => {
  const toasts = [];
  const { calls, fn } = makeApiPostJsonStub([
    { ok: false, d: { status: "error", code: "duplicate_campaign_id" } },
  ]);
  const s = loadAttemptSandbox(fn, toasts);
  await s.gcCreateCampaignAttempt({ name: "October Lucky Draw", type: "tournament" }, null, 1, "custom-tech-id", null);
  assert.equal(calls.length, 1, "must not retry a manually-chosen id");
  assert.equal(calls[0].body.campaign_id, "custom-tech-id");
  assert.ok(toasts[0].includes("❌"));
  assert.ok(!toasts[0].includes("duplicate_campaign_id"));
});

test("gcCreateCampaignAttempt: gives up and reports a friendly message after too many collisions", async () => {
  const toasts = [];
  const responses = Array.from({ length: 30 }, () => ({ ok: false, d: { status: "error", code: "duplicate_campaign_id" } }));
  const { calls, fn } = makeApiPostJsonStub(responses);
  const s = loadAttemptSandbox(fn, toasts);
  await s.gcCreateCampaignAttempt({ name: "October Lucky Draw", type: "tournament" }, "october-lucky-draw", 1, null, null);
  assert.ok(calls.length <= 25, "must cap retries rather than looping forever");
  assert.ok(toasts[toasts.length - 1].includes("❌"));
  assert.ok(!toasts[toasts.length - 1].includes("duplicate_campaign_id"));
});

test("gcCreateCampaignAttempt: non-collision failure surfaces its own code, not a slug-retry", async () => {
  const toasts = [];
  const { calls, fn } = makeApiPostJsonStub([
    { ok: false, d: { status: "error", code: "missing_name" } },
  ]);
  const s = loadAttemptSandbox(fn, toasts);
  await s.gcCreateCampaignAttempt({ name: "", type: "tournament" }, "october-lucky-draw", 1, null, null);
  assert.equal(calls.length, 1);
  assert.ok(toasts[0].includes("missing_name"));
});

// ---------------------------------------------------------------------
// Provider select: loads /api/admin/providers, human-readable label,
// submitted value is the exact provider_id.
// ---------------------------------------------------------------------

function loadOptionsSandbox(apiImpl) {
  return runInSandbox(OPTIONS_SRC + "\nthis.fetchGcProviders = fetchGcProviders; this.fetchGcCampaignsList = fetchGcCampaignsList; " +
    "this.renderGcProviderSelect = renderGcProviderSelect; this.renderGcPoolCampaignSelect = renderGcPoolCampaignSelect; " +
    "this.loadGcProviderSelect = loadGcProviderSelect; " +
    "this.gcOptionsCache = gcOptionsCache; this.gcKnownCampaignIds = gcKnownCampaignIds;", {
    $: makeDomStub({ "gc-c-provider": new FakeSelect(), "gc-pool-campaign": new FakeSelect() }),
    api: apiImpl,
    esc,
  });
}

test("provider select: fetchGcProviders hits the existing /api/admin/providers endpoint", () => {
  const calls = [];
  const s = loadOptionsSandbox((url) => { calls.push(url); return Promise.resolve({ providers: [] }); });
  return s.fetchGcProviders(false).then(() => {
    assert.deepEqual(calls, ["/api/admin/providers"]);
  });
});

test("provider select: renders a human-readable label, not the raw id", () => {
  const s = loadOptionsSandbox(() => Promise.resolve({
    providers: [{ provider_id: "prov_official_001", name: "Official Channel", type: "tournament", active: true }],
  }));
  return s.fetchGcProviders(false).then(() => {
    s.renderGcProviderSelect();
    const select = s.$("#gc-c-provider");
    const opt = select.options.find((o) => o.value === "prov_official_001");
    assert.ok(opt, "provider option must exist");
    assert.equal(opt.label, "Official Channel — tournament (active)");
    assert.ok(!opt.label.includes("prov_official_001"), "label must not be the raw id");
  });
});

test("provider select: inactive providers are labeled, not indistinguishable from active ones", () => {
  const s = loadOptionsSandbox(() => Promise.resolve({
    providers: [
      { provider_id: "p_active", name: "Official Channel", type: "tournament", active: true },
      { provider_id: "p_inactive", name: "External Game Provider A", type: "external_website", active: false },
    ],
  }));
  return s.fetchGcProviders(false).then(() => {
    s.renderGcProviderSelect();
    const select = s.$("#gc-c-provider");
    const active = select.options.find((o) => o.value === "p_active");
    const inactive = select.options.find((o) => o.value === "p_inactive");
    assert.ok(active.label.includes("(active)"));
    assert.ok(inactive.label.includes("(inactive)"));
    assert.notEqual(active.label.replace("active", ""), inactive.label.replace("inactive", ""));
  });
});

test("provider select: submitted value is the exact provider_id (option value), not the label", () => {
  const s = loadOptionsSandbox(() => Promise.resolve({
    providers: [{ provider_id: "prov_official_001", name: "Official Channel", type: "tournament", active: true }],
  }));
  return s.fetchGcProviders(false).then(() => {
    s.renderGcProviderSelect();
    const select = s.$("#gc-c-provider");
    select.value = "prov_official_001";
    assert.equal(select.value, "prov_official_001");
  });
});

test("provider select: re-entering the tab after the cache is warm preserves the admin's selection", async () => {
  const s = loadOptionsSandbox(() => Promise.resolve({
    providers: [
      { provider_id: "p_official", name: "Official Channel", type: "tournament", active: true },
      { provider_id: "p_other", name: "External Game Provider A", type: "external_website", active: true },
    ],
  }));
  // First entry: populates the cache and select.
  await s.loadGcProviderSelect(false);
  const select = s.$("#gc-c-provider");
  select.value = "p_official";
  assert.equal(select.value, "p_official");

  // Simulate switchView -> refreshCurrent(false) firing loadGcProviderSelect
  // again on tab re-entry, with the cache already warm this time.
  await s.loadGcProviderSelect(false);
  assert.equal(select.value, "p_official", "selection must survive re-entering the tab once the cache is warm");
});

test("provider select: network failure does not throw and leaves a retry-worthy state", async () => {
  const s = loadOptionsSandbox(() => Promise.reject(new Error("network down")));
  await assert.rejects(s.fetchGcProviders(false));
});

// ---------------------------------------------------------------------
// Reward pool campaign select: loads existing /api/admin/gc-campaigns
// endpoint, human-readable label, submitted value is the exact campaign_id.
// ---------------------------------------------------------------------

test("pool campaign select: fetchGcCampaignsList hits the existing /api/admin/gc-campaigns endpoint", () => {
  const calls = [];
  const s = loadOptionsSandbox((url) => { calls.push(url); return Promise.resolve({ campaigns: [] }); });
  return s.fetchGcCampaignsList(false).then(() => {
    assert.deepEqual(calls, ["/api/admin/gc-campaigns"]);
  });
});

test("pool campaign select: shows name + campaign_id, submits the exact campaign_id", () => {
  const s = loadOptionsSandbox(() => Promise.resolve({
    campaigns: [{ campaign_id: "october-lucky-draw", name: "October Lucky Draw" }],
  }));
  return s.fetchGcCampaignsList(false).then(() => {
    const select = s.$("#gc-pool-campaign");
    const opt = select.options.find((o) => o.value === "october-lucky-draw");
    assert.ok(opt);
    assert.equal(opt.label, "October Lucky Draw (october-lucky-draw)");
    select.value = "october-lucky-draw";
    assert.equal(select.value, "october-lucky-draw");
  });
});

test("pool campaign select: refreshing the list also tracks known ids for slug collision avoidance", () => {
  const s = loadOptionsSandbox(() => Promise.resolve({
    campaigns: [{ campaign_id: "october-lucky-draw", name: "October Lucky Draw" }],
  }));
  return s.fetchGcCampaignsList(false).then(() => {
    assert.equal(s.gcKnownCampaignIds["october-lucky-draw"], true);
  });
});

test("pool campaign select: a deleted campaign disappearing from the list clears it from the select on next refresh", () => {
  const s = loadOptionsSandbox(() => Promise.resolve({
    campaigns: [{ campaign_id: "october-lucky-draw", name: "October Lucky Draw" }],
  }));
  return s.fetchGcCampaignsList(false).then(() => {
    const select = s.$("#gc-pool-campaign");
    select.value = "october-lucky-draw";
    // Cache invalidated + refetched after the campaign is deleted elsewhere.
    s.api = () => Promise.resolve({ campaigns: [] });
    return s.fetchGcCampaignsList(true).then(() => {
      assert.equal(select.options.some((o) => o.value === "october-lucky-draw"), false);
    });
  });
});

// ---------------------------------------------------------------------
// HTML structure: raw ids live under Technical Details, not as top-level
// required inputs; the create form no longer has a free-text provider_id
// or campaign_id (reference) input.
// ---------------------------------------------------------------------

test("HTML: Campaign ID input lives inside the Advanced / Technical Details <details>", () => {
  const gcSection = slice(HTML, '<section id="view-gcCampaigns"', "</section>");
  const detailsIdx = gcSection.indexOf("Advanced / Technical Details");
  const idFieldIdx = gcSection.indexOf('id="gc-c-id"');
  assert.ok(detailsIdx !== -1, "Advanced / Technical Details block not found");
  assert.ok(idFieldIdx > detailsIdx, "gc-c-id must be nested inside the Technical Details block");
});

test("HTML: provider field in Create Campaign is a <select>, not a free-text input", () => {
  const gcSection = slice(HTML, '<section id="view-gcCampaigns"', "</section>");
  assert.match(gcSection, /<select[^>]*id="gc-c-provider"/);
  assert.doesNotMatch(gcSection, /<input[^>]*id="gc-c-provider"/);
});

test("HTML: campaign reference field in Reward Pools is a <select>, not a free-text input", () => {
  const rewardsSection = slice(HTML, '<section id="view-gcRewards"', "</section>");
  assert.match(rewardsSection, /<select[^>]*id="gc-pool-campaign"/);
  assert.doesNotMatch(rewardsSection, /<input[^>]*id="gc-pool-campaign"/);
});

// The /api/admin/gc-campaigns list is capped at 200 rows, so an older or
// lower-priority campaign can silently be missing from the dropdown —
// preserve the ability to link one anyway via a manual-entry fallback.
test("HTML: Reward Pools keeps a manual campaign-id fallback for campaigns beyond the dropdown's cap", () => {
  const rewardsSection = slice(HTML, '<section id="view-gcRewards"', "</section>");
  assert.match(rewardsSection, /<input[^>]*id="gc-pool-campaign-manual"/);
});

test("pool create submit: a manually-entered campaign id overrides the dropdown selection", () => {
  const SUBMIT_SRC = slice(JS, "  function bindGcRewards() {", "\n    var filterBtn = ") + "\n  }";
  const elements = {
    "gc-pool-id": { value: "gold" },
    "gc-pool-name": { value: "Gold" },
    "gc-pool-type": { value: "tournament_reward" },
    "gc-pool-scope": { value: "campaign_rewards" },
    "gc-pool-campaign": { value: "listed-campaign" },
    "gc-pool-campaign-manual": { value: "older-campaign-not-in-dropdown" },
  };
  const btn = { _listeners: {}, addEventListener(evt, fn) { this._listeners[evt] = fn; } };
  elements["gc-create-pool-btn"] = btn;
  const calls = [];
  const s = runInSandbox(SUBMIT_SRC + "\nthis.bindGcRewards = bindGcRewards;", {
    $: makeDomStub(elements),
    apiPostJson: (url, body) => { calls.push({ url, body }); return { then: () => {} }; },
    document: { addEventListener: () => {} },
  });
  s.bindGcRewards();
  btn._listeners.click();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].body.campaign_id, "older-campaign-not-in-dropdown");
});

test("pool create submit: falls back to the dropdown value when no manual id is entered", () => {
  const SUBMIT_SRC = slice(JS, "  function bindGcRewards() {", "\n    var filterBtn = ") + "\n  }";
  const elements = {
    "gc-pool-id": { value: "gold" },
    "gc-pool-name": { value: "Gold" },
    "gc-pool-type": { value: "tournament_reward" },
    "gc-pool-scope": { value: "campaign_rewards" },
    "gc-pool-campaign": { value: "listed-campaign" },
    "gc-pool-campaign-manual": { value: "" },
  };
  const btn = { _listeners: {}, addEventListener(evt, fn) { this._listeners[evt] = fn; } };
  elements["gc-create-pool-btn"] = btn;
  const calls = [];
  const s = runInSandbox(SUBMIT_SRC + "\nthis.bindGcRewards = bindGcRewards;", {
    $: makeDomStub(elements),
    apiPostJson: (url, body) => { calls.push({ url, body }); return { then: () => {} }; },
    document: { addEventListener: () => {} },
  });
  s.bindGcRewards();
  btn._listeners.click();
  assert.equal(calls[0].body.campaign_id, "listed-campaign");
});

test("HTML: Campaign name field has no placeholder implying a raw identifier", () => {
  const gcSection = slice(HTML, '<section id="view-gcCampaigns"', "</section>");
  assert.match(gcSection, /id="gc-c-name" placeholder="Campaign name"/);
});
