/**
 * P0.2: Campaign Centre / Community Centre tab-to-filter consolidation.
 *
 * static/admin-dashboard.js merged three near-identical "Running"/"Drafts"
 * campaign tabs (both driven by /api/admin/campaign-builder/campaigns with
 * only the status query differing) into one "Campaigns" tab + segmented
 * status filter, and five near-identical Community Centre post-status tabs
 * (all driven by /api/admin/community/posts) into one "Posts" tab + filter.
 * "Scheduled" in Campaign Centre and "Poll Results" in Community Centre were
 * investigated and found to hit a genuinely different endpoint/collection
 * (legacy /api/admin/campaigns, and /api/admin/community/polls
 * respectively) so they were deliberately left as separate tabs/views.
 *
 * Mirrors test_campaign_centre_delete_ui.test.js: the dashboard is one large
 * inline-script-free file with no build step and no jsdom in this repo, so
 * the relevant functions are extracted as text and executed in a sandboxed
 * vm context against a small hand-rolled DOM/fetch stub, capturing the URL
 * each loader would request rather than exercising real rendering.
 *
 * Run with: node --test test_admin_dashboard_p0_2_filters.test.js
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

// A thenable stub: loadActiveCampaigns/loadCcBoard chain .then()/.catch()
// off the API call before doing any DOM rendering. We only care about which
// URL was requested, so the stub never actually invokes those callbacks —
// this keeps the sandbox free of the (large, unrelated) rendering functions.
function makeApiStub(calls) {
  const thenable = { then: () => thenable, catch: () => thenable };
  return function (urlArg) {
    calls.push(urlArg);
    return thenable;
  };
}

// Minimal DOM stub: only supports the two exact lookups these loaders use
// ("#id" and "#id .active"), backed by a plain activeStatus map the test
// sets before calling the loader.
function makeDomStub(activeStatusByFilterId) {
  return function $(sel) {
    const m = /^#([\w-]+) \.active$/.exec(sel);
    if (m) {
      const status = activeStatusByFilterId[m[1]];
      if (status === undefined) return null;
      return { dataset: { status } };
    }
    return { innerHTML: "", classList: { toggle() {}, contains: () => false } };
  };
}

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign(
    { console, encodeURIComponent, Object },
    sandboxExtra
  );
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

// ---------------------------------------------------------------------
// Campaign Centre: loadActiveCampaigns (backs the merged "Campaigns" tab)
// ---------------------------------------------------------------------

const LOAD_ACTIVE_CAMPAIGNS_SRC = slice(
  JS,
  "  function loadActiveCampaigns(force) {",
  "\n  function _cbRenderCampaignList("
);

test("Campaign Centre filter: Running -> status=active on the campaign-builder endpoint", () => {
  const calls = [];
  const sandbox = runInSandbox(LOAD_ACTIVE_CAMPAIGNS_SRC + "\nthis.loadActiveCampaigns = loadActiveCampaigns;", {
    $: makeDomStub({ "ac-status-filter": "active" }),
    cbApi: makeApiStub(calls),
  });
  sandbox.loadActiveCampaigns();
  assert.equal(calls.length, 1);
  assert.equal(calls[0], "/api/admin/campaign-builder/campaigns?status=active");
});

test("Campaign Centre filter: Draft -> status=draft on the campaign-builder endpoint", () => {
  const calls = [];
  const sandbox = runInSandbox(LOAD_ACTIVE_CAMPAIGNS_SRC + "\nthis.loadActiveCampaigns = loadActiveCampaigns;", {
    $: makeDomStub({ "ac-status-filter": "draft" }),
    cbApi: makeApiStub(calls),
  });
  sandbox.loadActiveCampaigns();
  assert.equal(calls[0], "/api/admin/campaign-builder/campaigns?status=draft");
});

test("Campaign Centre filter: All -> no status query param (backend excludes archived by default)", () => {
  const calls = [];
  const sandbox = runInSandbox(LOAD_ACTIVE_CAMPAIGNS_SRC + "\nthis.loadActiveCampaigns = loadActiveCampaigns;", {
    $: makeDomStub({ "ac-status-filter": "" }),
    cbApi: makeApiStub(calls),
  });
  sandbox.loadActiveCampaigns();
  assert.equal(calls[0], "/api/admin/campaign-builder/campaigns");
});

test("Campaign Centre filter: no filter button found falls back to Running (active) default", () => {
  const calls = [];
  const sandbox = runInSandbox(LOAD_ACTIVE_CAMPAIGNS_SRC + "\nthis.loadActiveCampaigns = loadActiveCampaigns;", {
    $: makeDomStub({}),
    cbApi: makeApiStub(calls),
  });
  sandbox.loadActiveCampaigns();
  assert.equal(calls[0], "/api/admin/campaign-builder/campaigns?status=active");
});

// ---------------------------------------------------------------------
// Community Centre: loadCcBoard (backs the merged "Posts" tab)
// ---------------------------------------------------------------------

const CC_BOARD_SRC = slice(JS, "  var CC_BOARD_QUERY = {", "\n  function ccRefreshBoard() {") +
  "\n  function ccRefreshBoard() { if (state.view === 'ccPollResults') loadCcPollResults(true); else loadCcBoard(true); }";

function runCcBoard(activeStatus) {
  const calls = [];
  const sandbox = runInSandbox(CC_BOARD_SRC + "\nthis.loadCcBoard = loadCcBoard; this.CC_BOARD_QUERY = CC_BOARD_QUERY;", {
    $: makeDomStub({ "cc-board-filter": activeStatus }),
    api: makeApiStub(calls),
    statePanel: () => {},
    state: { view: "ccBoard" },
    loadCcPollResults: () => {},
  });
  sandbox.loadCcBoard();
  return calls;
}

test("Community Centre filter: Scheduled -> status=scheduled on /api/admin/community/posts", () => {
  const calls = runCcBoard("scheduled");
  assert.equal(calls[0], "/api/admin/community/posts?limit=100&status=scheduled");
});

test("Community Centre filter: Draft -> status=draft", () => {
  assert.equal(runCcBoard("draft")[0], "/api/admin/community/posts?limit=100&status=draft");
});

test("Community Centre filter: Pending Approval -> status=pending_approval", () => {
  assert.equal(runCcBoard("pending_approval")[0], "/api/admin/community/posts?limit=100&status=pending_approval");
});

test("Community Centre filter: Published -> statuses=published,partially_published (preserves the pre-existing combined-status query)", () => {
  assert.equal(
    runCcBoard("published")[0],
    "/api/admin/community/posts?limit=100&statuses=published%2Cpartially_published"
  );
});

test("Community Centre filter: Failed -> status=failed", () => {
  assert.equal(runCcBoard("failed")[0], "/api/admin/community/posts?limit=100&status=failed");
});

test("Community Centre filter: All -> no status/statuses query param at all", () => {
  assert.equal(runCcBoard("")[0], "/api/admin/community/posts?limit=100");
});

// ---------------------------------------------------------------------
// Structural checks: VIEWS/MODULES/HTML wiring for the consolidation
// ---------------------------------------------------------------------

function loadViews() {
  const src = slice(JS, "var VIEWS =[", "];") + "];";
  return new Function(src + "\nreturn VIEWS;")();
}

function loadModules() {
  const src = slice(JS, "var MODULES = [", "\n  var currentModuleKey = null;");
  return new Function(src + "\nreturn MODULES;")();
}

function tabsFor(modules, key) {
  const mod = modules.find((m) => m.key === key);
  assert.ok(mod, `module "${key}" not found`);
  return mod.tabs;
}

test("removed tab view names (draftCampaigns) no longer exist in VIEWS", () => {
  assert.ok(!loadViews().includes("draftCampaigns"), "draftCampaigns should have been removed from VIEWS");
});

test("Campaign Centre no longer has separate Running/Drafts tabs, just one Campaigns tab", () => {
  const tabs = tabsFor(loadModules(), "campaign");
  const labels = tabs.map((t) => t.label);
  assert.ok(!labels.includes("Running"));
  assert.ok(!labels.includes("Drafts"));
  assert.ok(labels.includes("Campaigns"));
  const campaignsTab = tabs.find((t) => t.label === "Campaigns");
  assert.equal(campaignsTab.view, "activeCampaigns");
});

test("Campaign Centre 'Legacy Targeting' (formerly mislabeled 'Scheduled') stays a separate tab: different backend system", () => {
  const tabs = tabsFor(loadModules(), "campaign");
  const legacyTab = tabs.find((t) => t.view === "campaigns");
  assert.ok(legacyTab, "legacy /api/admin/campaigns tab must still exist as its own tab");
  // Proven not a status alias of activeCampaigns: distinct collection, distinct
  // CAMPAIGN_STATUSES vocabulary, distinct CRUD editor (campaigns.py vs campaign_builder.py).
  assert.notEqual(legacyTab.view, "activeCampaigns");
});

test("cross-link to the former 'Running' tab (paused-campaign alert) still targets the merged Campaigns view", () => {
  assert.ok(JS.includes('view: "activeCampaigns"'), "summary dashboard's paused-campaign signal must still point at activeCampaigns");
  // And that view's default selected filter (first HTML button marked "active") is Running,
  // so the cross-link lands the admin on the right status without extra clicks.
  const section = slice(HTML, '<section id="view-activeCampaigns"', "</section>");
  const defaultBtnMatch = /<button data-status="([^"]*)" class="active">/.exec(section);
  assert.ok(defaultBtnMatch, "expected a default-active filter button in the Campaigns view");
  assert.equal(defaultBtnMatch[1], "active", "default filter should be Running (status=active)");
});

// Regression test for a real bug flagged in review: the #ac-status-filter
// selection persists in the DOM across navigation (by design, so it survives
// refreshes while the admin stays on the Campaigns tab). But that means if an
// admin previously clicked Draft, left the tab, and then followed the
// paused-campaign attention signal (which is always about a Running
// campaign), goToViewAndClick("activeCampaigns", "") must not silently leave
// the stale Draft filter selected — it should reset to Running first.
const GO_TO_VIEW_AND_CLICK_SRC = slice(
  JS,
  "  window.goToViewAndClick = function (view, btnId) {",
  "\n  // title/subtitle/CTA empty state"
);

test("goToViewAndClick resets the Campaigns status filter to Running when navigating in via cross-link", () => {
  const buttons = [
    { dataset: { status: "active" }, active: false },
    { dataset: { status: "draft" }, active: true }, // admin had Draft selected before leaving the tab
    { dataset: { status: "" }, active: false },
  ];
  const fakeButtons = buttons.map((b) => ({
    dataset: b.dataset,
    classList: { toggle: (cls, on) => { b.active = on; } },
  }));

  let switchedTo = null;
  const sandbox = runInSandbox(GO_TO_VIEW_AND_CLICK_SRC + "\nthis.goToViewAndClick = window.goToViewAndClick;", {
    window: {},
    $all: (sel) => (sel === "#ac-status-filter button" ? fakeButtons : []),
    switchView: (v) => { switchedTo = v; },
    document: { getElementById: () => null },
    setTimeout: (fn) => fn(),
  });
  sandbox.goToViewAndClick("activeCampaigns", "");

  assert.equal(switchedTo, "activeCampaigns");
  assert.deepEqual(
    buttons.map((b) => b.active),
    [true, false, false],
    "Running must end up selected and Draft deselected after the cross-link navigation"
  );
});

test("goToViewAndClick leaves other views' filter state alone (only activeCampaigns is special-cased)", () => {
  let touchedFilter = false;
  const sandbox = runInSandbox(GO_TO_VIEW_AND_CLICK_SRC + "\nthis.goToViewAndClick = window.goToViewAndClick;", {
    window: {},
    $all: (sel) => { if (sel === "#ac-status-filter button") touchedFilter = true; return []; },
    switchView: () => {},
    document: { getElementById: () => null },
    setTimeout: (fn) => fn(),
  });
  sandbox.goToViewAndClick("affiliatePending", "");
  assert.equal(touchedFilter, false);
});

test("Community Centre no longer has five separate status tabs, just one Posts tab", () => {
  const tabs = tabsFor(loadModules(), "community");
  const labels = tabs.map((t) => t.label);
  ["Scheduled", "Drafts", "Pending Approval", "Published", "Failed"].forEach((removed) => {
    assert.ok(!labels.includes(removed), `"${removed}" should no longer be its own tab`);
  });
  assert.ok(labels.includes("Posts"));
  const postsTab = tabs.find((t) => t.label === "Posts");
  assert.equal(postsTab.view, "ccBoard");
});

test("Poll Results stays a fully separate tab/view (unique endpoint + renderer, not a status filter)", () => {
  const tabs = tabsFor(loadModules(), "community");
  const pollTab = tabs.find((t) => t.label === "Poll Results");
  assert.ok(pollTab, "Poll Results tab must still exist");
  assert.equal(pollTab.view, "ccPollResults");
  assert.notEqual(pollTab.view, "ccBoard", "Poll Results must not share the merged Posts view");

  assert.ok(loadViews().includes("ccPollResults"), "ccPollResults must be declared in VIEWS");

  // Confirm the two loaders really do hit different endpoints (the reason
  // Poll Results was excluded from the merge).
  const pollLoaderSrc = slice(JS, "  function loadCcPollResults() {", "\n  function ccRefreshBoard() {");
  assert.ok(pollLoaderSrc.includes("/api/admin/community/polls"));
  assert.ok(!pollLoaderSrc.includes("/api/admin/community/posts"));

  const boardLoaderSrc = slice(JS, "  function loadCcBoard() {", "\n  function loadCcPollResults() {");
  assert.ok(boardLoaderSrc.includes("/api/admin/community/posts"));
  assert.ok(!boardLoaderSrc.includes("/api/admin/community/polls"));
});

test("no leftover currentCcBoard state (dead code from the removed per-status tabs)", () => {
  assert.ok(!JS.includes("currentCcBoard"), "currentCcBoard should have been removed along with the per-status ccBoard tabs");
});
