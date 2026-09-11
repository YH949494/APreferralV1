/**
 * Admin Dashboard -> Mini App -> Existing Drops must show both standard
 * voucher drops (db.drops, via GET /v2/miniapp/admin/drops_v2) AND Mission
 * Pool campaigns (gc_campaigns with mechanic="mission_pool", via the same
 * GET /api/admin/mission-pool/campaigns Campaign Centre's Mission Reward
 * Pool list uses).
 *
 * Root cause this fixes: loadDrops() in static/admin-dashboard.js only ever
 * fetched drops_v2, which only queries db.drops — a collection Mission Pool
 * campaigns never live in. There was no filter excluding mechanic ==
 * "mission_pool"; the endpoint simply had no way to know missions exist.
 * The mission-pool/campaigns response already carries everything the table
 * needs (name, state, campaign_status, starts_at/ends_at, submissions,
 * winners, pool_available) — this is a frontend merge, no backend change.
 *
 * Mirrors test_campaign_centre_delete_ui.test.js / test_admin_dashboard_p0_2_filters.test.js:
 * no build step, no jsdom in this repo, so the relevant source is extracted
 * as text and executed in a sandboxed vm context against small hand-rolled
 * DOM/fetch stubs.
 *
 * Run with: node --test test_admin_mini_app_existing_drops.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const JS = fs.readFileSync(JS_PATH, "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

const HELPERS_SRC = slice(JS, "  function fmt(v) {", "\n  function apiPostJson(");
const STATE_PANEL_SRC = slice(JS, "  function statePanel(elId, kind, msg) {", "\n  function expandTable(");
const ROW_AND_LOAD_SRC = slice(JS, "  function dropScheduleHtml(startsAt, endsAt) {", "\n  function bindDrops() {");
const MISSION_CLICK_SRC = slice(
  JS,
  '    document.addEventListener("click", function (event) {\n      var btn = event.target && event.target.closest && event.target.closest("[data-mission-action]");',
  "\n  }\n\n  // Same live-inventory check runAction()"
);
const MP_INVENTORY_PREFLIGHT_SRC = slice(
  JS,
  "  function mpInventoryPreflight(campaignId) {",
  "\n  // ---------- Affiliate Voucher Pools"
);
// P0.15 — the click handler (inside MISSION_CLICK_SRC) now calls this for
// its success toast copy. Defined before bindDrops() in the real file (so
// it stays outside MISSION_CLICK_SRC's own start/end markers), hence its
// own slice here.
const MP_DROPS_SUCCESS_SRC = slice(
  JS,
  "  function mpDropsActionSuccessMessage(action, isResume) {",
  "\n  function loadDrops(force) {"
);

function featureSource() {
  return HELPERS_SRC + "\n" + STATE_PANEL_SRC + "\n" + ROW_AND_LOAD_SRC;
}

// ---------------------------------------------------------------------
// Structural checks on the raw source (independent of runtime execution)
// ---------------------------------------------------------------------

test("loadDrops fetches both drops_v2 and mission-pool/campaigns", () => {
  const src = slice(JS, "  function loadDrops(force) {", "\n  function bindDrops() {");
  assert.match(src, /\/v2\/miniapp\/admin\/drops_v2/);
  assert.match(src, /\/api\/admin\/mission-pool\/campaigns/);
});

test("mission action buttons post to Mission Pool/Campaign Centre endpoints, never a drops endpoint", () => {
  const src = slice(
    JS,
    '    document.addEventListener("click", function (event) {\n      var btn = event.target && event.target.closest && event.target.closest("[data-mission-action]");',
    "\n  }\n\n  // ---------- Affiliate Voucher Pools"
  );
  assert.match(src, /action === "publish" \|\| action === "pause"/, "publish/pause must route to the gc-campaigns lifecycle endpoints");
  assert.match(src, /\/api\/admin\/gc-campaigns\//);
  assert.match(src, /\/api\/admin\/mission-pool\//);
  assert.doesNotMatch(src, /\/v2\/miniapp\/admin\/drops/);
});

// ---------------------------------------------------------------------
// Minimal hand-rolled DOM
// ---------------------------------------------------------------------

function makeFakeEl() {
  return { innerHTML: "", textContent: "", value: "" };
}

function makeDomStub(elements) {
  return function $(sel) {
    const id = sel.replace(/^#/, "");
    if (Object.prototype.hasOwnProperty.call(elements, id)) return elements[id];
    return null;
  };
}

function makeFetchQueue() {
  const calls = [];
  const queue = [];
  const fetchImpl = (fetchPath, opts) => {
    calls.push({ path: fetchPath, opts });
    const next = queue.shift();
    if (!next) return Promise.reject(new Error("unexpected fetch call: " + fetchPath));
    if (next.reject) return Promise.reject(next.reject);
    return Promise.resolve({
      status: next.status,
      ok: next.status >= 200 && next.status < 300,
      json: () => Promise.resolve(next.body),
    });
  };
  fetchImpl.calls = calls;
  fetchImpl.push = (status, body) => queue.push({ status, body });
  fetchImpl.pushReject = (err) => queue.push({ reject: err });
  return fetchImpl;
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve)).then(() => new Promise((resolve) => setImmediate(resolve)));
}

function makeContext(elements) {
  const fetchImpl = makeFetchQueue();
  const toasts = [];
  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    fetch: fetchImpl,
    window: { location: { href: "" } },
    encodeURIComponent,
    setTimeout,
    clearTimeout,
    Promise,
    $: makeDomStub(elements || {}),
  };
  const context = vm.createContext(sandbox);
  vm.runInContext(featureSource(), context, { filename: "admin-dashboard-existing-drops.js" });
  // gcPill/ccUtcToKlDisplay/toast live elsewhere in the real file — stub
  // them the same way test_campaign_centre_delete_ui.test.js stubs
  // loadGcCampaigns, since their own logic isn't what's under test here.
  vm.runInContext(
    'this.gcPill = function (status) { return "<span class=\\"pill\\">" + (status || "\\u2014") + "</span>"; };' +
      'this.ccUtcToKlDisplay = function (iso) { return iso ? ("KL:" + iso) : "\\u2014"; };' +
      "this.toast = function (msg, kind) { __toasts.push({ msg: msg, kind: kind }); };",
    context
  );
  context.__toasts = toasts;
  return { context, fetchImpl, toasts };
}

// ---------------------------------------------------------------------
// mpBucketStatus / mpActionsHtml — status + action mapping
// ---------------------------------------------------------------------

test("mpBucketStatus maps operational states to upcoming/live/paused/ended", () => {
  const { context } = makeContext();
  assert.equal(context.mpBucketStatus("draft"), "upcoming");
  assert.equal(context.mpBucketStatus("scheduled"), "upcoming");
  assert.equal(context.mpBucketStatus("live"), "live");
  assert.equal(context.mpBucketStatus("paused"), "paused");
  assert.equal(context.mpBucketStatus("cancelled"), "ended");
  assert.equal(context.mpBucketStatus("closed"), "ended");
  assert.equal(context.mpBucketStatus("processing"), "ended");
  assert.equal(context.mpBucketStatus("completed"), "ended");
});

test("upcoming mission gets Start/Edit/End, and Delete only when campaign_status is deletable", () => {
  const { context } = makeContext();
  const notDeletable = context.mpActionsHtml({ campaign_id: "m1", state: "scheduled", campaign_status: "scheduled", name: "Mission 1" });
  assert.match(notDeletable, /data-mission-action="publish"[^>]*>Start</);
  assert.match(notDeletable, /data-mission-action="edit"/);
  assert.match(notDeletable, /data-mission-action="cancel"[^>]*>End</);
  assert.doesNotMatch(notDeletable, /data-mission-action="delete"/);

  const deletable = context.mpActionsHtml({ campaign_id: "m2", state: "draft", campaign_status: "draft", name: "Mission 2" });
  assert.match(deletable, /data-mission-action="delete"/);
});

test("live mission gets Pause/End(close)/Open, never Start or Delete", () => {
  const { context } = makeContext();
  const html = context.mpActionsHtml({ campaign_id: "m3", state: "live", campaign_status: "live", name: "Mission 3" });
  assert.match(html, /data-mission-action="pause"/);
  assert.match(html, /data-mission-action="close"[^>]*>End</);
  assert.match(html, /data-mission-action="open"[^>]*>Open\/View</);
  assert.doesNotMatch(html, /data-mission-action="publish"/);
  assert.doesNotMatch(html, /data-mission-action="delete"/);
});

test("paused mission gets Resume(publish)/End(cancel)/Open", () => {
  const { context } = makeContext();
  const html = context.mpActionsHtml({ campaign_id: "m4", state: "paused", campaign_status: "paused", name: "Mission 4" });
  assert.match(html, /data-mission-action="publish"[^>]*>Resume</);
  assert.match(html, /data-mission-action="cancel"[^>]*>End</);
  assert.match(html, /data-mission-action="open"/);
});

test("ended mission (completed) gets View Results only, plus Delete when eligible", () => {
  const { context } = makeContext();
  const notDeletable = context.mpActionsHtml({ campaign_id: "m5", state: "completed", campaign_status: "ended", name: "Mission 5" });
  assert.match(notDeletable, /data-mission-action="open"[^>]*>View Results</);
  assert.match(notDeletable, /data-mission-action="delete"/, "campaign_status ended is a deletable status");

  const archived = context.mpActionsHtml({ campaign_id: "m6", state: "closed", campaign_status: "archived", name: "Mission 6" });
  assert.match(archived, /data-mission-action="delete"/);

  const stillProcessing = context.mpActionsHtml({ campaign_id: "m7", state: "processing", campaign_status: "ended" });
  // processing campaign_status is reported as "ended" by the backend once
  // status flips, so it's deletable per _DELETABLE_STATUSES too — only a
  // raw live/paused/scheduled campaign_status must never show Delete.
  assert.match(stillProcessing, /data-mission-action="delete"/);
});

test("Delete never appears for a live or paused campaign_status regardless of bucket", () => {
  const { context } = makeContext();
  const live = context.mpActionsHtml({ campaign_id: "m8", state: "live", campaign_status: "live" });
  const paused = context.mpActionsHtml({ campaign_id: "m9", state: "paused", campaign_status: "paused" });
  assert.doesNotMatch(live, /data-mission-action="delete"/);
  assert.doesNotMatch(paused, /data-mission-action="delete"/);
});

// ---------------------------------------------------------------------
// loadDrops: merge behavior, resilience, KL formatting, dedupe
// ---------------------------------------------------------------------

function baseElements() {
  return {
    "drops-list-body": makeFakeEl(),
    "dra-drop-id": makeFakeEl(),
  };
}

test("a live Mission Pool campaign appears in Existing Drops alongside standard drops", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, {
    items: [{ dropId: "d1", name: "VIP Drop", type: "pooled", status: "active", priority: 100, codesFree: 3, codesTotal: 10 }],
  });
  fetchImpl.push(200, {
    campaigns: [{
      campaign_id: "mission-1", name: "Summer Mission", state: "live", campaign_status: "live",
      starts_at: "2026-01-01T00:00:00Z", ends_at: "2026-01-08T00:00:00Z",
      submissions: 42, winners: 5, pool_available: 20,
    }],
  });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /VIP Drop/);
  assert.match(html, /Summer Mission/);
  assert.match(html, />Mission</);
  assert.match(html, /42/);
  assert.match(html, /data-mission-action="pause"/);
});

test("an upcoming mission appears with the upcoming status and Start/Edit/End/Delete-gated actions", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, { items: [] });
  fetchImpl.push(200, {
    campaigns: [{
      campaign_id: "mission-2", name: "Autumn Mission", state: "scheduled", campaign_status: "scheduled",
      starts_at: "2026-03-01T00:00:00Z", ends_at: null, submissions: 0, winners: 0, pool_available: 50,
    }],
  });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /Autumn Mission/);
  assert.match(html, /data-mission-action="publish"[^>]*>Start</);
  assert.match(html, /data-mission-action="edit"/);
  assert.doesNotMatch(html, /data-mission-action="delete"/, "a scheduled campaign_status is not deletable");
});

test("paused and ended missions map to the correct bucket and actions in the rendered table", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, { items: [] });
  fetchImpl.push(200, {
    campaigns: [
      { campaign_id: "mp-1", name: "Paused Mission", state: "paused", campaign_status: "paused", submissions: 1, winners: 0, pool_available: 9 },
      { campaign_id: "mp-2", name: "Ended Mission", state: "completed", campaign_status: "ended", submissions: 9, winners: 3, pool_available: 0 },
    ],
  });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /Paused Mission/);
  assert.match(html, /data-mission-action="cancel" data-id="mp-1">End</);
  assert.match(html, /Ended Mission/);
  assert.match(html, /data-mission-action="open"[^>]*data-id="mp-2"[^>]*>View Results</);
});

test("deleted campaigns are hidden — mission-pool/campaigns already excludes status=deleted, so the row list stays free of tombstones", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, { items: [] });
  // The backend (GET /api/admin/mission-pool/campaigns) filters out
  // status="deleted" tombstones server-side; a deleted campaign therefore
  // never appears in the campaigns array the frontend receives.
  fetchImpl.push(200, { campaigns: [{ campaign_id: "live-1", name: "Still Live", state: "live", campaign_status: "live" }] });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /Still Live/);
  // 1 header <tr> in <thead> + 1 body <tr> for the single (non-tombstoned)
  // campaign — no extra row for a deleted campaign the backend already
  // excludes from its response.
  assert.equal((html.match(/<tr>/g) || []).length, 2);
});

test("standard drops still appear unchanged when the mission list is present", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, {
    items: [{ dropId: "d2", name: "Legacy Drop", type: "personalised", status: "upcoming", priority: 50, assigned: 4, claimed: 1 }],
  });
  fetchImpl.push(200, { campaigns: [] });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /Legacy Drop/);
  assert.match(html, /Assigned 4 \/ Claimed 1/);
  assert.match(html, /data-drop-op="start_now"/);
});

test("no duplicate row appears if the mission-pool/campaigns response repeats a campaign_id", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, { items: [] });
  fetchImpl.push(200, {
    campaigns: [
      { campaign_id: "dup-1", name: "Dup Mission", state: "live", campaign_status: "live" },
      { campaign_id: "dup-1", name: "Dup Mission", state: "live", campaign_status: "live" },
    ],
  });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.equal((html.match(/Dup Mission/g) || []).length, 1);
});

test("KL schedule formatting is applied to mission rows via the shared ccUtcToKlDisplay helper", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, { items: [] });
  fetchImpl.push(200, {
    campaigns: [{ campaign_id: "kl-1", name: "KL Mission", state: "live", campaign_status: "live", starts_at: "2026-05-01T00:00:00Z", ends_at: "2026-05-02T00:00:00Z" }],
  });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /KL:2026-05-01T00:00:00Z/);
  assert.match(html, /KL:2026-05-02T00:00:00Z/);
});

test("a failed mission-list request does not remove standard-drop rows", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.push(200, {
    items: [{ dropId: "d3", name: "Resilient Drop", type: "pooled", status: "active", priority: 10, codesFree: 1, codesTotal: 1 }],
  });
  fetchImpl.pushReject(new Error("network down"));

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /Resilient Drop/);
});

test("a failed standard-drop request does not remove mission rows", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.pushReject(new Error("drops_v2 down"));
  fetchImpl.push(200, {
    campaigns: [{ campaign_id: "resilient-1", name: "Resilient Mission", state: "live", campaign_status: "live" }],
  });

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /Resilient Mission/);
});

test("both sources failing shows an error state, not a blank silent table", async () => {
  const els = baseElements();
  const { context, fetchImpl } = makeContext(els);
  fetchImpl.pushReject(new Error("drops_v2 down"));
  fetchImpl.pushReject(new Error("missions down"));

  context.loadDrops();
  await flush();

  const html = els["drops-list-body"].innerHTML;
  assert.match(html, /Failed to load/i);
});

// ---------------------------------------------------------------------
// Mission action click handler: posts to the right endpoint per action
// ---------------------------------------------------------------------

function makeClickContext() {
  const listeners = [];
  const toasts = [];
  const fetchImpl = makeFetchQueue();
  const opened = { admin: [], edit: [], deleted: [] };
  const invalidated = [];
  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    fetch: fetchImpl,
    window: { location: { href: "" } },
    encodeURIComponent,
    document: { addEventListener: (evt, fn) => listeners.push({ evt, fn }) },
    confirm: () => true,
  };
  const context = vm.createContext(sandbox);
  vm.runInContext(
    // P0.15 — the click handler now posts through apiPostJson (not apiPost),
    // via a gcRunAction stub (defined below) rather than the real gcRunAction
    // — which would also need btnStart/btnStop's real DOM (classList/
    // innerHTML) expectations the hand-rolled mock button here doesn't have.
    slice(JS, "  function fmt(v) {", "\n  function apiPatchJson(") +
      "\n" + MP_DROPS_SUCCESS_SRC + "\n" + MISSION_CLICK_SRC + "\n" + MP_INVENTORY_PREFLIGHT_SRC,
    context,
    { filename: "admin-dashboard-mission-click.js" }
  );
  vm.runInContext(
    "this.toast = function (msg, kind) { __toasts.push({ msg: msg, kind: kind }); };" +
      "this.openMissionAdmin = function (id) { __opened.admin.push(id); };" +
      "this.openMissionEdit = function (id) { __opened.edit.push(id); };" +
      "this.openGcDeleteModal = function (id, name, cb) { __opened.deleted.push(id); if (cb) cb(); };" +
      "this.gcInvalidateCampaignsCache = function () { __invalidated.push(true); };" +
      "this.loadDrops = function () {};" +
      'this.mpConfirmCopy = function () { return ""; };' +
      // P0.15 — minimal stand-in for admin-dashboard.js's gcRunAction: the
      // run()->{ok,status,d}->toast/invalidateCache/onSuccess contract the
      // real one guarantees. Full confirm/in-flight/button-disable coverage
      // against the real gcRunAction lives in
      // test_admin_dashboard_p0_8_action_hardening.test.js.
      "this.gcRunAction = function (opts) {" +
      "  return Promise.resolve().then(opts.run).then(function (res) {" +
      "    if (!res || !res.ok || !res.d || res.d.status !== \"ok\") {" +
      "      toast(\"❌ \" + (opts.fallbackError || \"Couldn't complete this action. Try again.\"), \"error\");" +
      "      return;" +
      "    }" +
      "    var msg = typeof opts.successMessage === \"function\" ? opts.successMessage(res.d) : opts.successMessage;" +
      "    if (msg) toast(\"✅ \" + msg, \"success\");" +
      "    if (opts.invalidateCache !== false) gcInvalidateCampaignsCache();" +
      "    if (opts.onSuccess) opts.onSuccess(res.d);" +
      "  }, function () {" +
      "    toast(\"❌ \" + (opts.fallbackError || \"Couldn't complete this action. Try again.\"), \"error\");" +
      "  });" +
      "};",
    context
  );
  context.__toasts = toasts;
  context.__opened = opened;
  context.__invalidated = invalidated;
  return { context, fetchImpl, toasts, opened, listeners, invalidated };
}

function triggerMissionClick(listeners, action, id, name) {
  const entry = listeners[listeners.length - 1];
  const btn = { dataset: { missionAction: action, id: id, name: name || "" }, disabled: false };
  entry.fn({ target: { closest: () => btn } });
  return btn;
}

test("Start (upcoming) runs the inventory preflight, then posts to gc-campaigns publish", async () => {
  const { fetchImpl, listeners, toasts } = makeClickContext();
  fetchImpl.push(200, { status: "ok", reward: { sufficient: true, winner_count: 5, available: 20, pool_id: "p1" } });
  fetchImpl.push(200, { status: "ok" });
  triggerMissionClick(listeners, "publish", "mission-x");
  await flush();
  assert.equal(fetchImpl.calls.length, 2);
  assert.equal(fetchImpl.calls[0].path, "/api/admin/mission-pool/mission-x/edit-state");
  assert.equal(fetchImpl.calls[1].path, "/api/admin/gc-campaigns/mission-x/publish");
  assert.equal(fetchImpl.calls[1].opts.method, "POST");
  assert.ok(toasts.some((t) => /success/.test(t.kind)));
});

test("Start is blocked (no publish call) when the inventory preflight reports an insufficient pool", async () => {
  const { fetchImpl, listeners, toasts } = makeClickContext();
  fetchImpl.push(200, { status: "ok", reward: { sufficient: false, winner_count: 30, available: 5, pool_id: "p1" } });
  triggerMissionClick(listeners, "publish", "mission-x2");
  await flush();
  assert.equal(fetchImpl.calls.length, 1, "publish must never be posted when the pool can't cover the winner target");
  assert.equal(fetchImpl.calls[0].path, "/api/admin/mission-pool/mission-x2/edit-state");
  assert.ok(toasts.some((t) => t.kind === "error" && /Publishing blocked/.test(t.msg)));
});

test("Resume (paused -> publish) also runs the inventory preflight before posting", async () => {
  const { fetchImpl, listeners } = makeClickContext();
  fetchImpl.push(200, { status: "ok", reward: { sufficient: true, winner_count: 1, available: 1, pool_id: "p2" } });
  fetchImpl.push(200, { status: "ok" });
  triggerMissionClick(listeners, "publish", "mission-resume");
  await flush();
  assert.equal(fetchImpl.calls[0].path, "/api/admin/mission-pool/mission-resume/edit-state");
  assert.equal(fetchImpl.calls[1].path, "/api/admin/gc-campaigns/mission-resume/publish");
});

test("Pause posts to gc-campaigns pause", async () => {
  const { fetchImpl, listeners } = makeClickContext();
  fetchImpl.push(200, { status: "ok" });
  triggerMissionClick(listeners, "pause", "mission-y");
  await flush();
  assert.equal(fetchImpl.calls[0].path, "/api/admin/gc-campaigns/mission-y/pause");
});

test("End on a live mission posts to mission-pool close", async () => {
  const { fetchImpl, listeners } = makeClickContext();
  fetchImpl.push(200, { status: "ok" });
  triggerMissionClick(listeners, "close", "mission-z");
  await flush();
  assert.equal(fetchImpl.calls[0].path, "/api/admin/mission-pool/mission-z/close");
});

test("End on an upcoming/paused mission posts to mission-pool cancel", async () => {
  const { fetchImpl, listeners } = makeClickContext();
  fetchImpl.push(200, { status: "ok" });
  triggerMissionClick(listeners, "cancel", "mission-w");
  await flush();
  assert.equal(fetchImpl.calls[0].path, "/api/admin/mission-pool/mission-w/cancel");
});

test("Open/View and Edit navigate instead of calling the API", () => {
  const { fetchImpl, listeners, opened } = makeClickContext();
  triggerMissionClick(listeners, "open", "mission-v");
  triggerMissionClick(listeners, "edit", "mission-u");
  assert.equal(fetchImpl.calls.length, 0);
  assert.deepEqual(opened.admin, ["mission-v"]);
  assert.deepEqual(opened.edit, ["mission-u"]);
});

test("Delete routes through the shared campaign-deletion modal, not a direct API call", () => {
  const { fetchImpl, listeners, opened } = makeClickContext();
  triggerMissionClick(listeners, "delete", "mission-t", "Mission T");
  assert.equal(fetchImpl.calls.length, 0, "the modal owns the actual DELETE call, not this handler");
  assert.deepEqual(opened.deleted, ["mission-t"]);
});

test("Delete also invalidates the shared Player Campaigns cache, so a deleted mission can't linger there", () => {
  const { listeners, opened, invalidated } = makeClickContext();
  triggerMissionClick(listeners, "delete", "mission-cache", "Mission Cache");
  assert.deepEqual(opened.deleted, ["mission-cache"]);
  assert.equal(invalidated.length, 1, "gcInvalidateCampaignsCache must run on successful delete");
});
