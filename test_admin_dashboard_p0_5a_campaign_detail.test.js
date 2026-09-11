/**
 * P0.5a: Campaign Detail — a READ-ONLY guided-setup container for a single
 * gc_campaigns document, built on top of static/admin-dashboard.js.
 *
 * Covers:
 *  - computeSetupChecklist() mirrors campaign_centre._transition()'s publish
 *    gate per campaign type (tournament / mission_pool / registration-only),
 *    including which rows are even applicable (dynamic checklist — never a
 *    fixed five rows).
 *  - Progress counts only applicable rows.
 *  - gcFirstIncompleteRequiredRow() / gcCampaignDetailContinueHtml() drive
 *    "Continue Setup" / "Publish Campaign" (P0.9 replaced the old dead-end
 *    "Ready to Publish — go to Campaigns list" copy with a real CTA; see
 *    test_admin_dashboard_p0_9_publish_preview.test.js for the full Publish/
 *    Preview suite).
 *  - gcComputeShareState() for the Share Campaign section.
 *  - Technical Details carries raw backend ids; the beginner checklist/
 *    advanced views never do.
 *  - loadCampaignDetail() never issues a mutating request.
 *  - The VIEWS/HTML/MODULES sync guard and the P0.2/P0.3 regression suites
 *    stay green with this view added.
 *
 * Mirrors test_admin_dashboard_p0_3_id_fields.test.js: the dashboard is one
 * large inline-script-free file with no build step and no jsdom in this
 * repo, so the relevant functions are extracted as text and executed in a
 * sandboxed vm context against small stand-ins for the DOM/fetch layer.
 *
 * Run with: node --test test_admin_dashboard_p0_5a_campaign_detail.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { execFileSync } = require("node:child_process");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const JS = fs.readFileSync(JS_PATH, "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// Deterministic stand-in for the real ccUtcToKlDisplay (Composer's fixed
// +8h UTC->KL formatter, defined elsewhere in the file) — exact KL string
// formatting isn't part of this suite's contract, just that a start/end
// pair is rendered and "no end date" reads as valid, not a warning.
function ccUtcToKlDisplay(iso) {
  return "KL:" + iso;
}

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign({ console, Object, String, Promise, JSON }, sandboxExtra);
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

// ---------------------------------------------------------------------
// Pure checklist derivation + pure HTML builders — no DOM, no network.
// ---------------------------------------------------------------------
const PURE_SRC = slice(
  JS,
  "  var GC_TYPE_LABELS = {",
  "\n  // ---- Composer + orchestration (DOM-touching)"
);
// P0.15 — gcComputeShareState now calls gcCampaignIdIsLinkSafe, defined in
// an earlier block than the GC_TYPE_LABELS marker PURE_SRC starts from.
const SHARE_SAFE_ID_SRC = slice(JS, "  // ---------- Share-safe campaign_id budget (P0.15) ----------", "\n  // ---------- Campaign ID slug generation (P0.3)");

function loadPure() {
  return runInSandbox(SHARE_SAFE_ID_SRC + "\n" + PURE_SRC + "\nthis.__x = { computeSetupChecklist, gcChecklistProgress, " +
    "gcFirstIncompleteRequiredRow, gcCanTransitionToLive, gcIsReadyToPublish, gcComputeShareState, " +
    "gcCampaignDetailChecklistHtml, gcCampaignDetailContinueHtml, gcCampaignDetailShareHtml, " +
    "gcCampaignDetailAdvancedHtml, gcCampaignDetailTechnicalHtml };", { esc, ccUtcToKlDisplay }).__x;
}

const M = loadPure();

// ---------------------------------------------------------------------
// Fixtures — shaped exactly like campaign_centre.py's gc_campaigns
// documents / API responses.
// ---------------------------------------------------------------------
function tournamentCampaign(overrides) {
  return Object.assign({
    campaign_id: "july-tournament",
    name: "July Tournament",
    type: "tournament",
    status: "draft",
    description: "Top 3 leaderboard prize",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: "2026-10-31T15:59:00Z" },
    destination: { provider_id: "mywin", open_mode: "telegram_web_app", path: "/x", ready: true },
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 3 } }] },
    registration: { enabled: false },
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

function missionPoolCampaign(overrides) {
  return Object.assign({
    campaign_id: "mission-1",
    name: "Trivia Mission",
    type: "mission_pool",
    mechanic: "mission_pool",
    status: "draft",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: null },
    mission_config: { mission_type: "keyword", prompt: "What is the promo code?" },
    mission_pool: { pool_id: "MP-1" },
    registration: { enabled: false },
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

function registrationOnlyCampaign(overrides) {
  return Object.assign({
    campaign_id: "lucky-draw",
    name: "Community Lucky Draw",
    type: "external_website",
    status: "draft",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: null },
    destination: { provider_id: "", open_mode: "telegram_web_app", path: "", ready: false },
    registration: { enabled: true, required_fields: ["full_name", "contact_number"] },
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

const activeProvider = { provider_id: "mywin", name: "MyWin Tournament Site", active: true };
const inactiveProvider = { provider_id: "mywin", name: "MyWin Tournament Site", active: false };
const rewardPool = { pool_id: "MP-1", name: "October Prizes", stock: { available: 42, issued: 3 } };

function rowsByKey(rows) {
  const out = {};
  rows.forEach((r) => { out[r.key] = r; });
  return out;
}

// Functions run inside a vm sandbox build their arrays/object literals in
// that sandbox's own realm, so assert.deepEqual's reference-sensitive
// checks (Array.prototype, etc.) don't treat them as equal to a plain
// literal written in this file even when the contents match. A JSON
// round-trip normalizes both sides back to this realm's plain
// arrays/objects — safe here since every value in play is JSON-safe
// (strings/numbers/booleans/null).
function plain(v) { return JSON.parse(JSON.stringify(v)); }
function keys(rows) { return rows.map((r) => r.key); }

// ---------------------------------------------------------------------
// 1. Complete tournament
// ---------------------------------------------------------------------
test("complete tournament: campaign/when/rewards/destination all complete, 4 applicable steps", () => {
  const rows = M.computeSetupChecklist(tournamentCampaign(), [activeProvider], []);
  assert.deepEqual(plain(keys(rows)), ["campaign", "when", "rewards", "destination"]);
  rows.forEach((r) => assert.equal(r.complete, true, r.key + " should be complete"));
  const progress = M.gcChecklistProgress(rows);
  assert.deepEqual(plain(progress), { completed: 4, total: 4 });
});

// ---------------------------------------------------------------------
// 2. Tournament missing rewards
// ---------------------------------------------------------------------
test("tournament missing rewards: rewards row incomplete, others unaffected", () => {
  const campaign = tournamentCampaign({ reward_config: { rules: [] } });
  const rows = rowsByKey(M.computeSetupChecklist(campaign, [activeProvider], []));
  assert.equal(rows.rewards.complete, false);
  assert.equal(rows.rewards.summary, "No rewards added");
  assert.equal(rows.rewards.actionTarget, "rewards_tournament");
  assert.equal(rows.destination.complete, true);
});

// ---------------------------------------------------------------------
// 3. Tournament missing destination/provider readiness
// ---------------------------------------------------------------------
test("tournament missing destination readiness: destination row incomplete when not ready", () => {
  const campaign = tournamentCampaign({
    destination: { provider_id: "mywin", open_mode: "telegram_web_app", path: "/x", ready: false },
  });
  const rows = rowsByKey(M.computeSetupChecklist(campaign, [activeProvider], []));
  assert.equal(rows.destination.complete, false);
  assert.match(rows.destination.summary, /not marked ready/);
});

test("tournament with inactive provider: destination row incomplete", () => {
  const rows = rowsByKey(M.computeSetupChecklist(tournamentCampaign(), [inactiveProvider], []));
  assert.equal(rows.destination.complete, false);
  assert.match(rows.destination.summary, /inactive/);
});

// ---------------------------------------------------------------------
// 4. mission_pool missing pool
// ---------------------------------------------------------------------
test("mission_pool missing pool: rewards row incomplete with 'no pool linked' summary", () => {
  const campaign = missionPoolCampaign({ mission_pool: { pool_id: "" } });
  const rows = rowsByKey(M.computeSetupChecklist(campaign, [], []));
  assert.equal(rows.rewards.complete, false);
  assert.equal(rows.rewards.summary, "No reward pool linked");
  assert.equal(rows.rewards.actionTarget, "rewards_mission");
});

test("mission_pool with linked pool: rewards row complete and shows stock", () => {
  const rows = rowsByKey(M.computeSetupChecklist(missionPoolCampaign(), [], [rewardPool]));
  assert.equal(rows.rewards.complete, true);
  assert.match(rows.rewards.summary, /42 codes available/);
});

test("mission_pool missing mission_config: mission row incomplete", () => {
  const campaign = missionPoolCampaign({ mission_config: {} });
  const rows = rowsByKey(M.computeSetupChecklist(campaign, [], [rewardPool]));
  assert.equal(rows.mission.complete, false);
  assert.equal(rows.mission.summary, "Mission not configured");
});

// ---------------------------------------------------------------------
// 5. mission_pool hides destination
// ---------------------------------------------------------------------
test("mission_pool campaign never shows a destination row", () => {
  const rows = M.computeSetupChecklist(missionPoolCampaign(), [], [rewardPool]);
  assert.equal(rows.some((r) => r.key === "destination"), false);
  assert.deepEqual(plain(keys(rows)), ["campaign", "when", "mission", "rewards"]);
});

// ---------------------------------------------------------------------
// 6 & 7. registration-only campaign hides destination and counts only
// applicable steps
// ---------------------------------------------------------------------
test("registration-only campaign hides the destination row (registration.enabled skips the gate regardless of type)", () => {
  const rows = M.computeSetupChecklist(registrationOnlyCampaign(), [], []);
  assert.equal(rows.some((r) => r.key === "destination"), false);
});

test("registration-only campaign counts only applicable steps (campaign/when/registration = 3)", () => {
  const rows = M.computeSetupChecklist(registrationOnlyCampaign(), [], []);
  assert.deepEqual(plain(keys(rows)), ["campaign", "when", "registration"]);
  assert.deepEqual(plain(M.gcChecklistProgress(rows)), { completed: 3, total: 3 });
});

test("registration row is never shown for a campaign with registration not enabled", () => {
  const rows = M.computeSetupChecklist(tournamentCampaign(), [activeProvider], []);
  assert.equal(rows.some((r) => r.key === "registration"), false);
});

// ---------------------------------------------------------------------
// 8. No end date is valid
// ---------------------------------------------------------------------
test("no end date is valid: When row is complete and reads 'No end date', not a warning", () => {
  const campaign = tournamentCampaign({ schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: null } });
  const rows = rowsByKey(M.computeSetupChecklist(campaign, [activeProvider], []));
  assert.equal(rows.when.complete, true);
  assert.match(rows.when.summary, /No end date/);
});

test("missing start date: When row incomplete", () => {
  const campaign = tournamentCampaign({ schedule: { starts_at: null, ends_at: null } });
  const rows = rowsByKey(M.computeSetupChecklist(campaign, [activeProvider], []));
  assert.equal(rows.when.complete, false);
});

// ---------------------------------------------------------------------
// 9 & 10. Share link present / missing
// ---------------------------------------------------------------------
test("share link present: uses the server-derived registration_deep_link verbatim", () => {
  const campaign = registrationOnlyCampaign({ registration_deep_link: "https://t.me/AdvantPlayBot?startapp=campaign_lucky-draw" });
  const state = M.gcComputeShareState(campaign);
  assert.equal(state.available, true);
  assert.equal(state.link, "https://t.me/AdvantPlayBot?startapp=campaign_lucky-draw");
});

test("share link missing (registration disabled): explains why, not a blank area", () => {
  const state = M.gcComputeShareState(tournamentCampaign());
  assert.equal(state.available, false);
  assert.match(state.reason, /doesn't have a shareable link/);
});

test("share link missing (registration enabled but no link yet): different reason (bot username)", () => {
  const campaign = registrationOnlyCampaign();
  const state = M.gcComputeShareState(campaign);
  assert.equal(state.available, false);
  assert.match(state.reason, /bot username/);
});

// ---------------------------------------------------------------------
// 11. First incomplete step drives Continue Setup
// ---------------------------------------------------------------------
test("first incomplete required row drives Continue Setup label/target", () => {
  const campaign = tournamentCampaign({ reward_config: { rules: [] } });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  const next = M.gcFirstIncompleteRequiredRow(rows);
  assert.equal(next.key, "rewards");
  const html = M.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Continue Setup → Rewards/);
  assert.match(html, /data-cd-goto="rewards_tournament"/);
});

test("Continue Setup targets the earliest incomplete row, not just any incomplete row", () => {
  const campaign = tournamentCampaign({
    schedule: { starts_at: null, ends_at: null },
    reward_config: { rules: [] },
  });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcFirstIncompleteRequiredRow(rows).key, "when");
});

// ---------------------------------------------------------------------
// 12. All complete -> ready state
// ---------------------------------------------------------------------
test("all complete campaign -> Publish Campaign button, not Continue Setup", () => {
  const campaign = tournamentCampaign();
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcFirstIncompleteRequiredRow(rows), null);
  assert.equal(M.gcIsReadyToPublish(rows, campaign), true);
  const html = M.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Publish Campaign/);
  assert.match(html, /data-gc-action="publish"/);
  assert.doesNotMatch(html, /Continue Setup/);
});

// Regression for a Codex review finding on the first version of this PR:
// Ready-to-Publish must be judged against the real publish gate
// (_transition()'s status-transition validity + field completeness), never
// against effective_visibility.reasons — that field encodes public-
// visibility *timing* (has it started/ended yet), a different question
// from "would clicking Publish succeed right now".

test("draft campaign scheduled to start in the future still shows Publish Campaign (not a visibility/timing question)", () => {
  const campaign = tournamentCampaign({
    schedule: { starts_at: "2027-01-01T00:00:00Z", ends_at: null },
    effective_visibility: {
      publicly_visible: false,
      reasons: ["status is 'draft', not 'live'", "scheduled to start at 2027-01-01T00:00:00+00:00"],
    },
  });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcFirstIncompleteRequiredRow(rows), null);
  assert.equal(M.gcIsReadyToPublish(rows, campaign), true);
  const html = M.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Publish Campaign/);
});

test("archived campaign with every field complete never shows Publish Campaign (archived cannot transition to live)", () => {
  const campaign = tournamentCampaign({ status: "archived" });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcFirstIncompleteRequiredRow(rows), null, "fields are all complete");
  assert.equal(M.gcCanTransitionToLive("archived"), false);
  assert.equal(M.gcIsReadyToPublish(rows, campaign), false);
  const html = M.gcCampaignDetailContinueHtml(rows, campaign);
  assert.doesNotMatch(html, /Publish Campaign/);
  assert.match(html, /can.t be published/);
});

test("ended campaign with every field complete is never Ready to Publish", () => {
  const campaign = tournamentCampaign({ status: "ended" });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcCanTransitionToLive("ended"), false);
  assert.equal(M.gcIsReadyToPublish(rows, campaign), false);
});

test("paused campaign with every field complete is Ready to Publish (paused->live is valid)", () => {
  const campaign = tournamentCampaign({ status: "paused" });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcCanTransitionToLive("paused"), true);
  assert.equal(M.gcIsReadyToPublish(rows, campaign), true);
});

// ---------------------------------------------------------------------
// 13. Technical Details carries raw ids; the beginner view never does
// ---------------------------------------------------------------------
test("Technical Details contains raw campaign/provider/pool ids; checklist and Advanced views never do", () => {
  const campaign = missionPoolCampaign({
    campaign_id: "RAW-CAMPAIGN-ID-9001",
    destination: { provider_id: "RAW-PROVIDER-ID-42" },
    mission_pool: { pool_id: "RAW-POOL-ID-7" },
  });
  const rows = M.computeSetupChecklist(campaign, [], [{ pool_id: "RAW-POOL-ID-7", name: "Prizes", stock: { available: 1 } }]);

  const technicalHtml = M.gcCampaignDetailTechnicalHtml(campaign);
  assert.match(technicalHtml, /RAW-CAMPAIGN-ID-9001/);
  assert.match(technicalHtml, /RAW-PROVIDER-ID-42/);
  assert.match(technicalHtml, /RAW-POOL-ID-7/);

  const checklistHtml = M.gcCampaignDetailChecklistHtml(rows);
  assert.doesNotMatch(checklistHtml, /RAW-CAMPAIGN-ID-9001/);
  assert.doesNotMatch(checklistHtml, /RAW-PROVIDER-ID-42/);
  assert.doesNotMatch(checklistHtml, /RAW-POOL-ID-7/);

  const advancedHtml = M.gcCampaignDetailAdvancedHtml(campaign, []);
  assert.doesNotMatch(advancedHtml, /RAW-CAMPAIGN-ID-9001/);
  assert.doesNotMatch(advancedHtml, /RAW-PROVIDER-ID-42/);
  assert.doesNotMatch(advancedHtml, /RAW-POOL-ID-7/);

  const shareHtml = M.gcCampaignDetailShareHtml(M.gcComputeShareState(campaign));
  assert.doesNotMatch(shareHtml, /RAW-CAMPAIGN-ID-9001/);
});

// ---------------------------------------------------------------------
// 14. No mutation / PUT / POST is ever issued from Campaign Detail
// ---------------------------------------------------------------------
test("Campaign Detail's business-logic block never references a mutating helper", () => {
  const BLOCK_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  function bindCampaignDetail() {");
  // apiPost/apiPostJson/apiPutJson/apiDelete are the only fetch helpers in
  // this file that send a non-GET request (see the top-of-file api()/
  // apiPost()/apiPostJson()/apiPutJson()/apiDelete() definitions) — none of
  // them may appear anywhere in computeSetupChecklist, the HTML builders,
  // or loadCampaignDetail/renderCampaignDetail.
  assert.doesNotMatch(BLOCK_SRC, /\bapiPost\(/);
  assert.doesNotMatch(BLOCK_SRC, /\bapiPostJson\(/);
  assert.doesNotMatch(BLOCK_SRC, /\bapiPutJson\(/);
  assert.doesNotMatch(BLOCK_SRC, /\bapiDelete\(/);
});

test("loadCampaignDetail() only ever calls the GET helper (api), never a mutating one", async () => {
  // Starts after gcCampaignDetailHtml's own definition (that composer is
  // stubbed below — its real behavior is covered by the PURE_SRC tests
  // above) so only renderCampaignDetail/loadCampaignDetail are evaluated.
  const LOAD_SRC = slice(
    JS,
    "  // Entry point from the Campaigns list (\"View Details\"). No history/hash",
    "\n  function bindCampaignDetail() {"
  );

  const calls = { api: [], apiPost: [], apiPostJson: [], apiPutJson: [], apiDelete: [], statePanel: [] };
  const domNodes = {};
  function domFor(sel) {
    if (!domNodes[sel]) domNodes[sel] = { innerHTML: "", textContent: "" };
    return domNodes[sel];
  }
  const fakeCampaign = missionPoolCampaign();

  const sandbox = runInSandbox(LOAD_SRC + "\nthis.__load = loadCampaignDetail;", {
    esc, ccUtcToKlDisplay,
    state: { campaignId: "mission-1" },
    $: function (sel) { return domFor(sel); },
    statePanel: function (id, kind, msg) { calls.statePanel.push([id, kind, msg]); },
    api: function (url) { calls.api.push(url); return Promise.resolve({ status: "ok", campaign: fakeCampaign }); },
    apiPost: function () { calls.apiPost.push(arguments); return Promise.resolve({}); },
    apiPostJson: function () { calls.apiPostJson.push(arguments); return Promise.resolve({}); },
    apiPutJson: function () { calls.apiPutJson.push(arguments); return Promise.resolve({}); },
    apiDelete: function () { calls.apiDelete.push(arguments); return Promise.resolve({}); },
    fetchGcProviders: function () { return Promise.resolve([]); },
    fetchGcRewardPools: function () { return Promise.resolve([{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }]); },
    gcPill: function (status) { return "<span>" + esc(status) + "</span>"; },
    // P0.10: loadCampaignDetail's title pill now calls gcDisplayPill (the
    // Mission-lifecycle-aware wrapper around gcPill) instead of gcPill
    // directly — stubbed here the same way gcPill itself is, since this
    // test only cares about the GET-only wiring property, not pill
    // rendering (covered by the P0.10 suite instead).
    gcDisplayPill: function (c) { return "<span>" + esc((c && c.status) || "") + "</span>"; },
    // Real checklist/HTML derivation is covered by the PURE_SRC tests above;
    // this test only cares that loadCampaignDetail wires GET-only data into
    // the composer and never reaches for a mutating helper, so the composer
    // itself is stubbed out here rather than re-pulling in its whole
    // dependency chain (computeSetupChecklist, gcComputeShareState, ...).
    gcCampaignDetailHtml: function (campaign, providers, pools) {
      return "Back to Campaigns stub for " + campaign.campaign_id + " (providers=" + providers.length + ", pools=" + pools.length + ")";
    },
  });

  sandbox.__load(false);
  // Flush the microtask chain (api().then(...) -> Promise.all([...]).then(...)).
  for (let i = 0; i < 10; i++) { await Promise.resolve(); }

  assert.deepEqual(calls.apiPost, []);
  assert.deepEqual(calls.apiPostJson, []);
  assert.deepEqual(calls.apiPutJson, []);
  assert.deepEqual(calls.apiDelete, []);
  assert.deepEqual(plain(calls.api), ["/api/admin/gc-campaigns/mission-1"]);
  assert.match(domFor("#cd-body").innerHTML, /Back to Campaigns stub for mission-1/);
  assert.match(domFor("#view-title").innerHTML, /Trivia Mission/);
});

test("renderCampaignDetail() sets state.campaignId and only switches the view (no fetch of its own)", () => {
  const RENDER_SRC = slice(
    JS,
    "  // Entry point from the Campaigns list (\"View Details\"). No history/hash",
    "\n  // GET-only: the single campaign fetch"
  );
  const switchCalls = [];
  const sandbox = runInSandbox(RENDER_SRC + "\nthis.__render = renderCampaignDetail;", {
    state: {},
    switchView: function (v) { switchCalls.push(v); },
  });
  sandbox.__render("some-campaign");
  assert.equal(sandbox.state.campaignId, "some-campaign");
  assert.deepEqual(switchCalls, ["campaignDetail"]);
});

// ---------------------------------------------------------------------
// 15 & 16. VIEWS/HTML/MODULES sync guard and the P0.2/P0.3 regression
// suites remain green with this view added.
// ---------------------------------------------------------------------
test("campaignDetail is registered as a VIEWS entry with a matching HTML section", () => {
  const HTML = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.html"), "utf8");
  const VIEWS_SRC = slice(JS, "var VIEWS =[", "];") + "];";
  const views = new Function(VIEWS_SRC + "\nreturn VIEWS;")();
  assert.ok(views.includes("campaignDetail"), "campaignDetail missing from VIEWS");
  assert.match(HTML, /<section\s+id="view-campaignDetail"/);
});

test("VIEWS/HTML/MODULES sync guard and P0.2/P0.3 regression suites still pass", () => {
  execFileSync(process.execPath, [
    "--test",
    "test_admin_dashboard_views_sync.test.js",
    "test_admin_dashboard_p0_2_filters.test.js",
    "test_admin_dashboard_p0_3_id_fields.test.js",
  ], { cwd: __dirname, stdio: "pipe" });
});
