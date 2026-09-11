/**
 * P0.10: Make gc_campaigns display Mission campaigns truthfully.
 *
 * The P0 audit found four lies the Campaign Centre table/Campaign Detail
 * could tell about a Mission Pool campaign:
 *   1. A cancelled Mission (mission_pool.cancelled=true, set by Mission
 *      Admin's admin_cancel_mission) can still show as LIVE — cancelling
 *      never touches gc_campaigns.status.
 *   2. A closed Mission (admin_close_mission sets status="ended") can show
 *      ENDED/Completed even though processing/winner selection hasn't even
 *      started (mission_pool.processing_stage is still "pending").
 *   3. mission_pool.processing_stage's in-between states (processing,
 *      completed) never surfaced in gc_campaigns at all.
 *   4. Campaign Detail's generic "When" editor is minute-precision and can
 *      silently truncate a Mission's second-precision eligibility cutoff
 *      (schedule.ends_at) on save.
 *
 * This suite covers the fix: a pure derived display-state helper
 * (gcEffectiveDisplayState, mirroring mission_pool_ux.operational_state()'s
 * exact precedence) used everywhere gc_campaigns shows a Mission's status,
 * with backend transition legality (GC_VALID_STATUS_TRANSITIONS) left
 * completely untouched — the derived state is presentation-only and is
 * NEVER fed into a lifecycle endpoint.
 *
 * Mirrors the existing P0.4/P0.5a/P0.5b/P0.9/P0.8 harness: no build step,
 * no jsdom in this repo — relevant source ranges are extracted as text and
 * executed in sandboxed vm contexts against small stand-ins for the DOM/
 * fetch layer.
 *
 * Run with: node --test test_admin_dashboard_p0_10_mission_lifecycle.test.js
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

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function ccUtcToKlDisplay(iso) {
  return iso ? "KL:" + iso : "—";
}

function gcPill(status) { return '<span class="pill">' + esc(status || "—") + "</span>"; }

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign({ console, Object, String, Promise, JSON, Array }, sandboxExtra);
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

async function flush(n) {
  for (let i = 0; i < (n || 12); i++) await Promise.resolve();
}

// ---------------------------------------------------------------------
// Source ranges — same markers the existing P0.4/P0.5a/P0.5b/P0.9 suites
// already rely on (see those files' own comments for why each boundary is
// where it is).
// ---------------------------------------------------------------------
const PURE_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Composer + orchestration (DOM-touching)");
// PURE_SRC + the small gap that holds gcCampaignDetailHtml itself (never
// re-implemented as a stub here — this suite wants the REAL composer, so the
// lifecycle banner/CTA integration is proven end to end).
const DETAIL_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  function bindCampaignDetail() {");
const PROVIDER_LABEL_SRC = slice(JS, "  function gcProviderOptionLabel(p) {", "\n  function renderGcProviderSelect()");
const KL_SRC = slice(JS, "  function ccPad2(n)", "\n  var CC_CONTENT_ICON");
const ORCH_SRC = slice(JS, "  // Entry point from the Campaigns list", "\n  // ---------- Mission Reward Pool (Phase 2.1");
const INVALIDATE_SRC = slice(JS, "  function gcInvalidateCampaignsCache() {", "\n  function fetchGcProviders(force)");
const ACTION_SRC = slice(JS, "  var GC_ACTION_ERROR_MESSAGES = {", "\n  function bindGcCampaigns() {");
const PREVIEW_FULL_SRC = INVALIDATE_SRC + "\n" + PURE_SRC + "\n" + ACTION_SRC;

function extractFunctionSource(source, name) {
  const start = source.indexOf("function " + name + "(");
  assert.notEqual(start, -1, name + " not found in admin-dashboard.js");
  let depth = 0;
  let i = source.indexOf("{", start);
  for (; i < source.length; i++) {
    if (source[i] === "{") depth++;
    else if (source[i] === "}") { depth--; if (depth === 0) break; }
  }
  return source.slice(start, i + 1);
}

// gcCampaignRowHtml/gcOverflowMenuHtml call gcMissionActionsHtml, which is
// defined earlier in the file than the "var GC_TYPE_LABELS = {" marker — the
// same prepend-by-name P0.4's own suite already uses.
const MISSION_ACTIONS_SRC = extractFunctionSource(JS, "gcMissionActionsHtml");

function loadPure() {
  return runInSandbox(MISSION_ACTIONS_SRC + "\n" + PURE_SRC + "\nthis.__x = { " +
    "gcIsMissionCampaign, gcEffectiveDisplayState, gcDisplayPill, gcMissionListSummary, " +
    "gcMissionLifecycleBannerHtml, gcListActions, gcGroupCampaigns, gcCampaignRowHtml, " +
    "gcOverflowMenuHtml, computeSetupChecklist, gcCampaignDetailContinueHtml, " +
    "gcCampaignDetailChecklistHtml, GC_VALID_STATUS_TRANSITIONS };",
    { esc, ccUtcToKlDisplay, gcPill }).__x;
}
const M = loadPure();

// Objects built inside a vm sandbox belong to that sandbox's own realm, so
// assert/strict's reference-sensitive deepEqual never treats them as equal
// to a plain literal written in this file even when every field matches
// (see the identical `plain()` helper in test_admin_dashboard_p0_4_campaign_
// list.test.js). A JSON round-trip normalizes a vm-realm value back into
// this file's realm; safe here since every value in play is JSON-safe.
function plain(v) { return JSON.parse(JSON.stringify(v)); }

function loadDetail() {
  return runInSandbox(DETAIL_SRC + "\nthis.__x = { gcCampaignDetailHtml, computeSetupChecklist };",
    { esc, ccUtcToKlDisplay, gcPill }).__x;
}
const D = loadDetail();

// ---------------------------------------------------------------------
// Fixtures — shaped like campaign_centre.py's gc_campaigns documents,
// mission_pool.py's fresh_mission_pool_processing_state()/merge_mission_
// pool_config() field names (processing_stage, cancelled, cancelled_at,
// closed_at) and mission_pool_ux.operational_state()'s own precedence.
// ---------------------------------------------------------------------
function baseMission(overrides) {
  return Object.assign({
    campaign_id: "trivia-1",
    name: "Trivia Mission",
    type: "mission_pool",
    mechanic: "mission_pool",
    status: "draft",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: "2026-10-31T15:59:59Z" },
    mission_config: { mission_type: "keyword", prompt: "What is the promo code?" },
    mission_pool: { pool_id: "MP-1", cancelled: false, processing_stage: "pending" },
    registration: { enabled: false },
    mission_active_rewards: 0,
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

function nonMission(overrides) {
  return Object.assign({
    campaign_id: "july-tournament",
    name: "July Tournament",
    type: "tournament",
    mechanic: "tournament",
    status: "draft",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: "2026-10-31T15:59:00Z" },
    destination: { provider_id: "mywin", open_mode: "telegram_web_app", path: "/x", ready: true },
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 3 } }] },
    registration: { enabled: false },
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

// =======================================================================
// 1. gcEffectiveDisplayState — the derived-state truth table.
// =======================================================================
test("derived state: ordinary draft/scheduled/live/paused Mission falls back to canonical status", () => {
  ["draft", "scheduled", "live", "paused"].forEach((status) => {
    assert.equal(M.gcEffectiveDisplayState(baseMission({ status })), status, status);
  });
});

test("derived state: cancelled Mission is 'cancelled' regardless of campaign.status", () => {
  ["live", "paused", "draft", "scheduled"].forEach((status) => {
    const c = baseMission({ status, mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
    assert.equal(M.gcEffectiveDisplayState(c), "cancelled", "status=" + status);
  });
});

test("derived state: closed (ended, stage pending) is 'closed_needs_processing', never 'completed'", () => {
  const c = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", cancelled: false, processing_stage: "pending" } });
  assert.equal(M.gcEffectiveDisplayState(c), "closed_needs_processing");
});

test("derived state: ended with an in-progress stage is 'processing'", () => {
  ["processing_eligibility", "qualified_snapshot_ready", "selecting_winners", "winners_selected", "allocating_rewards", "notifying"].forEach((stage) => {
    const c = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", cancelled: false, processing_stage: stage } });
    assert.equal(M.gcEffectiveDisplayState(c), "processing", "stage=" + stage);
  });
});

test("derived state: ended with stage 'completed' is 'completed' — canonical status='ended' alone is never enough", () => {
  const c = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", cancelled: false, processing_stage: "completed" } });
  assert.equal(M.gcEffectiveDisplayState(c), "completed");
});

test("derived state: archived follows the exact same stage precedence as ended", () => {
  assert.equal(M.gcEffectiveDisplayState(baseMission({ status: "archived", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } })), "closed_needs_processing");
  assert.equal(M.gcEffectiveDisplayState(baseMission({ status: "archived", mission_pool: { pool_id: "MP-1", processing_stage: "completed" } })), "completed");
});

test("derived state: missing/unknown mission_pool block degrades safely (never claims completed/cancelled)", () => {
  const noBlock = baseMission({ status: "draft" });
  delete noBlock.mission_pool;
  assert.equal(M.gcEffectiveDisplayState(noBlock), "draft");

  const endedNoBlock = baseMission({ status: "ended" });
  delete endedNoBlock.mission_pool;
  assert.equal(M.gcEffectiveDisplayState(endedNoBlock), "closed_needs_processing", "an ended Mission with no lifecycle data must default to needs-processing, never completed");
});

test("derived state: non-Mission campaigns are always exactly the canonical status, unchanged", () => {
  ["draft", "scheduled", "live", "paused", "ended", "archived"].forEach((status) => {
    assert.equal(M.gcEffectiveDisplayState(nonMission({ status })), status);
  });
});

// =======================================================================
// 2. gcDisplayPill — presentation only, never GC_VALID_STATUS_TRANSITIONS.
// =======================================================================
test("gcDisplayPill: cancelled Mission never shows LIVE", () => {
  const c = baseMission({ status: "live", mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const html = M.gcDisplayPill(c);
  assert.match(html, />Cancelled</);
  assert.doesNotMatch(html, />live</i);
});

test("gcDisplayPill: closed-needs-processing Mission never shows Completed/ENDED", () => {
  const c = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  const html = M.gcDisplayPill(c);
  assert.match(html, />Needs processing</);
  assert.doesNotMatch(html, />ended</i);
  assert.doesNotMatch(html, />Completed</);
});

test("gcDisplayPill: processing and completed Mission each get their own plain-English pill", () => {
  const processing = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "selecting_winners" } });
  assert.match(M.gcDisplayPill(processing), />Processing</);

  const completed = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "completed" } });
  assert.match(M.gcDisplayPill(completed), />Completed</);
});

test("gcDisplayPill: ordinary Mission and every non-Mission campaign fall back to gcPill(campaign.status) unchanged", () => {
  assert.equal(M.gcDisplayPill(baseMission({ status: "live" })), gcPill("live"));
  assert.equal(M.gcDisplayPill(nonMission({ status: "draft" })), gcPill("draft"));
  assert.equal(M.gcDisplayPill(nonMission({ status: "ended" })), gcPill("ended"));
});

test("backend legality: GC_VALID_STATUS_TRANSITIONS is untouched and keyed only by canonical statuses — no derived state ever appears in it", () => {
  const derivedStates = ["cancelled", "closed_needs_processing", "processing", "completed"];
  Object.keys(M.GC_VALID_STATUS_TRANSITIONS).forEach((key) => {
    assert.ok(!derivedStates.includes(key), "a derived display state leaked into GC_VALID_STATUS_TRANSITIONS: " + key);
  });
  assert.deepEqual(Object.keys(M.GC_VALID_STATUS_TRANSITIONS).sort(), ["archived", "draft", "ended", "live", "paused", "scheduled"]);
});

// =======================================================================
// 3. Campaign list — grouping / pill / summary / actions.
// =======================================================================
test("list: a cancelled Mission is not grouped under Active even though status is still 'live'", () => {
  const cancelled = baseMission({ campaign_id: "c1", status: "live", mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const live = baseMission({ campaign_id: "c2", status: "live" });
  const groups = M.gcGroupCampaigns([cancelled, live], "");
  const active = groups.find((g) => g.heading === "Active");
  const completed = groups.find((g) => g.heading === "Completed");
  assert.deepEqual(plain(active.items.map((c) => c.campaign_id)), ["c2"], "cancelled must not sit in Active");
  assert.ok(completed, "Completed/Inactive bucket must exist");
  assert.deepEqual(plain(completed.items.map((c) => c.campaign_id)), ["c1"]);
});

test("list: closed-needs-processing and processing Missions land in their own 'Needs Attention' group, not Completed", () => {
  const closed = baseMission({ campaign_id: "c1", status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  const processing = baseMission({ campaign_id: "c2", status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "selecting_winners" } });
  const groups = M.gcGroupCampaigns([closed, processing], "");
  const needsAttention = groups.find((g) => g.heading === "Needs Attention");
  assert.ok(needsAttention, "Needs Attention group must exist");
  assert.deepEqual(plain(needsAttention.items.map((c) => c.campaign_id)).sort(), ["c1", "c2"]);
  assert.equal(groups.find((g) => g.heading === "Completed"), undefined, "nothing here is truly Completed");
});

test("list: a truly completed Mission is grouped under Completed", () => {
  const completed = baseMission({ campaign_id: "c1", status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "completed" } });
  const groups = M.gcGroupCampaigns([completed], "");
  assert.deepEqual(plain(groups.map((g) => g.heading)), ["Completed"]);
});

test("list: non-Mission grouping is completely unchanged (live/paused->Active, draft/scheduled->Upcoming, ended/archived->Completed)", () => {
  const items = [
    nonMission({ campaign_id: "a", status: "live" }),
    nonMission({ campaign_id: "b", status: "paused" }),
    nonMission({ campaign_id: "c", status: "draft" }),
    nonMission({ campaign_id: "d", status: "ended" }),
  ];
  const groups = M.gcGroupCampaigns(items, "");
  assert.deepEqual(plain(groups.map((g) => g.heading)), ["Active", "Upcoming", "Completed"]);
  assert.deepEqual(plain(groups[0].items.map((c) => c.campaign_id)), ["a", "b"]);
});

test("list: a specific status filter still returns one ungrouped bucket (unchanged from P0.4)", () => {
  const groups = M.gcGroupCampaigns([nonMission({ status: "live" })], "live");
  assert.equal(groups.length, 1);
  assert.equal(groups[0].heading, null);
});

test("list: setup summary — cancelled/closed/processing/completed each get their own one-liner; ordinary Mission setup keeps the N/total copy", () => {
  const cancelled = baseMission({ mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  assert.equal(M.gcMissionListSummary(cancelled), "Mission cancelled");

  const closed = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  assert.equal(M.gcMissionListSummary(closed), "Processing required");

  const processing = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "notifying" } });
  assert.equal(M.gcMissionListSummary(processing), "Processing in progress");

  const completed = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "completed" } });
  assert.equal(M.gcMissionListSummary(completed), "Mission completed");

  const ordinary = baseMission({ status: "draft" });
  assert.equal(M.gcMissionListSummary(ordinary), null, "ordinary draft/live/paused Mission keeps the normal checklist-derived summary");

  assert.equal(M.gcMissionListSummary(nonMission()), null, "never applies to non-Mission campaigns");
});

test("list row: cancelled Mission renders the Cancelled pill and 'Mission cancelled' summary, and a closed one surfaces Process Mission next to Manage", () => {
  const cancelled = baseMission({ mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const rows = M.computeSetupChecklist(cancelled, [], []);
  const html = M.gcCampaignRowHtml(cancelled, rows);
  assert.match(html, />Cancelled</);
  assert.match(html, />Mission cancelled</);

  const closed = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  const closedHtml = M.gcCampaignRowHtml(closed, M.computeSetupChecklist(closed, [], []));
  assert.match(closedHtml, /data-gc-action="mission"[^>]*>Process Mission</, "a visible, obvious action next to Manage — not buried only in the kebab menu");

  const processing = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "notifying" } });
  const processingHtml = M.gcCampaignRowHtml(processing, M.computeSetupChecklist(processing, [], []));
  assert.match(processingHtml, /data-gc-action="mission"[^>]*>Open Mission</);
});

// =======================================================================
// 4. Action legality — canonical status still drives GC_VALID_STATUS_
//    TRANSITIONS-derived legality; derived state only hides misleading UI.
// =======================================================================
test("gcListActions: a cancelled Mission (status still 'live') hides Publish/Resume and Pause", () => {
  const cancelled = baseMission({ status: "live", mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const actions = M.gcListActions(cancelled);
  assert.equal(actions.canPublish, false);
  assert.equal(actions.canPause, false);
  // Archive stays governed purely by canonical backend legality — cancelling
  // doesn't change whether tidying the campaign away is sensible.
  assert.equal(actions.canArchive, true, "GC_VALID_STATUS_TRANSITIONS.live still allows archiving a cancelled Mission");
});

test("gcListActions: an ordinary (non-cancelled) live Mission is completely unaffected", () => {
  const live = baseMission({ status: "live" });
  const actions = M.gcListActions(live);
  assert.equal(actions.canPause, true);
  assert.equal(actions.canPublish, false, "already live");
});

test("gcListActions: a cancelled Mission whose status is 'paused' also hides Resume/Pause", () => {
  const cancelledPaused = baseMission({ status: "paused", mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const actions = M.gcListActions(cancelledPaused);
  assert.equal(actions.canPublish, false, "Resume must not be offered on a cancelled Mission");
  assert.equal(actions.canPause, false);
});

test("gcMissionActionsHtml: Close Mission never shows for a cancelled Mission even while status is still 'live'", () => {
  const html = extractFunctionSource(JS, "gcMissionActionsHtml");
  const fn = new Function("esc", "return (" + html + ")")(esc);
  const cancelledLive = { mechanic: "mission_pool", status: "live", mission_pool: { cancelled: true }, mission_active_rewards: 0 };
  assert.doesNotMatch(fn(cancelledLive), /data-gc-action="close-mission"/);

  const ordinaryLive = { mechanic: "mission_pool", status: "live", mission_pool: { cancelled: false }, mission_active_rewards: 0 };
  assert.match(fn(ordinaryLive), /data-gc-action="close-mission"/, "an un-cancelled live Mission is unaffected");
});

test("gcMissionActionsHtml: End Rewards is unaffected by cancellation (already-allocated vouchers are never reclaimed)", () => {
  const html = extractFunctionSource(JS, "gcMissionActionsHtml");
  const fn = new Function("esc", "return (" + html + ")")(esc);
  const cancelled = { mechanic: "mission_pool", status: "live", mission_pool: { cancelled: true }, mission_active_rewards: 3 };
  assert.match(fn(cancelled), /data-gc-action="end-rewards"/);
});

test("wiring: gcListActions never appears inside GC_VALID_STATUS_TRANSITIONS or any lifecycle endpoint body — derived state stays a display concept only", () => {
  const src = extractFunctionSource(JS, "gcListActions");
  assert.doesNotMatch(src, /apiPost|apiPutJson|apiPostJson/, "gcListActions must stay a pure, read-only decision function");
});

// =======================================================================
// 5. Campaign Detail — lifecycle banner + CTA (real gcCampaignDetailHtml,
//    not a stub, per the DETAIL_SRC harness above).
// =======================================================================
test("Campaign Detail: cancelled Mission shows 'Mission cancelled' and never Publish/Resume", () => {
  const cancelled = baseMission({ status: "live", mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const html = D.gcCampaignDetailHtml(cancelled, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }], null);
  assert.match(html, /Mission cancelled/);
  assert.doesNotMatch(html, /Publish Campaign/);
  assert.doesNotMatch(html, /Resume Campaign/);
  assert.doesNotMatch(html, /data-gc-action="publish"/);
});

test("Campaign Detail: closed-needs-processing shows the processing-required action, not a completed message", () => {
  const closed = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  const html = D.gcCampaignDetailHtml(closed, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }], null);
  assert.match(html, /Mission closed — processing required/);
  assert.match(html, /data-cd-mission-process="1"[^>]*>Process Mission</);
  assert.doesNotMatch(html, /Mission completed/);
  assert.doesNotMatch(html, /Publish Campaign/);
});

test("Campaign Detail: processing shows an Open Mission action routed to Mission Admin", () => {
  const processing = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "allocating_rewards" } });
  const html = D.gcCampaignDetailHtml(processing, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }], null);
  assert.match(html, /Processing rewards\/winners/);
  assert.match(html, /data-cd-mission-process="1"[^>]*>Open Mission</);
});

test("Campaign Detail: a truly completed Mission shows the completed state, not setup/publish CTAs", () => {
  const completed = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "completed" } });
  const html = D.gcCampaignDetailHtml(completed, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }], null);
  assert.match(html, /Mission completed/);
  assert.doesNotMatch(html, /Publish Campaign/);
  assert.doesNotMatch(html, /Continue Setup/);
});

test("Campaign Detail: setup progress never overrides lifecycle truth — checklist can say 'Setup 4 / 4 complete' while the lifecycle banner still says cancelled/closed", () => {
  const closed = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  const rows = D.computeSetupChecklist(closed, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }]);
  assert.equal(rows.filter((r) => r.applicable && !r.complete).length, 0, "checklist itself is fully configured");
  const html = D.gcCampaignDetailHtml(closed, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }], null);
  assert.match(html, /Setup 4 \/ 4 complete/);
  assert.match(html, /Mission closed — processing required/, "the lifecycle banner must still tell the truth alongside a complete checklist");
});

test("Campaign Detail: an ordinary live/paused/draft/scheduled Mission is completely unaffected (normal Setup/Publish flow)", () => {
  const live = baseMission({ status: "live" });
  const html = D.gcCampaignDetailHtml(live, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }], null);
  assert.match(html, /is live/);
  assert.doesNotMatch(html, /Mission cancelled|processing required|Mission completed/);
});

test("Campaign Detail: cancelled Mission's checklist 'When' row is untouched, still routes to Mission Admin (no inline edit leaks through cancellation)", () => {
  const cancelled = baseMission({ status: "live", mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const rows = D.computeSetupChecklist(cancelled, [], [{ pool_id: "MP-1", name: "Prizes", stock: { available: 5 } }]);
  const html = M.gcCampaignDetailChecklistHtml(rows, { editingSection: null, campaign: cancelled, providers: [] });
  assert.match(html, /data-cd-when-mission-edit="1"[^>]*>Edit in Mission Admin</);
  assert.doesNotMatch(html, /data-cd-edit="when"/);
});

// =======================================================================
// 6. The "When" editor safety fix — the critical suite.
// =======================================================================
test("When editor: a Mission's 'when' row never renders the generic inline [Edit] button", () => {
  const mission = baseMission();
  const rows = M.computeSetupChecklist(mission, [], [{ pool_id: "MP-1" }]);
  const html = M.gcCampaignDetailChecklistHtml(rows, { editingSection: null, campaign: mission, providers: [] });
  assert.doesNotMatch(html, /data-cd-edit="when"/);
  assert.match(html, /Edit in Mission Admin/);
});

test("When editor: a Mission's 'when' row keeps this even mid-edit of another section (editingSection='campaign')", () => {
  const mission = baseMission();
  const rows = M.computeSetupChecklist(mission, [], [{ pool_id: "MP-1" }]);
  const html = M.gcCampaignDetailChecklistHtml(rows, { editingSection: "campaign", campaign: mission, providers: [] });
  assert.doesNotMatch(html, /data-cd-edit="when"/);
  assert.match(html, /Edit in Mission Admin/);
});

test("When editor: non-Mission campaigns keep the generic inline editor completely unchanged", () => {
  const t = nonMission();
  const rows = M.computeSetupChecklist(t, [{ provider_id: "mywin", active: true }], []);
  const html = M.gcCampaignDetailChecklistHtml(rows, { editingSection: null, campaign: t, providers: [] });
  assert.match(html, /data-cd-edit="when">Edit</);
  assert.doesNotMatch(html, /Edit in Mission Admin/);
});

function loadOrchestration(overrides) {
  const domNodes = {};
  function node(sel) {
    if (!domNodes[sel]) domNodes[sel] = { value: "", checked: false, textContent: "", innerHTML: "", style: {} };
    return domNodes[sel];
  }
  const calls = { api: [], apiPutJson: [], openMissionEdit: [], openMissionAdmin: [], toast: [] };
  const apiQueue = [];
  const sandboxBase = {
    esc,
    state: { campaignId: "trivia-1" },
    $: node,
    $all: () => [],
    switchView: () => {},
    statePanel: () => {},
    activateTab: () => {},
    openMissionAdmin: (id) => { calls.openMissionAdmin.push(id); },
    openMissionEdit: (id) => { calls.openMissionEdit.push(id); },
    openCampaignRegistrationConfig: () => {},
    fetchGcProviders: () => Promise.resolve([]),
    fetchGcRewardPools: () => Promise.resolve([]),
    gcPill: (status) => "<span>" + esc(status) + "</span>",
    toast: (msg, kind) => { calls.toast.push([msg, kind]); },
    api: (url) => {
      calls.api.push(url);
      const next = apiQueue.shift();
      return next ? Promise.resolve(next) : Promise.reject(new Error("no api() response queued"));
    },
    apiPutJson: (url, body) => {
      calls.apiPutJson.push({ url, body });
      return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } });
    },
    gcCampaignDetailHtml: (campaign) => "rendered:" + campaign.campaign_id,
  };
  Object.assign(sandboxBase, overrides || {});
  const fullSrc = PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\n" + ORCH_SRC +
    "\nthis.cdSaveSection = cdSaveSection; this.cdOpenEdit = cdOpenEdit; this.cdViewState = cdViewState; " +
    "this.__triggerClick = function (selector, dataset) { " +
    "  var target = { closest: function (sel) { return sel === selector ? { dataset: dataset || {} } : null; } }; " +
    "  document._trigger('click', { target: target }); " +
    "};";
  // A minimal fake `document` good enough for bindCampaignDetail's delegated
  // click handler (addEventListener/_trigger only — no real DOM needed).
  const listeners = {};
  sandboxBase.document = {
    addEventListener: (evt, fn) => { (listeners[evt] = listeners[evt] || []).push(fn); },
    _trigger: (evt, e) => { (listeners[evt] || []).forEach((fn) => fn(e)); },
  };
  const sandbox = runInSandbox(fullSrc, sandboxBase);
  sandbox.bindCampaignDetail();
  return { sandbox, calls, apiQueue, node };
}

test("When editor: cdOpenEdit refuses to open the inline form for a Mission's 'when' section (defense in depth)", () => {
  const { sandbox } = loadOrchestration();
  sandbox.cdViewState.campaign = baseMission();
  sandbox.cdOpenEdit("when");
  assert.notEqual(sandbox.cdViewState.editingSection, "when", "a Mission must never enter the inline When edit state");
});

test("When editor: cdOpenEdit still opens normally for a Mission's other editable sections", () => {
  const { sandbox } = loadOrchestration();
  sandbox.cdViewState.campaign = baseMission();
  sandbox.cdOpenEdit("campaign");
  assert.equal(sandbox.cdViewState.editingSection, "campaign");
});

test("When editor: cdSaveSection('when', ...) for a Mission issues NO api()/apiPutJson() call at all — Campaign Detail can never PUT a schedule for a Mission", async () => {
  const { sandbox, calls, node } = loadOrchestration();
  sandbox.cdViewState.campaign = baseMission();
  node("#cd-edit-starts").value = "2026-10-31T23:59";
  sandbox.cdSaveSection("when", { disabled: false, textContent: "" });
  await flush();
  assert.deepEqual(calls.api, [], "no canonical re-GET may ever happen for a Mission's when-save");
  assert.deepEqual(calls.apiPutJson, [], "no PUT of any kind may ever be sent");
});

test("When editor: cdSaveSection('when', ...) for a NON-Mission campaign is completely unaffected (still PUTs schedule)", async () => {
  const { sandbox, calls, apiQueue, node } = loadOrchestration();
  sandbox.cdViewState.campaign = nonMission();
  const fresh = nonMission();
  apiQueue.push({ status: "ok", campaign: fresh });
  apiQueue.push({ status: "ok", campaign: fresh });
  node("#cd-edit-starts").value = "2026-10-02T08:00";
  node("#cd-edit-no-end").checked = false;
  node("#cd-edit-ends").value = "2026-10-31T23:59";
  sandbox.cdSaveSection("when", {});
  await flush();
  assert.equal(calls.apiPutJson.length, 1);
  assert.deepEqual(plain(Object.keys(calls.apiPutJson[0].body)), ["schedule"]);
});

test("When editor: the [Edit in Mission Admin] button routes to openMissionEdit for the current campaign, never opens the inline form", () => {
  const { sandbox, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = baseMission();
  sandbox.__triggerClick("[data-cd-when-mission-edit]", {});
  assert.deepEqual(calls.openMissionEdit, ["trivia-1"]);
  assert.notEqual(sandbox.cdViewState.editingSection, "when");
});

test("When editor: Continue Setup's target='when' for an incomplete Mission schedule also routes to Mission Admin, never the inline editor", () => {
  const { sandbox, calls } = loadOrchestration();
  const incomplete = baseMission({ schedule: { starts_at: null, ends_at: null } });
  sandbox.cdViewState.campaign = incomplete;
  sandbox.__triggerClick("[data-cd-goto]", { cdGoto: "when" });
  assert.deepEqual(calls.openMissionEdit, ["trivia-1"]);
  assert.notEqual(sandbox.cdViewState.editingSection, "when");
});

test("Process Mission / Open Mission CTA button routes to openMissionAdmin with the campaign id", () => {
  const { sandbox, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  sandbox.__triggerClick("[data-cd-mission-process]", { id: "trivia-1" });
  assert.deepEqual(calls.openMissionAdmin, ["trivia-1"]);
});

// =======================================================================
// 7. Preview — derived Mission display state, using cached campaign data.
// =======================================================================
function loadPreview(overrides) {
  const sandboxBase = {
    esc, ccUtcToKlDisplay, gcPill,
    state: { view: "gcCampaigns", campaignId: null },
    cdViewState: { campaign: null },
    gcOptionsCache: { providers: [], providersPromise: null, campaigns: [], campaignsPromise: Promise.resolve([]) },
    gcKnownCampaignIds: {},
    document: { body: { children: [] }, createElement: () => ({ style: {}, appendChild() {}, addEventListener() {} }), addEventListener() {} },
    toast: () => {},
    loadGcCampaigns: () => {},
    loadCampaignDetail: () => {},
  };
  Object.assign(sandboxBase, overrides || {});
  return runInSandbox(PREVIEW_FULL_SRC, sandboxBase);
}

test("Preview: a cancelled Mission's badge is 'Cancelled', never a LIVE status badge, when cached campaign data is available", () => {
  const sandbox = loadPreview();
  const cachedMission = baseMission({ status: "live", mission_pool: { pool_id: "MP-1", cancelled: true, processing_stage: "pending" } });
  const resp = { status: "ok", card: { name: "Trivia Mission", description: "", button_text: "", banner_url: "" }, effective_visibility: { publicly_visible: false, reasons: [] } };
  const html = sandbox.gcPreviewModalBodyHtml(resp, cachedMission);
  assert.match(html, />Cancelled</);
  assert.doesNotMatch(html, />live</i);
});

test("Preview: a closed-needs-processing Mission's badge never implies Completed", () => {
  const sandbox = loadPreview();
  const cachedMission = baseMission({ status: "ended", mission_pool: { pool_id: "MP-1", processing_stage: "pending" } });
  const resp = { status: "ok", card: { name: "Trivia Mission", description: "", button_text: "", banner_url: "" }, effective_visibility: { publicly_visible: false, reasons: [] } };
  const html = sandbox.gcPreviewModalBodyHtml(resp, cachedMission);
  assert.match(html, />Needs processing</);
  assert.doesNotMatch(html, />Completed</);
});

test("Preview: a non-Mission campaign's badge is unaffected (still gcPill(status) verbatim)", () => {
  const sandbox = loadPreview();
  const cached = nonMission({ status: "scheduled" });
  const resp = { status: "ok", card: { name: "July Tournament", description: "", button_text: "", banner_url: "" }, effective_visibility: { publicly_visible: false, reasons: [] } };
  const html = sandbox.gcPreviewModalBodyHtml(resp, cached);
  assert.match(html, />scheduled</);
});

test("Preview: no cached campaign at all falls back to the admin_badges draft hint, unchanged from before P0.10", () => {
  const sandbox = loadPreview();
  const resp = { status: "ok", card: { name: "X", description: "", button_text: "", banner_url: "" }, admin_badges: ["draft"], effective_visibility: { publicly_visible: false, reasons: [] } };
  const html = sandbox.gcPreviewModalBodyHtml(resp, null);
  assert.match(html, />draft</);
});

// =======================================================================
// 8. Cache invalidation (Codex review, P1) — gcEffectiveDisplayState is
// only as truthful as the cached campaign it reads. Without this, an
// operator who cancels/closes/resumes/processes a Mission from Mission
// Admin and returns to Player Campaigns would still see the loaded-before-
// the-mutation gcOptionsCache.campaigns entry — the exact "cancelled shows
// Live" bug this PR fixes, reintroduced by staleness instead of by a wrong
// derivation. mission-admin.js's postAction() is the single choke point
// every lifecycle mutation (close/cancel/resume/process/end_rewards/
// publish/pause) already flows through, so that's the one place this hook
// needs to fire from.
// =======================================================================
const MISSION_JS_PATH = path.join(__dirname, "static", "mission-admin.js");

function freshMissionModule() {
  delete require.cache[require.resolve(MISSION_JS_PATH)];
  return require(MISSION_JS_PATH);
}

function makeMissionHost(routes, extra) {
  const calls = [];
  function respond(method, pathname) {
    calls.push({ method, path: pathname });
    const handler = routes[method + " " + pathname];
    return handler ? Promise.resolve(handler) : Promise.reject(new Error("no route for " + method + " " + pathname));
  }
  return Object.assign({
    $: () => undefined,
    esc: (v) => String(v == null ? "" : v),
    api: (p) => respond("GET", p),
    apiPost: (p) => respond("POST", p),
    apiPostJson: (p) => respond("POSTJ", p),
    apiPutJson: (p) => respond("PUTJ", p),
    toast: () => {},
    confirm: () => true,
    copy: () => {},
  }, extra || {}, { __calls: calls });
}

async function flushMission(n) {
  for (let i = 0; i < (n || 6); i++) await Promise.resolve();
}

["close", "cancel", "resume", "process", "end_rewards"].forEach((action) => {
  test("mission-admin.js: a successful '" + action + "' invalidates the host's campaigns cache when provided", async () => {
    const mod = freshMissionModule();
    let invalidated = 0;
    const endpointAction = action === "end_rewards" ? "end-rewards" : action;
    const host = makeMissionHost(
      {
        ["POST /api/admin/mission-pool/m1/" + endpointAction]: { status: "ok", count_affected: 1 },
        "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: { campaign_id: "m1", type: "mission_pool", status: "live" } },
        "GET /api/admin/mission-pool/m1/edit-state": { status: "ok", reward: { sufficient: true } },
        "GET /api/admin/mission-pool/m1/summary": { status: "ok", grains: {} },
      },
      { invalidateCampaignsCache: () => { invalidated++; } }
    );
    mod.init(host);
    await mod.dispatch(action, "m1");
    await flushMission();
    assert.equal(invalidated, 1, "invalidateCampaignsCache must fire exactly once on a successful " + action);
  });
});

test("mission-admin.js: a FAILED action never invalidates the cache", async () => {
  const mod = freshMissionModule();
  let invalidated = 0;
  const host = makeMissionHost(
    {
      "POST /api/admin/mission-pool/m1/close": { status: "error", code: "already_closed" },
      "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: { campaign_id: "m1", type: "mission_pool", status: "live" } },
      "GET /api/admin/mission-pool/m1/edit-state": { status: "ok", reward: { sufficient: true } },
      "GET /api/admin/mission-pool/m1/summary": { status: "ok", grains: {} },
    },
    { invalidateCampaignsCache: () => { invalidated++; } }
  );
  mod.init(host);
  await mod.dispatch("close", "m1");
  await flushMission();
  assert.equal(invalidated, 0, "a rejected mutation changed nothing server-side and must not invalidate the cache");
});

test("mission-admin.js: postAction never throws when host provides no invalidateCampaignsCache (back-compat for existing hosts/tests)", async () => {
  const mod = freshMissionModule();
  const host = makeMissionHost({
    "POST /api/admin/mission-pool/m1/close": { status: "ok" },
    "GET /api/admin/gc-campaigns/m1": { status: "ok", campaign: { campaign_id: "m1", type: "mission_pool", status: "live" } },
    "GET /api/admin/mission-pool/m1/edit-state": { status: "ok", reward: { sufficient: true } },
    "GET /api/admin/mission-pool/m1/summary": { status: "ok", grains: {} },
  });
  delete host.invalidateCampaignsCache;
  mod.init(host);
  await assert.doesNotReject(async () => { await mod.dispatch("close", "m1"); await flushMission(); });
});

test("wiring: admin-dashboard.js's loadMissionPool() passes gcInvalidateCampaignsCache into mission-admin.js's init()", () => {
  const src = extractFunctionSource(JS, "loadMissionPool");
  assert.match(src, /invalidateCampaignsCache:\s*gcInvalidateCampaignsCache/);
});
