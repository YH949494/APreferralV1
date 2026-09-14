/**
 * P0.4: Campaigns list redesign — replaces the dense gc_campaigns table
 * (name/type/status/visibility + a wall of equally-weighted buttons) with
 * simple rows whose only actions are a primary "Manage" (opens the existing
 * P0.5a Campaign Detail) and a "•••" overflow of the rare/status-gated
 * actions.
 *
 * Covers:
 *  - A row renders name / human type label / status pill / date range /
 *    setup summary / Manage — and never a raw campaign_id, provider_id,
 *    pool_id, or raw type/mechanic enum as visible text.
 *  - Manage always opens Campaign Detail for that exact campaign_id.
 *  - gcListActions()/GC_VALID_STATUS_TRANSITIONS/GC_DELETABLE_STATUSES mirror
 *    campaign_centre.py's own status machine — only legal overflow actions
 *    ever render, and Delete only where the backend allows it.
 *  - gcOverflowMenuHtml reuses gcMissionActionsHtml (untouched, still
 *    covered directly by test_mission_admin_ui.test.js) rather than
 *    reimplementing Close Mission / End Rewards legality.
 *  - The existing typed-confirmation delete modal (openGcDeleteModal) is
 *    untouched — see test_campaign_centre_delete_ui.test.js.
 *  - gcSetupSummary reuses computeSetupChecklist (P0.5a) — same source of
 *    truth as Campaign Detail, never a second/looser notion of "ready".
 *  - Status filtering (gcGroupCampaigns) and empty states
 *    (gcEmptyStateForFilter) for every filter value.
 *  - Unknown campaign type degrades to a cleaned label, not raw snake_case.
 *  - Performance: loadGcCampaigns issues exactly one campaigns fetch, one
 *    providers fetch, and a pools fetch only when a row needs one — never
 *    one request per row.
 *
 * Mirrors test_admin_dashboard_p0_5a_campaign_detail.test.js: the dashboard
 * is one large inline-script-free file with no build step and no jsdom in
 * this repo, so the relevant functions are extracted as text and executed
 * in a sandboxed vm context against small stand-ins for the DOM/fetch layer.
 *
 * Run with: node --test test_admin_dashboard_p0_4_campaign_list.test.js
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

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign({ console, Object, String, Promise, JSON, Array }, sandboxExtra);
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

// Deterministic stand-in for the real ccUtcToKlDisplay (Composer's fixed
// +8h UTC->KL formatter, defined elsewhere in the file) — computeSetupChecklist's
// "When" row calls it via gcScheduleRangeSummary; exact KL string formatting
// isn't part of this suite's contract, just that a value comes back.
function ccUtcToKlDisplay(iso) {
  return "KL:" + iso;
}

// Functions run inside a vm sandbox build their arrays/object literals in
// that sandbox's own realm, so assert.deepEqual's reference-sensitive
// checks don't treat them as equal to a plain literal written in this file
// even when the contents match. A JSON round-trip normalizes both sides
// back to this realm's plain arrays/objects (safe here — every value in
// play is JSON-safe).
function plain(v) { return JSON.parse(JSON.stringify(v)); }

// The P0.4 pure helpers all live contiguously in the same block P0.5a's
// suite already extracts (var GC_TYPE_LABELS ... up to the beginner-view
// HTML builders comment) — gcPill/gcMissionActionsHtml are defined earlier
// in the file and are prepended by name so gcCampaignRowHtml/
// gcOverflowMenuHtml (which call them) resolve correctly.
const PURE_SRC =
  extractFunctionSource(JS, "gcPill") + "\n" +
  extractFunctionSource(JS, "gcMissionActionsHtml") + "\n" +
  slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Pure HTML builders (beginner view");

function loadPure() {
  return runInSandbox(PURE_SRC + "\nthis.__x = { " +
    "gcTypeLabel, gcHumanizeFallback, gcPill, gcMissionActionsHtml, " +
    "computeSetupChecklist, gcChecklistProgress, gcSetupSummary, gcListDateSummary, " +
    "GC_VALID_STATUS_TRANSITIONS, gcCanTransitionTo, gcCanTransitionToLive, GC_DELETABLE_STATUSES, " +
    "gcListActions, gcOverflowMenuHtml, gcCampaignRowHtml, gcGroupCampaigns, gcEmptyStateForFilter " +
    "};", { esc, ccUtcToKlDisplay }).__x;
}

const M = loadPure();

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------
function tournamentCampaign(overrides) {
  return Object.assign({
    campaign_id: "july-tournament",
    name: "July Tournament",
    type: "tournament",
    status: "draft",
    mechanic: "tournament",
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
    status: "live",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: null },
    mission_config: { mission_type: "keyword", prompt: "What is the promo code?" },
    mission_pool: { pool_id: "MP-1" },
    registration: { enabled: false },
    mission_active_rewards: 0,
    effective_visibility: { publicly_visible: true, reasons: [] },
  }, overrides || {});
}

const activeProvider = { provider_id: "mywin", name: "MyWin Tournament Site", active: true };

// ---------------------------------------------------------------------
// 1. Row content — name / human type / status / dates / setup / Manage,
//    never raw ids/enums.
// ---------------------------------------------------------------------
test("a complete tournament row renders name, human type, status pill, dates, setup summary, and Manage", () => {
  const campaign = tournamentCampaign();
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  const html = M.gcCampaignRowHtml(campaign, rows);

  assert.match(html, />July Tournament</, "campaign name must render");
  assert.match(html, />Tournament</, "human type label must render, not the raw enum");
  assert.doesNotMatch(html, />tournament</, "the raw snake_case/lowercase type enum must never render as visible text");
  assert.match(html, /class="pill neutral">draft</, "status pill must reflect the backend status");
  assert.match(html, /2026-10-01/, "start date must render");
  assert.match(html, /2026-10-31/, "end date must render");
  assert.match(html, />Setup complete</);
  assert.match(html, /data-gc-action="detail" data-id="july-tournament">Manage</);
});

test("row never exposes campaign_id, provider_id, or pool_id as visible text (only in data-id attributes)", () => {
  const campaign = missionPoolCampaign();
  const rows = M.computeSetupChecklist(campaign, [], [{ pool_id: "MP-1", name: "October Prizes", stock: { available: 5 } }]);
  const html = M.gcCampaignRowHtml(campaign, rows);

  // Strip every id-carrying attribute value (data-id / data-name / the
  // row-click delegate's data-gc-row-id / aria-label, which also echoes the
  // name), then the remaining text must not contain the raw ids at all.
  const visibleOnly = html
    .replace(/data-id="[^"]*"/g, "")
    .replace(/data-name="[^"]*"/g, "")
    .replace(/data-gc-row-id="[^"]*"/g, "")
    .replace(/aria-label="[^"]*"/g, "");
  assert.ok(!visibleOnly.includes("mission-1"), "campaign_id leaked outside data-id");
  assert.ok(!visibleOnly.includes("MP-1"), "pool_id leaked into visible row text");
  assert.ok(!visibleOnly.includes("provider_id"));
});

test("unknown campaign type degrades to a cleaned label, not raw snake_case", () => {
  assert.equal(M.gcTypeLabel("seasonal_lucky_wheel"), "Seasonal Lucky Wheel");
  assert.equal(M.gcTypeLabel(""), "Unknown type");
  assert.equal(M.gcTypeLabel(undefined), "Unknown type");
  // Known types are unaffected.
  assert.equal(M.gcTypeLabel("mission_pool"), "Mission");
});

// ---------------------------------------------------------------------
// 2. Manage opens Campaign Detail for the correct campaign id.
// ---------------------------------------------------------------------
test("Manage carries data-gc-action=\"detail\" with this row's exact campaign_id", () => {
  const a = M.gcCampaignRowHtml(tournamentCampaign({ campaign_id: "aaa" }), []);
  const b = M.gcCampaignRowHtml(tournamentCampaign({ campaign_id: "bbb" }), []);
  assert.match(a, /data-gc-action="detail" data-id="aaa"/);
  assert.match(b, /data-gc-action="detail" data-id="bbb"/);
  assert.doesNotMatch(a, /data-id="bbb"/);
});

test("the document-level row-click handler routes to renderCampaignDetail(id), skipping clicks on real controls", () => {
  // Structural: locate the row-click delegation added in bindGcCampaigns and
  // confirm it guards on button/.gc-row-menu before calling renderCampaignDetail.
  const bindSrc = extractFunctionSource(JS, "bindGcCampaigns");
  assert.match(bindSrc, /data-gc-row-id/);
  assert.match(bindSrc, /e\.target\.closest\("button"\)/);
  assert.match(bindSrc, /renderCampaignDetail\(card\.dataset\.gcRowId\)/);
});

// ---------------------------------------------------------------------
// 3. Legal overflow actions per status (mirrors campaign_centre.py's
//    _VALID_STATUS_TRANSITIONS / _DELETABLE_STATUSES).
// ---------------------------------------------------------------------
test("GC_VALID_STATUS_TRANSITIONS mirrors campaign_centre._VALID_STATUS_TRANSITIONS exactly", () => {
  assert.deepEqual(JSON.parse(JSON.stringify(M.GC_VALID_STATUS_TRANSITIONS)), {
    draft: ["draft", "scheduled", "live", "archived"],
    scheduled: ["scheduled", "live", "draft", "paused", "archived"],
    live: ["live", "paused", "ended", "archived"],
    paused: ["paused", "live", "ended", "archived"],
    ended: ["ended", "archived"],
    archived: ["archived"],
  });
});

test("GC_DELETABLE_STATUSES mirrors campaign_centre._DELETABLE_STATUSES exactly", () => {
  assert.deepEqual(JSON.parse(JSON.stringify(M.GC_DELETABLE_STATUSES)).sort(), ["archived", "draft", "ended"]);
});

const ALL_STATUSES = ["draft", "scheduled", "live", "paused", "ended", "archived"];

test("Publish/Resume only offered where campaign_centre._transition() would accept a 'live' target, and never on an already-live row", () => {
  ALL_STATUSES.forEach((status) => {
    const actions = M.gcListActions({ status });
    const expected = M.gcCanTransitionTo(status, "live") && status !== "live";
    assert.equal(actions.canPublish, expected, "status=" + status);
  });
  assert.equal(M.gcListActions({ status: "paused" }).publishLabel, "Resume");
  assert.equal(M.gcListActions({ status: "draft" }).publishLabel, "Publish");
});

test("Pause only offered from scheduled/live, never a no-op re-pause", () => {
  ALL_STATUSES.forEach((status) => {
    const actions = M.gcListActions({ status });
    const expected = M.gcCanTransitionTo(status, "paused") && status !== "paused";
    assert.equal(actions.canPause, expected, "status=" + status);
  });
  assert.equal(M.gcListActions({ status: "scheduled" }).canPause, true);
  assert.equal(M.gcListActions({ status: "live" }).canPause, true);
  assert.equal(M.gcListActions({ status: "draft" }).canPause, false);
  assert.equal(M.gcListActions({ status: "paused" }).canPause, false);
});

test("Archive offered from every status except already-archived", () => {
  ALL_STATUSES.forEach((status) => {
    assert.equal(M.gcListActions({ status }).canArchive, status !== "archived", "status=" + status);
  });
});

test("Delete only offered where the backend allows permanent deletion (draft/archived/ended)", () => {
  ALL_STATUSES.forEach((status) => {
    const expected = ["draft", "archived", "ended"].indexOf(status) !== -1;
    assert.equal(M.gcListActions({ status }).canDelete, expected, "status=" + status);
  });
});

test("an ended campaign never offers Publish (illegal transition), matches the backend gate", () => {
  const actions = M.gcListActions({ status: "ended" });
  const html = M.gcOverflowMenuHtml({ campaign_id: "x", status: "ended" }, actions);
  assert.doesNotMatch(html, /data-gc-action="publish"/);
  assert.doesNotMatch(html, /data-gc-action="pause"/);
  assert.match(html, /data-gc-action="archive"/);
  assert.match(html, /data-gc-action="delete"/);
});

test("an archived campaign offers only Preview/Registration/Duplicate — no Publish/Pause/Archive, Delete still legal", () => {
  const actions = M.gcListActions({ status: "archived" });
  const html = M.gcOverflowMenuHtml({ campaign_id: "x", status: "archived" }, actions);
  assert.doesNotMatch(html, /data-gc-action="publish"/);
  assert.doesNotMatch(html, /data-gc-action="pause"/);
  assert.doesNotMatch(html, /data-gc-action="archive"/);
  assert.match(html, /data-gc-action="delete"/);
  assert.match(html, /data-gc-action="preview"/);
  assert.match(html, /data-gc-action="duplicate"/);
});

test("a live campaign offers Pause/Archive but never Publish/Delete", () => {
  const actions = M.gcListActions({ status: "live" });
  const html = M.gcOverflowMenuHtml({ campaign_id: "x", status: "live" }, actions);
  assert.doesNotMatch(html, /data-gc-action="publish"/);
  assert.match(html, /data-gc-action="pause"/);
  assert.match(html, /data-gc-action="archive"/);
  assert.doesNotMatch(html, /data-gc-action="delete"/);
});

test("a paused campaign offers Resume (publish action, relabelled) and Archive, never Pause again", () => {
  const actions = M.gcListActions({ status: "paused" });
  const html = M.gcOverflowMenuHtml({ campaign_id: "x", status: "paused" }, actions);
  assert.match(html, /data-gc-action="publish"[^>]*>Resume</);
  assert.doesNotMatch(html, /data-gc-action="pause"/);
  assert.match(html, /data-gc-action="archive"/);
});

test("Duplicate and Preview are always offered regardless of status", () => {
  ALL_STATUSES.forEach((status) => {
    const html = M.gcOverflowMenuHtml({ campaign_id: "x", status }, M.gcListActions({ status }));
    assert.match(html, /data-gc-action="duplicate"/, "status=" + status);
    assert.match(html, /data-gc-action="preview"/, "status=" + status);
  });
});

test("Mission Pool actions (Open Mission / Close Mission / End Rewards) are isolated to mission_pool rows", () => {
  const nonMission = M.gcOverflowMenuHtml(
    { campaign_id: "x", status: "live", mechanic: "tournament", mission_active_rewards: 5 },
    M.gcListActions({ status: "live", mechanic: "tournament" })
  );
  assert.doesNotMatch(nonMission, /data-gc-action="mission"/);
  assert.doesNotMatch(nonMission, /data-gc-action="close-mission"/);
  assert.doesNotMatch(nonMission, /data-gc-action="end-rewards"/);

  const mission = M.gcOverflowMenuHtml(
    { campaign_id: "x", status: "live", mechanic: "mission_pool", mission_active_rewards: 5 },
    M.gcListActions({ status: "live", mechanic: "mission_pool" })
  );
  assert.match(mission, /data-gc-action="mission"[^>]*>Open Mission</);
  assert.match(mission, /data-gc-action="close-mission"/);
  assert.match(mission, /data-gc-action="end-rewards"/);
});

// ---------------------------------------------------------------------
// 5. Existing destructive confirmation stays active (openGcDeleteModal
//    itself is untouched by P0.4 — see test_campaign_centre_delete_ui).
// ---------------------------------------------------------------------
test("Delete still routes through the typed-confirmation modal, not a plain confirm()", () => {
  // P0.16 §B — the handler now branches on whether Delete was triggered
  // from Campaign Detail's own overflow (to navigate away afterward
  // instead of the default in-place list refresh), but it must still be
  // openGcDeleteModal doing the confirming, never window.confirm/a bare
  // apiDelete call.
  const bindSrc = extractFunctionSource(JS, "bindGcCampaigns");
  const deleteStart = bindSrc.indexOf('action === "delete"');
  assert.notEqual(deleteStart, -1, "delete branch not found");
  const deleteBranch = bindSrc.slice(deleteStart, bindSrc.indexOf("\n    });", deleteStart));
  assert.match(deleteBranch, /openGcDeleteModal\(id, btn\.dataset\.name/);
  assert.doesNotMatch(deleteBranch, /window\.confirm|[^.]\bconfirm\(/);
});

// ---------------------------------------------------------------------
// 6. Setup summary — reuses computeSetupChecklist (P0.5a), never a second
//    notion of "ready", and never a per-row detail fetch.
// ---------------------------------------------------------------------
test("setup summary: complete campaign reads 'Setup complete'", () => {
  const rows = M.computeSetupChecklist(tournamentCampaign(), [activeProvider], []);
  assert.equal(M.gcSetupSummary(rows), "Setup complete");
});

test("setup summary: incomplete rewards reads 'N / total setup complete · Rewards missing'", () => {
  const campaign = tournamentCampaign({ reward_config: { rules: [] } });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcSetupSummary(rows), "3 / 4 setup complete · Rewards missing");
});

test("setup summary: another missing step (destination not ready) names that step", () => {
  const campaign = tournamentCampaign({ destination: { provider_id: "mywin", open_mode: "telegram_web_app", path: "/x", ready: false } });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcSetupSummary(rows), "3 / 4 setup complete · Where users go missing");
});

test("setup summary reuses the exact P0.5a checklist — a campaign that can't publish still reports its true completeness", () => {
  // An archived campaign with every field filled in is 100% complete even
  // though it can no longer be published — gcSetupSummary answers "is setup
  // done", not "can this go live right now" (that's Ready-to-Publish, which
  // stays Campaign Detail-only).
  const campaign = tournamentCampaign({ status: "archived" });
  const rows = M.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(M.gcSetupSummary(rows), "Setup complete");
});

test("loadGcCampaigns never issues a per-row detail/provider/pool request (no N+1)", () => {
  // Strip // line comments first — this function's own comments explain
  // the caching/dedup behavior in prose and legitimately mention these
  // function names, which would otherwise inflate the match counts below.
  const rawSrc = extractFunctionSource(JS, "loadGcCampaigns");
  const src = rawSrc.split("\n").map((line) => line.replace(/\/\/.*$/, "")).join("\n");
  // Exactly one call each to the list/providers/pools fetchers, and no
  // per-campaign endpoint (/api/admin/gc-campaigns/<id>) anywhere in the
  // loader.
  assert.equal((src.match(/fetchGcCampaignsList\(/g) || []).length, 1);
  assert.equal((src.match(/fetchGcProviders\(/g) || []).length, 1, "must reuse loadGcProviderSelect's own fetch, never force a second /api/admin/providers call");
  assert.equal((src.match(/fetchGcRewardPools\(/g) || []).length, 1, "pools must be fetched at most once per page load, not per row");
  assert.doesNotMatch(src, /\.forEach\([^)]*\bapi\(/, "no per-row fetch loop");
  assert.doesNotMatch(src, /\.map\([^)]*\bapi\(/, "no per-row fetch loop");
});

test("loadGcCampaigns never forces a second /api/admin/providers request on top of loadGcProviderSelect's own", () => {
  const src = extractFunctionSource(JS, "loadGcCampaigns");
  assert.match(src, /fetchGcProviders\(\)/, "the second call must be unforced so it reuses loadGcProviderSelect's in-flight/cached promise");
  assert.doesNotMatch(src, /fetchGcProviders\(force\)/, "forcing it again here would discard the in-flight request and fire a duplicate call");
});

test("a specific status filter queries the backend's own ?status= filter, never a client-side filter over the (200-row-capped) unfiltered cache", () => {
  const src = extractFunctionSource(JS, "loadGcCampaigns");
  assert.match(src, /"\/api\/admin\/gc-campaigns\?status="\s*\+\s*encodeURIComponent\(statusFilter\)/);
  assert.doesNotMatch(src, /\.filter\(function \(c\) \{ return c\.status === statusFilter/,
    "must not silently drop campaigns beyond the unfiltered list's 200-row cap by filtering it client-side");
});

test("a stale (superseded) load never repaints the list, on success or on error", () => {
  const src = extractFunctionSource(JS, "loadGcCampaigns");
  assert.match(src, /var token = \+\+gcCampaignsLoadToken;/);
  assert.match(src, /if \(token !== gcCampaignsLoadToken\) return;/g);
  assert.equal((src.match(/if \(token !== gcCampaignsLoadToken\) return;/g) || []).length, 2,
    "the stale-load guard must cover both the success path and the .catch error path");
});

// ---------------------------------------------------------------------
// 7. Status filtering / grouping.
// ---------------------------------------------------------------------
test("gcGroupCampaigns returns one ungrouped bucket for any specific status filter", () => {
  const items = [tournamentCampaign({ status: "live" })];
  const groups = M.gcGroupCampaigns(items, "live");
  assert.equal(groups.length, 1);
  assert.equal(groups[0].heading, null);
  assert.equal(groups[0].items.length, 1);
});

test("gcGroupCampaigns buckets the unfiltered 'All' list into Active/Upcoming/Completed, skipping empty buckets", () => {
  const items = [
    tournamentCampaign({ campaign_id: "a", status: "live" }),
    tournamentCampaign({ campaign_id: "b", status: "paused" }),
    tournamentCampaign({ campaign_id: "c", status: "draft" }),
    tournamentCampaign({ campaign_id: "d", status: "ended" }),
  ];
  const groups = M.gcGroupCampaigns(items, "");
  assert.deepEqual(plain(groups.map((g) => g.heading)), ["Active", "Upcoming", "Completed"]);
  assert.deepEqual(plain(groups[0].items.map((c) => c.campaign_id)), ["a", "b"]);
  assert.deepEqual(plain(groups[1].items.map((c) => c.campaign_id)), ["c"]);
  assert.deepEqual(plain(groups[2].items.map((c) => c.campaign_id)), ["d"]);

  // No scheduled/archived campaigns on this page -> those buckets vanish
  // rather than rendering an empty "Upcoming"/"Completed" heading twice.
  const noArchived = M.gcGroupCampaigns(
    [tournamentCampaign({ campaign_id: "a", status: "live" })], ""
  );
  assert.deepEqual(plain(noArchived.map((g) => g.heading)), ["Active"]);
});

test("the filter bar's #gc-status-filter covers every backend status plus All", () => {
  const HTML = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.html"), "utf8");
  const start = HTML.indexOf('id="gc-status-filter"');
  const end = HTML.indexOf("</div>", start);
  const block = HTML.slice(start, end);
  ["", "live", "scheduled", "draft", "paused", "ended", "archived"].forEach((status) => {
    assert.match(block, new RegExp('data-status="' + status + '"'), "missing filter button for status=" + JSON.stringify(status));
  });
});

test("#gc-status-filter gets its own dedicated horizontal-scroll rule (all 7 buttons stay reachable on narrow screens)", () => {
  const CSS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.css"), "utf8");
  const filterRuleStart = CSS.indexOf("#gc-status-filter {");
  assert.notEqual(filterRuleStart, -1, "#gc-status-filter must have its own dedicated rule, not rely on .seg alone");
  const filterRule = CSS.slice(filterRuleStart, CSS.indexOf("}", filterRuleStart) + 1);
  assert.match(filterRule, /display:\s*flex/);
  assert.match(filterRule, /flex-wrap:\s*nowrap/, "single row — scrolls rather than wraps, per the preferred pattern");
  assert.match(filterRule, /overflow-x:\s*auto/, "must be independently scrollable rather than clipped by .seg's overflow:hidden");
  assert.match(filterRule, /overflow-y:\s*hidden/);

  const buttonRuleStart = CSS.indexOf("#gc-status-filter button {");
  assert.notEqual(buttonRuleStart, -1, "buttons must not shrink to fit — they need a dedicated no-shrink rule to stay tappable");
  const buttonRule = CSS.slice(buttonRuleStart, CSS.indexOf("}", buttonRuleStart) + 1);
  assert.match(buttonRule, /flex:\s*0 0 auto/);
});

test(".seg's shared global rule is unchanged by the #gc-status-filter fix — no other segmented control is affected", () => {
  const CSS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.css"), "utf8");
  const segRuleStart = CSS.indexOf(".seg {");
  assert.notEqual(segRuleStart, -1);
  const segRule = CSS.slice(segRuleStart, CSS.indexOf("}", segRuleStart) + 1);
  assert.equal(segRule, ".seg { display: inline-flex; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }",
    ".seg itself must keep its original declaration, unmodified");
  // Belt-and-braces: .seg must never itself gain scroll/wrap behavior —
  // that would change every other segmented control on the page (Campaign
  // Centre's #ac-status-filter / #campaigns-status-filter, etc.), which is
  // exactly what this fix is scoped to avoid.
  assert.doesNotMatch(segRule, /overflow-x/);
  assert.doesNotMatch(segRule, /flex-wrap/);
});

// ---------------------------------------------------------------------
// 8. Empty states — every filter stays useful, never a blank table.
// ---------------------------------------------------------------------
["live", "scheduled", "draft", "paused", "ended", "archived", "", "unknown"].forEach((statusFilter) => {
  test("gcEmptyStateForFilter(" + JSON.stringify(statusFilter) + ") returns a non-blank title/sub", () => {
    const state = M.gcEmptyStateForFilter(statusFilter);
    assert.ok(state.title && state.title.length > 0);
    assert.ok(state.sub && state.sub.length > 0);
  });
});

// P0.6 superseded the beginner "+ New Campaign" entry point with a guided
// creation wizard (gcOpenCampaignWizard) — the legacy inline form
// (gcScrollToCreateForm) is still reachable but no longer what these CTAs
// open. See test_admin_dashboard_p0_6_campaign_wizard.test.js for the
// wizard's own coverage.
test("empty states for filters an admin can act on (live/scheduled/draft/all) offer a + New Campaign CTA", () => {
  ["live", "scheduled", "draft", ""].forEach((statusFilter) => {
    const state = M.gcEmptyStateForFilter(statusFilter);
    assert.match(state.ctaHtml || "", /gcOpenCampaignWizard/, "status=" + JSON.stringify(statusFilter));
  });
});

test("+ New Campaign opens the guided creation wizard, not a direct backend call", () => {
  const start = JS.indexOf("window.gcOpenCampaignWizard = function");
  assert.notEqual(start, -1, "window.gcOpenCampaignWizard not found");
  const end = JS.indexOf("};", start) + 2;
  const fnText = JS.slice(start, end);
  assert.match(fnText, /switchView\("gcCampaignWizard"\)/, "must open the wizard view");
  assert.doesNotMatch(fnText, /fetch\(|apiPost\(/, "must not itself create a campaign — only open the wizard");
});

test("the legacy inline create form (gcScrollToCreateForm) still only scrolls/focuses, never creates a campaign", () => {
  const start = JS.indexOf("window.gcScrollToCreateForm = function");
  assert.notEqual(start, -1, "window.gcScrollToCreateForm not found");
  const end = JS.indexOf("};", start) + 2;
  const fnText = JS.slice(start, end);
  assert.match(fnText, /#gc-c-name/, "must focus the existing Create Campaign name field");
  assert.doesNotMatch(fnText, /fetch\(|apiPost\(/, "must not itself create a campaign — only scroll/focus the existing form");
});

// ---------------------------------------------------------------------
// 9. Error handling — generic message, no raw exception text.
// ---------------------------------------------------------------------
test("a failed campaigns load shows a generic message, never the raw exception text", () => {
  const src = extractFunctionSource(JS, "loadGcCampaigns");
  assert.match(src, /Couldn't load campaigns\. Try again\./);
  assert.doesNotMatch(src, /Failed to load campaigns: ["']?\s*\+\s*e\.message/);
});

// ---------------------------------------------------------------------
// 10. Existing coverage stays green (smoke-checked here; full suites run
//     separately — see the files named below).
// ---------------------------------------------------------------------
test("gcPill/gcTypeLabel/computeSetupChecklist are unchanged in shape and still importable from the same block P0.5a's suite reads", () => {
  // A structural smoke check, not a re-run: the real regression coverage
  // lives in test_admin_dashboard_p0_5a_campaign_detail.test.js,
  // test_admin_dashboard_p0_2_filters.test.js,
  // test_admin_dashboard_p0_3_id_fields.test.js, and
  // test_admin_dashboard_views_sync.test.js — all still pass unmodified.
  assert.equal(typeof M.computeSetupChecklist, "function");
  assert.equal(typeof M.gcPill, "function");
  assert.equal(typeof M.gcTypeLabel, "function");
});
