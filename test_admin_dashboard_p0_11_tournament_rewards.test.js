/**
 * P0.11: Tournament Rewards — the smallest safe editor for
 * reward_config.rules, so a Tournament campaign can actually be published
 * from the Admin Dashboard.
 *
 * Backend contract this editor is built against (see reward_engine.py /
 * campaign_centre.py / tournament_rewards.py / voucher_pool_service.py —
 * no backend file is touched by this PR):
 *   - reward_config.rules is a flat list of condition-based rules; a "rank"
 *     rule needs exactly {rule_id, condition_type:"rank",
 *     params:{min_rank,max_rank}, pool_id}, with an OPTIONAL reward_label
 *     and an OPTIONAL pool_type. validate_reward_rules enforces min_rank>=1,
 *     max_rank>=min_rank, non-overlapping ranges once sorted, and a
 *     non-empty pool_id — nothing else. Gaps are legal, the same pool may
 *     be reused across tiers, and there is no min/max tier count.
 *   - Every matching winner gets exactly ONE voucher
 *     (tournament_rewards._create_or_confirm_rewards/_atomic_allocate_
 *     voucher) — there is no "quantity per winner" concept anywhere in the
 *     reward engine, so the editor shows a fixed "Quantity per winner: 1"
 *     rather than an input the backend could never honor.
 *   - A rule's pool_type, when present, must match the allocated voucher's
 *     actual pool_type (voucher_pool_service.allocate_voucher's
 *     expected_pool_type filter) or it silently never allocates; when
 *     absent it defaults to "tournament_reward" server-side. Every rule
 *     this editor writes stamps pool_type from the SELECTED pool's own
 *     registered pool_type so that landmine can never ship.
 *   - Only pools registered with allocation_scope in {campaign_rewards,
 *     shared} (voucher_pool_service.CAMPAIGN_ALLOCATABLE_SCOPES) can ever
 *     actually be allocated to a Tournament winner — a provable
 *     incompatibility, so those are the only ones ever offered.
 *   - Stock is checked only at result-APPROVAL time
 *     (tournament_rewards.approve_submission), never at rule-save time —
 *     a zero-stock pool is a legal, selectable configuration.
 *
 * Mirrors the established P0.5a/P0.5b/P0.9/P0.10 harness: no build step, no
 * jsdom — relevant source ranges are extracted as text and executed in
 * sandboxed vm contexts against small stand-ins for the DOM/fetch layer.
 *
 * Run with: node --test test_admin_dashboard_p0_11_tournament_rewards.test.js
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

function plain(v) { return JSON.parse(JSON.stringify(v)); }

function runInSandbox(code, sandboxExtra) {
  const sandbox = sandboxExtra || {};
  sandbox.console = sandbox.console || console;
  sandbox.Object = sandbox.Object || Object;
  sandbox.String = sandbox.String || String;
  sandbox.Date = sandbox.Date || Date;
  sandbox.Promise = sandbox.Promise || Promise;
  sandbox.JSON = sandbox.JSON || JSON;
  sandbox.Array = sandbox.Array || Array;
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

async function flush(n) {
  for (let i = 0; i < (n || 12); i++) await Promise.resolve();
}

// ---------------------------------------------------------------------
// Same source ranges the existing P0.5a/P0.5b/P0.9/P0.10 suites already
// rely on — never re-implemented stand-ins.
// ---------------------------------------------------------------------
const PROVIDER_LABEL_SRC = slice(JS, "  function gcProviderOptionLabel(p) {", "\n  function renderGcProviderSelect()");
const KL_SRC = slice(JS, "  function ccPad2(n)", "\n  var CC_CONTENT_ICON");
const PURE_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Composer + orchestration (DOM-touching)");
const ORCH_SRC = slice(JS, "  // Entry point from the Campaigns list", "\n  // ---------- Mission Reward Pool (Phase 2.1");

function loadPure() {
  return runInSandbox(PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\nthis.__x = { " +
    "computeSetupChecklist, gcCampaignDetailChecklistHtml, gcCampaignDetailContinueHtml, " +
    "cdRewardEligiblePools, cdRewardPoolLabel, cdRewardPoolOptionsHtml, cdOrdinal, cdRankRangeLabel, " +
    "cdRewardTierSummaryLines, cdRewardsReadHtml, cdRewardTierRowHtml, cdRewardsEditHtml };",
    { esc }).__x;
}

const P = loadPure();

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------
function tournamentCampaign(overrides) {
  return Object.assign({
    campaign_id: "july-tournament",
    name: "July Tournament",
    type: "tournament",
    status: "draft",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: "2026-10-31T15:59:00Z" },
    destination: { provider_id: "mywin", open_mode: "telegram_web_app", path: "/x", ready: true },
    reward_config: { rules: [] },
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

const goldPool = { pool_id: "pool-gold-secret", name: "$50 Tournament Pool", pool_type: "tournament_reward", allocation_scope: "campaign_rewards", status: "active", stock: { available: 120, issued: 4 } };
const silverPool = { pool_id: "pool-silver-secret", name: "$20 Tournament Pool", pool_type: "tournament_reward", allocation_scope: "campaign_rewards", status: "active", stock: { available: 0, issued: 0 } };
const inactivePool = { pool_id: "pool-old", name: "Old Prize Pool", pool_type: "tournament_reward", allocation_scope: "campaign_rewards", status: "paused", stock: { available: 5, issued: 0 } };
const affiliatePool = { pool_id: "pool-affiliate", name: "Affiliate Payout Pool", pool_type: "affiliate", allocation_scope: "affiliate_rewards", status: "active", stock: { available: 50, issued: 0 } };
const sharedPool = { pool_id: "pool-shared", name: "Shared Prize Pool", pool_type: "other", allocation_scope: "shared", status: "active", stock: { available: 3, issued: 0 } };

// =======================================================================
// A. Pool eligibility / labeling — provable incompatibility only
// =======================================================================
test("cdRewardEligiblePools keeps only allocation_scope in {campaign_rewards, shared} (mirrors CAMPAIGN_ALLOCATABLE_SCOPES)", () => {
  const eligible = P.cdRewardEligiblePools([goldPool, affiliatePool, sharedPool]);
  assert.deepEqual(plain(eligible.map((p) => p.pool_id)), ["pool-gold-secret", "pool-shared"]);
});

test("cdRewardPoolLabel shows the human-readable name and stock count, never pool_id", () => {
  const label = P.cdRewardPoolLabel(goldPool);
  assert.equal(label, "$50 Tournament Pool · 120 available");
  assert.doesNotMatch(label, /pool-gold-secret/);
});

test("cdRewardPoolLabel shows '0 available' rather than hiding a zero-stock pool (legal to configure — stock is only checked at approval time)", () => {
  assert.equal(P.cdRewardPoolLabel(silverPool), "$20 Tournament Pool · 0 available");
});

test("cdRewardPoolLabel marks an inactive pool in its label", () => {
  assert.match(P.cdRewardPoolLabel(inactivePool), /\(inactive\)$/);
});

test("cdRewardPoolOptionsHtml never renders pool_id as visible text, only as the option value", () => {
  const html = P.cdRewardPoolOptionsHtml([goldPool], "pool-gold-secret");
  const occurrences = (html.match(/pool-gold-secret/g) || []).length;
  assert.equal(occurrences, 1);
  assert.match(html, /value="pool-gold-secret" selected>\$50 Tournament Pool · 120 available</);
});

test("cdRewardPoolOptionsHtml keeps a currently-referenced-but-unlisted pool selected (never silently unlinks it)", () => {
  const html = P.cdRewardPoolOptionsHtml([goldPool], "pool-deleted-or-rescoped");
  assert.match(html, /<option value="pool-deleted-or-rescoped" selected>/);
  const visible = html.match(/<option[^>]*value="pool-deleted-or-rescoped"[^>]*>([^<]*)</);
  assert.doesNotMatch(visible[1], /pool-deleted-or-rescoped/);
});

test("cdRewardPoolOptionsHtml adds no fallback option when nothing is selected or the pool is listed", () => {
  assert.doesNotMatch(P.cdRewardPoolOptionsHtml([goldPool], ""), /not in the loaded list/);
  assert.doesNotMatch(P.cdRewardPoolOptionsHtml([goldPool], "pool-gold-secret"), /not in the loaded list/);
});

// =======================================================================
// B. Rank-range display text
// =======================================================================
test("cdOrdinal / cdRankRangeLabel: single rank reads '1st place', a range reads 'Nth–Mth'", () => {
  assert.equal(P.cdRankRangeLabel(1, 1), "1st place");
  assert.equal(P.cdRankRangeLabel(2, 3), "2nd–3rd");
  assert.equal(P.cdRankRangeLabel(4, 10), "4th–10th");
});

test("cdOrdinal handles the 11th/12th/13th exception and 21st/22nd/23rd", () => {
  assert.equal(P.cdOrdinal(11), "11th");
  assert.equal(P.cdOrdinal(12), "12th");
  assert.equal(P.cdOrdinal(13), "13th");
  assert.equal(P.cdOrdinal(21), "21st");
  assert.equal(P.cdOrdinal(22), "22nd");
  assert.equal(P.cdOrdinal(23), "23rd");
  assert.equal(P.cdOrdinal(101), "101st");
});

test("cdRewardTierSummaryLines sorts by rank ascending and ignores non-rank rules", () => {
  const rules = [
    { rule_id: "r3", condition_type: "rank", params: { min_rank: 4, max_rank: 10 }, pool_id: "pool-gold-secret", reward_label: "$5 Voucher" },
    { rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret", reward_label: "$50 Voucher" },
    { rule_id: "r-consolation", condition_type: "participation", params: {}, pool_id: "pool-shared" },
    { rule_id: "r2", condition_type: "rank", params: { min_rank: 2, max_rank: 3 }, pool_id: "pool-silver-secret", reward_label: "$20 Voucher" },
  ];
  const lines = P.cdRewardTierSummaryLines(rules, [goldPool, silverPool]);
  assert.deepEqual(plain(lines), [
    { rankLabel: "1st place", rewardLine: "$50 Voucher × 1" },
    { rankLabel: "2nd–3rd", rewardLine: "$20 Voucher × 1" },
    { rankLabel: "4th–10th", rewardLine: "$5 Voucher × 1" },
  ]);
});

test("cdRewardTierSummaryLines falls back to the pool's own name when reward_label is blank", () => {
  const rules = [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret" }];
  const lines = P.cdRewardTierSummaryLines(rules, [goldPool]);
  assert.equal(lines[0].rewardLine, "$50 Tournament Pool × 1");
});

// =======================================================================
// C. Editor form — no raw ids, fixed quantity, empty states
// =======================================================================
test("cdRewardsEditHtml: 'No reward pools are available yet' with a Manage Reward Pools exit when none exist at all", () => {
  const html = P.cdRewardsEditHtml(tournamentCampaign(), [], []);
  assert.match(html, /No reward pools are available yet/);
  assert.match(html, /data-cd-goto-pools="1"/);
  assert.match(html, /Manage Reward Pools/);
  assert.doesNotMatch(html, /data-cd-save-rewards/);
});

test("cdRewardsEditHtml: explains incompatibility (not a guess) when pools exist but none are allocatable to a Tournament", () => {
  const html = P.cdRewardsEditHtml(tournamentCampaign(), [affiliatePool], []);
  assert.match(html, /None of the existing reward pools can be used for Tournament rewards/);
  assert.match(html, /data-cd-goto-pools="1"/);
  assert.doesNotMatch(html, /data-cd-save-rewards/);
});

test("cdRewardsEditHtml renders Rank from/Rank to/Reward pool per tier, a fixed Quantity of 1, and Save/Cancel — never pool_id as visible text", () => {
  const draft = [{ key: "t1", ruleId: "r1", minRank: 1, maxRank: 1, poolId: "pool-gold-secret", originalPoolId: "pool-gold-secret", originalRewardLabel: "$50 Voucher", originalPoolType: "tournament_reward" }];
  const html = P.cdRewardsEditHtml(tournamentCampaign(), [goldPool], draft);
  assert.match(html, /Reward tier 1/);
  assert.match(html, /Rank from/);
  assert.match(html, /Rank to/);
  assert.match(html, /data-cd-reward-field="minRank"[^>]*value="1"/);
  assert.match(html, /data-cd-reward-field="maxRank"[^>]*value="1"/);
  assert.match(html, /Quantity per winner: 1/);
  assert.doesNotMatch(html, /Quantity per winner: <input/, "quantity is never an editable input — the backend has no such field");
  assert.match(html, /Remove tier/);
  assert.match(html, /\+ Add reward tier/);
  assert.match(html, /data-cd-save-rewards="1"/);
  assert.match(html, /data-cd-cancel="rewards"/);
  const occurrences = (html.match(/pool-gold-secret/g) || []).length;
  assert.equal(occurrences, 1, "pool_id appears exactly once, as the option's value");
});

test("cdRewardsEditHtml renders one row per draft tier, numbered in order", () => {
  const draft = [
    { key: "t1", ruleId: null, minRank: 1, maxRank: 1, poolId: "", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
    { key: "t2", ruleId: null, minRank: "", maxRank: "", poolId: "", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
  ];
  const html = P.cdRewardsEditHtml(tournamentCampaign(), [goldPool], draft);
  assert.match(html, /Reward tier 1/);
  assert.match(html, /Reward tier 2/);
  assert.match(html, /data-cd-reward-remove="t1"/);
  assert.match(html, /data-cd-reward-remove="t2"/);
});

// =======================================================================
// D. Campaign Detail read view + checklist integration
// =======================================================================
test("cdRewardsReadHtml shows 'Set up rewards' when incomplete, 'Edit rewards' when complete, and the plain-English tier breakdown", () => {
  const rows = P.computeSetupChecklist(tournamentCampaign({
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret", reward_label: "$50 Voucher" }] },
  }), [], [goldPool]);
  const rewardsRow = rows.filter((r) => r.key === "rewards")[0];
  const html = P.cdRewardsReadHtml(rewardsRow, tournamentCampaign({
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret", reward_label: "$50 Voucher" }] },
  }), [goldPool]);
  assert.match(html, /Edit rewards/);
  assert.doesNotMatch(html, />Set up rewards</);
  assert.match(html, /1st place/);
  assert.match(html, /\$50 Voucher × 1/);
});

test("cdRewardsReadHtml shows 'Set up rewards' and no tier breakdown when empty", () => {
  const rows = P.computeSetupChecklist(tournamentCampaign(), [], []);
  const rewardsRow = rows.filter((r) => r.key === "rewards")[0];
  const html = P.cdRewardsReadHtml(rewardsRow, tournamentCampaign(), []);
  assert.match(html, />Set up rewards</);
  assert.doesNotMatch(html, /1st place/);
});

test("gcCampaignDetailChecklistHtml: a Tournament's rewards row is inline-editable (data-cd-edit=\"rewards\"), never a raw pool_id", () => {
  const campaign = tournamentCampaign({
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret", reward_label: "$50 Voucher" }] },
  });
  const rows = P.computeSetupChecklist(campaign, [], [goldPool]);
  const html = P.gcCampaignDetailChecklistHtml(rows, { editingSection: null, campaign, providers: [], pools: [goldPool] });
  assert.match(html, /data-cd-edit="rewards"/);
  assert.doesNotMatch(html, /pool-gold-secret/);
});

test("gcCampaignDetailChecklistHtml: opening the rewards row (editingSection='rewards') renders the tier editor in place", () => {
  const campaign = tournamentCampaign();
  const rows = P.computeSetupChecklist(campaign, [], []);
  const draft = [{ key: "t1", ruleId: null, minRank: "", maxRank: "", poolId: "", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const html = P.gcCampaignDetailChecklistHtml(rows, { editingSection: "rewards", campaign, providers: [], pools: [goldPool], rewardsDraft: draft });
  assert.match(html, /data-cd-save-rewards="1"/);
  assert.match(html, /Tournament Rewards/);
});

test("gcCampaignDetailChecklistHtml: a Mission Pool campaign's rewards row is UNAFFECTED — still routes via data-cd-goto=\"rewards_mission\", never the tier editor", () => {
  const campaign = missionPoolCampaign();
  const rows = P.computeSetupChecklist(campaign, [], [{ pool_id: "MP-1", name: "October Prizes", stock: { available: 42, issued: 0 } }]);
  const html = P.gcCampaignDetailChecklistHtml(rows, { editingSection: null, campaign, providers: [], pools: [{ pool_id: "MP-1", name: "October Prizes", stock: { available: 42, issued: 0 } }] });
  assert.doesNotMatch(html, /data-cd-edit="rewards"/);
  assert.doesNotMatch(html, /Tournament Rewards/);
});

test("Registration/External campaigns never show a rewards row at all (unaffected by P0.11)", () => {
  const external = { campaign_id: "x", name: "X", type: "external_website", status: "draft", schedule: { starts_at: "2026-10-01T01:00:00Z" }, destination: { provider_id: "", open_mode: "external_url", path: "", ready: false }, registration: { enabled: false } };
  const rows = P.computeSetupChecklist(external, [], []);
  assert.equal(rows.some((r) => r.key === "rewards"), false);
});

test("Continue Setup for a Tournament missing rewards still targets rewards_tournament (P0.11 wires this to the new editor, not a new target)", () => {
  const campaign = tournamentCampaign();
  const rows = P.computeSetupChecklist(campaign, [], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Continue Setup → Rewards/);
  assert.match(html, /data-cd-goto="rewards_tournament"/);
});

// =======================================================================
// Orchestration sandbox — cdSaveRewards / cdRewardTiersFromDraft /
// cdRewardsInitDraft / click wiring.
// =======================================================================
function loadOrchestration(overrides) {
  const nodes = {};
  function node(sel) {
    if (!nodes[sel]) nodes[sel] = { value: "", checked: false, textContent: "", innerHTML: "", style: {} };
    return nodes[sel];
  }
  const calls = { api: [], apiPutJson: [], toast: [], switchView: [], gcCampaignDetailHtml: [] };
  const apiQueue = [];
  let fieldNodes = [];

  const sandboxBase = {
    esc,
    state: { campaignId: "july-tournament" },
    $: (sel) => node(sel),
    $all: (sel) => (sel === "[data-cd-reward-field]" ? fieldNodes : []),
    switchView: (v) => { calls.switchView.push(v); },
    statePanel: () => {},
    activateTab: () => {},
    openMissionAdmin: () => {},
    openMissionEdit: () => {},
    openCampaignRegistrationConfig: () => {},
    fetchGcProviders: () => Promise.resolve([]),
    fetchGcRewardPools: () => Promise.resolve(sandboxBase.__pools || []),
    gcPill: (status) => "<span>" + esc(status) + "</span>",
    gcDisplayPill: () => "<span>pill</span>",
    toast: (msg, kind) => { calls.toast.push([msg, kind]); },
    api: (url) => {
      calls.api.push(url);
      const next = apiQueue.shift();
      return next ? Promise.resolve(next) : Promise.reject(new Error("no api() response queued"));
    },
    apiPutJson: (url, body) => {
      calls.apiPutJson.push({ url, body });
      const behavior = sandboxBase.__putBehavior;
      if (behavior === "network_error") return Promise.reject(new Error("network down"));
      if (behavior) return Promise.resolve({ ok: false, status: 400, d: { status: "error", code: behavior } });
      return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } });
    },
    gcCampaignDetailHtml: (campaign, providers, pools, editingSection, rewardsDraft) => {
      calls.gcCampaignDetailHtml.push({ campaign, providers, pools, editingSection, rewardsDraft });
      return "rendered:" + (campaign && campaign.campaign_id);
    },
  };
  Object.assign(sandboxBase, overrides || {});

  const listeners = {};
  sandboxBase.document = {
    addEventListener: (evt, fn) => { (listeners[evt] = listeners[evt] || []).push(fn); },
    _trigger: (evt, e) => { (listeners[evt] || []).forEach((fn) => fn(e)); },
  };

  const fullSrc = PROVIDER_LABEL_SRC + "\n" + KL_SRC + "\n" + PURE_SRC + "\n" + ORCH_SRC +
    "\nthis.cdSaveSection = cdSaveSection; this.cdOpenEdit = cdOpenEdit; this.cdViewState = cdViewState; " +
    "this.cdSaveRewards = cdSaveRewards; this.cdRewardTiersFromDraft = cdRewardTiersFromDraft; " +
    "this.cdMergeRewardRules = cdMergeRewardRules; " +
    "this.cdRewardsInitDraft = cdRewardsInitDraft; this.cdRewardsSyncDraftFromForm = cdRewardsSyncDraftFromForm; " +
    "this.cdRewardsFriendlyError = cdRewardsFriendlyError; " +
    "this.__triggerClick = function (selector, dataset) { " +
    "  var target = { closest: function (sel) { return sel === selector ? { dataset: dataset || {} } : null; } }; " +
    "  document._trigger('click', { target: target }); " +
    "};";

  const sandbox = runInSandbox(fullSrc, sandboxBase);
  sandbox.bindCampaignDetail();
  return {
    sandbox, calls, apiQueue, node,
    setFieldNodes: (list) => { fieldNodes = list; },
  };
}

function fieldNode(key, field, value) {
  return { value: String(value), dataset: { cdRewardKey: key, cdRewardField: field } };
}

// -----------------------------------------------------------------------
// E. cdRewardTiersFromDraft — validation mirrors reward_engine.
// validate_reward_rules exactly.
// -----------------------------------------------------------------------
test("Schema: a single minimal valid rule (rank 1-1, one pool) builds successfully", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool });
  assert.equal(built.error, undefined);
  assert.equal(built.rules.length, 1);
  assert.deepEqual(plain(built.rules[0].params), { min_rank: 1, max_rank: 1 });
  assert.equal(built.rules[0].pool_id, "pool-gold-secret");
  assert.equal(built.rules[0].pool_type, "tournament_reward");
  assert.equal(built.rules[0].condition_type, "rank");
  assert.ok(built.rules[0].rule_id);
});

test("Schema: multiple valid, non-overlapping rules (with a legal gap) build successfully, sorted ascending", () => {
  const { sandbox } = loadOrchestration();
  const draft = [
    { minRank: "4", maxRank: "10", poolId: "pool-silver-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
    { minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
    // Deliberate gap: ranks 2-3 are never covered — legal per reward_engine (no "no gaps" rule).
  ];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool, "pool-silver-secret": silverPool });
  assert.equal(built.error, undefined);
  assert.deepEqual(plain(built.rules.map((r) => r.params)), [{ min_rank: 1, max_rank: 1 }, { min_rank: 4, max_rank: 10 }]);
});

test("Schema: the same pool may be reused across tiers", () => {
  const { sandbox } = loadOrchestration();
  const draft = [
    { minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
    { minRank: "2", maxRank: "2", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
  ];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool });
  assert.equal(built.error, undefined);
  assert.equal(built.rules.length, 2);
});

test("Schema: a zero-stock pool is a valid, selectable configuration (stock is only checked at approval time)", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "2", maxRank: "3", poolId: "pool-silver-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-silver-secret": silverPool });
  assert.equal(built.error, undefined);
});

test("Schema: invalid rank bounds — min_rank < 1 is rejected", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "0", maxRank: "3", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  assert.equal(sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool }).error, "invalid_rank_range");
});

test("Schema: invalid rank bounds — max_rank < min_rank is rejected", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "5", maxRank: "3", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  assert.equal(sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool }).error, "invalid_rank_range");
});

test("Schema: malformed rank input (blank/non-numeric) is rejected client-side, never sent as NaN", () => {
  const { sandbox } = loadOrchestration();
  assert.equal(sandbox.cdRewardTiersFromDraft([{ minRank: "", maxRank: "3", poolId: "pool-gold-secret" }], {}).error, "invalid_rank_range");
  assert.equal(sandbox.cdRewardTiersFromDraft([{ minRank: "abc", maxRank: "3", poolId: "pool-gold-secret" }], {}).error, "invalid_rank_range");
});

test("Schema: missing pool_id is rejected", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "1", maxRank: "1", poolId: "", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  assert.equal(sandbox.cdRewardTiersFromDraft(draft, {}).error, "missing_pool_id");
});

test("Schema: overlapping rank ranges are rejected (mirrors reward_engine._rank_ranges_overlap)", () => {
  const { sandbox } = loadOrchestration();
  const draft = [
    { minRank: "1", maxRank: "5", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
    { minRank: "5", maxRank: "8", poolId: "pool-silver-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
  ];
  assert.equal(sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool, "pool-silver-secret": silverPool }).error, "overlapping_rank_ranges");
});

test("Schema: adjacent-but-not-overlapping ranges (1-4, 5-8) are accepted", () => {
  const { sandbox } = loadOrchestration();
  const draft = [
    { minRank: "1", maxRank: "4", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
    { minRank: "5", maxRank: "8", poolId: "pool-silver-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
  ];
  assert.equal(sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool, "pool-silver-secret": silverPool }).error, undefined);
});

test("Schema: an empty draft is rejected (at_least_one_tier_required) rather than silently building an empty rules list — see the Codex-review test group below for why", () => {
  const { sandbox } = loadOrchestration();
  const built = sandbox.cdRewardTiersFromDraft([], {});
  assert.equal(built.error, "at_least_one_tier_required");
});

test("pool_type is stamped from the selected pool's own registered pool_type — never left to guess a mismatched default", () => {
  const { sandbox } = loadOrchestration();
  const weirdTypePool = { pool_id: "pool-x", name: "X", pool_type: "voucher_drop", allocation_scope: "shared", status: "active", stock: { available: 1 } };
  const draft = [{ minRank: "1", maxRank: "1", poolId: "pool-x", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-x": weirdTypePool });
  assert.equal(built.rules[0].pool_type, "voucher_drop");
});

test("reward_label defaults to the pool's own name for a brand-new tier", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool });
  assert.equal(built.rules[0].reward_label, "$50 Tournament Pool");
});

test("reward_label is preserved verbatim when the tier's pool_id is unchanged (no meaningful change)", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: "pool-gold-secret", originalRewardLabel: "Champion Reward (hand-typed)", originalPoolType: "tournament_reward" }];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool });
  assert.equal(built.rules[0].reward_label, "Champion Reward (hand-typed)");
});

test("reward_label is refreshed to the new pool's name when the tier's pool_id is changed", () => {
  const { sandbox } = loadOrchestration();
  const draft = [{ minRank: "1", maxRank: "1", poolId: "pool-silver-secret", originalPoolId: "pool-gold-secret", originalRewardLabel: "Old label for the gold pool", originalPoolType: "tournament_reward" }];
  const built = sandbox.cdRewardTiersFromDraft(draft, { "pool-gold-secret": goldPool, "pool-silver-secret": silverPool });
  assert.equal(built.rules[0].reward_label, "$20 Tournament Pool");
});

// -----------------------------------------------------------------------
// F. cdRewardsInitDraft — existing-rule preservation.
// -----------------------------------------------------------------------
test("cdRewardsInitDraft loads existing rank rules, sorted ascending, with original rule_id/reward_label/pool_type carried through", () => {
  const { sandbox } = loadOrchestration();
  const campaign = tournamentCampaign({
    reward_config: {
      rules: [
        { rule_id: "r-4-10", condition_type: "rank", params: { min_rank: 4, max_rank: 10 }, pool_id: "pool-silver-secret", reward_label: "$5 Voucher", pool_type: "tournament_reward" },
        { rule_id: "r-1-1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret", reward_label: "$50 Voucher", pool_type: "tournament_reward" },
      ],
    },
  });
  const draft = sandbox.cdRewardsInitDraft(campaign);
  assert.equal(draft.length, 2);
  assert.equal(draft[0].ruleId, "r-1-1");
  assert.equal(draft[0].minRank, 1);
  assert.equal(draft[1].ruleId, "r-4-10");
  assert.equal(draft[0].originalRewardLabel, "$50 Voucher");
  assert.equal(draft[0].originalPoolType, "tournament_reward");
});

test("cdRewardsInitDraft starts with exactly one blank tier when no rules exist yet", () => {
  const { sandbox } = loadOrchestration();
  const draft = sandbox.cdRewardsInitDraft(tournamentCampaign());
  assert.equal(draft.length, 1);
  assert.equal(draft[0].poolId, "");
});

test("cdRewardsInitDraft never surfaces a non-rank rule (e.g. a hand-authored consolation rule) into the editable draft", () => {
  const { sandbox } = loadOrchestration();
  const campaign = tournamentCampaign({
    reward_config: { rules: [{ rule_id: "consolation", condition_type: "participation", params: {}, pool_id: "pool-shared" }] },
  });
  const draft = sandbox.cdRewardsInitDraft(campaign);
  // No rank rules exist, so the editor still offers one blank tier to fill in —
  // the consolation rule itself is preserved separately by cdSaveRewards (see G).
  assert.equal(draft.length, 1);
  assert.equal(draft[0].ruleId, null);
});

// -----------------------------------------------------------------------
// G. cdSaveRewards — read-merge-PUT safety, concurrency, errors.
// -----------------------------------------------------------------------
test("Save safety: every reward_config sibling field survives a rules-only edit (read-merge-PUT)", async () => {
  const { sandbox, calls, apiQueue, node, setFieldNodes } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: null, minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const stored = { rules: [], other_future_field: "keep-me", source: "external_leaderboard", approval_required: true, auto_allocate: false };
  const latest = tournamentCampaign({ reward_config: stored });
  apiQueue.push({ status: "ok", campaign: latest }); // pre-PUT canonical GET
  apiQueue.push({ status: "ok", campaign: tournamentCampaign({ reward_config: Object.assign({}, stored, { rules: [{ rule_id: "x" }] }) }) }); // post-PUT canonical GET

  setFieldNodes([fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret")]);
  sandbox.cdSaveRewards({ disabled: false, textContent: "" });
  await flush();

  assert.equal(calls.apiPutJson.length, 1);
  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(Object.keys(body).sort(), ["reward_config"]);
  assert.equal(body.reward_config.other_future_field, "keep-me");
  assert.equal(body.reward_config.source, "external_leaderboard");
  assert.equal(body.reward_config.approval_required, true);
  assert.equal(body.reward_config.rules.length, 1);
  assert.equal(body.reward_config.rules[0].pool_id, "pool-gold-secret");
});

test("Save safety: a non-rank rule already on the campaign is preserved verbatim alongside the edited rank tiers", async () => {
  const { sandbox, calls, apiQueue, setFieldNodes } = loadOrchestration();
  const consolationRule = { rule_id: "consolation", condition_type: "participation", params: {}, pool_id: "pool-shared", reward_label: "Thanks for playing" };
  sandbox.cdViewState.campaign = tournamentCampaign({ reward_config: { rules: [consolationRule] } });
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: null, minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const latest = tournamentCampaign({ reward_config: { rules: [consolationRule] } });
  apiQueue.push({ status: "ok", campaign: latest });
  apiQueue.push({ status: "ok", campaign: latest });

  setFieldNodes([fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret")]);
  sandbox.cdSaveRewards({});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.reward_config.rules.length, 2);
  const preserved = body.reward_config.rules.filter((r) => r.rule_id === "consolation")[0];
  assert.deepEqual(preserved, consolationRule);
});

// Codex review (P1): reward_engine.match_rule() returns the first matching
// rule in list order, and a tournament winner's context carries both
// `rank` and `score` — so a preserved rule that matches on something else
// (score_threshold, a catch-all "participation" consolation rule, ...) can
// match the exact same context a rank rule would. Naively concatenating
// every edited rank rule before every preserved rule would silently move a
// bonus rule's priority, or let a catch-all start shadowing every rank
// rule it wasn't already ordered ahead of.
test("cdMergeRewardRules reinserts the rank-rule block at the ORIGINAL position of the first rank rule — a preceding bonus rule keeps precedence", () => {
  const { sandbox } = loadOrchestration();
  const bonusRule = { rule_id: "bonus", condition_type: "score_threshold", params: { min_score: 99999 }, pool_id: "pool-bonus" };
  const oldRank1 = { rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret" };
  const trailingRule = { rule_id: "trailing", condition_type: "campaign_tag", params: { tag: "x" }, pool_id: "pool-x" };
  const latestRules = [bonusRule, oldRank1, trailingRule];
  const editedRank = [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 5 }, pool_id: "pool-gold-secret" }];

  const merged = sandbox.cdMergeRewardRules(latestRules, editedRank);

  assert.deepEqual(plain(merged.map((r) => r.rule_id)), ["bonus", "r1", "trailing"]);
});

test("cdMergeRewardRules inserts new rank tiers at the very front when no rank rule existed yet — never after an existing catch-all rule", () => {
  const { sandbox } = loadOrchestration();
  const catchAll = { rule_id: "consolation", condition_type: "participation", params: {}, pool_id: "pool-shared" };
  const editedRank = [{ rule_id: "new1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret" }];

  const merged = sandbox.cdMergeRewardRules([catchAll], editedRank);

  // The rank rule must come BEFORE the always-matching catch-all, or
  // reward_engine.match_rule() would never reach it for any winner.
  assert.deepEqual(plain(merged.map((r) => r.rule_id)), ["new1", "consolation"]);
});

test("Save safety: a preceding bonus rule's precedence over the rank tiers survives an unrelated tier edit (Codex P1)", async () => {
  const { sandbox, calls, apiQueue, setFieldNodes } = loadOrchestration();
  const bonusRule = { rule_id: "bonus", condition_type: "score_threshold", params: { min_score: 99999 }, pool_id: "pool-bonus", reward_label: "Perfect Score Bonus" };
  const rankRule = { rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret" };
  sandbox.cdViewState.campaign = tournamentCampaign({ reward_config: { rules: [bonusRule, rankRule] } });
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: "r1", minRank: "1", maxRank: "2", poolId: "pool-gold-secret", originalPoolId: "pool-gold-secret", originalRewardLabel: null, originalPoolType: null }];
  const latest = tournamentCampaign({ reward_config: { rules: [bonusRule, rankRule] } });
  apiQueue.push({ status: "ok", campaign: latest });
  apiQueue.push({ status: "ok", campaign: latest });

  setFieldNodes([fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "2"), fieldNode("t1", "poolId", "pool-gold-secret")]);
  sandbox.cdSaveRewards({});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.deepEqual(body.reward_config.rules.map((r) => r.rule_id), ["bonus", "r1"]);
});

// Codex review (P1): with zero rank rules, tournament_integration.
// _validate_payload()'s allowed_ranks (built purely from rank-type rules
// via reward_engine.rank_ranges) is empty, so every submitted winner would
// be rejected with winner_rank_outside_reward_rules — even though a
// preserved non-rank rule could leave reward_config.rules non-empty and
// the checklist/publish gate reading "configured". Removing every tier
// must be blocked, not silently accepted.
test("Removing every tier is rejected — a Tournament can never be saved with zero rank rules", () => {
  const { sandbox } = loadOrchestration();
  const built = sandbox.cdRewardTiersFromDraft([], {});
  assert.equal(built.error, "at_least_one_tier_required");
});

test("Save safety: removing the last rank tier while a preserved consolation rule remains is rejected client-side, never reaches the network", async () => {
  const { sandbox, calls, node, setFieldNodes } = loadOrchestration();
  const consolationRule = { rule_id: "consolation", condition_type: "participation", params: {}, pool_id: "pool-shared" };
  sandbox.cdViewState.campaign = tournamentCampaign({ reward_config: { rules: [consolationRule, { rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret" }] } });
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = []; // admin removed the only rank tier
  setFieldNodes([]);

  sandbox.cdSaveRewards({});
  await flush();

  assert.deepEqual(calls.api, []);
  assert.deepEqual(calls.apiPutJson, []);
  assert.match(node("#cd-edit-rewards-error").textContent, /Add at least one reward tier/);
});

test("Save safety: concurrent update — another admin changed a sibling field between GET and PUT; the newest sibling survives, not this page's stale snapshot", async () => {
  const { sandbox, calls, apiQueue, setFieldNodes } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign({ reward_config: { rules: [], other_future_field: "STALE-PAGE-SNAPSHOT" } });
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: null, minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const freshLatest = tournamentCampaign({ reward_config: { rules: [], other_future_field: "NEWEST-FROM-ANOTHER-ADMIN" } });
  apiQueue.push({ status: "ok", campaign: freshLatest });
  apiQueue.push({ status: "ok", campaign: freshLatest });

  setFieldNodes([fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret")]);
  sandbox.cdSaveRewards({});
  await flush();

  const body = plain(calls.apiPutJson[0].body);
  assert.equal(body.reward_config.other_future_field, "NEWEST-FROM-ANOTHER-ADMIN");
});

test("Save safety: invalid tiers are rejected client-side with a friendly message and never reach the network", async () => {
  const { sandbox, calls, node, setFieldNodes } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: null, minRank: "5", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  setFieldNodes([fieldNode("t1", "minRank", "5"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret")]);

  sandbox.cdSaveRewards({});
  await flush();

  assert.deepEqual(calls.api, []);
  assert.deepEqual(calls.apiPutJson, []);
  assert.match(node("#cd-edit-rewards-error").textContent, /rank must start at 1/);
});

test("Save safety: a backend validation error maps to plain English, never a raw code", async () => {
  const { sandbox, calls, node, apiQueue, setFieldNodes } = loadOrchestration({ __putBehavior: "overlapping_rank_ranges" });
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: null, minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  apiQueue.push({ status: "ok", campaign: tournamentCampaign() });
  setFieldNodes([fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret")]);

  sandbox.cdSaveRewards({});
  await flush();

  const msg = node("#cd-edit-rewards-error").textContent;
  assert.match(msg, /can't overlap/);
  assert.doesNotMatch(msg, /overlapping_rank_ranges/);
});

test("Save safety: an unknown backend code falls back to the generic friendly message, never raw JSON/code", () => {
  const { sandbox } = loadOrchestration();
  assert.equal(sandbox.cdRewardsFriendlyError("some_future_unmapped_code"), "Couldn't save rewards. Try again.");
});

test("Save safety: a network failure shows a friendly message, never throws", async () => {
  const { sandbox, node, apiQueue, setFieldNodes } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: null, minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  // No response queued for the pre-PUT canonical GET -> api() rejects.
  setFieldNodes([fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret")]);

  sandbox.cdSaveRewards({});
  await flush();

  assert.match(node("#cd-edit-rewards-error").textContent, /Couldn't save rewards. Try again./);
});

test("Save success: canonical re-GET updates cdViewState.campaign so the checklist recomputes to complete, without ever calling publish", async () => {
  const { sandbox, calls, apiQueue, setFieldNodes } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.pools = [goldPool];
  sandbox.cdViewState.rewardsDraft = [{ key: "t1", ruleId: null, minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null }];
  const preLatest = tournamentCampaign();
  const postSave = tournamentCampaign({
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 1 }, pool_id: "pool-gold-secret", pool_type: "tournament_reward", reward_label: "$50 Tournament Pool" }] },
  });
  apiQueue.push({ status: "ok", campaign: preLatest });
  apiQueue.push({ status: "ok", campaign: postSave });
  setFieldNodes([fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret")]);

  sandbox.cdSaveRewards({});
  await flush();

  assert.equal(sandbox.cdViewState.campaign.reward_config.rules.length, 1);
  assert.equal(sandbox.cdViewState.editingSection, null);
  assert.equal(calls.toast[calls.toast.length - 1][0], "✅ Saved");

  // Re-derive the checklist from the freshly saved canonical campaign —
  // rewards must now read as complete (Tournament's 4th/final gate, given
  // the fixture's schedule/destination are already set).
  const rows = P.computeSetupChecklist(sandbox.cdViewState.campaign, [{ provider_id: "mywin", name: "MyWin", active: true }], [goldPool]);
  const rewardsRow = rows.filter((r) => r.key === "rewards")[0];
  assert.equal(rewardsRow.complete, true);

  // P0.11 explicitly never auto-publishes after a rewards save.
  assert.ok(!calls.apiPutJson.some((c) => c.url.indexOf("/publish") !== -1));
  assert.ok(!calls.api.some((u) => u.indexOf("/publish") !== -1));
});

// -----------------------------------------------------------------------
// H. Click wiring — Continue Setup / row button / add-remove tier / empty
// state exit, and non-destructive Cancel.
// -----------------------------------------------------------------------
test("Continue Setup → Rewards opens the Tournament Rewards editor directly (no more dead-end toast)", () => {
  const { sandbox, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.pools = [];
  sandbox.__triggerClick("[data-cd-goto]", { cdGoto: "rewards_tournament" });
  assert.equal(sandbox.cdViewState.editingSection, "rewards");
  assert.deepEqual(calls.toast, [], "no dead-end toast is ever shown for rewards_tournament anymore");
});

test("The rewards row's own [Set up rewards]/[Edit rewards] button (data-cd-edit=\"rewards\") opens the same editor", () => {
  const { sandbox } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.__triggerClick("[data-cd-edit]", { cdEdit: "rewards" });
  assert.equal(sandbox.cdViewState.editingSection, "rewards");
});

test("+ Add reward tier appends a blank tier, preserving whatever is currently typed in the other rows", () => {
  const { sandbox, setFieldNodes } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdOpenEdit("rewards"); // one blank tier to start
  const firstKey = sandbox.cdViewState.rewardsDraft[0].key;
  setFieldNodes([fieldNode(firstKey, "minRank", "7"), fieldNode(firstKey, "maxRank", "9"), fieldNode(firstKey, "poolId", "pool-gold-secret")]);

  sandbox.__triggerClick("[data-cd-reward-add]", {});

  assert.equal(sandbox.cdViewState.rewardsDraft.length, 2);
  assert.equal(sandbox.cdViewState.rewardsDraft[0].minRank, "7");
  assert.equal(sandbox.cdViewState.rewardsDraft[0].poolId, "pool-gold-secret");
  assert.equal(sandbox.cdViewState.rewardsDraft[1].poolId, "");
});

test("Remove tier deletes only that tier, preserving the others' current values", () => {
  const { sandbox, setFieldNodes } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.rewardsDraft = [
    { key: "t1", ruleId: null, minRank: "1", maxRank: "1", poolId: "pool-gold-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
    { key: "t2", ruleId: null, minRank: "2", maxRank: "3", poolId: "pool-silver-secret", originalPoolId: null, originalRewardLabel: null, originalPoolType: null },
  ];
  sandbox.cdViewState.editingSection = "rewards";
  setFieldNodes([
    fieldNode("t1", "minRank", "1"), fieldNode("t1", "maxRank", "1"), fieldNode("t1", "poolId", "pool-gold-secret"),
    fieldNode("t2", "minRank", "2"), fieldNode("t2", "maxRank", "3"), fieldNode("t2", "poolId", "pool-silver-secret"),
  ]);

  sandbox.__triggerClick("[data-cd-reward-remove]", { cdRewardRemove: "t1" });

  assert.equal(sandbox.cdViewState.rewardsDraft.length, 1);
  assert.equal(sandbox.cdViewState.rewardsDraft[0].key, "t2");
  assert.equal(sandbox.cdViewState.rewardsDraft[0].poolId, "pool-silver-secret");
});

test("Manage Reward Pools (empty-state exit) navigates to the Reward Pools screen, never traps the admin", () => {
  const { sandbox, calls } = loadOrchestration();
  sandbox.__triggerClick("[data-cd-goto-pools]", {});
  assert.deepEqual(calls.switchView, ["gcRewards"]);
});

test("Refresh pools re-fetches the pool list and re-renders, without touching the campaign itself", async () => {
  const { sandbox, calls } = loadOrchestration({ __pools: [goldPool, silverPool] });
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdViewState.editingSection = "rewards";
  sandbox.cdViewState.pools = [];
  sandbox.__triggerClick("[data-cd-reward-refresh-pools]", {});
  await flush();
  assert.deepEqual(sandbox.cdViewState.pools.map((p) => p.pool_id), ["pool-gold-secret", "pool-silver-secret"]);
});

test("Cancel discards the draft and leaves the campaign completely unchanged (no network call)", () => {
  const { sandbox, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdOpenEdit("rewards");
  assert.equal(sandbox.cdViewState.editingSection, "rewards");
  sandbox.__triggerClick("[data-cd-cancel]", { cdCancel: "rewards" });
  assert.equal(sandbox.cdViewState.editingSection, null);
  assert.deepEqual(calls.api, []);
  assert.deepEqual(calls.apiPutJson, []);
});

test("cdOpenEdit('rewards') builds the draft purely from the cached snapshot — no network call (Refresh pools is opt-in only)", () => {
  const { sandbox, calls } = loadOrchestration();
  sandbox.cdViewState.campaign = tournamentCampaign();
  sandbox.cdOpenEdit("rewards");
  assert.deepEqual(calls.api, []);
  assert.equal(sandbox.cdViewState.editingSection, "rewards");
  assert.equal(sandbox.cdViewState.rewardsDraft.length, 1);
});
