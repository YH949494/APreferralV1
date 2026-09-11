/**
 * P0.9: Campaign Detail's two remaining P0 beginner-flow dead ends.
 *
 *  1. "✓ Ready to Publish — go to Campaigns list" only ever navigated back
 *     to the list — it never actually published anything. Campaign Detail
 *     now shows a real [ Publish Campaign ] / [ Resume Campaign ] primary
 *     action, reusing P0.8's gcRunAction (confirm gate, in-flight guard,
 *     friendly error mapping, canonical reload — never a new publish
 *     endpoint or a second confirm/refresh implementation).
 *  2. Preview dumped raw backend JSON into a blocking `alert(...)`. Preview
 *     now opens a real modal (gcOpenPreview / gcRenderPreviewModal /
 *     gcPreviewModalBodyHtml), shared by the Campaigns list overflow menu
 *     and Campaign Detail's own "Preview Campaign" button — exactly one
 *     preview UI, never two — with a small plain-English translator
 *     (gcVisibilityReasonText) for campaign_centre.visibility_explanation's
 *     reason strings.
 *
 * Mirrors the existing P0.5a/P0.8/delete-modal harness: no build step, no
 * jsdom in this repo — relevant source ranges are extracted as text and
 * executed in sandboxed vm contexts, plus a small hand-rolled DOM stub
 * (copied from test_campaign_centre_delete_ui.test.js's own FakeElement,
 * since gcRenderPreviewModal builds its Close button via createElement/
 * appendChild the same way openGcDeleteModal does — directly testable
 * without an HTML parser) for the modal's open/close behavior.
 *
 * Run with: node --test test_admin_dashboard_p0_9_publish_preview.test.js
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

// Same three source ranges test_admin_dashboard_p0_5a_campaign_detail.test.js
// (PURE) and test_admin_dashboard_p0_8_action_hardening.test.js (INVALIDATE +
// ACTION) already pull independently — concatenated here into one sandbox so
// gcPreviewModalBodyHtml (defined in ACTION_SRC) can call gcVisibilityReasonText/
// gcFindProvider/computeSetupChecklist (defined in PURE_SRC), and gcRunAction
// (ACTION_SRC) can call gcInvalidateCampaignsCache (INVALIDATE_SRC), exactly as
// they do in the real file.
const INVALIDATE_SRC = slice(JS, "  function gcInvalidateCampaignsCache() {", "\n  function fetchGcProviders(force)");
const PURE_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Composer + orchestration (DOM-touching)");
const ACTION_SRC = slice(JS, "  var GC_ACTION_ERROR_MESSAGES = {", "\n  function bindGcCampaigns() {");
const FULL_SRC = INVALIDATE_SRC + "\n" + PURE_SRC + "\n" + ACTION_SRC;

// Faithful stand-ins for helpers defined outside these slices (same
// established pattern as test_admin_dashboard_p0_5a_campaign_detail.test.js's
// own esc/ccUtcToKlDisplay stand-ins) — never reimplementations of the logic
// under test, only of unrelated formatting helpers these slices happen to call.
function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}
function ccUtcToKlDisplay(iso) {
  if (!iso) return "—";
  return "2026-10-01 09:00 KL"; // deterministic stand-in; exact KL formatting is ccUtcToKlDisplay's own contract, not this suite's
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

async function withRejectionGuard(fn) {
  let caught = null;
  const handler = (reason) => { caught = reason; };
  process.on("unhandledRejection", handler);
  try {
    await fn();
    await flush(20);
  } finally {
    process.off("unhandledRejection", handler);
  }
  return caught;
}

// ---------------------------------------------------------------------
// Fixtures — shaped like campaign_centre.py's gc_campaigns documents.
// ---------------------------------------------------------------------
const activeProvider = { provider_id: "prov-1", name: "MyWin", active: true, type: "casino" };

function standardDropCampaign(overrides) {
  return Object.assign({
    campaign_id: "summer-drop",
    name: "Summer Drop",
    type: "standard_drop",
    status: "draft",
    description: "A great campaign",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: null },
    destination: { provider_id: "prov-1", path: "/x", open_mode: "telegram_web_app", ready: true },
    registration: { enabled: false },
    reward_config: { rules: [] },
  }, overrides || {});
}

function registrationCampaign(overrides) {
  return Object.assign({
    campaign_id: "signup-draw",
    name: "Signup Draw",
    type: "standard_drop",
    status: "draft",
    description: "Register to win",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: null },
    destination: { provider_id: "", path: "", open_mode: "telegram_web_app", ready: false },
    registration: { enabled: true, required_fields: ["full_name"] },
    reward_config: { rules: [] },
  }, overrides || {});
}

function tournamentCampaign(overrides) {
  return Object.assign({
    campaign_id: "july-tournament",
    name: "July Tournament",
    type: "tournament",
    status: "draft",
    description: "Top 3 leaderboard prize",
    schedule: { starts_at: "2026-10-01T01:00:00Z", ends_at: null },
    destination: { provider_id: "prov-1", path: "/x", open_mode: "telegram_web_app", ready: true },
    reward_config: { rules: [{ rule_id: "r1", condition_type: "rank", params: { min_rank: 1, max_rank: 3 } }] },
    registration: { enabled: false },
  }, overrides || {});
}

// =======================================================================
// Part 1 — Publish CTA (pure gcCampaignDetailContinueHtml, no DOM/network)
// =======================================================================
function loadPure() {
  return runInSandbox(PURE_SRC + "\nthis.__x = { computeSetupChecklist, gcFirstIncompleteRequiredRow, " +
    "gcCanTransitionToLive, gcIsReadyToPublish, gcVisibilityReasonText, gcServerPublishBlockReason, " +
    "gcCampaignDetailContinueHtml };", { esc, ccUtcToKlDisplay }).__x;
}
const P = loadPure();

test("Publish CTA: complete registration campaign shows Publish Campaign", () => {
  const campaign = registrationCampaign();
  const rows = P.computeSetupChecklist(campaign, [], []);
  assert.equal(P.gcFirstIncompleteRequiredRow(rows), null);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Publish Campaign/);
  assert.match(html, /data-gc-action="publish"/);
  assert.match(html, /data-id="signup-draw"/);
  assert.doesNotMatch(html, /data-gc-resume/);
});

test("Publish CTA: incomplete registration campaign (no start date) shows Continue Setup instead", () => {
  const campaign = registrationCampaign({ schedule: { starts_at: null, ends_at: null } });
  const rows = P.computeSetupChecklist(campaign, [], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Continue Setup/);
  assert.doesNotMatch(html, /Publish Campaign/);
});

test("Publish CTA: tournament missing reward rules shows Continue Setup, never Publish", () => {
  const campaign = tournamentCampaign({ reward_config: { rules: [] } });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Continue Setup → Rewards/);
  assert.doesNotMatch(html, /Publish Campaign/);
});

test("Publish CTA: incomplete destination shows Continue Setup, never Publish", () => {
  const campaign = standardDropCampaign({ destination: { provider_id: "", path: "", ready: false } });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Continue Setup/);
  assert.doesNotMatch(html, /Publish Campaign/);
});

test("Publish CTA: paused campaign shows Resume Campaign, tagged data-gc-resume", () => {
  const campaign = tournamentCampaign({ status: "paused" });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Resume Campaign/);
  assert.match(html, /data-gc-resume="1"/);
  assert.doesNotMatch(html, /Publish Campaign/);
});

test("Publish CTA: live campaign shows live status, never Publish/Resume", () => {
  const campaign = tournamentCampaign({ status: "live" });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.doesNotMatch(html, /Publish Campaign/);
  assert.doesNotMatch(html, /Resume Campaign/);
  assert.doesNotMatch(html, /data-gc-action="publish"/);
  assert.match(html, /is live/);
});

test("Publish CTA: ended campaign shows the can't-be-published message, never Publish", () => {
  const campaign = tournamentCampaign({ status: "ended" });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.doesNotMatch(html, /Publish Campaign/);
  assert.match(html, /can.t be published/);
});

test("Publish CTA: archived campaign shows the can't-be-published message, never Publish", () => {
  const campaign = tournamentCampaign({ status: "archived" });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.doesNotMatch(html, /Publish Campaign/);
  assert.match(html, /can.t be published/);
});

test("Publish CTA: checklist complete but server (effective_visibility) disagrees -> no Publish, friendly reason shown", () => {
  // The locally-cached `providers` array still says prov-1 is active (client
  // thinks the destination row is complete), but this canonical GET's own
  // effective_visibility — recomputed server-side on every load — already
  // knows the provider went inactive in the meantime. Server truth wins.
  const campaign = standardDropCampaign({
    effective_visibility: {
      publicly_visible: false,
      reasons: ["status is 'draft', not 'live'", "linked provider is inactive"],
    },
  });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  assert.equal(P.gcFirstIncompleteRequiredRow(rows), null, "checklist itself is complete");
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.doesNotMatch(html, /Publish Campaign/);
  assert.doesNotMatch(html, /data-gc-action="publish"/);
  assert.match(html, /The selected provider is inactive\./);
});

test("Publish CTA: a scheduled future start (timing-only reason) never blocks Publish", () => {
  // Regression for the same Codex-review finding P0.5a's suite already
  // covers for gcIsReadyToPublish: gcServerPublishBlockReason must ignore
  // timing-only reasons (status/schedule) and only ever gate on the
  // structural destination/provider reasons _transition() itself checks.
  const campaign = standardDropCampaign({
    schedule: { starts_at: "2027-01-01T00:00:00Z", ends_at: null },
    effective_visibility: {
      publicly_visible: false,
      reasons: ["status is 'draft', not 'live'", "scheduled to start at 2027-01-01T00:00:00+00:00"],
    },
  });
  const rows = P.computeSetupChecklist(campaign, [activeProvider], []);
  const html = P.gcCampaignDetailContinueHtml(rows, campaign);
  assert.match(html, /Publish Campaign/);
});

// ---------------------------------------------------------------------
// gcVisibilityReasonText — the plain-English translator (P0.9 §8)
// ---------------------------------------------------------------------
test("gcVisibilityReasonText: maps every known backend reason to the spec's plain-English copy", () => {
  assert.equal(P.gcVisibilityReasonText("status is 'draft', not 'live'"), "This campaign is still a draft.");
  assert.equal(P.gcVisibilityReasonText("scheduled to start at 2027-01-01T00:00:00+00:00"), "This campaign hasn't started yet — starts 2026-10-01 09:00 KL.");
  assert.equal(P.gcVisibilityReasonText("destination.ready is false"), "Destination setup is incomplete.");
  assert.equal(P.gcVisibilityReasonText("linked provider does not exist"), "The selected provider no longer exists.");
  assert.equal(P.gcVisibilityReasonText("linked provider is inactive"), "The selected provider is inactive.");
});

test("gcVisibilityReasonText: an unrecognized reason gets the generic fallback, never the raw string", () => {
  const msg = P.gcVisibilityReasonText("some_new_backend_reason_nobody_mapped_yet");
  assert.equal(msg, "Campaign is not currently visible to players.");
  assert.doesNotMatch(msg, /some_new_backend_reason/);
});

// =======================================================================
// Part 2 — Publish action (gcRunAction integration: confirm/success/failure)
// =======================================================================
function makeBtn(label) {
  return { textContent: label || "Action", disabled: false, dataset: {}, classList: { add() {}, remove() {} }, innerHTML: "" };
}

function loadAction(overrides) {
  const calls = { toast: [], confirmSimple: [], loadGcCampaigns: [], loadCampaignDetail: [] };
  const sandboxBase = {
    state: { view: "campaignDetail", campaignId: "summer-drop" },
    cdViewState: { campaign: null },
    gcOptionsCache: { providers: [], providersPromise: null, campaigns: [], campaignsPromise: Promise.resolve([]) },
    gcKnownCampaignIds: {},
    esc, ccUtcToKlDisplay, gcPill,
    toast: (msg, kind) => { calls.toast.push([msg, kind]); },
    confirmSimple: (title, message) => {
      calls.confirmSimple.push({ title, message });
      return Promise.resolve(sandboxBase.__confirmResult !== false);
    },
    btnStart: (btn) => { if (!btn || btn.__loading) return false; btn.__loading = true; btn.disabled = true; return true; },
    btnStop: (btn) => { if (!btn) return; btn.__loading = false; btn.disabled = false; },
    loadGcCampaigns: (force) => { calls.loadGcCampaigns.push(force); },
    loadCampaignDetail: (force) => { calls.loadCampaignDetail.push(force); },
  };
  Object.assign(sandboxBase, overrides || {});
  const sandbox = runInSandbox(FULL_SRC, sandboxBase);
  return { sandbox, calls };
}

function publishOpts(sandbox, extra) {
  return Object.assign({
    id: "summer-drop", action: "publish", button: makeBtn("Publish Campaign"),
    confirmTitle: "Publish Summer Drop?",
    confirmMessage: "This will make the campaign live when its schedule and visibility rules allow.",
    run: sandbox.__run || (() => Promise.resolve({ ok: true, status: 200, d: { status: "ok" } })),
    successMessage: "Campaign published.",
    fallbackError: "Couldn't publish this campaign. Try again.",
  }, extra || {});
}

test("Publish action: cancelling the confirmation never issues a request", async () => {
  const { sandbox, calls } = loadAction({ __confirmResult: false });
  let runCalled = false;
  await sandbox.gcRunAction(publishOpts(sandbox, { run: () => { runCalled = true; return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }); } }));
  await flush();
  assert.equal(runCalled, false);
  assert.equal(calls.toast.length, 0);
  assert.equal(calls.confirmSimple.length, 1);
  assert.equal(calls.confirmSimple[0].title, "Publish Summer Drop?");
});

test("Publish action: accepting the confirmation issues exactly one request", async () => {
  const { sandbox } = loadAction();
  let runCount = 0;
  await sandbox.gcRunAction(publishOpts(sandbox, { run: () => { runCount++; return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }); } }));
  await flush();
  assert.equal(runCount, 1);
});

test("Publish action: success reloads the canonical Campaign Detail, shows a success toast, never navigates to the list", async () => {
  const { sandbox, calls } = loadAction();
  await sandbox.gcRunAction(publishOpts(sandbox));
  await flush();
  assert.deepEqual(calls.toast, [["✅ Campaign published.", "success"]]);
  assert.deepEqual(calls.loadCampaignDetail, [true], "must re-GET the canonical campaign — the status pill/setup area/CTA all re-render from that response");
  assert.equal(calls.loadGcCampaigns.length, 0, "must never navigate back to the Campaigns list");
});

test("Publish action: Resume uses its own confirm copy and success message", async () => {
  const { sandbox, calls } = loadAction();
  await sandbox.gcRunAction(publishOpts(sandbox, {
    confirmTitle: "Resume Summer Drop?",
    confirmMessage: "This will make the campaign live again.",
    successMessage: "Campaign resumed.",
  }));
  await flush();
  assert.equal(calls.confirmSimple[0].title, "Resume Summer Drop?");
  assert.deepEqual(calls.toast, [["✅ Campaign resumed.", "success"]]);
});

test("Publish action: a structured backend failure shows the mapped friendly message, never a raw code, never reloads", async () => {
  const { sandbox, calls } = loadAction();
  const rejection = await withRejectionGuard(() => sandbox.gcRunAction(publishOpts(sandbox, {
    run: () => Promise.resolve({ ok: false, status: 400, d: { status: "error", code: "reward_rules_required" } }),
  })));
  assert.equal(rejection, null, "must never become an unhandled promise rejection");
  assert.deepEqual(calls.toast, [["❌ Set up tournament rewards before publishing.", "error"]]);
  assert.equal(calls.loadCampaignDetail.length, 0);
});

test("Publish action: every P0.9-listed failure code maps to its spec copy", () => {
  const { sandbox } = loadAction();
  const cases = {
    reward_rules_required: "Set up tournament rewards before publishing.",
    destination_not_ready: "Complete the destination setup before publishing.",
    provider_inactive: "The selected provider is inactive. Choose an active provider.",
    mission_config_required: "Complete the mission setup before publishing.",
    mission_pool_config_required: "Link a reward pool before publishing.",
    invalid_status_transition: "This action is no longer available for the campaign's current status.",
  };
  Object.keys(cases).forEach((code) => {
    assert.equal(sandbox.gcActionErrorMessage({ d: { code } }), cases[code], code);
  });
});

test("Publish action: repeated clicks while a request is in flight are blocked (gcRunAction in-flight guard)", async () => {
  const { sandbox } = loadAction();
  let resolveRun;
  let runCount = 0;
  const opts = publishOpts(sandbox, {
    run: () => { runCount++; return new Promise((r) => { resolveRun = r; }); },
  });
  const p1 = sandbox.gcRunAction(opts);
  await flush(3);
  const p2 = sandbox.gcRunAction(opts); // same id+action while in flight
  resolveRun({ ok: true, status: 200, d: { status: "ok" } });
  await Promise.all([p1, p2]);
  await flush();
  assert.equal(runCount, 1, "a second click on the same campaign+action must be a no-op, not a second request");
});

// ---------------------------------------------------------------------
// Source-level wiring: the Detail Publish button is data-gc-action="publish"
// dispatched by bindGcCampaigns' shared document click handler, never
// bindCampaignDetail's data-cd-goto handler (which only ever navigates).
// ---------------------------------------------------------------------
test("wiring: the publish click branch never calls activateTab/loadGcCampaigns directly (no forced list navigation)", () => {
  const marker = 'action === "publish"';
  const start = JS.indexOf(marker, JS.indexOf('document.addEventListener("click"', JS.indexOf("function bindGcCampaigns")));
  const nextBranch = JS.indexOf('else if (action ===', start + marker.length);
  const chunk = JS.slice(start, nextBranch);
  assert.doesNotMatch(chunk, /activateTab\(/);
  assert.match(chunk, /gcRunAction\(/);
});

test("wiring: data-cd-goto=\"publish-list\" is no longer emitted by any Campaign Detail builder", () => {
  const htmlSrc = JS.slice(JS.indexOf("function gcCampaignDetailContinueHtml"), JS.indexOf("function gcCampaignDetailShareHtml"));
  assert.doesNotMatch(htmlSrc, /publish-list/, "the old dead-end CTA target must be gone");
});

// =======================================================================
// Part 3 — Preview modal
// =======================================================================

// ---- Minimal hand-rolled DOM (mirrors test_campaign_centre_delete_ui.test.js's
// FakeElement — gcRenderPreviewModal builds its Close button via
// createElement/appendChild the same way openGcDeleteModal does) ----
class FakeElement {
  constructor(tag) {
    this.tagName = (tag || "div").toUpperCase();
    this.className = "";
    this.style = {};
    this.children = [];
    this.parent = null;
    this._text = "";
    this._html = "";
    this._listeners = {};
    this.focused = false;
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = v == null ? "" : String(v); }
  get innerHTML() { return this._html; }
  set innerHTML(v) { this._html = v == null ? "" : String(v); }
  appendChild(node) { node.parent = this; this.children.push(node); return node; }
  remove() {
    if (this.parent) { this.parent.children = this.parent.children.filter((c) => c !== this); this.parent = null; }
  }
  addEventListener(evt, fn) { (this._listeners[evt] = this._listeners[evt] || []).push(fn); }
  _trigger(evt, evtObj) { (this._listeners[evt] || []).slice().forEach((fn) => fn(evtObj || { target: this })); }
  focus() { this.focused = true; }
  get classList() { return { add() {}, remove() {}, contains() { return false; } }; }
}

function walkAll(node, out) {
  out.push(node);
  node.children.forEach((c) => walkAll(c, out));
  return out;
}
function findByText(root, tag, text) {
  return walkAll(root, []).find((n) => n.tagName === tag && n.textContent === text);
}

function makeDocument() {
  const body = new FakeElement("body");
  const docListeners = {};
  return {
    body,
    createElement: (tag) => new FakeElement(tag),
    addEventListener: (evt, fn) => { (docListeners[evt] = docListeners[evt] || []).push(fn); },
    removeEventListener: (evt, fn) => {
      if (!docListeners[evt]) return;
      docListeners[evt] = docListeners[evt].filter((f) => f !== fn);
    },
    _trigger: (evt, evtObj) => { (docListeners[evt] || []).slice().forEach((fn) => fn(evtObj)); },
    _listenerCount: (evt) => (docListeners[evt] || []).length,
  };
}

function loadPreview(overrides) {
  const calls = { toast: [] };
  const document = makeDocument();
  const sandboxBase = {
    state: { view: "gcCampaigns", campaignId: null },
    cdViewState: { campaign: null },
    gcOptionsCache: { providers: [activeProvider], providersPromise: null, campaigns: [], campaignsPromise: Promise.resolve([]) },
    gcKnownCampaignIds: {},
    esc, ccUtcToKlDisplay, gcPill,
    document,
    toast: (msg, kind) => { calls.toast.push([msg, kind]); },
    confirmSimple: () => Promise.resolve(true),
    btnStart: (btn) => { if (!btn || btn.__loading) return false; btn.__loading = true; btn.disabled = true; return true; },
    btnStop: (btn) => { if (!btn) return; btn.__loading = false; btn.disabled = false; },
    loadGcCampaigns: () => {},
    loadCampaignDetail: () => {},
  };
  Object.assign(sandboxBase, overrides || {});
  const sandbox = runInSandbox(FULL_SRC, sandboxBase);
  return { sandbox, calls, document };
}

function previewResponse(overrides) {
  return Object.assign({
    status: "ok",
    card: {
      campaign_id: "summer-drop",
      name: "Summer Drop",
      type: "standard_drop",
      description: "Win big prizes this summer!",
      button_text: "Play now",
      banner_url: "",
    },
    admin_badges: ["draft"],
    effective_visibility: { publicly_visible: false, reasons: ["status is 'draft', not 'live'"] },
  }, overrides || {});
}

test("Preview: gcOpenPreview from the Campaigns list context opens the modal", async () => {
  const resp = previewResponse();
  const { sandbox, document } = loadPreview({
    gcOptionsCache: { providers: [activeProvider], campaigns: [standardDropCampaign()], campaignsPromise: Promise.resolve([]) },
    api: () => Promise.resolve(resp),
  });
  await sandbox.gcOpenPreview("summer-drop", makeBtn("Preview"));
  await flush();
  assert.equal(document.body.children.length, 1, "modal overlay must be appended");
  const overlay = document.body.children[0];
  assert.equal(overlay.className, "modal-overlay");
});

test("Preview: gcOpenPreview from Campaign Detail (cdViewState.campaign set) opens the same modal helper", async () => {
  const resp = previewResponse();
  const { sandbox, document } = loadPreview({
    cdViewState: { campaign: standardDropCampaign() },
    api: () => Promise.resolve(resp),
  });
  await sandbox.gcOpenPreview("summer-drop", makeBtn("Preview Campaign"));
  await flush();
  assert.equal(document.body.children.length, 1);
  // Same status badge derivation (gcFindCachedCampaign checks cdViewState
  // first) proves this is the identical renderer the list context uses,
  // not a second implementation.
  const box = document.body.children[0].children[0];
  assert.match(box.innerHTML, /draft/);
});

test("Preview: the old raw-JSON alert() is gone from the source", () => {
  assert.doesNotMatch(JS, /alert\("Card:/);
  assert.doesNotMatch(JS, /JSON\.stringify\(r\.card/);
});

test("Preview: raw campaign_id/provider_id are never rendered in the modal body", () => {
  const resp = previewResponse({
    card: { campaign_id: "RAW-CAMPAIGN-ID-9001", name: "Summer Drop", type: "standard_drop", description: "d", button_text: "Go", banner_url: "" },
  });
  const cached = { campaign_id: "RAW-CAMPAIGN-ID-9001", status: "draft", type: "standard_drop", destination: { provider_id: "RAW-PROVIDER-ID-42", ready: true }, mechanic: "standard_drop", registration: { enabled: false } };
  const { sandbox } = loadPreview();
  const html = sandbox.gcPreviewModalBodyHtml(resp, cached);
  assert.doesNotMatch(html, /RAW-CAMPAIGN-ID-9001/);
  assert.doesNotMatch(html, /RAW-PROVIDER-ID-42/);
});

test("Preview: the raw `type` enum is never rendered", () => {
  const resp = previewResponse({ card: { campaign_id: "x", name: "X", type: "external_subscription_verification", description: "", button_text: "", banner_url: "" } });
  const { sandbox } = loadPreview();
  const html = sandbox.gcPreviewModalBodyHtml(resp, null);
  assert.doesNotMatch(html, /external_subscription_verification/);
});

test("Preview: no JSON, no snake_case codes, no raw boolean literal, no raw UTC timestamp leak into the body", () => {
  const resp = previewResponse({
    effective_visibility: {
      publicly_visible: false,
      reasons: ["destination.ready is false", "linked provider is inactive", "scheduled to start at 2027-01-01T00:00:00+00:00"],
    },
  });
  const { sandbox } = loadPreview();
  const html = sandbox.gcPreviewModalBodyHtml(resp, null);
  assert.doesNotMatch(html, /\{"/, "no embedded JSON");
  assert.doesNotMatch(html, /destination\.ready/);
  assert.doesNotMatch(html, /linked provider/);
  assert.doesNotMatch(html, />true<|>false</, "no raw boolean literal");
  assert.doesNotMatch(html, /2027-01-01T00:00:00/, "no raw UTC timestamp");
  assert.match(html, /Destination setup is incomplete\./);
  assert.match(html, /The selected provider is inactive\./);
});

test("Preview: known reasons are translated; an unknown reason gets the generic fallback", () => {
  const resp = previewResponse({
    effective_visibility: { publicly_visible: false, reasons: ["destination.ready is false", "a_future_backend_reason"] },
  });
  const { sandbox } = loadPreview();
  const html = sandbox.gcPreviewModalBodyHtml(resp, null);
  assert.match(html, /Destination setup is incomplete\./);
  assert.match(html, /Campaign is not currently visible to players\./);
  assert.doesNotMatch(html, /a_future_backend_reason/);
});

test("Preview: a visible (publicly_visible: true) campaign shows the visible line and no reasons list", () => {
  const resp = previewResponse({ effective_visibility: { publicly_visible: true, reasons: [] } });
  const { sandbox } = loadPreview();
  const html = sandbox.gcPreviewModalBodyHtml(resp, null);
  assert.match(html, /Visible to players/);
  assert.doesNotMatch(html, /<ul/);
});

test("Preview: a fetch failure shows the friendly preview_failed message, never throws, never opens a modal", async () => {
  const { sandbox, calls, document } = loadPreview({ api: () => Promise.reject(new Error("HTTP 500")) });
  const rejection = await withRejectionGuard(() => sandbox.gcOpenPreview("summer-drop", makeBtn("Preview")));
  assert.equal(rejection, null);
  assert.deepEqual(calls.toast, [["❌ Couldn't load campaign preview. Try again.", "error"]]);
  assert.equal(document.body.children.length, 0, "no modal must open on failure");
});

test("Preview modal: Close button removes the overlay and its Escape listener", async () => {
  const resp = previewResponse();
  const { sandbox, document } = loadPreview({ api: () => Promise.resolve(resp) });
  await sandbox.gcOpenPreview("summer-drop", makeBtn("Preview"));
  await flush();
  const overlay = document.body.children[0];
  const closeBtn = findByText(overlay, "BUTTON", "Close");
  assert.ok(closeBtn, "Close button must exist");
  const listenersBefore = document._listenerCount("keydown");
  closeBtn._trigger("click");
  assert.equal(document.body.children.length, 0, "overlay removed from the document");
  assert.equal(document._listenerCount("keydown"), listenersBefore - 1, "Escape listener must be cleaned up on close");
});

test("Preview modal: Escape key closes the modal", async () => {
  const resp = previewResponse();
  const { sandbox, document } = loadPreview({ api: () => Promise.resolve(resp) });
  await sandbox.gcOpenPreview("summer-drop", makeBtn("Preview"));
  await flush();
  assert.equal(document.body.children.length, 1);
  document._trigger("keydown", { key: "Escape" });
  assert.equal(document.body.children.length, 0);
});

test("Preview modal: clicking outside the box (on the overlay) closes the modal", async () => {
  const resp = previewResponse();
  const { sandbox, document } = loadPreview({ api: () => Promise.resolve(resp) });
  await sandbox.gcOpenPreview("summer-drop", makeBtn("Preview"));
  await flush();
  const overlay = document.body.children[0];
  overlay._trigger("click", { target: overlay });
  assert.equal(document.body.children.length, 0);
});

test("Preview modal: clicking inside the box does not close the modal", async () => {
  const resp = previewResponse();
  const { sandbox, document } = loadPreview({ api: () => Promise.resolve(resp) });
  await sandbox.gcOpenPreview("summer-drop", makeBtn("Preview"));
  await flush();
  const overlay = document.body.children[0];
  const box = overlay.children[0];
  overlay._trigger("click", { target: box }); // target is the box, not the overlay itself
  assert.equal(document.body.children.length, 1, "a click that bubbled from inside the box must not close the modal");
});

test("Preview modal: narrow-width safety — banner image and long text never force horizontal overflow", () => {
  const resp = previewResponse({
    card: {
      campaign_id: "x", name: "X", type: "standard_drop",
      description: "A very long description that must wrap instead of forcing the narrow-width modal to scroll horizontally on a phone-width viewport ".repeat(3),
      button_text: "Go", banner_url: "https://example.com/banner.jpg",
    },
  });
  const { sandbox } = loadPreview();
  const html = sandbox.gcPreviewModalBodyHtml(resp, null);
  assert.match(html, /<img[^>]*max-width:100%/, "banner image must be capped to the modal width");
  assert.match(html, /word-break:break-word/, "long text must wrap, never overflow");
});

test("Preview: both entry points (list overflow and Campaign Detail) dispatch to the same gcOpenPreview helper", () => {
  // Both buttons share data-gc-action="preview" and the single delegated
  // click handler in bindGcCampaigns calls gcOpenPreview exactly once —
  // never a second preview renderer for either surface.
  const overflowSrc = JS.slice(JS.indexOf("function gcOverflowMenuHtml"), JS.indexOf("function gcCampaignRowHtml"));
  assert.match(overflowSrc, /data-gc-action="preview"/);
  const detailSrc = JS.slice(JS.indexOf("function gcCampaignDetailHtml("), JS.indexOf("function renderCampaignDetail("));
  assert.match(detailSrc, /data-gc-action="preview"/);
  const bindStart = JS.indexOf("function bindGcCampaigns");
  const clickHandlerSrc = JS.slice(bindStart, JS.indexOf('    });\n\n    $all("#gc-status-filter button")', bindStart));
  const previewBranches = clickHandlerSrc.match(/gcOpenPreview\(/g) || [];
  assert.equal(previewBranches.length, 1, "exactly one call site should ever invoke gcOpenPreview from the click handler");
});

// ---------------------------------------------------------------------
// Regression: existing P0.2-P0.6/P0.8, Mission, delete-modal, Existing
// Drops, and VIEWS/HTML/MODULES sync suites stay green with P0.9 added.
// ---------------------------------------------------------------------
test("Regression: existing test suites still pass", () => {
  const files = [
    "test_admin_dashboard_p0_2_filters.test.js",
    "test_admin_dashboard_p0_3_id_fields.test.js",
    "test_admin_dashboard_p0_4_campaign_list.test.js",
    "test_admin_dashboard_p0_5a_campaign_detail.test.js",
    "test_admin_dashboard_p0_5b_campaign_detail_edit.test.js",
    "test_admin_dashboard_p0_6_campaign_wizard.test.js",
    "test_admin_dashboard_p0_8_action_hardening.test.js",
    "test_admin_dashboard_views_sync.test.js",
    "test_campaign_centre_delete_ui.test.js",
    "test_mission_admin_ui.test.js",
  ];
  files.forEach((f) => {
    execFileSync(process.execPath, ["--test", path.join(__dirname, f)], { stdio: "pipe" });
  });
});
