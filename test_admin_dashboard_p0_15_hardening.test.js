/**
 * P0.15: closes the remaining correctness gaps the P0.13 audit found on top
 * of P0.8/P0.10/P0.14:
 *
 *   B. Share-safe campaign_id budget — "campaign_" + campaign_id must fit
 *      Telegram's 64-char start-param limit, so campaign_id itself is
 *      capped at 55 chars everywhere it's generated or validated
 *      client-side (GC_LINK_SAFE_CAMPAIGN_ID_MAX / gcCampaignIdIsLinkSafe /
 *      gcFitCampaignIdSuffix), plus Campaign Detail's Share block now
 *      distinguishes "bot username not configured" from "campaign_id too
 *      long/unsafe" instead of always blaming the bot username.
 *   C. Duplicate tombstone retry — a single client-side `<id>-copy` guess
 *      can land on a tombstoned id and dead-end permanently;
 *      gcDuplicateCampaignAttempt retries with the next `-copy-N` suffix,
 *      capped, server remaining authoritative throughout.
 *   E. Legacy create's starts_at/ends_at now go through ccKlInputToUtcIso
 *      (Asia/Kuala_Lumpur) instead of new Date(...).toISOString() (browser
 *      timezone).
 *
 * Mirrors the existing P0.3/P0.8 harness: no build step, no jsdom — the
 * relevant source ranges are extracted as text and executed in sandboxed vm
 * contexts against small stand-ins.
 *
 * Run with: node --test test_admin_dashboard_p0_15_hardening.test.js
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

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign({ console, Object, String, Promise, JSON, Array, Math, RegExp }, sandboxExtra);
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

async function flush(n) {
  for (let i = 0; i < (n || 12); i++) await Promise.resolve();
}

// ---------------------------------------------------------------------
// Source ranges
// ---------------------------------------------------------------------
// Share-safe id constant/helpers + gcSlugify/gcSlugCandidate/
// gcFirstAvailableSuffix, up to (not including) loadGcCampaigns — same
// range test_admin_dashboard_p0_3_id_fields.test.js's OPTIONS_SRC uses.
const OPTIONS_SRC = slice(JS, "  var gcOptionsCache = {", "\n  function loadGcCampaigns(force) {");
// GC_ACTION_ERROR_MESSAGES/gcActionErrorMessage/gcRunAction/gcActionsInFlight
// plus (P0.15) gcDuplicateIdCandidate/gcFirstAvailableDuplicateSuffix/
// gcNextDuplicateId/gcDuplicateCampaignAttempt/GC_DUPLICATE_MAX_ATTEMPTS.
const ACTION_SRC = slice(JS, "  var GC_ACTION_ERROR_MESSAGES = {", "\n  function bindGcCampaigns() {");
const INVALIDATE_SRC = slice(JS, "  function gcInvalidateCampaignsCache() {", "\n  function fetchGcProviders(force)");
// gcComputeShareState/gcShareUnavailableReasonText/gcIsMissionCampaign —
// same PURE_SRC range test_admin_dashboard_p0_5a/p0_10 use.
const PURE_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  // ---- Composer + orchestration (DOM-touching)");
const SHARE_SAFE_ID_SRC = slice(JS, "  // ---------- Share-safe campaign_id budget (P0.15) ----------", "\n  // ---------- Campaign ID slug generation (P0.3)");

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// =======================================================================
// B2/B3 — Share-safe slug generation + manual id validation
// =======================================================================
function loadSlug(overrides) {
  const sandboxBase = { gcKnownCampaignIds: {} };
  Object.assign(sandboxBase, overrides || {});
  return runInSandbox(
    OPTIONS_SRC + "\nthis.gcSlugify = gcSlugify; this.gcSlugCandidate = gcSlugCandidate; " +
      "this.gcFirstAvailableSuffix = gcFirstAvailableSuffix; this.gcKnownCampaignIds = gcKnownCampaignIds; " +
      "this.gcCampaignIdIsLinkSafe = gcCampaignIdIsLinkSafe; this.GC_LINK_SAFE_CAMPAIGN_ID_MAX = GC_LINK_SAFE_CAMPAIGN_ID_MAX; " +
      "this.GC_CAMPAIGN_ID_FORMAT_ERROR = GC_CAMPAIGN_ID_FORMAT_ERROR;",
    sandboxBase
  );
}

test("GC_LINK_SAFE_CAMPAIGN_ID_MAX is exactly 55 (64-char Telegram start-param budget minus the 9-char 'campaign_' prefix)", () => {
  const s = loadSlug();
  assert.equal(s.GC_LINK_SAFE_CAMPAIGN_ID_MAX, 55);
});

test("short normal slug is unchanged", () => {
  const s = loadSlug();
  assert.equal(s.gcSlugify("October Lucky Draw"), "october-lucky-draw");
});

test("a name that slugifies to exactly 55 chars is valid and unchanged", () => {
  const s = loadSlug();
  const name = "a".repeat(55);
  const slug = s.gcSlugify(name);
  assert.equal(slug.length, 55);
  assert.equal(slug, "a".repeat(55));
});

test("a name that slugifies past 55 chars truncates safely, with no dangling separator", () => {
  const s = loadSlug();
  const name = "a-very-long-campaign-name-that-goes-on-and-on-and-on-and-on-and-on-forever";
  const slug = s.gcSlugify(name);
  assert.ok(slug.length <= 55, "slug must never exceed the share-safe budget: " + slug.length);
  assert.ok(!slug.endsWith("-"), "truncation must not leave a dangling separator");
});

test("a -2 collision suffix on a base already near the cap still fits within 55", () => {
  const base = "b".repeat(60); // gcSlugify would already cap this, but exercise gcSlugCandidate directly
  const s = loadSlug();
  const candidate = s.gcSlugCandidate(base, 2);
  assert.ok(candidate.length <= 55, "candidate length: " + candidate.length);
  assert.match(candidate, /-2$/, "the suffix itself must survive the truncation, never be dropped or distorted");
});

test("a high collision suffix (e.g. -12) still fits within 55 and keeps the exact suffix", () => {
  const base = "c".repeat(60);
  const s = loadSlug();
  const candidate = s.gcSlugCandidate(base, 12);
  assert.ok(candidate.length <= 55, "candidate length: " + candidate.length);
  assert.match(candidate, /-12$/);
});

test("gcCampaignIdIsLinkSafe: manual 56-char id is rejected", () => {
  const s = loadSlug();
  assert.equal(s.gcCampaignIdIsLinkSafe("a".repeat(56)), false);
});

test("gcCampaignIdIsLinkSafe: manual 55-char id is accepted", () => {
  const s = loadSlug();
  assert.equal(s.gcCampaignIdIsLinkSafe("a".repeat(55)), true);
});

test("gcCampaignIdIsLinkSafe: invalid characters are rejected", () => {
  const s = loadSlug();
  ["has space", "has/slash", "has.dot", "has_✓emoji", ""].forEach((bad) => {
    assert.equal(s.gcCampaignIdIsLinkSafe(bad), false, "should reject: " + JSON.stringify(bad));
  });
});

test("gcCampaignIdIsLinkSafe: a normal valid manual id is accepted unchanged", () => {
  const s = loadSlug();
  assert.equal(s.gcCampaignIdIsLinkSafe("july-tournament_2026"), true);
});

test("GC_CAMPAIGN_ID_FORMAT_ERROR names both the charset and the 55-char limit", () => {
  const s = loadSlug();
  assert.match(s.GC_CAMPAIGN_ID_FORMAT_ERROR, /letters, numbers, - or _/);
  assert.match(s.GC_CAMPAIGN_ID_FORMAT_ERROR, /55/);
});

test("manual id validation is wired into the legacy create handler", () => {
  const start = JS.indexOf("function bindGcCampaigns()");
  const end = JS.indexOf("\n  document.addEventListener(\"click\"", start);
  const chunk = JS.slice(start, end);
  assert.match(chunk, /gcCampaignIdIsLinkSafe\(manualId\)/);
  assert.match(chunk, /GC_CAMPAIGN_ID_FORMAT_ERROR/);
});

test("manual id validation is wired into the wizard's step-1 validator", () => {
  const start = JS.indexOf("function gcwValidateStep(step)");
  const end = JS.indexOf("\n  }\n\n", start);
  const chunk = JS.slice(start, end);
  assert.match(chunk, /gcCampaignIdIsLinkSafe\(wizManualId\)/);
  assert.match(chunk, /GC_CAMPAIGN_ID_FORMAT_ERROR/);
});

// =======================================================================
// B4 — Share-state reason differentiation + Mission share link
// =======================================================================
function loadShareState() {
  return runInSandbox(
    SHARE_SAFE_ID_SRC + "\n" + PURE_SRC +
      "\nthis.gcComputeShareState = gcComputeShareState; this.gcCampaignIdIsLinkSafe = gcCampaignIdIsLinkSafe;",
    { esc }
  );
}

test("share link present (registration): uses the server link verbatim", () => {
  const s = loadShareState();
  const state = s.gcComputeShareState({
    campaign_id: "lucky-draw", registration: { enabled: true },
    registration_deep_link: "https://t.me/Bot?startapp=campaign_lucky-draw",
  });
  assert.equal(state.available, true);
  assert.equal(state.link, "https://t.me/Bot?startapp=campaign_lucky-draw");
});

test("share link present (mission): surfaces the existing mission_<id> deep link instead of claiming no link exists", () => {
  const s = loadShareState();
  const state = s.gcComputeShareState({
    campaign_id: "trivia-1", mechanic: "mission_pool",
    mission_link: "https://t.me/Bot?startapp=mission_trivia-1",
  });
  assert.equal(state.available, true);
  assert.equal(state.link, "https://t.me/Bot?startapp=mission_trivia-1");
});

test("share link missing (registration enabled, bot username not configured): distinct reason", () => {
  const s = loadShareState();
  const state = s.gcComputeShareState({
    campaign_id: "lucky-draw", registration: { enabled: true },
    registration_deep_link: null, registration_deep_link_unavailable_reason: "bot_username_not_configured",
  });
  assert.equal(state.available, false);
  assert.match(state.reason, /bot username/);
  assert.doesNotMatch(state.reason, /too long/);
});

test("share link missing (registration enabled, campaign_id not link-safe): distinct, non-repairable reason", () => {
  const s = loadShareState();
  const state = s.gcComputeShareState({
    campaign_id: "x".repeat(60), registration: { enabled: true },
    registration_deep_link: null, registration_deep_link_unavailable_reason: "campaign_id_not_link_safe",
  });
  assert.equal(state.available, false);
  assert.match(state.reason, /too long/);
  assert.doesNotMatch(state.reason, /bot username/);
  // never framed as something the admin can just fix
  assert.doesNotMatch(state.reason, /temporarily/i);
});

test("share link missing (mission, campaign_id not link-safe): same reason distinction applies to Mission", () => {
  const s = loadShareState();
  const state = s.gcComputeShareState({
    campaign_id: "y".repeat(60), mechanic: "mission_pool",
    mission_link: null, mission_link_unavailable_reason: "campaign_id_not_link_safe",
  });
  assert.equal(state.available, false);
  assert.match(state.reason, /too long/);
});

test("existing (legacy) campaign with an id over 55 chars gets the exact 'too long' explanation even without a server reason field", () => {
  // Defensive client-side fallback for a stale cached campaign object that
  // predates the server sending registration_deep_link_unavailable_reason.
  const s = loadShareState();
  const state = s.gcComputeShareState({
    campaign_id: "z".repeat(60), registration: { enabled: true }, registration_deep_link: null,
  });
  assert.equal(state.available, false);
  assert.match(state.reason, /too long/);
});

test("a non-registration, non-mission campaign type still gets the generic 'no shareable link' message, unaffected", () => {
  const s = loadShareState();
  const state = s.gcComputeShareState({
    campaign_id: "july-tournament", type: "tournament", registration: { enabled: false },
  });
  assert.equal(state.available, false);
  assert.match(state.reason, /doesn't have a shareable link/);
});

// =======================================================================
// C — Duplicate tombstone retry (gcDuplicateCampaignAttempt)
// =======================================================================
function makeApiPostJsonQueue() {
  const calls = [];
  const queue = [];
  const impl = (path, body) => {
    calls.push({ path, body });
    const next = queue.shift();
    if (!next) return Promise.reject(new Error("no response queued for " + path));
    return Promise.resolve(next);
  };
  impl.calls = calls;
  impl.push = (res) => queue.push(res);
  return impl;
}

function loadDuplicate(overrides) {
  const sandboxBase = { gcKnownCampaignIds: {} };
  Object.assign(sandboxBase, overrides || {});
  const fullSrc = SHARE_SAFE_ID_SRC + "\n" + INVALIDATE_SRC + "\n" + ACTION_SRC +
    "\nthis.gcDuplicateCampaignAttempt = gcDuplicateCampaignAttempt; " +
    "this.gcDuplicateIdCandidate = gcDuplicateIdCandidate; " +
    "this.gcFirstAvailableDuplicateSuffix = gcFirstAvailableDuplicateSuffix; " +
    "this.GC_DUPLICATE_MAX_ATTEMPTS = GC_DUPLICATE_MAX_ATTEMPTS;";
  return runInSandbox(fullSrc, sandboxBase);
}

test("first Duplicate click: no collision, one request, candidate is <id>-copy", async () => {
  const apiPostJson = makeApiPostJsonQueue();
  const s = loadDuplicate({ apiPostJson });
  apiPostJson.push({ ok: true, status: 201, d: { status: "ok", campaign_id: "src-copy" } });
  const res = await s.gcDuplicateCampaignAttempt("src", s.gcFirstAvailableDuplicateSuffix("src"), s.GC_DUPLICATE_MAX_ATTEMPTS);
  assert.equal(apiPostJson.calls.length, 1);
  assert.equal(apiPostJson.calls[0].body.campaign_id, "src-copy");
  assert.equal(res.d.campaign_id, "src-copy");
});

test("second Duplicate click (known -copy already taken client-side): starts straight at -copy-2", async () => {
  const apiPostJson = makeApiPostJsonQueue();
  const s = loadDuplicate({ apiPostJson, gcKnownCampaignIds: { "src-copy": true } });
  apiPostJson.push({ ok: true, status: 201, d: { status: "ok", campaign_id: "src-copy-2" } });
  const res = await s.gcDuplicateCampaignAttempt("src", s.gcFirstAvailableDuplicateSuffix("src"), s.GC_DUPLICATE_MAX_ATTEMPTS);
  assert.equal(apiPostJson.calls.length, 1);
  assert.equal(apiPostJson.calls[0].body.campaign_id, "src-copy-2");
  assert.equal(res.d.campaign_id, "src-copy-2");
});

test("stale client cache collision: backend rejects duplicate_campaign_id, retries once with the next suffix", async () => {
  const apiPostJson = makeApiPostJsonQueue();
  const s = loadDuplicate({ apiPostJson }); // gcKnownCampaignIds is stale/empty — doesn't know src-copy is now taken
  apiPostJson.push({ ok: false, status: 409, d: { status: "error", code: "duplicate_campaign_id" } });
  apiPostJson.push({ ok: true, status: 201, d: { status: "ok", campaign_id: "src-copy-2" } });
  const res = await s.gcDuplicateCampaignAttempt("src", 1, s.GC_DUPLICATE_MAX_ATTEMPTS);
  assert.equal(apiPostJson.calls.length, 2);
  assert.equal(apiPostJson.calls[0].body.campaign_id, "src-copy");
  assert.equal(apiPostJson.calls[1].body.campaign_id, "src-copy-2");
  assert.equal(res.d.campaign_id, "src-copy-2");
});

test("tombstoned <id>-copy (campaign_id_previously_deleted): retries with -copy-2, never dead-ends", async () => {
  const apiPostJson = makeApiPostJsonQueue();
  const s = loadDuplicate({ apiPostJson });
  apiPostJson.push({ ok: false, status: 409, d: { status: "error", code: "campaign_id_previously_deleted" } });
  apiPostJson.push({ ok: true, status: 201, d: { status: "ok", campaign_id: "src-copy-2" } });
  const res = await s.gcDuplicateCampaignAttempt("src", 1, s.GC_DUPLICATE_MAX_ATTEMPTS);
  assert.equal(apiPostJson.calls.length, 2);
  assert.equal(res.ok, true);
  assert.equal(res.d.campaign_id, "src-copy-2");
});

test("tombstoned <id>-copy AND <id>-copy-2: keeps retrying and eventually succeeds at -copy-3", async () => {
  const apiPostJson = makeApiPostJsonQueue();
  const s = loadDuplicate({ apiPostJson });
  apiPostJson.push({ ok: false, status: 409, d: { status: "error", code: "campaign_id_previously_deleted" } });
  apiPostJson.push({ ok: false, status: 409, d: { status: "error", code: "campaign_id_previously_deleted" } });
  apiPostJson.push({ ok: true, status: 201, d: { status: "ok", campaign_id: "src-copy-3" } });
  const res = await s.gcDuplicateCampaignAttempt("src", 1, s.GC_DUPLICATE_MAX_ATTEMPTS);
  assert.equal(apiPostJson.calls.length, 3);
  assert.deepEqual(apiPostJson.calls.map((c) => c.body.campaign_id), ["src-copy", "src-copy-2", "src-copy-3"]);
  assert.equal(res.d.campaign_id, "src-copy-3");
});

test("retry is capped at GC_DUPLICATE_MAX_ATTEMPTS — never an unbounded loop", async () => {
  const apiPostJson = makeApiPostJsonQueue();
  const s = loadDuplicate({ apiPostJson });
  for (let i = 0; i < s.GC_DUPLICATE_MAX_ATTEMPTS; i++) {
    apiPostJson.push({ ok: false, status: 409, d: { status: "error", code: "duplicate_campaign_id" } });
  }
  const res = await s.gcDuplicateCampaignAttempt("src", 1, s.GC_DUPLICATE_MAX_ATTEMPTS);
  assert.equal(apiPostJson.calls.length, s.GC_DUPLICATE_MAX_ATTEMPTS, "must stop retrying at the cap");
  assert.equal(res.ok, false, "the final (still-failing) response is returned, not swallowed");
  assert.equal(res.d.code, "duplicate_campaign_id");
});

test("an unrelated error (e.g. not_found) is never retried", async () => {
  const apiPostJson = makeApiPostJsonQueue();
  const s = loadDuplicate({ apiPostJson });
  apiPostJson.push({ ok: false, status: 404, d: { status: "error", code: "not_found" } });
  const res = await s.gcDuplicateCampaignAttempt("src", 1, s.GC_DUPLICATE_MAX_ATTEMPTS);
  assert.equal(apiPostJson.calls.length, 1, "must not retry on an error other than the two collision codes");
  assert.equal(res.d.code, "not_found");
});

test("wiring: the Duplicate click handler calls gcDuplicateCampaignAttempt (not a single-shot request) and opens the backend-returned campaign on success", () => {
  const start = JS.indexOf('action === "duplicate"');
  const end = JS.indexOf('action === "preview"', start);
  const chunk = JS.slice(start, end);
  assert.match(chunk, /gcRunAction\(/);
  assert.match(chunk, /gcDuplicateCampaignAttempt\(id,\s*gcFirstAvailableDuplicateSuffix\(id\),\s*GC_DUPLICATE_MAX_ATTEMPTS\)/);
  assert.match(chunk, /renderCampaignDetail\(d\.campaign_id\)/);
});

// =======================================================================
// E — Legacy create timezone fix
// =======================================================================
function loadKl() {
  return runInSandbox(
    slice(JS, "  function ccPad2(n)", "\n  var CC_CONTENT_ICON") + "\nthis.ccKlInputToUtcIso = ccKlInputToUtcIso;",
    {}
  );
}

test("ccKlInputToUtcIso: a KL (UTC+8) input converts to the correct UTC instant regardless of the machine's own timezone", () => {
  const s = loadKl();
  // 2026-10-01 09:00 in Asia/Kuala_Lumpur (UTC+8) is 2026-10-01T01:00:00Z.
  assert.equal(s.ccKlInputToUtcIso("2026-10-01T09:00"), "2026-10-01T01:00:00.000Z");
});

test("ccKlInputToUtcIso: empty/missing value returns null, not a crash or 'Invalid Date'", () => {
  const s = loadKl();
  assert.equal(s.ccKlInputToUtcIso(""), null);
  assert.equal(s.ccKlInputToUtcIso(null), null);
  assert.equal(s.ccKlInputToUtcIso(undefined), null);
});

test("legacy create no longer uses new Date(...).toISOString() (browser-local) for starts_at/ends_at", () => {
  const start = JS.indexOf("var createBtn = $(\"#gc-create-campaign-btn\");");
  const end = JS.indexOf("createBtn.addEventListener", start) === -1 ? JS.length : JS.indexOf("\n      });\n    }\n", start);
  const chunk = JS.slice(start, end);
  assert.match(chunk, /starts_at:\s*ccKlInputToUtcIso\(\$\("#gc-c-starts"\)\.value\)/);
  assert.match(chunk, /ends_at:\s*ccKlInputToUtcIso\(\$\("#gc-c-ends"\)\.value\)/);
  assert.doesNotMatch(chunk, /new Date\(\$\("#gc-c-starts"\)\.value\)/);
  assert.doesNotMatch(chunk, /new Date\(\$\("#gc-c-ends"\)\.value\)/);
});

test("the legacy form's start/end inputs are now labeled Asia/Kuala_Lumpur", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.html"), "utf8");
  const start = html.indexOf('id="gc-c-starts"');
  const chunk = html.slice(Math.max(0, start - 200), start + 400);
  assert.match(chunk, /Asia\/Kuala_Lumpur/);
});

// =======================================================================
// A3 — Mission error mappings for the actual backend codes
// (mission_pool.py's admin close/cancel/resume/process/end-rewards
// endpoints only ever return not_found, auth_failed, and (process only)
// mission_pool_disabled — confirmed by reading mission_pool.py, not
// assumed; the hypothetical per-transition codes the P0.13 audit
// speculated about don't exist in the codebase.)
// =======================================================================
function loadActionErrors() {
  return runInSandbox(ACTION_SRC + "\nthis.gcActionErrorMessage = gcActionErrorMessage;", {});
}

test("mission_pool_disabled maps to a friendly, non-raw message", () => {
  const s = loadActionErrors();
  const msg = s.gcActionErrorMessage({ d: { code: "mission_pool_disabled" } });
  assert.doesNotMatch(msg, /_/);
  assert.match(msg, /disabled/i);
});

test("auth_failed maps to a friendly, non-raw message", () => {
  const s = loadActionErrors();
  const msg = s.gcActionErrorMessage({ d: { code: "auth_failed" } });
  assert.doesNotMatch(msg, /_/);
});

test("not_found (the real code mission-pool admin endpoints return on a missing/non-mission campaign) is already mapped", () => {
  const s = loadActionErrors();
  assert.equal(s.gcActionErrorMessage({ d: { code: "not_found" } }), "This campaign no longer exists.");
});
