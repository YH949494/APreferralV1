/**
 * P0.8: Harden every gc_campaigns lifecycle/action path (Publish/Resume/
 * Pause/Archive/Duplicate/Preview/Registration-save) so a rejected/non-2xx
 * backend response can never fail silently, never shows a raw snake_case
 * backend code, and never lets the UI repaint as if the action succeeded.
 *
 * The audit this PR fixes: every action called apiPost(), which throws a
 * bare `new Error("HTTP 400")` on any non-2xx response — discarding the
 * `{status:"error", code:"..."}` body campaign_centre.py's _transition()/
 * duplicate_campaign/update_campaign actually return — with NO .catch() at
 * any call site, so that rejection was an unhandled promise rejection and
 * the admin saw nothing at all (not even a red toast).
 *
 * This suite covers the new single choke point, gcRunAction (and its
 * helpers GC_ACTION_ERROR_MESSAGES/gcActionErrorMessage/gcDefaultRefresh/
 * gcNextDuplicateId), plus source-level wiring checks proving every call
 * site (publish/pause/archive/duplicate/preview/registration-save) was
 * actually switched onto it and onto apiPostJson/apiPutJson (which resolve
 * with `{ok, status, d}` instead of throwing away the JSON body).
 *
 * Mirrors the existing P0.4/P0.5b/P0.6 harness: no build step, no jsdom —
 * relevant source ranges are extracted as text and executed in sandboxed vm
 * contexts against small stand-ins, plus plain string/regex assertions
 * against the raw file for wiring facts a runtime test can't easily see.
 *
 * Run with: node --test test_admin_dashboard_p0_8_action_hardening.test.js
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

function runInSandbox(code, sandboxExtra) {
  const sandbox = sandboxExtra || {};
  sandbox.console = sandbox.console || console;
  sandbox.Object = sandbox.Object || Object;
  sandbox.String = sandbox.String || String;
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

// Proves a chunk of async work never produces an unhandled promise
// rejection — the exact bug class this whole PR exists to close (apiPost's
// throw-on-non-2xx with no .catch() at any gc_campaigns action call site).
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
// Real source: the whole P0.8 action-hardening block (GC_ACTION_ERROR_
// MESSAGES, gcActionErrorMessage, gcDefaultRefresh, gcActionsInFlight,
// gcRunAction, gcNextDuplicateId) plus gcInvalidateCampaignsCache (P0.4,
// reused as-is — never reimplemented here).
// ---------------------------------------------------------------------
const INVALIDATE_SRC = slice(JS, "  function gcInvalidateCampaignsCache() {", "\n  function fetchGcProviders(force)");
const ACTION_SRC = slice(JS, "  var GC_ACTION_ERROR_MESSAGES = {", "\n  function bindGcCampaigns() {");

function makeBtn(label) {
  return { textContent: label || "Action", disabled: false, dataset: {}, classList: { add() {}, remove() {} }, innerHTML: "" };
}

function loadAction(overrides) {
  const calls = { toast: [], confirmSimple: [], btnStart: [], btnStop: [], loadGcCampaigns: [], loadCampaignDetail: [] };
  const sandboxBase = {
    state: { view: "gcCampaigns", campaignId: null },
    gcOptionsCache: { providers: null, providersPromise: null, campaigns: ["stub"], campaignsPromise: Promise.resolve(["stub"]) },
    gcKnownCampaignIds: {},
    toast: (msg, kind) => { calls.toast.push([msg, kind]); },
    confirmSimple: (title, message) => {
      calls.confirmSimple.push({ title, message });
      return Promise.resolve(sandboxBase.__confirmResult !== false);
    },
    btnStart: (btn, text) => {
      calls.btnStart.push({ btn, text });
      if (!btn || btn.__loading) return false;
      btn.__loading = true; btn.disabled = true;
      return true;
    },
    btnStop: (btn) => {
      calls.btnStop.push(btn);
      if (!btn) return;
      btn.__loading = false; btn.disabled = false;
    },
    loadGcCampaigns: (force) => { calls.loadGcCampaigns.push(force); },
    loadCampaignDetail: (force) => { calls.loadCampaignDetail.push(force); },
  };
  Object.assign(sandboxBase, overrides || {});

  const fullSrc = INVALIDATE_SRC + "\n" + ACTION_SRC +
    "\nthis.gcRunAction = gcRunAction; this.gcActionErrorMessage = gcActionErrorMessage; " +
    "this.GC_ACTION_ERROR_MESSAGES = GC_ACTION_ERROR_MESSAGES; this.gcDefaultRefresh = gcDefaultRefresh; " +
    "this.gcNextDuplicateId = gcNextDuplicateId; this.gcOptionsCache = gcOptionsCache;";

  const sandbox = runInSandbox(fullSrc, sandboxBase);
  return { sandbox, calls };
}

// ---------------------------------------------------------------------
// A. Error mapping — every code the audit + spec calls out, plain-English,
//    never the raw snake_case code.
// ---------------------------------------------------------------------
test("A: every spec-required backend code maps to a plain-English message", () => {
  const { sandbox } = loadAction();
  const cases = {
    reward_rules_required: "Set up tournament rewards before publishing.",
    destination_not_ready: "Complete the destination setup before publishing.",
    provider_inactive: "The selected provider is inactive. Choose an active provider.",
    provider_not_found: "The selected provider no longer exists. Choose another provider.",
    mission_config_required: "Complete the mission setup before publishing.",
    mission_pool_config_required: "Link a reward pool before publishing.",
    invalid_status_transition: "This action is no longer available for the campaign's current status.",
    not_found: "This campaign no longer exists.",
  };
  Object.keys(cases).forEach((code) => {
    const msg = sandbox.gcActionErrorMessage({ d: { code } });
    assert.equal(msg, cases[code], "code: " + code);
    assert.doesNotMatch(msg, /_/, "friendly message for " + code + " must not leak a snake_case fragment");
  });
});

test("A: invalid_status_for_deletion gets its own friendly message", () => {
  const { sandbox } = loadAction();
  assert.match(sandbox.gcActionErrorMessage({ d: { code: "invalid_status_for_deletion" } }), /archive it first/);
});

test("A: an unknown/unmapped code falls back to the generic retry message, never the raw code", () => {
  const { sandbox, calls } = loadAction();
  const errSpy = [];
  const origError = console.error;
  console.error = (...a) => errSpy.push(a);
  try {
    const msg = sandbox.gcActionErrorMessage({ d: { code: "some_new_backend_code_nobody_mapped_yet" } });
    assert.equal(msg, "Couldn't complete this action. Try again.");
    assert.doesNotMatch(msg, /some_new_backend_code/);
    // still logged for debugging — raw code is never lost, only hidden from the admin surface
    assert.ok(errSpy.some((a) => String(a.join(" ")).includes("some_new_backend_code_nobody_mapped_yet")));
  } finally { console.error = origError; }
});

test("A: no code at all (network error / malformed response) uses the caller's fallback, or the generic default", () => {
  const { sandbox } = loadAction();
  assert.equal(sandbox.gcActionErrorMessage(null, "Couldn't publish this campaign. Try again."), "Couldn't publish this campaign. Try again.");
  assert.equal(sandbox.gcActionErrorMessage({ d: {} }), "Couldn't complete this action. Try again.");
});

// ---------------------------------------------------------------------
// B. gcRunAction — success path
// ---------------------------------------------------------------------
test("B: success shows a success toast, invalidates the campaigns cache, refreshes the list, and calls onSuccess", async () => {
  const { sandbox, calls } = loadAction();
  const btn = makeBtn("Publish");
  let onSuccessArg = null;
  await sandbox.gcRunAction({
    id: "camp-1", action: "publish", button: btn,
    run: () => Promise.resolve({ ok: true, status: 200, d: { status: "ok", campaign_status: "live" } }),
    successMessage: "Campaign published.",
    onSuccess: (d) => { onSuccessArg = d; },
  });
  await flush();

  assert.deepEqual(calls.toast, [["✅ Campaign published.", "success"]]);
  assert.equal(sandbox.gcOptionsCache.campaigns, null, "cache must be invalidated on success");
  assert.deepEqual(calls.loadGcCampaigns, [true]);
  assert.equal(calls.loadCampaignDetail.length, 0);
  assert.deepEqual(onSuccessArg, { status: "ok", campaign_status: "live" });
  assert.equal(btn.disabled, false, "button must be re-enabled after success");
});

test("B: successMessage may be a function of the response body", async () => {
  const { sandbox, calls } = loadAction();
  await sandbox.gcRunAction({
    id: "camp-1", action: "duplicate",
    run: () => Promise.resolve({ ok: true, status: 201, d: { status: "ok", campaign_id: "camp-1-copy" } }),
    successMessage: (d) => "Duplicated as " + d.campaign_id,
  });
  await flush();
  assert.deepEqual(calls.toast, [["✅ Duplicated as camp-1-copy", "success"]]);
});

// ---------------------------------------------------------------------
// C. gcRunAction — no silent failures: structured backend error
// ---------------------------------------------------------------------
test("C: a structured error response (res.d.status !== 'ok') shows the friendly error, never a success toast, never refreshes", async () => {
  const { sandbox, calls } = loadAction();
  const btn = makeBtn("Publish");
  let onSuccessCalled = false;
  const rejection = await withRejectionGuard(() => sandbox.gcRunAction({
    id: "camp-1", action: "publish", button: btn,
    run: () => Promise.resolve({ ok: false, status: 400, d: { status: "error", code: "reward_rules_required" } }),
    successMessage: "Campaign published.",
    onSuccess: () => { onSuccessCalled = true; },
  }));

  assert.equal(rejection, null, "must never produce an unhandled promise rejection");
  assert.equal(calls.toast.length, 1);
  assert.equal(calls.toast[0][1], "error");
  assert.equal(calls.toast[0][0], "❌ Set up tournament rewards before publishing.");
  assert.equal(onSuccessCalled, false, "onSuccess must never run for a rejected action");
  assert.deepEqual(calls.loadGcCampaigns, [], "the list must never repaint as if a rejected action succeeded");
  assert.notEqual(sandbox.gcOptionsCache.campaigns, null, "cache must not be invalidated on failure");
  assert.equal(btn.disabled, false, "button must be re-enabled after a failure, not left stuck");
});

test("C: a rejected run() promise (e.g. apiPostJson's own 401-redirect throw, or a genuine network error) is caught, not left unhandled", async () => {
  const { sandbox, calls } = loadAction();
  const btn = makeBtn("Archive");
  const rejection = await withRejectionGuard(() => sandbox.gcRunAction({
    id: "camp-1", action: "archive", button: btn,
    run: () => Promise.reject(new Error("network down")),
    fallbackError: "Couldn't archive this campaign. Try again.",
  }));

  assert.equal(rejection, null);
  assert.deepEqual(calls.toast, [["❌ Couldn't archive this campaign. Try again.", "error"]]);
  assert.deepEqual(calls.loadGcCampaigns, []);
  assert.equal(btn.disabled, false);
});

test("C: a synchronous throw inside run() is also caught, not left unhandled", async () => {
  const { sandbox, calls } = loadAction();
  const rejection = await withRejectionGuard(() => sandbox.gcRunAction({
    id: "camp-1", action: "pause",
    run: () => { throw new Error("boom"); },
    fallbackError: "Couldn't pause this campaign. Try again.",
  }));
  assert.equal(rejection, null);
  assert.deepEqual(calls.toast, [["❌ Couldn't pause this campaign. Try again.", "error"]]);
});

test("C: a falsy/malformed response (no res.d at all) is treated as a failure, not a crash", async () => {
  const { sandbox, calls } = loadAction();
  const rejection = await withRejectionGuard(() => sandbox.gcRunAction({
    id: "camp-1", action: "publish",
    run: () => Promise.resolve(undefined),
  }));
  assert.equal(rejection, null);
  assert.deepEqual(calls.toast, [["❌ Couldn't complete this action. Try again.", "error"]]);
});

// ---------------------------------------------------------------------
// D. Confirmation gating (Archive)
// ---------------------------------------------------------------------
test("D: cancelling the confirmation never calls run(), never toasts, never refreshes", async () => {
  const { sandbox, calls } = loadAction({ __confirmResult: false });
  let runCalled = false;
  await sandbox.gcRunAction({
    id: "camp-1", action: "archive",
    confirmTitle: "Archive campaign?",
    confirmMessage: 'Archive "Weekly Giveaway"?',
    run: () => { runCalled = true; return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }); },
    successMessage: "Campaign archived.",
  });
  await flush();

  assert.equal(runCalled, false);
  assert.deepEqual(calls.toast, []);
  assert.deepEqual(calls.loadGcCampaigns, []);
  assert.equal(calls.confirmSimple.length, 1);
  assert.match(calls.confirmSimple[0].message, /Weekly Giveaway/);
});

test("D: confirming proceeds through run() as normal", async () => {
  const { sandbox, calls } = loadAction({ __confirmResult: true });
  await sandbox.gcRunAction({
    id: "camp-1", action: "archive",
    confirmMessage: "Archive it?",
    run: () => Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }),
    successMessage: "Campaign archived.",
  });
  await flush();
  assert.deepEqual(calls.toast, [["✅ Campaign archived.", "success"]]);
});

test("D: no confirmMessage means no confirmation prompt at all (Publish/Pause never confirm)", async () => {
  const { sandbox, calls } = loadAction();
  await sandbox.gcRunAction({
    id: "camp-1", action: "pause",
    run: () => Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }),
  });
  await flush();
  assert.equal(calls.confirmSimple.length, 0);
});

// ---------------------------------------------------------------------
// E. Repeated-click guard: campaign_id + action key
// ---------------------------------------------------------------------
test("E: the same campaign_id+action cannot run twice concurrently", async () => {
  const { sandbox, calls } = loadAction();
  let runCount = 0;
  let resolveFirst;
  const run = () => {
    runCount++;
    return new Promise((resolve) => { resolveFirst = resolve; });
  };

  const p1 = sandbox.gcRunAction({ id: "camp-1", action: "publish", run, successMessage: "ok" });
  const p2 = sandbox.gcRunAction({ id: "camp-1", action: "publish", run, successMessage: "ok" }); // double-click
  await flush();

  assert.equal(runCount, 1, "a second concurrent call for the same id+action must be a no-op, not a second request");

  resolveFirst({ ok: true, status: 200, d: { status: "ok" } });
  await Promise.all([p1, p2]);
  await flush();
  assert.equal(calls.toast.filter((t) => t[1] === "success").length, 1);
});

test("E: a different action on the same campaign is NOT blocked", async () => {
  const { sandbox } = loadAction();
  let publishCalled = false, pauseCalled = false;
  let resolvePublish;
  sandbox.gcRunAction({
    id: "camp-1", action: "publish",
    run: () => { publishCalled = true; return new Promise((r) => { resolvePublish = r; }); },
  });
  await flush();
  await sandbox.gcRunAction({
    id: "camp-1", action: "pause",
    run: () => { pauseCalled = true; return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }); },
  });
  await flush();

  assert.equal(publishCalled, true);
  assert.equal(pauseCalled, true, "pause on the same campaign must not be blocked by an in-flight publish");
  resolvePublish({ ok: true, status: 200, d: { status: "ok" } });
});

test("E: the same action on a different campaign is NOT blocked", async () => {
  const { sandbox } = loadAction();
  let calledFor = [];
  let resolveFirst;
  sandbox.gcRunAction({
    id: "camp-1", action: "archive",
    run: () => { calledFor.push("camp-1"); return new Promise((r) => { resolveFirst = r; }); },
  });
  await flush();
  await sandbox.gcRunAction({
    id: "camp-2", action: "archive",
    run: () => { calledFor.push("camp-2"); return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }); },
  });
  await flush();

  assert.deepEqual(calledFor.sort(), ["camp-1", "camp-2"]);
  resolveFirst({ ok: true, status: 200, d: { status: "ok" } });
});

test("E: after a run completes (success or failure), the same key can run again", async () => {
  const { sandbox } = loadAction();
  let n = 0;
  const run = () => { n++; return Promise.resolve({ ok: true, status: 200, d: { status: "ok" } }); };
  await sandbox.gcRunAction({ id: "camp-1", action: "publish", run });
  await flush();
  await sandbox.gcRunAction({ id: "camp-1", action: "publish", run });
  await flush();
  assert.equal(n, 2);
});

// ---------------------------------------------------------------------
// F. Button in-flight state (double-click / disabled while running)
// ---------------------------------------------------------------------
test("F: the triggering button is disabled while the request is in flight and re-enabled after", async () => {
  const { sandbox, calls } = loadAction();
  const btn = makeBtn("Publish");
  let resolveRun;
  const p = sandbox.gcRunAction({
    id: "camp-1", action: "publish", button: btn, loadingText: "Publishing...",
    run: () => new Promise((r) => { resolveRun = r; }),
  });
  await flush();
  assert.equal(calls.btnStart.length, 1);
  assert.equal(btn.disabled, true, "button must be disabled while in flight");

  resolveRun({ ok: true, status: 200, d: { status: "ok" } });
  await p;
  await flush();
  assert.equal(calls.btnStop.length, 1);
  assert.equal(btn.disabled, false);
});

// ---------------------------------------------------------------------
// G. gcDefaultRefresh — reusable beyond the list DOM (spec §10)
// ---------------------------------------------------------------------
test("G: refreshes the Campaigns list when that's the current view", () => {
  const { sandbox, calls } = loadAction({ state: { view: "gcCampaigns", campaignId: null } });
  sandbox.gcDefaultRefresh("camp-1");
  assert.deepEqual(calls.loadGcCampaigns, [true]);
  assert.deepEqual(calls.loadCampaignDetail, []);
});

test("G: refreshes Campaign Detail instead when that's the current view for this exact campaign", () => {
  const { sandbox, calls } = loadAction({ state: { view: "campaignDetail", campaignId: "camp-1" } });
  sandbox.gcDefaultRefresh("camp-1");
  assert.deepEqual(calls.loadCampaignDetail, [true]);
  assert.deepEqual(calls.loadGcCampaigns, []);
});

test("G: does not refresh Campaign Detail for a DIFFERENT campaign than the one on screen", () => {
  const { sandbox, calls } = loadAction({ state: { view: "campaignDetail", campaignId: "camp-2" } });
  sandbox.gcDefaultRefresh("camp-1");
  assert.deepEqual(calls.loadCampaignDetail, []);
  assert.deepEqual(calls.loadGcCampaigns, []);
});

test("B: gcRunAction's success path is skipped when opts.refresh === false (e.g. Duplicate navigates away instead)", async () => {
  const { sandbox, calls } = loadAction();
  await sandbox.gcRunAction({
    id: "camp-1", action: "duplicate", refresh: false,
    run: () => Promise.resolve({ ok: true, status: 201, d: { status: "ok", campaign_id: "camp-1-copy" } }),
  });
  await flush();
  assert.deepEqual(calls.loadGcCampaigns, []);
  assert.deepEqual(calls.loadCampaignDetail, []);
  assert.equal(sandbox.gcOptionsCache.campaigns, null, "cache is still invalidated even when the repaint itself is skipped");
});

// ---------------------------------------------------------------------
// H. gcNextDuplicateId — client-side proposal, backend stays authoritative
// ---------------------------------------------------------------------
test("H: proposes '<id>-copy' when that id is free", () => {
  const { sandbox } = loadAction({ gcKnownCampaignIds: {} });
  assert.equal(sandbox.gcNextDuplicateId("july-tournament"), "july-tournament-copy");
});

test("H: proposes '<id>-copy-2' when '<id>-copy' is already taken (matches the second-duplicate 409 the audit found)", () => {
  const { sandbox } = loadAction({ gcKnownCampaignIds: { "july-tournament-copy": true } });
  assert.equal(sandbox.gcNextDuplicateId("july-tournament"), "july-tournament-copy-2");
});

test("H: keeps incrementing past multiple existing copies", () => {
  const { sandbox } = loadAction({
    gcKnownCampaignIds: { "x-copy": true, "x-copy-2": true, "x-copy-3": true },
  });
  assert.equal(sandbox.gcNextDuplicateId("x"), "x-copy-4");
});

// ---------------------------------------------------------------------
// I. Source-level wiring — proves the actual call sites were switched onto
//    gcRunAction + apiPostJson/apiPutJson, not left on the old apiPost()
//    pattern the audit flagged. A runtime test can't see "which helper a
//    handler used" without this.
// ---------------------------------------------------------------------
function gcActionBranch(action) {
  const marker = 'action === "' + action + '"';
  const start = JS.indexOf(marker, JS.indexOf('document.addEventListener("click"', JS.indexOf("function bindGcCampaigns")));
  assert.notEqual(start, -1, "branch not found for action: " + action);
  // Grab a generous chunk (next branch starts with "else if (action ===" or
  // the closing of the whole listener) — enough to contain the full options
  // object literal passed to gcRunAction.
  const nextBranch = JS.indexOf('else if (action ===', start + marker.length);
  const closeListener = JS.indexOf('    });\n\n    $all("#gc-status-filter button")', start);
  const end = nextBranch !== -1 && nextBranch < closeListener ? nextBranch : closeListener;
  assert.ok(end > start, "could not bound branch chunk for action: " + action);
  return JS.slice(start, end);
}

test("I: Publish/Resume routes through gcRunAction and apiPostJson (never bare apiPost)", () => {
  const chunk = gcActionBranch("publish");
  assert.match(chunk, /gcRunAction\(/);
  assert.match(chunk, /apiPostJson\("\/api\/admin\/gc-campaigns\/"\s*\+\s*id\s*\+\s*"\/publish"/);
  assert.doesNotMatch(chunk, /apiPost\(/, "must not use the throw-on-non-2xx helper that discards the backend's error code");
});

test("I: Pause routes through gcRunAction and apiPostJson", () => {
  const chunk = gcActionBranch("pause");
  assert.match(chunk, /gcRunAction\(/);
  assert.match(chunk, /apiPostJson\("\/api\/admin\/gc-campaigns\/"\s*\+\s*id\s*\+\s*"\/pause"/);
  assert.doesNotMatch(chunk, /apiPost\(/);
});

test("I: Archive routes through gcRunAction, apiPostJson, and confirms with the campaign name", () => {
  const chunk = gcActionBranch("archive");
  assert.match(chunk, /gcRunAction\(/);
  assert.match(chunk, /apiPostJson\("\/api\/admin\/gc-campaigns\/"\s*\+\s*id\s*\+\s*"\/archive"/);
  assert.match(chunk, /confirmMessage/);
  assert.match(chunk, /archiveName/);
  assert.doesNotMatch(chunk, /apiPost\(/);
});

test("I: Duplicate routes through gcRunAction, apiPostJson, and proposes a unique id via gcNextDuplicateId", () => {
  const chunk = gcActionBranch("duplicate");
  assert.match(chunk, /gcRunAction\(/);
  assert.match(chunk, /gcNextDuplicateId\(id\)/);
  assert.match(chunk, /apiPostJson\("\/api\/admin\/gc-campaigns\/"\s*\+\s*id\s*\+\s*"\/duplicate",\s*\{\s*campaign_id:\s*proposedId\s*\}/);
  assert.match(chunk, /renderCampaignDetail\(d\.campaign_id\)/, "must open Campaign Detail for the backend-returned new copy, never a guessed id");
  assert.doesNotMatch(chunk, /apiPost\(/);
});

test("I: Duplicate's success toast never includes the raw campaign_id template", () => {
  const chunk = gcActionBranch("duplicate");
  assert.doesNotMatch(chunk, /Duplicated as draft/, "the old raw-id toast copy must be gone");
  assert.match(chunk, /dupName/);
});

test("I: Preview is caught (never a bare .then with no .catch) and maps failures to the spec's exact copy", () => {
  const chunk = gcActionBranch("preview");
  assert.match(chunk, /gcRunAction\(/);
  assert.match(chunk, /GC_ACTION_ERROR_MESSAGES\.preview_failed/);
});

test("I: GC_ACTION_ERROR_MESSAGES.preview_failed is exactly the spec copy", () => {
  const { sandbox } = loadAction();
  assert.equal(sandbox.GC_ACTION_ERROR_MESSAGES.preview_failed, "Couldn't load campaign preview. Try again.");
});

test("I: Registration settings save routes through gcRunAction, never a raw '❌ ' + res.d.code toast", () => {
  const btnIdx = JS.indexOf('$("#cr-cfg-save-btn")');
  assert.notEqual(btnIdx, -1);
  const chunk = JS.slice(btnIdx, JS.indexOf("});", JS.indexOf("gcRunAction(", btnIdx)) + 3);
  assert.match(chunk, /gcRunAction\(/);
  assert.match(chunk, /apiPutJson\("\/api\/admin\/gc-campaigns\/"\s*\+\s*campaignId/);
  assert.doesNotMatch(chunk, /res\.d\.code \|\| "update_failed"/, "must no longer surface the raw backend code");
});

// ---------------------------------------------------------------------
// J. Registration error codes from campaign_registration.validate_registration_config
//    are all mapped (spec §9 minimum + the actual codes discovered in
//    campaign_registration.py's _validate_scope_block/validate_registration_config).
// ---------------------------------------------------------------------
test("J: every campaign_registration.py validation code maps to a friendly message", () => {
  const { sandbox } = loadAction();
  [
    "invalid_required_fields", "invalid_audience_scope", "invalid_audience_regions",
    "audience_regions_required", "invalid_shipping_scope", "invalid_shipping_regions",
    "shipping_regions_required", "invalid_reminder_hours", "invalid_base_entries",
    "country_region_required_for_selected_audience",
  ].forEach((code) => {
    const msg = sandbox.gcActionErrorMessage({ d: { code } });
    assert.notEqual(msg, "Couldn't complete this action. Try again.", "missing mapping for " + code);
    assert.doesNotMatch(msg, /_/);
  });
});

// ---------------------------------------------------------------------
// Regression: existing P0.2-P0.6, Mission, delete-modal, Existing Drops,
// and VIEWS/HTML/MODULES suites this PR must not disturb.
// ---------------------------------------------------------------------
test("Existing P0.2-P0.6 / Mission / delete / Existing Drops / VIEWS-sync regression suites still pass", () => {
  execFileSync(process.execPath, [
    "--test",
    "test_admin_dashboard_views_sync.test.js",
    "test_admin_dashboard_p0_2_filters.test.js",
    "test_admin_dashboard_p0_3_id_fields.test.js",
    "test_admin_dashboard_p0_4_campaign_list.test.js",
    "test_admin_dashboard_p0_5a_campaign_detail.test.js",
    "test_admin_dashboard_p0_5b_campaign_detail_edit.test.js",
    "test_admin_dashboard_p0_6_campaign_wizard.test.js",
    "test_admin_mini_app_existing_drops.test.js",
    "test_campaign_centre_delete_ui.test.js",
    "test_mission_admin_ui.test.js",
    "test_mission_admin_qa.test.js",
  ], { cwd: __dirname, stdio: "pipe" });
});
