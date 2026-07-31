/**
 * Asserts the Mini App's Welcome Journey visibility gate in static/index.html:
 *   - renderWelcomeProgress() only proceeds when both `visible` and
 *     `eligible` are explicitly true (backend is authoritative; the
 *     frontend must not infer visibility from partial/legacy fields).
 *   - syncWelcomeStatusCard() applies the identical gate *before* any
 *     status branch (issued/claimed/in-progress), so status="claimed" or
 *     a userClaimed retained drop can never override visible=false or
 *     eligible=false.
 *   - hideWelcomeProgress() clears stale card content (step label, timeline
 *     squares, countdown, expiry, next-action text) so a hidden card never
 *     flashes a previous poll's progress when shown again.
 *
 * Run with: node --test test_welcome_journey_visibility_gate.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function readIndexHtml() {
  return fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
}

const START_MARKER = "function hideWelcomeProgress(";
const END_MARKER = "async function loadWelcomeProgress(";

function loadFunctionSource() {
  const html = readIndexHtml();
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "hideWelcomeProgress not found in static/index.html");
  assert.ok(end !== -1, "loadWelcomeProgress end marker not found in static/index.html");
  return html.slice(start, end);
}

function makeEl() {
  const el = {
    style: {},
    textContent: "",
    innerHTML: "",
    classList: {
      _set: new Set(),
      add(c) { this._set.add(c); },
      remove(c) { this._set.delete(c); },
      contains(c) { return this._set.has(c); },
    },
    appendChild() {},
  };
  return el;
}

function makeSandbox() {
  const elements = {};
  const ids = [
    "welcome-progress-section", "progress-section", "welcome-progress-body",
    "welcome-status-card-body", "welcome-status-badge", "welcome-status-message",
    "welcome-status-issued", "welcome-step-label", "welcome-progress-squares",
    "welcome-progress-next-action", "welcome-progress-countdown",
    "welcome-progress-expiry", "welcome-channel-reminder", "welcome-checkin-btn",
    "welcome-progress-title",
  ];
  for (const id of ids) elements[id] = makeEl();

  const document = {
    getElementById(id) {
      return Object.prototype.hasOwnProperty.call(elements, id) ? elements[id] : null;
    },
    createElement() {
      return makeEl();
    },
  };

  const calls = { hideWelcomeProgress: 0, showWelcomeStatusCard: [] };

  const sandbox = {
    document,
    t: (key) => key,
    fmtKL: (v) => String(v),
    safeInt: (v, d) => {
      const n = parseInt(v, 10);
      return Number.isFinite(n) ? n : d;
    },
    console,
  };
  vm.createContext(sandbox);
  const src =
    loadFunctionSource() +
    "\nthis.__syncWelcomeStatusCard = syncWelcomeStatusCard;" +
    "\nthis.__setWelcomeProgressData = function (d) { _welcomeProgressData = d; };" +
    "\nthis.__setWelcomeRewardDrop = function (d) { _welcomeRewardDrop = d; };" +
    "\nthis.__wrapCalls = function (hide, show) {" +
    "  const origHide = hideWelcomeProgress;" +
    "  const origShow = showWelcomeStatusCard;" +
    "  hideWelcomeProgress = function (...a) { hide(); return origHide.apply(this, a); };" +
    "  showWelcomeStatusCard = function (...a) { show(a); return origShow.apply(this, a); };" +
    "};";
  vm.runInContext(src, sandbox);

  sandbox.__wrapCalls(
    () => { calls.hideWelcomeProgress += 1; },
    (args) => { calls.showWelcomeStatusCard.push(args); }
  );

  return { sandbox, elements, calls };
}

test("renderWelcomeProgress gates on visible === true && eligible === true", () => {
  const html = readIndexHtml();
  const idx = html.indexOf("function renderWelcomeProgress(");
  assert.ok(idx !== -1, "renderWelcomeProgress not found");
  const body = html.slice(idx, html.indexOf("function loadWelcomeProgress(", idx));

  assert.ok(
    body.includes('data?.visible !== true || data?.eligible !== true'),
    "renderWelcomeProgress must require both visible===true and eligible===true before rendering"
  );
  assert.ok(
    body.includes("hideWelcomeProgress();"),
    "renderWelcomeProgress must call hideWelcomeProgress() on the ineligible/not-visible path"
  );
});

test("hideWelcomeProgress clears stale step/timeline/message content", () => {
  const html = readIndexHtml();
  const idx = html.indexOf("function hideWelcomeProgress(");
  assert.ok(idx !== -1, "hideWelcomeProgress not found");
  const body = html.slice(idx, html.indexOf("function highlightWelcomeRewardCard(", idx));

  assert.ok(body.includes('section.style.display = "none"'), "must hide the section");
  assert.ok(body.includes("welcome-step-label"), "must clear the step label");
  assert.ok(body.includes("welcome-progress-squares"), "must clear the timeline squares");
  assert.ok(body.includes("welcome-progress-next-action"), "must clear the next-action message");
  assert.ok(body.includes("_welcomeProgressData = null"), "must drop stale cached progress data");
});

test("syncWelcomeStatusCard applies the visibility gate before the claimed branch", () => {
  const html = readIndexHtml();
  const idx = html.indexOf("function syncWelcomeStatusCard(");
  assert.ok(idx !== -1, "syncWelcomeStatusCard not found");
  const body = html.slice(idx, html.indexOf("function formatWelcomeCountdown(", idx));
  const gateIdx = body.indexOf("hide_welcome_card === true");
  const claimedIdx = body.indexOf('status === "claimed"');
  assert.ok(gateIdx !== -1, "syncWelcomeStatusCard must contain the visibility gate");
  assert.ok(claimedIdx !== -1, "syncWelcomeStatusCard must still branch on claimed status");
  assert.ok(gateIdx < claimedIdx, "the visibility gate must run before the claimed branch");
});

test("syncWelcomeStatusCard: retained claimed drop with visible=false/eligible=false/hide_welcome_card=true is hidden", () => {
  const { sandbox, elements, calls } = makeSandbox();
  sandbox.__setWelcomeProgressData({
    status: "claimed",
    visible: false,
    eligible: false,
    hide_welcome_card: true,
  });
  sandbox.__setWelcomeRewardDrop({ userClaimed: true, claimedAt: "2026-01-01T00:00:00Z" });

  sandbox.__syncWelcomeStatusCard();

  assert.equal(calls.hideWelcomeProgress, 1, "hideWelcomeProgress() must be called");
  assert.equal(calls.showWelcomeStatusCard.length, 0, "showWelcomeStatusCard() must never be called");
  assert.equal(elements["welcome-progress-section"].style.display, "none");
});

test("syncWelcomeStatusCard: claimed status hidden when only visible=false", () => {
  const { sandbox, calls } = makeSandbox();
  sandbox.__setWelcomeProgressData({ status: "claimed", visible: false, eligible: true, hide_welcome_card: false });
  sandbox.__setWelcomeRewardDrop({ userClaimed: true });
  sandbox.__syncWelcomeStatusCard();
  assert.equal(calls.hideWelcomeProgress, 1);
  assert.equal(calls.showWelcomeStatusCard.length, 0);
});

test("syncWelcomeStatusCard: claimed status hidden when only eligible=false", () => {
  const { sandbox, calls } = makeSandbox();
  sandbox.__setWelcomeProgressData({ status: "claimed", visible: true, eligible: false, hide_welcome_card: false });
  sandbox.__setWelcomeRewardDrop({ userClaimed: true });
  sandbox.__syncWelcomeStatusCard();
  assert.equal(calls.hideWelcomeProgress, 1);
  assert.equal(calls.showWelcomeStatusCard.length, 0);
});

test("syncWelcomeStatusCard: claimed status hidden when only hide_welcome_card=true", () => {
  const { sandbox, calls } = makeSandbox();
  sandbox.__setWelcomeProgressData({ status: "claimed", visible: true, eligible: true, hide_welcome_card: true });
  sandbox.__setWelcomeRewardDrop({ userClaimed: true });
  sandbox.__syncWelcomeStatusCard();
  assert.equal(calls.hideWelcomeProgress, 1);
  assert.equal(calls.showWelcomeStatusCard.length, 0);
});

test("syncWelcomeStatusCard: in-progress eligible payload renders unchanged (gate passes through)", () => {
  const { sandbox, calls, elements } = makeSandbox();
  sandbox.__setWelcomeProgressData({
    status: "in_progress",
    visible: true,
    eligible: true,
    hide_welcome_card: false,
    required_days: 3,
    completed_days: 1,
  });
  sandbox.__syncWelcomeStatusCard();
  assert.equal(calls.hideWelcomeProgress, 0, "hideWelcomeProgress() must not be called");
  assert.equal(calls.showWelcomeStatusCard.length, 0, "showWelcomeStatusCard() must not be called for in-progress");
  assert.equal(elements["welcome-progress-section"].style.display, "block");
  assert.equal(elements["welcome-progress-body"].style.display, "");
});

test("syncWelcomeStatusCard: claimed status explicitly allowed (visible=true, eligible=true, hide_welcome_card=false) still renders claimed card", () => {
  const { sandbox, calls } = makeSandbox();
  sandbox.__setWelcomeProgressData({ status: "claimed", visible: true, eligible: true, hide_welcome_card: false });
  sandbox.__setWelcomeRewardDrop({ userClaimed: true, claimedAt: "2026-01-01T00:00:00Z" });
  sandbox.__syncWelcomeStatusCard();
  assert.equal(calls.hideWelcomeProgress, 0);
  assert.equal(calls.showWelcomeStatusCard.length, 1);
  assert.equal(calls.showWelcomeStatusCard[0][0], "claimed");
  assert.equal(calls.showWelcomeStatusCard[0][1], "2026-01-01T00:00:00Z");
});

test("syncWelcomeStatusCard: production-shaped already-claimed payload from build_welcome_progress_response is hidden", () => {
  // Mirrors vouchers.py build_welcome_progress_response()'s "claimed" payload
  // shape returned by GET /api/welcome-progress/<user_id> (visible=False,
  // eligible=False, hide_welcome_card=True are always set together there).
  const { sandbox, calls } = makeSandbox();
  sandbox.__setWelcomeProgressData({
    visible: false,
    eligible: false,
    status: "claimed",
    required_days: 3,
    completed_days: 3,
    remaining_days: 0,
    progress_pct: 100,
    reward_value: "$1",
    eligible_until: null,
    message: "Your Welcome Voucher has already been claimed.",
    hide_welcome_card: true,
    welcome_pending_reason: "ALREADY_CLAIMED",
    reason_code: "welcome_already_issued",
    can_checkin: false,
    next_checkin_at: null,
    voucher_code: "WELCOME-ABC123",
  });
  sandbox.__setWelcomeRewardDrop({ userClaimed: true, claimedAt: "2026-01-01T00:00:00Z" });

  sandbox.__syncWelcomeStatusCard();

  assert.equal(calls.hideWelcomeProgress, 1);
  assert.equal(calls.showWelcomeStatusCard.length, 0);
});

test("no other caller of showWelcomeStatusCard bypasses syncWelcomeStatusCard's gate", () => {
  const html = readIndexHtml();
  const matches = [...html.matchAll(/showWelcomeStatusCard\(/g)];
  // Expect exactly: 1 function definition + 2 call sites, both inside
  // syncWelcomeStatusCard (already proven to sit after the gate above).
  assert.equal(matches.length, 3, "showWelcomeStatusCard should only be defined once and called from syncWelcomeStatusCard");
});
