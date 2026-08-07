/**
 * Regression coverage for the Welcome Voucher progress-card copy selection
 * in static/index.html (welcomeCheckinProgressCopy / renderWelcomeProgress /
 * showWelcomeStatusCard).
 *
 * Root cause under test: renderWelcomeProgress() had a CHANNEL_NOT_JOINED
 * branch that unconditionally rendered "🎉 Your welcome reward is unlocked."
 * even when the backend's authoritative `unlocked` flag
 * (get_welcome_reward_progress() in vouchers.py) was false because the user
 * had not (re)joined the official channel. The backend was already correct
 * (welcome_pending_reason="CHANNEL_NOT_JOINED" with a "Join the official
 * channel..." message) — this was a frontend-only mapping bug.
 *
 * These tests exercise the real static/index.html source (extracted between
 * the same markers as test_welcome_journey_visibility_gate.test.js) against
 * the real static/i18n.js translation dictionary, so a regression in either
 * file — or a translation-key mismatch — fails here.
 *
 * Run with: node --test test_welcome_progress_status_copy.test.js
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

function readI18n() {
  return fs.readFileSync(path.join(__dirname, "static", "i18n.js"), "utf8");
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
    dataset: {},
    classList: {
      _set: new Set(),
      add(c) { this._set.add(c); },
      remove(c) { this._set.delete(c); },
      contains(c) { return this._set.has(c); },
      toggle() {},
    },
    children: [],
    appendChild(child) { this.children.push(child); },
    addEventListener() {},
  };
  return el;
}

// Builds a real DOM/window stub sufficient to load static/i18n.js verbatim,
// then loads the welcome-progress rendering functions from static/index.html
// into the *same* vm context so `t()` is the real translation function
// (catches translation-key typos/mismatches, not just call-site wiring).
function makeSandbox(lang) {
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
    readyState: "complete",
    head: { appendChild() {} },
    getElementById(id) {
      return Object.prototype.hasOwnProperty.call(elements, id) ? elements[id] : null;
    },
    createElement() {
      return makeEl();
    },
    addEventListener() {},
    querySelectorAll() { return []; },
  };

  const localStorageStore = {};
  const localStorage = {
    getItem(k) { return Object.prototype.hasOwnProperty.call(localStorageStore, k) ? localStorageStore[k] : null; },
    setItem(k, v) { localStorageStore[k] = String(v); },
  };

  const sandbox = {
    document,
    localStorage,
    navigator: { language: "en-US" },
    window: null, // filled below (self-reference)
    CustomEvent: function CustomEvent(name, opts) { this.name = name; this.detail = opts && opts.detail; },
    console,
    fmtKL: (v) => String(v),
    safeInt: (v, d) => {
      const n = parseInt(v, 10);
      return Number.isFinite(n) ? n : d;
    },
  };
  sandbox.window = sandbox;
  sandbox.window.dispatchEvent = () => {};
  vm.createContext(sandbox);

  // Force the requested language before i18n.js's IIFE runs detectLang().
  if (lang) localStorageStore.ap_language = lang;

  vm.runInContext(readI18n(), sandbox, { filename: "static/i18n.js" });
  assert.equal(typeof sandbox.t, "function", "i18n.js must expose window.t");
  if (lang) assert.equal(sandbox.currentLanguage, lang, "sandbox language must match requested lang");

  const src =
    loadFunctionSource() +
    "\nthis.__renderWelcomeProgress = renderWelcomeProgress;" +
    "\nthis.__showWelcomeStatusCard = showWelcomeStatusCard;" +
    "\nthis.__welcomeCheckinProgressCopy = welcomeCheckinProgressCopy;";
  vm.runInContext(src, sandbox, { filename: "static/index.html (extracted)" });

  return { sandbox, elements };
}

function nextAction(elements) {
  return elements["welcome-progress-next-action"].textContent;
}

// ---------------------------------------------------------------------
// Core regression: 1/3 completed, not subscribed, not unlocked must never
// show "unlocked" text.
// ---------------------------------------------------------------------
test("REGRESSION: completed=1/3, channel not joined, unlocked=false -> no 'unlocked' text", () => {
  const { sandbox, elements } = makeSandbox("en");
  sandbox.__renderWelcomeProgress({
    visible: true,
    eligible: true,
    hide_welcome_card: false,
    status: "in_progress",
    required_days: 3,
    completed_days: 1,
    welcome_pending_reason: "CHANNEL_NOT_JOINED",
  });
  const text = nextAction(elements);
  assert.ok(!/unlock/i.test(text) || /to unlock/i.test(text), "must not claim the reward is already unlocked");
  assert.ok(!text.includes("🎉"), "must not show the celebratory unlocked copy");
  assert.equal(text, sandbox.t("welcome_progress_many_left", { completed: 1, remaining: 2 }));
  assert.equal(elements["welcome-channel-reminder"].style.display, "block", "channel reminder must be shown");
});

// ---------------------------------------------------------------------
// 0/1/2/3 completed check-ins (channel already joined -> no pending reason)
// ---------------------------------------------------------------------
for (const completed of [0, 1, 2]) {
  test(`completed=${completed}/3, joined, in progress -> progress copy (not unlocked)`, () => {
    const { sandbox, elements } = makeSandbox("en");
    sandbox.__renderWelcomeProgress({
      visible: true,
      eligible: true,
      hide_welcome_card: false,
      status: "in_progress",
      required_days: 3,
      completed_days: completed,
      welcome_pending_reason: null,
    });
    const text = nextAction(elements);
    assert.ok(!text.includes("🎉"), "must not show unlocked celebration copy while in progress");
    const remaining = 3 - completed;
    const expected = completed === 0
      ? sandbox.t("welcome_progress_zero", { required: 3 })
      : (remaining === 1
        ? sandbox.t("welcome_progress_one_left", { completed })
        : sandbox.t("welcome_progress_many_left", { completed, remaining }));
    assert.equal(text, expected);
  });
}

test("completed=3/3 (status issued) is handled upstream, not by renderWelcomeProgress's default branch", () => {
  const { sandbox, elements } = makeSandbox("en");
  sandbox.__renderWelcomeProgress({
    visible: true,
    eligible: true,
    hide_welcome_card: false,
    status: "issued",
    required_days: 3,
    completed_days: 3,
    welcome_pending_reason: null,
  });
  // Defensive fallback copy only; syncWelcomeStatusCard() intercepts status
  // "issued" before renderWelcomeProgress is ever called in production.
  assert.equal(nextAction(elements), sandbox.t("voucher_temporarily_unavailable"));
});

// ---------------------------------------------------------------------
// Subscribed / unsubscribed combinations at each completion level
// ---------------------------------------------------------------------
test("completed=3/3, channel not joined -> 'check-ins complete, verify channel' (never 'unlocked')", () => {
  const { sandbox, elements } = makeSandbox("en");
  sandbox.__renderWelcomeProgress({
    visible: true,
    eligible: true,
    hide_welcome_card: false,
    status: "in_progress",
    required_days: 3,
    completed_days: 3,
    welcome_pending_reason: "CHANNEL_NOT_JOINED",
  });
  const text = nextAction(elements);
  assert.equal(text, sandbox.t("welcome_checkins_done_verify_channel"));
  assert.ok(!text.includes("🎉"), "must not claim the reward is unlocked while channel isn't verified");
  assert.equal(elements["welcome-channel-reminder"].style.display, "block");
});

test("completed=2/3, channel not joined -> progress copy + channel reminder, not the done-copy", () => {
  const { sandbox, elements } = makeSandbox("en");
  sandbox.__renderWelcomeProgress({
    visible: true,
    eligible: true,
    hide_welcome_card: false,
    status: "in_progress",
    required_days: 3,
    completed_days: 2,
    welcome_pending_reason: "CHANNEL_NOT_JOINED",
  });
  const text = nextAction(elements);
  assert.equal(text, sandbox.t("welcome_progress_one_left", { completed: 2 }));
  assert.notEqual(text, sandbox.t("welcome_checkins_done_verify_channel"));
  assert.equal(elements["welcome-channel-reminder"].style.display, "block");
});

// ---------------------------------------------------------------------
// Reward unlocked but not yet claimed / issued & claimed
// ---------------------------------------------------------------------
test("showWelcomeStatusCard('ready') renders the unlocked message only via the ready mode", () => {
  const { sandbox, elements } = makeSandbox("en");
  sandbox.__showWelcomeStatusCard("ready", null);
  assert.equal(elements["welcome-status-message"].textContent, sandbox.t("welcome_status_message_ready"));
  assert.equal(elements["welcome-status-badge"].textContent, sandbox.t("welcome_status_badge_ready"));
});

test("showWelcomeStatusCard('claimed') renders the claimed message, not the unlocked one", () => {
  const { sandbox, elements } = makeSandbox("en");
  sandbox.__showWelcomeStatusCard("claimed", "2026-01-01T00:00:00Z");
  assert.equal(elements["welcome-status-message"].textContent, sandbox.t("welcome_status_message_claimed"));
  assert.notEqual(elements["welcome-status-message"].textContent, sandbox.t("welcome_status_message_ready"));
  assert.equal(elements["welcome-status-badge"].textContent, sandbox.t("welcome_status_badge_claimed"));
  assert.ok(elements["welcome-status-issued"].textContent.includes("2026-01-01T00:00:00Z"));
});

// ---------------------------------------------------------------------
// Hidden / ineligible / expired states never reach the progress copy path
// ---------------------------------------------------------------------
for (const [label, payload] of Object.entries({
  hidden: { visible: false, eligible: false, hide_welcome_card: true, status: "not_eligible" },
  ineligible: { visible: false, eligible: false, hide_welcome_card: true, status: "not_eligible", welcome_pending_reason: "AUDIENCE_MISMATCH" },
  expired: { visible: false, eligible: false, hide_welcome_card: true, status: "expired" },
})) {
  test(`state=${label}: renderWelcomeProgress hides the card instead of rendering copy`, () => {
    const { sandbox, elements } = makeSandbox("en");
    sandbox.__renderWelcomeProgress({ required_days: 3, completed_days: 1, ...payload });
    assert.equal(elements["welcome-progress-section"].style.display, "none");
    assert.equal(elements["welcome-progress-next-action"].textContent, "");
  });
}

// ---------------------------------------------------------------------
// Localization: Thai UI must not fall back to English "unlocked" text
// ---------------------------------------------------------------------
test("TH locale: CHANNEL_NOT_JOINED at 1/3 renders Thai copy, no English 'unlocked' fallback", () => {
  const { sandbox, elements } = makeSandbox("th");
  sandbox.__renderWelcomeProgress({
    visible: true,
    eligible: true,
    hide_welcome_card: false,
    status: "in_progress",
    required_days: 3,
    completed_days: 1,
    welcome_pending_reason: "CHANNEL_NOT_JOINED",
  });
  const text = nextAction(elements);
  assert.equal(text, sandbox.t("welcome_progress_many_left", { completed: 1, remaining: 2 }));
  assert.ok(/[฀-๿]/.test(text), "expected Thai script in the rendered copy");
  assert.ok(!text.includes("unlocked"), "Thai UI must not fall back to the English 'unlocked' string");
  assert.ok(!text.includes("🎉"), "Thai UI must not show the incorrect celebratory copy either");
});

test("TH locale: 3/3 completed but channel not joined uses the Thai 'verify channel' copy", () => {
  const { sandbox, elements } = makeSandbox("th");
  sandbox.__renderWelcomeProgress({
    visible: true,
    eligible: true,
    hide_welcome_card: false,
    status: "in_progress",
    required_days: 3,
    completed_days: 3,
    welcome_pending_reason: "CHANNEL_NOT_JOINED",
  });
  const text = nextAction(elements);
  assert.equal(text, sandbox.t("welcome_checkins_done_verify_channel"));
  assert.ok(/[฀-๿]/.test(text), "expected Thai script in the rendered copy");
});

test("TH locale: showWelcomeStatusCard('ready') renders the Thai unlocked message, not English", () => {
  const { sandbox, elements } = makeSandbox("th");
  sandbox.__showWelcomeStatusCard("ready", null);
  const text = elements["welcome-status-message"].textContent;
  assert.equal(text, sandbox.t("welcome_status_message_ready"));
  assert.notEqual(text, "Your Welcome Reward has been unlocked.");
  assert.ok(/[฀-๿]/.test(text), "expected Thai script in the rendered copy");
});

test("welcomeCheckinProgressCopy: 0/3, 1/3, 2/3 all produce distinct, non-unlocked copy", () => {
  const { sandbox } = makeSandbox("en");
  const zero = sandbox.__welcomeCheckinProgressCopy(0, 3);
  const one = sandbox.__welcomeCheckinProgressCopy(1, 3);
  const two = sandbox.__welcomeCheckinProgressCopy(2, 3);
  assert.equal(zero, sandbox.t("welcome_progress_zero", { required: 3 }));
  assert.equal(one, sandbox.t("welcome_progress_many_left", { completed: 1, remaining: 2 }));
  assert.equal(two, sandbox.t("welcome_progress_one_left", { completed: 2 }));
  assert.notEqual(zero, one);
  assert.notEqual(one, two);
  for (const s of [zero, one, two]) {
    assert.ok(!s.includes("🎉"), `"${s}" must not use the unlocked-celebration marker`);
  }
});

test("timeline uses 'Check-in N' labels, not 'Day N' (check-ins need not be consecutive days)", () => {
  const html = readIndexHtml();
  const idx = html.indexOf("function renderWelcomeTimeline(");
  assert.ok(idx !== -1, "renderWelcomeTimeline not found");
  const body = html.slice(idx, html.indexOf("function renderWelcomeProgress(", idx));
  assert.ok(!/`Day \$\{day\}`/.test(body), "timeline label must not be the hard-coded 'Day N' string");
  assert.ok(body.includes('t("welcome_checkin_label"'), "timeline label must go through the welcome_checkin_label i18n key");
});
