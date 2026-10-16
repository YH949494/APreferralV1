/**
 * Tests for the Mini App's rejoin-buffer claim-block handling
 * (claimErrorToUi / renderClaimError / recheckAfterRejoinBuffer) in
 * static/index.html. These functions turn a blocked
 * `rejoin_buffer_active` claim response into an explicit "Please stay
 * subscribed" card with an HH:MM:SS countdown, instead of the generic
 * "Temporarily unavailable" message.
 *
 * The functions live inline in static/index.html (no build step), so they
 * are extracted as source text and executed in a sandboxed vm context with
 * mocked DOM/fetch/timer globals.
 *
 * Run with: node --test test_rejoin_buffer_claim_ui.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "let claimErrorCountdownTimer = null;";
const END_MARKER = "\n    async function loadVouchers() {";

function loadFunctionsSource() {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "claimErrorCountdownTimer declaration not found in static/index.html");
  assert.ok(end !== -1, "loadVouchers end marker not found in static/index.html");
  return html.slice(start, end);
}

function makeNode() {
  const node = {
    style: {},
    className: "",
    textContent: "",
    innerHTML: "",
    children: [],
    attrs: {},
    listeners: {},
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    addEventListener(type, handler) {
      this.listeners[type] = handler;
    },
    setAttribute(name, value) {
      this.attrs[name] = value;
    },
    removeAttribute(name) {
      delete this.attrs[name];
    },
    get disabled() {
      return Object.prototype.hasOwnProperty.call(this.attrs, "disabled");
    },
  };
  return node;
}

function buildSandbox({ fetchImpl } = {}) {
  const container = makeNode();
  container.style.display = "none";

  const intervals = new Map();
  let nextIntervalId = 1;

  const translations = {
    retry: "Retry",
    retry_available_in: (vars) => `Retry available in ${vars.n}s`,
    you_can_retry: "You can retry now",
  };

  // Countdown must be derived from wall-clock time (Date.now() vs. the
  // server-supplied deadline), not from counting interval callbacks, so it
  // stays correct even if the Mini App is backgrounded/throttled. Give tests
  // a controllable clock instead of waiting on real timers.
  const clock = { now: Date.now() };
  class ControlledDate extends Date {
    constructor(...args) {
      if (args.length === 0) {
        super(clock.now);
      } else {
        super(...args);
      }
    }
    static now() {
      return clock.now;
    }
  }

  const sandbox = {
    Date: ControlledDate,
    document: {
      getElementById(id) {
        if (id === "claim-error") return container;
        return null;
      },
      createElement() {
        return makeNode();
      },
      contains() {
        return true;
      },
      addEventListener() {},
      body: { contains: () => true },
    },
    t(key, vars) {
      const entry = translations[key];
      if (typeof entry === "function") return entry(vars || {});
      return entry || key;
    },
    hapticNotify() {},
    API_V2: "/api/v2",
    tg: {},
    getLatestInitData() {
      return "init-data";
    },
    v2Fetch: fetchImpl || (async () => { throw new Error("v2Fetch not mocked"); }),
    console,
    setInterval(fn, ms) {
      const id = nextIntervalId++;
      intervals.set(id, { fn, ms, cleared: false });
      return id;
    },
    clearInterval(id) {
      const entry = intervals.get(id);
      if (entry) entry.cleared = true;
    },
  };

  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);

  return { sandbox, container, intervals, clock };
}

function run(sandbox, extraSource) {
  const src = loadFunctionsSource() + (extraSource || "");
  vm.runInContext(src, sandbox);
}

function activeIntervalCount(intervals) {
  let n = 0;
  for (const entry of intervals.values()) {
    if (!entry.cleared) n += 1;
  }
  return n;
}

// ---------------------------------------------------------------------
// claimErrorToUi: rejoin_buffer_active recognition
// ---------------------------------------------------------------------

test("claimErrorToUi recognises rejoin_buffer_active via `code`", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi(
    { code: "rejoin_buffer_active", retry_after_sec: 5400, buffer_until: "2026-07-29T18:00:00Z" },
    403
  );
  assert.equal(uiModel.title, "Please stay subscribed");
  assert.equal(
    uiModel.message,
    "You recently rejoined the official channel. You can claim after the waiting period."
  );
  assert.equal(uiModel.isRejoinBuffer, true);
  assert.equal(uiModel.retryAfterSec, 5400);
});

test("claimErrorToUi recognises rejoin_buffer_active via `reason` only (no code)", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi(
    { reason: "rejoin_buffer_active", retry_after_sec: 120 },
    403
  );
  assert.equal(uiModel.isRejoinBuffer, true);
  assert.equal(uiModel.title, "Please stay subscribed");
});

test("claimErrorToUi recognises rejoin_buffer_active via `code` only (no reason)", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active" }, 403);
  assert.equal(uiModel.isRejoinBuffer, true);
  assert.equal(uiModel.retryAfterSec, null);
});

test("generic/unknown errors still map to the generic temporarily-unavailable message", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi({ code: "server_error" }, 500);
  assert.equal(uiModel.isRejoinBuffer, undefined);
  assert.notEqual(uiModel.title, "Please stay subscribed");
});

test("sold_out, not_subscribed, and rate_limited keep their existing (non-generic) behaviour", () => {
  const { sandbox } = buildSandbox();
  run(sandbox);

  const soldOut = sandbox.claimErrorToUi({ code: "sold_out" }, 410);
  assert.equal(soldOut.hideRetry, true);

  const notSubscribed = sandbox.claimErrorToUi({ code: "not_subscribed" }, 403);
  assert.equal(notSubscribed.ctaAction, "join_channel");

  const rateLimited = sandbox.claimErrorToUi({ code: "rate_limited", retry_after_sec: 3 }, 429);
  assert.equal(rateLimited.retryAfterSec, 3);
  assert.notEqual(rateLimited.title, "Please stay subscribed");
});

// ---------------------------------------------------------------------
// renderClaimError: countdown behaviour
// ---------------------------------------------------------------------

test("renderClaimError shows an HH:MM:SS countdown and disables Retry while retry_after_sec > 0", () => {
  const { sandbox, intervals } = buildSandbox();
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi(
    { code: "rejoin_buffer_active", retry_after_sec: 3661 },
    403
  );
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  const card = sandbox.document.getElementById("claim-error").children[0];
  const actions = card.children.find((c) => c.children && c.children.length >= 2);
  const retryBtn = actions.children.find((c) => c.textContent === "Retry");
  const countdown = actions.children.find((c) => c !== retryBtn && "textContent" in c);

  assert.ok(retryBtn.disabled, "Retry must be disabled while retry_after_sec > 0");
  assert.equal(countdown.textContent, "Try again in 01:01:01");
  assert.equal(activeIntervalCount(intervals), 1, "exactly one countdown timer should be running");
});

// The deadline used to come from the absolute buffer_until compared against
// Date.now(); a device clock ahead of the server then saw an expired deadline
// on every re-render and rechecked in a tight loop (a /vouchers/claim request
// storm — and an out-of-memory crash of this very test file). The server's
// relative retry_after_sec is now authoritative.
test("countdown deadline comes from server-relative retry_after_sec, not the device-clock-compared buffer_until", () => {
  const { sandbox, intervals, clock } = buildSandbox();
  run(sandbox);

  // Device clock is 2h ahead of the server: buffer_until already looks past.
  const bufferUntil = new Date(clock.now - 2 * 3600 * 1000).toISOString();
  const uiModel = sandbox.claimErrorToUi(
    { code: "rejoin_buffer_active", retry_after_sec: 3600, buffer_until: bufferUntil },
    403
  );
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  const card = sandbox.document.getElementById("claim-error").children[0];
  const actions = card.children.find((c) => c.children && c.children.length >= 2);
  const countdown = actions.children.find((c) => c.textContent && c.textContent.startsWith("Try again"));

  assert.equal(countdown.textContent, "Try again in 01:00:00");
  assert.equal(activeIntervalCount(intervals), 1, "countdown keeps running despite the skewed buffer_until");

  const [, entry] = [...intervals.entries()][0];
  clock.now += 3600 * 1000;
  entry.fn();
  assert.equal(activeIntervalCount(intervals), 0, "countdown ends once retry_after_sec has elapsed");
});

test("countdown expiry re-enables Retry after a passing eligibility recheck", async () => {
  const fetchImpl = async () => ({
    ok: true,
    status: 200,
    json: async () => ({ status: "ok", check_only: true, subscribed: true }),
  });
  const { sandbox, intervals, clock } = buildSandbox({ fetchImpl });
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 6 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  const card = sandbox.document.getElementById("claim-error").children[0];
  const actions = card.children.find((c) => c.children && c.children.length >= 2);
  const retryBtn = actions.children.find((c) => c.textContent === "Retry");

  assert.ok(retryBtn.disabled);

  const [id, entry] = [...intervals.entries()][0];
  clock.now += 5000;
  await entry.fn(); // remaining=1
  clock.now += 1000;
  await entry.fn(); // remaining=0 -> triggers recheck

  assert.equal(activeIntervalCount(intervals), 0, "timer must be cleared once countdown ends");
  assert.equal(sandbox.document.getElementById("claim-error").style.display, "none", "error card cleared once eligible");
});

test("countdown survives a stalled/throttled timer by deriving remaining from the deadline, not tick count", async () => {
  const fetchImpl = async () => ({
    ok: true,
    status: 200,
    json: async () => ({ status: "ok", check_only: true, subscribed: true }),
  });
  const { sandbox, intervals, clock } = buildSandbox({ fetchImpl });
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 5 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  // Simulate the Mini App being backgrounded: the interval callback only
  // fires once, well after the deadline has actually passed.
  const [, entry] = [...intervals.entries()][0];
  clock.now += 30000;
  await entry.fn();

  assert.equal(activeIntervalCount(intervals), 0, "a single late tick past the deadline must still resolve the countdown");
});

test("countdown expiry re-renders the block when the rejoin buffer is still active", async () => {
  const fetchImpl = async () => ({
    ok: false,
    status: 403,
    json: async () => ({
      status: "blocked",
      code: "rejoin_buffer_active",
      reason: "rejoin_buffer_active",
      retry_after_sec: 30,
      buffer_until: "2026-07-29T19:00:00Z",
    }),
  });
  const { sandbox, intervals, clock } = buildSandbox({ fetchImpl });
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 1 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  const [, firstEntry] = [...intervals.entries()][0];
  clock.now += 1000;
  await firstEntry.fn(); // remaining=0 -> recheck -> still blocked -> re-render

  // Exactly one live interval after the re-render (the old one cleared, one new one started).
  assert.equal(activeIntervalCount(intervals), 1, "no duplicate timers after re-render");

  const card = sandbox.document.getElementById("claim-error").children[0];
  const title = card.children.find((c) => c.textContent === "Please stay subscribed");
  assert.ok(title, "re-rendered card should still show the rejoin-buffer title");
});

test("renderClaimError never creates more than one active countdown timer across repeated calls", () => {
  const { sandbox, intervals } = buildSandbox();
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 100 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });

  assert.equal(activeIntervalCount(intervals), 1, "re-rendering must clear the previous timer, not stack new ones");
});

test("clearClaimError stops any running rejoin-buffer countdown timer", () => {
  const { sandbox, intervals } = buildSandbox();
  run(sandbox);

  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 100 }, 403);
  sandbox.renderClaimError(uiModel, { dropId: "drop1" });
  assert.equal(activeIntervalCount(intervals), 1);

  sandbox.clearClaimError();
  assert.equal(activeIntervalCount(intervals), 0);
});

// ---------------------------------------------------------------------
// Request-storm guards: clock skew, minimum delay, bounded auto-rechecks,
// one claim request in flight
// ---------------------------------------------------------------------

function stillActiveFetch(calls, overrides = {}) {
  return async () => {
    calls.push(1);
    return {
      ok: false,
      status: 403,
      json: async () => ({
        status: "blocked",
        code: "rejoin_buffer_active",
        reason: "rejoin_buffer_active",
        retry_after_sec: 1,
        // Already in the past on the device clock (device ahead of server).
        buffer_until: "2000-01-01T00:00:00Z",
        ...overrides,
      }),
    };
  };
}

async function driveTicks(intervals, clock, rounds, stepMs) {
  for (let i = 0; i < rounds; i += 1) {
    clock.now += stepMs;
    const live = [...intervals.values()].filter((e) => !e.cleared);
    for (const entry of live) await entry.fn();
  }
}

test("a re-render after a still-active recheck never rechecks immediately (no tight loop)", async () => {
  const calls = [];
  const { sandbox, intervals, clock } = buildSandbox({ fetchImpl: stillActiveFetch(calls) });
  run(sandbox);

  sandbox.renderClaimError(
    sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 1 }, 403),
    { dropId: "drop1" }
  );
  // Floor: nothing fires before REJOIN_RECHECK_MIN_DELAY_SEC (5s).
  await driveTicks(intervals, clock, 4, 1000);
  assert.equal(calls.length, 0);
  await driveTicks(intervals, clock, 1, 1000);
  assert.equal(calls.length, 1, "exactly one recheck once the floored deadline passes");
  // The re-render's own immediate tick must not fire a second request.
  assert.equal(activeIntervalCount(intervals), 1);
});

test("automatic rechecks are capped; afterwards Retry is re-enabled for a manual tap", async () => {
  const calls = [];
  const { sandbox, intervals, clock, container } = buildSandbox({ fetchImpl: stillActiveFetch(calls) });
  run(sandbox);

  sandbox.renderClaimError(
    sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 1 }, 403),
    { dropId: "drop1" }
  );
  // Ten minutes of simulated time.
  await driveTicks(intervals, clock, 600, 1000);

  assert.equal(calls.length, 3, "no more than REJOIN_MAX_AUTO_RECHECKS automatic requests");
  assert.equal(activeIntervalCount(intervals), 0, "no timer left running after the cap");
  // The fake DOM keeps every appended card (innerHTML="" does not clear
  // children), so read the latest one.
  const latestCard = container.children[container.children.length - 1];
  const actions = latestCard.children.find((c) => c.children && c.children.length >= 2);
  const retryBtn = actions.children.find((c) => c.textContent === "Retry");
  assert.equal(retryBtn.disabled, false, "Retry is enabled for a manual attempt");
});

test("a fresh (user-initiated) block render resets the auto-recheck budget", async () => {
  const calls = [];
  const { sandbox, intervals, clock } = buildSandbox({ fetchImpl: stillActiveFetch(calls) });
  run(sandbox);
  const uiModel = sandbox.claimErrorToUi({ code: "rejoin_buffer_active", retry_after_sec: 1 }, 403);

  sandbox.renderClaimError(uiModel, { dropId: "drop1" });
  await driveTicks(intervals, clock, 600, 1000);
  assert.equal(calls.length, 3);

  sandbox.renderClaimError(uiModel, { dropId: "drop1" }); // user tapped Claim again
  await driveTicks(intervals, clock, 600, 1000);
  assert.equal(calls.length, 6);
});

test("only one recheck request is in flight at a time", async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const calls = [];
  const fetchImpl = async () => {
    calls.push(1);
    await gate;
    return { ok: true, status: 200, json: async () => ({ status: "ok", check_only: true }) };
  };
  const { sandbox } = buildSandbox({ fetchImpl });
  run(sandbox);

  const first = sandbox.recheckAfterRejoinBuffer("drop1", null, null);
  const second = sandbox.recheckAfterRejoinBuffer("drop1", null, null);
  await second;
  assert.equal(calls.length, 1, "overlapping recheck must not issue a second request");
  release();
  await first;
  await sandbox.recheckAfterRejoinBuffer("drop1", null, null);
  assert.equal(calls.length, 2, "the flag is released once the first request settles");
});

test("a recheck never overlaps a user-initiated claim already in flight", async () => {
  const calls = [];
  const { sandbox } = buildSandbox({ fetchImpl: stillActiveFetch(calls) });
  sandbox.window = { __claimInFlight: true };
  run(sandbox);

  await sandbox.recheckAfterRejoinBuffer("drop1", null, null);
  assert.equal(calls.length, 0);
  assert.equal(sandbox.window.__claimInFlight, true, "the user's claim keeps ownership of the flag");

  sandbox.window.__claimInFlight = false;
  await sandbox.recheckAfterRejoinBuffer("drop1", null, null);
  assert.equal(calls.length, 1);
  assert.equal(sandbox.window.__claimInFlight, false, "recheck releases the shared flag when done");
});
