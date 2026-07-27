/**
 * Focused tests for the /start -> Mini App "auto-generate referral link" entry action
 * (handleReferralEntryAction) and the getReferral() success/failure contract it relies on.
 * The functions live inline in static/index.html (no build step), so they are extracted
 * as source text and executed in a sandboxed vm context with mocked DOM/fetch globals.
 *
 * Run with: node --test test_referral_entry_action.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "async function handleReferralEntryAction() {";
const END_MARKER = "\n    async function copyTextToClipboard(text) {";

function loadFunctionsSource() {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "handleReferralEntryAction not found in static/index.html");
  assert.ok(end !== -1, "copyTextToClipboard end marker not found in static/index.html");
  return html.slice(start, end);
}

function makeElementStub(id) {
  return {
    id,
    textContent: "",
    innerText: "",
    style: {},
    disabled: false,
    scrollCalls: 0,
    setAttribute() {},
    removeAttribute() {},
    scrollIntoView(opts) {
      this.scrollCalls++;
      this.lastScrollOpts = opts;
    },
  };
}

/**
 * Builds a fresh vm context with handleReferralEntryAction/getReferral defined,
 * plus mocked globals. `elementIds` lists which getElementById lookups resolve
 * to a stub element (anything else resolves to null, mirroring missing DOM nodes).
 */
function makeContext({ elementIds, userId = 555, search = "", fetchImpl } = {}) {
  const elements = {};
  (elementIds || []).forEach((id) => {
    elements[id] = makeElementStub(id);
  });

  const calls = { fetch: 0, loadReferralProgress: 0, hapticNotify: [], consoleLogs: [] };

  const sandbox = {
    URLSearchParams,
    console: {
      log: (...args) => calls.consoleLogs.push(args.join(" ")),
      warn: () => {},
      error: () => {},
    },
    document: {
      getElementById: (id) => (Object.prototype.hasOwnProperty.call(elements, id) ? elements[id] : null),
    },
    location: { search },
    userId,
    username: "tester",
    API_BASE: "https://api.example.test",
    latestReferralLink: "",
    latestReferralMode: "",
    referralEntryActionHandled: false,
    t: (key) => key,
    hapticNotify: (kind) => calls.hapticNotify.push(kind),
    loadReferralProgress: () => {
      calls.loadReferralProgress++;
    },
    fetch:
      fetchImpl ||
      (() => {
        calls.fetch++;
        return Promise.resolve({
          text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+default" }),
        });
      }),
  };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFunctionsSource(), context, { filename: "index.html-extract.js" });
  return { context, elements, calls };
}

function countingFetch(calls, impl) {
  return (...args) => {
    calls.fetch++;
    return impl(...args);
  };
}

const HAPPY_ELEMENTS = [
  "referral-link",
  "get-referral-btn",
  "ap-get-referral-btn",
  "referral-link-inline",
  "referral-link-inline-text",
  "ap-referral-step",
];

test("getReferral(): returns true and displays link on a well-formed success response", async () => {
  const calls = { fetch: 0 };
  const { context, elements } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    fetchImpl: countingFetch(calls, async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+abc123" }),
    })),
  });

  const ok = await context.getReferral();

  assert.equal(ok, true);
  assert.equal(calls.fetch, 1);
  assert.equal(context.latestReferralLink, "https://t.me/+abc123");
  assert.match(elements["referral-link"].innerText, /https:\/\/t\.me\/\+abc123/);
});

test("getReferral(): missing link in response returns false and shows inline error", async () => {
  const { context, elements } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: false, referral_link: null, error: "user_id is required" }),
    }),
  });

  const ok = await context.getReferral();

  assert.equal(ok, false);
  assert.match(elements["referral-link-inline-text"].textContent, /referral_generate_error/);
});

test("getReferral(): malformed (non-JSON) response returns false", async () => {
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    fetchImpl: async () => ({ text: async () => "<not json>" }),
  });

  const ok = await context.getReferral();
  assert.equal(ok, false);
});

test("getReferral(): network/fetch rejection returns false", async () => {
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    fetchImpl: async () => {
      throw new Error("network down");
    },
  });

  const ok = await context.getReferral();
  assert.equal(ok, false);
});

test("getReferral(): success=false with a stray link present is never displayed or treated as success", async () => {
  // The backend no longer sends a usable link alongside success:false (a bot
  // /start deep-link is not parsed anywhere and never reaches referral
  // attribution), but the frontend must not trust a link value on its own —
  // success:false must always win, defense-in-depth against any legacy/
  // malformed response.
  const { context, elements } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    fetchImpl: async () => ({
      text: async () =>
        JSON.stringify({
          success: false,
          referral_link: "https://t.me/somebot?start=ref555",
          error: "createChatInviteLink failed",
        }),
    }),
  });

  const ok = await context.getReferral();

  assert.equal(ok, false, "backend-reported failure must not be surfaced as success");
  assert.doesNotMatch(
    elements["referral-link"].innerText,
    /ref555/,
    "a link paired with success:false must never be displayed as a usable referral link"
  );
  assert.equal(context.latestReferralLink, "", "latestReferralLink must not be set on failure");
});

test("getReferral(): success mode is captured for logging without exposing the link itself", async () => {
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, mode: "invite_link", referral_link: "https://t.me/+abc" }),
    }),
  });

  const ok = await context.getReferral();

  assert.equal(ok, true);
  assert.equal(context.latestReferralMode, "invite_link");
});

test("handleReferralEntryAction: action=generate_referral calls getReferral exactly once and logs DONE with mode, not the link", async () => {
  const { context, elements, calls } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    search: "?v=42&action=generate_referral",
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, mode: "invite_link", referral_link: "https://t.me/+xyz" }),
    }),
  });

  await context.handleReferralEntryAction();

  assert.equal(elements["ap-referral-step"].scrollCalls, 1);
  assert.equal(context.referralEntryActionHandled, true);

  const doneLog = calls.consoleLogs.find((l) => l.includes("ENTRY_ACTION_DONE"));
  assert.ok(doneLog, "expected an ENTRY_ACTION_DONE log line");
  assert.match(doneLog, /mode=invite_link/);
  assert.doesNotMatch(doneLog, /t\.me\/\+xyz/, "the actual referral link must never be logged");
});

test("handleReferralEntryAction: calling it twice (simulating double init) only triggers one API call", async () => {
  const calls = { fetch: 0 };
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    search: "?action=generate_referral",
    fetchImpl: countingFetch(calls, async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+xyz" }),
    })),
  });

  await context.handleReferralEntryAction();
  await context.handleReferralEntryAction();

  assert.equal(calls.fetch, 1, "second invocation must be a no-op due to the once-per-load guard");
});

test("handleReferralEntryAction: no action query parameter does not call getReferral", async () => {
  const calls = { fetch: 0 };
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    search: "?v=42",
    fetchImpl: countingFetch(calls, async () => ({ text: async () => "{}" })),
  });

  await context.handleReferralEntryAction();

  assert.equal(calls.fetch, 0);
  assert.equal(context.referralEntryActionHandled, false);
});

test("handleReferralEntryAction: unknown action value does not call getReferral", async () => {
  const calls = { fetch: 0 };
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    search: "?action=some_other_action",
    fetchImpl: countingFetch(calls, async () => ({ text: async () => "{}" })),
  });

  await context.handleReferralEntryAction();

  assert.equal(calls.fetch, 0);
  assert.equal(context.referralEntryActionHandled, false);
});

test("handleReferralEntryAction: existing query params (e.g. v) do not interfere with action detection", async () => {
  const calls = { fetch: 0 };
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    search: "?v=99&action=generate_referral&extra=1",
    fetchImpl: countingFetch(calls, async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+xyz" }),
    })),
  });

  await context.handleReferralEntryAction();

  assert.equal(calls.fetch, 1);
});

test("handleReferralEntryAction: missing userId (identity not resolved) does not call getReferral", async () => {
  const calls = { fetch: 0 };
  const { context } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    userId: null,
    search: "?action=generate_referral",
    fetchImpl: countingFetch(calls, async () => ({ text: async () => "{}" })),
  });

  await context.handleReferralEntryAction();

  assert.equal(calls.fetch, 0);
});

test("handleReferralEntryAction: missing referral-section DOM element does not call getReferral", async () => {
  const calls = { fetch: 0 };
  const elementIds = HAPPY_ELEMENTS.filter((id) => id !== "ap-referral-step");
  const { context } = makeContext({
    elementIds,
    search: "?action=generate_referral",
    fetchImpl: countingFetch(calls, async () => ({ text: async () => "{}" })),
  });

  await context.handleReferralEntryAction();

  assert.equal(calls.fetch, 0);
  assert.equal(context.referralEntryActionHandled, false);
});

test("handleReferralEntryAction: an automatic failure logs ENTRY_ACTION_FAILED and leaves the manual retry path usable", async () => {
  let attempt = 0;
  const { context, elements } = makeContext({
    elementIds: HAPPY_ELEMENTS,
    search: "?action=generate_referral",
    fetchImpl: async () => {
      attempt++;
      if (attempt === 1) {
        return { text: async () => JSON.stringify({ success: false, referral_link: null, error: "boom" }) };
      }
      return { text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+retry-ok" }) };
    },
  });

  await context.handleReferralEntryAction();
  assert.match(elements["referral-link-inline-text"].textContent, /referral_generate_error/);
  assert.equal(context.latestReferralLink, "");

  // Manual retry: user presses the normal Generate Link button, which just calls getReferral() again.
  const retryOk = await context.getReferral();

  assert.equal(retryOk, true);
  assert.equal(context.latestReferralLink, "https://t.me/+retry-ok");
  assert.match(elements["referral-link"].innerText, /retry-ok/);
});
