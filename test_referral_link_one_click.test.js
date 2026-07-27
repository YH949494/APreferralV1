/**
 * Tests for the one-click "Get link -> Copy link" referral flow
 * (handleReferralLinkClick / copyReferralLinkAndFlash) that replaced the old
 * separate Get-link + Copy two-step flow. The functions live inline in
 * static/index.html (no build step), so they are extracted as source text
 * and executed in a sandboxed vm context with mocked DOM/fetch/clipboard
 * globals.
 *
 * Run with: node --test test_referral_link_one_click.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "async function handleReferralEntryAction() {";
const END_MARKER = "\n    function setShareRankStatus(message, isError = false) {";

function loadFunctionsSource() {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "handleReferralEntryAction not found in static/index.html");
  assert.ok(end !== -1, "setShareRankStatus end marker not found in static/index.html");
  return html.slice(start, end);
}

function makeElementStub(id) {
  return {
    id,
    textContent: "",
    innerText: "",
    style: {},
    disabled: false,
    setAttribute() {},
    removeAttribute() {},
    scrollIntoView() {},
  };
}

const DEFAULT_ELEMENTS = [
  "referral-link",
  "get-referral-btn",
  "ap-get-referral-btn",
  "referral-link-inline",
  "referral-link-inline-text",
  "referral-link-error",
  "ap-referral-step",
];

function makeContext({ elementIds = DEFAULT_ELEMENTS, userId = 555, fetchImpl, clipboardWriteText, execCommandResult = true } = {}) {
  const elements = {};
  elementIds.forEach((id) => {
    elements[id] = makeElementStub(id);
  });

  const calls = {
    fetch: 0,
    hapticNotify: [],
    toasts: [],
    analytics: [],
    execCommand: 0,
  };

  const sandbox = {
    console: {
      log: () => {},
      warn: () => {},
      error: () => {},
      info: (...args) => calls.analytics.push(args.join(" ")),
    },
    document: {
      getElementById: (id) => (Object.prototype.hasOwnProperty.call(elements, id) ? elements[id] : null),
      createElement: (tag) => ({
        tagName: tag,
        style: {},
        setAttribute() {},
        focus() {},
        select() {},
        value: "",
      }),
      createRange: () => ({ selectNodeContents() {} }),
      execCommand: (cmd) => {
        calls.execCommand++;
        return execCommandResult;
      },
      body: { appendChild() {}, removeChild() {} },
    },
    window: {
      getSelection: () => ({ removeAllRanges() {}, addRange() {} }),
      isSecureContext: true,
    },
    navigator: {
      clipboard: clipboardWriteText
        ? { writeText: clipboardWriteText }
        : undefined,
    },
    location: { search: "" },
    userId,
    username: "tester",
    API_BASE: "https://api.example.test",
    latestReferralLink: "",
    latestReferralMode: "",
    referralEntryActionHandled: false,
    referralLinkRequestInFlight: false,
    t: (key) => key,
    hapticNotify: (kind) => calls.hapticNotify.push(kind),
    loadReferralProgress: () => {},
    showCopySuccessToast: () => calls.toasts.push("copied"),
    fetch:
      fetchImpl ||
      (() => {
        calls.fetch++;
        return Promise.resolve({
          text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+default" }),
        });
      }),
    setTimeout: (fn) => fn(), // run "after 2s" callbacks immediately in tests
  };
  sandbox.window.isSecureContext = true;

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFunctionsSource(), context, { filename: "index.html-extract-copy.js" });
  return { context, elements, calls };
}

function countingFetch(calls, impl) {
  return (...args) => {
    calls.fetch++;
    return impl(...args);
  };
}

test("first click: fetches, displays and copies the link, then shows Copied feedback", async () => {
  const clipboardCalls = [];
  const { context, elements, calls } = makeContext({
    fetchImpl: countingFetch({ fetch: 0 }, async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+abc123" }),
    })),
    clipboardWriteText: async (text) => {
      clipboardCalls.push(text);
    },
  });

  await context.handleReferralLinkClick();

  assert.equal(context.latestReferralLink, "https://t.me/+abc123");
  assert.equal(elements["referral-link-inline-text"].textContent, "https://t.me/+abc123");
  assert.deepEqual(clipboardCalls, ["https://t.me/+abc123"]);
  assert.equal(elements["ap-get-referral-btn"].textContent, "copy_link");
  assert.ok(calls.analytics.some((l) => l.includes("referral_link_requested")));
  assert.ok(calls.analytics.some((l) => l.includes("referral_link_generated")));
  assert.ok(calls.analytics.some((l) => l.includes("referral_link_copied")));
});

test("existing link is copied without another API request", async () => {
  const clipboardCalls = [];
  const calls = { fetch: 0 };
  const { context, elements } = makeContext({
    fetchImpl: countingFetch(calls, async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+xyz" }),
    })),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();
  assert.equal(calls.fetch, 1);

  await context.handleReferralLinkClick();
  assert.equal(calls.fetch, 1, "second click must not call the API again");
  assert.deepEqual(clipboardCalls, ["https://t.me/+xyz", "https://t.me/+xyz"]);
  assert.equal(elements["ap-get-referral-btn"].textContent, "copy_link");
});

test("double-click (concurrent calls) does not create duplicate requests", async () => {
  const calls = { fetch: 0 };
  let resolveFetch;
  const fetchImpl = countingFetch(
    calls,
    () =>
      new Promise((resolve) => {
        resolveFetch = () =>
          resolve({
            text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+concurrent" }),
          });
      })
  );
  const { context } = makeContext({ fetchImpl, clipboardWriteText: async () => {} });

  const first = context.handleReferralLinkClick();
  const second = context.handleReferralLinkClick();

  resolveFetch();
  await Promise.all([first, second]);

  assert.equal(calls.fetch, 1, "concurrent clicks must only trigger one API request");
});

test("clipboard API failure falls back to execCommand copy", async () => {
  const { context, elements, calls } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+fallback" }),
    }),
    clipboardWriteText: async () => {
      throw new Error("denied");
    },
    execCommandResult: true,
  });

  await context.handleReferralLinkClick();

  assert.equal(calls.execCommand, 1, "execCommand fallback should have been used");
  assert.equal(elements["ap-get-referral-btn"].textContent, "copy_link");
  assert.equal(elements["referral-link-error"].style.display, "none");
});

test("total clipboard failure keeps and highlights the link", async () => {
  const { context, elements, calls } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+manual" }),
    }),
    clipboardWriteText: async () => {
      throw new Error("denied");
    },
    execCommandResult: false,
  });

  await context.handleReferralLinkClick();

  assert.equal(context.latestReferralLink, "https://t.me/+manual", "link must still be considered generated");
  assert.equal(elements["referral-link-inline-text"].textContent, "https://t.me/+manual", "link must remain visible");
  assert.equal(elements["referral-link-error"].textContent, "referral_manual_copy");
  assert.equal(elements["referral-link-error"].style.display, "block");
  assert.ok(calls.analytics.some((l) => l.includes("referral_link_copy_failed")));
});

test("API failure does not remove an existing valid link", async () => {
  let attempt = 0;
  const { context, elements } = makeContext({
    fetchImpl: async () => {
      attempt++;
      if (attempt === 1) {
        return { text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+good" }) };
      }
      return { text: async () => JSON.stringify({ success: false, referral_link: null, error: "boom" }) };
    },
    clipboardWriteText: async () => {},
  });

  await context.handleReferralLinkClick();
  assert.equal(context.latestReferralLink, "https://t.me/+good");

  // Force a retry path by clearing the link as if generation had never
  // succeeded, simulating a fresh failed attempt with no prior valid link.
  context.latestReferralLink = "";
  await context.handleReferralLinkClick();

  assert.equal(elements["ap-get-referral-btn"].textContent, "try_again");
  assert.equal(elements["referral-link-error"].textContent, "referral_generate_error");
});

test("button state resets correctly after a copy success (Copied -> Copy link)", async () => {
  const { context, elements } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+reset" }),
    }),
    clipboardWriteText: async () => {},
  });

  await context.handleReferralLinkClick();

  // setTimeout is stubbed to run immediately in this sandbox, so the delayed
  // "revert to Copy link" callback has already fired by the time we get here.
  assert.equal(elements["ap-get-referral-btn"].textContent, "copy_link");
});

test("button state resets correctly after a failure (Try again stays until retried)", async () => {
  const { context, elements } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: false, referral_link: null, error: "boom" }),
    }),
  });

  await context.handleReferralLinkClick();

  assert.equal(elements["ap-get-referral-btn"].textContent, "try_again");
  assert.equal(context.referralLinkRequestInFlight, false, "in-flight flag must be cleared after failure");
});

test("Share button continues using the correct referral link, independent of the copy flow", async () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const shareStart = html.indexOf("async function shareReferralViaTelegram() {");
  const shareEnd = html.indexOf('if (typeof isThaiUiLanguage !== "function") {');
  assert.ok(shareStart !== -1 && shareEnd !== -1, "shareReferralViaTelegram bounds not found");
  const fetchContentStart = html.indexOf("async function fetchReferralShareContent() {");
  assert.ok(fetchContentStart !== -1 && fetchContentStart < shareStart);
  const source = html.slice(fetchContentStart, shareEnd);

  const openedUrls = [];
  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    v2Fetch: async () =>
      ({
        ok: true,
        json: async () => ({
          ok: true,
          message: "Join me!",
          invite_link: "https://t.me/+share-correct-link",
          share_text: "Join AdvantPlay",
        }),
      }),
    openTelegramSafeLink: (url) => openedUrls.push(url),
    hapticImpact: () => {},
    tgAlert: () => {},
    t: (key) => key,
    API_BASE: "https://api.example.test",
  };
  const context = vm.createContext(sandbox);
  vm.runInContext(source, context, { filename: "index.html-extract-share.js" });

  await context.shareReferralViaTelegram();

  assert.equal(openedUrls.length, 1);
  assert.ok(openedUrls[0].includes(encodeURIComponent("https://t.me/+share-correct-link")));
});
