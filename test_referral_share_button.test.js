/**
 * Tests for the orange "📤 Share" button (shareReferralViaTelegram) fix:
 * Share must reuse the canonical in-memory share package populated by
 * Get Link (latestReferralLink / latestSharePackage) instead of
 * independently re-fetching or re-generating the referral link, and must
 * never show "Could not get your referral link" while a valid link is
 * already visible on screen.
 *
 * The functions live inline in static/index.html (no build step), so they
 * are extracted as source text and executed in a sandboxed vm context with
 * mocked DOM/fetch/Telegram globals, mirroring
 * test_referral_link_one_click.test.js's approach.
 *
 * Run with: node --test test_referral_share_button.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "async function handleReferralEntryAction() {";
const END_MARKER = '\n    if (typeof isThaiUiLanguage !== "function") {';

function loadFunctionsSource() {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "handleReferralEntryAction not found in static/index.html");
  assert.ok(end !== -1, "isThaiUiLanguage end marker not found in static/index.html");
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
  "referral-caption-manual",
  "referral-caption-manual-text",
];

function shareContentFetch(message, extra = {}) {
  return () =>
    Promise.resolve({
      ok: true,
      status: 200,
      json: async () => ({
        ok: true,
        message,
        share_text: extra.share_text || "share text without link",
        invite_link: extra.invite_link || "https://t.me/+default",
        playback_url: null,
        hook_text: null,
      }),
    });
}

function failingShareContentFetch(status = 500, error = "boom") {
  return () =>
    Promise.resolve({
      ok: false,
      status,
      json: async () => ({ ok: false, error }),
    });
}

function makeContext({
  userId = 555,
  v2FetchImpl,
  latestReferralLink = "",
  latestSharePackage = null,
} = {}) {
  const elements = {};
  DEFAULT_ELEMENTS.forEach((id) => {
    elements[id] = makeElementStub(id);
  });

  const calls = {
    v2Fetch: 0,
    hapticNotify: [],
    hapticImpact: [],
    analytics: [],
    alerts: [],
    opened: [],
  };

  const shareBtn = makeElementStub("share-btn");

  const sandbox = {
    console: {
      log: () => {},
      warn: (...args) => calls.analytics.push(["warn", ...args].join(" ")),
      error: () => {},
      info: (...args) => calls.analytics.push(args.join(" ")),
    },
    document: {
      getElementById: (id) => (Object.prototype.hasOwnProperty.call(elements, id) ? elements[id] : null),
      createElement: () => ({ style: {}, setAttribute() {}, focus() {}, select() {}, value: "" }),
      createRange: () => ({ selectNodeContents() {} }),
      body: { appendChild() {}, removeChild() {} },
    },
    window: { getSelection: () => ({ removeAllRanges() {}, addRange() {} }), isSecureContext: true },
    navigator: { clipboard: { writeText: async () => {} } },
    location: { search: "" },
    userId,
    username: "tester",
    API_BASE: "https://api.example.test",
    latestReferralLink,
    latestReferralMode: "",
    latestSharePackage,
    referralLinkRequestInFlight: false,
    shareRequestInFlight: false,
    shareContentRequestInFlight: null,
    referralEntryActionHandled: false,
    t: (key) => key,
    hapticNotify: (kind) => calls.hapticNotify.push(kind),
    hapticImpact: (kind) => calls.hapticImpact.push(kind),
    tgAlert: (msg) => calls.alerts.push(msg),
    loadReferralProgress: () => {},
    showCopySuccessToast: () => {},
    openTelegramSafeLink: (url) => {
      calls.opened.push(url);
      return true;
    },
    fetch: () => Promise.reject(new Error("getReferral's /api/referral fetch should not be used by Share tests")),
    v2Fetch: (...args) => {
      calls.v2Fetch++;
      return (v2FetchImpl || shareContentFetch("default"))(...args);
    },
    setTimeout: (fn) => fn(),
  };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFunctionsSource(), context, { filename: "index.html-extract-share.js" });
  return { context, elements, calls, shareBtn };
}

test("1. Get Link succeeds -> Share reuses cached package, zero additional share-content requests", async () => {
  const { context, calls, shareBtn } = makeContext({
    latestReferralLink: "https://t.me/+cached",
    latestSharePackage: {
      referral_link: "https://t.me/+cached",
      caption: "cached caption\nhttps://t.me/+cached",
      share_text: "cached caption body",
      share_url: "",
    },
    v2FetchImpl: () => {
      throw new Error("share-content must not be fetched on a cache hit");
    },
  });

  await context.shareReferralViaTelegram(shareBtn);

  assert.equal(calls.v2Fetch, 0, "no share-content fetch on cache hit");
  assert.equal(calls.opened.length, 1);
  assert.ok(calls.opened[0].includes(encodeURIComponent("https://t.me/+cached")));
  assert.ok(calls.analytics.some((l) => l.includes("[REFERRAL_SHARE][CACHE_HIT]")));
  assert.equal(calls.alerts.length, 0, "no error alert when link is already cached");
});

test("2. share-content API fails while cached link exists -> fallback share still opens, link appears once", async () => {
  const { context, calls, shareBtn } = makeContext({
    latestReferralLink: "https://t.me/+haslink",
    latestSharePackage: null, // link known, but no cached share content yet
    v2FetchImpl: failingShareContentFetch(500, "invite_link_failed"),
  });

  await context.shareReferralViaTelegram(shareBtn);

  assert.equal(calls.v2Fetch, 1, "share-content is attempted once when content is genuinely missing");
  assert.equal(calls.alerts.length, 0, "a share-content failure is never shown as a missing-link error");
  assert.equal(calls.opened.length, 1, "fallback share still opens");
  const openedUrl = calls.opened[0];
  const linkOccurrences = openedUrl.split(encodeURIComponent("https://t.me/+haslink")).length - 1;
  assert.equal(linkOccurrences, 1, "the link must appear exactly once in the final share payload");
  assert.ok(calls.analytics.some((l) => l.includes("[REFERRAL_SHARE][FETCH_FAIL]")));
  assert.ok(calls.analytics.some((l) => l.includes("[REFERRAL_SHARE][FALLBACK]")));
});

test("3. no cached link -> Share may fetch/generate once, then opens", async () => {
  const { context, calls, shareBtn } = makeContext({
    latestReferralLink: "",
    latestSharePackage: null,
    v2FetchImpl: shareContentFetch("caption\nhttps://t.me/+fresh", { invite_link: "https://t.me/+fresh" }),
  });

  await context.shareReferralViaTelegram(shareBtn);

  assert.equal(calls.v2Fetch, 1);
  assert.equal(context.latestReferralLink, "https://t.me/+fresh");
  assert.equal(calls.opened.length, 1);
  assert.ok(calls.opened[0].includes(encodeURIComponent("https://t.me/+fresh")));
  assert.equal(calls.alerts.length, 0);
});

test("4. link appears exactly once in the final Telegram share payload (cache-hit path)", async () => {
  const { context, calls, shareBtn } = makeContext({
    latestReferralLink: "https://t.me/+once",
    latestSharePackage: {
      referral_link: "https://t.me/+once",
      caption: "cached caption\nhttps://t.me/+once",
      share_text: "cached body text, no link here",
      share_url: "",
    },
  });

  await context.shareReferralViaTelegram(shareBtn);

  const openedUrl = calls.opened[0];
  const occurrences = openedUrl.split(encodeURIComponent("https://t.me/+once")).length - 1;
  assert.equal(occurrences, 1);
});

test("5. double click triggers only one share operation", async () => {
  let resolveFetch;
  const pending = new Promise((resolve) => {
    resolveFetch = resolve;
  });
  const { context, calls, shareBtn } = makeContext({
    latestReferralLink: "",
    latestSharePackage: null,
    v2FetchImpl: () =>
      pending.then(() => ({
        ok: true,
        status: 200,
        json: async () => ({
          ok: true,
          message: "caption\nhttps://t.me/+race",
          share_text: "body",
          invite_link: "https://t.me/+race",
        }),
      })),
  });

  const first = context.shareReferralViaTelegram(shareBtn);
  const second = context.shareReferralViaTelegram(shareBtn); // fired before the first resolves
  resolveFetch();
  await Promise.all([first, second]);

  assert.equal(calls.v2Fetch, 1, "only one share-content request across both clicks");
  assert.equal(calls.opened.length, 1, "only one Telegram share window opened");
});

test("6. real missing-link failure (no cache, fetch fails) still shows the correct error", async () => {
  const { context, calls, shareBtn } = makeContext({
    latestReferralLink: "",
    latestSharePackage: null,
    v2FetchImpl: failingShareContentFetch(500, "invite_link_failed"),
  });

  await context.shareReferralViaTelegram(shareBtn);

  assert.equal(calls.alerts.length, 1);
  assert.equal(calls.alerts[0], "could_not_get_link");
  assert.equal(calls.opened.length, 0, "no share window opens on a genuine missing-link failure");
});

test("7. share button is re-enabled and label restored after success and after failure", async () => {
  const successCtx = makeContext({
    latestReferralLink: "https://t.me/+ok",
    latestSharePackage: { referral_link: "https://t.me/+ok", caption: "c\nhttps://t.me/+ok", share_text: "c" },
  });
  successCtx.shareBtn.textContent = "📤 Share";
  await successCtx.context.shareReferralViaTelegram(successCtx.shareBtn);
  assert.equal(successCtx.shareBtn.disabled, false);
  assert.equal(successCtx.shareBtn.textContent, "📤 Share");

  const failCtx = makeContext({
    latestReferralLink: "",
    latestSharePackage: null,
    v2FetchImpl: failingShareContentFetch(500, "invite_link_failed"),
  });
  failCtx.shareBtn.textContent = "📤 Share";
  await failCtx.context.shareReferralViaTelegram(failCtx.shareBtn);
  assert.equal(failCtx.shareBtn.disabled, false);
  assert.equal(failCtx.shareBtn.textContent, "📤 Share");
});

test("8. Share pressed while Get Link's own share-content fetch is still in-flight reuses that single request", async () => {
  let resolveFetch;
  const pending = new Promise((resolve) => {
    resolveFetch = resolve;
  });
  const { context, calls, shareBtn } = makeContext({
    latestReferralLink: "https://t.me/+shared",
    latestSharePackage: null, // Get Link generated the link but hasn't cached a caption yet
    v2FetchImpl: () =>
      pending.then(() => ({
        ok: true,
        status: 200,
        json: async () => ({
          ok: true,
          message: "one true caption\nhttps://t.me/+shared",
          share_text: "one true caption",
          invite_link: "https://t.me/+shared",
        }),
      })),
  });

  // Get Link's copy step is mid-request (getReferralCaption -> fetchAndCacheShareContent)...
  const getLinkCaption = context.getReferralCaption("https://t.me/+shared");
  // ...when the user presses Share before it resolves.
  const share = context.shareReferralViaTelegram(shareBtn);
  resolveFetch();
  const [caption] = await Promise.all([getLinkCaption, share]);

  assert.equal(calls.v2Fetch, 1, "Share must reuse Get Link's in-flight request, not fire a second one");
  assert.equal(caption, "one true caption\nhttps://t.me/+shared");
  assert.equal(calls.opened.length, 1);
  assert.ok(
    calls.opened[0].includes(encodeURIComponent("one true caption")),
    "Share's payload must use the same resolved caption Get Link cached, not a divergent one"
  );
});
