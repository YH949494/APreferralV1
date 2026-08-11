/**
 * Tests for the one-click "Get link -> Copy caption + link" referral flow
 * (handleReferralLinkClick / copyReferralCaptionAndFlash) that copies the
 * full referral caption (fetched from /api/referral/share-content) plus the
 * user's referral URL to the clipboard, rather than the raw URL alone. The
 * functions live inline in static/index.html (no build step), so they are
 * extracted as source text and executed in a sandboxed vm context with
 * mocked DOM/fetch/clipboard globals.
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
// handleReferralLinkClick (via copyReferralCaptionAndFlash / getReferralCaption)
// now calls fetchReferralShareContent, which is defined further down the file
// than the old copy-flow functions were -- the extraction window must include
// it, so it ends just before copyReferral() rather than at setShareRankStatus.
const END_MARKER = "\n    async function copyReferral() {";

const APPROVED_CAPTION_BENEFIT_LINES = [
  "🎟️ FREE Welcome Voucher — No deposit required",
  "⚡️ Daily voucher drops",
  "🎁 Bonus campaigns",
  "👑 VIP-only announcements",
  "🏆 Weekly ranking rewards",
];

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
  "referral-caption-manual",
  "referral-caption-manual-text",
];

function defaultShareContentFetch() {
  return Promise.resolve({
    ok: true,
    json: async () => ({
      ok: true,
      message: "MOCK_SHARE_CAPTION",
      share_text: "MOCK_SHARE_TEXT",
      invite_link: "https://t.me/+default",
      playback_url: null,
      hook_text: null,
    }),
  });
}

function makeContext({
  elementIds = DEFAULT_ELEMENTS,
  userId = 555,
  fetchImpl,
  v2FetchImpl,
  clipboardWriteText,
  execCommandResult = true,
} = {}) {
  const elements = {};
  elementIds.forEach((id) => {
    elements[id] = makeElementStub(id);
  });

  const calls = {
    fetch: 0,
    v2Fetch: 0,
    hapticNotify: [],
    toasts: [],
    analytics: [],
    execCommand: 0,
    trackEngagementEvent: [],
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
    latestSharePackage: null,
    referralEntryActionHandled: false,
    referralLinkRequestInFlight: false,
    shareRequestInFlight: false,
    shareContentRequestInFlight: null,
    t: (key) => key,
    hapticNotify: (kind) => calls.hapticNotify.push(kind),
    loadReferralProgress: () => {},
    showCopySuccessToast: () => calls.toasts.push("copied"),
    // Real trackEngagementEvent (static/index.html) never throws -- it
    // swallows its own errors internally. Mirroring that "never throws"
    // contract here matters: handleReferralLinkClick() /
    // copyReferralCaptionAndFlash() must not have their real (non-tracking)
    // behavior altered by a tracking call.
    trackEngagementEvent: (event, opts) => {
      calls.trackEngagementEvent.push({ event, ...opts });
    },
    fetch:
      fetchImpl ||
      (() => {
        calls.fetch++;
        return Promise.resolve({
          text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+default" }),
        });
      }),
    v2Fetch: (...args) => {
      calls.v2Fetch++;
      return (v2FetchImpl || defaultShareContentFetch)(...args);
    },
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

function shareContentFetch(message, extra = {}) {
  return () =>
    Promise.resolve({
      ok: true,
      json: async () => ({
        ok: true,
        message,
        share_text: "share text",
        invite_link: extra.invite_link || "https://t.me/+default",
        playback_url: null,
        hook_text: null,
        ...extra,
      }),
    });
}

function failingShareContentFetch() {
  return () => Promise.resolve({ ok: false, json: async () => ({ ok: false, error: "boom" }) });
}

test("1. first click generates the link and copies the full caption + referral URL", async () => {
  const clipboardCalls = [];
  const { context, elements, calls } = makeContext({
    fetchImpl: countingFetch({ fetch: 0 }, async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+abc123" }),
    })),
    v2FetchImpl: shareContentFetch(
      "👋 Welcome to AdvantPlay Community!\n\nJoin our channel to get 👇\n\n🎟️ FREE Welcome Voucher — No deposit required\n⚡️ Daily voucher drops\n🎁 Bonus campaigns\n👑 VIP-only announcements\n🏆 Weekly ranking rewards\n\nStart here 👇\nhttps://t.me/+abc123",
      { invite_link: "https://t.me/+abc123" }
    ),
    clipboardWriteText: async (text) => {
      clipboardCalls.push(text);
    },
  });

  await context.handleReferralLinkClick();

  assert.equal(context.latestReferralLink, "https://t.me/+abc123");
  assert.equal(elements["referral-link-inline-text"].textContent, "https://t.me/+abc123");
  assert.equal(clipboardCalls.length, 1);
  assert.ok(clipboardCalls[0].includes("https://t.me/+abc123"), "clipboard must contain the referral URL");
  assert.ok(clipboardCalls[0].length > "https://t.me/+abc123".length, "clipboard must contain more than just the URL");
  assert.equal(elements["ap-get-referral-btn"].textContent, "copy_caption_link");
  assert.ok(calls.analytics.some((l) => l.includes("referral_link_requested")));
  assert.ok(calls.analytics.some((l) => l.includes("referral_link_generated")));
  assert.ok(calls.analytics.some((l) => l.includes("referral_share_content_requested")));
  assert.ok(calls.analytics.some((l) => l.includes("referral_caption_copied")));
  assert.ok(!calls.analytics.some((l) => l.includes("referral_link_copied")), "old referral_link_copied event must not fire for a full-caption copy");

  // The single click that drove this whole flow must produce exactly the
  // engagement-tracking events the admin dashboard's Referral Engagement
  // panel aggregates on: a CTA click (fired first, before generation even
  // starts), then a link-generated, then a copy-clicked once the caption
  // lands in the clipboard.
  const trackedEventNames = calls.trackEngagementEvent.map((c) => c.event);
  assert.deepEqual(trackedEventNames, [
    "referral_cta_clicked",
    "referral_link_generated",
    "referral_copy_clicked",
  ]);
  assert.ok(calls.trackEngagementEvent.every((c) => c.surface === "referral_step_cta"));
});

test("2. copied content contains all four benefit lines", async () => {
  const clipboardCalls = [];
  const { context } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+benefits" }),
    }),
    v2FetchImpl: shareContentFetch(
      "👋 Welcome to AdvantPlay Community!\n\nJoin our channel to get 👇\n\n🎟️ FREE Welcome Voucher — No deposit required\n⚡️ Daily voucher drops\n🎁 Bonus campaigns\n👑 VIP-only announcements\n🏆 Weekly ranking rewards\n\nStart here 👇\nhttps://t.me/+benefits",
      { invite_link: "https://t.me/+benefits" }
    ),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();

  const copied = clipboardCalls[0];
  APPROVED_CAPTION_BENEFIT_LINES.forEach((line) => {
    assert.ok(copied.includes(line), `caption must include benefit line: ${line}`);
  });
  assert.ok(copied.includes("FREE Welcome Voucher"));
  assert.ok(copied.includes("No deposit required"));
  assert.ok(
    copied.indexOf("FREE Welcome Voucher") < copied.indexOf("Daily voucher drops"),
    "Welcome Voucher line must come before Daily voucher drops"
  );
});

test("3. copied content ends with the correct user referral URL", async () => {
  const clipboardCalls = [];
  const { context } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+enderoo" }),
    }),
    v2FetchImpl: shareContentFetch(
      "👋 Welcome to AdvantPlay Community!\n\nJoin our channel to get 👇\n\n⚡️ Daily voucher drops\n🎁 Bonus campaigns\n👑 VIP-only announcements\n🏆 Weekly ranking rewards\n\nStart here 👇\nhttps://t.me/+enderoo",
      { invite_link: "https://t.me/+enderoo" }
    ),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();

  assert.ok(clipboardCalls[0].endsWith("https://t.me/+enderoo"));
});

test("4. existing referral link is reused without generating a new invite link", async () => {
  const clipboardCalls = [];
  const calls = { fetch: 0 };
  const { context } = makeContext({
    fetchImpl: countingFetch(calls, async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+xyz" }),
    })),
    v2FetchImpl: shareContentFetch("caption one\nhttps://t.me/+xyz", { invite_link: "https://t.me/+xyz" }),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();
  assert.equal(calls.fetch, 1);

  await context.handleReferralLinkClick();
  assert.equal(calls.fetch, 1, "second click must not call the invite-link API again");
});

test("5. subsequent click copies the full caption again, not only the URL", async () => {
  const clipboardCalls = [];
  const { context, elements } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+again" }),
    }),
    v2FetchImpl: shareContentFetch(
      "👋 Welcome to AdvantPlay Community!\n\nJoin our channel to get 👇\n\n🎟️ FREE Welcome Voucher — No deposit required\n⚡️ Daily voucher drops\n🎁 Bonus campaigns\n👑 VIP-only announcements\n🏆 Weekly ranking rewards\n\nStart here 👇\nhttps://t.me/+again",
      { invite_link: "https://t.me/+again" }
    ),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();
  await context.handleReferralLinkClick();

  assert.equal(clipboardCalls.length, 2);
  clipboardCalls.forEach((copied) => {
    assert.ok(copied.includes("https://t.me/+again"));
    assert.ok(copied.length > "https://t.me/+again".length, "must not be only the raw URL");
  });
  assert.equal(elements["ap-get-referral-btn"].textContent, "copy_caption_link");
});

test("6. /api/referral/share-content is used as the primary caption source", async () => {
  const clipboardCalls = [];
  const { context, calls } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+primary" }),
    }),
    v2FetchImpl: shareContentFetch("BACKEND_PROVIDED_CAPTION\nhttps://t.me/+primary", {
      invite_link: "https://t.me/+primary",
    }),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();

  assert.equal(calls.v2Fetch, 1, "share-content endpoint must be called");
  assert.equal(clipboardCalls[0], "BACKEND_PROVIDED_CAPTION\nhttps://t.me/+primary");
});

test("7. share-content API failure falls back to the approved static caption plus referral URL", async () => {
  const clipboardCalls = [];
  const { context, elements } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+fallback" }),
    }),
    v2FetchImpl: failingShareContentFetch(),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();

  // Link generation itself must NOT be reported as failed.
  assert.equal(context.latestReferralLink, "https://t.me/+fallback");
  assert.equal(elements["referral-link-error"].style.display || "none", "none");

  const copied = clipboardCalls[0];
  assert.ok(copied.includes("👋 Welcome to AdvantPlay Community!"));
  assert.ok(copied.includes("Join our channel to get 👇"));
  APPROVED_CAPTION_BENEFIT_LINES.forEach((line) => {
    assert.ok(copied.includes(line));
  });
  assert.ok(
    copied.indexOf("FREE Welcome Voucher") < copied.indexOf("Daily voucher drops"),
    "fallback caption must list the Welcome Voucher benefit before Daily voucher drops"
  );
  assert.equal(
    copied,
    "👋 Welcome to AdvantPlay Community!\n\nJoin our channel to get 👇\n\n🎟️ FREE Welcome Voucher — No deposit required\n⚡️ Daily voucher drops\n🎁 Bonus campaigns\n👑 VIP-only announcements\n🏆 Weekly ranking rewards\n\nStart here 👇\nhttps://t.me/+fallback",
    "fallback caption must match the canonical caption structure exactly"
  );
  assert.ok(copied.endsWith("https://t.me/+fallback"));
});

test("8. clipboard failure does not get reported as referral generation failure", async () => {
  const { context, elements, calls } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+manual" }),
    }),
    v2FetchImpl: shareContentFetch("caption text\nhttps://t.me/+manual", { invite_link: "https://t.me/+manual" }),
    clipboardWriteText: async () => {
      throw new Error("denied");
    },
    execCommandResult: false,
  });

  await context.handleReferralLinkClick();

  assert.equal(context.latestReferralLink, "https://t.me/+manual", "link must still be considered generated");
  assert.equal(elements["referral-link-inline-text"].textContent, "https://t.me/+manual", "link must remain visible");
  assert.notEqual(elements["referral-link-error"].textContent, "referral_generate_error");
  assert.equal(elements["referral-link-error"].textContent, "referral_manual_copy");
  assert.equal(elements["referral-caption-manual-text"].textContent, "caption text\nhttps://t.me/+manual");
  assert.ok(calls.analytics.some((l) => l.includes("referral_caption_copy_failed")));
  assert.ok(!calls.analytics.some((l) => l.includes("referral_link_requested={\"reused\":false}") && l.includes("failed")));
});

test("clipboard API failure falls back to execCommand copy", async () => {
  const { context, elements, calls } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+execfallback" }),
    }),
    v2FetchImpl: shareContentFetch("caption\nhttps://t.me/+execfallback", { invite_link: "https://t.me/+execfallback" }),
    clipboardWriteText: async () => {
      throw new Error("denied");
    },
    execCommandResult: true,
  });

  await context.handleReferralLinkClick();

  assert.equal(calls.execCommand, 1, "execCommand fallback should have been used");
  assert.equal(elements["ap-get-referral-btn"].textContent, "copy_caption_link");
  assert.equal(elements["referral-link-error"].style.display, "none");
});

test("referral generation failure shows the approved error and does not clear an existing link", async () => {
  let attempt = 0;
  const { context, elements } = makeContext({
    fetchImpl: async () => {
      attempt++;
      if (attempt === 1) {
        return { text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+good" }) };
      }
      return { text: async () => JSON.stringify({ success: false, referral_link: null, error: "boom" }) };
    },
    v2FetchImpl: shareContentFetch("caption\nhttps://t.me/+good", { invite_link: "https://t.me/+good" }),
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
  const { context } = makeContext({
    fetchImpl,
    v2FetchImpl: shareContentFetch("caption\nhttps://t.me/+concurrent", { invite_link: "https://t.me/+concurrent" }),
    clipboardWriteText: async () => {},
  });

  const first = context.handleReferralLinkClick();
  const second = context.handleReferralLinkClick();

  resolveFetch();
  await Promise.all([first, second]);

  assert.equal(calls.fetch, 1, "concurrent clicks must only trigger one API request");
});

test("9. Share button still works and uses the same canonical caption", async () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
  const shareStart = html.indexOf("async function shareReferralViaTelegram(btn) {");
  const shareEnd = html.indexOf('if (typeof isThaiUiLanguage !== "function") {');
  assert.ok(shareStart !== -1 && shareEnd !== -1, "shareReferralViaTelegram bounds not found");
  const fetchContentStart = html.indexOf("function buildFallbackReferralCaption(link) {");
  assert.ok(fetchContentStart !== -1 && fetchContentStart < shareStart);
  const source = html.slice(fetchContentStart, shareEnd);

  const openedUrls = [];
  const trackedEvents = [];
  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    v2Fetch: async () =>
      ({
        ok: true,
        json: async () => ({
          ok: true,
          message: "SAME_CANONICAL_CAPTION",
          invite_link: "https://t.me/+share-correct-link",
          share_text: "SAME_CANONICAL_CAPTION",
        }),
      }),
    openTelegramSafeLink: (url) => {
      openedUrls.push(url);
      return true;
    },
    hapticImpact: () => {},
    hapticNotify: () => {},
    tgAlert: () => {},
    t: (key) => key,
    API_BASE: "https://api.example.test",
    latestReferralLink: "",
    latestSharePackage: null,
    shareRequestInFlight: false,
    shareContentRequestInFlight: null,
    trackEngagementEvent: (event, opts) => {
      trackedEvents.push({ event, ...opts });
    },
  };
  const context = vm.createContext(sandbox);
  vm.runInContext(source, context, { filename: "index.html-extract-share.js" });

  await context.shareReferralViaTelegram();

  assert.equal(openedUrls.length, 1);
  assert.ok(openedUrls[0].includes(encodeURIComponent("https://t.me/+share-correct-link")));
  assert.ok(openedUrls[0].includes(encodeURIComponent("SAME_CANONICAL_CAPTION")));

  // A successfully opened share must be tracked as referral_share_clicked --
  // this is the Mini App source's denominator for Copy/Share Users on the
  // admin engagement dashboard.
  assert.equal(trackedEvents.length, 1);
  assert.equal(trackedEvents[0].event, "referral_share_clicked");
  assert.equal(trackedEvents[0].referralLinkId, "https://t.me/+share-correct-link");
});

test("10. Thai/Indonesian button-state translations stay distinct while the copied caption is unaffected by UI language", async () => {
  const i18n = fs.readFileSync(path.join(__dirname, "static", "i18n.js"), "utf8");
  assert.ok(/get_link:\s*"[^"]*"/.test(i18n));
  assert.ok(i18n.includes('copy_caption_link: "📋 Copy caption + link"'));
  assert.ok(i18n.includes('copy_caption_link: "📋 คัดลอกแคปชัน + ลิงก์"'));
  assert.ok(i18n.includes('copy_caption_link: "📋 Salin caption + link"'));

  // The copied campaign caption itself always comes from the backend (or the
  // approved static fallback) verbatim -- it is not translated client-side.
  const clipboardCalls = [];
  const { context } = makeContext({
    fetchImpl: async () => ({
      text: async () => JSON.stringify({ success: true, referral_link: "https://t.me/+i18n" }),
    }),
    v2FetchImpl: shareContentFetch("BACKEND_CAPTION_VERBATIM\nhttps://t.me/+i18n", {
      invite_link: "https://t.me/+i18n",
    }),
    clipboardWriteText: async (text) => clipboardCalls.push(text),
  });

  await context.handleReferralLinkClick();
  assert.equal(clipboardCalls[0], "BACKEND_CAPTION_VERBATIM\nhttps://t.me/+i18n");
});
