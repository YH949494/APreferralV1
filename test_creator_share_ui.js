/**
 * Tests for the Creator Share Centre frontend (static/creator-share.html).
 *
 * The page's logic is a single inline <script> IIFE (no build step), so it
 * is extracted as source text and executed in a sandboxed vm context with
 * mocked DOM/fetch/Telegram globals, mirroring
 * test_referral_share_button.test.js's approach for static/index.html.
 *
 * Run with: node --test test_creator_share_ui.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function loadScriptSource() {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  const start = html.indexOf("<script>\n(function () {");
  const end = html.indexOf("</script>", start);
  assert.ok(start !== -1, "inline IIFE script not found in static/creator-share.html");
  assert.ok(end !== -1, "closing </script> not found");
  return html.slice(start + "<script>".length, end);
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

class ClassList {
  constructor() {
    this._set = new Set();
  }
  add(...names) {
    names.forEach((n) => this._set.add(n));
  }
  remove(...names) {
    names.forEach((n) => this._set.delete(n));
  }
  toggle(name, force) {
    if (force === undefined) {
      this._set.has(name) ? this._set.delete(name) : this._set.add(name);
    } else if (force) {
      this._set.add(name);
    } else {
      this._set.delete(name);
    }
  }
  contains(name) {
    return this._set.has(name);
  }
}

function makeElement(id) {
  const listeners = {};
  return {
    id,
    classList: new ClassList(),
    _text: "",
    disabled: false,
    style: {},
    scrollIntoViewCalls: [],
    get textContent() {
      return this._text;
    },
    set textContent(v) {
      this._text = v;
    },
    scrollIntoView(opts) {
      this.scrollIntoViewCalls.push(opts);
    },
    addEventListener(event, handler) {
      listeners[event] = listeners[event] || [];
      listeners[event].push(handler);
    },
    // Real click events carry a real preventDefault(); tests track whether
    // it was called via `_lastEvent.defaultPrevented`.
    _trigger(event) {
      const evt = {
        target: this,
        defaultPrevented: false,
        preventDefault() {
          this.defaultPrevented = true;
        },
      };
      this._lastEvent = evt;
      (listeners[event] || []).forEach((h) => h(evt));
      return evt;
    },
  };
}

const ELEMENT_IDS = [
  "initial-loading",
  "access-denied",
  "app-shell",
  "btn-generate",
  "generate-status",
  "package-card",
  "package-caption",
  "package-playback",
  "package-cta",
  "package-benefits",
  "package-starter",
  "package-link",
  "btn-copy",
  "btn-try-another",
  "btn-telegram-share",
  "copy-toast",
  "share-status",
  "btn-toggle-rewards",
  "rewards-section",
  "stats-card",
  "stat-invited-value",
  "stat-qualified-value",
  "stat-tier-message",
];

// Mirrors the `class="... hidden"` markup already present in
// static/creator-share.html, since the lightweight element stubs below don't
// parse the real HTML/CSS -- only classes the inline script itself toggles.
const INITIALLY_HIDDEN_IDS = ["access-denied", "app-shell", "package-card", "package-caption", "package-playback", "rewards-section", "stat-tier-message"];

function buildSandbox({ initData = "tg_init_data_ok", fetchImpl, clipboardWriteText, execCommandResult = true, openTelegramLink, windowOpenResult, windowOpenThrows } = {}) {
  const elements = {};
  ELEMENT_IDS.forEach((id) => {
    elements[id] = makeElement(id);
    if (INITIALLY_HIDDEN_IDS.includes(id)) elements[id].classList.add("hidden");
  });

  const createdTextareas = [];
  const documentStub = {
    getElementById(id) {
      return elements[id] || null;
    },
    createElement(tag) {
      if (tag === "textarea") {
        const ta = {
          style: {},
          value: "",
          focus() {},
          select() {},
        };
        createdTextareas.push(ta);
        return ta;
      }
      return { style: {} };
    },
    body: {
      appendChild() {},
      removeChild() {},
    },
  };

  const fetchCalls = [];
  const fetchFn =
    fetchImpl ||
    (() =>
      Promise.resolve({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ status: "ok" }),
      }));

  const wrappedFetch = (url, opts) => {
    fetchCalls.push({ url, opts });
    return fetchFn(url, opts);
  };

  const telegramWebApp = { initData, ready() {}, expand() {}, openTelegramLink };

  const navigatorStub = {
    clipboard:
      clipboardWriteText === undefined
        ? { writeText: (text) => Promise.resolve(clipboardWriteText === undefined ? undefined : clipboardWriteText).then(() => {}) }
        : clipboardWriteText === null
        ? undefined
        : { writeText: clipboardWriteText },
  };

  const windowOpenCalls = [];
  const sandbox = {
    window: {
      Telegram: { WebApp: telegramWebApp },
      open: (url, target, features) => {
        windowOpenCalls.push({ url, target, features });
        if (windowOpenThrows) throw new Error("window.open blocked");
        // With "noopener" a real browser returns null even on a successful
        // open, so tests must not treat this return value as success/failure.
        return windowOpenResult === undefined ? null : windowOpenResult;
      },
    },
    document: documentStub,
    fetch: wrappedFetch,
    navigator: navigatorStub,
    encodeURIComponent,
    setImmediate,
    setTimeout,
    clearTimeout,
    console,
    execCommandResult,
  };
  sandbox.window.document = documentStub;
  sandbox.document.execCommand = () => execCommandResult;

  vm.createContext(sandbox);
  vm.runInContext(loadScriptSource(), sandbox);

  return { sandbox, elements, fetchCalls, createdTextareas, windowOpenCalls };
}

function statusOkResponse() {
  return Promise.resolve({
    ok: true,
    status: 200,
    json: () => Promise.resolve({ status: "ok", creator: { user_id: 1, access: true, creator_tier: "pilot" } }),
  });
}

function generateOkResponse(overrides) {
  return Promise.resolve({
    ok: true,
    status: 200,
    json: () =>
      Promise.resolve(
        Object.assign(
          {
            status: "ok",
            package_id: "pkg_1",
            hook_text: "Hook line",
            playback_url: "https://rx.apreplay.com/Abc123",
            referral_link: "https://t.me/+abc",
            share_text:
              "Hook line\nhttps://rx.apreplay.com/Abc123\n\nWant more replays like this—and rewards too?\nJoin AdvantPlay for:\n🎟️ Free welcome voucher\n⚡️ Daily voucher drops\n🏆 Weekly rewards\n\nStart here 👇\nhttps://t.me/+abc",
          },
          overrides || {}
        )
      ),
  });
}

test("initial loading state clears once status resolves", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  assert.equal(elements["initial-loading"].classList.contains("hidden"), false);
  await flush();
  await flush();
  assert.equal(elements["initial-loading"].classList.contains("hidden"), true);
  assert.equal(elements["app-shell"].classList.contains("hidden"), false);
});

test("no post generated: only Get My Share Post is shown", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();

  assert.equal(elements["btn-generate"].classList.contains("hidden"), false);
  assert.equal(elements["package-card"].classList.contains("hidden"), true);
});

test("Generate renders the post split into caption/playback/CTA/link and hides Get My Share Post", async () => {
  const { elements, fetchCalls } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  assert.equal(elements["package-caption"].textContent, "Hook line");
  assert.equal(elements["package-playback"].textContent, "https://rx.apreplay.com/Abc123");
  assert.equal(elements["package-cta"].textContent, "Want more replays like this—and rewards too?");
  assert.equal(elements["package-benefits"].textContent, "Join AdvantPlay for: 🎟️ Free welcome voucher · ⚡️ Daily voucher drops · 🏆 Weekly rewards");
  assert.equal(elements["package-starter"].textContent, "Start here 👇");
  assert.equal(elements["package-link"].textContent, "https://t.me/+abc");
  assert.equal(elements["package-card"].classList.contains("hidden"), false);
  assert.equal(elements["btn-generate"].classList.contains("hidden"), true, "Get My Share Post is replaced once a post exists");

  const generateCall = fetchCalls.find((c) => c.url.includes("/api/creator/share/generate"));
  assert.ok(generateCall, "generate endpoint was called");
  assert.equal(JSON.parse(generateCall.opts.body).platform, "generic");
});

test("Get My Share Post reveals the card, smooth-scrolls it into view, copies share_text exactly once, and shows the copied confirmation", async () => {
  let writeTextCalls = [];
  const { elements, fetchCalls } = buildSandbox({
    clipboardWriteText: (text) => {
      writeTextCalls.push(text);
      return Promise.resolve();
    },
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  assert.equal(elements["package-card"].classList.contains("hidden"), true, "card starts hidden");

  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  assert.equal(elements["package-card"].classList.contains("hidden"), false, "card must be revealed immediately after generation");
  assert.equal(
    elements["package-card"].scrollIntoViewCalls.length,
    1,
    "the generated card must be smooth-scrolled into view exactly once"
  );
  assert.equal(elements["package-card"].scrollIntoViewCalls[0].behavior, "smooth");

  assert.equal(writeTextCalls.length, 1, "successful generation must trigger exactly one clipboard write");
  assert.equal(
    writeTextCalls[0],
    "Hook line\nhttps://rx.apreplay.com/Abc123\n\nWant more replays like this—and rewards too?\nJoin AdvantPlay for:\n🎟️ Free welcome voucher\n⚡️ Daily voucher drops\n🏆 Weekly rewards\n\nStart here 👇\nhttps://t.me/+abc",
    "clipboard write must carry the complete generated share_text"
  );

  assert.equal(elements["generate-status"].textContent, "✓ Post Copied");
  assert.equal(elements["generate-status"].classList.contains("error"), false);

  const copiedCall = fetchCalls.find((c) => c.url.includes("/copied"));
  assert.ok(copiedCall, "the automatic copy should record the copied event, same as a manual copy");
});

test("clipboard failure on auto-copy still reveals the post and keeps the manual Copy Post button functional", async () => {
  let attempt = 0;
  const { elements, fetchCalls } = buildSandbox({
    clipboardWriteText: (text) => {
      attempt += 1;
      // First (automatic) attempt fails; a later manual retry succeeds.
      return attempt === 1 ? Promise.reject(new Error("denied")) : Promise.resolve();
    },
    execCommandResult: false,
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse({ package_id: "pkg_retry" });
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  assert.equal(elements["package-card"].classList.contains("hidden"), false, "post must still be shown even though the auto-copy failed");
  assert.notEqual(
    elements["generate-status"].textContent,
    "✓ Post Copied",
    "must never claim success when the automatic copy failed"
  );
  assert.equal(elements["btn-copy"].textContent, "Copy Post", "manual Copy Post button must remain available");

  elements["btn-copy"]._trigger("click");
  await flush();
  await flush();

  assert.equal(elements["btn-copy"].textContent, "✓ Copied — Go Share It", "manual retry via Copy Post must still work");
  const copiedCall = fetchCalls.find((c) => c.url.includes("/copied"));
  assert.ok(copiedCall, "the successful manual retry should record the copied event");
});

test("repeated clicks on Get My Share Post while a request is in flight do not send duplicate generation requests", async () => {
  let resolveGenerate;
  const generatePromise = new Promise((resolve) => {
    resolveGenerate = resolve;
  });

  const { elements, fetchCalls } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generatePromise;
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  elements["btn-generate"]._trigger("click");
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  const generateCallsWhileInFlight = fetchCalls.filter((c) => c.url.includes("/api/creator/share/generate"));
  assert.equal(generateCallsWhileInFlight.length, 1, "clicking Get My Share Post repeatedly must not fire duplicate generate requests");

  resolveGenerate({
    ok: true,
    status: 200,
    json: () =>
      Promise.resolve({
        status: "ok",
        package_id: "pkg_once",
        hook_text: null,
        playback_url: null,
        referral_link: "https://t.me/+once",
        share_text: "Want more replays like this—and rewards too?\nJoin AdvantPlay for:\n🎟️ Free welcome voucher\n⚡️ Daily voucher drops\n🏆 Weekly rewards\n\nStart here 👇\nhttps://t.me/+once",
      }),
  });
  await flush();
  await flush();

  // A further click after the first request has resolved is a new, legitimate request.
  elements["btn-try-another"]._trigger("click");
  await flush();
  await flush();

  const allGenerateCalls = fetchCalls.filter((c) => c.url.includes("/api/creator/share/generate"));
  assert.equal(allGenerateCalls.length, 2, "a click after the in-flight request settles is a separate, legitimate request");
});

test("a stale auto-copy completion (from a package superseded by 'Give Me Another Post') does not overwrite the newer package's UI, but still credits the right package for /copied", async () => {
  const resolvers = [];
  let generateCallCount = 0;
  const { elements, fetchCalls } = buildSandbox({
    clipboardWriteText: () => new Promise((resolve) => { resolvers.push(resolve); }),
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) {
        generateCallCount += 1;
        return generateCallCount === 1
          ? generateOkResponse({ package_id: "pkg_A", hook_text: "Hook A", share_text: "TEXT_A" })
          : generateOkResponse({ package_id: "pkg_B", hook_text: "Hook B", share_text: "TEXT_B" });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();
  assert.equal(elements["package-caption"].textContent, "Hook A");

  // pkg_A's automatic clipboard write is still in flight (resolvers[0] pending)
  // when the user regenerates -- pkg_B becomes current before A's copy settles.
  elements["btn-try-another"]._trigger("click");
  await flush();
  await flush();
  assert.equal(elements["package-caption"].textContent, "Hook B");
  assert.equal(elements["generate-status"].textContent, "", "pkg_B's own auto-copy hasn't settled yet");

  // Now let pkg_A's stale copy resolve.
  resolvers[0]();
  await flush();
  await flush();

  assert.notEqual(
    elements["generate-status"].textContent,
    "✓ Post Copied",
    "a stale copy completion for the superseded package must not claim success on the current (pkg_B) status line"
  );
  assert.equal(elements["package-caption"].textContent, "Hook B", "current package's rendered content must be untouched by the stale completion");

  let copiedCalls = fetchCalls.filter((c) => c.url.includes("/copied"));
  assert.equal(copiedCalls.length, 1, "the stale completion must still be tracked");
  assert.ok(copiedCalls[0].url.includes("pkg_A"), "tracking must credit the package that was actually copied (pkg_A), not the now-current pkg_B");

  // Finally resolve pkg_B's own (still-current) auto-copy.
  resolvers[1]();
  await flush();
  await flush();

  assert.equal(elements["generate-status"].textContent, "✓ Post Copied", "pkg_B's own copy completing while still current must update the status line");
  copiedCalls = fetchCalls.filter((c) => c.url.includes("/copied"));
  assert.equal(copiedCalls.length, 2);
  assert.ok(copiedCalls[1].url.includes("pkg_B"));
});

test("empty caption/playback rows are hidden without showing the literal word None", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) {
        return generateOkResponse({
          hook_text: null,
          playback_url: null,
          referral_link: "https://t.me/+bare",
          share_text: "Want more replays like this—and rewards too?\nJoin AdvantPlay for:\n🎟️ Free welcome voucher\n⚡️ Daily voucher drops\n🏆 Weekly rewards\n\nStart here 👇\nhttps://t.me/+bare",
        });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  assert.equal(elements["package-caption"].classList.contains("hidden"), true);
  assert.equal(elements["package-caption"].textContent, "");
  assert.equal(elements["package-playback"].classList.contains("hidden"), true);
  assert.equal(elements["package-link"].textContent, "https://t.me/+bare");
});

test("Copy Post copies the exact unmodified share_text, shows confirmation, then reverts after ~2s", async () => {
  const shareText = "Only hook\n\nWant more replays like this—and rewards too?\nJoin AdvantPlay for:\n🎟️ Free welcome voucher\n⚡️ Daily voucher drops\n🏆 Weekly rewards\n\nStart here 👇\nhttps://t.me/+xyz";
  let writeTextArg = null;
  const { elements, fetchCalls } = buildSandbox({
    clipboardWriteText: (text) => {
      writeTextArg = text;
      return Promise.resolve();
    },
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) {
        return generateOkResponse({ package_id: "pkg_42", hook_text: "Only hook", playback_url: null, referral_link: "https://t.me/+xyz", share_text: shareText });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  assert.equal(elements["btn-copy"].textContent, "Copy Post");

  elements["btn-copy"]._trigger("click");
  await flush();
  await flush();

  assert.equal(writeTextArg, shareText, "clipboard.writeText must receive the exact, unaltered share_text");
  assert.equal(elements["btn-copy"].textContent, "✓ Copied — Go Share It");

  const copiedCall = fetchCalls.find((c) => c.url.includes("/copied"));
  assert.ok(copiedCall, "copied endpoint should be called after a successful clipboard copy");
  assert.ok(copiedCall.url.includes("pkg_42"));

  await new Promise((resolve) => setTimeout(resolve, 2100));
  assert.equal(elements["btn-copy"].textContent, "Copy Post", "button label reverts to Copy Post after ~2s");
});

test("clipboard failure shows an inline error and never a fake success state", async () => {
  const { elements, fetchCalls } = buildSandbox({
    clipboardWriteText: () => Promise.reject(new Error("denied")),
    execCommandResult: false,
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse({ package_id: "pkg_fail" });
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  elements["btn-copy"]._trigger("click");
  await flush();
  await flush();

  const copiedCall = fetchCalls.find((c) => c.url.includes("/copied"));
  assert.equal(copiedCall, undefined, "copied endpoint must not be called when the copy itself failed");
  assert.equal(elements["btn-copy"].textContent, "Copy Post", "must not flip to the success label on failure");
  assert.match(elements["copy-toast"].textContent, /couldn't copy/i);
});

test("clipboard fallback (execCommand) works when navigator.clipboard is unavailable", async () => {
  const { elements, fetchCalls, createdTextareas } = buildSandbox({
    clipboardWriteText: null,
    execCommandResult: true,
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse({ package_id: "pkg_fallback" });
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  // Generation already triggered one automatic copy via the fallback path.
  assert.equal(createdTextareas.length, 1, "auto-copy after generation must use the fallback textarea path");

  elements["btn-copy"]._trigger("click");
  await flush();
  await flush();

  assert.equal(createdTextareas.length, 2, "manual Copy Post must also use the fallback textarea path");
  const copiedCall = fetchCalls.find((c) => c.url.includes("/copied"));
  assert.ok(copiedCall, "fallback copy success should still record the copied event");
});

test("Telegram share inside Telegram WebApp uses openTelegramLink, with the referral link only in url= and never duplicated", async () => {
  let openedUrl = null;
  const { elements, fetchCalls } = buildSandbox({
    openTelegramLink: (url) => {
      openedUrl = url;
    },
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  elements["btn-telegram-share"]._trigger("click");
  await flush();

  assert.ok(openedUrl, "openTelegramLink should have been called");
  assert.ok(openedUrl.startsWith("https://t.me/share/url?"), "must use the t.me/share/url composer");

  const encodedLink = encodeURIComponent("https://t.me/+abc");
  assert.ok(openedUrl.includes("url=" + encodedLink), "the personal referral link must be carried in the url= param");
  assert.ok(!openedUrl.includes("text=" + encodeURIComponent("https://t.me/+abc")), "the referral link must not also be embedded inside text=");

  const occurrences = openedUrl.split(encodedLink).length - 1;
  assert.equal(occurrences, 1, "the referral link must appear exactly once in the composed share URL (no duplication)");
  assert.equal(elements["share-status"].textContent, "");

  const clickCall = fetchCalls.find((c) => c.url.includes("/share-clicked"));
  assert.ok(clickCall, "share-clicked should be recorded before/around opening the Telegram share action");
});

test("Telegram share outside Telegram (ordinary browser) falls back to window.open with noopener,noreferrer", async () => {
  const { elements, windowOpenCalls } = buildSandbox({
    openTelegramLink: undefined, // no Telegram bridge => ordinary browser path
    windowOpenResult: { closed: false }, // simulates a successful popup
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  elements["btn-telegram-share"]._trigger("click");
  await flush();

  assert.equal(windowOpenCalls.length, 1, "window.open must be used when no Telegram WebApp bridge is present");
  assert.ok(windowOpenCalls[0].url.startsWith("https://t.me/share/url?"));
  assert.equal(windowOpenCalls[0].target, "_blank");
  assert.equal(windowOpenCalls[0].features, "noopener,noreferrer");
  assert.equal(elements["share-status"].textContent, "", "no fallback error when the popup opens successfully");
});

test("window.open's null return (the normal, expected result when noopener is set) must NOT be treated as a blocked popup", async () => {
  // With "noopener", real browsers intentionally return null even when the
  // window opened successfully -- there is no reference to hand back. A
  // naive `!!win` success check would misreport this as failure every time.
  const { elements, windowOpenCalls } = buildSandbox({
    openTelegramLink: undefined,
    windowOpenResult: null, // the real-world return value for a successful noopener open
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  elements["btn-telegram-share"]._trigger("click");
  await flush();

  assert.equal(windowOpenCalls.length, 1);
  assert.equal(elements["share-status"].textContent, "", "a null return from window.open must not trigger the fallback error message");
});

test("Telegram share failure (window.open throws, e.g. a genuinely blocked popup) shows a visible fallback message", async () => {
  const { elements } = buildSandbox({
    openTelegramLink: undefined,
    windowOpenThrows: true, // simulates the browser actually refusing to open the popup
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  elements["btn-telegram-share"]._trigger("click");
  await flush();

  assert.match(elements["share-status"].textContent, /couldn't open telegram/i);
});

test("Telegram share click handler calls preventDefault so it can't submit a form or reload the Mini App", async () => {
  const { elements } = buildSandbox({
    openTelegramLink: (url) => {},
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generateOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  const evt = elements["btn-telegram-share"]._trigger("click");
  await flush();

  assert.equal(evt.defaultPrevented, true, "telegramShare must call event.preventDefault()");
});

test("Telegram share caption and referral link are fully URL-encoded, including special characters", async () => {
  let openedUrl = null;
  const { elements } = buildSandbox({
    openTelegramLink: (url) => {
      openedUrl = url;
    },
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) {
        return generateOkResponse({
          hook_text: "Big win & huge payout?! 100% real 🎉",
          playback_url: "https://rx.apreplay.com/Abc 123?x=y&z=1",
          referral_link: "https://t.me/+abc?start=ref&x=1",
          share_text: "irrelevant for this test",
        });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  elements["btn-telegram-share"]._trigger("click");
  await flush();

  assert.ok(openedUrl, "openTelegramLink should have been called");
  assert.match(openedUrl, /^https:\/\/t\.me\/share\/url\?url=[^&]*&text=.*$/, "exactly one url= param and one text= param");

  // Split into the two encoded param VALUES (each value is opaque to
  // encodeURIComponent, so a literal "&" only ever appears as the one
  // separator between url= and text=).
  const queryString = openedUrl.slice(openedUrl.indexOf("?") + 1);
  const urlValue = queryString.slice("url=".length, queryString.indexOf("&text="));
  const textValue = queryString.slice(queryString.indexOf("&text=") + "&text=".length);

  // Note: encodeURIComponent intentionally leaves "!" unescaped (RFC3986
  // unreserved set), so it is not checked here.
  ["&", "?", " ", "🎉", "="].forEach((raw) => {
    assert.ok(!urlValue.includes(raw), `raw "${raw}" must not leak unescaped into the encoded url= value`);
    assert.ok(!textValue.includes(raw), `raw "${raw}" must not leak unescaped into the encoded text= value`);
  });

  assert.ok(openedUrl.includes("url=" + encodeURIComponent("https://t.me/+abc?start=ref&x=1")));
  assert.ok(openedUrl.includes(encodeURIComponent("Big win & huge payout?! 100% real 🎉")));
  assert.ok(openedUrl.includes(encodeURIComponent("https://rx.apreplay.com/Abc 123?x=y&z=1")));
});

test("Give Me Another Post disables the buttons during the request", async () => {
  let resolveGenerate;
  const generatePromise = new Promise((resolve) => {
    resolveGenerate = resolve;
  });

  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) return generatePromise;
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();

  assert.equal(elements["btn-generate"].disabled, true);
  assert.equal(elements["btn-try-another"].disabled, true);

  resolveGenerate({
    ok: true,
    status: 200,
    json: () =>
      Promise.resolve({
        status: "ok",
        package_id: "pkg_x",
        hook_text: null,
        playback_url: null,
        referral_link: "https://t.me/+x",
        share_text: "Want more replays like this—and rewards too?\nJoin AdvantPlay for:\n🎟️ Free welcome voucher\n⚡️ Daily voucher drops\n🏆 Weekly rewards\n\nStart here 👇\nhttps://t.me/+x",
      }),
  });
  await flush();
  await flush();

  assert.equal(elements["btn-generate"].disabled, false);
});

test("content generation failure shows an inline error, not a silent no-op", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/generate")) {
        return Promise.resolve({ ok: false, status: 502, json: () => Promise.resolve({ status: "error", code: "generation_failed" }) });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  assert.match(elements["generate-status"].textContent, /couldn't generate/i);
  assert.equal(elements["package-card"].classList.contains("hidden"), true, "no package should render on failure");
});

test("access denied: not an approved creator", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({ ok: false, status: 403, json: () => Promise.resolve({ status: "error", code: "creator_not_authorized" }) });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();

  assert.equal(elements["app-shell"].classList.contains("hidden"), true);
  assert.match(elements["access-denied"].textContent, /approved creators only/i);
});

test("access denied: creator suspended", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({ ok: false, status: 403, json: () => Promise.resolve({ status: "error", code: "creator_suspended" }) });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();

  assert.equal(elements["app-shell"].classList.contains("hidden"), true);
  assert.match(elements["access-denied"].textContent, /suspended/i);
});

test("network error on status fetch clears loading and renders an error state", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return Promise.reject(new Error("network down"));
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();

  assert.equal(elements["initial-loading"].classList.contains("hidden"), true);
  assert.equal(elements["access-denied"].classList.contains("hidden"), false);
});

test("reward ladder: collapsed by default, expands and collapses on toggle, no progress data shown", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();

  assert.equal(elements["rewards-section"].classList.contains("hidden"), true);

  elements["btn-toggle-rewards"]._trigger("click");
  assert.equal(elements["rewards-section"].classList.contains("hidden"), false);
  assert.equal(elements["btn-toggle-rewards"].textContent, "Hide Referral Rewards");

  elements["btn-toggle-rewards"]._trigger("click");
  assert.equal(elements["rewards-section"].classList.contains("hidden"), true);
  assert.equal(elements["btn-toggle-rewards"].textContent, "See Referral Rewards");
});

test("reward ladder markup lists the five static tiers and the monthly reset note, with no dynamic highlighting", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  assert.match(html, /10 qualified referrals.*\$10/);
  assert.match(html, /25 qualified referrals.*\$15/);
  assert.match(html, /50 qualified referrals.*\$50/);
  assert.match(html, /150 qualified referrals.*\$125/);
  assert.match(html, /250 qualified referrals.*\$250/);
  assert.match(html, /reset on the 1st of every month/i);
  assert.match(html, /Only qualified referrals count/i);
});

test("no weekly performance section or milestone bar remains on this page", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  assert.ok(!/THIS WEEK/i.test(html), "weekly performance section must be removed");
  assert.ok(!/progress-bar|progress-track|milestone/i.test(html), "no progress bar or milestone-tracking markup");
});

function resultsOkResponse(overrides) {
  return Promise.resolve({
    ok: true,
    status: 200,
    json: () =>
      Promise.resolve(
        Object.assign(
          {
            status: "ok",
            results: {
              total_referral_joins: 12,
              qualified_referrals: 7,
              pending_referrals: 5,
              revoked_referrals: 0,
              current_week_referrals: 2,
              current_week_qualified: 1,
              latest_generated_at: null,
              total_packages_generated: 0,
              next_reward_tier: { qualified_needed: 3, reward_amount: 10 },
            },
          },
          overrides || {}
        )
      ),
  });
}

test("compact stats section: shows Total Invited/Total Qualified counts and the this-month tier progress message", async () => {
  const { elements, fetchCalls } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/results")) return resultsOkResponse();
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();
  await flush();

  assert.equal(elements["stat-invited-value"].textContent, 12);
  assert.equal(elements["stat-qualified-value"].textContent, 7);
  assert.equal(elements["stat-tier-message"].textContent, "This Month: 3 more qualified referrals to unlock $10");
  assert.equal(elements["stat-tier-message"].classList.contains("hidden"), false);
  assert.equal(elements["stats-card"].classList.contains("hidden"), false);

  const resultsCall = fetchCalls.find((c) => c.url.includes("/api/creator/share/results"));
  assert.ok(resultsCall, "the results endpoint is called to populate the compact stats section");
});

test("stat labels are scope-qualified: Total Invited / Total Qualified are lifetime, distinct from the this-month tier message", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  assert.match(html, /Total Invited/);
  assert.match(html, /Total Qualified/);
  assert.doesNotMatch(html, />Invited</, "bare \"Invited\" label must not remain -- only \"Total Invited\"");
  assert.doesNotMatch(html, />Qualified</, "bare \"Qualified\" label must not remain -- only \"Total Qualified\"");
});

test("compact stats section: highest tier reached shows the this-month max-tier message", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/results")) return resultsOkResponse({ results: { total_referral_joins: 300, qualified_referrals: 300, next_reward_tier: null } });
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();
  await flush();

  assert.equal(elements["stat-tier-message"].textContent, "This Month: Highest reward tier reached");
});

test("compact stats section: failed results fetch hides the section without blocking Get My Share Post", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) return statusOkResponse();
      if (url.includes("/api/creator/share/results")) return Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve({ status: "error" }) });
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();
  await flush();

  assert.equal(elements["stats-card"].classList.contains("hidden"), true);
  assert.equal(elements["btn-generate"].classList.contains("hidden"), false, "Get My Share Post must remain usable");
});

test("no admin controls are present in the creator page markup", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  const forbidden = ["rsc-hooks", "rsc-playback", "bulk-import", "admin-dashboard", "Caption Hook", "Playback Pool"];
  forbidden.forEach((needle) => {
    assert.ok(!html.includes(needle), `creator-share.html must not contain admin markup: "${needle}"`);
  });
});

test("no blue primary buttons remain: accent color is AdvantPlay orange", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  assert.ok(!/#4f7dff/i.test(html), "old blue accent color must be fully removed");
  assert.ok(html.includes("--ap-orange: #FF5A00;"), "primary accent must be AdvantPlay orange");
  assert.ok(html.includes("--ap-orange-pressed: #E64F00;"), "pressed state must be the darker orange");
});

test("no guaranteed-income style claims are present in the copy", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  const banned = ["guaranteed income", "easy money", "passive income", "instant cash", "become rich", "financial freedom"];
  banned.forEach((phrase) => {
    assert.ok(!html.toLowerCase().includes(phrase), `must not use banned phrase: "${phrase}"`);
  });
});

test("layout stays within a narrow mobile viewport (360-430px)", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  assert.match(html, /name="viewport" content="width=device-width/);
  assert.match(html, /max-width:\s*430px/);
});

test("dynamic package fields are rendered via textContent, never innerHTML, so they are always HTML-escaped", () => {
  const source = loadScriptSource();
  assert.ok(source.includes("el.textContent = text;"));
  assert.ok(!/package(Caption|Playback|Cta|Link)\.innerHTML/.test(source));
});
