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
    get textContent() {
      return this._text;
    },
    set textContent(v) {
      this._text = v;
    },
    addEventListener(event, handler) {
      listeners[event] = listeners[event] || [];
      listeners[event].push(handler);
    },
    _trigger(event) {
      (listeners[event] || []).forEach((h) => h({ target: this }));
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
  "package-link",
  "btn-copy",
  "btn-try-another",
  "btn-telegram-share",
  "copy-toast",
  "share-status",
  "btn-toggle-rewards",
  "rewards-section",
];

// Mirrors the `class="... hidden"` markup already present in
// static/creator-share.html, since the lightweight element stubs below don't
// parse the real HTML/CSS -- only classes the inline script itself toggles.
const INITIALLY_HIDDEN_IDS = ["access-denied", "app-shell", "package-card", "package-caption", "package-playback", "rewards-section"];

function buildSandbox({ initData = "tg_init_data_ok", fetchImpl, clipboardWriteText, execCommandResult = true, openTelegramLink, windowOpenResult } = {}) {
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
      open: (url, target) => {
        windowOpenCalls.push(url);
        return windowOpenResult === undefined ? {} : windowOpenResult;
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
              "Hook line\nhttps://rx.apreplay.com/Abc123\n\nMore player replays and rewards inside AdvantPlay:\nhttps://t.me/+abc",
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
  assert.equal(elements["package-cta"].textContent, "More player replays and rewards inside AdvantPlay:");
  assert.equal(elements["package-link"].textContent, "https://t.me/+abc");
  assert.equal(elements["package-card"].classList.contains("hidden"), false);
  assert.equal(elements["btn-generate"].classList.contains("hidden"), true, "Get My Share Post is replaced once a post exists");

  const generateCall = fetchCalls.find((c) => c.url.includes("/api/creator/share/generate"));
  assert.ok(generateCall, "generate endpoint was called");
  assert.equal(JSON.parse(generateCall.opts.body).platform, "generic");
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
          share_text: "More player replays and rewards inside AdvantPlay:\nhttps://t.me/+bare",
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
  const shareText = "Only hook\n\nMore player replays and rewards inside AdvantPlay:\nhttps://t.me/+xyz";
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

  elements["btn-copy"]._trigger("click");
  await flush();
  await flush();

  assert.equal(createdTextareas.length, 1, "fallback path must create a temporary textarea");
  const copiedCall = fetchCalls.find((c) => c.url.includes("/copied"));
  assert.ok(copiedCall, "fallback copy success should still record the copied event");
});

test("Telegram share success clears any prior error and records the share-clicked event", async () => {
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
  assert.ok(!openedUrl.includes("&url="), "share URL must not carry a separate url= param");
  const occurrences = openedUrl.split("t.me%2F%2Babc").length - 1;
  assert.equal(occurrences, 1, "the referral link must appear exactly once in the composed share URL");
  assert.equal(elements["share-status"].textContent, "");

  const clickCall = fetchCalls.find((c) => c.url.includes("/share-clicked"));
  assert.ok(clickCall, "share-clicked should be recorded before/around opening the Telegram share action");
});

test("Telegram share failure (no WebApp bridge, popup blocked) shows an inline error", async () => {
  const { elements } = buildSandbox({
    openTelegramLink: undefined,
    windowOpenResult: null, // simulates a blocked popup
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
        share_text: "More player replays and rewards inside AdvantPlay:\nhttps://t.me/+x",
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

test("no monthly progress card, milestone bar, or results-endpoint call remains on this page", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  const source = loadScriptSource();
  assert.ok(!/THIS WEEK/i.test(html), "weekly performance section must be removed");
  assert.ok(!/THIS MONTH/i.test(html), "no monthly progress card may be added on this page");
  assert.ok(!source.includes("/api/creator/share/results"), "this page must not call the results/leaderboard endpoint");
  assert.ok(!/progress-bar|progress-track|milestone/i.test(html), "no progress bar or milestone-tracking markup");
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
