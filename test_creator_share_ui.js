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
  "access-denied",
  "app-shell",
  "btn-generate",
  "generate-status",
  "package-card",
  "package-text",
  "btn-copy",
  "btn-generate-another",
  "btn-telegram-share",
  "copy-toast",
  "results",
  "stat-referrals",
  "stat-qualified",
];

function buildSandbox({ initData = "tg_init_data_ok", fetchImpl, clipboardWriteText, execCommandResult = true, openTelegramLink } = {}) {
  const elements = {};
  ELEMENT_IDS.forEach((id) => {
    elements[id] = makeElement(id);
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

  const telegramWebApp = {
    initData,
    ready() {},
    expand() {},
    openTelegramLink: openTelegramLink || (() => {}),
  };

  const navigatorStub = {
    clipboard:
      clipboardWriteText === undefined
        ? { writeText: (text) => Promise.resolve(clipboardWriteText === undefined ? undefined : clipboardWriteText).then(() => {}) }
        : clipboardWriteText === null
        ? undefined
        : { writeText: clipboardWriteText },
  };

  const sandbox = {
    window: { Telegram: { WebApp: telegramWebApp } },
    document: documentStub,
    fetch: wrappedFetch,
    navigator: navigatorStub,
    encodeURIComponent,
    setImmediate,
    console,
    execCommandResult,
  };
  sandbox.window.document = documentStub;
  sandbox.document.execCommand = () => execCommandResult;

  vm.createContext(sandbox);
  vm.runInContext(loadScriptSource(), sandbox);

  return { sandbox, elements, fetchCalls, createdTextareas };
}

test("Generate renders complete package", async () => {
  const shareText = "Hook line\nhttps://rx.apreplay.com/Abc123\n\nMore player replays and rewards inside AdvantPlay:\nhttps://t.me/+abc";
  const { elements, fetchCalls } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ status: "ok", creator: { user_id: 1, access: true, creator_tier: "pilot" } }),
        });
      }
      if (url.includes("/api/creator/share/results")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ status: "ok", results: { current_week_referrals: 3, current_week_qualified: 1 } }),
        });
      }
      if (url.includes("/api/creator/share/generate")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () =>
            Promise.resolve({
              status: "ok",
              package_id: "pkg_1",
              hook_text: "Hook line",
              playback_url: "https://rx.apreplay.com/Abc123",
              referral_link: "https://t.me/+abc",
              share_text: shareText,
            }),
        });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  assert.equal(elements["app-shell"].classList.contains("hidden"), false);

  elements["btn-generate"]._trigger("click");
  await flush();
  await flush();

  assert.equal(elements["package-text"].textContent, shareText);
  assert.equal(elements["package-card"].classList.contains("visible"), true);
  const generateCall = fetchCalls.find((c) => c.url.includes("/api/creator/share/generate"));
  assert.ok(generateCall, "generate endpoint was called");
  assert.equal(JSON.parse(generateCall.opts.body).platform, "generic");
});

test("Copy All copies exact share_text and copied endpoint called only after successful clipboard copy", async () => {
  const shareText = "Only hook\n\nMore player replays and rewards inside AdvantPlay:\nhttps://t.me/+xyz";
  let writeTextArg = null;
  const { elements, fetchCalls } = buildSandbox({
    clipboardWriteText: (text) => {
      writeTextArg = text;
      return Promise.resolve();
    },
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok", creator: { access: true } }) });
      }
      if (url.includes("/api/creator/share/generate")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () =>
            Promise.resolve({
              status: "ok",
              package_id: "pkg_42",
              hook_text: "Only hook",
              playback_url: null,
              referral_link: "https://t.me/+xyz",
              share_text: shareText,
            }),
        });
      }
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

  assert.equal(writeTextArg, shareText, "clipboard.writeText must receive the exact share_text");
  assert.equal(elements["copy-toast"].textContent, "Copied — paste it into WhatsApp, Facebook or X.");

  const copiedCall = fetchCalls.find((c) => c.url.includes("/copied"));
  assert.ok(copiedCall, "copied endpoint should be called after a successful clipboard copy");
  assert.ok(copiedCall.url.includes("pkg_42"));
});

test("copied endpoint is NOT called when clipboard copy fails", async () => {
  const { elements, fetchCalls } = buildSandbox({
    clipboardWriteText: () => Promise.reject(new Error("denied")),
    execCommandResult: false,
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok", creator: { access: true } }) });
      }
      if (url.includes("/api/creator/share/generate")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () =>
            Promise.resolve({
              status: "ok",
              package_id: "pkg_fail",
              hook_text: null,
              playback_url: null,
              referral_link: "https://t.me/+fail",
              share_text: "More player replays and rewards inside AdvantPlay:\nhttps://t.me/+fail",
            }),
        });
      }
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
});

test("clipboard fallback (execCommand) works when navigator.clipboard is unavailable", async () => {
  const { elements, fetchCalls, createdTextareas } = buildSandbox({
    clipboardWriteText: null, // simulates navigator.clipboard being undefined
    execCommandResult: true,
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok", creator: { access: true } }) });
      }
      if (url.includes("/api/creator/share/generate")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () =>
            Promise.resolve({
              status: "ok",
              package_id: "pkg_fallback",
              hook_text: null,
              playback_url: null,
              referral_link: "https://t.me/+fb",
              share_text: "More player replays and rewards inside AdvantPlay:\nhttps://t.me/+fb",
            }),
        });
      }
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

test("Generate Another disables the buttons during the request", async () => {
  let resolveGenerate;
  const generatePromise = new Promise((resolve) => {
    resolveGenerate = resolve;
  });

  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok", creator: { access: true } }) });
      }
      if (url.includes("/api/creator/share/generate")) {
        return generatePromise;
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  elements["btn-generate"]._trigger("click");
  await flush();

  assert.equal(elements["btn-generate"].disabled, true);
  assert.equal(elements["btn-generate-another"].disabled, true);

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

test("unauthorized response renders the access-denied state without admin controls", async () => {
  const { elements } = buildSandbox({
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({
          ok: false,
          status: 403,
          json: () => Promise.resolve({ status: "error", code: "creator_not_authorized" }),
        });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok" }) });
    },
  });

  await flush();
  await flush();

  assert.equal(elements["app-shell"].classList.contains("hidden"), true);
  assert.equal(elements["access-denied"].classList.contains("hidden"), false);
  assert.match(elements["access-denied"].textContent, /approved creators only/i);
});

test("Telegram share does not duplicate the referral URL between text and url params", async () => {
  let openedUrl = null;
  const shareText = "Hook\nhttps://rx.apreplay.com/Xyz\n\nMore player replays and rewards inside AdvantPlay:\nhttps://t.me/+onlyonce";
  const { elements, fetchCalls } = buildSandbox({
    openTelegramLink: (url) => {
      openedUrl = url;
    },
    fetchImpl: (url) => {
      if (url.includes("/api/creator/share/status")) {
        return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ status: "ok", creator: { access: true } }) });
      }
      if (url.includes("/api/creator/share/generate")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () =>
            Promise.resolve({
              status: "ok",
              package_id: "pkg_tg",
              hook_text: "Hook",
              playback_url: "https://rx.apreplay.com/Xyz",
              referral_link: "https://t.me/+onlyonce",
              share_text: shareText,
            }),
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
  assert.ok(!openedUrl.includes("&url="), "share URL must not carry a separate url= param");
  const occurrences = openedUrl.split("t.me%2F%2Bonlyonce").length - 1;
  assert.equal(occurrences, 1, "the referral link must appear exactly once in the composed share URL");

  const clickCall = fetchCalls.find((c) => c.url.includes("/share-clicked"));
  assert.ok(clickCall, "share-clicked should be recorded before/around opening the Telegram share action");
});

test("no admin controls are present in the creator page markup", () => {
  const html = fs.readFileSync(path.join(__dirname, "static", "creator-share.html"), "utf8");
  const forbidden = [
    "rsc-hooks",
    "rsc-playback",
    "bulk-import",
    "admin-dashboard",
    "Caption Hook",
    "Playback Pool",
  ];
  forbidden.forEach((needle) => {
    assert.ok(!html.includes(needle), `creator-share.html must not contain admin markup: "${needle}"`);
  });
});

test("dynamic package text is rendered via textContent, never innerHTML, so it is always HTML-escaped", () => {
  const source = loadScriptSource();
  assert.ok(source.includes("els.packageText.textContent = pkg.share_text"));
  assert.ok(!/packageText\.innerHTML/.test(source));
});
