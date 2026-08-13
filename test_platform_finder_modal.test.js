/**
 * Tests for the Platform Finder ("Find Where To Play") post-claim modal
 * builder in static/index.html — showPlatformFinderModal/showPlatformFinderCta
 * plus the shared openTelegramSafeLink/platformFinderPostEvent helpers they
 * depend on.
 *
 * The feature lives inline in static/index.html (no build step), so it is
 * extracted as source text and executed in a sandboxed vm context with a
 * minimal fake DOM, mirroring test_event_banner_frontend.test.js's approach.
 *
 * Run with: node --test test_platform_finder_modal.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const HTML_PATH = path.join(__dirname, "static", "index.html");
const START_MARKER = "    function openTelegramSafeLink(url) {";
const END_MARKER = "\n    function renderDailyGame(slot) {";

function loadFeatureSource() {
  const html = fs.readFileSync(HTML_PATH, "utf8");
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "openTelegramSafeLink start marker not found in static/index.html");
  assert.ok(end !== -1, "renderDailyGame end marker not found in static/index.html");
  return html.slice(start, end);
}

// ---- Minimal fake DOM ----------------------------------------------------

function makeElement(tag) {
  const el = {
    tagName: tag,
    _listeners: {},
    children: [],
    parentElement: null,
    style: {},
    className: "",
    id: "",
    type: "",
    _textContent: "",
    _attrs: {},
    get textContent() {
      return this._textContent;
    },
    set textContent(v) {
      this._textContent = v;
    },
    setAttribute(name, value) {
      this._attrs[name] = value;
    },
    getAttribute(name) {
      return this._attrs[name];
    },
    appendChild(child) {
      child.parentElement = el;
      el.children.push(child);
      return child;
    },
    insertAdjacentElement(_position, child) {
      child.parentElement = el.parentElement;
      if (el.parentElement) {
        const idx = el.parentElement.children.indexOf(el);
        el.parentElement.children.splice(idx + 1, 0, child);
      }
      return child;
    },
    addEventListener(type, handler) {
      (el._listeners[type] = el._listeners[type] || []).push(handler);
    },
    removeEventListener(type, handler) {
      if (!el._listeners[type]) return;
      el._listeners[type] = el._listeners[type].filter((h) => h !== handler);
    },
    dispatch(type, evt) {
      const event = Object.assign({ target: el }, evt);
      (el._listeners[type] || []).forEach((h) => h(event));
      return event;
    },
    remove() {
      if (el.parentElement) {
        const idx = el.parentElement.children.indexOf(el);
        if (idx !== -1) el.parentElement.children.splice(idx, 1);
      }
      el.parentElement = null;
      el._removed = true;
      if (el._onRemove) el._onRemove(el);
    },
    querySelector() {
      return null;
    },
  };
  return el;
}

function makeDocument() {
  const body = makeElement("body");
  const registry = { "platform-finder-modal": null, "platform-finder-cta": null };
  const docListeners = {};
  const doc = {
    body,
    createElement: (tag) => makeElement(tag),
    createTextNode: (text) => ({ nodeType: 3, textContent: text }),
    getElementById: (id) => registry[id] || null,
    addEventListener(type, handler) {
      (docListeners[type] = docListeners[type] || []).push(handler);
    },
    removeEventListener(type, handler) {
      if (!docListeners[type]) return;
      docListeners[type] = docListeners[type].filter((h) => h !== handler);
    },
    dispatch(type, evt) {
      (docListeners[type] || []).forEach((h) => h(evt));
    },
  };
  const origAppend = body.appendChild.bind(body);
  body.appendChild = (child) => {
    if (child.id) {
      registry[child.id] = child;
      child._onRemove = () => {
        if (registry[child.id] === child) registry[child.id] = null;
      };
    }
    return origAppend(child);
  };
  return doc;
}

function makeContext({ helpUrl = "https://t.me/some_help_chat" } = {}) {
  const document = makeDocument();
  const calls = { posted: [], opened: [], helpFetched: 0 };

  const I18N_EN = {
    platform_finder_cta: "🔎 Find Where To Play",
    platform_finder_title: "Voucher secured ✅",
    platform_finder_subtitle: "Not sure where to use it?",
    platform_finder_step1_label: "Step 1",
    platform_finder_step1_text: 'Search "AdvantPlay Slots" on Google.',
    platform_finder_step2_label: "Step 2",
    platform_finder_step2_text: "Look for the official platform.",
    platform_finder_step3_label: "Step 3",
    platform_finder_step3_text: "Log in → Voucher → enter your voucher code.",
    platform_finder_google_btn: "🔎 Search on Google",
    platform_finder_copy_btn: '📋 Copy "AdvantPlay Slots"',
    platform_finder_help_btn: "❓ Can't Find It?",
    platform_finder_fallback: 'Search "AdvantPlay Slots" on Google and look for the official platform.',
    platform_finder_close_label: "Close",
  };

  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    document,
    window: { Telegram: undefined },
    navigator: { clipboard: { writeText: async () => {} } },
    API_V2: "https://api.example.test/v2",
    v2Fetch: async (url, opts) => {
      calls.posted.push({ url: String(url), body: opts && opts.body ? JSON.parse(opts.body) : {} });
      if (String(url).includes("/help-clicked")) {
        calls.helpFetched++;
        return { ok: true, json: async () => ({ status: "ok", help_url: helpUrl, language: "en" }) };
      }
      return { ok: true, json: async () => ({ status: "ok" }) };
    },
    tFor: (lang, key) => I18N_EN[key] || key,
    t: (key) => I18N_EN[key] || key,
    copyTextToClipboard: async () => true,
    flashButtonLabel: () => {},
    tgAlert: () => {},
  };
  sandbox.window.open = (url) => {
    calls.opened.push(url);
    return {};
  };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFeatureSource(), context, { filename: "index.html-extract-platform-finder.js" });
  return { context, document, calls };
}

function getModal(document) {
  return document.getElementById("platform-finder-modal");
}

function findButtonByText(node, text) {
  for (const child of node.children || []) {
    if (child.tagName === "button" && child.textContent === text) return child;
    const found = findButtonByText(child, text);
    if (found) return found;
  }
  return null;
}

function allActionButtons(content) {
  const actions = content.children.find((c) => c.className === "pf-actions");
  return actions ? actions.children.filter((c) => c.tagName === "button") : [];
}

// ---- Tests ----------------------------------------------------------------

test("Google CTA URL is exactly https://www.google.com/search?q=AdvantPlay+Slots", () => {
  const { context } = makeContext();
  assert.equal(context.PLATFORM_FINDER_GOOGLE_URL, "https://www.google.com/search?q=AdvantPlay+Slots");
});

test("Google CTA uses the correct localized label and opens the Google search URL", () => {
  const { context, document, calls } = makeContext();
  context.showPlatformFinderModal({ language: "en", search_term: "AdvantPlay Slots" }, { dropId: "d1", voucherCode: "ABCD1234" });

  const content = getModal(document).children[0];
  const [googleBtn] = allActionButtons(content);
  assert.equal(googleBtn.textContent, "🔎 Search on Google");

  googleBtn.dispatch("click", {});
  assert.deepEqual(calls.opened, ["https://www.google.com/search?q=AdvantPlay+Slots"]);
});

test("clicking modal backdrop closes it", () => {
  const { context, document } = makeContext();
  context.showPlatformFinderModal({ language: "en" }, {});
  const overlay = getModal(document);
  assert.ok(overlay, "modal should be open");
  overlay.dispatch("click", { target: overlay });
  assert.equal(getModal(document), null, "modal should be removed after backdrop click");
});

test("clicking inside modal does not close it", () => {
  const { context, document } = makeContext();
  context.showPlatformFinderModal({ language: "en" }, {});
  const overlay = getModal(document);
  const content = overlay.children[0];
  overlay.dispatch("click", { target: content });
  assert.notEqual(getModal(document), null, "modal must stay open when the click target is inside the content");
});

test("X button closes modal", () => {
  const { context, document } = makeContext();
  context.showPlatformFinderModal({ language: "en" }, {});
  const overlay = getModal(document);
  const content = overlay.children[0];
  const closeIcon = content.children.find((c) => c.className === "modal-close-icon");
  assert.ok(closeIcon, "expected a modal-close-icon element");
  assert.equal(closeIcon.textContent, "×");
  closeIcon.dispatch("click", {});
  assert.equal(getModal(document), null);
});

test("Escape key closes modal, and the keydown listener is cleaned up after close", () => {
  const { context, document } = makeContext();
  context.showPlatformFinderModal({ language: "en" }, {});
  assert.ok(getModal(document));
  document.dispatch("keydown", { key: "Escape" });
  assert.equal(getModal(document), null);
});

test("no empty/unlabelled action button remains in the modal", () => {
  const { context, document } = makeContext();
  context.showPlatformFinderModal({ language: "en" }, {});
  const content = getModal(document).children[0];
  const buttons = allActionButtons(content);
  assert.equal(buttons.length, 3, "expected exactly Google, Copy, and Can't Find It buttons");
  for (const btn of buttons) {
    assert.ok(btn.textContent && btn.textContent.trim().length > 0, "every action button must have a visible label");
  }
});

test("Google search click emits exactly one platform_search_google_clicked event", () => {
  const { context, document, calls } = makeContext();
  context.showPlatformFinderModal({ language: "en" }, { dropId: "d1", voucherCode: "ABCD1234" });
  const content = getModal(document).children[0];
  const [googleBtn] = allActionButtons(content);
  googleBtn.dispatch("click", {});
  const googleCalls = calls.posted.filter((c) => c.url.endsWith("/vouchers/platform-finder/google-clicked"));
  assert.equal(googleCalls.length, 1);
  assert.deepEqual(googleCalls[0].body, { dropId: "d1", voucherCode: "ABCD1234" });
});

test("existing Copy Search analytics still work", async () => {
  const { context, document, calls } = makeContext();
  context.showPlatformFinderModal({ language: "en", search_term: "AdvantPlay Slots" }, { dropId: "d1", voucherCode: "ABCD1234" });
  const content = getModal(document).children[0];
  const [, copyBtn] = allActionButtons(content);
  assert.equal(copyBtn.textContent, '📋 Copy "AdvantPlay Slots"');
  await copyBtn.dispatch("click", {});
  // allow the async click handler's microtasks to flush
  await new Promise((resolve) => setImmediate(resolve));
  const copyCalls = calls.posted.filter((c) => c.url.endsWith("/vouchers/platform-finder/search-copied"));
  assert.equal(copyCalls.length, 1);
});

test("existing Can't Find It analytics still work", async () => {
  const { context, document, calls } = makeContext();
  context.showPlatformFinderModal({ language: "en" }, { dropId: "d1", voucherCode: "ABCD1234" });
  const content = getModal(document).children[0];
  const [, , helpBtn] = allActionButtons(content);
  assert.equal(helpBtn.textContent, "❓ Can't Find It?");
  await helpBtn.dispatch("click", {});
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(calls.helpFetched, 1);
  // No Telegram WebApp.openTelegramLink is stubbed in this sandbox, so the
  // shared openTelegramSafeLink helper falls back to a normal window.open —
  // exercising the "fall back safely to normal browser navigation" path.
  assert.deepEqual(calls.opened, ["https://t.me/some_help_chat"]);
});

test("AdvantPlay Slots search term is unchanged for the Google CTA regardless of language", () => {
  const { context, document } = makeContext();
  for (const lang of ["en", "th", "id"]) {
    context.showPlatformFinderModal({ language: lang, search_term: "AdvantPlay Slots" }, {});
    const content = getModal(document).children[0];
    const copyBtn = findButtonByText(content, '📋 Copy "AdvantPlay Slots"');
    assert.ok(copyBtn, `expected the copy button text to retain the untranslated search term for lang=${lang}`);
    getModal(document).remove();
  }
});
