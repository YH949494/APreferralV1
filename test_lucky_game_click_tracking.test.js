/**
 * Tests for the Lucky Game card's click/impression tracking (static/index.html)
 * — making the existing daily-pick tile (#daily-game-section) clickable to
 * https://advantplay.com/our-games.html with UTM params, plus best-effort,
 * non-blocking, attribution-correct analytics.
 *
 * Two layers:
 *  - Structural/text assertions against the shipped markup for things that
 *    are inherently declarative (onclick/role/tabindex wiring, the literal
 *    destination URL), same style as test_lucky_games_frontend.test.js.
 *  - Real behavioral tests: the tracking/render/cache-revalidation functions
 *    are extracted from static/index.html and executed in a sandboxed vm
 *    context with a minimal fake DOM/localStorage/fetch, mirroring
 *    test_platform_finder_modal.test.js's approach. This is what actually
 *    proves the cache-revalidation-before-tracking behavior (never
 *    misattributing an event to the wrong game), not just that certain
 *    tokens appear in the source.
 *
 * Run with: node --test test_lucky_game_click_tracking.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const HTML_PATH = path.join(__dirname, "static", "index.html");
const START_MARKER = "    const LUCKY_GAME_DESTINATION_URL =";
const END_MARKER = "\n    function fmtKL(iso) {";
const CACHE_PREFIX = "miniapp_daily_game_v1_";

function readHtml() {
  return fs.readFileSync(HTML_PATH, "utf8");
}

function loadFeatureSource() {
  const html = readHtml();
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "LUCKY_GAME_DESTINATION_URL start marker not found in static/index.html");
  assert.ok(end !== -1, "fmtKL end marker not found in static/index.html");
  return html.slice(start, end);
}

// ---------------------------------------------------------------------------
// Structural/markup checks
// ---------------------------------------------------------------------------

test("15. the card is activatable (onclick handler on #daily-game-section)", () => {
  const html = readHtml();
  const tileMatch = html.match(/<div id="daily-game-section"[^>]*>/);
  assert.ok(tileMatch, "expected to find the #daily-game-section opening tag");
  assert.match(tileMatch[0], /onclick="handleLuckyGameClick\(\)"/, "tile must call handleLuckyGameClick() on click");
});

test("20. Enter/Space keyboard activation is wired on the card", () => {
  const html = readHtml();
  const tileMatch = html.match(/<div id="daily-game-section"[^>]*>/);
  assert.ok(tileMatch, "expected to find the #daily-game-section opening tag");
  assert.match(tileMatch[0], /role="button"/, "tile must expose role=button for a11y");
  assert.match(tileMatch[0], /tabindex="0"/, "tile must be focusable for keyboard use");
  assert.match(tileMatch[0], /onkeydown="handleLuckyGameKeydown\(event\)"/, "tile must handle keydown");
  assert.match(
    html,
    /function handleLuckyGameKeydown\(event\)\s*\{[\s\S]{0,200}Enter[\s\S]{0,200}handleLuckyGameClick\(\)/,
    "keydown handler must activate on Enter/Space by calling handleLuckyGameClick()"
  );
});

test("18. destination URL carries the required UTM parameters", () => {
  const html = readHtml();
  assert.match(
    html,
    /LUCKY_GAME_DESTINATION_URL\s*=\s*\n?\s*"https:\/\/advantplay\.com\/our-games\.html\?utm_source=telegram&utm_medium=miniapp&utm_campaign=lucky_game"/,
    "destination URL must be the AdvantPlay games page tagged for miniapp/lucky_game attribution"
  );
});

test("22. Lucky Game tile content markup is unchanged", () => {
  const html = readHtml();
  assert.match(html, /id="daily-game-content"/, "content slot must still exist");
  assert.match(
    html,
    /contentEl\.innerHTML = `<div style="font-size:18px;font-weight:700;">\$\{name\}<\/div>\$\{tag\}\$\{maxwin\}\$\{reason\}`;/,
    "rendered card markup must be unchanged"
  );
});

// ---------------------------------------------------------------------------
// Behavioral tests (sandboxed vm execution of the real functions)
// ---------------------------------------------------------------------------

function makeSimpleElement() {
  return {
    _textContent: "",
    _innerHTML: "",
    style: {},
    get textContent() { return this._textContent; },
    set textContent(v) { this._textContent = v; this._innerHTML = v; },
    get innerHTML() { return this._innerHTML; },
    set innerHTML(v) { this._innerHTML = v; },
  };
}

function makeLocalStorage() {
  const store = new Map();
  return {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => { store.set(k, String(v)); },
    removeItem: (k) => store.delete(k),
  };
}

// Creates a fresh sandboxed context with the Lucky Game tracking code
// extracted from static/index.html. Returns handles to observe every
// externally-visible effect (DOM writes, localStorage, fetch/tracking
// calls, window.open/openLink navigation) plus a way to control what the
// mocked GET /v2/miniapp/daily-game returns.
function makeContext() {
  const calls = { tracked: [], opened: [], dailyGameFetches: 0 };
  const elements = {
    "daily-game-section": makeSimpleElement(),
    "daily-game-content": makeSimpleElement(),
  };
  const localStorage = makeLocalStorage();
  let dailyGameHandler = async () => ({
    ok: true,
    json: async () => ({
      ok: true,
      date_kl: "1970-01-01",
      slot: { id: "unset", name: "Unset" },
      tracking_key: "daily_game:1970-01-01:unset",
    }),
  });

  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    document: { getElementById: (id) => elements[id] || null },
    window: {},
    localStorage,
    setTimeout,
    clearTimeout,
    API_BASE: "https://api.example.test",
    API_V2: "https://api.example.test/v2",
    DAILY_GAME_CACHE_PREFIX: CACHE_PREFIX,
    escapeHtml: (s) => String(s),
    getLatestInitData: () => "",
    v2Fetch: async (url, opts) => {
      calls.dailyGameFetches++;
      return dailyGameHandler(url, opts);
    },
    fetch: (url, opts) => {
      calls.tracked.push({ url: String(url), body: opts && opts.body ? JSON.parse(opts.body) : {} });
      return Promise.resolve({ ok: true, json: async () => ({ success: true }) });
    },
  };
  sandbox.window.open = (url) => { calls.opened.push(url); };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFeatureSource(), context, { filename: "index.html-extract-lucky-game.js" });

  return {
    context,
    calls,
    elements,
    localStorage,
    setDailyGameHandler(fn) { dailyGameHandler = fn; },
  };
}

function seedCache(localStorage, dateKl, { slot, trackingKey, legacy = false }) {
  const entry = legacy ? { date_kl: dateKl, slot } : { date_kl: dateKl, slot, tracking_key: trackingKey };
  localStorage.setItem(`${CACHE_PREFIX}${dateKl}`, JSON.stringify(entry));
}

function okDailyGame({ dateKl, slot, trackingKey }) {
  return async () => ({
    ok: true,
    json: async () => ({ ok: true, date_kl: dateKl, slot, tracking_key: trackingKey }),
  });
}

test("9. handleLuckyGameClick() opens via Telegram.WebApp.openLink when available", () => {
  const { context, calls } = makeContext();
  let opened = null;
  context.window.Telegram = { WebApp: { openLink: (url) => { opened = url; } } };
  context.handleLuckyGameClick();
  assert.ok(opened, "openLink should have been called");
  assert.ok(opened.includes("advantplay.com/our-games.html"));
  assert.equal(calls.opened.length, 0, "window.open fallback must not fire when openLink succeeded");
});

test("9. falls back to window.open when Telegram.WebApp.openLink is unavailable", () => {
  const { context, calls } = makeContext();
  context.handleLuckyGameClick();
  assert.equal(calls.opened.length, 1);
  assert.ok(calls.opened[0].includes("advantplay.com/our-games.html"));
});

test("21. one physical interaction cannot double-fire (re-entrancy guard)", () => {
  const { context, calls } = makeContext();
  context.handleLuckyGameClick();
  context.handleLuckyGameClick();
  assert.equal(calls.opened.length, 1, "a second immediate call must be a no-op");
});

test("19. navigation happens even with no confirmed tracking_key yet, but no click event is sent", () => {
  const { context, calls } = makeContext();
  context.handleLuckyGameClick();
  assert.equal(calls.opened.length, 1, "destination must still open");
  assert.equal(calls.tracked.length, 0, "click must be skipped, never sent without a confirmed tracking_key");
});

test("1. cached tracking_key matching the server's -> renders it, emits one confirmed impression, click tracks it", async () => {
  const { context, calls, localStorage, elements, setDailyGameHandler } = makeContext();
  const dateKl = context.getCurrentKlDateString();
  const slot = { id: "g1", name: "Fighting Bull" };
  const trackingKey = `daily_game:${dateKl}:g1`;
  seedCache(localStorage, dateKl, { slot, trackingKey });
  setDailyGameHandler(okDailyGame({ dateKl, slot, trackingKey }));

  await context.loadDailyGame();

  assert.match(elements["daily-game-content"].innerHTML, /Fighting Bull/);
  assert.equal(calls.tracked.length, 1, "exactly one impression");
  assert.equal(calls.tracked[0].body.event, "impression");
  assert.equal(calls.tracked[0].body.tracking_key, trackingKey);

  context.handleLuckyGameClick();
  assert.equal(calls.tracked.length, 2);
  assert.equal(calls.tracked[1].body.event, "click");
  assert.equal(calls.tracked[1].body.tracking_key, trackingKey);
});

test("2. server reselection replaces a stale cached game -> renders + tracks only the new game", async () => {
  const { context, calls, localStorage, elements, setDailyGameHandler } = makeContext();
  const dateKl = context.getCurrentKlDateString();
  const trackingKeyA = `daily_game:${dateKl}:gameA`;
  const trackingKeyB = `daily_game:${dateKl}:gameB`;
  const slotB = { id: "gameB", name: "Golden Empire" };
  seedCache(localStorage, dateKl, { slot: { id: "gameA", name: "Fighting Bull" }, trackingKey: trackingKeyA });
  setDailyGameHandler(okDailyGame({ dateKl, slot: slotB, trackingKey: trackingKeyB }));

  await context.loadDailyGame();

  assert.match(elements["daily-game-content"].innerHTML, /Golden Empire/);
  assert.doesNotMatch(elements["daily-game-content"].innerHTML, /Fighting Bull/);
  assert.equal(calls.tracked.length, 1, "only the new (canonical) game's impression is sent");
  assert.equal(calls.tracked[0].body.tracking_key, trackingKeyB);

  context.handleLuckyGameClick();
  assert.equal(calls.tracked[1].body.tracking_key, trackingKeyB, "click must track the new game, never the stale cached one");

  const stored = JSON.parse(localStorage.getItem(`${CACHE_PREFIX}${dateKl}`));
  assert.equal(stored.tracking_key, trackingKeyB);
  assert.equal(stored.slot.id, "gameB");
});

test("3. legacy cache without tracking_key is revalidated, never invents a tracking_key client-side", async () => {
  const { context, calls, localStorage, setDailyGameHandler } = makeContext();
  const dateKl = context.getCurrentKlDateString();
  const slot = { id: "g1", name: "Fighting Bull" };
  const trackingKey = `daily_game:${dateKl}:g1`;
  seedCache(localStorage, dateKl, { slot, legacy: true }); // no tracking_key field at all
  setDailyGameHandler(okDailyGame({ dateKl, slot, trackingKey }));

  await context.loadDailyGame();

  assert.equal(calls.tracked.length, 1);
  assert.equal(calls.tracked[0].body.tracking_key, trackingKey, "must use the server-confirmed key, not fabricate one");
});

test("4. daily-game revalidation failure falls back to cached display without misattributed tracking", async () => {
  const { context, calls, localStorage, elements, setDailyGameHandler } = makeContext();
  const dateKl = context.getCurrentKlDateString();
  const slot = { id: "g1", name: "Fighting Bull" };
  seedCache(localStorage, dateKl, { slot, trackingKey: `daily_game:${dateKl}:g1` });
  setDailyGameHandler(async () => { throw new Error("network down"); });

  await context.loadDailyGame();

  assert.match(elements["daily-game-content"].innerHTML, /Fighting Bull/, "cached card stays displayed");
  assert.notEqual(elements["daily-game-section"].style.display, "none");
  assert.equal(calls.tracked.length, 0, "no impression may be sent for an unrevalidated cached game");

  context.handleLuckyGameClick();
  assert.equal(calls.opened.length, 1, "the destination must still open");
  assert.equal(calls.tracked.length, 0, "click must also be skipped rather than risk misattribution");
});

test("daily-game failure with no prior cache hides the section (existing behavior preserved)", async () => {
  const { context, elements, setDailyGameHandler } = makeContext();
  setDailyGameHandler(async () => { throw new Error("network down"); });

  await context.loadDailyGame();

  assert.equal(elements["daily-game-section"].style.display, "none");
});

test("16. no impression when the daily-game response is ok:false or missing a tracking_key", async () => {
  const { context, calls, setDailyGameHandler } = makeContext();
  setDailyGameHandler(async () => ({ ok: true, json: async () => ({ ok: false }) }));

  await context.loadDailyGame();

  assert.equal(calls.tracked.length, 0);
});
