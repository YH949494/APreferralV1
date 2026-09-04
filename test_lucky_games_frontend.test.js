/**
 * Tests for the Lucky Games card list — static/index.html's
 * `luckyGamesFeature` IIFE, backed by GET /api/lucky-games (lucky_games.py).
 *
 * The feature lives inline in static/index.html (no build step), so it is
 * extracted as source text and executed in a sandboxed vm context, mirroring
 * test_event_banner_frontend.test.js's approach.
 *
 * Run with: node --test test_lucky_games_frontend.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const HTML_PATH = path.join(__dirname, "static", "index.html");
const START_MARKER = "(function luckyGamesFeature() {";
const END_MARKER = "\n    })();\n  </script>";

function readHtml() {
  return fs.readFileSync(HTML_PATH, "utf8");
}

function loadFeatureSource() {
  const html = readHtml();
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "luckyGamesFeature IIFE not found in static/index.html");
  assert.ok(end !== -1, "luckyGamesFeature IIFE end marker not found in static/index.html");
  return html.slice(start, end + "\n    })();".length);
}

/** Minimal fake DOM element supporting exactly what luckyGamesFeature uses. */
class FakeElement {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this._attrs = {};
    this.style = { display: "" };
    this._listeners = {};
    this._src = "";
    this._text = "";
    this.className = "";
  }
  set innerHTML(value) {
    this._innerHTML = value;
    if (value === "") this.children = [];
  }
  get innerHTML() {
    return this._innerHTML || "";
  }
  set textContent(value) {
    this._text = value;
  }
  get textContent() {
    return this._text;
  }
  set src(value) {
    this._src = value;
  }
  get src() {
    return this._src;
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  setAttribute(name, value) {
    this._attrs[name] = value;
  }
  addEventListener(type, fn) {
    this._listeners[type] = fn;
  }
  trigger(type) {
    if (this._listeners[type]) this._listeners[type]();
  }
}

function makeContext({ fetchImpl } = {}) {
  const section = new FakeElement("div");
  const list = new FakeElement("div");
  const elements = { "lucky-games-section": section, "lucky-games-list": list };

  const calls = { opened: [] };

  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    document: {
      getElementById: (id) => (Object.prototype.hasOwnProperty.call(elements, id) ? elements[id] : null),
      createElement: (tag) => new FakeElement(tag),
      createElementNS: (_ns, tag) => new FakeElement(tag),
    },
    window: {
      Telegram: { WebApp: { openLink: (url) => calls.opened.push(["openLink", url]) } },
      open: (url) => calls.opened.push(["window.open", url]),
    },
    fetch: fetchImpl,
    API_BASE: "https://api.example.test",
    openTelegramSafeLink: (url) => calls.opened.push(["telegram", url]),
  };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFeatureSource(), context, { filename: "index.html-extract-lucky-games.js" });
  return { context, section, list, calls };
}

function gamesResponse(games) {
  return { ok: true, json: async () => ({ status: "ok", games }) };
}

async function settle() {
  for (let i = 0; i < 6; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

const SAMPLE_GAME = {
  id: "abc123",
  name: "Infinity Ocean",
  label: "Lucky Game",
  volatility: "High-Med",
  max_win: "25000x",
  image_url: "https://cdn.example.com/infinity-ocean.webp",
  game_url: "https://games.example.com/infinity-ocean",
  provider: "PG Soft",
};

test("1. section starts hidden and shows once published games load", async () => {
  const { section } = makeContext({ fetchImpl: async () => gamesResponse([SAMPLE_GAME]) });
  assert.equal(section.style.display, "");
  await settle();
  assert.equal(section.style.display, "block");
});

test("2. renders name, label, volatility and max win as plain text (no HTML injection)", async () => {
  const maliciousName = '<img src=x onerror="alert(1)">Infinity Ocean';
  const { list } = makeContext({
    fetchImpl: async () => gamesResponse([{ ...SAMPLE_GAME, name: maliciousName }]),
  });
  await settle();

  assert.equal(list.children.length, 1);
  const card = list.children[0];
  const [imgWrap, nameEl, labelEl, metaEl] = card.children;
  assert.equal(nameEl.textContent, maliciousName, "raw text must be set via textContent, never parsed as HTML");
  assert.equal(labelEl.textContent, "Lucky Game");
  assert.equal(metaEl.textContent, "High-Med · Max 25000x");
  assert.equal(imgWrap.children[0].src, SAMPLE_GAME.image_url);
});

test("3. source guard: dynamic fields are set via textContent, never innerHTML", () => {
  const src = loadFeatureSource();
  assert.doesNotMatch(src, /\.innerHTML\s*=\s*`/, "no template-literal innerHTML assembly of API values");
  assert.match(src, /nameEl\.textContent = game\.name/);
});

test("4. missing image_url shows the fallback icon directly, no <img> element", async () => {
  const { list } = makeContext({
    fetchImpl: async () => gamesResponse([{ ...SAMPLE_GAME, image_url: "" }]),
  });
  await settle();
  const card = list.children[0];
  const imgWrap = card.children[0];
  assert.equal(imgWrap.children.length, 1);
  assert.equal(imgWrap.children[0].tag, "svg");
});

test("5. image load failure falls back to the icon instead of a broken image", async () => {
  const { list } = makeContext({ fetchImpl: async () => gamesResponse([SAMPLE_GAME]) });
  await settle();
  const card = list.children[0];
  const imgWrap = card.children[0];
  const img = imgWrap.children[0];
  assert.equal(img.tag, "img");

  img.onerror();
  assert.equal(imgWrap.children.length, 1);
  assert.equal(imgWrap.children[0].tag, "svg");
});

test("6. clicking a card with a plain https game_url opens it via WebApp.openLink", async () => {
  const { list, calls } = makeContext({ fetchImpl: async () => gamesResponse([SAMPLE_GAME]) });
  await settle();
  const card = list.children[0];
  card.trigger("click");
  assert.deepEqual(calls.opened[0], ["openLink", SAMPLE_GAME.game_url]);
});

test("7. clicking a t.me game_url routes through openTelegramSafeLink, https through WebApp.openLink otherwise", async () => {
  const { list, calls } = makeContext({
    fetchImpl: async () => gamesResponse([{ ...SAMPLE_GAME, game_url: "https://t.me/somegame" }]),
  });
  await settle();
  list.children[0].trigger("click");
  assert.deepEqual(calls.opened[0], ["telegram", "https://t.me/somegame"]);
});

test("8. a card with no game_url is not clickable (no click listener attached)", async () => {
  const { list } = makeContext({
    fetchImpl: async () => gamesResponse([{ ...SAMPLE_GAME, game_url: "" }]),
  });
  await settle();
  const card = list.children[0];
  assert.equal(typeof card._listeners.click, "undefined");
});

test("9. empty published-game list leaves the section hidden", async () => {
  const { section, list } = makeContext({ fetchImpl: async () => gamesResponse([]) });
  await settle();
  assert.equal(section.style.display, "none");
  assert.equal(list.children.length, 0);
});

test("10. API failure fails silently — section stays hidden, nothing throws", async () => {
  const { section } = makeContext({ fetchImpl: async () => { throw new Error("boom"); } });
  await settle();
  assert.equal(section.style.display, "none");
});

test("11. non-OK HTTP response hides the section instead of throwing", async () => {
  const { section } = makeContext({ fetchImpl: async () => ({ ok: false, status: 500, json: async () => ({}) }) });
  await settle();
  assert.equal(section.style.display, "none");
});

test("12. games missing a name are filtered out of the render", async () => {
  const { list, section } = makeContext({
    fetchImpl: async () => gamesResponse([{ ...SAMPLE_GAME, name: "" }, SAMPLE_GAME]),
  });
  await settle();
  assert.equal(list.children.length, 1);
  assert.equal(section.style.display, "block");
});
