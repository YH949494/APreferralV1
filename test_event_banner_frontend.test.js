/**
 * Tests for the Event Banner (image-only, dynamic) Mini App top banner —
 * static/index.html's `eventBannerFeature` IIFE, backed by event_banner.py.
 *
 * The feature lives inline in static/index.html (no build step), so it is
 * extracted as source text and executed in a sandboxed vm context with
 * mocked DOM/fetch/Telegram globals, mirroring
 * test_referral_share_button.test.js's approach.
 *
 * Run with: node --test test_event_banner_frontend.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const HTML_PATH = path.join(__dirname, "static", "index.html");
const START_MARKER = "(function eventBannerFeature() {";
const END_MARKER = "\n    })();\n  </script>";

function readHtml() {
  return fs.readFileSync(HTML_PATH, "utf8");
}

function loadFeatureSource() {
  const html = readHtml();
  const start = html.indexOf(START_MARKER);
  const end = html.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "eventBannerFeature IIFE not found in static/index.html");
  assert.ok(end !== -1, "eventBannerFeature IIFE end marker not found in static/index.html");
  return html.slice(start, end + "\n    })();".length);
}

function makeBannerElement() {
  return {
    id: "ap-event-banner",
    style: { display: "none" },
    onclick: null,
    _ariaLabel: "",
    setAttribute(name, value) {
      if (name === "aria-label") this._ariaLabel = value;
    },
  };
}

function makeImgElement() {
  const el = {
    id: "ap-event-banner-img",
    alt: "",
    onload: null,
    onerror: null,
    _src: "",
    set src(value) {
      this._src = value;
    },
    get src() {
      return this._src;
    },
  };
  return el;
}

function makeContext({ fetchImpl, initData = "abc123", region = "Malaysia" } = {}) {
  const banner = makeBannerElement();
  const img = makeImgElement();
  const elements = { "ap-event-banner": banner, "ap-event-banner-img": img };

  const calls = { fetch: [], track: [], opened: [] };

  const trackingFetch = (url, opts) => {
    if (String(url).includes("/api/event-banner/track")) {
      calls.track.push(JSON.parse((opts && opts.body) || "{}"));
      return Promise.resolve({ ok: true, json: async () => ({ status: "ok" }) });
    }
    calls.fetch.push(url);
    return fetchImpl(url, opts);
  };

  const sandbox = {
    console: { log() {}, warn() {}, error() {}, info() {} },
    document: {
      getElementById: (id) => (Object.prototype.hasOwnProperty.call(elements, id) ? elements[id] : null),
    },
    window: {
      Telegram: { WebApp: { openLink: (url) => calls.opened.push(["openLink", url]) } },
    },
    localStorage: { getItem: (key) => (key === "region" ? region : null) },
    AbortController: class {
      constructor() {
        this.signal = {};
      }
      abort() {}
    },
    setTimeout: () => 1,
    clearTimeout: () => {},
    fetch: trackingFetch,
    API_BASE: "https://api.example.test",
    UI_VERSION: "test-ui-v1",
    tg: { initData },
    getLatestInitData: () => initData,
    waitForInitData: async () => initData,
    withInitDataInQuery: (p) => `${p}?init_data=${initData}`,
    openTelegramSafeLink: (url) => calls.opened.push(["telegram", url]),
  };

  const context = vm.createContext(sandbox);
  vm.runInContext(loadFeatureSource(), context, { filename: "index.html-extract-event-banner.js" });
  return { context, banner, img, calls };
}

function bannerResponse(overrides = {}) {
  return {
    ok: true,
    json: async () => ({
      status: "ok",
      banner: {
        event_id: "weekend_tournament_202608",
        image_url: "https://cdn.example.com/banner.webp",
        destination_url: "https://example.com/event",
        alt_text: "Weekend tournament event",
        ...overrides,
      },
    }),
  };
}

async function settle() {
  // Flush the microtask/timer queue enough times for the fetch -> json ->
  // DOM-update async chain inside loadEventBanner() to fully resolve.
  for (let i = 0; i < 6; i++) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

test("1. DOM order: banner markup sits above the Campaign Rewards hero card", () => {
  const html = readHtml();
  const bannerIdx = html.indexOf('id="ap-event-banner"');
  const rewardsIdx = html.indexOf('id="campaign-centre-root"');
  assert.ok(bannerIdx !== -1 && rewardsIdx !== -1);
  assert.ok(bannerIdx < rewardsIdx, "event banner must render before the Campaign Rewards section");
});

test("2. banner markup is image-only: no visible text, badge, CTA, or description", () => {
  const html = readHtml();
  const start = html.indexOf('<button type="button" id="ap-event-banner"');
  const end = html.indexOf("</button>", start) + "</button>".length;
  const markup = html.slice(start, end);
  // Only the <button> wrapper and a single <img> — no other tags/text nodes.
  const innerText = markup
    .replace(/<button[^>]*>/, "")
    .replace(/<img[^>]*>/, "")
    .replace("</button>", "")
    .trim();
  assert.equal(innerText, "", "banner must contain nothing but the image");
  assert.doesNotMatch(markup, /countdown|badge|cta|btn-primary/i);
});

test("3. eligible banner: full area becomes clickable and fires impression once on load", async () => {
  const { banner, img, calls } = makeContext({ fetchImpl: async () => bannerResponse() });
  await settle();

  assert.equal(banner.style.display, "none", "banner hidden until image confirms load");
  // Simulate the <img> finishing its network load.
  img.onload();
  assert.equal(banner.style.display, "block");
  img.onload(); // a second render/re-load must not double-count the impression
  assert.equal(calls.track.filter((t) => t.type === "impression").length, 1);
  assert.equal(calls.track[0].event_id, "weekend_tournament_202608");
  assert.equal(typeof banner.onclick, "function");
});

test("4. click fires analytics before navigating, using tg.openLink for https destinations", async () => {
  const { banner, calls } = makeContext({ fetchImpl: async () => bannerResponse() });
  await settle();

  banner.onclick();
  assert.equal(calls.track.filter((t) => t.type === "click").length, 1);
  assert.deepEqual(calls.opened[0], ["openLink", "https://example.com/event"]);
});

test("5. click still navigates when the analytics call fails outright", async () => {
  const fetchImpl = async () => bannerResponse();
  const { banner, calls, context } = makeContext({ fetchImpl });
  await settle();

  // Force the track fetch itself to reject to prove navigation is not gated on it.
  context.fetch = (url, opts) => {
    if (String(url).includes("/track")) return Promise.reject(new Error("network down"));
    return fetchImpl(url, opts);
  };

  assert.doesNotThrow(() => banner.onclick());
  assert.equal(calls.opened.length, 1, "navigation happens synchronously regardless of analytics outcome");
});

test("6. image load failure hides the entire banner and fires image_error", async () => {
  const { banner, img, calls } = makeContext({ fetchImpl: async () => bannerResponse() });
  await settle();
  img.onload();
  assert.equal(banner.style.display, "block");
  img.onerror();
  assert.equal(banner.style.display, "none");
  assert.equal(calls.track.filter((t) => t.type === "image_error").length, 1);
});

test("7. banner:null response leaves the banner hidden with no reserved spacing", async () => {
  const { banner } = makeContext({
    fetchImpl: async () => ({ ok: true, json: async () => ({ status: "ok", banner: null }) }),
  });
  await settle();
  assert.equal(banner.style.display, "none");
});

test("8. API failure fails silently — banner stays hidden, nothing throws", async () => {
  const { banner } = makeContext({ fetchImpl: async () => { throw new Error("boom"); } });
  await settle();
  assert.equal(banner.style.display, "none");
});

test("9. layout guard: container reserves space via aspect-ratio and never exceeds page width", () => {
  const html = readHtml();
  const cssStart = html.indexOf(".ap-event-banner {");
  const cssEnd = html.indexOf("}", cssStart);
  const css = html.slice(cssStart, cssEnd);
  assert.match(css, /width:\s*100%/);
  assert.match(css, /box-sizing:\s*border-box/);
  assert.match(css, /aspect-ratio:\s*16\s*\/\s*7/);
});
