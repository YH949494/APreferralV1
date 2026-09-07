/**
 * Regression tests for removing the duplicate "🎰 Lucky Games" catalogue
 * section from the user-facing Mini App (static/index.html).
 *
 * Product decision: only ONE Lucky Game surface per day — the existing
 * daily tile (#daily-game-section, sourced from /v2/miniapp/daily-game).
 * The bottom multi-card catalogue (#lucky-games-section, fed by
 * GET /api/lucky-games) is removed from the Mini App entirely. The
 * lucky_games MongoDB catalogue, GET /api/lucky-games, and the Admin
 * Dashboard's catalogue UI are all untouched — only the fetch/render code
 * for the bottom section is gone.
 *
 * Run with: node --test test_lucky_games_frontend.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const HTML_PATH = path.join(__dirname, "static", "index.html");

function readHtml() {
  return fs.readFileSync(HTML_PATH, "utf8");
}

test("1. Mini App renders only one Lucky Game section (the daily tile)", () => {
  const html = readHtml();
  const dailyTileMatches = html.match(/id="daily-game-section"/g) || [];
  assert.equal(dailyTileMatches.length, 1, "expected exactly one #daily-game-section tile");
});

test("2. bottom #lucky-games-section catalogue markup is no longer rendered", () => {
  const html = readHtml();
  assert.doesNotMatch(html, /id="lucky-games-section"/, "catalogue section div must be removed");
  assert.doesNotMatch(html, /id="lucky-games-list"/, "catalogue list container must be removed");
  assert.doesNotMatch(html, /class="lg-cards-row"/, "catalogue card-row markup must be removed");
});

test("3. the luckyGamesFeature fetch/render IIFE (GET /api/lucky-games catalogue) is removed", () => {
  const html = readHtml();
  assert.doesNotMatch(html, /function luckyGamesFeature/, "catalogue fetch/render feature must be removed");
  assert.doesNotMatch(html, /renderLuckyGameCard/, "catalogue card renderer must be removed");
  assert.doesNotMatch(html, /fetch\(`\$\{API_BASE\}\/api\/lucky-games`\)/, "Mini App must no longer fetch the catalogue endpoint");
});

test("4. orphaned .lg-card* CSS is removed", () => {
  const html = readHtml();
  assert.doesNotMatch(html, /\.lg-card\b/, "orphaned .lg-card CSS must be removed");
  assert.doesNotMatch(html, /#lucky-games-section\s*\{/, "orphaned #lucky-games-section CSS must be removed");
});

test("5. daily Lucky Game tile still loads from /v2/miniapp/daily-game", () => {
  const html = readHtml();
  assert.match(html, /id="daily-game-section"/, "daily tile must still exist");
  assert.match(html, /id="daily-game-content"/, "daily tile content slot must still exist");
  assert.match(html, /async function loadDailyGame\(/, "loadDailyGame must still exist");
  assert.match(html, /v2Fetch\(`\$\{API_V2\}\/daily-game`\)/, "daily tile must still fetch /v2/miniapp/daily-game");
});

test("6. daily Lucky Game tile design/classes are unchanged", () => {
  const html = readHtml();
  assert.match(
    html,
    /<div id="daily-game-section" class="ap-tile ap-tile-gold" style="display:block;">/,
    "daily tile markup/classes must be unchanged"
  );
});
