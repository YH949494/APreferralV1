/**
 * Regression tests for the Lucky Game card's click/impression tracking
 * (static/index.html) — making the existing daily-pick tile
 * (#daily-game-section) clickable to https://advantplay.com/our-games.html
 * with UTM params, plus best-effort, non-blocking analytics.
 *
 * These are structural/text assertions against the shipped markup and
 * script (same style as test_lucky_games_frontend.test.js), not a full DOM
 * simulation — there is no jsdom/jest harness in this project.
 *
 * Run with: node --test test_lucky_game_click_tracking.test.js
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

test("9. handleLuckyGameClick() opens via Telegram.WebApp.openLink with a window.open fallback", () => {
  const html = readHtml();
  const fnMatch = html.match(/function handleLuckyGameClick\(\)\s*\{[\s\S]*?\n    \}/);
  assert.ok(fnMatch, "expected to find handleLuckyGameClick()");
  const body = fnMatch[0];
  assert.match(body, /window\.Telegram\?\.WebApp/, "must check for Telegram.WebApp");
  assert.match(body, /waApp\.openLink\(LUCKY_GAME_DESTINATION_URL\)/, "must call openLink when available");
  assert.match(body, /window\.open\(LUCKY_GAME_DESTINATION_URL,\s*"_blank",\s*"noopener,noreferrer"\)/, "must fall back to window.open");
});

test("17 & 19. click tracking is fired but never awaited before navigation (fire-and-forget)", () => {
  const html = readHtml();
  const fnMatch = html.match(/function handleLuckyGameClick\(\)\s*\{[\s\S]*?\n    \}/);
  assert.ok(fnMatch, "expected to find handleLuckyGameClick()");
  const body = fnMatch[0];
  assert.doesNotMatch(body, /await\s+trackLuckyGameEvent/, "trackLuckyGameEvent must never be awaited before navigation");
  const trackIdx = body.indexOf("trackLuckyGameEvent(\"click\")");
  const openIdx = body.search(/waApp\.openLink|window\.open\(LUCKY_GAME_DESTINATION_URL/);
  assert.ok(trackIdx !== -1, "click must call trackLuckyGameEvent(\"click\")");
  assert.ok(trackIdx < openIdx, "tracking call must be issued before navigation, without blocking it");
});

test("21. one physical interaction cannot double-fire the click handler", () => {
  const html = readHtml();
  assert.match(html, /let luckyGameClickBusy = false;/, "expected a re-entrancy guard flag");
  const fnMatch = html.match(/function handleLuckyGameClick\(\)\s*\{[\s\S]*?\n    \}/);
  assert.ok(fnMatch, "expected to find handleLuckyGameClick()");
  assert.match(fnMatch[0], /if\s*\(luckyGameClickBusy\)\s*return;/, "must bail out on a re-entrant call");
  assert.match(fnMatch[0], /luckyGameClickBusy\s*=\s*true;/, "must arm the guard");
});

test("16. impression is tracked once per render via renderDailyGame()", () => {
  const html = readHtml();
  const fnMatch = html.match(/function renderDailyGame\(slot, dateKl\)\s*\{[\s\S]*?\n    \}/);
  assert.ok(fnMatch, "expected renderDailyGame(slot, dateKl)");
  const body = fnMatch[0];
  assert.match(body, /trackLuckyGameEvent\("impression"\)/, "must track an impression");
  assert.match(body, /luckyGameLastImpressionKey !== trackingKey/, "must dedupe repeated renders of the same tracking_key client-side");
});

test("15/22. impression is never tracked on an unavailable/empty daily game (early-return before render)", () => {
  const html = readHtml();
  const fnMatch = html.match(/function renderDailyGame\(slot, dateKl\)\s*\{([\s\S]*?)\n    \}/);
  assert.ok(fnMatch, "expected renderDailyGame(slot, dateKl)");
  const body = fnMatch[1];
  const guardIdx = body.indexOf("Today’s recommendation is unavailable right now");
  const trackIdx = body.indexOf("trackLuckyGameEvent(\"impression\")");
  assert.ok(guardIdx !== -1 && trackIdx !== -1, "expected both the unavailable-state guard and the impression tracking call");
  assert.ok(guardIdx < trackIdx, "the unavailable-game early return must precede impression tracking");
});

test("22. Lucky Game tile display/content structure remains unchanged", () => {
  const html = readHtml();
  assert.match(html, /id="daily-game-content"/, "content slot must still exist");
  assert.match(
    html,
    /contentEl\.innerHTML = `<div style="font-size:18px;font-weight:700;">\$\{name\}<\/div>\$\{tag\}\$\{maxwin\}\$\{reason\}`;/,
    "rendered card markup must be unchanged"
  );
});

test("tracking POSTs to /api/lucky-game/track with keepalive, no blocking await", () => {
  const html = readHtml();
  const fnMatch = html.match(/function trackLuckyGameEvent\(eventType\)\s*\{[\s\S]*?\n    \}/);
  assert.ok(fnMatch, "expected to find trackLuckyGameEvent()");
  const body = fnMatch[0];
  assert.match(body, /\/api\/lucky-game\/track/, "must POST to the lucky-game track endpoint");
  assert.match(body, /keepalive:\s*true/, "must use keepalive so the request survives navigation");
  assert.match(body, /\.catch\(function \(\) \{\}\);/, "fetch rejection must be swallowed, never thrown");
  assert.doesNotMatch(fnMatch[0], /\bawait\b/, "trackLuckyGameEvent itself must not await the network call");
});
