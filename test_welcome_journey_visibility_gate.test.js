/**
 * Asserts the Mini App's Welcome Journey visibility gate in static/index.html:
 *   - renderWelcomeProgress() only proceeds when both `visible` and
 *     `eligible` are explicitly true (backend is authoritative; the
 *     frontend must not infer visibility from partial/legacy fields).
 *   - hideWelcomeProgress() clears stale card content (step label, timeline
 *     squares, countdown, expiry, next-action text) so a hidden card never
 *     flashes a previous poll's progress when shown again.
 *
 * Run with: node --test test_welcome_journey_visibility_gate.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readIndexHtml() {
  return fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
}

test("renderWelcomeProgress gates on visible === true && eligible === true", () => {
  const html = readIndexHtml();
  const idx = html.indexOf("function renderWelcomeProgress(");
  assert.ok(idx !== -1, "renderWelcomeProgress not found");
  const body = html.slice(idx, html.indexOf("function loadWelcomeProgress(", idx));

  assert.ok(
    body.includes('data?.visible !== true || data?.eligible !== true'),
    "renderWelcomeProgress must require both visible===true and eligible===true before rendering"
  );
  assert.ok(
    body.includes("hideWelcomeProgress();"),
    "renderWelcomeProgress must call hideWelcomeProgress() on the ineligible/not-visible path"
  );
});

test("hideWelcomeProgress clears stale step/timeline/message content", () => {
  const html = readIndexHtml();
  const idx = html.indexOf("function hideWelcomeProgress(");
  assert.ok(idx !== -1, "hideWelcomeProgress not found");
  const body = html.slice(idx, html.indexOf("function highlightWelcomeRewardCard(", idx));

  assert.ok(body.includes('section.style.display = "none"'), "must hide the section");
  assert.ok(body.includes("welcome-step-label"), "must clear the step label");
  assert.ok(body.includes("welcome-progress-squares"), "must clear the timeline squares");
  assert.ok(body.includes("welcome-progress-next-action"), "must clear the next-action message");
  assert.ok(body.includes("_welcomeProgressData = null"), "must drop stale cached progress data");
});
