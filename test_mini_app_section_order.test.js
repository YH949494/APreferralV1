/**
 * Asserts the Mini App section DOM order in static/index.html:
 *   Campaign Rewards -> Current Welcome Progress -> Your Rewards Journey
 *
 * "Current Welcome Progress" (#welcome-progress-section) previously sat far
 * below the Leaderboard / Channel / Info Hub area; it must render as a
 * single, non-duplicated container immediately after the Campaign Rewards
 * markup and immediately before the "Your Rewards Journey" card
 * (#progress-section).
 *
 * Run with: node --test test_mini_app_section_order.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readIndexHtml() {
  return fs.readFileSync(path.join(__dirname, "static", "index.html"), "utf8");
}

test("welcome-progress-section appears exactly once", () => {
  const html = readIndexHtml();
  const matches = html.match(/id="welcome-progress-section"/g) || [];
  assert.equal(matches.length, 1, "welcome-progress-section must not be duplicated");
});

test("DOM order: campaign rewards -> welcome progress -> your rewards journey", () => {
  const html = readIndexHtml();

  const campaignRewardsIdx = html.indexOf('id="campaign-centre-root"');
  const welcomeProgressIdx = html.indexOf('id="welcome-progress-section"');
  const rewardsJourneyIdx = html.indexOf('id="progress-section"');

  assert.ok(campaignRewardsIdx !== -1, "campaign-centre-root not found");
  assert.ok(welcomeProgressIdx !== -1, "welcome-progress-section not found");
  assert.ok(rewardsJourneyIdx !== -1, "progress-section (Your Rewards Journey) not found");

  assert.ok(
    campaignRewardsIdx < welcomeProgressIdx,
    "Campaign Rewards must precede Current Welcome Progress in DOM order"
  );
  assert.ok(
    welcomeProgressIdx < rewardsJourneyIdx,
    "Current Welcome Progress must precede Your Rewards Journey in DOM order"
  );
});

test("welcome progress renders above Leaderboard / Channel / Info Hub", () => {
  const html = readIndexHtml();

  const welcomeProgressIdx = html.indexOf('id="welcome-progress-section"');
  const leaderboardChipIdx = html.indexOf("toggleLeaderboardSection()");
  const leaderboardSectionIdx = html.indexOf('id="leaderboard-section"');
  const infoHubIdx = html.indexOf('id="infoHubBtn"');

  assert.ok(welcomeProgressIdx !== -1);
  assert.ok(leaderboardChipIdx !== -1);
  assert.ok(leaderboardSectionIdx !== -1);
  assert.ok(infoHubIdx !== -1);

  assert.ok(welcomeProgressIdx < leaderboardChipIdx, "welcome progress must render above the Leaderboard chip");
  assert.ok(welcomeProgressIdx < leaderboardSectionIdx, "welcome progress must render above the Leaderboard panel");
  assert.ok(welcomeProgressIdx < infoHubIdx, "welcome progress must render above Info Hub");
});

test("no dynamic DOM-move logic repositions welcome-progress-section after load", () => {
  const html = readIndexHtml();
  const start = html.indexOf('id="welcome-progress-section"');
  const scriptStart = html.indexOf("<script", start);
  const relevantJs = html.slice(scriptStart === -1 ? start : scriptStart);

  // Only visibility toggles (style.display) are expected to touch this
  // section at runtime; there must be no appendChild/insertBefore/
  // insertAdjacent* call operating on it.
  const movePatterns = [
    /welcome-progress-section["'`)][^;]*\.(appendChild|insertBefore|insertAdjacentElement|insertAdjacentHTML)/,
  ];
  for (const pattern of movePatterns) {
    assert.ok(!pattern.test(relevantJs), `unexpected DOM-move logic matching ${pattern}`);
  }
});
