/**
 * Legacy undated voucher inventory is hidden in Admin → Affiliate Centre;
 * V2 scheduled-batch UI and management actions are untouched.
 *
 * Run with: node --test test_admin_legacy_voucher_inventory_hidden.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const HTML = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.html"), "utf8");
const JS = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");

function legacySection() {
  const m = HTML.match(/<div id="ab-legacy-section"([^>]*)>([\s\S]*?<div id="ab-legacy-summary"[^>]*><\/div>)\s*<\/div>/);
  assert.ok(m, "legacy section wrapper not found");
  return { attrs: m[1], inner: m[2] };
}

test("legacy section wrapper is hidden", () => {
  const { attrs } = legacySection();
  assert.match(attrs, /\bhidden\b/);
  assert.match(attrs, /display:\s*none/);
});

test("legacy heading and summary container live only inside the hidden wrapper", () => {
  const { inner } = legacySection();
  assert.ok(inner.includes("Legacy Undated Codes (pre-batch uploads)"));
  assert.equal(HTML.split("Legacy Undated Codes").length - 1, 1);
  assert.equal(HTML.split('id="ab-legacy-summary"').length - 1, 1);
});

test("legacy render target still exists so renderAffiliateBatches cannot null-deref", () => {
  assert.ok(JS.includes('$("#ab-legacy-summary")'));
});

test("V2 scheduled-batch components remain visible", () => {
  const before = HTML.slice(0, HTML.indexOf('id="ab-legacy-section"'));
  const after = HTML.slice(HTML.indexOf('id="ab-legacy-section"'));
  assert.ok(before.includes('id="ab-create-btn"'));
  assert.ok(after.includes("Scheduled &amp; Active Batches"));
  assert.ok(after.includes('id="ab-body"'));
  assert.ok(after.includes('id="ab-filter-pool"'));
  // none of the V2 elements are inside the hidden wrapper
  const { inner } = legacySection();
  for (const id of ["ab-create-btn", "ab-body", "ab-filter-pool"]) {
    assert.ok(!inner.includes('id="' + id + '"'), id + " must not be hidden");
  }
});

test("batch management actions still rendered by JS", () => {
  for (const attr of ["data-ab-view", "data-ab-addcodes", "data-ab-edit", "data-ab-reconcile", "data-ab-enable", "data-ab-disable"]) {
    assert.ok(JS.includes(attr), attr + " missing");
  }
  assert.ok(JS.includes("/api/admin/affiliate-voucher-batches"));
});
