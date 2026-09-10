/**
 * Structural regression guard for static/admin-dashboard.js / .html.
 *
 * switchView() does `$("#view-" + v).classList` for every entry in VIEWS
 * with no null guard, so a VIEWS entry whose HTML section was ever removed
 * (or an HTML section whose VIEWS entry was removed) blanks the whole Admin
 * Dashboard with a TypeError on the next navigation. This suite locks
 * VIEWS and the `<section id="view-...">` blocks in admin-dashboard.html to
 * exactly the same set, and locks every tab's `view` reference in MODULES
 * to a name that still exists in VIEWS, so a future tab/view removal that
 * forgets one side fails CI instead of failing in the browser.
 *
 * Run with: node --test test_admin_dashboard_views_sync.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const HTML_PATH = path.join(__dirname, "static", "admin-dashboard.html");
const JS_SOURCE = fs.readFileSync(JS_PATH, "utf8");
const HTML_SOURCE = fs.readFileSync(HTML_PATH, "utf8");

function extractBetween(source, startMarker, endMarker, label) {
  const start = source.indexOf(startMarker);
  assert.ok(start !== -1, `start marker not found for ${label}`);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(end !== -1, `end marker not found for ${label}`);
  return source.slice(start, end);
}

// Evaluated with `new Function` (current realm) rather than the `vm` module
// so the returned arrays/objects are plain, native-realm values that
// assert.deepEqual can compare against literals without cross-realm quirks.
function loadViews() {
  const VIEWS_SRC = extractBetween(JS_SOURCE, "var VIEWS =[", "];", "VIEWS") + "];";
  return new Function(VIEWS_SRC + "\nreturn VIEWS;")();
}

function loadModules() {
  const MODULES_SRC = extractBetween(
    JS_SOURCE,
    "var MODULES = [",
    "\n  var currentModuleKey = null;",
    "MODULES"
  );
  return new Function(MODULES_SRC + "\nreturn MODULES;")();
}

// Every `<section id="view-...">` block is an application view switchView()
// can target. `#view-title` (an <h1>, not a <section>) is deliberately not
// matched by this pattern.
function htmlViewSectionIds() {
  const re = /<section\s+id="view-([A-Za-z0-9_]+)"/g;
  const ids = [];
  let m;
  while ((m = re.exec(HTML_SOURCE)) !== null) ids.push(m[1]);
  return ids;
}

test("every VIEWS entry has a matching #view-<name> HTML section", () => {
  const views = loadViews();
  const sectionIds = new Set(htmlViewSectionIds());
  const missing = views.filter((v) => !sectionIds.has(v));
  assert.deepEqual(missing, [], `VIEWS entries with no HTML section: ${missing.join(", ")}`);
});

test("every #view-<name> HTML section is declared in VIEWS", () => {
  const views = new Set(loadViews());
  const sectionIds = htmlViewSectionIds();
  const orphaned = sectionIds.filter((id) => !views.has(id));
  assert.deepEqual(orphaned, [], `HTML sections with no VIEWS entry: ${orphaned.join(", ")}`);
});

test("VIEWS has no duplicate entries", () => {
  const views = loadViews();
  const seen = new Set(views);
  assert.equal(seen.size, views.length, "VIEWS contains duplicate view names");
});

test("HTML has no duplicate #view-<name> section ids", () => {
  const sectionIds = htmlViewSectionIds();
  const seen = new Set(sectionIds);
  assert.equal(seen.size, sectionIds.length, "admin-dashboard.html has duplicate #view-<name> sections");
});

test("every MODULES tab.view references a view declared in VIEWS", () => {
  const views = new Set(loadViews());
  const modules = loadModules();
  const badRefs = [];
  modules.forEach((mod) => {
    mod.tabs.forEach((tab, idx) => {
      if (tab.external) return; // external tabs open a separate page, no switchView() call
      if (!views.has(tab.view)) {
        badRefs.push(`${mod.key}.tabs[${idx}] ("${tab.label}") -> "${tab.view}"`);
      }
    });
  });
  assert.deepEqual(badRefs, [], `MODULES tabs pointing at a missing view: ${badRefs.join("; ")}`);
});
