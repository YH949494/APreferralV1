/**
 * Tests for the Community Centre message/caption editor's pure Telegram-HTML
 * helper functions (ccRteSanitizeHtml / ccRteHtmlToPlainText /
 * ccRteValidateUrl / ccRteQuoteAction / ccRteShouldApplyToSelection) in
 * static/admin-dashboard.js.
 *
 * These are pure string-in/string-out functions with no DOM dependency —
 * the DOM-facing half of the editor (toolbar wiring, contenteditable
 * Selection/Range manipulation, execCommand) is exercised by hand in a
 * browser since this repo has no jsdom/browser test harness (see
 * test_community_retry_modal.test.js for the established sandboxed-vm
 * pattern this file follows).
 *
 * Run with: node --test test_cc_rich_text_editor.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const START_MARKER = "  var CC_RTE_TAG_ALIASES = {";
const END_MARKER = "  var cc = {";

function loadFunctionsSource() {
  const js = fs.readFileSync(path.join(__dirname, "static", "admin-dashboard.js"), "utf8");
  const start = js.indexOf(START_MARKER);
  const end = js.indexOf(END_MARKER, start);
  assert.ok(start !== -1, "CC_RTE_TAG_ALIASES not found in static/admin-dashboard.js");
  assert.ok(end !== -1, "cc state object end marker not found in static/admin-dashboard.js");
  return js.slice(start, end);
}

function makeContext() {
  const sandbox = { URL, console };
  vm.createContext(sandbox);
  const src = loadFunctionsSource() +
    "\nthis.__ccRteSanitizeHtml = ccRteSanitizeHtml;" +
    "\nthis.__ccRteHtmlToPlainText = ccRteHtmlToPlainText;" +
    "\nthis.__ccRteValidateUrl = ccRteValidateUrl;" +
    "\nthis.__ccRteQuoteAction = ccRteQuoteAction;" +
    "\nthis.__ccRteShouldApplyToSelection = ccRteShouldApplyToSelection;" +
    "\nthis.__ccRteIsSafeHref = ccRteIsSafeHref;";
  vm.runInContext(src, sandbox);
  return sandbox;
}

const ctx = makeContext();
const sanitize = ctx.__ccRteSanitizeHtml;
const toPlainText = ctx.__ccRteHtmlToPlainText;
const validateUrl = ctx.__ccRteValidateUrl;
const quoteAction = ctx.__ccRteQuoteAction;
const shouldApply = ctx.__ccRteShouldApplyToSelection;

// ---------------------------------------------------------------------
// Toolbar formatting -> canonical Telegram HTML
// ---------------------------------------------------------------------

test("bold formatting", () => {
  assert.equal(sanitize("<b>Big reward</b> tonight"), "<b>Big reward</b> tonight");
  assert.equal(sanitize("<strong>Big reward</strong> tonight"), "<b>Big reward</b> tonight");
});

test("italic formatting", () => {
  assert.equal(sanitize("<i>Big reward</i> tonight"), "<i>Big reward</i> tonight");
  assert.equal(sanitize("<em>Big reward</em> tonight"), "<i>Big reward</i> tonight");
});

test("underline formatting", () => {
  assert.equal(sanitize("<u>Big reward</u> tonight"), "<u>Big reward</u> tonight");
  assert.equal(sanitize("<ins>Big reward</ins> tonight"), "<u>Big reward</u> tonight");
});

test("strikethrough formatting", () => {
  assert.equal(sanitize("<s>Big reward</s> tonight"), "<s>Big reward</s> tonight");
  assert.equal(sanitize("<strike>Big reward</strike> tonight"), "<s>Big reward</s> tonight");
  assert.equal(sanitize("<del>Big reward</del> tonight"), "<s>Big reward</s> tonight");
});

test("inline code", () => {
  assert.equal(sanitize("Run <code>npm test</code> first"), "Run <code>npm test</code> first");
});

test("quote (single-line)", () => {
  assert.equal(sanitize("<blockquote>Quoted text</blockquote>"), "<blockquote>Quoted text</blockquote>");
});

test("quote (multi-line)", () => {
  const raw = "<blockquote>line one\nline two</blockquote>";
  assert.equal(sanitize(raw), raw);
});

test("nested quote prevention — flattened, not nested", () => {
  const out = sanitize("<blockquote>outer <blockquote>inner</blockquote> tail</blockquote>");
  assert.equal(out, "<blockquote>outer inner tail</blockquote>");
  assert.equal((out.match(/<blockquote/g) || []).length, 1);
});

test("link insertion", () => {
  assert.equal(
    sanitize('<a href="https://example.com">click here</a>'),
    '<a href="https://example.com">click here</a>'
  );
});

test("spoiler (span.tg-spoiler) supported", () => {
  assert.equal(
    sanitize('<span class="tg-spoiler">hidden</span>'),
    '<span class="tg-spoiler">hidden</span>'
  );
});

test("clear formatting — plain text passes through untouched", () => {
  assert.equal(sanitize("Big reward tonight"), "Big reward tonight");
  // Simulates the editor's clear-formatting result: tags removed, text kept.
  assert.equal(sanitize("Big reward tonight"), toPlainText(sanitize("<b>Big reward</b> tonight")));
});

// ---------------------------------------------------------------------
// Link URL validation
// ---------------------------------------------------------------------

test("invalid URL rejection — javascript: scheme", () => {
  assert.match(validateUrl("javascript:alert(1)"), /not allowed/);
});

test("invalid URL rejection — data: scheme", () => {
  assert.match(validateUrl("data:text/html,evil"), /not allowed/);
});

test("invalid URL rejection — malformed URL", () => {
  assert.ok(validateUrl("ht!tp://not a url"));
});

test("invalid URL rejection — empty URL", () => {
  assert.match(validateUrl(""), /required/);
});

test("valid https URL accepted", () => {
  assert.equal(validateUrl("https://example.com/path"), null);
});

test("valid approved tg:// link accepted", () => {
  assert.equal(validateUrl("tg://resolve?domain=SomeBot"), null);
});

test("ccRteIsSafeHref rejects unsafe schemes", () => {
  assert.equal(ctx.__ccRteIsSafeHref("javascript:alert(1)"), false);
  assert.equal(ctx.__ccRteIsSafeHref("data:text/html,x"), false);
  assert.equal(ctx.__ccRteIsSafeHref("https://example.com"), true);
  assert.equal(ctx.__ccRteIsSafeHref("tg://resolve"), true);
});

// ---------------------------------------------------------------------
// Existing HTML draft loading / round-tripping (no double-escaping)
// ---------------------------------------------------------------------

test("existing HTML draft loading — valid tags preserved", () => {
  const draft = "<b>Bold</b> and <i>italic</i> and <a href=\"https://t.me/x\">link</a>";
  assert.equal(sanitize(draft), draft);
});

test("existing HTML draft loading — unsupported tags stripped safely", () => {
  const draft = '<div class="msg"><font color="red">red text</font></div>';
  const out = sanitize(draft);
  assert.ok(!out.includes("<div"));
  assert.ok(!out.includes("<font"));
  assert.ok(out.includes("red text"));
});

test("no double escaping of already-safe text", () => {
  assert.equal(sanitize("Terms &amp; Conditions"), "Terms &amp; Conditions");
  assert.equal(toPlainText(sanitize("Terms &amp; Conditions")), "Terms & Conditions");
});

// ---------------------------------------------------------------------
// Paste sanitization / unsupported tags / script injection
// ---------------------------------------------------------------------

test("paste sanitization strips fonts/colours/classes/inline CSS", () => {
  const pasted = '<p style="color:red;font-family:Arial"><span class="Apple-style-span" style="font-weight:bold">Hello</span></p>';
  const out = sanitize(pasted);
  assert.ok(!out.includes("style="));
  assert.ok(!out.includes("Apple-style-span"));
  assert.ok(out.includes("Hello"));
});

test("unsupported tags removed (table/img/iframe/object)", () => {
  const raw = '<table><tr><td>x</td></tr></table><img src="a.png"><iframe src="evil"></iframe><object data="x"></object>keep';
  const out = sanitize(raw);
  assert.ok(!/<table|<img|<iframe|<object/.test(out));
  assert.ok(out.includes("keep"));
});

test("script injection blocked — tag and content removed", () => {
  const out = sanitize('<script>alert(document.cookie)</script>Hello<b>World</b>');
  assert.ok(!out.includes("<script"));
  assert.ok(!out.includes("alert(document.cookie)"));
  assert.equal(out, "Hello<b>World</b>");
});

test("style tag content removed entirely", () => {
  const out = sanitize("<style>body{background:red}</style>visible");
  assert.ok(!out.includes("<style"));
  assert.ok(!out.includes("background:red"));
  assert.equal(out, "visible");
});

test("inline event handler attributes stripped", () => {
  const out = sanitize('<a href="https://example.com" onclick="alert(1)">click</a>');
  assert.ok(!out.includes("onclick"));
  assert.ok(out.includes('href="https://example.com"'));
});

test("arbitrary span classes stripped, tg-spoiler kept", () => {
  const out = sanitize('<span class="evil-class">x</span><span class="tg-spoiler">y</span>');
  assert.ok(!out.includes("evil-class"));
  assert.equal(out, 'x<span class="tg-spoiler">y</span>');
});

// ---------------------------------------------------------------------
// Line breaks (paste / draft loading)
// ---------------------------------------------------------------------

test("div/p/br line breaks converted to newlines", () => {
  assert.equal(sanitize("<div>line one</div><div>line two</div>"), "line one\nline two");
  assert.equal(sanitize("line one<br>line two"), "line one\nline two");
});

// ---------------------------------------------------------------------
// Visible character count excludes HTML tags
// ---------------------------------------------------------------------

test("visible character count excludes HTML tags", () => {
  const html = "<b>Big reward</b> tonight";
  assert.equal(toPlainText(html), "Big reward tonight");
  assert.equal(toPlainText(html).length, "Big reward tonight".length);
  assert.notEqual(toPlainText(html).length, html.length);
});

test("visible character count decodes entities to single characters", () => {
  assert.equal(toPlainText("Terms &amp; Conditions").length, "Terms & Conditions".length);
});

// ---------------------------------------------------------------------
// Nested quote prevention / empty selection / quote toggle decision
// ---------------------------------------------------------------------

test("quote toggle: not inside a quote -> wrap", () => {
  assert.equal(quoteAction(false), "wrap");
});

test("quote toggle: already inside a quote -> unwrap (no nesting)", () => {
  assert.equal(quoteAction(true), "unwrap");
});

test("empty selection behaviour — toolbar actions are a no-op", () => {
  assert.equal(shouldApply(""), false);
  assert.equal(shouldApply(null), false);
  assert.equal(shouldApply(undefined), false);
  assert.equal(shouldApply("selected text"), true);
});

// ---------------------------------------------------------------------
// Backend sanitization remains authoritative — sanity cross-check
// ---------------------------------------------------------------------

test("client sanitizer output stays within the backend allowlist tag set", () => {
  const BACKEND_ALLOWED = ["b", "i", "u", "s", "code", "pre", "blockquote", "a", "span"];
  const out = sanitize('<b>b</b><i>i</i><u>u</u><s>s</s><code>c</code><blockquote>q</blockquote>' +
    '<a href="https://x.com">a</a><span class="tg-spoiler">sp</span><script>bad()</script><div>d</div>');
  const tagsFound = Array.from(new Set((out.match(/<\/?([a-z]+)/g) || []).map((t) => t.replace(/[<\/]/g, ""))));
  tagsFound.forEach((t) => assert.ok(BACKEND_ALLOWED.includes(t), `unexpected tag <${t}> in sanitized output`));
});
