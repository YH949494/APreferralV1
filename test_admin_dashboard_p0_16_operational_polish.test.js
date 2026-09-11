/**
 * P0.16 — operational/admin usability follow-up to the P0.13 audit (backend
 * N+1 removal is covered separately in test_campaign_centre_p0_16.py /
 * test_tournament_rewards_p0_16.py; this file covers the frontend pieces):
 *
 *   A. 200-row campaign list truncation notice (gcTruncationNoticeText +
 *      loadGcCampaigns wiring reads total/truncated off the response).
 *   B. Campaign Detail's own "•••" overflow (gcCampaignDetailOverflowHtml)
 *      reuses gcListActions/gcOverflowMenuHtml verbatim — same legality as
 *      the Campaigns list, never a second action system — plus the
 *      Detail-triggered Delete navigate-away wiring.
 *   E/F. Mobile modal + card CSS fixes (static assertions on
 *      static/admin-dashboard.css).
 *   G. Keyboard/accessibility: card Enter/Space parity, confirmSimple
 *      Escape/focus handling, modal role/aria-modal/aria-label semantics,
 *      overflow-menu aria-expanded sync.
 *
 * Mirrors the existing P0.4/P0.10/P0.15 harness: no build step, no jsdom —
 * relevant source ranges are extracted as text and executed in sandboxed vm
 * contexts against small stand-ins.
 *
 * Run with: node --test test_admin_dashboard_p0_16_operational_polish.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_PATH = path.join(__dirname, "static", "admin-dashboard.js");
const JS = fs.readFileSync(JS_PATH, "utf8");
const CSS_PATH = path.join(__dirname, "static", "admin-dashboard.css");
const CSS = fs.readFileSync(CSS_PATH, "utf8");
const HTML_PATH = path.join(__dirname, "static", "admin-dashboard.html");
const HTML = fs.readFileSync(HTML_PATH, "utf8");

function slice(src, startMarker, endMarker) {
  const start = src.indexOf(startMarker);
  const end = src.indexOf(endMarker, start);
  assert.ok(start !== -1, "start marker not found: " + startMarker);
  assert.ok(end > start, "end marker not found after: " + startMarker);
  return src.slice(start, end);
}

function extractFunctionSource(source, name) {
  const start = source.indexOf("function " + name + "(");
  assert.notEqual(start, -1, name + " not found in admin-dashboard.js");
  let depth = 0;
  let i = source.indexOf("{", start);
  for (; i < source.length; i++) {
    if (source[i] === "{") depth++;
    else if (source[i] === "}") { depth--; if (depth === 0) break; }
  }
  return source.slice(start, i + 1);
}

function esc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function runInSandbox(code, sandboxExtra) {
  const sandbox = Object.assign({ console, Object, String, Promise, JSON, Array, Math }, sandboxExtra);
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox);
  return sandbox;
}

// =======================================================================
// A — gcTruncationNoticeText (pure) + loadGcCampaigns wiring
// =======================================================================

const NOTICE_SRC = extractFunctionSource(JS, "gcTruncationNoticeText");

function loadNotice() {
  return runInSandbox(NOTICE_SRC + "\nthis.gcTruncationNoticeText = gcTruncationNoticeText;", {});
}

test("gcTruncationNoticeText: exact wording matches the spec example (200 of 237)", () => {
  const s = loadNotice();
  assert.equal(
    s.gcTruncationNoticeText(200, 237),
    "Showing the first 200 of 237 campaigns. Use the status filters to narrow the list."
  );
});

test("gcTruncationNoticeText: never claims an error tone (no '!' or 'Error')", () => {
  const s = loadNotice();
  const msg = s.gcTruncationNoticeText(200, 500);
  assert.doesNotMatch(msg, /error/i);
  assert.doesNotMatch(msg, /!/);
});

test("loadGcCampaigns: reads total/truncated off both the unfiltered-cache and the direct filtered-fetch response paths", () => {
  const src = extractFunctionSource(JS, "loadGcCampaigns");
  // Filtered path: status=... fetch's own d.total/d.truncated.
  assert.match(src, /truncated:\s*!!d\.truncated/);
  // Unfiltered path: gcOptionsCache's cached total/truncated (set by
  // fetchGcCampaignsList), not re-derived from items.length.
  assert.match(src, /gcOptionsCache\.campaignsTotal/);
  assert.match(src, /gcOptionsCache\.campaignsTruncated/);
});

test("loadGcCampaigns: shows the notice only when truncated, and hides+clears it otherwise", () => {
  const src = extractFunctionSource(JS, "loadGcCampaigns");
  assert.match(src, /gcTruncationNoticeText\(ctx\.items\.length,\s*ctx\.total\)/);
  assert.match(src, /noticeEl\.classList\.remove\("hidden"\)/);
  assert.match(src, /noticeEl\.classList\.add\("hidden"\)/);
});

test("loadGcCampaigns: the empty-state branch also hides the notice (never a stale truncation banner over an empty list)", () => {
  const src = extractFunctionSource(JS, "loadGcCampaigns");
  const emptyBranchStart = src.indexOf("if (!ctx.items.length)");
  const emptyBranchEnd = src.indexOf("}", src.indexOf("emptyState(gcEmptyStateForFilter", emptyBranchStart));
  const branch = src.slice(emptyBranchStart, emptyBranchEnd);
  assert.match(branch, /noticeEl\.classList\.add\("hidden"\)/);
});

test("fetchGcCampaignsList stores total/truncated on gcOptionsCache from the server response, defaulting total to items.length when absent", () => {
  const src = extractFunctionSource(JS, "fetchGcCampaignsList");
  assert.match(src, /gcOptionsCache\.campaignsTotal\s*=\s*typeof data\.total === "number" \? data\.total : items\.length/);
  assert.match(src, /gcOptionsCache\.campaignsTruncated\s*=\s*!!data\.truncated/);
});

test("HTML: the truncation notice container sits above #gc-campaigns-body and starts hidden, non-error styled", () => {
  const idx = HTML.indexOf('id="gc-truncation-notice"');
  assert.notEqual(idx, -1, "notice container not found");
  const bodyIdx = HTML.indexOf('id="gc-campaigns-body"');
  assert.ok(bodyIdx > idx, "notice must render above the campaigns body");
  const tag = HTML.slice(HTML.lastIndexOf("<div", idx), idx + 200);
  assert.match(tag, /class="banner warn hidden"/, "must start hidden and use the non-error 'warn' banner style");
});

// =======================================================================
// B — Campaign Detail overflow menu (gcCampaignDetailOverflowHtml)
// =======================================================================

const DETAIL_SRC = slice(JS, "  var GC_TYPE_LABELS = {", "\n  function bindCampaignDetail() {");
const MISSION_ACTIONS_SRC = extractFunctionSource(JS, "gcMissionActionsHtml");

function loadDetailOverflow() {
  return runInSandbox(
    MISSION_ACTIONS_SRC + "\n" + DETAIL_SRC +
      "\nthis.__x = { gcCampaignDetailOverflowHtml, gcListActions, gcOverflowMenuHtml, gcCampaignDetailHtml, computeSetupChecklist };",
    { esc, ccUtcToKlDisplay: (iso) => "KL:" + iso }
  ).__x;
}

function liveTournament(overrides) {
  return Object.assign({
    campaign_id: "july-tournament", name: "July Tournament", type: "tournament",
    status: "live", mechanic: "standard_drop",
  }, overrides || {});
}

test("Campaign Detail overflow: a live tournament gets Pause", () => {
  const D = loadDetailOverflow();
  const html = D.gcCampaignDetailOverflowHtml(liveTournament());
  assert.match(html, /data-gc-action="pause"/);
});

test("Campaign Detail overflow: an archived campaign does not get Pause", () => {
  const D = loadDetailOverflow();
  const html = D.gcCampaignDetailOverflowHtml(liveTournament({ status: "archived" }));
  assert.doesNotMatch(html, /data-gc-action="pause"/);
});

test("Campaign Detail overflow: Mission actions (Close Mission/End Rewards) match the same gcMissionActionsHtml the list uses", () => {
  const D = loadDetailOverflow();
  const live = { campaign_id: "trivia-1", name: "Trivia", mechanic: "mission_pool", status: "live", mission_pool: { cancelled: false }, mission_active_rewards: 2 };
  const html = D.gcCampaignDetailOverflowHtml(live);
  assert.match(html, /data-gc-action="close-mission"/);
  assert.match(html, /data-gc-action="end-rewards"/);
});

test("Campaign Detail overflow: a cancelled Mission never offers Publish/Resume/Pause/Close Mission", () => {
  const D = loadDetailOverflow();
  const cancelled = { campaign_id: "trivia-2", name: "Trivia", mechanic: "mission_pool", status: "live", mission_pool: { cancelled: true } };
  const html = D.gcCampaignDetailOverflowHtml(cancelled);
  assert.doesNotMatch(html, /data-gc-action="publish"/);
  assert.doesNotMatch(html, /data-gc-action="pause"/);
  assert.doesNotMatch(html, /data-gc-action="close-mission"/);
});

test("Campaign Detail and Campaigns list produce byte-identical menu items for the same campaign (single shared builder, not a second action system)", () => {
  const D = loadDetailOverflow();
  const campaign = liveTournament({ status: "paused" });
  const detailHtml = D.gcCampaignDetailOverflowHtml(campaign);
  const listMenuHtml = D.gcOverflowMenuHtml(campaign, D.gcListActions(campaign));
  assert.ok(detailHtml.includes(listMenuHtml), "the detail overflow must embed gcOverflowMenuHtml's own output verbatim");
});

test("Campaign Detail overflow: the kebab wrapper carries data-gc-kebab so it's picked up by the same delegated document click handler as the list", () => {
  const D = loadDetailOverflow();
  const html = D.gcCampaignDetailOverflowHtml(liveTournament());
  assert.match(html, /data-gc-kebab="1"/);
  assert.match(html, /aria-haspopup="true"/);
  assert.match(html, /aria-expanded="false"/);
});

test("gcCampaignDetailHtml renders the overflow next to Preview Campaign, alongside (not replacing) the primary Publish/Resume CTA", () => {
  const D = loadDetailOverflow();
  const campaign = liveTournament({ status: "paused", schedule: { starts_at: "2026-01-01T00:00:00Z" } });
  const rows = D.computeSetupChecklist(campaign, [], []);
  const html = D.gcCampaignDetailHtml(campaign, [], [], null);
  assert.match(html, /Preview Campaign/);
  assert.match(html, /data-gc-kebab="1"/);
  // The primary CTA (Resume Campaign) still renders — the overflow is
  // additive, never a replacement for the lifecycle banner/CTA.
  assert.match(html, /data-gc-action="publish"[^>]*data-gc-resume="1"/);
});

// =======================================================================
// B — Delete-from-Detail navigates away instead of refreshing in place
// =======================================================================

test("wiring: a delete triggered from Campaign Detail (state.view===campaignDetail, matching campaign) invalidates the cache and navigates to the list, not the default in-place refresh", () => {
  const start = JS.indexOf('else if (action === "delete") {');
  assert.notEqual(start, -1, "delete branch not found");
  const end = JS.indexOf("\n      }\n    });", start);
  const chunk = JS.slice(start, end);
  assert.match(chunk, /state\.view === "campaignDetail" && state\.campaignId === id/);
  assert.match(chunk, /gcInvalidateCampaignsCache\(\)/);
  assert.match(chunk, /activateTab\("growth",\s*0\)/);
});

test("wiring: a delete triggered from the Campaigns list (not Detail) keeps the default onDeleted (openGcDeleteModal's own loadGcCampaigns(true) fallback)", () => {
  const start = JS.indexOf('else if (action === "delete") {');
  const end = JS.indexOf("\n      }\n    });", start);
  const chunk = JS.slice(start, end);
  assert.match(chunk, /wasDetail \? function \(\) \{/, "must only override onDeleted when wasDetail is true");
});

// =======================================================================
// E — Mobile modal CSS (static assertions)
// =======================================================================

function cssRule(css, selector) {
  const idx = css.indexOf(selector + " {");
  assert.notEqual(idx, -1, "CSS rule not found: " + selector);
  const end = css.indexOf("}", idx);
  return css.slice(idx, end + 1);
}

test("CSS: .modal-overlay allows vertical scrolling with safe padding, and no longer clips overflow via align-items:center", () => {
  const rule = cssRule(CSS, ".modal-overlay");
  assert.match(rule, /overflow-y:\s*auto/);
  assert.match(rule, /align-items:\s*flex-start/);
  assert.match(rule, /padding:/);
});

test("CSS: .modal-box has a viewport-relative max-height with overflow-y:auto (dvh preferred, vh fallback present)", () => {
  const rule = cssRule(CSS, ".modal-box");
  assert.match(rule, /max-height:\s*calc\(100vh/);
  assert.match(rule, /max-height:\s*calc\(100dvh/);
  assert.match(rule, /overflow-y:\s*auto/);
});

test("CSS: .modal-box keeps a margin:auto centering behavior so desktop centering isn't broken", () => {
  const rule = cssRule(CSS, ".modal-box");
  assert.match(rule, /margin:\s*auto/);
});

// =======================================================================
// F — Mobile card CSS (static assertions)
// =======================================================================

test("CSS: .campaign-card-actions wraps instead of squeezing Manage + Process Mission + •••", () => {
  const rule = cssRule(CSS, ".campaign-card-actions");
  assert.match(rule, /flex-wrap:\s*wrap/);
});

test("CSS: .campaign-card-title has min-width:0 and overflow-wrap:anywhere so a long name wraps instead of clipping the status pill", () => {
  const rule = cssRule(CSS, ".campaign-card-title");
  assert.match(rule, /min-width:\s*0/);
  assert.match(rule, /overflow-wrap:\s*anywhere/);
});

test("CSS: .gc-kv-value (Technical Details / Advanced Settings values) wraps long campaign_id/provider_id/URLs instead of clipping", () => {
  const rule = cssRule(CSS, ".gc-kv-value");
  assert.match(rule, /overflow-wrap:\s*anywhere/);
  assert.match(rule, /min-width:\s*0/);
});

test("Campaign Detail's Advanced Settings and Technical Details rows use .gc-kv-row/.gc-kv-value, not raw unwrapped inline flex rows", () => {
  const advancedSrc = extractFunctionSource(JS, "gcCampaignDetailAdvancedHtml");
  const technicalSrc = extractFunctionSource(JS, "gcCampaignDetailTechnicalHtml");
  assert.match(advancedSrc, /class="gc-kv-row"/);
  assert.match(advancedSrc, /class="gc-kv-value"/);
  assert.match(technicalSrc, /class="gc-kv-row"/);
  assert.match(technicalSrc, /class="gc-kv-value"/);
});

test("CSS: #gc-status-filter keeps its existing horizontal-scroll interaction and adds a non-invasive trailing edge-fade affordance", () => {
  const rule = cssRule(CSS, "#gc-status-filter");
  assert.match(rule, /overflow-x:\s*auto/, "must keep horizontal scroll, not switch to wrap");
  assert.match(rule, /mask-image:\s*linear-gradient/);
});

// =======================================================================
// G — Keyboard/accessibility
// =======================================================================

test("card keyboard parity: Enter/Space activation is wired via a delegated keydown listener that calls renderCampaignDetail, mirroring the click handler's own guard", () => {
  const bindSrc = extractFunctionSource(JS, "bindGcCampaigns");
  const idx = bindSrc.indexOf('e.key !== "Enter" && e.key !== " "');
  assert.notEqual(idx, -1, "Enter/Space keydown guard not found");
  const chunk = bindSrc.slice(idx, bindSrc.indexOf("});", idx));
  assert.match(chunk, /renderCampaignDetail\(card\.dataset\.gcRowId\)/);
  assert.match(chunk, /e\.preventDefault\(\)/, "Space must not also scroll the page");
  assert.match(chunk, /e\.target\.closest\("button"\)/, "a real control inside the card must keep its own handling");
});

test("HTML: the campaign card is still role=button tabindex=0 (keyboard parity was added, not swapped for removing the role)", () => {
  const src = extractFunctionSource(JS, "gcCampaignRowHtml");
  assert.match(src, /role="button"/);
  assert.match(src, /tabindex="0"/);
});

// ---- confirmSimple: Escape / initial focus / focus restore ----

const CONFIRM_SRC = slice(JS, "  // Unique id generator for aria-labelledby targets", "\n  function setMeta(text) {");

class FakeEl {
  constructor(tag) {
    this.tagName = (tag || "div").toUpperCase();
    this.id = "";
    this.children = [];
    this.parent = null;
    this._text = "";
    this._html = "";
    this._listeners = {};
    this._attrs = {};
    this.focused = false;
  }
  get textContent() { return this._text; }
  set textContent(v) { this._text = v == null ? "" : String(v); }
  get innerHTML() { return this._html; }
  set innerHTML(v) {
    this._html = v == null ? "" : String(v);
    // Minimal innerHTML->tree materialization: confirmSimple/confirmTyped
    // build their whole modal via one innerHTML assignment (a wrapping
    // <div class="modal-box" role=... aria-...>...</div> containing h3/p/
    // input/button descendants), then look up ids and read role/aria-*
    // attributes off the box — a real HTML parser isn't needed, just a
    // small nesting-aware one covering the tags these two builders emit.
    this.children = [];
    const VOID = new Set(["input"]);
    const stack = [this];
    const re = /<(\/?)(div|h3|p|input|button|strong)\b([^>]*)>/g;
    let m;
    while ((m = re.exec(this._html))) {
      const closing = m[1] === "/";
      const tag2 = m[2];
      const attrsStr = m[3] || "";
      const top = stack[stack.length - 1];
      if (closing) {
        if (stack.length > 1 && top.tagName === tag2.toUpperCase()) stack.pop();
        continue;
      }
      const child = new FakeEl(tag2);
      child.parent = top;
      const idMatch = attrsStr.match(/\bid="([^"]*)"/);
      if (idMatch) child.id = idMatch[1];
      const attrRe = /([a-zA-Z-]+)="([^"]*)"/g;
      let am;
      while ((am = attrRe.exec(attrsStr))) child._attrs[am[1]] = am[2];
      top.children.push(child);
      if (!VOID.has(tag2)) stack.push(child);
    }
  }
  appendChild(node) { node.parent = this; this.children.push(node); return node; }
  remove() { if (this.parent) { this.parent.children = this.parent.children.filter((c) => c !== this); this.parent = null; } }
  addEventListener(evt, fn) { (this._listeners[evt] = this._listeners[evt] || []).push(fn); }
  _trigger(evt, evtObj) { (this._listeners[evt] || []).slice().forEach((fn) => fn(evtObj || { target: this })); }
  focus() { this.focused = true; }
  setAttribute(name, value) { this._attrs[name] = String(value); }
  getAttribute(name) { return Object.prototype.hasOwnProperty.call(this._attrs, name) ? this._attrs[name] : null; }
  querySelector(sel) {
    const id = sel.slice(1);
    const stack = [...this.children];
    while (stack.length) {
      const node = stack.shift();
      if (node.id === id) return node;
      stack.push(...node.children);
    }
    return null;
  }
}

function makeConfirmContext() {
  const docListeners = {};
  const body = new FakeEl("body");
  const document = {
    body,
    createElement: (tag) => new FakeEl(tag),
    activeElement: null,
    addEventListener: (evt, fn) => { (docListeners[evt] = docListeners[evt] || []).push(fn); },
    removeEventListener: (evt, fn) => { if (docListeners[evt]) docListeners[evt] = docListeners[evt].filter((f) => f !== fn); },
    _trigger: (evt, evtObj) => { (docListeners[evt] || []).slice().forEach((fn) => fn(evtObj)); },
  };
  const sandbox = { console, Object, String, Promise, JSON, Array, Math, document, esc };
  vm.createContext(sandbox);
  vm.runInContext(CONFIRM_SRC + "\nthis.confirmSimple = confirmSimple; this.confirmTyped = confirmTyped;", sandbox);
  return { sandbox, document };
}

test("confirmSimple: Cancel gets initial focus (never the destructive Confirm), so a reflex Enter/Space never confirms", async () => {
  const { sandbox, document } = makeConfirmContext();
  const trigger = new FakeEl("button");
  document.activeElement = trigger;
  sandbox.confirmSimple("Archive campaign?", "Are you sure?");
  const overlay = document.body.children[document.body.children.length - 1];
  const box = overlay.children[0];
  const cancelBtn = box.querySelector("#confirm-simple-cancel");
  assert.ok(cancelBtn, "cancel button must exist");
  assert.equal(cancelBtn.focused, true, "Cancel must receive initial focus, not Confirm");
});

test("confirmSimple: Escape resolves false and restores focus to the trigger element", async () => {
  const { sandbox, document } = makeConfirmContext();
  const trigger = new FakeEl("button");
  document.activeElement = trigger;
  const p = sandbox.confirmSimple("Archive campaign?", "Are you sure?");
  document._trigger("keydown", { key: "Escape" });
  const result = await p;
  assert.equal(result, false);
  assert.equal(trigger.focused, true, "focus must be restored to whatever triggered the modal");
});

test("confirmSimple: Confirm resolves true", async () => {
  const { sandbox, document } = makeConfirmContext();
  const p = sandbox.confirmSimple("Archive campaign?", "Are you sure?");
  const overlay = document.body.children[document.body.children.length - 1];
  const box = overlay.children[0];
  box.querySelector("#confirm-simple-ok")._trigger("click");
  assert.equal(await p, true);
});

test("confirmSimple: the modal box carries role=dialog, aria-modal=true, aria-labelledby", () => {
  const { sandbox, document } = makeConfirmContext();
  sandbox.confirmSimple("Archive campaign?", "Are you sure?");
  const overlay = document.body.children[document.body.children.length - 1];
  const box = overlay.children[0];
  assert.equal(box.getAttribute("role"), "dialog");
  assert.equal(box.getAttribute("aria-modal"), "true");
  assert.ok(box.getAttribute("aria-labelledby"));
});

test("confirmTyped: Escape is bound at document level too (not just the input), same fix pattern as the Delete modal", () => {
  const src = extractFunctionSource(JS, "confirmTyped");
  assert.match(src, /document\.addEventListener\("keydown"/);
  assert.match(src, /document\.removeEventListener\("keydown"/);
});

test("confirmTyped: the modal box carries role=dialog, aria-modal=true, aria-labelledby", () => {
  const src = extractFunctionSource(JS, "confirmTyped");
  assert.match(src, /role="dialog"/);
  assert.match(src, /aria-modal="true"/);
  assert.match(src, /aria-labelledby="/);
});

test("Preview modal: carries role=dialog, aria-modal=true, aria-label", () => {
  const src = extractFunctionSource(JS, "gcRenderPreviewModal");
  assert.match(src, /setAttribute\("role",\s*"dialog"\)/);
  assert.match(src, /setAttribute\("aria-modal",\s*"true"\)/);
  assert.match(src, /setAttribute\("aria-label"/);
});

test("overflow menu: aria-expanded is synced true/false when the kebab opens/closes", () => {
  const start = JS.indexOf("function gcCloseAllRowMenus()");
  assert.notEqual(start, -1, "gcCloseAllRowMenus not found");
  const end = JS.indexOf("\n    document.addEventListener(\"keydown\", function (e) {\n      if (e.key === \"Escape\") gcCloseAllRowMenus();", start);
  const chunk = JS.slice(start, end === -1 ? start + 1200 : end + 300);
  assert.match(chunk, /setAttribute\("aria-expanded",\s*"false"\)/);
  assert.match(chunk, /setAttribute\("aria-expanded",\s*"true"\)/);
});

test("Escape closes any open row menu (list or Campaign Detail overflow), via the same gcCloseAllRowMenus used on outside-click", () => {
  const idx = JS.indexOf("document.addEventListener(\"keydown\", function (e) {\n      if (e.key === \"Escape\") gcCloseAllRowMenus();");
  assert.notEqual(idx, -1, "row-menu Escape handler not found");
});
