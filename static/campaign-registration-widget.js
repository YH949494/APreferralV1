/**
 * Campaign Registration — Mini App registration modal + reminder banner.
 *
 * ACTIVATION RULE (mirrors mission-pool-widget.js)
 * -------------------------------------------------
 * This widget always makes exactly one read on Mini App load
 * (GET /api/campaign-registration/active) and renders NOTHING unless the
 * server confirms there is a currently-open registration campaign the user
 * has not yet registered for. A normal Mini App open with no active
 * registration campaign costs one request and zero DOM changes.
 *
 * Telegram user id / username are NEVER read from this file for submission —
 * they are derived server-side from verified initData. This file only
 * collects the four user-entered fields and forwards initData for auth.
 *
 * A Campaign Registration deep link (t.me/<bot>?startapp=campaign_<id>) is a
 * NAVIGATION HINT ONLY: it scrolls/forces the modal open for that campaign if
 * it is the active one and the user is unregistered; it never changes what
 * the server allows.
 */
(function () {
  "use strict";

  var ROOT_ID = "campaign-registration-root";
  var API_TIMEOUT_MS = 12000;
  var START_PARAM_WAIT_MS = 1500;
  var START_PARAM_POLL_MS = 100;

  function tgApp() {
    try { return (window.Telegram && window.Telegram.WebApp) || null; } catch (e) { return null; }
  }

  function getInitData() {
    var tg = tgApp();
    try {
      if (tg && typeof tg.initData === "string" && tg.initData.length) return tg.initData;
    } catch (e) {}
    try { return new URLSearchParams(location.search).get("init_data") || ""; } catch (e) { return ""; }
  }

  function withInitData(path) {
    var initData = getInitData();
    if (!initData) return path;
    return path + (path.indexOf("?") === -1 ? "?" : "&") + "init_data=" + encodeURIComponent(initData);
  }

  function apiCall(method, path, body) {
    var controller = null;
    var timer = null;
    try { controller = new AbortController(); } catch (e) {}
    var opts = { method: method, credentials: "same-origin", headers: { "Content-Type": "application/json" } };
    if (body) opts.body = JSON.stringify(body);
    if (controller) opts.signal = controller.signal;
    if (controller) timer = setTimeout(function () { try { controller.abort(); } catch (e) {} }, API_TIMEOUT_MS);

    return fetch(withInitData(path), opts)
      .then(function (r) {
        return r.json().catch(function () { return null; }).then(function (d) {
          return { ok: r.ok, httpStatus: r.status, data: d };
        });
      })
      .catch(function () { return { ok: false, timeout: true, data: null }; })
      .then(function (res) { if (timer) clearTimeout(timer); return res; });
  }

  function apiGet(path) { return apiCall("GET", path, null); }
  function apiPost(path, body) { return apiCall("POST", path, body || {}); }

  // ---------------------------------------------------------------------
  // Deep link resolution
  // ---------------------------------------------------------------------

  var CAMPAIGN_PREFIX = "campaign_";
  var SAFE_PARAM = /^[A-Za-z0-9_-]{1,64}$/;

  function parseCampaignParam(raw) {
    if (typeof raw !== "string") return null;
    raw = raw.trim();
    if (raw.indexOf(CAMPAIGN_PREFIX) !== 0) return null; // e.g. mission_/attr_ params
    var id = raw.slice(CAMPAIGN_PREFIX.length);
    if (!id || !SAFE_PARAM.test(CAMPAIGN_PREFIX + id)) return null;
    return id;
  }

  function resolveCampaignRef() {
    var tg = tgApp();
    try {
      var sp = tg && tg.initDataUnsafe && tg.initDataUnsafe.start_param;
      var fromStart = parseCampaignParam(sp);
      if (fromStart) return fromStart;
    } catch (e) {}
    try {
      var qs = new URLSearchParams(location.search);
      var fromQsStart = parseCampaignParam(qs.get("tgWebAppStartParam"));
      if (fromQsStart) return fromQsStart;
      var explicit = qs.get("campaign");
      if (explicit && SAFE_PARAM.test(CAMPAIGN_PREFIX + explicit)) return explicit;
    } catch (e) {}
    return null;
  }

  function waitFor(check, cb) {
    var immediate = check();
    if (immediate) { cb(immediate); return; }
    var waited = 0;
    (function poll() {
      var found = check();
      if (found) { cb(found); return; }
      waited += START_PARAM_POLL_MS;
      if (waited >= START_PARAM_WAIT_MS) { cb(null); return; }
      var t = setTimeout(poll, START_PARAM_POLL_MS);
      try { if (t && typeof t.unref === "function") t.unref(); } catch (e) {}
    }());
  }

  function waitForCampaignRef(cb) { waitFor(resolveCampaignRef, cb); }
  function waitForInitData(cb) { waitFor(getInitData, cb); }

  // ---------------------------------------------------------------------
  // DOM helpers / styles
  // ---------------------------------------------------------------------

  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    attrs = attrs || {};
    Object.keys(attrs).forEach(function (k) {
      if (k === "class") node.className = attrs[k];
      else if (k === "text") node.textContent = attrs[k];
      else node.setAttribute(k, attrs[k]);
    });
    (children || []).forEach(function (c) { if (c) node.appendChild(c); });
    return node;
  }

  var stylesInjected = false;
  function injectStyles() {
    if (stylesInjected) return;
    stylesInjected = true;
    var style = document.createElement("style");
    style.textContent = [
      "#campaign-registration-root{margin:0 0 16px;}",
      ".cr-banner{display:flex;align-items:center;justify-content:space-between;gap:10px;background:rgba(255,138,61,.12);border:1px solid rgba(255,138,61,.4);border-radius:12px;padding:12px 14px;margin-bottom:16px;}",
      ".cr-banner-text{font-size:14px;font-weight:600;}",
      ".cr-banner-btn{border:none;border-radius:10px;padding:8px 14px;font-size:13px;font-weight:700;cursor:pointer;background:linear-gradient(90deg,#ff8a3d,#f5b63f);color:#1a1200;flex:0 0 auto;}",
      ".cr-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);display:flex;align-items:flex-end;justify-content:center;z-index:9999;}",
      "@media (min-width:520px){.cr-overlay{align-items:center;}}",
      ".cr-modal{background:#161616;color:#f5f5f5;border-radius:16px 16px 0 0;padding:20px;width:100%;max-width:480px;max-height:88vh;overflow-y:auto;box-sizing:border-box;}",
      "@media (min-width:520px){.cr-modal{border-radius:16px;}}",
      ".cr-title{font-weight:700;font-size:18px;margin-bottom:6px;}",
      ".cr-sub{font-size:13px;opacity:.8;margin-bottom:16px;line-height:1.4;}",
      ".cr-field{margin-bottom:12px;}",
      ".cr-label{font-size:12px;font-weight:600;margin-bottom:6px;display:block;}",
      ".cr-input{width:100%;box-sizing:border-box;border-radius:10px;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.04);color:inherit;padding:11px 13px;font-size:14px;font-family:inherit;}",
      ".cr-input.cr-invalid{border-color:#ff5c5c;}",
      ".cr-error{color:#ff8a8a;font-size:12px;margin-top:4px;display:none;}",
      ".cr-field.cr-invalid .cr-error{display:block;}",
      ".cr-actions{display:flex;flex-direction:column;gap:8px;margin-top:16px;}",
      ".cr-btn-primary{border:none;border-radius:10px;padding:13px 16px;font-size:15px;font-weight:700;cursor:pointer;background:linear-gradient(90deg,#ff8a3d,#f5b63f);color:#1a1200;}",
      ".cr-btn-primary[disabled]{opacity:.55;cursor:default;}",
      ".cr-btn-secondary{border:none;border-radius:10px;padding:11px 16px;font-size:14px;font-weight:600;cursor:pointer;background:rgba(255,255,255,.08);color:inherit;}",
      ".cr-msg{font-size:13px;margin-top:10px;line-height:1.4;}",
      ".cr-msg-error{color:#ff8a8a;}",
      ".cr-success{text-align:center;padding:8px 0;}",
      ".cr-success-title{font-size:19px;font-weight:700;margin-bottom:10px;}",
      ".cr-success-body{font-size:14px;line-height:1.5;opacity:.9;margin-bottom:18px;}",
    ].join("\n");
    document.head.appendChild(style);
  }

  // ---------------------------------------------------------------------
  // Field config
  // ---------------------------------------------------------------------

  var FIELD_DEFS = [
    { key: "full_name", label: "Full Name", placeholder: "Your full name", type: "text" },
    { key: "contact_number", label: "Contact Number", placeholder: "e.g. +60 12-345 6789", type: "tel" },
    { key: "country_region", label: "Country / Region", placeholder: "e.g. Malaysia, Singapore, ...", type: "text" },
    { key: "delivery_address", label: "Delivery Address", placeholder: "Full delivery address", type: "text" },
  ];

  var SUBMIT_ERROR_COPY = {
    missing_full_name: "Please enter your full name.",
    missing_contact_number: "Please enter a contact number.",
    missing_country_region: "Please enter your country / region.",
    missing_delivery_address: "Please enter your delivery address.",
    region_not_eligible: "Registration isn't open for your region yet.",
    channel_subscription_required: "Please join our official channel first, then try again.",
    subscription_check_failed: "We couldn't verify your channel subscription. Please try again.",
    registration_unavailable: "This registration is no longer available.",
  };

  function errorText(code) {
    return SUBMIT_ERROR_COPY[code] || "We couldn't complete registration. Please try again.";
  }

  var FIELD_ERROR_CODE = {};
  FIELD_DEFS.forEach(function (f) { FIELD_ERROR_CODE["missing_" + f.key] = f.key; });

  // ---------------------------------------------------------------------
  // Rendering: banner
  // ---------------------------------------------------------------------

  function renderBanner(root, view, onOpen) {
    root.innerHTML = "";
    injectStyles();
    var banner = el("div", { class: "cr-banner" }, [
      el("div", { class: "cr-banner-text", text: "🎁 Community Lucky Draw — Register Now" }),
    ]);
    var btn = el("button", { class: "cr-banner-btn", type: "button", text: "Register" });
    btn.addEventListener("click", function () { onOpen(); });
    banner.appendChild(btn);
    root.appendChild(banner);
  }

  // ---------------------------------------------------------------------
  // Rendering: modal
  // ---------------------------------------------------------------------

  function fieldRow(def, invalidMsg) {
    var input = el("input", {
      class: "cr-input", type: def.type, placeholder: def.placeholder,
      "data-field": def.key, maxlength: "300",
    });
    var errorEl = el("div", { class: "cr-error", text: invalidMsg || "This field is required." });
    var wrap = el("div", { class: "cr-field", "data-field-wrap": def.key }, [
      el("label", { class: "cr-label", text: def.label }),
      input,
      errorEl,
    ]);
    return { wrap: wrap, input: input, errorEl: errorEl };
  }

  function showSuccess(modal, view) {
    modal.innerHTML = "";
    var baseEntries = (view.campaign && view.campaign.base_entries) || 1;
    var body = el("div", { class: "cr-success" }, [
      el("div", { class: "cr-success-title", text: "✅ Registration Complete" }),
      el("div", {
        class: "cr-success-body",
        text: "You're entered into the Community Lucky Draw.\n" + baseEntries + " base entr" + (baseEntries === 1 ? "y" : "ies") + " received.",
      }),
    ]);
    var doneBtn = el("button", { class: "cr-btn-primary", type: "button", text: "Done" });
    doneBtn.addEventListener("click", function () { closeModal(); markRegisteredPermanently(view.campaign.campaign_id); });
    body.appendChild(doneBtn);
    modal.appendChild(body);
  }

  var activeOverlay = null;

  function closeModal() {
    if (activeOverlay && activeOverlay.parentNode) activeOverlay.parentNode.removeChild(activeOverlay);
    activeOverlay = null;
  }

  var registeredCampaignIds = {};
  function markRegisteredPermanently(campaignId) {
    registeredCampaignIds[campaignId] = true;
    var root = document.getElementById(ROOT_ID);
    if (root) root.innerHTML = "";
  }

  function requiredFieldSet(view) {
    var required = (view.campaign && view.campaign.required_fields) || FIELD_DEFS.map(function (f) { return f.key; });
    var set = {};
    required.forEach(function (k) { set[k] = true; });
    return set;
  }

  function openModal(view, onDismiss) {
    injectStyles();
    closeModal();

    var required = requiredFieldSet(view);
    var inputs = {};
    var rows = [];
    FIELD_DEFS.forEach(function (def) {
      if (!required[def.key]) return;
      var row = fieldRow(def);
      inputs[def.key] = row;
      rows.push(row.wrap);
    });

    var msg = el("div", { class: "cr-msg", style: "display:none;" });
    var submitBtn = el("button", { class: "cr-btn-primary", type: "button", text: "Register & Get 1 Entry" });
    var notNowBtn = el("button", { class: "cr-btn-secondary", type: "button", text: "Not now" });

    var modal = el("div", { class: "cr-modal" }, [
      el("div", { class: "cr-title", text: "🎁 " + (view.campaign.name || "Community Lucky Draw") }),
      el("div", { class: "cr-sub", text: "Register once to enter the Community Lucky Draw. It only takes a moment." }),
    ]);
    rows.forEach(function (r) { modal.appendChild(r); });
    modal.appendChild(msg);
    var actions = el("div", { class: "cr-actions" }, [submitBtn, notNowBtn]);
    modal.appendChild(actions);

    var overlay = el("div", { class: "cr-overlay" }, [modal]);
    document.body.appendChild(overlay);
    activeOverlay = overlay;

    notNowBtn.addEventListener("click", function () {
      closeModal();
      onDismiss();
    });

    submitBtn.addEventListener("click", function () {
      var payload = {};
      var firstInvalid = null;
      Object.keys(inputs).forEach(function (key) {
        var row = inputs[key];
        var value = String(row.input.value || "").trim();
        payload[key] = value;
        var invalid = required[key] && !value;
        row.wrap.classList.toggle("cr-invalid", invalid);
        if (invalid && !firstInvalid) firstInvalid = row.input;
      });
      if (firstInvalid) {
        msg.className = "cr-msg cr-msg-error";
        msg.textContent = "Please fill in all required fields.";
        msg.style.display = "block";
        firstInvalid.focus();
        return;
      }
      msg.style.display = "none";
      submitBtn.disabled = true;
      submitBtn.textContent = "Registering…";
      doRegister(view, payload, modal, inputs, submitBtn, msg);
    });
  }

  function doRegister(view, payload, modal, inputs, submitBtn, msg) {
    apiPost("/api/campaign-registration/" + encodeURIComponent(view.campaign.campaign_id) + "/register", payload)
      .then(function (res) {
        if (res.timeout) {
          msg.className = "cr-msg";
          msg.textContent = "We couldn't confirm your registration. Please try again.";
          msg.style.display = "block";
          submitBtn.disabled = false;
          submitBtn.textContent = "Register & Get 1 Entry";
          return;
        }
        var data = res.data || {};
        if (data.status === "ok") {
          showSuccess(modal, view);
          return;
        }
        var code = data.code;
        var fieldKey = FIELD_ERROR_CODE[code];
        if (fieldKey && inputs[fieldKey]) {
          inputs[fieldKey].wrap.classList.add("cr-invalid");
          inputs[fieldKey].input.focus();
        }
        msg.className = "cr-msg cr-msg-error";
        msg.textContent = errorText(code);
        msg.style.display = "block";
        submitBtn.disabled = false;
        submitBtn.textContent = "Register & Get 1 Entry";
      });
  }

  // ---------------------------------------------------------------------
  // Mount
  // ---------------------------------------------------------------------

  function dismiss(view) {
    apiPost("/api/campaign-registration/" + encodeURIComponent(view.campaign.campaign_id) + "/dismiss", {});
  }

  // After a dismissal, drop back to the reminder banner; opening it from the
  // banner re-opens the modal and, on a second dismissal, re-renders the
  // banner again — a small self-referential loop, named (not
  // arguments.callee, which "use strict" forbids) so it can call itself.
  function showBannerThenModal(root, view) {
    renderBanner(root, view, function () {
      openModal(view, function () {
        dismiss(view);
        showBannerThenModal(root, view);
      });
    });
  }

  function mount() {
    var root = document.getElementById(ROOT_ID);
    if (!root) return;

    waitForInitData(function () {
      apiGet("/api/campaign-registration/active").then(function (res) {
        var view = res.data || {};
        if (!res.ok || view.status !== "ok" || !view.campaign || view.registered) return;
        if (registeredCampaignIds[view.campaign.campaign_id]) return;

        waitForCampaignRef(function (deepLinkCampaignId) {
          var forceOpen = deepLinkCampaignId && deepLinkCampaignId === view.campaign.campaign_id;

          if (view.should_prompt || forceOpen) {
            openModal(view, function () {
              dismiss(view);
              apiGet("/api/campaign-registration/active").then(function (r2) {
                var v2 = r2.data || {};
                if (r2.ok && v2.status === "ok" && v2.campaign && !v2.registered) {
                  showBannerThenModal(root, v2);
                }
              });
            });
          } else {
            showBannerThenModal(root, view);
          }
        });
      });
    });
  }

  window.CampaignRegistrationWidget = {
    parseCampaignParam: parseCampaignParam,
    resolveCampaignRef: resolveCampaignRef,
    mount: mount,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount);
  } else {
    mount();
  }
}());
