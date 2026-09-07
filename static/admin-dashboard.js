/* APReferral Admin Dashboard — browser-only, session-cookie auth.
   No Telegram SDK. Reuses existing /api/admin/* endpoints. */
(function () {
  "use strict";

  var state = {
    view: "summary",
    summaryWindow: "7d",
    funnelWindow: "7d",
    abuseWindow: "7d",
    referralsWindow: "7d",
    voucherWindow: "7d",
    segmentsMode: "snapshot",
    segmentsMonth: "",
    segmentsFilter: "",
    roiMonth: "",
  };

  function $(sel, root) { return (root || document).querySelector(sel); }
  function $all(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function fmt(v) {
    if (v === null || v === undefined) return "—";
    if (typeof v === "number") return v.toLocaleString();
    return String(v);
  }

  function esc(v) {
    return String(v === null || v === undefined ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function api(path) {
    return fetch(path, { credentials: "same-origin", headers: { "Accept": "application/json" } })
      .then(function (r) {
        if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
        return r.json().then(function (j) {
          if (!r.ok) throw new Error((j && j.message) || ("HTTP " + r.status));
          return j;
        });
      });
  }

  function apiPost(path) {
    return fetch(path, { method: "POST", credentials: "same-origin", headers: { "Accept": "application/json" } })
      .then(function (r) {
        if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
        return r.json().then(function (j) {
          if (!r.ok) throw new Error((j && j.message) || ("HTTP " + r.status));
          return j;
        });
      });
  }

  function apiPostJson(path, body) {
    return fetch(path, {
      method: "POST", credentials: "same-origin",
      headers: { "Accept": "application/json", "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
      return r.json().then(function (j) { return { ok: r.ok, status: r.status, d: j }; });
    });
  }

  function apiPatchJson(path, body) {
    return fetch(path, {
      method: "PATCH", credentials: "same-origin",
      headers: { "Accept": "application/json", "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
      return r.json().then(function (j) { return { ok: r.ok, status: r.status, d: j }; });
    });
  }

  function apiPutJson(path, body) {
    return fetch(path, {
      method: "PUT", credentials: "same-origin",
      headers: { "Accept": "application/json", "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
      return r.json().then(function (j) { return { ok: r.ok, status: r.status, d: j }; });
    });
  }

  function apiPatchJson(path, body) {
    return fetch(path, {
      method: "PATCH", credentials: "same-origin",
      headers: { "Accept": "application/json", "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
      return r.json().then(function (j) { return { ok: r.ok, status: r.status, d: j }; });
    });
  }

  function apiDelete(path) {
    return fetch(path, { method: "DELETE", credentials: "same-origin", headers: { "Accept": "application/json" } })
      .then(function (r) {
        if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
        return r.json().then(function (j) { return { ok: r.ok, status: r.status, d: j }; });
      });
  }

  function banner(msg, kind) {
    var el = $("#global-banner");
    if (!msg) { el.innerHTML = ""; return; }
    el.innerHTML = '<div class="banner ' + (kind || "error") + '">' + msg + "</div>";
    if (kind === "ok") toast(msg, "success");
    else if (kind === "error") toast(msg, "error");
  }

  // ---------- Toast framework: every action gets non-blocking feedback ----------
  function toastStack() {
    var el = $("#toast-stack");
    if (!el) {
      el = document.createElement("div");
      el.id = "toast-stack";
      el.className = "toast-stack";
      document.body.appendChild(el);
    }
    return el;
  }
  function toast(msg, kind) {
    if (!msg) return;
    var stack = toastStack();
    var el = document.createElement("div");
    el.className = "toast toast-" + (kind || "success");
    el.textContent = msg;
    stack.appendChild(el);
    setTimeout(function () {
      el.classList.add("fade-out");
      setTimeout(function () { el.remove(); }, 260);
    }, 4200);
  }
  window.toast = toast;

  // ---------- Reusable button loading-state feedback ----------
  function btnStart(btn, loadingText) {
    if (!btn || btn.dataset.loading === "1") return false;
    btn.dataset.loading = "1";
    if (btn.dataset.originalText === undefined) btn.dataset.originalText = btn.textContent;
    btn.disabled = true;
    btn.classList.add("btn-loading");
    btn.innerHTML = '<span class="btn-spinner"></span>' + esc(loadingText || "Working...");
    return true;
  }
  function btnStop(btn) {
    if (!btn) return;
    btn.dataset.loading = "";
    btn.disabled = false;
    btn.classList.remove("btn-loading");
    if (btn.dataset.originalText !== undefined) btn.textContent = btn.dataset.originalText;
  }

  // ---------- Typed-confirmation modal for destructive actions ----------
  function confirmTyped(word, title, message) {
    return new Promise(function (resolve) {
      var overlay = document.createElement("div");
      overlay.className = "modal-overlay";
      overlay.innerHTML =
        '<div class="modal-box">' +
        '<h3>' + esc(title) + '</h3>' +
        '<p>' + esc(message) + ' Type <strong>' + esc(word) + '</strong> to confirm.</p>' +
        '<input class="filter-input" id="confirm-typed-input" placeholder="Type ' + esc(word) + '" autocomplete="off" />' +
        '<div class="modal-actions">' +
        '<button class="btn" id="confirm-typed-cancel">Cancel</button>' +
        '<button class="btn primary" id="confirm-typed-ok">Confirm</button>' +
        '</div></div>';
      document.body.appendChild(overlay);
      var input = overlay.querySelector("#confirm-typed-input");
      input.focus();
      function done(result) { overlay.remove(); resolve(result); }
      overlay.querySelector("#confirm-typed-cancel").addEventListener("click", function () { done(false); });
      overlay.querySelector("#confirm-typed-ok").addEventListener("click", function () {
        done((input.value || "").trim() === word);
      });
      overlay.addEventListener("click", function (e) { if (e.target === overlay) done(false); });
      input.addEventListener("keydown", function (e) {
        if (e.key === "Enter") done((input.value || "").trim() === word);
        if (e.key === "Escape") done(false);
      });
    });
  }

  // ---------- Plain Yes/No confirmation modal (non-typed) ----------
  function confirmSimple(title, message) {
    return new Promise(function (resolve) {
      var overlay = document.createElement("div");
      overlay.className = "modal-overlay";
      overlay.innerHTML =
        '<div class="modal-box">' +
        '<h3>' + esc(title) + '</h3>' +
        '<p>' + esc(message) + '</p>' +
        '<div class="modal-actions">' +
        '<button class="btn" id="confirm-simple-cancel">Cancel</button>' +
        '<button class="btn primary" id="confirm-simple-ok">Confirm</button>' +
        '</div></div>';
      document.body.appendChild(overlay);
      function done(result) { overlay.remove(); resolve(result); }
      overlay.querySelector("#confirm-simple-cancel").addEventListener("click", function () { done(false); });
      overlay.querySelector("#confirm-simple-ok").addEventListener("click", function () { done(true); });
      overlay.addEventListener("click", function (e) { if (e.target === overlay) done(false); });
    });
  }

  function setMeta(text) {
    var el = $("#dashboard-meta");
    if (el) el.textContent = text || "";
  }

  function renderMeta(d, fallbackWindow) {
    d = d || {};
    var parts = [];
    if (d.window_label || d.window || fallbackWindow) {
      parts.push("Window: " + (d.window_label || d.window || fallbackWindow));
    }
    if (d.generated_at || d.as_of) {
      parts.push("Last updated: " + new Date(d.generated_at || d.as_of).toLocaleString());
    }
    if (d.data_source) parts.push("Source: " + d.data_source);
    setMeta(parts.join(" · "));
  }

  function kpiCard(label, value, sub, missing) {
    var cls = missing ? "value missing" : "value";
    var val = missing ? "Data Not Available" : fmt(value);
    return '<div class="kpi"><div class="label">' + label + '</div>' +
      '<div class="' + cls + '">' + val + "</div>" +
      (sub ? '<div class="sub">' + sub + "</div>" : "") + "</div>";
  }

  function skeletonGrid(el, n) {
    var html = "";
    for (var i = 0; i < n; i++) html += '<div class="kpi"><div class="skeleton"></div></div>';
    el.innerHTML = html;
  }

  // ---------- Attention Required (Home) ----------
  function attentionItem(sev, title, sub) {
    return '<div class="attention-item sev-' + sev + '"><span class="attention-dot"></span>' +
      '<div class="attention-text"><strong>' + esc(title) + '</strong>' + (sub ? '<span>' + esc(sub) + '</span>' : '') + '</div></div>';
  }

  // Returns a Promise<[{sev, title, sub}]> — the raw signal list, shared by
  // the Home "Attention Required" panel and the topbar notification bell.
  function computeAttentionSignals() {
    return Promise.all([
      fetch("/v2/miniapp/admin/pools/summary", { credentials: "same-origin", headers: { "Accept": "application/json" } })
        .then(function (r) { return r.json(); }).catch(function () { return null; }),
      cbApi("/api/admin/campaign-builder/campaigns?status=active").catch(function () { return { ok: false }; }),
      fetch("/v2/miniapp/admin/affiliate/pending?status=PENDING_REVIEW", { credentials: "same-origin", headers: { "Accept": "application/json" } })
        .then(function (r) { return r.json(); }).catch(function () { return null; }),
    ]).then(function (results) {
      var pools = results[0], activeCampaigns = results[1], affiliatePending = results[2];
      var signals = [];

      if (pools && pools.items) {
        pools.items.forEach(function (p) {
          // p.claimable_available === null (with blocking_reason
          // "claimability_check_failed") means the claimability check
          // itself failed — never fall back to p.available/raw_available
          // as if it were claimable here, or a pool the bot genuinely
          // can't issue from would silently read as "fine".
          if (p.blocking_reason === "claimability_check_failed") {
            signals.push({ sev: "yellow", title: "Voucher pool status unknown: " + p.pool_id, sub: "Claimability check failed — see Pool Summary", view: "affiliatePools" });
            return;
          }
          // No claimable_available at all (older/degraded response, no
          // key) is the only case allowed to fall back to raw.
          var claimable = typeof p.claimable_available === "number" ? p.claimable_available : p.available;
          if (typeof claimable === "number" && claimable < 10) {
            signals.push({ sev: "red", title: "Voucher pool low: " + p.pool_id, sub: claimable + " code(s) claimable" + (p.blocking_reason ? " (" + p.blocking_reason + ")" : ""), view: "affiliatePools" });
          }
        });
      }

      if (activeCampaigns && activeCampaigns.ok && activeCampaigns.body && activeCampaigns.body.campaigns) {
        activeCampaigns.body.campaigns.filter(function (c) { return c.batch_status === "paused"; })
          .forEach(function (c) {
            signals.push({ sev: "yellow", title: "Campaign paused: " + c.campaign_name, sub: "Batch release is paused — resume from Campaign Center", view: "activeCampaigns" });
          });
      }

      if (affiliatePending && affiliatePending.items && affiliatePending.items.length) {
        signals.push({ sev: "yellow", title: affiliatePending.items.length + " affiliate reward(s) pending approval", sub: "Review in Affiliate Center → Pending Approvals", view: "affiliatePending" });
      }

      return signals;
    });
  }

  function loadAttentionRequired() {
    var el = $("#attention-required");
    if (!el) return;
    el.innerHTML = '<div class="attention-empty">Checking for issues…</div>';
    computeAttentionSignals().then(function (signals) {
      el.innerHTML = signals.length
        ? signals.map(function (s) { return attentionItem(s.sev, s.title, s.sub); }).join("")
        : '<div class="attention-empty">✅ Nothing needs attention right now.</div>';
    }).catch(function () {
      el.innerHTML = '<div class="attention-empty">Could not check for issues.</div>';
    });
  }

  // ---------- Notification bell (topbar) ----------
  function refreshNotifBell() {
    var countEl = $("#notif-count");
    var dropdownEl = $("#notif-dropdown");
    if (!countEl) return;
    computeAttentionSignals().then(function (signals) {
      lastNotifSignals = signals;
      if (signals.length) {
        countEl.textContent = signals.length > 9 ? "9+" : String(signals.length);
        countEl.classList.remove("hidden");
      } else {
        countEl.classList.add("hidden");
      }
      if (dropdownEl && !dropdownEl.classList.contains("hidden")) renderNotifDropdown();
    }).catch(function () { /* silent — bell is a passive convenience, not the source of truth */ });
  }

  var lastNotifSignals = [];
  function renderNotifDropdown() {
    var dropdownEl = $("#notif-dropdown");
    if (!dropdownEl) return;
    dropdownEl.innerHTML = lastNotifSignals.length
      ? lastNotifSignals.map(function (s) {
          return '<div class="attention-item sev-' + s.sev + '" onclick="goToViewAndClick(\'' + s.view + '\',\'\')" tabindex="0" role="button">' +
            '<span class="attention-dot"></span><div class="attention-text"><strong>' + esc(s.title) + '</strong>' +
            (s.sub ? '<span>' + esc(s.sub) + '</span>' : '') + '</div></div>';
        }).join("")
      : '<div class="attention-empty">✅ Nothing needs attention right now.</div>';
  }

  function bindNotifBell() {
    var bell = $("#notif-bell");
    var dropdownEl = $("#notif-dropdown");
    if (!bell || !dropdownEl) return;
    bell.addEventListener("click", function (e) {
      e.stopPropagation();
      var opening = dropdownEl.classList.contains("hidden");
      dropdownEl.classList.toggle("hidden", !opening);
      if (opening) renderNotifDropdown();
    });
    document.addEventListener("click", function (e) {
      if (!dropdownEl.classList.contains("hidden") && !dropdownEl.contains(e.target) && e.target !== bell) {
        dropdownEl.classList.add("hidden");
      }
    });
    refreshNotifBell();
    setInterval(refreshNotifBell, 90000);
  }

  // ---------- Summary ----------
  function loadSummary(refresh) {
    setMeta("Window: " + state.summaryWindow + " · Loading…");
    ["cards-users", "cards-community", "cards-referrals", "cards-vouchers", "cards-system"].forEach(function (id) {
      skeletonGrid($("#" + id), 4);
    });
    loadAttentionRequired();
    api("/api/admin/dashboard/summary?window=" + encodeURIComponent(state.summaryWindow) + (refresh ? "&refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, state.summaryWindow);
        banner(d.partial_errors ? ("Some metrics failed to load: " + d.partial_errors.join("; ")) : null, "warn");

        var u = d.users;
        function tgCard(label, val, stale, cachedAt, extraClass) {
          var missing = val === null || val === undefined;
          var staleHtml = stale
            ? ' <span class="tag heuristic" title="Last updated: ' + (cachedAt ? new Date(cachedAt).toLocaleString() : "unknown") + '">stale</span>'
            : '';
          var sub = stale
            ? "Telegram API unavailable — last known value"
            : (cachedAt ? ("cached " + new Date(cachedAt).toLocaleTimeString()) : "");
          return '<div class="kpi ' + (extraClass || "") + '"><div class="label">' + label + staleHtml + '</div>' +
            '<div class="' + (missing ? "value missing" : "value") + '">' + (missing ? "—" : fmt(val)) + '</div>' +
            '<div class="sub">' + sub + '</div></div>';
        }
        $("#cards-users").innerHTML =
          tgCard("OFFICIAL CHANNEL SUBSCRIBERS", u.official_channel_subscribers, u.official_channel_subscribers_stale, u.official_channel_subscribers_cached_at, "kpi-headline") +
          tgCard("ADVANTPLAY CHATROOM MEMBERS", u.chatroom_members, u.chatroom_members_stale, u.chatroom_members_cached_at, "kpi-secondary") +
          kpiCard("Registered Users", u.registered) +
          kpiCard("Active Users", u.active_selected, d.window_label) +
          kpiCard("Active 7d", u.active_7d) +
          kpiCard("Active 30d", u.active_30d) +
          kpiCard("Active Today", u.active_today);

        $("#cards-community").innerHTML =
          kpiCard("Check-ins", d.community.checkins_selected, d.window_label) +
          kpiCard("Check-ins Today", d.community.checkins_today) +
          kpiCard("Welcome Eligible", d.welcome.eligible) +
          kpiCard("Welcome Claimed", d.welcome.claimed,
            d.welcome.conversion_pct !== null ? (d.welcome.conversion_pct + "% conversion") : "");

        $("#cards-referrals").innerHTML =
          kpiCard("Pending Referrals", d.referrals.pending) +
          kpiCard("Qualified", d.referrals.qualified, d.window_label) +
          kpiCard("Revoked Referrals", d.referrals.revoked);

        $("#cards-vouchers").innerHTML =
          kpiCard("Active Campaigns", d.vouchers.active_campaigns) +
          kpiCard("Claims", d.vouchers.claims, d.window_label) +
          kpiCard("Remaining Codes", d.vouchers.remaining_codes) +
          kpiCard("Affiliate Pending", d.affiliate.pending_review) +
          kpiCard("Affiliate Approved", d.affiliate.approved, d.window_label);

        var sys = d.system;
        var st = sys.worker_status || "unknown";
        var dot = '<span class="status-dot ' + st + '"></span>';
        var ageTxt = sys.snapshot_age_seconds === null ? "no heartbeat" : (sys.snapshot_age_seconds + "s ago");
        $("#cards-system").innerHTML =
          '<div class="kpi"><div class="label">Worker Status</div><div class="value" style="font-size:18px">' + dot + st + "</div></div>" +
          kpiCard("Snapshot Freshness", ageTxt) +
          kpiCard("Last Snapshot Publish", sys.last_snapshot_publish ? new Date(sys.last_snapshot_publish).toLocaleString() : "—") +
          kpiCard("Last Scheduler Run", sys.last_scheduler_run ? new Date(sys.last_scheduler_run).toLocaleString() : "—");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Window: " + state.summaryWindow + " · Failed to update");
          ["cards-users", "cards-community", "cards-referrals", "cards-vouchers", "cards-system"].forEach(function (id) {
            $("#" + id).innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          });
        }
      });
  }

  // ---------- Funnel ----------
  function loadFunnel(refresh) {
    setMeta("Window: " + state.funnelWindow + " · Loading…");
    var body = $("#funnel-body");
    body.innerHTML = '<div class="loading">Loading funnel…</div>';
    api("/api/admin/dashboard/funnel?window=" + encodeURIComponent(state.funnelWindow) + (refresh ? "&refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, state.funnelWindow);
        if (!(d.stages || []).length) {
          body.innerHTML = '<div class="empty">No data for selected period.</div>';
          return;
        }
        var maxCount = 0;
        d.stages.forEach(function (s) { if (typeof s.count === "number" && s.count > maxCount) maxCount = s.count; });
        var rows = d.stages.map(function (s) {
          var missing = s.count === null;
          var barPct = (!missing && maxCount > 0) ? Math.round(100 * s.count / maxCount) : 0;
          var conv = s.conversion_pct === null || s.conversion_pct === undefined ? "—" : s.conversion_pct + "%";
          var drop = s.dropoff_pct === null || s.dropoff_pct === undefined ? "—" : s.dropoff_pct + "%";
          return "<tr>" +
            "<td><strong>" + s.name + "</strong>" + (s.note ? '<div class="note">' + s.note + "</div>" : "") + "</td>" +
            "<td>" + (missing ? '<span class="value missing">Data Not Available</span>' : fmt(s.count)) + "</td>" +
            '<td><div class="bar-wrap"><div class="bar" style="width:' + barPct + '%"></div></div></td>' +
            "<td>" + conv + "</td>" +
            "<td>" + drop + "</td>" +
            '<td><span class="tag ' + s.data_quality + '">' + s.data_quality + "</span></td>" +
            "</tr>";
        }).join("");
        body.innerHTML =
          '<table class="funnel-table"><thead><tr>' +
          "<th>Stage</th><th>Users</th><th></th><th>Conversion</th><th>Drop-off</th><th>Quality</th>" +
          "</tr></thead><tbody>" + rows + "</tbody></table>" +
          '<div class="note">Conversion is relative to the first stage with data.</div>';
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Window: " + state.funnelWindow + " · Failed to update");
          body.innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
        }
      });
  }

  // ---------- Abuse ----------
  function loadAbuse(refresh) {
    setMeta("Window: " + state.abuseWindow + " · Loading…");
    var grid = $("#cards-abuse");
    skeletonGrid(grid, 5);
    $("#abuse-notes").innerHTML = "";
    api("/api/admin/dashboard/abuse?window=" + encodeURIComponent(state.abuseWindow) + (refresh ? "&refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, state.abuseWindow);
        var m = d.metrics;
        function card(label, item) {
          var missing = item.value === null || item.value === undefined;
          return '<div class="kpi"><div class="label">' + label +
            ' <span class="tag ' + item.data_quality + '">' + item.data_quality + "</span></div>" +
            '<div class="' + (missing ? "value missing" : "value") + '">' + (missing ? "Data Not Available" : fmt(item.value)) + "</div>" +
            '<div class="sub">' + item.note + "</div></div>";
        }
        grid.innerHTML =
          card("Repeat Claimers", m.repeat_claimers) +
          card("Blocked IPs", m.blocked_ips) +
          card("Suspicious Referrers", m.suspicious_referrers) +
          card("Voucher Hunters", m.voucher_hunter_count) +
          card("Welcome Abuse", m.welcome_abuse_count);
        $("#abuse-notes").innerHTML =
          (d.partial_errors ? '<div class="banner warn">Partial errors: ' + d.partial_errors.join("; ") + "</div>" : "");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Window: " + state.abuseWindow + " · Failed to update");
          grid.innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          $("#abuse-notes").innerHTML = "";
        }
      });
  }

  function esc(s) {
    return String(s === null || s === undefined ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }
  function dt(v) { return v ? new Date(v).toLocaleString() : "—"; }
  function pct(v) { return (v === null || v === undefined) ? "—" : v + "%"; }

  function dqCard(label, m) {
    m = m || {};
    var missing = m.value === null || m.value === undefined || m.data_quality === "missing";
    var dq = m.data_quality && m.data_quality !== "exact"
      ? ' <span class="tag ' + m.data_quality + ' dq">' + m.data_quality + "</span>" : "";
    var val = missing ? "Data Not Available" : fmt(m.value);
    return '<div class="kpi"><div class="label">' + label + dq + "</div>" +
      '<div class="' + (missing ? "value missing" : "value") + '">' + val + "</div>" +
      (m.note ? '<div class="sub">' + esc(m.note) + "</div>" : "") + "</div>";
  }

  function statePanel(elId, kind, msg) {
    var el = $("#" + elId);
    if (!el) return;
    if (kind === "loading") {
      var rows = "";
      for (var i = 0; i < 3; i++) rows += '<div class="skeleton-row"><div class="skeleton"></div><div class="skeleton"></div></div>';
      el.innerHTML = '<div class="skeleton-stack" aria-busy="true" aria-label="' + esc(msg || "Loading") + '">' + rows + "</div>";
      return;
    }
    if (kind === "empty") {
      el.innerHTML = emptyState(msg);
      return;
    }
    el.innerHTML = '<div class="' + kind + '">' + esc(msg) + "</div>";
  }

  // Empty-state CTAs may live on a different nav view than the one currently
  // shown (e.g. "No active campaigns" shown on Overview, but the create
  // wizard lives under Create Campaign) — navigate there first, then click.
  window.goToViewAndClick = function (view, btnId) {
    switchView(view);
    setTimeout(function () {
      var b = document.getElementById(btnId);
      if (b) b.click();
    }, 60);
  };

  // title/subtitle/CTA empty state; accepts a plain string (legacy callers) or
  // { icon, title, sub, ctaHtml } for a richer empty state with an action button.
  function emptyState(msg) {
    if (typeof msg === "string" || !msg) {
      return '<div class="empty-state"><div class="empty-state-icon">' + (msg && /fail|error/i.test(msg) ? "⚠" : "✅") + '</div>' +
        '<div class="empty-state-sub">' + esc(msg || "Nothing here yet.") + '</div></div>';
    }
    return '<div class="empty-state">' +
      '<div class="empty-state-icon">' + esc(msg.icon || "✅") + '</div>' +
      '<div class="empty-state-title">' + esc(msg.title || "") + '</div>' +
      (msg.sub ? '<div class="empty-state-sub">' + esc(msg.sub) + '</div>' : "") +
      (msg.ctaHtml || "") + '</div>';
  }

  function expandTable(headers, rows) {
    if (!rows.length) return '<div class="empty">No data for selected period.</div>';
    var head = "<thead><tr>" + headers.map(function (h) {
      return '<th' + (h.num ? ' class="num"' : "") + ">" + esc(h.label) + "</th>";
    }).join("") + "</tr></thead>";
    var body = rows.map(function (r, i) {
      var tds = r.cells.map(function (c) {
        return '<td' + (c.num ? ' class="num"' : "") + ">" + (c.html || esc(c.text)) + "</td>";
      }).join("");
      var main = '<tr class="row-main" data-row="' + i + '" data-search="' + esc(r.search || "") + '">' + tds + "</tr>";
      var detail = '<tr class="row-detail hidden" data-detail="' + i + '"><td colspan="' + headers.length +
        '"><div class="detail-pad">' + (r.detailHtml || "") + "</div></td></tr>";
      return main + detail;
    }).join("");
    return '<table class="data-table">' + head + "<tbody>" + body + "</tbody></table>";
  }

  function bindExpand(containerId, onExpand) {
    var c = $("#" + containerId);
    if (!c) return;
    $all("tr.row-main", c).forEach(function (tr) {
      tr.addEventListener("click", function (e) {
        if (e.target.classList.contains("clickable")) return;
        var i = tr.dataset.row;
        var detail = $('tr.row-detail[data-detail="' + i + '"]', c);
        var opening = detail.classList.contains("hidden");
        detail.classList.toggle("hidden");
        tr.classList.toggle("open", opening);
        if (opening && onExpand && !detail.dataset.loaded) {
          detail.dataset.loaded = "1";
          onExpand(tr, $(".detail-pad", detail));
        }
      });
    });
  }

  function applyFilter(containerId, q) {
    q = (q || "").trim().toLowerCase();
    var c = $("#" + containerId);
    if (!c) return;
    $all("tr.row-main", c).forEach(function (tr) {
      var hit = !q || (tr.dataset.search || "").toLowerCase().indexOf(q) !== -1;
      tr.style.display = hit ? "" : "none";
      var detail = $('tr.row-detail[data-detail="' + tr.dataset.row + '"]', c);
      if (detail && !hit) { detail.classList.add("hidden"); tr.classList.remove("open"); }
    });
  }

  function kvBlock(title, pairs) {
    var rows = pairs.map(function (p) {
      return '<div class="kv"><span class="k">' + esc(p[0]) + '</span><span class="v">' +
        (p[2] || esc(p[1] === null || p[1] === undefined || p[1] === "" ? "—" : p[1])) + "</span></div>";
    }).join("");
    return '<div class="detail-block"><h4>' + esc(title) + "</h4>" + rows + "</div>";
  }

  function userLink(uid, label) {
    if (uid === null || uid === undefined) return "—";
    return '<span class="clickable" data-user="' + esc(uid) + '">' + esc(label || uid) + "</span>";
  }
  function bindUserLinks(containerId) {
    $all(".clickable[data-user]", $("#" + containerId)).forEach(function (el) {
      el.addEventListener("click", function (e) {
        e.stopPropagation();
        switchView("users");
        $("#user-search").value = el.dataset.user;
        loadUser(el.dataset.user);
      });
    });
  }

  function loadVouchers(refresh) {
    setMeta("Window: " + state.voucherWindow + " · Loading…");
    skeletonGrid($("#cards-voucher-summary"), 6);
    statePanel("vouchers-body", "loading", "Loading campaigns…");
    api("/api/admin/dashboard/vouchers?window=" + encodeURIComponent(state.voucherWindow) + (refresh ? "&refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, state.voucherWindow);
        var s = d.summary;
        $("#cards-voucher-summary").innerHTML =
          dqCard("Active Campaigns", s.active_campaigns) + dqCard("Upcoming", s.upcoming_campaigns) +
          dqCard("Ended", s.ended_campaigns) + dqCard("Total Codes", s.total_codes) +
          dqCard("Claimed Codes", s.claimed_codes) + dqCard("Remaining Codes", s.remaining_codes) +
          dqCard("Claim Rate", s.claim_rate_pct.value === null ? s.claim_rate_pct : { value: s.claim_rate_pct.value + "%", data_quality: s.claim_rate_pct.data_quality }) +
          dqCard("Failed Claims", s.failed_claims) + dqCard("Repeat Claimers", s.repeat_claimers) +
          dqCard("Welcome Claims", s.welcome_claims);

        var rows = (d.campaigns || []).map(function (c) {
          var det = c.detail || {};
          var fr = (det.failure_reasons || []).map(function (f) {
            return '<tr><td>' + esc(f.reason) + '</td><td class="num">' + fmt(f.count) + "</td></tr>";
          }).join("") || '<tr><td colspan="2">No failures recorded.</td></tr>';
          var detailHtml = '<div class="detail-grid">' +
            kvBlock("Campaign", [["Drop ID", c.drop_id], ["Type", c.type], ["Start", dt(c.starts_at)], ["End", dt(c.ends_at)]]) +
            kvBlock("Claim Attempts", [["Claimed", (det.claim_attempts || {}).claimed], ["Failed", (det.claim_attempts || {}).failed], ["Total Codes", (det.claim_attempts || {}).total_codes]]) +
            kvBlock("Pool Breakdown", [["Public Remaining", (det.pool_breakdown || {}).public_remaining], ["My Remaining", (det.pool_breakdown || {}).my_remaining]]) +
            '<div class="detail-block"><h4>Failure Reasons</h4><table class="mini-table"><thead><tr><th>Reason</th><th class="num">Count</th></tr></thead><tbody>' + fr + "</tbody></table></div>" +
            '<div class="detail-block"><h4>Metadata</h4><pre class="payload">' + esc(JSON.stringify(det.metadata || {}, null, 2)) + "</pre></div>" +
            "</div>";
          return {
            search: c.name + " " + c.status + " " + c.drop_id,
            cells: [
              { html: "<strong>" + esc(c.name) + "</strong>" },
              { html: '<span class="pill ' + esc(c.status) + '">' + esc(c.status) + "</span>" },
              { text: dt(c.starts_at) + " → " + dt(c.ends_at) },
              { num: true, text: fmt(c.total_codes) }, { num: true, text: fmt(c.claimed) },
              { num: true, text: fmt(c.remaining) }, { num: true, text: pct(c.claim_rate_pct) }
            ],
            detailHtml: detailHtml
          };
        });
        $("#vouchers-body").innerHTML = expandTable(
          [{ label: "Campaign" }, { label: "Status" }, { label: "Start / End" }, { label: "Total", num: true },
           { label: "Claimed", num: true }, { label: "Remaining", num: true }, { label: "Claim %", num: true }], rows);
        bindExpand("vouchers-body");
        if (d.partial_errors) banner("Some voucher metrics degraded: " + d.partial_errors.join("; "), "warn");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Window: " + state.voucherWindow + " · Failed to update");
          $("#cards-voucher-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("vouchers-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  function loadReferrals(refresh) {
    setMeta("Window: " + state.referralsWindow + " · Loading…");
    skeletonGrid($("#cards-referrals-summary"), 7);
    statePanel("referrals-body", "loading", "Loading referrers…");
    api("/api/admin/dashboard/referrals?window=" + encodeURIComponent(state.referralsWindow) + (refresh ? "&refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, state.referralsWindow);
        var s = d.summary;
        $("#cards-referrals-summary").innerHTML =
          dqCard("Total Referrers", s.total_referrers) + dqCard("Total Invitees", s.total_invitees) +
          dqCard("Qualified", s.qualified_referrals) + dqCard("Pending", s.pending_referrals) +
          dqCard("Revoked", s.revoked_referrals) + dqCard("Invitee Check-in Rate", s.invitee_checkin_rate_pct) +
          dqCard("Invitee Welcome Claim Rate", s.invitee_welcome_claim_rate_pct);

        var rows = (d.referrers || []).map(function (r) {
          return {
            search: r.referrer_id + " " + (r.username || ""),
            cells: [
              { html: userLink(r.referrer_id, (r.username ? "@" + r.username : r.referrer_id)) },
              { num: true, text: fmt(r.invitees) }, { num: true, text: fmt(r.qualified) },
              { num: true, text: fmt(r.pending) }, { num: true, text: fmt(r.welcome_claimed) },
              { num: true, text: fmt(r.checkin_completed) }, { num: true, text: pct(r.quality_pct) }
            ],
            detailHtml: '<div class="loading">Loading invitees…</div>',
            _uid: r.referrer_id
          };
        });
        $("#referrals-body").innerHTML = expandTable(
          [{ label: "Referrer" }, { label: "Invitees", num: true }, { label: "Qualified", num: true },
           { label: "Pending", num: true }, { label: "Welcome", num: true }, { label: "Check-in", num: true },
           { label: "Quality %", num: true }], rows);
        bindExpand("referrals-body", function (tr, pad) {
          var uid = rows[tr.dataset.row]._uid;
          api("/api/admin/dashboard/referrals/detail?user_id=" + encodeURIComponent(uid))
            .then(function (det) {
              var inv = (det.invitees || []).map(function (x) {
                return "<tr><td>" + userLink(x.invitee_id, x.username ? "@" + x.username : x.invitee_id) + "</td><td>" + dt(x.join_date) +
                  '</td><td><span class="pill ' + esc(x.referral_status) + '">' + esc(x.referral_status) + "</span></td><td>" +
                  (x.checkin_completed ? "✓" : "—") + "</td><td>" + (x.welcome_claimed ? "✓" : "—") + "</td></tr>";
              }).join("") || '<tr><td colspan="5">No invitees.</td></tr>';
              pad.innerHTML = '<table class="mini-table"><thead><tr><th>Invitee</th><th>Join Date</th><th>Status</th><th>Check-in</th><th>Welcome</th></tr></thead><tbody>' + inv + "</tbody></table>";
              bindUserLinks("referrals-body");
            })
            .catch(function (e) { pad.innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>"; });
        });
        bindUserLinks("referrals-body");
        if (d.note) $("#referrals-body").insertAdjacentHTML("beforeend", '<div class="note">' + esc(d.note) + "</div>");
        if (d.partial_errors) banner("Some referral metrics degraded: " + d.partial_errors.join("; "), "warn");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Window: " + state.referralsWindow + " · Failed to update");
          $("#cards-referrals-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("referrals-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  function loadAffiliate(refresh) {
    setMeta("Loading…");
    skeletonGrid($("#cards-affiliate-summary"), 4);
    statePanel("affiliate-body", "loading", "Loading affiliates…");
    $("#affiliate-pools").innerHTML = "";
    api("/api/admin/dashboard/affiliate" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, "all time");
        var s = d.summary;
        $("#cards-affiliate-summary").innerHTML =
          dqCard("Pending Review", s.pending_review) + dqCard("Approved", s.approved) +
          dqCard("Issued", s.issued) + dqCard("Rejected", s.rejected);

        var pools = (d.pool_availability || []).map(function (p) {
          var claimable = typeof p.currently_claimable === "number" ? fmt(p.currently_claimable) : "—";
          return "<tr><td>" + esc(p.pool_id) + '</td><td class="num">' + fmt(p.available) + '</td><td class="num">' + claimable + '</td><td class="num">' + fmt(p.issued) + "</td></tr>";
        }).join("");
        var mi = ((d.monthly_issuance || {}).by_status || []).map(function (m) {
          return "<tr><td>" + esc(m.status) + '</td><td class="num">' + fmt(m.count) + "</td></tr>";
        }).join("") || '<tr><td colspan="2">No issuance this month.</td></tr>';
        $("#affiliate-pools").innerHTML = '<div class="detail-grid">' +
          '<div class="detail-block"><h4>Pool Availability</h4><table class="mini-table"><thead><tr><th>Pool</th><th class="num">Total Available</th><th class="num">Currently Claimable</th><th class="num">Issued</th></tr></thead><tbody>' + pools + "</tbody></table></div>" +
          '<div class="detail-block"><h4>Monthly Issuance (' + esc((d.monthly_issuance || {}).month_key || "") + ')</h4><table class="mini-table"><thead><tr><th>Status</th><th class="num">Count</th></tr></thead><tbody>' + mi + "</tbody></table></div></div>";

        var rows = (d.affiliates || []).map(function (a) {
          return {
            search: a.user_id + " " + (a.tier || "") + " " + (a.status || ""),
            cells: [
              { html: userLink(a.user_id) }, { text: a.tier || "—" },
              { html: '<span class="pill ' + esc((a.status || "neutral").toLowerCase()) + '">' + esc(a.status || "—") + "</span>" },
              { num: true, text: fmt(a.qualified_count) },
              { text: a.conversion_pct === null ? "Data Not Available" : pct(a.conversion_pct) },
              { text: dt(a.updated_at) }
            ],
            detailHtml: '<div class="loading">Loading ledger…</div>', _uid: a.user_id
          };
        });
        $("#affiliate-body").innerHTML = expandTable(
          [{ label: "User" }, { label: "Tier" }, { label: "Status" }, { label: "Qualified", num: true },
           { label: "Conversion" }, { label: "Updated" }], rows);
        bindExpand("affiliate-body", function (tr, pad) {
          var uid = rows[tr.dataset.row]._uid;
          api("/api/admin/dashboard/affiliate/detail?user_id=" + encodeURIComponent(uid))
            .then(function (det) { pad.innerHTML = affiliateDetailHtml(det); })
            .catch(function (e) { pad.innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>"; });
        });
        bindUserLinks("affiliate-body");
        if (d.note) $("#affiliate-body").insertAdjacentHTML("beforeend", '<div class="note">' + esc(d.note) + "</div>");
        if (d.partial_errors) banner("Some affiliate metrics degraded: " + d.partial_errors.join("; "), "warn");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          $("#cards-affiliate-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          $("#affiliate-pools").innerHTML = "";
          statePanel("affiliate-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  function loadReactivation(refresh) {
    setMeta("Loading…");
    skeletonGrid($("#cards-reactivation-summary"), 4);
    statePanel("reactivation-body", "loading", "Loading campaign...");
    api("/api/admin/channel-reactivation/summary" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, "all time");
        $("#reactivation-status").textContent = d.active ? "Active" : "Paused";
        $("#cards-reactivation-summary").innerHTML =
          kpiCard("Eligible Users", d.eligible_users) +
          kpiCard("Messages Sent", d.messages_sent, fmt(d.messages_sent_today) + " today") +
          kpiCard("Successful Verifications", d.successful_verifications) +
          kpiCard("Tier 1 Completed", d.tier1_completed || 0) +
          kpiCard("Tier 1 Issued", d.tier1_issued || 0) +
          kpiCard("Tier 2 Completed", d.tier2_completed || 0) +
          kpiCard("Tier 2 Issued", d.tier2_issued || 0) +
          kpiCard("Tier 3 Completed", d.tier3_completed || 0) +
          kpiCard("Tier 3 Issued", d.tier3_issued || 0) +
          kpiCard("Out of Stock", ((d.out_of_stock_by_tier || {}).tier1 || 0) + ((d.out_of_stock_by_tier || {}).tier2 || 0) + ((d.out_of_stock_by_tier || {}).tier3 || 0));
        var pools = (d.reactivation_pools || []).map(function (p) {
          return '<tr><td>' + esc(p.pool_id) + '</td><td class="num">' + fmt(p.available) + '</td><td class="num">' + fmt(p.issued) + '</td></tr>';
        }).join("") || '<tr><td colspan="3">No pool rows.</td></tr>';
        $("#reactivation-body").innerHTML =
          '<div class="detail-grid">' +
          kvBlock("Safety Limits", [["Daily Send Limit", d.daily_limit], ["Messages Sent Today", d.messages_sent_today], ["Per-Minute Limit", d.minute_limit]]) +
          kvBlock("Campaign", [["Campaign ID", d.campaign_id], ["Status", d.active ? "Active" : "Paused"], ["Updated", dt(d.updated_at)]]) +
          kvBlock("Out of Stock", [["Tier 1", (d.out_of_stock_by_tier || {}).tier1 || 0], ["Tier 2", (d.out_of_stock_by_tier || {}).tier2 || 0], ["Tier 3", (d.out_of_stock_by_tier || {}).tier3 || 0]]) +
          '</div>' +
          '<div class="detail-block" style="margin-top:12px;"><h4>Reactivation Voucher Pools</h4><table class="mini-table"><thead><tr><th>Pool</th><th class="num">Available</th><th class="num">Issued</th></tr></thead><tbody>' + pools + '</tbody></table></div>' +
          '<div class="detail-block" style="margin-top:12px;"><h4>Upload Reactivation Codes</h4><div class="filters"><select id="reactivation-upload-pool" class="filter-input"><option>COMEBACK_T1</option><option>COMEBACK_T2</option><option>COMEBACK_T3</option></select><textarea id="reactivation-upload-codes" class="filter-input" rows="5" placeholder="code\nABC123\nABC456" style="min-width:260px;"></textarea><button class="btn" id="reactivation-upload-btn">Upload Codes</button></div><div class="note" id="reactivation-upload-status"></div></div>';
        bindReactivationUpload();
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          $("#cards-reactivation-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("reactivation-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  var JOURNEY_STATUS_META = {
    disabled: { label: "DISABLED", pill: "neutral" },
    test_only: { label: "TEST ONLY", pill: "pending" },
    live: { label: "LIVE", pill: "active" },
    scheduled: { label: "SCHEDULED", pill: "upcoming" },
    expired: { label: "EXPIRED", pill: "expired" },
    config_error: { label: "CONFIG ERROR", pill: "rejected" },
  };

  function loadReactivationJourneyConfig() {
    var host = $("#reactivation-journey-config");
    if (!host) return;
    host.innerHTML = '<div class="note">Loading rollout config...</div>';
    fetch("/api/admin/reactivation/journey/config", { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then(function (r) { return r.json().then(function (j) { if (!r.ok) throw new Error(j.message || "HTTP " + r.status); return j; }); })
      .then(function (j) {
        renderReactivationJourneyStatus(j.config || {});
        renderReactivationJourneyConfig(j.config || {});
      })
      .catch(function (e) {
        host.innerHTML = '<div class="banner error">Failed to load config: ' + esc(e.message) + "</div>";
        var statusHost = $("#reactivation-journey-status");
        if (statusHost) statusHost.innerHTML = '<div class="banner error">Failed to load journey status: ' + esc(e.message) + "</div>";
      });
  }

  function renderReactivationJourneyStatus(cfg) {
    var host = $("#reactivation-journey-status");
    if (!host) return;
    var meta = JOURNEY_STATUS_META[cfg.computed_status] || { label: (cfg.computed_status || "UNKNOWN").toUpperCase(), pill: "neutral" };
    var t1 = cfg.tier1 || {}, t2 = cfg.tier2 || {}, t3 = cfg.tier3 || {};
    function tierSummary(t) { return t.pool_enabled === false ? "Off" : "On"; }
    var liveNote = cfg.computed_status === "live" || cfg.computed_status === "test_only"
      ? ""
      : '<div class="note" style="margin-top:6px;">Journey rewards are NOT live for real users right now.</div>';
    host.innerHTML =
      '<h4 style="margin-top:0;">Reactivation Journey Status <span class="pill ' + esc(meta.pill) + '" style="margin-left:8px;">' + esc(meta.label) + '</span></h4>' +
      '<div class="detail-grid">' +
      kvBlock("Rollout", [
        ["Mode", cfg.mode || "—"],
        ["Reward Type", cfg.reward_type || "—"],
        ["Test Users", (cfg.test_user_ids || []).length],
      ]) +
      kvBlock("Campaign Window", [
        ["Campaign Start", dt(cfg.campaign_start_at)],
        ["Campaign End", dt(cfg.campaign_end_at)],
        ["Server Time (KL)", cfg.server_now_kl ? new Date(cfg.server_now_kl).toLocaleString() : "—"],
      ]) +
      kvBlock("Tiers Enabled", [
        ["Tier 1", tierSummary(t1)],
        ["Tier 2", tierSummary(t2)],
        ["Tier 3", tierSummary(t3)],
      ]) +
      kvBlock("Config", [
        ["Last Updated", dt(cfg.updated_at)],
      ]) +
      "</div>" + liveNote;
  }

  function renderReactivationJourneyConfig(cfg) {
    var host = $("#reactivation-journey-config");
    if (!host) return;
    var modes = ["disabled", "test_users_only", "enabled"];
    var rewardTypes = ["disabled", "xp_only", "tiered_vouchers", "xp_plus_tiered_vouchers"];
    function opts(list, current) {
      return list.map(function (v) { return '<option value="' + v + '"' + (v === current ? " selected" : "") + ">" + v + "</option>"; }).join("");
    }
    function isoLocal(v) {
      if (!v) return "";
      var d = new Date(v);
      if (isNaN(d.getTime())) return "";
      var pad = function (n) { return (n < 10 ? "0" : "") + n; };
      return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) + "T" + pad(d.getHours()) + ":" + pad(d.getMinutes());
    }
    var t1 = cfg.tier1 || {}, t2 = cfg.tier2 || {}, t3 = cfg.tier3 || {};
    function field(labelText, inputHtml) {
      return '<div class="rjc-field"><label>' + labelText + "</label>" + inputHtml + "</div>";
    }
    host.innerHTML =
      '<div class="rjc-grid">' +
      field("Mode", '<select id="rjc-mode" class="filter-input">' + opts(modes, cfg.mode) + "</select>") +
      field("Reward Type", '<select id="rjc-reward-type" class="filter-input">' + opts(rewardTypes, cfg.reward_type) + "</select>") +
      "</div>" +
      '<div class="rjc-grid rjc-grid-1">' +
      field("Test User IDs (comma/newline separated)", '<textarea id="rjc-test-users" class="filter-input" rows="2" style="width:100%;">' + esc((cfg.test_user_ids || []).join(", ")) + "</textarea>") +
      "</div>" +
      '<div class="rjc-grid">' +
      field("Campaign Start", '<input type="datetime-local" id="rjc-start" class="filter-input" value="' + isoLocal(cfg.campaign_start_at) + '"/>') +
      field("Campaign End", '<input type="datetime-local" id="rjc-end" class="filter-input" value="' + isoLocal(cfg.campaign_end_at) + '"/>') +
      "</div>" +
      [1, 2, 3].map(function (tier) {
        var t = tier === 1 ? t1 : tier === 2 ? t2 : t3;
        var extra = tier === 1 ? "" :
          field("Check-ins Required", '<input type="number" min="1" id="rjc-t' + tier + '-days" class="filter-input" value="' + (t.threshold_days != null ? t.threshold_days : "") + '"/>') +
          field("Completion Window (days)", '<input type="number" min="1" id="rjc-t' + tier + '-window" class="filter-input" value="' + (t.window_days != null ? t.window_days : "") + '"/>');
        return '<div class="rjc-tier-card"><h5>Tier ' + tier + ' Reward</h5>' +
          field("Voucher Pool Enabled", '<select id="rjc-t' + tier + '-pool" class="filter-input"><option value="true"' + (t.pool_enabled !== false ? " selected" : "") + '>Yes</option><option value="false"' + (t.pool_enabled === false ? " selected" : "") + '>No</option></select>') +
          extra +
          field("XP Reward", '<input type="number" min="0" id="rjc-t' + tier + '-xp" class="filter-input" value="' + (t.xp_amount || 0) + '"/>') +
          "</div>";
      }).join("") +
      '<div class="controls" style="margin-top:8px;"><button class="btn primary" id="rjc-save-btn">Save Rollout Config</button><span class="note" id="rjc-save-status"></span></div>';

    $("#rjc-save-btn").addEventListener("click", function () {
      var btn = this;
      var status = $("#rjc-save-status");
      if (!btnStart(btn, "Saving...")) return;
      status.textContent = "";
      var payload = {
        mode: $("#rjc-mode").value,
        reward_type: $("#rjc-reward-type").value,
        test_user_ids: ($("#rjc-test-users").value || "").split(/[,\n]/).map(function (x) { return x.trim(); }).filter(Boolean),
        campaign_start_at: $("#rjc-start").value ? new Date($("#rjc-start").value).toISOString() : null,
        campaign_end_at: $("#rjc-end").value ? new Date($("#rjc-end").value).toISOString() : null,
        tier1: { pool_enabled: $("#rjc-t1-pool").value === "true", xp_amount: parseInt($("#rjc-t1-xp").value, 10) || 0 },
        tier2: {
          pool_enabled: $("#rjc-t2-pool").value === "true",
          threshold_days: parseInt($("#rjc-t2-days").value, 10) || 5,
          window_days: parseInt($("#rjc-t2-window").value, 10) || 7,
          xp_amount: parseInt($("#rjc-t2-xp").value, 10) || 0,
        },
        tier3: {
          pool_enabled: $("#rjc-t3-pool").value === "true",
          threshold_days: parseInt($("#rjc-t3-days").value, 10) || 20,
          window_days: parseInt($("#rjc-t3-window").value, 10) || 30,
          xp_amount: parseInt($("#rjc-t3-xp").value, 10) || 0,
        },
      };
      fetch("/api/admin/reactivation/journey/config", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Accept": "application/json", "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }).then(function (r) {
        return r.json().then(function (j) { if (!r.ok) throw new Error(j.reason || j.message || "HTTP " + r.status); return j; });
      }).then(function (j) {
        toast("✅ Rollout config saved", "success");
        status.textContent = "Saved.";
        // Refresh from the server rather than trusting the local payload, so
        // the status card reflects the authoritative computed_status/updated_at.
        loadReactivationJourneyConfig();
      }).catch(function (e) {
        toast("❌ Failed to save rollout config: " + e.message, "error");
        status.textContent = "Failed: " + e.message;
      }).finally(function () {
        btnStop(btn);
      });
    });
  }

  function bindReactivationUpload() {
    var btn = $("#reactivation-upload-btn");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var pool = $("#reactivation-upload-pool").value;
      var codesText = $("#reactivation-upload-codes").value || "";
      var codes = codesText.replace(/\r/g, "\n").split("\n").map(function (x) { return x.trim(); }).filter(Boolean);
      var status = $("#reactivation-upload-status");
      status.textContent = "Uploading...";
      fetch("/api/admin/reactivation/journey/pools/upload", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Accept": "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ pool_id: pool, codes: codes })
      }).then(function (r) {
        return r.json().then(function (j) { if (!r.ok) throw new Error(j.message || j.reason || ("HTTP " + r.status)); return j; });
      }).then(function (j) {
        status.textContent = "Inserted " + fmt(j.inserted) + " codes; duplicates " + fmt(j.duplicates || 0) + ".";
        loadReactivation(true);
      }).catch(function (e) {
        status.textContent = "Failed: " + e.message;
      });
    });
  }
  function setReactivation(active, btn) {
    var path = active ? "/api/admin/channel-reactivation/start" : "/api/admin/channel-reactivation/pause";
    if (btn) btnStart(btn, active ? "⏳ Starting..." : "⏳ Pausing...");
    apiPost(path)
      .then(function () {
        toast(active ? "✅ Reactivation campaign started" : "✅ Reactivation campaign paused", "success");
        loadReactivation(true);
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") banner("❌ Failed to update campaign: " + e.message, "error");
      })
      .finally(function () { if (btn) btnStop(btn); });
  }

  function affiliateDetailHtml(det) {
    var ledger = (det.ledger || []).map(function (l) {
      return "<tr><td>" + esc(l.ledger_type) + "</td><td>" + esc(l.tier || "—") + '</td><td><span class="pill ' +
        esc((l.status || "neutral").toLowerCase()) + '">' + esc(l.status) + "</span></td><td>" + esc(l.year_month || "—") +
        '</td><td class="num">' + fmt(l.qualified_count) + "</td><td>" + esc((l.risk_flags || []).join(", ") || "—") + "</td><td>" + dt(l.updated_at) + "</td></tr>";
    }).join("") || '<tr><td colspan="7">No ledger records.</td></tr>';
    var vouchers = (det.vouchers_issued || []).map(function (v) {
      return "<tr><td>" + esc(v.voucher_code) + "</td><td>" + esc(v.pool_id || v.tier || "—") + "</td><td>" + dt(v.issued_at) + "</td></tr>";
    }).join("") || '<tr><td colspan="3">No vouchers issued.</td></tr>';
    return '<div class="detail-block"><h4>Ledger Records</h4><table class="mini-table"><thead><tr><th>Type</th><th>Tier</th><th>Status</th><th>Month</th><th class="num">Qualified</th><th>Risk Flags</th><th>Updated</th></tr></thead><tbody>' + ledger + "</tbody></table></div>" +
      '<div class="detail-block" style="margin-top:12px;"><h4>Vouchers Issued</h4><table class="mini-table"><thead><tr><th>Code</th><th>Pool</th><th>Issued</th></tr></thead><tbody>' + vouchers + "</tbody></table></div>";
  }

  function loadAudit(refresh) {
    setMeta("Loading…");
    skeletonGrid($("#cards-audit-summary"), 6);
    statePanel("audit-body", "loading", "Loading audit trail…");
    api("/api/admin/dashboard/audit" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, "all time");
        var s = d.summary;
        $("#cards-audit-summary").innerHTML =
          dqCard("Admin Logins", s.admin_logins) + dqCard("Auth Events", s.auth_events) +
          dqCard("Voucher Operations", s.voucher_operations) + dqCard("Affiliate Status Changes", s.affiliate_status_changes) +
          dqCard("Scheduler Events", s.scheduler_events) + dqCard("Referral Operations", s.referral_operations);

        var rows = (d.events || []).map(function (e) {
          var det = e.detail || {};
          var detailHtml = '<div class="detail-grid">' +
            '<div class="detail-block"><h4>Sanitized Payload</h4><pre class="payload">' + esc(JSON.stringify(det.payload || {}, null, 2)) + "</pre></div>" +
            '<div class="detail-block"><h4>Related IDs</h4><pre class="payload">' + esc(JSON.stringify(det.related_ids || {}, null, 2)) + "</pre></div>" +
            (det.error ? '<div class="detail-block"><h4>Error</h4><pre class="payload">' + esc(det.error) + "</pre></div>" : "") + "</div>";
          return {
            search: e.actor + " " + e.action + " " + e.result + " " + e.target,
            cells: [
              { text: dt(e.time) }, { text: e.actor }, { html: "<strong>" + esc(e.action) + "</strong>" },
              { text: e.target }, { html: '<span class="pill ' + esc(String(e.result).toLowerCase().replace(/[^a-z_]/g, "")) + '">' + esc(e.result) + "</span>" }
            ],
            detailHtml: detailHtml
          };
        });
        $("#audit-body").innerHTML = expandTable(
          [{ label: "Time" }, { label: "Actor" }, { label: "Action" }, { label: "Target" }, { label: "Result" }], rows);
        bindExpand("audit-body");
        if (d.partial_errors) banner("Some audit sources degraded: " + d.partial_errors.join("; "), "warn");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          $("#cards-audit-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("audit-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  // ---------- Segment ROI ----------
  var SEG_COLORS = {
    high_value: "#51cf66",
    normal_actual: "#4dabf7",
    active_community_player: "#74c0fc",
    low_value: "#ffd43b",
    voucher_hunter: "#ff9f43",
    ghost: "#8892a4",
    unclassified: "#6b7080",
  };

  function roiColor(score) {
    if (score >= 50) return "var(--ok,#51cf66)";
    if (score >= 10) return "var(--accent-2,#4dabf7)";
    if (score >= 1) return "var(--warn,#ffd43b)";
    return "var(--bad,#ff6b6b)";
  }

  function roiBadge(score) {
    var color = roiColor(score);
    return '<span style="font-weight:700;color:' + color + '">' + fmt(score) + '</span>';
  }

  function _populateRoiMonthDropdown(months) {
    var sel = $("#roi-month");
    if (!sel) return;
    var html = '<option value="">Current month (default)</option>';
    (months || []).forEach(function (m) {
      html += '<option value="' + esc(m) + '"' + (m === state.roiMonth ? " selected" : "") + '>' + esc(m) + '</option>';
    });
    sel.innerHTML = html;
    if (state.roiMonth) sel.value = state.roiMonth;
  }

  function loadSegmentRoi(refresh) {
    setMeta("Loading Segment ROI…");
    var grid = $("#cards-roi-summary");
    var recEl = $("#roi-recommendations");
    var tblEl = $("#roi-table");
    var rankEl = $("#roi-ranking");
    var trendEl = $("#roi-trend");
    if (grid) skeletonGrid(grid, 4);
    if (recEl) recEl.innerHTML = '<div class="loading">Loading…</div>';
    if (tblEl) tblEl.innerHTML = "";
    if (rankEl) rankEl.innerHTML = "";
    if (trendEl) trendEl.innerHTML = "";

    var params = [];
    if (state.roiMonth) params.push("snapshot_month=" + encodeURIComponent(state.roiMonth));

    api("/api/admin/dashboard/segment-roi" + (params.length ? "?" + params.join("&") : ""))
      .then(function (d) {
        setMeta("Period: " + d.period + " · Last updated: " + (d.generated_at ? new Date(d.generated_at).toLocaleString() : ""));
        _populateRoiMonthDropdown(d.available_months || []);

        // Summary KPIs
        var totalUsers = 0, totalBet = 0, totalClaims = 0;
        (d.segments || []).forEach(function (s) {
          totalUsers += s.users || 0;
          totalBet += s.after_bet_amount || 0;
          totalClaims += s.claim_count || 0;
        });
        var overallRoi = totalClaims > 0 ? (totalBet / totalClaims).toFixed(2) : "—";
        if (grid) grid.innerHTML =
          '<div class="kpi kpi-headline"><div class="label">Total Users</div><div class="value">' + fmt(totalUsers) + '</div></div>' +
          '<div class="kpi"><div class="label">Total Bet Amount</div><div class="value">' + fmt(Math.round(totalBet)) + '</div></div>' +
          '<div class="kpi"><div class="label">Total Claims (Voucher Cost)</div><div class="value">' + fmt(totalClaims) + '</div></div>' +
          '<div class="kpi kpi-secondary"><div class="label">Overall ROI Score</div><div class="value">' + overallRoi + '</div><div class="sub">bet per claim</div></div>';

        // Recommendations
        var rec = d.recommendations || {};
        if (recEl) {
          var recHtml = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;">';
          function recCard(title, value, note, color) {
            return '<div style="background:var(--bg-card,#1e2230);border:1px solid var(--border,#2d3348);border-radius:8px;padding:14px;">' +
              '<div style="font-size:11px;text-transform:uppercase;letter-spacing:0.4px;color:var(--muted,#8892a4);">' + title + '</div>' +
              '<div style="font-size:18px;font-weight:700;margin-top:6px;color:' + (color || "var(--text)") + '">' + esc(value || "—") + '</div>' +
              (note ? '<div style="font-size:11px;color:var(--muted,#8892a4);margin-top:4px;">' + esc(note) + '</div>' : '') +
              '</div>';
          }
          recHtml += recCard("Deserves More Rewards", rec.deserves_more_rewards, "Highest ROI score", "var(--ok,#51cf66)");
          recHtml += recCard("Fewer Vouchers", rec.fewer_vouchers, "High claims, low betting", "var(--bad,#ff6b6b)");
          recHtml += recCard("Produces Real Betting", (rec.produces_real_betting || []).join(", ") || "—", "Top segments by bet amount", "var(--accent-2,#4dabf7)");
          recHtml += recCard("Mainly Cost (no betting)", (rec.mainly_cost || []).join(", ") || "—", "Claims with zero bet return", "var(--warn,#ffd43b)");
          recHtml += '</div>';
          recEl.innerHTML = recHtml;
        }

        // Comparison Table
        if (tblEl) {
          var cols = ["Segment", "Users", "Claims", "After Bet", "Withdrawal", "Referrals", "Check-ins",
                      "Cost/User", "Bet/User", "Claim/User", "Ref/User", "ROI Score"];
          var thRow = cols.map(function (c) { return '<th>' + esc(c) + '</th>'; }).join('');
          var rows = (d.segments || []).map(function (s, i) {
            var dot = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' +
              (SEG_COLORS[s.segment] || "#6b7080") + ';margin-right:5px;vertical-align:middle;"></span>';
            return '<tr>' +
              '<td>' + dot + '<strong>' + esc(s.segment) + '</strong></td>' +
              '<td class="num">' + fmt(s.users) + '</td>' +
              '<td class="num">' + fmt(s.claim_count) + '</td>' +
              '<td class="num">' + fmt(Math.round(s.after_bet_amount)) + '</td>' +
              '<td class="num">' + fmt(Math.round(s.withdrawal_amount)) + '</td>' +
              '<td class="num">' + fmt(s.referral_count) + '</td>' +
              '<td class="num">' + fmt(s.checkin_count) + '</td>' +
              '<td class="num">' + fmt(s.cost_per_user) + '</td>' +
              '<td class="num">' + fmt(s.bet_per_user) + '</td>' +
              '<td class="num">' + fmt(s.claim_per_user) + '</td>' +
              '<td class="num">' + fmt(s.referral_per_user) + '</td>' +
              '<td class="num">' + roiBadge(s.roi_score) + '</td>' +
              '</tr>';
          }).join('');
          tblEl.innerHTML = '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr>' + thRow + '</tr></thead>' +
            '<tbody>' + rows + '</tbody></table></div>';
        }

        // Ranking
        if (rankEl) {
          var rankHtml = '<div style="display:flex;flex-direction:column;gap:8px;">';
          (d.ranking || []).forEach(function (seg, i) {
            var segData = (d.segments || []).find(function (s) { return s.segment === seg; }) || {};
            var roi = segData.roi_score !== undefined ? segData.roi_score : 0;
            var maxRoi = d.segments && d.segments.length ? (d.segments[0].roi_score || 1) : 1;
            var barPct = maxRoi > 0 ? Math.round(100 * roi / maxRoi) : 0;
            var medal = i === 0 ? " 🥇" : i === 1 ? " 🥈" : i === 2 ? " 🥉" : "";
            var color = SEG_COLORS[seg] || "#6b7080";
            rankHtml += '<div style="background:var(--bg-card,#1e2230);border:1px solid var(--border,#2d3348);border-radius:8px;padding:10px 14px;">' +
              '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">' +
              '<span style="font-weight:600;font-size:13px;">' +
              '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + color + ';margin-right:6px;vertical-align:middle;"></span>' +
              (i + 1) + '. ' + esc(seg) + esc(medal) + '</span>' +
              '<span style="font-size:13px;color:' + roiColor(roi) + ';font-weight:700;">ROI ' + fmt(roi) + '</span>' +
              '</div>' +
              '<div style="height:6px;background:var(--border,#2d3348);border-radius:3px;">' +
              '<div style="height:6px;width:' + barPct + '%;background:' + roiColor(roi) + ';border-radius:3px;transition:width 0.4s;"></div></div>' +
              '<div style="font-size:11px;color:var(--muted,#8892a4);margin-top:5px;">' +
              esc(segData.users || 0) + ' users · ' + esc(segData.claim_count || 0) + ' claims · ' +
              fmt(Math.round(segData.after_bet_amount || 0)) + ' bet</div>' +
              '</div>';
          });
          rankHtml += '</div>';
          rankEl.innerHTML = rankHtml;
        }

        // Trend
        if (trendEl) {
          var trend = d.trend || [];
          if (!trend.length) {
            trendEl.innerHTML = '<div class="empty">No trend data available.</div>';
          } else {
            var allSegs = [];
            trend.forEach(function (t) {
              Object.keys(t.segments || {}).forEach(function (s) {
                if (allSegs.indexOf(s) === -1) allSegs.push(s);
              });
            });
            var trendHtml = '<div style="overflow-x:auto"><table class="mini-table">' +
              '<thead><tr><th>Segment</th>';
            trend.forEach(function (t) { trendHtml += '<th>' + esc(t.month) + ' ROI</th><th>' + esc(t.month) + ' Users</th>'; });
            trendHtml += '<th>MoM Change</th></tr></thead><tbody>';
            allSegs.sort().forEach(function (seg) {
              trendHtml += '<tr><td><span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:' +
                (SEG_COLORS[seg] || "#6b7080") + ';margin-right:5px;vertical-align:middle;"></span>' + esc(seg) + '</td>';
              var rois = [];
              trend.forEach(function (t) {
                var sd = (t.segments || {})[seg] || {};
                var r = sd.roi_score !== undefined ? sd.roi_score : null;
                var u = sd.users !== undefined ? sd.users : null;
                rois.push(r);
                trendHtml += '<td style="color:' + (r !== null ? roiColor(r) : "var(--muted)") + '">' +
                  (r !== null ? fmt(r) : "—") + '</td>';
                trendHtml += '<td>' + (u !== null ? fmt(u) : "—") + '</td>';
              });
              var validRois = rois.filter(function (r) { return r !== null; });
              var momHtml = "—";
              if (validRois.length >= 2) {
                var last = validRois[validRois.length - 1];
                var prev = validRois[validRois.length - 2];
                if (prev !== 0) {
                  var change = ((last - prev) / Math.max(prev, 0.01) * 100).toFixed(1);
                  var arrow = last >= prev ? "↑" : "↓";
                  var momColor = last >= prev ? "var(--ok,#51cf66)" : "var(--bad,#ff6b6b)";
                  momHtml = '<span style="color:' + momColor + '">' + arrow + " " + Math.abs(change) + '%</span>';
                }
              }
              trendHtml += '<td>' + momHtml + '</td></tr>';
            });
            trendHtml += '</tbody></table></div>';
            trendEl.innerHTML = trendHtml;
          }
        }
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Segment ROI · Failed");
          if (grid) grid.innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + '</div>';
          if (recEl) recEl.innerHTML = "";
          if (tblEl) tblEl.innerHTML = "";
          if (rankEl) rankEl.innerHTML = "";
          if (trendEl) trendEl.innerHTML = "";
        }
      });
  }

  // ---------- Segment Overview ----------
  function initSegmentMonthOptions() {
    var el = $("#segments-month");
    if (!el || el.options.length) return;
    var opts = ['<option value="">Pick a month…</option>'];
    var d = new Date();
    d.setUTCDate(1);
    for (var i = 0; i < 12; i++) {
      var y = d.getUTCFullYear(), m = d.getUTCMonth() + 1;
      var value = y + "-" + (m < 10 ? "0" + m : m);
      var label = d.toLocaleDateString(undefined, { month: "long", year: "numeric", timeZone: "UTC" });
      opts.push('<option value="' + value + '">' + esc(label) + "</option>");
      d.setUTCMonth(d.getUTCMonth() - 1);
    }
    el.innerHTML = opts.join("");
  }

  function loadSegmentProbabilityConfig(refresh) {
    setMeta("Loading segment probability configuration…");
    statePanel("seg-prob-body", "loading", "Loading…");
    api("/api/admin/dashboard/segment-probability-config")
      .then(function (d) {
        setMeta("Source: " + (d.source || "config.SEGMENT_PROBABILITY_CONFIG"));
        var np = d.new_player_override || {};
        $("#cards-seg-prob-summary").innerHTML =
          kpiCard("New Player Override", "100%", "player_age_type=new_player AND first 3 assignments", false) +
          kpiCard("Segments configured", String((d.rows || []).length), "", false);
        var rows = (d.rows || []).map(function (row) {
          return "<tr><td>" + esc(row.segment) + "</td><td><strong>" + esc(String(row.probability_pct)) + "%</strong></td><td>" + esc(row.description) + "</td></tr>";
        });
        $("#seg-prob-body").innerHTML =
          "<table class='data-table'><thead><tr><th>Segment</th><th>Probability</th><th>Description</th></tr></thead><tbody>" +
          rows.join("") +
          "<tr style='border-top:2px solid var(--border)'><td><em>new_player override</em></td><td><strong>100%</strong></td><td>" + esc(np.description || "") + "</td></tr>" +
          "</tbody></table>";
      })
      .catch(function (e) {
        setMeta("Failed");
        statePanel("seg-prob-body", "banner error", "Failed: " + e.message);
      });
  }

  function loadSegments(refresh) {
    setMeta("Mode: " + state.segmentsMode + " · Loading…");
    skeletonGrid($("#cards-segments-summary"), 6);
    statePanel("segments-body", "loading", "Loading segments…");
    var qs = state.segmentsMode === "month" && state.segmentsMonth
      ? "month=" + encodeURIComponent(state.segmentsMonth)
      : "mode=" + encodeURIComponent(state.segmentsMode);
    if (refresh) qs += "&refresh=1";
    if (state.segmentsFilter) qs += "&segment=" + encodeURIComponent(state.segmentsFilter);
    api("/api/admin/dashboard/segments?" + qs)
      .then(function (d) {
        renderMeta(d, d.mode_label || state.segmentsMode);
        var s = d.summary;
        $("#cards-segments-summary").innerHTML =
          dqCard("Total Users", s.total_users) +
          dqCard("Users With Segment", s.users_with_segment) +
          dqCard("Users Without Segment", s.users_without_segment) +
          dqCard("Public Pool — Claimed", s.public_pool_claimed) +
          dqCard("Public Pool — Never Claimed", s.public_pool_not_claimed) +
          dqCard("Recently Synced", s.recently_updated);

        if (d.segment_filter) {
          $("#cards-segments-summary").insertAdjacentHTML("beforeend",
            '<div class="kpi"><div class="label">Filtered: ' + esc(d.segment_filter) + '</div>' +
            '<div class="value">' + (d.filtered_count === null || d.filtered_count === undefined ? "0" : fmt(d.filtered_count)) + "</div></div>");
        }

        var rows = (d.top_segments || []).map(function (row) {
          return "<tr><td>" + esc(row.segment) + '</td><td class="num">' + fmt(row.count) + "</td></tr>";
        }).join("");
        $("#segments-body").innerHTML = rows
          ? '<table class="mini-table"><thead><tr><th>Segment</th><th class="num">Users</th></tr></thead><tbody>' + rows + "</tbody></table>"
          : '<div class="empty">No segment data for selected period.</div>';

        if (d.partial_errors) banner("Some segment metrics degraded: " + d.partial_errors.join("; "), "warn");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Mode: " + state.segmentsMode + " · Failed to update");
          $("#cards-segments-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("segments-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  // ---------- Validation / UIM Compare ----------
  var VALIDATION_STATUS_LABEL = { green: "Matched", yellow: "Warning", red: "Failed", gray: "Missing" };

  function loadValidation(refresh) {
    setMeta("Loading…");
    skeletonGrid($("#cards-validation-summary"), 4);
    statePanel("validation-body", "loading", "Loading UIM vs backend comparison…");
    var qs = refresh ? "refresh=1" : "";
    api("/api/admin/dashboard/validation" + (qs ? "?" + qs : ""))
      .then(function (d) {
        renderMeta(d, "current (live)");
        var s = d.summary || {};
        $("#cards-validation-summary").innerHTML =
          dqCard("Total Metrics Compared", { value: s.total_metrics_compared }) +
          dqCard("Matched Metrics", { value: s.matched_metrics }) +
          dqCard("Warning Metrics", { value: s.warning_metrics }) +
          dqCard("Failed Metrics", { value: s.failed_metrics }) +
          dqCard("Missing Source Data", { value: s.missing_metrics });

        if (d.uim_source && !d.uim_source.ok) {
          banner("UIM source unavailable: " + (d.uim_source.error || "unknown error") + " — showing missing_source for all metrics.", "warn");
        }

        var rows = (d.metrics || []).map(function (m) {
          var statusClass = m.status === "green" ? "ok" : m.status === "yellow" ? "neutral" : m.status === "red" ? "rejected" : "neutral";
          var gapText = m.gap
            ? "[" + m.gap.implementation_status + "] " + m.gap.backend_gap
            : (m.status === "gray" || m.status === "red" ? "Not yet documented (Phase 5B)" : "");
          return "<tr><td>" + esc(m.metric) + "</td>" +
            '<td class="num">' + (m.uim_value === null || m.uim_value === undefined ? "—" : fmt(m.uim_value)) + "</td>" +
            '<td class="num">' + (m.backend_value === null || m.backend_value === undefined ? "—" : fmt(m.backend_value)) + "</td>" +
            '<td class="num">' + (m.difference === null || m.difference === undefined ? "—" : fmt(m.difference)) + "</td>" +
            '<td class="num">' + (m.difference_pct === null || m.difference_pct === undefined ? "—" : fmt(m.difference_pct) + "%") + "</td>" +
            '<td><span class="pill ' + statusClass + '">' + (VALIDATION_STATUS_LABEL[m.status] || m.status) + "</span></td>" +
            '<td style="max-width:360px;font-size:12px;color:var(--muted,#888);">' + esc(gapText) + "</td></tr>";
        }).join("");
        $("#validation-body").innerHTML = rows
          ? '<table class="mini-table"><thead><tr><th>Metric</th><th class="num">UIM Value</th><th class="num">Backend Value</th><th class="num">Difference</th><th class="num">Difference %</th><th>Status</th><th>Gap / Why</th></tr></thead><tbody>' + rows + "</tbody></table>"
          : '<div class="empty">No metrics to compare.</div>';

        if (d.partial_errors) banner("Some validation metrics degraded: " + d.partial_errors.join("; "), "warn");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          $("#cards-validation-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("validation-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  // ---------- Backend Segment Engine (Phase 3, shadow mode) ----------
  function _populateBsePeriodDropdowns(autoSelectLatestWeek) {
    return api("/api/admin/dashboard/backend-segment-engine/available-periods")
      .then(function (d) {
        var weekSel = $("#bse-week");
        var monthSel = $("#bse-month");
        if (!weekSel || !monthSel) return;
        var prevWeek = weekSel.value;
        var prevMonth = monthSel.value;
        weekSel.innerHTML = '<option value="">All weeks</option>';
        (d.snapshot_weeks || []).forEach(function (w) {
          var opt = document.createElement("option");
          opt.value = w;
          opt.textContent = w;
          weekSel.appendChild(opt);
        });
        monthSel.innerHTML = '<option value="">All months</option>';
        (d.snapshot_months || []).forEach(function (m) {
          var opt = document.createElement("option");
          opt.value = m;
          opt.textContent = m;
          monthSel.appendChild(opt);
        });
        if (autoSelectLatestWeek && d.snapshot_weeks && d.snapshot_weeks.length > 0) {
          weekSel.value = d.snapshot_weeks[0];
        } else if (prevWeek) {
          weekSel.value = prevWeek;
        }
        if (prevMonth) monthSel.value = prevMonth;
      })
      .catch(function () {});
  }

  function loadBackendSegmentEngine(refresh) {
    setMeta("Loading…");
    skeletonGrid($("#cards-bse-summary"), 8);
    statePanel("bse-segment-body", "loading", "Loading backend segment engine snapshot…");
    var weekSel = $("#bse-week");
    var monthSel = $("#bse-month");
    var week = (weekSel || {}).value || "";
    var month = (monthSel || {}).value || "";
    // Week takes priority; if week is set, ignore month
    if (week) month = "";
    var qs = [
      refresh ? "refresh=1" : "",
      week ? "snapshot_week=" + encodeURIComponent(week) : "",
      (!week && month) ? "month=" + encodeURIComponent(month) : ""
    ].filter(Boolean).join("&");
    api("/api/admin/dashboard/backend-segment-engine" + (qs ? "?" + qs : ""))
      .then(function (d) {
        var label = d.snapshot_week ? ("week " + d.snapshot_week) : ("month " + (d.snapshot_month || ""));
        renderMeta(d, "snapshot " + label);
        var s = d.summary || {};
        var actualPlayers = (s.high_value || 0) + (s.low_value || 0) + (s.normal_actual || 0);
        $("#cards-bse-summary").innerHTML =
          dqCard("Users Evaluated", { value: s.total_users_evaluated }) +
          dqCard("High Value", { value: s.high_value }) +
          dqCard("Low Value", { value: s.low_value }) +
          dqCard("Normal Actual", { value: s.normal_actual }) +
          dqCard("Actual Players (HV+LV+NA)", { value: s.actual_players != null ? s.actual_players : actualPlayers }) +
          dqCard("Voucher Hunter", { value: s.voucher_hunter }) +
          dqCard("Ghost", { value: s.ghost }) +
          dqCard("Active Community", { value: s.active_community_player }) +
          dqCard("Unclassified", { value: s.unclassified }) +
          dqCard("UIM Compared", { value: s.uim_compared }) +
          dqCard("Match Rate %", { value: s.match_rate }) +
          dqCard("Mismatch Rate %", { value: s.mismatch_rate });

        var segRows = Object.keys(d.segment_distribution || {}).sort().map(function (k) {
          return "<tr><td>" + esc(k) + '</td><td class="num">' + fmt(d.segment_distribution[k]) + "</td></tr>";
        }).join("");
        $("#bse-segment-body").innerHTML = segRows
          ? '<table class="mini-table"><thead><tr><th>Backend Segment</th><th class="num">Users</th></tr></thead><tbody>' + segRows + "</tbody></table>"
          : '<div class="empty">No backend segment snapshots for this period yet.</div>';

        var riskRows = Object.keys(d.claim_risk_distribution || {}).sort().map(function (k) {
          return "<tr><td>" + esc(k) + '</td><td class="num">' + fmt(d.claim_risk_distribution[k]) + "</td></tr>";
        }).join("");
        $("#bse-claim-risk-body").innerHTML = riskRows
          ? '<table class="mini-table"><thead><tr><th>Claim Risk Level</th><th class="num">Users</th></tr></thead><tbody>' + riskRows + "</tbody></table>"
          : '<div class="empty">No claim risk data for this period yet.</div>';

        var age = d.player_age_distribution || {};
        var ageRows = ["new_player", "old_player", "unknown"].map(function (k) {
          return "<tr><td>" + esc(k) + '</td><td class="num">' + fmt(age[k] || 0) + "</td></tr>";
        }).join("");
        $("#bse-age-body").innerHTML = ageRows
          ? '<table class="mini-table"><thead><tr><th>Player Age Type</th><th class="num">Users</th></tr></thead><tbody>' + ageRows + "</tbody></table>"
          : '<div class="empty">No player age data available.</div>';

        var cmpRows = (d.comparison_rows || []).map(function (r) {
          var matchCell = r.match
            ? '<td style="color:var(--green,green)">match</td>'
            : '<td style="color:var(--red,red)">mismatch</td>';
          return "<tr><td>" + esc(r.account || "") + "</td><td>" + esc(r.backend_segment || "") + "</td><td>" + esc(r.uim_segment || "") + "</td>" + matchCell + "<td>" + esc(r.confidence || "") + "</td><td>" + esc(r.reason || "") + "</td></tr>";
        }).join("");
        $("#bse-comparison-body").innerHTML = cmpRows
          ? '<table class="mini-table"><thead><tr><th>Account</th><th>Backend Segment</th><th>UIM Segment</th><th>Match</th><th>Confidence</th><th>Reason</th></tr></thead><tbody>' + cmpRows + "</tbody></table>"
          : '<div class="empty">No UIM comparison data available. Run the engine with marketing data to populate.</div>';
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          $("#cards-bse-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("bse-segment-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  // ---------- Backend Segment Engine Run (Phase 3C — async job) ----------
  function runBackendSegmentEngine(dryRun) {
    var weekInput = $("#bse-run-week");
    var week = (weekInput || {}).value || "";
    week = week.trim();
    var resultEl = $("#bse-run-result");
    // Validate format only when a value is provided; blank = process all periods.
    if (week && !/^\d{4}-W(0[1-9]|[1-4]\d|5[0-3])$/.test(week)) {
      if (resultEl) resultEl.innerHTML = '<span style="color:var(--red,#e05c5c)">Invalid format. Use YYYY-Www (e.g. 2026-W25), or leave blank to process all periods.</span>';
      return;
    }
    var label = dryRun ? "Dry run" : "Commit run";
    var scopeLabel = week ? ("week " + week) : "all periods";
    if (resultEl) resultEl.innerHTML = '<span style="color:var(--muted,#8892a4)">' + label + ' queuing for ' + esc(scopeLabel) + '…</span>';
    var dryBtn = $("#bse-dry-run-btn");
    var commitBtn = $("#bse-commit-run-btn");
    if (dryBtn) dryBtn.disabled = true;
    if (commitBtn) commitBtn.disabled = true;
    fetch("/api/admin/dashboard/backend-segment-engine/run" + window.location.search, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Accept": "application/json", "Content-Type": "application/json" },
      body: JSON.stringify({ snapshot_week: week || null, dry_run: dryRun }),
    })
      .then(function (r) {
        if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
        return r.json().then(function (j) { return { ok: r.ok, body: j }; });
      })
      .then(function (res) {
        var d = res.body;
        if (!res.ok || !d.ok) {
          if (dryBtn) dryBtn.disabled = false;
          if (commitBtn) commitBtn.disabled = false;
          if (resultEl) resultEl.innerHTML = '<span style="color:var(--red,#e05c5c)">Error: ' + esc(d.error || "unknown error") + '</span>';
          return;
        }
        var jobId = d.job_id;
        if (resultEl) resultEl.innerHTML = '<span style="color:var(--muted,#8892a4)">' + label + ' running for ' + esc(scopeLabel) + '… (job ' + esc(jobId) + ')</span>';
        _pollBseJob(jobId, dryRun, week, dryBtn, commitBtn, resultEl);
      })
      .catch(function (e) {
        if (dryBtn) dryBtn.disabled = false;
        if (commitBtn) commitBtn.disabled = false;
        if (e.message !== "unauthorized") {
          if (resultEl) resultEl.innerHTML = '<span style="color:var(--red,#e05c5c)">Failed: ' + esc(e.message) + '</span>';
        }
      });
  }

  function _pollBseJob(jobId, dryRun, week, dryBtn, commitBtn, resultEl) {
    var label = dryRun ? "Dry run" : "Commit run";
    var qs = window.location.search;
    var statusBase = "/api/admin/dashboard/backend-segment-engine/run-status" + qs + (qs ? "&" : "?");
    var interval = setInterval(function () {
      fetch(statusBase + "job_id=" + encodeURIComponent(jobId), {
        credentials: "same-origin",
        headers: { "Accept": "application/json" },
      })
        .then(function (r) {
          // 401: redirect to login, abort poll
          if (r.status === 401) {
            clearInterval(interval);
            window.location.href = "/static/admin-login.html";
            throw new Error("unauthorized");
          }
          var httpOk = r.ok;
          // Attempt JSON parse; recover gracefully if body is not JSON
          return r.json().then(
            function (d) { return { httpOk: httpOk, d: d, parseOk: true }; },
            function ()  { return { httpOk: httpOk, d: null, parseOk: false }; }
          );
        })
        .then(function (res) {
          if (!res) { return; } // swallowed by the 401 throw above
          // Non-JSON body (nginx 502/504 HTML, empty body, truncated response)
          if (!res.parseOk) {
            clearInterval(interval);
            if (dryBtn) dryBtn.disabled = false;
            if (commitBtn) commitBtn.disabled = false;
            if (resultEl) resultEl.innerHTML = '<span style="color:var(--red,#e05c5c)">Failed: server returned an unexpected non-JSON response</span>';
            return;
          }
          var d = res.d || {};
          // HTTP error (400/403/404/5xx) or API-level ok:false
          if (!res.httpOk || !d.ok) {
            clearInterval(interval);
            if (dryBtn) dryBtn.disabled = false;
            if (commitBtn) commitBtn.disabled = false;
            if (resultEl) resultEl.innerHTML = '<span style="color:var(--red,#e05c5c)">Failed: ' + esc(d.error || "unknown error") + '</span>';
            return;
          }
          var status = d.status;
          // Still in progress — keep polling
          if (status === "queued" || status === "running") {
            if (resultEl) resultEl.innerHTML = '<span style="color:var(--muted,#8892a4)">' + label + ' ' + esc(status) + ' for ' + esc(week) + '…</span>';
            return;
          }
          // Terminal state — stop polling regardless of status value
          clearInterval(interval);
          if (dryBtn) dryBtn.disabled = false;
          if (commitBtn) commitBtn.disabled = false;
          if (status === "failed") {
            if (resultEl) resultEl.innerHTML = '<span style="color:var(--red,#e05c5c)">Failed: ' + esc(d.error || "unknown error") + '</span>';
            return;
          }
          // Guard: only render green success on explicit "success" status
          if (status !== "success") {
            if (resultEl) resultEl.innerHTML = '<span style="color:var(--red,#e05c5c)">Failed: unexpected job status “' + esc(String(status)) + '”</span>';
            return;
          }
          var s = d.summary || {};
          var matchRateColor = (s.identity_match_rate >= 80) ? "var(--green,#4caf82)"
                             : (s.identity_match_rate >= 40) ? "var(--yellow,#e0b44a)"
                             : "var(--red,#e05c5c)";
          var lines = [
            (dryRun ? "<b>Dry run</b> completed" : "<b>Commit run</b> completed") + " for " + esc(week),
            "Rows: <b>" + fmt(s.total_rows) + "</b>",
            "Identity matched: <b style='color:" + matchRateColor + "'>" + fmt(s.matched_rows) + " (" + s.identity_match_rate + "%)</b>",
            "Unmatched: <b>" + fmt(s.unmatched_rows) + "</b>",
            "Users evaluated: <b>" + fmt(s.users_evaluated) + "</b>",
            dryRun ? "Snapshots written: <b>0 (dry run)</b>" : "Snapshots written: <b>" + fmt(s.snapshots_written) + "</b>",
          ];
          var segs = s.segment_distribution || {};
          var segParts = Object.keys(segs).sort().map(function (k) { return esc(k) + ": " + fmt(segs[k]); });
          if (segParts.length) lines.push("Segments: " + segParts.join(" | "));
          if (resultEl) resultEl.innerHTML = '<span style="color:var(--green,#4caf82)">' + lines.join(" &nbsp;&bull;&nbsp; ") + '</span>';
          if (!dryRun) {
            var weekFilter = $("#bse-week");
            if (weekFilter) weekFilter.value = week;
            loadBackendSegmentEngine(true);
          }
        })
        .catch(function (e) {
          if (e.message === "unauthorized") { return; }
          // Transient network error — keep polling silently
        });
    }, 2000);
  }

  // ---------- Phase 5: Backend vs UIM Comparison ----------
  var _uimcPage = 1;
  var _uimcPerPage = 200;

  function loadUimComparison(resetPage) {
    if (resetPage) _uimcPage = 1;
    var week = ($("#uimc-week") || {}).value || "";
    if (!week) {
      statePanel("uimc-detail-body", "empty", "Enter a snapshot_week (e.g. 2026-W25) and click Analyse.");
      $("#cards-uimc-summary").innerHTML = "";
      $("#uimc-matrix-body").innerHTML = "";
      $("#uimc-mismatch-pairs-body").innerHTML = "";
      $("#uimc-audit-body").innerHTML = "";
      $("#uimc-pagination").innerHTML = "";
      return;
    }
    statePanel("uimc-detail-body", "loading", "Loading comparison…");
    $("#cards-uimc-summary").innerHTML = "";
    var bSeg  = ($("#uimc-backend-seg") || {}).value || "";
    var uSeg  = ($("#uimc-uim-seg")     || {}).value || "";
    var match = ($("#uimc-match")        || {}).value || "";
    var risk  = ($("#uimc-risk")         || {}).value || "";
    var qs = [
      "snapshot_week=" + encodeURIComponent(week),
      bSeg  ? "backend_segment="  + encodeURIComponent(bSeg)  : "",
      uSeg  ? "uim_segment="      + encodeURIComponent(uSeg)  : "",
      match ? "match="            + encodeURIComponent(match) : "",
      risk  ? "claim_risk_level=" + encodeURIComponent(risk)  : "",
      "page="     + _uimcPage,
      "per_page=" + _uimcPerPage,
    ].filter(Boolean).join("&");
    api("/api/admin/dashboard/backend-segment-engine/uim-comparison?" + qs)
      .then(function (d) {
        // --- Summary cards ---
        var s = d.summary || {};
        $("#cards-uimc-summary").innerHTML =
          dqCard("Total Backend Users",  { value: s.total_backend_users }) +
          dqCard("Total UIM Users",      { value: s.total_uim_users }) +
          dqCard("Compared Users",       { value: s.compared_users }) +
          dqCard("Matched",              { value: s.matched_users }) +
          dqCard("Mismatched",           { value: s.mismatched_users }) +
          dqCard("Match Rate %",         { value: s.match_rate }) +
          dqCard("Mismatch Rate %",      { value: s.mismatch_rate });

        // --- Cross-tab matrix ---
        var mx = d.mismatch_matrix || {};
        var uSegs = mx.uim_segments || [];
        var mxRows = mx.rows || [];
        if (uSegs.length && mxRows.length) {
          var hdr = "<tr><th>Backend ↓ / UIM →</th>" + uSegs.map(function (u) { return "<th class='num'>" + esc(u) + "</th>"; }).join("") + "</tr>";
          var body = mxRows.map(function (r) {
            var cells = uSegs.map(function (u) {
              var v = (r.by_uim_segment || {})[u] || 0;
              var style = (r.backend_segment !== u && v > 0) ? " style='color:var(--red,#e05c5c)'" : "";
              return "<td class='num'" + style + ">" + fmt(v) + "</td>";
            }).join("");
            return "<tr><td><b>" + esc(r.backend_segment) + "</b></td>" + cells + "</tr>";
          }).join("");
          $("#uimc-matrix-body").innerHTML = '<div style="overflow-x:auto"><table class="mini-table"><thead>' + hdr + '</thead><tbody>' + body + '</tbody></table></div>';
        } else {
          $("#uimc-matrix-body").innerHTML = '<div class="empty">No comparison data available. Run the segment engine with marketing data first.</div>';
        }

        // --- Top mismatch pairs ---
        var pairs = d.top_mismatch_pairs || [];
        if (pairs.length) {
          var pairRows = pairs.map(function (p) {
            return '<tr style="cursor:pointer" data-bseg="' + esc(p.backend_segment) + '" data-useg="' + esc(p.uim_segment) + '">' +
              '<td>' + esc(p.backend_segment) + '</td>' +
              '<td>' + esc(p.uim_segment) + '</td>' +
              '<td class="num">' + fmt(p.count) + '</td>' +
              '<td class="num">' + p.percentage_of_compared_users + '%</td>' +
              '</tr>';
          }).join("");
          var pairHtml = '<div style="overflow-x:auto"><table class="mini-table" id="uimc-pairs-table">' +
            '<thead><tr><th>Backend Segment</th><th>UIM Segment</th><th class="num">Users</th><th class="num">% of Compared</th></tr></thead>' +
            '<tbody>' + pairRows + '</tbody></table></div>' +
            '<div style="font-size:11px;color:var(--muted,#8892a4);margin-top:4px;">Click a row to filter the Detail Table to that pair.</div>';
          $("#uimc-mismatch-pairs-body").innerHTML = pairHtml;
          var pairTable = document.getElementById("uimc-pairs-table");
          if (pairTable) {
            pairTable.addEventListener("click", function (e) {
              var row = e.target.closest("tr[data-bseg]");
              if (!row) return;
              var bSel = $("#uimc-backend-seg");
              var uSel = $("#uimc-uim-seg");
              var mSel = $("#uimc-match");
              if (bSel) bSel.value = row.getAttribute("data-bseg");
              if (uSel) uSel.value = row.getAttribute("data-useg");
              if (mSel) mSel.value = "false";
              loadUimComparison(true);
            });
          }
        } else {
          $("#uimc-mismatch-pairs-body").innerHTML = '<div class="empty">No mismatch pairs found.</div>';
        }

        // --- Rule audit ---
        var audit = d.rule_audit || {};
        var auditSegs = Object.keys(audit).sort();
        if (auditSegs.length) {
          var auditRows = auditSegs.map(function (seg) {
            var a = audit[seg] || {};
            function fmtAvg(v) { return v == null ? "—" : (typeof v === "number" ? v.toFixed(2) : esc(String(v))); }
            return "<tr><td><b>" + esc(seg) + "</b></td>" +
              '<td class="num">' + fmt(a.count) + "</td>" +
              '<td class="num">' + fmtAvg(a.avg_after_total_bet_amount) + "</td>" +
              '<td class="num">' + fmtAvg(a.avg_withdraw_amount)        + "</td>" +
              '<td class="num">' + fmtAvg(a.avg_claim_count)            + "</td>" +
              '<td class="num">' + fmtAvg(a.avg_referral_count)         + "</td>" +
              '<td class="num">' + fmtAvg(a.avg_checkin_count)          + "</td></tr>";
          }).join("");
          $("#uimc-audit-body").innerHTML =
            '<table class="mini-table"><thead><tr>' +
            "<th>Backend Segment</th><th class='num'>Count</th>" +
            "<th class='num'>Avg After Bet</th><th class='num'>Avg Withdraw</th>" +
            "<th class='num'>Avg Claims</th><th class='num'>Avg Referrals</th><th class='num'>Avg Checkins</th>" +
            "</tr></thead><tbody>" + auditRows + "</tbody></table>";
        } else {
          $("#uimc-audit-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // --- Detail table ---
        var rows = d.details || [];
        if (rows.length) {
          var detailRows = rows.map(function (r) {
            var matchCell = r.match === true
              ? '<td style="color:var(--green,green)">match</td>'
              : r.match === false
              ? '<td style="color:var(--red,red)">mismatch</td>'
              : '<td style="color:var(--muted,#8892a4)">—</td>';
            function fmtNum(v) { return (v == null || v === "") ? "—" : fmt(v); }
            return "<tr>" +
              "<td>" + esc(r.account || "") + "</td>" +
              "<td>" + esc(r.backend_segment || "") + "</td>" +
              "<td>" + esc(r.uim_segment || "—") + "</td>" +
              matchCell +
              "<td>" + esc(r.confidence || "") + "</td>" +
              "<td>" + esc(r.reason || "") + "</td>" +
              '<td class="num">' + fmtNum(r.after_total_bet_amount) + "</td>" +
              '<td class="num">' + fmtNum(r.withdraw_amount) + "</td>" +
              '<td class="num">' + fmtNum(r.claim_count) + "</td>" +
              '<td class="num">' + fmtNum(r.referral_count) + "</td>" +
              '<td class="num">' + fmtNum(r.checkin_count) + "</td>" +
              "<td>" + esc(r.player_age_type || "") + "</td>" +
              "<td>" + esc(r.claim_risk_level || "") + "</td>" +
              "</tr>";
          }).join("");
          var th = "<tr><th>Account</th><th>Backend Seg.</th><th>UIM Seg.</th><th>Match</th>" +
            "<th>Confidence</th><th>Reason</th>" +
            "<th class='num'>After Bet</th><th class='num'>Withdraw</th>" +
            "<th class='num'>Claims</th><th class='num'>Referrals</th><th class='num'>Checkins</th>" +
            "<th>Age Type</th><th>Claim Risk</th></tr>";
          $("#uimc-detail-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table"><thead>' + th + '</thead><tbody>' + detailRows + "</tbody></table></div>";
        } else {
          $("#uimc-detail-body").innerHTML = '<div class="empty">No rows match the current filters.</div>';
        }

        // --- Pagination ---
        var total = d.total_details || 0;
        var totalPages = Math.max(1, Math.ceil(total / _uimcPerPage));
        var pEl = $("#uimc-pagination");
        if (pEl) {
          pEl.innerHTML =
            '<button class="btn" id="uimc-prev-btn"' + (_uimcPage <= 1 ? " disabled" : "") + '>← Prev</button>' +
            '<span>Page ' + _uimcPage + ' / ' + totalPages + ' &nbsp;(' + fmt(total) + ' rows)</span>' +
            '<button class="btn" id="uimc-next-btn"' + (!d.has_more ? " disabled" : "") + '>Next →</button>';
          var prevBtn = $("#uimc-prev-btn");
          var nextBtn = $("#uimc-next-btn");
          if (prevBtn) prevBtn.addEventListener("click", function () { _uimcPage--; loadUimComparison(false); });
          if (nextBtn) nextBtn.addEventListener("click", function () { _uimcPage++; loadUimComparison(false); });
        }
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("uimc-detail-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  function exportUimComparisonCsv() {
    var week = ($("#uimc-week") || {}).value || "";
    if (!week) { alert("Enter a snapshot_week first."); return; }
    var bSeg  = ($("#uimc-backend-seg") || {}).value || "";
    var uSeg  = ($("#uimc-uim-seg")     || {}).value || "";
    var match = ($("#uimc-match")        || {}).value || "";
    var risk  = ($("#uimc-risk")         || {}).value || "";
    var qs = [
      "snapshot_week=" + encodeURIComponent(week),
      bSeg  ? "backend_segment="  + encodeURIComponent(bSeg)  : "",
      uSeg  ? "uim_segment="      + encodeURIComponent(uSeg)  : "",
      match ? "match="            + encodeURIComponent(match) : "",
      risk  ? "claim_risk_level=" + encodeURIComponent(risk)  : "",
    ].filter(Boolean).join("&");
    var url = "/api/admin/dashboard/backend-segment-engine/uim-comparison/export?" +
      qs + (window.location.search ? "&" + window.location.search.slice(1) : "");
    var a = document.createElement("a");
    a.href = url;
    a.download = "uim_comparison_" + week + ".csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  }

  // ---------- Voucher Hunter Mismatch Audit (Phase 5B) ----------
  function loadVoucherHunterMismatchAudit() {
    var week = ($("#vhma-week") || {}).value || "";
    if (!week) {
      statePanel("vhma-summary-body", "banner", "Enter a snapshot_week and click Run Audit.");
      $("#cards-vhma-totals").innerHTML = "";
      $("#vhma-breakdown-body").innerHTML = "";
      $("#vhma-samples-body").innerHTML = "";
      return;
    }
    statePanel("vhma-summary-body", "loading", "Running audit…");
    $("#cards-vhma-totals").innerHTML = "";
    $("#vhma-breakdown-body").innerHTML = "";
    $("#vhma-samples-body").innerHTML = "";
    api("/api/admin/dashboard/backend-segment-engine/voucher-hunter-mismatch-audit?snapshot_week=" + encodeURIComponent(week))
      .then(function (d) {
        // Totals
        var t = d.totals || {};
        $("#cards-vhma-totals").innerHTML =
          dqCard("UIM Voucher Hunter Users", { value: t.total_voucher_hunter_uim_users }) +
          dqCard("Total Mismatches",         { value: t.total_mismatches });

        // Summary table
        var st = d.summary_table || [];
        if (st.length) {
          var stRows = st.map(function (r) {
            return "<tr>" +
              "<td><b>" + esc(r.backend_segment) + "</b></td>" +
              '<td class="num">' + fmt(r.users) + "</td>" +
              '<td class="num">' + (r.pct_of_voucher_hunter != null ? r.pct_of_voucher_hunter + "%" : "—") + "</td>" +
              '<td class="num">' + (r.pct_of_mismatches != null ? r.pct_of_mismatches + "%" : "—") + "</td>" +
              "</tr>";
          }).join("");
          $("#vhma-summary-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Backend Segment</th><th class="num">Users</th><th class="num">% of Voucher Hunter</th><th class="num">% of Mismatches</th></tr></thead>' +
            "<tbody>" + stRows + "</tbody></table></div>";
        } else {
          $("#vhma-summary-body").innerHTML = '<div class="empty">No focus-segment mismatches found for this week.</div>';
        }

        // Breakdown table
        var bd = d.segment_breakdown || [];
        if (bd.length) {
          function fmtAvg(v) { return v == null ? "—" : (typeof v === "number" ? v.toFixed(2) : esc(String(v))); }
          var bdRows = bd.map(function (r) {
            return "<tr>" +
              "<td><b>" + esc(r.backend_segment) + "</b></td>" +
              '<td class="num">' + fmt(r.mismatch_count) + "</td>" +
              '<td class="num">' + fmtAvg(r.avg_after_total_bet_amount) + "</td>" +
              '<td class="num">' + fmtAvg(r.avg_withdraw_amount) + "</td>" +
              '<td class="num">' + fmtAvg(r.avg_claim_count) + "</td>" +
              '<td class="num">' + fmtAvg(r.avg_referral_count) + "</td>" +
              '<td class="num">' + fmtAvg(r.avg_checkin_count) + "</td>" +
              '<td class="num">' + fmt(r.new_player_count) + "</td>" +
              '<td class="num">' + fmt(r.old_player_count) + "</td>" +
              "</tr>";
          }).join("");
          $("#vhma-breakdown-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr>' +
            "<th>Backend Segment</th><th class='num'>Mismatches</th>" +
            "<th class='num'>Avg After Bet</th><th class='num'>Avg Withdraw</th>" +
            "<th class='num'>Avg Claims</th><th class='num'>Avg Referrals</th><th class='num'>Avg Checkins</th>" +
            "<th class='num'>New Players</th><th class='num'>Old Players</th>" +
            "</tr></thead><tbody>" + bdRows + "</tbody></table></div>";
        } else {
          $("#vhma-breakdown-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // Sample users per group
        var samps = d.sample_users || {};
        var segOrder = ["unclassified", "normal_actual", "low_value", "high_value", "ghost", "active_community_player"];
        var sampHtml = "";
        segOrder.forEach(function (seg) {
          var rows = samps[seg];
          if (!rows || !rows.length) return;
          function fmtNum(v) { return (v == null || v === "") ? "—" : fmt(v); }
          var tRows = rows.map(function (r) {
            return "<tr>" +
              "<td>" + esc(r.account || "") + "</td>" +
              "<td>" + esc(r.backend_segment || "") + "</td>" +
              "<td>voucher_hunter</td>" +
              '<td class="num">' + fmtNum(r.after_total_bet_amount) + "</td>" +
              '<td class="num">' + fmtNum(r.withdraw_amount) + "</td>" +
              '<td class="num">' + fmtNum(r.claim_count) + "</td>" +
              '<td class="num">' + fmtNum(r.referral_count) + "</td>" +
              '<td class="num">' + fmtNum(r.checkin_count) + "</td>" +
              "<td>" + esc(r.player_age_type || "") + "</td>" +
              "<td>" + esc(r.claim_risk_level || "") + "</td>" +
              "<td>" + esc(r.confidence || "") + "</td>" +
              "<td>" + esc(r.reason || "") + "</td>" +
              "</tr>";
          }).join("");
          sampHtml +=
            '<div style="font-weight:600;font-size:12px;margin:16px 0 6px;">' + esc(seg) + ' (' + rows.length + ' samples)</div>' +
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Account</th><th>Backend Seg.</th><th>UIM Seg.</th>' +
            "<th class='num'>After Bet</th><th class='num'>Withdraw</th>" +
            "<th class='num'>Claims</th><th class='num'>Referrals</th><th class='num'>Checkins</th>" +
            "<th>Age Type</th><th>Claim Risk</th><th>Confidence</th><th>Reason</th></tr></thead>" +
            "<tbody>" + tRows + "</tbody></table></div>";
        });
        $("#vhma-samples-body").innerHTML = sampHtml || '<div class="empty">No sample users available.</div>';
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("vhma-summary-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  // ---------- Unclassified Audit (Phase 5C) ----------
  function loadUnclassifiedAudit() {
    var week = ($("#unca-week") || {}).value || "";
    if (!week) {
      $("#cards-unca-summary").innerHTML = "";
      statePanel("unca-claim-risk-body", "banner", "Enter a snapshot_week and click Run Audit.");
      $("#unca-bucket-body").innerHTML = "";
      $("#unca-reasons-body").innerHTML = "";
      $("#unca-samples-body").innerHTML = "";
      return;
    }
    statePanel("unca-claim-risk-body", "loading", "Running audit…");
    $("#cards-unca-summary").innerHTML = "";
    $("#unca-bucket-body").innerHTML = "";
    $("#unca-reasons-body").innerHTML = "";
    $("#unca-samples-body").innerHTML = "";

    api("/api/admin/dashboard/backend-segment-engine/unclassified-audit?snapshot_week=" + encodeURIComponent(week))
      .then(function (d) {
        var kpis = d.summary_kpis || {};

        // Summary cards
        $("#cards-unca-summary").innerHTML =
          dqCard("Total Unclassified", { value: kpis.unclassified_users }) +
          dqCard("% of Backend Users", { value: (kpis.unclassified_pct != null ? kpis.unclassified_pct + "%" : "—") }) +
          dqCard("New Players", { value: kpis.new_players }) +
          dqCard("Old Players", { value: kpis.old_players }) +
          dqCard("Avg After Bet", { value: (kpis.avg_after_bet != null ? kpis.avg_after_bet.toFixed(2) : "—") }) +
          dqCard("Avg Withdraw", { value: (kpis.avg_withdraw != null ? kpis.avg_withdraw.toFixed(2) : "—") }) +
          dqCard("Avg Claims", { value: (kpis.avg_claims != null ? kpis.avg_claims.toFixed(2) : "—") }) +
          dqCard("Avg Referrals", { value: (kpis.avg_referrals != null ? kpis.avg_referrals.toFixed(2) : "—") }) +
          dqCard("Avg Checkins", { value: (kpis.avg_checkins != null ? kpis.avg_checkins.toFixed(2) : "—") });

        // Claim Risk Breakdown
        var risks = d.claim_risk_breakdown || [];
        if (risks.length) {
          var riskRows = risks.map(function (r) {
            return "<tr>" +
              "<td><b>" + esc(r.claim_risk) + "</b></td>" +
              '<td class="num">' + fmt(r.users) + "</td>" +
              '<td class="num">' + (r.percentage != null ? r.percentage + "%" : "—") + "</td>" +
              "</tr>";
          }).join("");
          $("#unca-claim-risk-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Claim Risk</th><th class="num">Users</th><th class="num">%</th></tr></thead>' +
            "<tbody>" + riskRows + "</tbody></table></div>";
        } else {
          $("#unca-claim-risk-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // Activity Buckets
        var buckets = d.activity_buckets || [];
        if (buckets.length) {
          var bktRows = buckets.map(function (r) {
            return "<tr>" +
              "<td><b>" + esc(r.bucket) + "</b></td>" +
              '<td class="num">' + fmt(r.users) + "</td>" +
              '<td class="num">' + (r.percentage != null ? r.percentage + "%" : "—") + "</td>" +
              "</tr>";
          }).join("");
          $("#unca-bucket-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Bucket</th><th class="num">Users</th><th class="num">%</th></tr></thead>' +
            "<tbody>" + bktRows + "</tbody></table></div>";
        } else {
          $("#unca-bucket-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // Top Reasons
        var reasons = d.top_reasons || [];
        if (reasons.length) {
          var rsnRows = reasons.map(function (r) {
            return "<tr>" +
              "<td>" + esc(r.reason) + "</td>" +
              '<td class="num">' + fmt(r.users) + "</td>" +
              '<td class="num">' + (r.percentage != null ? r.percentage + "%" : "—") + "</td>" +
              "</tr>";
          }).join("");
          $("#unca-reasons-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Reason</th><th class="num">Users</th><th class="num">%</th></tr></thead>' +
            "<tbody>" + rsnRows + "</tbody></table></div>";
        } else {
          $("#unca-reasons-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // Sample users per bucket
        var samps = d.sample_users || {};
        var bktOrder = ["inactive_light", "claim_only", "play_no_withdraw", "withdraw_user", "other"];
        var sampHtml = "";
        bktOrder.forEach(function (bkt) {
          var rows = samps[bkt];
          if (!rows || !rows.length) return;
          function fmtNum(v) { return (v == null || v === "") ? "—" : fmt(v); }
          var tRows = rows.map(function (r) {
            return "<tr>" +
              "<td>" + esc(r.account || "") + "</td>" +
              '<td class="num">' + fmtNum(r.after_bet) + "</td>" +
              '<td class="num">' + fmtNum(r.withdraw) + "</td>" +
              '<td class="num">' + fmtNum(r.claims) + "</td>" +
              '<td class="num">' + fmtNum(r.referrals) + "</td>" +
              '<td class="num">' + fmtNum(r.checkins) + "</td>" +
              "<td>" + esc(r.age_type || "") + "</td>" +
              "<td>" + esc(r.claim_risk || "") + "</td>" +
              "<td>" + esc(r.confidence || "") + "</td>" +
              "<td>" + esc(r.reason || "") + "</td>" +
              "</tr>";
          }).join("");
          sampHtml +=
            '<div style="font-weight:600;font-size:12px;margin:16px 0 6px;">' + esc(bkt) + ' (' + rows.length + ' samples)</div>' +
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Account</th>' +
            "<th class='num'>After Bet</th><th class='num'>Withdraw</th>" +
            "<th class='num'>Claims</th><th class='num'>Referrals</th><th class='num'>Checkins</th>" +
            "<th>Age Type</th><th>Claim Risk</th><th>Confidence</th><th>Reason</th></tr></thead>" +
            "<tbody>" + tRows + "</tbody></table></div>";
        });
        $("#unca-samples-body").innerHTML = sampHtml || '<div class="empty">No sample users available.</div>';
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("unca-claim-risk-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  // ---------- Segment Rule Simulator (Phase 5D) ----------
  function runSegmentRuleSimulator() {
    var week = ($("#srs-week") || {}).value || "";
    if (!week) {
      statePanel("srs-distribution-body", "banner", "Enter a snapshot_week and click Run Simulation.");
      $("#cards-srs-impact").innerHTML = "";
      $("#cards-srs-match-rate").innerHTML = "";
      $("#srs-movements-body").innerHTML = "";
      return;
    }

    function numVal(id, def) {
      var el = $("#" + id);
      var v = el ? el.value : "";
      var n = parseFloat(v);
      return isNaN(n) ? def : n;
    }

    var params = [
      "snapshot_week=" + encodeURIComponent(week),
      "ghost_max_checkins="  + numVal("srs-ghost-checkins", 0),
      "ghost_max_referrals=" + numVal("srs-ghost-referrals", 0),
      "ghost_max_claims="    + numVal("srs-ghost-claims", 0),
      "vh_min_claims="       + numVal("srs-vh-claims", 3),
      "vh_max_after_bet="    + numVal("srs-vh-after-bet", 0),
      "vh_max_checkins="     + numVal("srs-vh-checkins", 9999),
      "ac_min_checkins="     + numVal("srs-ac-checkins", 14),
      "ac_min_referrals="    + numVal("srs-ac-referrals", 1),
    ].join("&");

    statePanel("srs-distribution-body", "loading", "Running simulation…");
    $("#cards-srs-impact").innerHTML = "";
    $("#cards-srs-match-rate").innerHTML = "";
    $("#srs-movements-body").innerHTML = "";

    api("/api/admin/dashboard/backend-segment-engine/segment-rule-simulator?" + params)
      .then(function (d) {
        var pi = d.production_impact || {};
        var mr = d.match_rate_impact || {};

        function diffBadge(cur, sim) {
          var diff = sim - cur;
          if (diff === 0) return "";
          var col = diff > 0 ? "var(--green,#4caf88)" : "var(--red,#e05c5c)";
          return ' <span style="color:' + col + ';font-size:11px;">(' + (diff > 0 ? "+" : "") + diff + ')</span>';
        }

        function rateDiff(cur, sim) {
          if (cur == null || sim == null) return "";
          var diff = Math.round((sim - cur) * 100) / 100;
          if (diff === 0) return "";
          var col = diff > 0 ? "var(--green,#4caf88)" : "var(--red,#e05c5c)";
          return ' <span style="color:' + col + ';font-size:11px;">(' + (diff > 0 ? "+" : "") + diff + '%)</span>';
        }

        // Production impact cards
        $("#cards-srs-impact").innerHTML =
          '<div class="kpi"><div class="label">Current Unclassified</div><div class="value">' + fmt(pi.current_unclassified) + '</div></div>' +
          '<div class="kpi"><div class="label">Simulated Unclassified</div><div class="value">' + fmt(pi.simulated_unclassified) + diffBadge(pi.current_unclassified, pi.simulated_unclassified) + '</div></div>' +
          '<div class="kpi"><div class="label">Current Ghost</div><div class="value">' + fmt(pi.current_ghost) + '</div></div>' +
          '<div class="kpi"><div class="label">Simulated Ghost</div><div class="value">' + fmt(pi.simulated_ghost) + diffBadge(pi.current_ghost, pi.simulated_ghost) + '</div></div>' +
          '<div class="kpi"><div class="label">Current Voucher Hunter</div><div class="value">' + fmt(pi.current_voucher_hunter) + '</div></div>' +
          '<div class="kpi"><div class="label">Simulated Voucher Hunter</div><div class="value">' + fmt(pi.simulated_voucher_hunter) + diffBadge(pi.current_voucher_hunter, pi.simulated_voucher_hunter) + '</div></div>';

        // Match rate cards
        $("#cards-srs-match-rate").innerHTML =
          '<div class="kpi"><div class="label">Current Match Rate</div><div class="value">' + (pi.current_match_rate != null ? pi.current_match_rate + "%" : "—") + '</div></div>' +
          '<div class="kpi"><div class="label">Simulated Match Rate</div><div class="value">' + (pi.simulated_match_rate != null ? pi.simulated_match_rate + "%" : "—") + rateDiff(pi.current_match_rate, pi.simulated_match_rate) + '</div></div>' +
          '<div class="kpi"><div class="label">Current Mismatch Rate</div><div class="value">' + (mr.current_mismatch_rate != null ? mr.current_mismatch_rate + "%" : "—") + '</div></div>' +
          '<div class="kpi"><div class="label">Simulated Mismatch Rate</div><div class="value">' + (mr.simulated_mismatch_rate != null ? mr.simulated_mismatch_rate + "%" : "—") + rateDiff(mr.current_mismatch_rate, mr.simulated_mismatch_rate) + '</div></div>' +
          '<div class="kpi"><div class="label">Compared Users</div><div class="value">' + fmt(mr.compared_users) + '</div></div>' +
          '<div class="kpi"><div class="label">Match Rate Δ</div><div class="value">' + (mr.match_rate_delta != null ? (mr.match_rate_delta > 0 ? "+" : "") + mr.match_rate_delta + "%" : "—") + '</div></div>';

        // Distribution table
        var dist = d.segment_distribution || [];
        if (dist.length) {
          var distRows = dist.map(function (r) {
            var diff = r.difference || 0;
            var diffCol = diff > 0 ? "var(--green,#4caf88)" : diff < 0 ? "var(--red,#e05c5c)" : "";
            return "<tr>" +
              "<td><b>" + esc(r.segment) + "</b></td>" +
              '<td class="num">' + fmt(r.current_users) + "</td>" +
              '<td class="num">' + fmt(r.simulated_users) + "</td>" +
              '<td class="num" style="color:' + diffCol + '">' + esc(r.difference_str) + "</td>" +
              "</tr>";
          }).join("");
          $("#srs-distribution-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Segment</th><th class="num">Current</th><th class="num">Simulated</th><th class="num">Difference</th></tr></thead>' +
            "<tbody>" + distRows + "</tbody></table></div>";
        } else {
          $("#srs-distribution-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // Movements table
        var moves = d.top_movements || [];
        if (moves.length) {
          var moveRows = moves.map(function (r) {
            return "<tr>" +
              "<td>" + esc(r.from_segment) + "</td>" +
              "<td>→</td>" +
              "<td>" + esc(r.to_segment) + "</td>" +
              '<td class="num"><b>' + fmt(r.users) + "</b></td>" +
              "</tr>";
          }).join("");
          $("#srs-movements-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>From Segment</th><th></th><th>To Segment</th><th class="num">Users</th></tr></thead>' +
            "<tbody>" + moveRows + "</tbody></table></div>";
        } else {
          $("#srs-movements-body").innerHTML = '<div class="empty">No segment movements — all users stayed in the same segment.</div>';
        }
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("srs-distribution-body", "banner error", "Simulation failed: " + esc(e.message));
        }
      });
  }

  // ---------- VH Priority Impact Analysis (Phase 7C) ----------
  function runVhPriorityImpact() {
    var week = ($("#vhpi-week") || {}).value || "";
    if (!week) {
      statePanel("vhpi-migration-body", "banner", "Enter a snapshot_week and click Run Analysis.");
      ["cards-vhpi-decision","cards-vhpi-summary","cards-vhpi-lv","vhpi-candidates-body"].forEach(function(id){ $("#"+id).innerHTML=""; });
      return;
    }
    statePanel("vhpi-migration-body", "loading", "Running analysis…");
    ["cards-vhpi-decision","cards-vhpi-summary","cards-vhpi-lv","vhpi-candidates-body"].forEach(function(id){ $("#"+id).innerHTML=""; });

    api("/api/admin/dashboard/backend-segment-engine/vh-priority-impact?snapshot_week=" + encodeURIComponent(week))
      .then(function (d) {
        var sm = d.summary || {};
        var lv = d.low_value_impact || {};
        var dm = d.decision_metrics || {};
        var ev = (d.extreme_vh || {}).extreme_vh;

        // Decision metrics cards (key KPIs first)
        function pctColor(pct) {
          return pct == null ? "" : pct >= 50 ? "var(--red,#e05c5c)" : pct >= 20 ? "var(--yellow,#e0b44a)" : "var(--green,#4caf88)";
        }
        $("#cards-vhpi-decision").innerHTML =
          '<div class="kpi"><div class="label">Low Value Removed %</div>' +
          '<div class="value" style="color:' + pctColor(dm.low_value_removed_pct) + '">' +
          (dm.low_value_removed_pct != null ? dm.low_value_removed_pct + "%" : "—") + '</div></div>' +
          dqCard("VH Growth (% of total)", { value: dm.voucher_hunter_growth_pct != null ? dm.voucher_hunter_growth_pct + "%" : "—" }) +
          dqCard("Extreme VH Users", { value: ev != null ? ev : "—", note: "claims≥20, bet/claim<2" });

        // Summary cards
        $("#cards-vhpi-summary").innerHTML =
          dqCard("Users Scanned", { value: sm.users_scanned }) +
          dqCard("Users Changed", { value: sm.users_changed }) +
          dqCard("Low Value → VH", { value: sm.low_value_to_voucher_hunter }) +
          dqCard("Normal Actual → VH", { value: sm.normal_actual_to_voucher_hunter }) +
          dqCard("Other → VH", { value: sm.other_to_voucher_hunter });

        // Low value impact cards
        $("#cards-vhpi-lv").innerHTML =
          dqCard("Current Low Value", { value: lv.current_low_value }) +
          dqCard("Remaining Low Value", { value: lv.remaining_low_value }) +
          dqCard("Moved to VH", { value: lv.moved_to_voucher_hunter }) +
          '<div class="kpi"><div class="label">% of Low Value Removed</div>' +
          '<div class="value" style="color:' + pctColor(lv.pct_removed) + '">' +
          (lv.pct_removed != null ? lv.pct_removed + "%" : "—") + '</div></div>';

        // Migration breakdown table
        var mig = d.migration_breakdown || [];
        if (mig.length) {
          var migRows = mig.map(function(r) {
            return "<tr><td>" + esc(r.from_segment) + "</td><td>→</td><td>" + esc(r.to_segment) + "</td>" +
              '<td class="num"><b>' + fmt(r.users) + "</b></td></tr>";
          }).join("");
          $("#vhpi-migration-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>From</th><th></th><th>To</th><th class="num">Users</th></tr></thead>' +
            "<tbody>" + migRows + "</tbody></table></div>";
        } else {
          $("#vhpi-migration-body").innerHTML = '<div class="empty">No segment movements — priority change has no effect on this snapshot.</div>';
        }

        // Candidate table
        var cands = d.candidates || [];
        if (cands.length) {
          function fmtN(v) { return (v == null || v === "") ? "—" : (typeof v === "number" ? v.toFixed(2) : esc(String(v))); }
          function fmtI(v) { return (v == null || v === "") ? "—" : fmt(v); }
          var cRows = cands.map(function(r) {
            return "<tr>" +
              "<td>" + esc(r.account || "") + "</td>" +
              "<td>" + esc(r.current_segment || "") + "</td>" +
              "<td><b>" + esc(r.simulated_segment || "") + "</b></td>" +
              '<td class="num">' + fmtI(r.claim_count) + "</td>" +
              '<td class="num">' + fmtN(r.after_bet) + "</td>" +
              '<td class="num">' + fmtN(r.withdrawal) + "</td>" +
              '<td class="num">' + fmtN(r.after_bet_multiple) + "</td>" +
              '<td class="num">' + fmtN(r.after_bet_per_claim) + "</td>" +
              "<td>" + esc(r.claim_risk_level || "") + "</td>" +
              "<td>" + esc(r.player_age_type || "") + "</td>" +
              "</tr>";
          }).join("");
          $("#vhpi-candidates-body").innerHTML =
            '<div style="font-size:11px;color:var(--muted,#8892a4);margin-bottom:8px;">' + cands.length + ' users shown</div>' +
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Account</th><th>Current</th><th>Simulated</th>' +
            "<th class='num'>Claims</th><th class='num'>After Bet</th><th class='num'>Withdrawal</th>" +
            "<th class='num'>Bet Multiple</th><th class='num'>Bet/Claim</th>" +
            "<th>Claim Risk</th><th>Age Type</th></tr></thead>" +
            "<tbody>" + cRows + "</tbody></table></div>";
        } else {
          $("#vhpi-candidates-body").innerHTML = '<div class="empty">No users would change segment.</div>';
        }
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("vhpi-migration-body", "banner error", "Analysis failed: " + esc(e.message));
        }
      });
  }

  // ---------- Voucher Hunter Rule Simulator (Phase 6A) ----------
  function runVoucherHunterRuleSimulator() {
    var week = ($("#vhrs-week") || {}).value || "";
    if (!week) {
      statePanel("vhrs-migration-body", "banner", "Enter a snapshot_week and click Run Simulation.");
      ["cards-vhrs-summary","cards-vhrs-match","vhrs-fp-body","vhrs-fn-body"].forEach(function(id){ $("#"+id).innerHTML=""; });
      return;
    }

    function numVal(id, def) { var v = parseFloat(($("#"+id)||{}).value); return isNaN(v) ? def : v; }
    function chk(id) { var el = $("#"+id); return el ? el.checked : true; }

    var params = [
      "snapshot_week="       + encodeURIComponent(week),
      "claim_threshold="     + numVal("vhrs-claim", 10),
      "after_bet_threshold=" + numVal("vhrs-bet", 100),
      "referral_threshold="  + numVal("vhrs-ref", 20),
      "withdrawal_protection=" + (chk("vhrs-wd-protect") ? "true" : "false"),
      "high_bet_protection="   + (chk("vhrs-hb-protect") ? "true" : "false"),
    ].join("&");

    statePanel("vhrs-migration-body", "loading", "Running simulation…");
    ["cards-vhrs-summary","cards-vhrs-match","vhrs-fp-body","vhrs-fn-body"].forEach(function(id){ $("#"+id).innerHTML=""; });

    api("/api/admin/dashboard/backend-segment-engine/voucher-hunter-rule-simulator?" + params)
      .then(function (d) {
        var ss = d.simulation_summary || {};
        var mr = d.match_rate_simulation || {};

        function diffBadge(cur, sim) {
          if (cur == null || sim == null) return "";
          var diff = sim - cur;
          if (diff === 0) return "";
          var col = diff > 0 ? "var(--green,#4caf88)" : "var(--red,#e05c5c)";
          return ' <span style="color:' + col + ';font-size:11px;">(' + (diff > 0 ? "+" : "") + diff + ')</span>';
        }
        function rateDiff(cur, sim) {
          if (cur == null || sim == null) return "";
          var diff = Math.round((sim - cur) * 100) / 100;
          if (diff === 0) return "";
          var col = diff > 0 ? "var(--green,#4caf88)" : "var(--red,#e05c5c)";
          return ' <span style="color:' + col + ';font-size:11px;">(' + (diff > 0 ? "+" : "") + diff + '%)</span>';
        }

        // Summary cards
        $("#cards-vhrs-summary").innerHTML =
          dqCard("Total Users", { value: ss.total_users }) +
          dqCard("Current Backend VH", { value: ss.current_backend_voucher_hunter_count }) +
          dqCard("Simulated VH", { value: ss.simulated_voucher_hunter_count,
            note: (ss.simulated_voucher_hunter_pct != null ? ss.simulated_voucher_hunter_pct + "%" : "") }) +
          dqCard("UIM VH", { value: ss.current_uim_voucher_hunter_count });

        // Match rate cards
        $("#cards-vhrs-match").innerHTML =
          dqCard("Previous Match Rate", { value: (mr.previous_match_rate != null ? mr.previous_match_rate + "%" : "—") }) +
          dqCard("Simulated Match Rate", { value: (mr.match_rate != null ? mr.match_rate + "%" : "—") + (mr.delta != null ? ' <span style="font-size:11px;color:' + (mr.delta >= 0 ? "var(--green,#4caf88)" : "var(--red,#e05c5c)") + '">(' + (mr.delta >= 0 ? "+" : "") + mr.delta + "%)</span>" : "") }) +
          dqCard("Compared Users", { value: mr.compared_users }) +
          dqCard("Current Matches", { value: mr.current_matches }) +
          dqCard("Simulated Matches", { value: mr.simulated_matches,
            note: mr.simulated_mismatches != null ? mr.simulated_mismatches + " mismatches" : "" });

        // Migration table
        var mig = d.segment_migration || [];
        if (mig.length) {
          var migRows = mig.map(function(r) {
            return "<tr><td>" + esc(r.from_segment) + "</td><td>→</td><td>" + esc(r.to_segment) + "</td>" +
              '<td class="num"><b>' + fmt(r.users) + "</b></td></tr>";
          }).join("");
          $("#vhrs-migration-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>From</th><th></th><th>To</th><th class="num">Users</th></tr></thead>' +
            "<tbody>" + migRows + "</tbody></table></div>";
        } else {
          $("#vhrs-migration-body").innerHTML = '<div class="empty">No segment movements — rule produced identical classifications.</div>';
        }

        // Shared candidate table renderer
        function renderCandidates(rows, containerId) {
          if (!rows || !rows.length) { $("#"+containerId).innerHTML = '<div class="empty">No candidates.</div>'; return; }
          function fmtNum(v) { return (v == null || v === "") ? "—" : fmt(v); }
          var trs = rows.map(function(r) {
            return "<tr>" +
              "<td>" + esc(r.account || "") + "</td>" +
              "<td>" + esc(r.backend_segment || "") + "</td>" +
              "<td>" + esc(r.uim_segment || "") + "</td>" +
              '<td class="num">' + fmtNum(r.after_bet) + "</td>" +
              '<td class="num">' + fmtNum(r.withdrawal) + "</td>" +
              '<td class="num">' + fmtNum(r.claims) + "</td>" +
              '<td class="num">' + fmtNum(r.referrals) + "</td>" +
              '<td class="num">' + fmtNum(r.checkins) + "</td>" +
              "<td>" + esc(r.player_age_type || "") + "</td>" +
              "<td>" + esc(r.claim_risk_level || "") + "</td>" +
              "<td>" + esc(r.confidence || "") + "</td>" +
              "</tr>";
          }).join("");
          $("#"+containerId).innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Account</th><th>Backend Seg.</th><th>UIM Seg.</th>' +
            "<th class='num'>After Bet</th><th class='num'>Withdraw</th>" +
            "<th class='num'>Claims</th><th class='num'>Referrals</th><th class='num'>Checkins</th>" +
            "<th>Age Type</th><th>Claim Risk</th><th>Confidence</th></tr></thead>" +
            "<tbody>" + trs + "</tbody></table></div>";
        }

        renderCandidates(d.false_positive_review, "vhrs-fp-body");
        renderCandidates(d.false_negative_review, "vhrs-fn-body");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("vhrs-migration-body", "banner error", "Simulation failed: " + esc(e.message));
        }
      });
  }

  // ---------- Voucher Hunter False Positive Analysis (Phase 5E-FP) ----------
  function loadVoucherHunterFalsePositive() {
    var week = ($("#vhfp-week") || {}).value || "";
    if (!week) {
      $("#cards-vhfp-summary").innerHTML = "";
      statePanel("vhfp-atb-body", "banner", "Enter a snapshot_week and click Run Analysis.");
      ["vhfp-wd-body","vhfp-ref-body","vhfp-chk-body","vhfp-matrix-body","vhfp-fp-body"].forEach(function(id){ $("#"+id).innerHTML=""; });
      return;
    }
    statePanel("vhfp-atb-body", "loading", "Running analysis…");
    ["cards-vhfp-summary","vhfp-wd-body","vhfp-ref-body","vhfp-chk-body","vhfp-matrix-body","vhfp-fp-body"].forEach(function(id){ $("#"+id).innerHTML=""; });

    api("/api/admin/dashboard/backend-segment-engine/voucher-hunter-false-positive-analysis?snapshot_week=" + encodeURIComponent(week))
      .then(function (d) {
        var kpis = d.summary_kpis || {};

        // Summary cards
        $("#cards-vhfp-summary").innerHTML =
          dqCard("Total UIM VH", { value: kpis.total_uim_voucher_hunter }) +
          dqCard("With Any Bet", { value: kpis.users_with_any_bet, note: (kpis.users_with_any_bet_pct != null ? kpis.users_with_any_bet_pct + "%" : "") }) +
          dqCard("With Any Withdrawal", { value: kpis.users_with_any_withdrawal, note: (kpis.users_with_any_withdrawal_pct != null ? kpis.users_with_any_withdrawal_pct + "%" : "") }) +
          dqCard("With Any Referral", { value: kpis.users_with_any_referral, note: (kpis.users_with_any_referral_pct != null ? kpis.users_with_any_referral_pct + "%" : "") }) +
          dqCard("Bet ≥ 1000", { value: kpis.users_bet_gte_1000, note: (kpis.users_bet_gte_1000_pct != null ? kpis.users_bet_gte_1000_pct + "%" : "") });

        // Generic 3-col distribution table renderer
        function dist3(rows, containerId, cols) {
          if (!rows || !rows.length) { $("#"+containerId).innerHTML = '<div class="empty">No data.</div>'; return; }
          var ths = cols.map(function(c){ return "<th" + (c.num ? " class='num'" : "") + ">" + esc(c.label) + "</th>"; }).join("");
          var trs = rows.map(function(r) {
            return "<tr>" + cols.map(function(c) {
              var v = r[c.key];
              if (c.num) return '<td class="num">' + (v == null ? "—" : (c.pct ? v + "%" : (typeof v === "number" ? v.toFixed(2) : esc(String(v))))) + "</td>";
              return "<td><b>" + esc(String(v || "")) + "</b></td>";
            }).join("") + "</tr>";
          }).join("");
          $("#"+containerId).innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table"><thead><tr>' + ths + '</tr></thead><tbody>' + trs + '</tbody></table></div>';
        }

        // After Bet Distribution
        dist3(d.after_bet_distribution, "vhfp-atb-body", [
          {label:"Bucket", key:"bucket"},
          {label:"Users", key:"users", num:true},
          {label:"%", key:"percentage", num:true, pct:true},
          {label:"Avg Claims", key:"avg_claims", num:true},
          {label:"Avg Referrals", key:"avg_referrals", num:true},
          {label:"Avg Checkins", key:"avg_checkins", num:true},
        ]);

        // Withdrawal Distribution
        dist3(d.withdrawal_distribution, "vhfp-wd-body", [
          {label:"Bucket", key:"bucket"},
          {label:"Users", key:"users", num:true},
          {label:"%", key:"percentage", num:true, pct:true},
          {label:"Avg After Bet", key:"avg_after_bet", num:true},
        ]);

        // Referral Distribution
        dist3(d.referral_distribution, "vhfp-ref-body", [
          {label:"Bucket", key:"bucket"},
          {label:"Users", key:"users", num:true},
          {label:"%", key:"percentage", num:true, pct:true},
          {label:"Avg After Bet", key:"avg_after_bet", num:true},
          {label:"Avg Claims", key:"avg_claims", num:true},
        ]);

        // Check-in Distribution
        dist3(d.checkin_distribution, "vhfp-chk-body", [
          {label:"Bucket", key:"bucket"},
          {label:"Users", key:"users", num:true},
          {label:"%", key:"percentage", num:true, pct:true},
          {label:"Avg Claims", key:"avg_claims", num:true},
        ]);

        // Evidence Matrix
        dist3(d.evidence_matrix, "vhfp-matrix-body", [
          {label:"Backend Segment", key:"backend_segment"},
          {label:"Count", key:"count", num:true},
          {label:"%", key:"percentage", num:true, pct:true},
          {label:"Avg After Bet", key:"avg_after_bet", num:true},
          {label:"Avg Withdrawal", key:"avg_withdrawal", num:true},
          {label:"Avg Claims", key:"avg_claims", num:true},
          {label:"Avg Referrals", key:"avg_referrals", num:true},
          {label:"Avg Checkins", key:"avg_checkins", num:true},
        ]);

        // False Positive Candidates
        var fps = d.false_positive_candidates || [];
        if (fps.length) {
          function fmtNum(v) { return (v == null || v === "") ? "—" : fmt(v); }
          var fpRows = fps.map(function(r) {
            return "<tr>" +
              "<td>" + esc(r.account || "") + "</td>" +
              "<td>" + esc(r.backend_segment || "") + "</td>" +
              "<td>voucher_hunter</td>" +
              '<td class="num">' + fmtNum(r.after_bet) + "</td>" +
              '<td class="num">' + fmtNum(r.withdrawal) + "</td>" +
              '<td class="num">' + fmtNum(r.claims) + "</td>" +
              '<td class="num">' + fmtNum(r.referrals) + "</td>" +
              '<td class="num">' + fmtNum(r.checkins) + "</td>" +
              "<td>" + esc(r.player_age_type || "") + "</td>" +
              "<td>" + esc(r.claim_risk_level || "") + "</td>" +
              "<td>" + esc(r.confidence || "") + "</td>" +
              "</tr>";
          }).join("");
          $("#vhfp-fp-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Account</th><th>Backend Seg.</th><th>UIM Seg.</th>' +
            "<th class='num'>After Bet</th><th class='num'>Withdraw</th>" +
            "<th class='num'>Claims</th><th class='num'>Referrals</th><th class='num'>Checkins</th>" +
            "<th>Age Type</th><th>Claim Risk</th><th>Confidence</th></tr></thead>" +
            "<tbody>" + fpRows + "</tbody></table></div>";
        } else {
          $("#vhfp-fp-body").innerHTML = '<div class="empty">No false positive candidates found.</div>';
        }
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("vhfp-atb-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  // ---------- Voucher Hunter Rule Quality Analysis (Phase 5E) ----------
  function loadVoucherHunterQuality() {
    var week = ($("#vhqa-week") || {}).value || "";
    if (!week) {
      $("#cards-vhqa-summary").innerHTML = "";
      statePanel("vhqa-breakdown-body", "banner", "Enter a snapshot_week and click Run Analysis.");
      $("#vhqa-threshold-body").innerHTML = "";
      $("#vhqa-top-claims-body").innerHTML = "";
      $("#vhqa-top-bet-body").innerHTML = "";
      $("#vhqa-top-refs-body").innerHTML = "";
      return;
    }
    statePanel("vhqa-breakdown-body", "loading", "Running analysis…");
    $("#cards-vhqa-summary").innerHTML = "";
    $("#vhqa-threshold-body").innerHTML = "";
    $("#vhqa-top-claims-body").innerHTML = "";
    $("#vhqa-top-bet-body").innerHTML = "";
    $("#vhqa-top-refs-body").innerHTML = "";

    api("/api/admin/dashboard/backend-segment-engine/voucher-hunter-quality-analysis?snapshot_week=" + encodeURIComponent(week))
      .then(function (d) {
        // Summary card
        $("#cards-vhqa-summary").innerHTML =
          dqCard("Total UIM Voucher Hunter", { value: d.total_uim_voucher_hunter });

        // Group breakdown table
        var groups = d.group_breakdown || [];
        if (groups.length) {
          var gRows = groups.map(function (r) {
            function fmtA(v) { return v == null ? "—" : (typeof v === "number" ? v.toFixed(2) : esc(String(v))); }
            return "<tr>" +
              "<td><b>" + esc(r.backend_segment) + "</b></td>" +
              '<td class="num">' + fmt(r.user_count) + "</td>" +
              '<td class="num">' + (r.pct_of_total != null ? r.pct_of_total + "%" : "—") + "</td>" +
              '<td class="num">' + fmtA(r.avg_after_bet) + "</td>" +
              '<td class="num">' + fmtA(r.avg_withdraw) + "</td>" +
              '<td class="num">' + fmtA(r.avg_claims) + "</td>" +
              '<td class="num">' + fmtA(r.avg_referrals) + "</td>" +
              '<td class="num">' + fmtA(r.avg_checkins) + "</td>" +
              "<td>" + esc(r.dominant_claim_risk || "—") + "</td>" +
              '<td class="num">' + fmt(r.new_players) + "</td>" +
              '<td class="num">' + fmt(r.old_players) + "</td>" +
              "</tr>";
          }).join("");
          $("#vhqa-breakdown-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr>' +
            "<th>Backend Segment</th><th class='num'>Users</th><th class='num'>%</th>" +
            "<th class='num'>Avg After Bet</th><th class='num'>Avg Withdraw</th>" +
            "<th class='num'>Avg Claims</th><th class='num'>Avg Referrals</th><th class='num'>Avg Checkins</th>" +
            "<th>Dom. Claim Risk</th><th class='num'>New</th><th class='num'>Old</th>" +
            "</tr></thead><tbody>" + gRows + "</tbody></table></div>";
        } else {
          $("#vhqa-breakdown-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // Claim threshold breakdown
        var thresholds = d.claim_threshold_breakdown || [];
        if (thresholds.length) {
          var tRows = thresholds.map(function (r) {
            return "<tr>" +
              "<td><b>" + esc(r.threshold) + "</b></td>" +
              '<td class="num">' + fmt(r.count) + "</td>" +
              '<td class="num">' + (r.percentage != null ? r.percentage + "%" : "—") + "</td>" +
              "</tr>";
          }).join("");
          $("#vhqa-threshold-body").innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Threshold</th><th class="num">Count</th><th class="num">% of UIM VH</th></tr></thead>' +
            "<tbody>" + tRows + "</tbody></table></div>";
        } else {
          $("#vhqa-threshold-body").innerHTML = '<div class="empty">No data.</div>';
        }

        // Shared sample table renderer
        function renderTopTable(rows, containerId) {
          if (!rows || !rows.length) {
            $("#" + containerId).innerHTML = '<div class="empty">No data.</div>';
            return;
          }
          function fmtNum(v) { return (v == null || v === "") ? "—" : fmt(v); }
          var tRows = rows.map(function (r) {
            return "<tr>" +
              "<td>" + esc(r.account || "") + "</td>" +
              "<td>" + esc(r.backend_segment || "") + "</td>" +
              '<td class="num">' + fmtNum(r.claims) + "</td>" +
              '<td class="num">' + fmtNum(r.after_bet) + "</td>" +
              '<td class="num">' + fmtNum(r.withdraw) + "</td>" +
              '<td class="num">' + fmtNum(r.referrals) + "</td>" +
              '<td class="num">' + fmtNum(r.checkins) + "</td>" +
              "<td>" + esc(r.age_type || "") + "</td>" +
              "<td>" + esc(r.claim_risk || "") + "</td>" +
              "<td>" + esc(r.confidence || "") + "</td>" +
              "</tr>";
          }).join("");
          $("#" + containerId).innerHTML =
            '<div style="overflow-x:auto"><table class="mini-table">' +
            '<thead><tr><th>Account</th><th>Backend Seg.</th>' +
            "<th class='num'>Claims</th><th class='num'>After Bet</th><th class='num'>Withdraw</th>" +
            "<th class='num'>Referrals</th><th class='num'>Checkins</th>" +
            "<th>Age Type</th><th>Claim Risk</th><th>Confidence</th></tr></thead>" +
            "<tbody>" + tRows + "</tbody></table></div>";
        }

        renderTopTable(d.top_by_claims,    "vhqa-top-claims-body");
        renderTopTable(d.top_by_after_bet, "vhqa-top-bet-body");
        renderTopTable(d.top_by_referrals, "vhqa-top-refs-body");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("vhqa-breakdown-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  // ---------- Identity Match Audit ----------
  function _imaRateColor(pct) {
    return pct >= 80 ? "var(--green,#4caf88)" : pct >= 40 ? "var(--yellow,#e0b44a)" : "var(--red,#e05c5c)";
  }

  function loadIdentityMatchAudit() {
    var week = ($("#ima-week") || {}).value || "";
    if (!week) {
      $("#cards-ima-summary").innerHTML = "";
      statePanel("ima-body", "empty", "Enter a snapshot_week and click Run Audit.");
      return;
    }
    statePanel("ima-body", "loading", "Running audit — querying marketing_raw_data and voucher_claims…");
    $("#cards-ima-summary").innerHTML = "";
    api("/api/admin/dashboard/backend-segment-engine/identity-match-audit?snapshot_week=" +
        encodeURIComponent(week))
      .then(function (d) {
        var rateColor = _imaRateColor(d.identity_match_rate);
        var rateNote = d.identity_match_rate >= 80 ? "healthy" : d.identity_match_rate >= 40 ? "partial" : "critical";

        $("#cards-ima-summary").innerHTML =
          dqCard("Total Rows", { value: d.total_rows }) +
          dqCard("Matched Rows", { value: d.matched_rows, note: "user_id resolved" }) +
          dqCard("Unmatched Rows", { value: d.unmatched_rows, note: "no coupon match" }) +
          '<div class="kpi"><div class="label">Identity Match Rate</div>' +
          '<div class="value" style="color:' + rateColor + '">' + d.identity_match_rate + '%</div>' +
          '<div class="sub">' + rateNote + ' &middot; ' + esc(d.snapshot_week) + '</div></div>';

        var cols = ["account", "coupon_code", "user_id"];
        var thRow = cols.map(function (h) { return '<th>' + esc(h) + '</th>'; }).join('');

        function sampleTable(rows, title, matched) {
          var html = '<div style="font-weight:600;font-size:12px;margin:16px 0 6px;">' + title + ' (up to 20)</div>';
          if (rows && rows.length) {
            html += '<div style="overflow-x:auto"><table class="mini-table"><thead><tr>' + thRow + '</tr></thead><tbody>';
            rows.forEach(function (r) {
              var muted = matched ? "" : ' style="color:var(--muted,#8892a4)"';
              html += '<tr><td>' + esc(r.account || "") + '</td>' +
                '<td' + muted + '>' + esc(r.coupon_code || "—") + '</td>' +
                '<td' + muted + '>' + esc(r.user_id || "—") + '</td></tr>';
            });
            html += '</tbody></table></div>';
          } else {
            html += '<div class="empty">No rows.</div>';
          }
          return html;
        }

        var html = sampleTable(d.sample_matched, "Sample Matched Rows", true);
        html += sampleTable(d.sample_unmatched, "Sample Unmatched Rows", false);
        $("#ima-body").innerHTML = html;
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("ima-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  // ---------- Upload Player Performance (Phase 2A) ----------
  function rowsByTable(map, label) {
    var keys = Object.keys(map || {}).sort();
    if (!keys.length) return '<div class="empty">No ' + esc(label) + ' rows.</div>';
    return '<table class="mini-table"><thead><tr><th>' + esc(label) + '</th><th class="num">Rows</th></tr></thead><tbody>' +
      keys.map(function (k) {
        return '<tr><td>' + esc(k) + '</td><td class="num">' + fmt(map[k]) + '</td></tr>';
      }).join("") +
      '</tbody></table>';
  }

  function coverageText(coverage) {
    coverage = coverage || {};
    var months = coverage.months_label || (coverage.months || []).join(", ");
    var weeks = coverage.weeks_label || ((coverage.week_start && coverage.week_end) ? coverage.week_start + " -> " + coverage.week_end : "");
    return { months: months || "none", weeks: weeks || "none" };
  }

  function uploadPlayerPerformance() {
    var input = $("#upload-file-input");
    var file = input && input.files && input.files[0];
    if (!file) { statePanel("upload-result-body", "empty", "Select a file first."); return; }
    statePanel("upload-result-body", "loading", "Uploading and importing…");
    var formData = new FormData();
    formData.append("file", file);
    var manualPeriod = (($("#upload-manual-period") || {}).value || "").trim();
    if (manualPeriod) formData.append("manual_period", manualPeriod);
    fetch("/api/admin/data/upload-player-performance" + window.location.search, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Accept": "application/json" },
      body: formData,
    })
      .then(function (r) {
        if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
        return r.json().then(function (j) { return { ok: r.ok, body: j }; });
      })
      .then(function (res) {
        var d = res.body;
        if (!res.ok || !d.success) {
          statePanel("upload-result-body", "banner error", "Failed: " + esc(d.message || "upload failed"));
          return;
        }
        var cov = coverageText(d.coverage);
        $("#upload-result-body").innerHTML =
          '<div class="card-grid">' +
          dqCard("Rows Total", { value: d.rows_total }) +
          dqCard("Rows Imported", { value: d.rows_imported }) +
          dqCard("Rows Failed", { value: d.rows_failed }) +
          dqCard("Duplicate Rows", { value: d.duplicate_rows }) +
          "</div>" +
          '<table class="mini-table"><tbody>' +
          "<tr><td>Period Source</td><td>" + esc(d.period_source || "") + "</td></tr>" +
          "<tr><td>Months</td><td>" + esc(cov.months) + "</td></tr>" +
          "<tr><td>Weeks</td><td>" + esc(cov.weeks) + "</td></tr>" +
          "<tr><td>Upload Batch ID</td><td>" + esc(d.upload_batch_id) + "</td></tr>" +
          "<tr><td>Status</td><td>" + esc(d.status) + "</td></tr>" +
          "</tbody></table>" +
          '<div class="section-title">Rows by Month</div>' +
          rowsByTable(d.rows_by_snapshot_month, "Month") +
          '<div class="section-title">Rows by Week</div>' +
          rowsByTable(d.rows_by_snapshot_week, "Week");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("upload-result-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  // ---------- Upload History (Phase 2A) ----------
  function renderSegSyncStatus(s) {
    if (s === null || s === undefined || s === "pending") return "Not Started";
    if (s === "running") return "Syncing";
    if (s === "completed") return "Synced";
    if (s === "failed") return "Failed";
    return "Not Started";
  }

  function loadUimImportHistory() {
    statePanel("uim-import-history-body", "loading", "Loading UIM import history…");
    api("/api/admin/data/uim-import/history")
      .then(function (d) {
        var rows = (d.batches || []).map(function (b) {
          return "<tr><td>" + esc(b.committed_at) + "</td>" +
            '<td class="num">' + fmt(b.rows_written) + "</td>" +
            '<td class="num">' + fmt(b.rows_scanned) + "</td>" +
            '<td class="num">' + fmt(b.users_updated) + "</td>" +
            '<td class="num">' + fmt(b.users_missing) + "</td>" +
            "<td>" + renderSegSyncStatus(b.seg_sync_status) + "</td>" +
            "<td>" + esc(b.batch_id) + "</td></tr>";
        }).join("");
        $("#uim-import-history-body").innerHTML = rows
          ? '<table class="mini-table"><thead><tr><th>Committed At</th><th class="num">Rows Written</th><th class="num">Rows Scanned</th><th class="num">Users Updated</th><th class="num">Users Missing</th><th>Seg Sync</th><th>Batch ID</th></tr></thead><tbody>' + rows + "</tbody></table>"
          : '<div class="empty">No UIM imports yet.</div>';
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("uim-import-history-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  function loadUploadHistory() {
    statePanel("upload-history-body", "loading", "Loading upload history…");
    api("/api/admin/data/upload-history")
      .then(function (d) {
        var rows = (d.batches || []).map(function (b) {
          var cov = coverageText(b.coverage);
          var monthRows = Object.keys(b.rows_by_snapshot_month || {}).sort().map(function (m) {
            return esc(m) + ": " + fmt(b.rows_by_snapshot_month[m]);
          }).join("<br>");
          return "<tr><td>" + esc(b.uploaded_at) + "</td><td>" + esc(b.file_name) + "</td>" +
            "<td>" + esc(cov.months) + "</td><td>" + esc(cov.weeks) + "</td>" +
            "<td>" + (monthRows || "none") + "</td>" +
            '<td class="num">' + fmt(b.rows_imported) + '</td><td class="num">' + fmt(b.rows_failed) + "</td>" +
            "<td>" + esc(b.status) + "</td><td>" + esc(b.uploaded_by) + "</td></tr>";
        }).join("");
        $("#upload-history-body").innerHTML = rows
          ? '<table class="mini-table"><thead><tr><th>Upload Date</th><th>File Name</th><th>Months</th><th>Weeks</th><th>Rows by Month</th><th class="num">Rows Imported</th><th class="num">Rows Failed</th><th>Status</th><th>Uploader</th></tr></thead><tbody>' + rows + "</tbody></table>"
          : '<div class="empty">No uploads yet.</div>';
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("upload-history-body", "banner error", "Failed: " + esc(e.message));
        }
      });
  }

  // ---------- Raw Data Explorer (Phase 2B) ----------
  function updateExplorerPeriodOptions(available, selectedType, selectedPeriod) {
    var select = $("#explorer-period");
    if (!select) return;
    var options = ((available || {})[selectedType] || []);
    select.innerHTML = options.length
      ? options.map(function (p) {
          return '<option value="' + esc(p) + '"' + (p === selectedPeriod ? " selected" : "") + ">" + esc(p) + "</option>";
        }).join("")
      : '<option value="">Latest available</option>';
  }

  function loadRawExplorer(force) {
    var type = (($("#explorer-period-type") || {}).value || "weekly").trim();
    var period = (($("#explorer-period") || {}).value || "").trim();
    var url = "/api/admin/data/raw-explorer";
    var params = ["period_type=" + encodeURIComponent(type)];
    if (period) params.push("period=" + encodeURIComponent(period));
    if (params.length) url += "?" + params.join("&");

    var bodies = ["cards-explorer-summary", "explorer-quality-body", "explorer-campaign-body",
                  "explorer-platform-body", "explorer-currency-body", "explorer-snapshot-body"];
    bodies.forEach(function (id) { statePanel(id, "loading", "Loading…"); });

    api(url)
      .then(function (d) {
        var sf = d.snapshot_filter || {};
        updateExplorerPeriodOptions(d.available_periods, sf.period_type || type, sf.period || period);
        var label = $("#explorer-snapshot-label");
        if (label) {
          var parts = [];
          if (sf.period_type) parts.push("View: " + esc(sf.period_type));
          if (sf.period) parts.push("Period: " + esc(sf.period));
          label.textContent = parts.length ? "Showing snapshot — " + parts.join(" / ") : "No data uploaded yet.";
        }

        // Summary cards
        var s = d.summary || {};
        var fmtAmt = function (v) { return v == null ? "—" : fmt(Math.round(v)); };
        var summaryCards = [
          dqCard("Rows Total", { value: fmt(s.rows_total) }),
          dqCard("Distinct Accounts", { value: fmt(s.distinct_accounts) }),
          dqCard("Campaigns", { value: fmt(s.campaign_count) }),
          dqCard("Platforms", { value: fmt(s.platform_count) }),
          dqCard("Currencies", { value: fmt(s.currency_count) }),
          dqCard("New Players", { value: fmt(s.new_players) }),
          dqCard("Total Withdraw", { value: fmtAmt(s.total_withdraw_amount) }),
          dqCard("Total After Bet", { value: fmtAmt(s.total_after_bet_amount) }),
        ].join("");
        $("#cards-explorer-summary").innerHTML = summaryCards || '<div class="empty">No data.</div>';

        // Data quality
        var dq = d.data_quality || {};
        var checks = dq.checks || {};
        var statusColor = { green: "var(--ok)", yellow: "var(--warn)", red: "var(--bad)" };
        var overallStatus = dq.overall_status || "green";
        var qualityRows = Object.keys(checks).map(function (key) {
          var c = checks[key];
          var color = statusColor[c.status] || "var(--muted)";
          var label = key.replace(/_/g, " ").replace(/\b\w/g, function (l) { return l.toUpperCase(); });
          return "<tr>" +
            "<td>" + esc(label) + "</td>" +
            '<td class="num">' + fmt(c.count) + "</td>" +
            '<td class="num">' + (c.pct != null ? c.pct.toFixed(2) + "%" : "—") + "</td>" +
            '<td><span style="color:' + color + ';font-weight:600;">' + esc(c.status.toUpperCase()) + "</span></td>" +
            "</tr>";
        }).join("");
        var overallColor = statusColor[overallStatus] || "var(--muted)";
        var qualityHtml = qualityRows
          ? '<div style="margin-bottom:8px;">Overall: <span style="color:' + overallColor + ';font-weight:600;">' + esc(overallStatus.toUpperCase()) + '</span> &nbsp;·&nbsp; Total rows checked: ' + fmt(dq.total_rows) + '</div>' +
            '<table class="mini-table"><thead><tr><th>Check</th><th class="num">Issues</th><th class="num">%</th><th>Status</th></tr></thead><tbody>' + qualityRows + "</tbody></table>"
          : '<div class="empty">No data.</div>';
        $("#explorer-quality-body").innerHTML = qualityHtml;

        // Campaign breakdown
        var camps = d.campaign_breakdown || [];
        var campRows = camps.map(function (c) {
          return "<tr><td>" + esc(c.campaign_id) + "</td><td>" + esc(c.campaign_name) + "</td>" +
            '<td class="num">' + fmt(c.rows) + "</td>" +
            '<td class="num">' + fmt(c.accounts) + "</td>" +
            '<td class="num">' + fmtAmt(c.withdraw_amount) + "</td>" +
            '<td class="num">' + fmtAmt(c.after_total_bet_amount) + "</td></tr>";
        }).join("");
        $("#explorer-campaign-body").innerHTML = campRows
          ? '<table class="mini-table"><thead><tr><th>Campaign ID</th><th>Campaign Name</th><th class="num">Rows</th><th class="num">Accounts</th><th class="num">Withdraw</th><th class="num">After Bet</th></tr></thead><tbody>' + campRows + "</tbody></table>"
          : '<div class="empty">No campaign data.</div>';

        // Platform breakdown
        var platforms = d.platform_breakdown || [];
        var platRows = platforms.map(function (p) {
          return "<tr><td>" + esc(p.platform_code) + "</td>" +
            '<td class="num">' + fmt(p.rows) + "</td>" +
            '<td class="num">' + fmt(p.accounts) + "</td>" +
            '<td class="num">' + fmtAmt(p.withdraw_amount) + "</td>" +
            '<td class="num">' + fmtAmt(p.after_total_bet_amount) + "</td></tr>";
        }).join("");
        $("#explorer-platform-body").innerHTML = platRows
          ? '<table class="mini-table"><thead><tr><th>Platform Code</th><th class="num">Rows</th><th class="num">Accounts</th><th class="num">Withdraw</th><th class="num">After Bet</th></tr></thead><tbody>' + platRows + "</tbody></table>"
          : '<div class="empty">No platform data.</div>';

        // Currency breakdown
        var currencies = d.currency_breakdown || [];
        var currRows = currencies.map(function (c) {
          return "<tr><td>" + esc(c.currency_code) + "</td>" +
            '<td class="num">' + fmt(c.rows) + "</td>" +
            '<td class="num">' + fmt(c.accounts) + "</td>" +
            '<td class="num">' + fmtAmt(c.withdraw_amount) + "</td>" +
            '<td class="num">' + fmtAmt(c.after_total_bet_amount) + "</td></tr>";
        }).join("");
        $("#explorer-currency-body").innerHTML = currRows
          ? '<table class="mini-table"><thead><tr><th>Currency Code</th><th class="num">Rows</th><th class="num">Accounts</th><th class="num">Withdraw</th><th class="num">After Bet</th></tr></thead><tbody>' + currRows + "</tbody></table>"
          : '<div class="empty">No currency data.</div>';

        // Snapshot history
        var snaps = d.snapshot_summary || [];
        var snapRows = snaps.map(function (sn) {
          var cov = coverageText(sn.coverage);
          return "<tr><td>" + esc(cov.weeks) + "</td><td>" + esc(cov.months) + "</td>" +
            '<td class="num">' + fmt(sn.rows) + "</td>" +
            "<td>" + esc(sn.uploaded_at) + "</td>" +
            "<td>" + esc(sn.file_name) + "</td>" +
            "<td>" + esc(sn.status) + "</td></tr>";
        }).join("");
        $("#explorer-snapshot-body").innerHTML = snapRows
          ? '<table class="mini-table"><thead><tr><th>Weeks</th><th>Months</th><th class="num">Rows in Period</th><th>Uploaded At</th><th>File</th><th>Status</th></tr></thead><tbody>' + snapRows + "</tbody></table>"
          : '<div class="empty">No upload records.</div>';
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          bodies.forEach(function (id) {
            statePanel(id, "banner error", "Failed: " + esc(e.message));
          });
        }
      });
  }

  function loadUser(query) {
    if (!query) { setMeta(""); statePanel("user-body", "empty", "Search by Telegram user_id or username to view a user profile."); return; }
    setMeta("Loading…");
    statePanel("user-body", "loading", "Searching…");
    api("/api/admin/dashboard/user?query=" + encodeURIComponent(query))
      .then(function (d) {
        renderMeta(d, "all time");
        if (!d.success) { statePanel("user-body", "empty", d.message || "No user found."); return; }
        var p = d.profile, x = d.xp || {}, ci = d.checkin || {}, rs = d.referral_stats || {}, ws = d.welcome_status || {};
        var risk = (d.risk_flags || []).length
          ? (d.risk_flags || []).map(function (f) { return '<span class="pill rejected" style="margin:2px;">' + esc(f) + "</span>"; }).join("")
          : '<span class="pill ok">none</span>';
        var vh = (d.voucher_history || []).map(function (v) {
          return "<tr><td>" + esc(v.drop_id) + '</td><td><span class="pill ' + esc((v.status || "neutral").toLowerCase().replace(/[^a-z_]/g, "")) + '">' + esc(v.status) + "</span></td><td>" + esc(v.voucher_code || "—") + "</td><td>" + dt(v.claimed_at || v.created_at) + "</td></tr>";
        }).join("") || '<tr><td colspan="4">No voucher history.</td></tr>';
        var ah = (d.affiliate_history || []).map(function (a) {
          return "<tr><td>" + esc(a.ledger_type) + "</td><td>" + esc(a.tier || "—") + '</td><td><span class="pill ' + esc((a.status || "neutral").toLowerCase()) + '">' + esc(a.status) + "</span></td><td>" + esc(a.year_month || "—") + "</td><td>" + dt(a.updated_at) + "</td></tr>";
        }).join("") || '<tr><td colspan="5">No affiliate history.</td></tr>';
        $("#user-body").innerHTML =
          '<div class="detail-grid">' +
          kvBlock("Profile", [["User ID", p.user_id], ["Username", p.username ? "@" + p.username : "—"], ["Name", p.first_name], ["Status", p.status], ["VIP Tier", p.vip_tier], ["Joined Main", dt(p.joined_main_at)]]) +
          kvBlock("Segment & XP", [["Segment", d.segment], ["Total XP", x.total_xp], ["Weekly XP", x.weekly_xp], ["Monthly XP", x.monthly_xp]]) +
          kvBlock("Check-in", [["Streak", ci.streak], ["Freeze Tokens", ci.streak_freeze_tokens], ["First Check-in", dt(ci.first_checkin_at)], ["Last Check-in", dt(ci.last_checkin)]]) +
          kvBlock("Referrals", [["Snapshot Total", rs.total_referrals_snapshot], ["Made", rs.referrals_made], ["Qualified", rs.referrals_qualified], ["Was Referred", rs.was_referred]]) +
          kvBlock("Welcome", [["Eligible", ws.eligible], ["Claimed", ws.claimed], ["Lifecycle", ws.lifecycle_state], ["Claimed At", dt(ws.claimed_at)]]) +
          '<div class="detail-block"><h4>Risk Flags</h4>' + risk + "</div>" +
          "</div>" +
          '<div class="detail-block" style="margin-top:14px;"><h4>Voucher History</h4><table class="mini-table"><thead><tr><th>Drop</th><th>Status</th><th>Code</th><th>When</th></tr></thead><tbody>' + vh + "</tbody></table></div>" +
          '<div class="detail-block" style="margin-top:14px;"><h4>Affiliate History</h4><table class="mini-table"><thead><tr><th>Type</th><th>Tier</th><th>Status</th><th>Month</th><th>Updated</th></tr></thead><tbody>' + ah + "</tbody></table></div>";
        if (d.partial_errors) banner("Some user metrics degraded: " + d.partial_errors.join("; "), "warn");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          statePanel("user-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  // ---------- Rejoin Buffer admin toggle (belongs to the Voucher Rules settings category) ----------
  var REJOIN_BUFFER_HTML =
    '<div class="section" style="max-width:520px;margin-bottom:20px;">' +
    '<div class="section-title">Rejoin Buffer</div>' +
    '<div style="font-size:12px;color:var(--muted);margin-bottom:12px;">' +
    'Delays public/pooled voucher claims for users who leave and rejoin @AdvantPlayOfficial. ' +
    'Never applies to Welcome Reward, personalised vouchers, or affiliate rewards.</div>' +
    '<div style="margin-bottom:10px;"><label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Mode</label>' +
    '<select class="filter-input" id="rb-mode" style="width:100%;box-sizing:border-box;">' +
    '<option value="disabled">Disabled</option><option value="test_users_only">Test Users Only</option><option value="enabled">Enabled</option>' +
    '</select></div>' +
    '<div style="margin-bottom:10px;"><label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Buffer hours</label>' +
    '<input class="filter-input" id="rb-hours" type="number" min="1" step="1" style="width:100%;box-sizing:border-box;" /></div>' +
    '<div style="margin-bottom:12px;"><label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Test user IDs ' +
    '<span style="font-weight:400;color:var(--muted);">(one per line or comma-separated; used only in Test Users Only mode)</span></label>' +
    '<textarea id="rb-test-user-ids" rows="4" style="width:100%;box-sizing:border-box;padding:8px;border:1px solid var(--border);border-radius:6px;background:var(--card-bg);color:var(--text);resize:vertical;" placeholder="123456789&#10;987654321"></textarea></div>' +
    '<button class="btn primary" id="rb-save-btn">Save</button>' +
    '<span class="admin-chip" id="rb-status" style="margin-left:8px;">…</span>' +
    '<div id="rb-result" style="margin-top:10px;font-size:12px;"></div>' +
    '</div>';

  function loadRejoinBufferSettings() {
    var statusEl = $("#rb-status");
    if (statusEl) statusEl.textContent = "Loading…";
    return api("/v2/miniapp/admin/rejoin-buffer/settings")
      .then(function (d) {
        var s = d.settings || {};
        var modeEl = $("#rb-mode");
        var hoursEl = $("#rb-hours");
        var idsEl = $("#rb-test-user-ids");
        if (modeEl) modeEl.value = s.mode || "disabled";
        if (hoursEl) hoursEl.value = s.hours || 12;
        if (idsEl) idsEl.value = (s.test_user_ids || []).join("\n");
        if (statusEl) {
          statusEl.textContent = "Current: " + (s.mode || "disabled") + " · " + (s.hours || 12) + "h · " +
            ((s.test_user_ids || []).length) + " test user(s)";
        }
      })
      .catch(function (e) {
        if (e.message !== "unauthorized" && statusEl) statusEl.textContent = "Failed to load: " + e.message;
      });
  }

  function bindRejoinBufferSettings() {
    var btn = $("#rb-save-btn");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var resultEl = $("#rb-result");
      var mode = ($("#rb-mode").value || "disabled").trim();
      var hours = parseFloat($("#rb-hours").value);
      if (!hours || hours <= 0) { resultEl.textContent = "Buffer hours must be a positive number."; return; }
      var idsRaw = ($("#rb-test-user-ids").value || "").trim();
      var testUserIds = idsRaw ? idsRaw.split(/[\n,]+/).map(function (s) { return s.trim(); }).filter(Boolean) : [];
      btn.disabled = true;
      resultEl.textContent = "Saving…";
      fetch("/v2/miniapp/admin/rejoin-buffer/settings", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", "Accept": "application/json" },
        body: JSON.stringify({ mode: mode, hours: hours, test_user_ids: testUserIds }),
      }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); })
        .then(function (res) {
          if (!res.ok || res.d.status !== "ok") throw new Error(res.d.message || res.d.code || "unknown");
          resultEl.textContent = "Saved.";
          toast("✅ Rejoin buffer settings saved", "success");
          loadRejoinBufferSettings();
        })
        .catch(function (e) { resultEl.textContent = "Failed: " + e.message; toast("❌ Failed to save rejoin buffer settings: " + e.message, "error"); })
        .finally(function () { btn.disabled = false; });
    });
  }

  // ---------- Managed Settings (schema-driven, editable, MongoDB-backed) ----------
  var managedSettingsState = { schema: {}, values: {} };

  function msFieldId(group, name, sub) {
    return "ms-" + group + "-" + name + (sub ? "-" + sub : "");
  }

  function msRenderField(group, name, def, value) {
    var id = msFieldId(group, name);
    var label = '<label for="' + id + '">' + esc(def.label || name) + "</label>";
    if (def.type === "bool") {
      return '<div class="ms-field ms-bool-field"><input type="checkbox" id="' + id + '"' + (value ? " checked" : "") + "/>" + label + "</div>";
    }
    if (def.type === "list") {
      var joined = Array.isArray(value) ? value.join(", ") : (value || "");
      return '<div class="ms-field">' + label + '<textarea class="filter-input" id="' + id + '" rows="2">' + esc(joined) + "</textarea></div>";
    }
    if (def.type === "str" && Array.isArray(def.choices)) {
      var opts = def.choices.map(function (c) {
        return '<option value="' + esc(c) + '"' + (c === value ? " selected" : "") + ">" + esc(c) + "</option>";
      }).join("");
      return '<div class="ms-field">' + label + '<select class="filter-input" id="' + id + '">' + opts + "</select></div>";
    }
    if (def.type === "str" && def.multiline) {
      return '<div class="ms-field">' + label + '<textarea class="filter-input" id="' + id + '" rows="3">' + esc(value || "") + "</textarea></div>";
    }
    if (def.type === "str") {
      return '<div class="ms-field">' + label + '<input class="filter-input" type="text" id="' + id + '" value="' + esc(value || "") + '"/></div>';
    }
    if (def.type === "int" || def.type === "float") {
      var attrs = "";
      if (def.min !== undefined && def.min !== null) attrs += ' min="' + def.min + '"';
      if (def.max !== undefined && def.max !== null) attrs += ' max="' + def.max + '"';
      if (def.type === "float") attrs += ' step="0.1"';
      return '<div class="ms-field">' + label + '<input class="filter-input" type="number"' + attrs + ' id="' + id + '" value="' + (value === null || value === undefined ? "" : value) + '"/></div>';
    }
    return "";
  }

  function msRenderJobField(group, name, def, value) {
    value = value || {};
    var enabledId = msFieldId(group, name, "enabled");
    var rows = '<div class="ms-field ms-bool-field"><input type="checkbox" id="' + enabledId + '"' + (value.enabled !== false ? " checked" : "") + '/><label for="' + enabledId + '">Enabled</label></div>';
    Object.keys(value).forEach(function (key) {
      if (key === "enabled") return;
      var fid = msFieldId(group, name, key);
      var v = value[key];
      var niceLabel = key === "cron" ? "Cron" : key.replace(/_/g, " ").replace(/\b\w/g, function (c) { return c.toUpperCase(); });
      if (key === "cron") {
        rows += '<div class="ms-field"><label for="' + fid + '">' + niceLabel + ' <span style="font-weight:400;">(5-field crontab, blank = unchanged)</span></label><input class="filter-input" type="text" id="' + fid + '" value="' + esc(v || "") + '" placeholder="*/5 * * * *"/></div>';
      } else {
        rows += '<div class="ms-field"><label for="' + fid + '">' + niceLabel + "</label><input class=\"filter-input\" type=\"number\" id=\"" + fid + '" value="' + (v === null || v === undefined ? "" : v) + '"/></div>';
      }
    });
    return '<div class="ms-job-card"><h5>' + esc(def.label || name) + '</h5><div class="ms-grid">' + rows + "</div></div>";
  }

  function msCollectField(group, name, def, oldValue) {
    var id = msFieldId(group, name);
    if (def.type === "job") {
      var out = { enabled: !!$("#" + msFieldId(group, name, "enabled")).checked };
      Object.keys(oldValue || {}).forEach(function (key) {
        if (key === "enabled") return;
        var el = $("#" + msFieldId(group, name, key));
        if (!el) return;
        if (key === "cron") {
          var raw = (el.value || "").trim();
          out.cron = raw ? raw : (oldValue[key] || null);
        } else {
          var num = parseInt(el.value, 10);
          out[key] = isNaN(num) ? oldValue[key] : num;
        }
      });
      return out;
    }
    var el = $("#" + id);
    if (!el) return oldValue;
    if (def.type === "bool") return !!el.checked;
    if (def.type === "list") return (el.value || "").split(",").map(function (s) { return s.trim(); }).filter(Boolean);
    if (def.type === "int") { var n = parseInt(el.value, 10); return isNaN(n) ? oldValue : n; }
    if (def.type === "float") { var f = parseFloat(el.value); return isNaN(f) ? oldValue : f; }
    return el.value;
  }

  function msSaveGroup(group) {
    var schema = managedSettingsState.schema[group];
    var values = managedSettingsState.values[group] || {};
    var btn = $("#ms-save-" + group);
    var statusEl = $("#ms-status-" + group);
    if (!schema || !btn) return;
    if (!btnStart(btn, "Saving...")) return;
    var payload = {};
    Object.keys(schema.fields).forEach(function (name) {
      payload[name] = msCollectField(group, name, schema.fields[name], values[name]);
    });
    fetch("/api/admin/settings/" + encodeURIComponent(group), {
      method: "POST",
      credentials: "same-origin",
      headers: { "Accept": "application/json", "Content-Type": "application/json" },
      body: JSON.stringify({ settings: payload }),
    }).then(function (r) {
      return r.json().then(function (j) { if (!r.ok || !j.success) throw new Error(j.reason || j.message || "HTTP " + r.status); return j; });
    }).then(function (j) {
      managedSettingsState.values[group] = j.settings || payload;
      if (group === "telegram_config") cc.destinations = null;
      toast("✅ " + (schema.label || group) + " settings saved", "success");
      if (statusEl) statusEl.textContent = "Saved.";
    }).catch(function (e) {
      toast("❌ Failed to save " + (schema.label || group) + ": " + e.message, "error");
      if (statusEl) statusEl.textContent = "Failed: " + e.message;
    }).finally(function () {
      btnStop(btn);
    });
  }

  // Resolve the Settings-UI category a schema field belongs to. Mirrors
  // settings_service.field_category() — a per-field override in
  // schema.field_categories wins, otherwise the group's own schema.category
  // applies. The schema (and this metadata) comes straight from the backend
  // via /api/admin/settings, so there is exactly one source of truth.
  function msFieldCategory(schema, name) {
    var overrides = schema.field_categories || {};
    if (Object.prototype.hasOwnProperty.call(overrides, name)) return overrides[name];
    return schema.category || null;
  }

  function msRenderGroup(group, schema, values, fieldNames) {
    var names = fieldNames || Object.keys(schema.fields);
    var fieldsHtml = names.map(function (name) {
      var def = schema.fields[name];
      var value = values[name];
      return def.type === "job" ? msRenderJobField(group, name, def, value) : msRenderField(group, name, def, value);
    }).join("");
    return (
      '<details class="ms-group" id="ms-group-' + group + '" open>' +
      "<summary>" + esc(schema.label || group) + "</summary>" +
      (schema.description ? '<div class="ms-desc">' + esc(schema.description) + "</div>" : "") +
      '<div class="ms-body"><div class="ms-grid">' + fieldsHtml + "</div>" +
      '<div class="ms-actions"><button class="btn primary" id="ms-save-' + group + '">Save ' + esc(schema.label || group) + '</button><span class="ms-status" id="ms-status-' + group + '"></span></div>' +
      "</div></details>"
    );
  }

  // Loads the full schema/values (single source of truth), then filters to
  // only the fields whose resolved category matches the active Settings tab
  // before rendering — the complete schema is never rendered and CSS-hidden.
  function loadManagedSettings(category) {
    statePanel("managed-settings-body", "loading", "Loading settings…");
    return api("/api/admin/settings")
      .then(function (d) {
        managedSettingsState.schema = d.schema || {};
        managedSettingsState.values = d.settings || {};
        var groups = Object.keys(managedSettingsState.schema);
        var html = "";
        var matchedGroups = [];
        groups.forEach(function (group) {
          var schema = managedSettingsState.schema[group];
          var fieldNames = Object.keys(schema.fields).filter(function (name) {
            return msFieldCategory(schema, name) === category;
          });
          if (!fieldNames.length) return;
          matchedGroups.push(group);
          html += msRenderGroup(group, schema, managedSettingsState.values[group] || {}, fieldNames);
        });
        if (!matchedGroups.length && category !== "voucher_rules") {
          $("#managed-settings-body").innerHTML = emptyState("No configurable settings are available in this category yet.");
          return;
        }
        $("#managed-settings-body").innerHTML = html;
        matchedGroups.forEach(function (group) {
          var btn = $("#ms-save-" + group);
          if (btn) btn.addEventListener("click", function () { msSaveGroup(group); });
        });
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          statePanel("managed-settings-body", "banner error", "Failed to load settings: " + e.message);
        }
      });
  }

  // Legacy read-only settings aggregate (env-derived, not part of
  // SETTINGS_SCHEMA). Sections map to the same Settings-UI categories so
  // they only ever appear once, under the matching tab.
  var LEGACY_SETTINGS_SECTION_CATEGORY = {
    voucher_settings: "voucher_rules",
    referral_settings: "referral",
    affiliate_settings: "affiliate",
    xp_checkin_settings: "xp",
    bot_settings: "integrations",
    security: "security"
  };
  var LEGACY_SETTINGS_SECTION_TITLES = {
    voucher_settings: "Voucher Settings", referral_settings: "Referral Settings",
    affiliate_settings: "Affiliate Settings", xp_checkin_settings: "XP & Check-in",
    bot_settings: "Bot Settings", security: "Security"
  };

  function renderSettingsShell(category) {
    var html = "";
    if (category === "voucher_rules") html += REJOIN_BUFFER_HTML;
    html += '<div class="section-title" style="margin-bottom:8px;">Managed Settings</div>' +
      '<div style="font-size:12px;color:var(--muted);margin-bottom:12px;">' +
      'Everything below is stored in MongoDB and editable without a redeploy. Untouched fields keep working exactly ' +
      'as before (env-var / hardcoded defaults still apply until you save an override).</div>' +
      '<div id="managed-settings-body"></div>' +
      '<div id="settings-legacy-readonly"></div>';
    $("#settings-category-body").innerHTML = html;
    if (category === "voucher_rules") bindRejoinBufferSettings();
  }

  function loadSettings(refresh) {
    var category = currentSettingsCategory;
    setMeta("Loading…");
    renderSettingsShell(category);
    loadManagedSettings(category);
    if (category === "voucher_rules") loadRejoinBufferSettings();
    api("/api/admin/dashboard/settings" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
        renderMeta(d, "all time");
        var sec = d.sections || {};
        function render(obj, indent) {
          indent = indent || 0;
          if (obj === null || typeof obj !== "object") {
            return '<span class="v">' + esc(obj === null || obj === undefined ? "—" : (typeof obj === "object" ? JSON.stringify(obj) : obj)) + "</span>";
          }
          if (obj.masked) return '<span class="pill neutral">' + (obj.configured ? "configured (masked)" : "not set") + "</span>";
          if (Array.isArray(obj)) return '<span class="v">' + esc(obj.join(", ")) + "</span>";
          return Object.keys(obj).map(function (k) {
            var val = obj[k];
            var isObj = val && typeof val === "object" && !val.masked && !Array.isArray(val);
            return '<div class="kv"><span class="k">' + esc(k) + "</span><span class=\"v\">" +
              (isObj ? "" : render(val)) + "</span></div>" + (isObj ? '<div style="margin-left:14px;">' + render(val) + "</div>" : "");
          }).join("");
        }
        var matchedKeys = Object.keys(sec).filter(function (k) {
          return LEGACY_SETTINGS_SECTION_CATEGORY[k] === category;
        });
        var el = $("#settings-legacy-readonly");
        if (!el) return;
        if (!matchedKeys.length) { el.innerHTML = ""; return; }
        el.innerHTML = '<div style="margin:20px 0 16px;"><span class="tag heuristic" style="font-size:12px;padding:4px 12px;">READ ONLY — reference only</span></div>' +
          matchedKeys.map(function (k) {
            return '<div class="settings-section"><div class="section-title">' + esc(LEGACY_SETTINGS_SECTION_TITLES[k] || k) +
              "</div><div class=\"detail-block\">" + render(sec[k]) + "</div></div>";
          }).join("");
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
        }
      });
  }

  // -------------------------------------------------------------------------
  // CAMPAIGNS VIEW
  // -------------------------------------------------------------------------

  var campaignEditorMode = "create"; // "create" | "edit"
  var campaignEditorId = null;

  function _ceTargeting() {
    var segments = [];
    $all("#ce-segments input[type=checkbox]").forEach(function (cb) {
      if (cb.checked) segments.push(cb.value);
    });
    var ageCbs = [$('#ce-age-new'), $('#ce-age-old')].filter(Boolean);
    var ageTypes = ageCbs.filter(function (cb) { return cb.checked; }).map(function (cb) { return cb.value; });
    var riskCbs = $all(".ce-risk-cb");
    var risks = riskCbs.filter(function (cb) { return cb.checked; }).map(function (cb) { return cb.value; });
    var t = {};
    if (segments.length) t.segments = segments;
    if (ageTypes.length) t.player_age_types = ageTypes;
    if (risks.length) t.claim_risk_levels = risks;
    var refMin = parseInt($("#ce-ref-min").value);
    var refMax = parseInt($("#ce-ref-max").value);
    var chkMin = parseInt($("#ce-chk-min").value);
    var chkMax = parseInt($("#ce-chk-max").value);
    var recency = parseInt($("#ce-recency").value);
    if (!isNaN(refMin)) t.referral_count_min = refMin;
    if (!isNaN(refMax)) t.referral_count_max = refMax;
    if (!isNaN(chkMin)) t.checkin_count_min = chkMin;
    if (!isNaN(chkMax)) t.checkin_count_max = chkMax;
    if (!isNaN(recency) && recency > 0) t.activity_recency_days = recency;
    return t;
  }

  function _cePopulateForm(c) {
    $("#ce-name").value = c.name || "";
    $("#ce-description").value = c.description || "";
    $("#ce-type").value = c.campaign_type || "";
    $("#ce-voucher-value").value = c.voucher_value != null ? c.voucher_value : "";
    var t = c.targeting || {};
    $all("#ce-segments input[type=checkbox]").forEach(function (cb) {
      cb.checked = (t.segments || []).indexOf(cb.value) !== -1;
    });
    [$('#ce-age-new'), $('#ce-age-old')].filter(Boolean).forEach(function (cb) {
      cb.checked = (t.player_age_types || []).indexOf(cb.value) !== -1;
    });
    $all(".ce-risk-cb").forEach(function (cb) {
      cb.checked = (t.claim_risk_levels || []).indexOf(cb.value) !== -1;
    });
    $("#ce-ref-min").value = t.referral_count_min != null ? t.referral_count_min : "";
    $("#ce-ref-max").value = t.referral_count_max != null ? t.referral_count_max : "";
    $("#ce-chk-min").value = t.checkin_count_min != null ? t.checkin_count_min : "";
    $("#ce-chk-max").value = t.checkin_count_max != null ? t.checkin_count_max : "";
    $("#ce-recency").value = t.activity_recency_days != null ? t.activity_recency_days : "";
  }

  function _ceResetForm() {
    $("#ce-name").value = "";
    $("#ce-description").value = "";
    $("#ce-type").value = "";
    $("#ce-voucher-value").value = "";
    $all("#ce-segments input[type=checkbox]").forEach(function (cb) { cb.checked = false; });
    [$('#ce-age-new'), $('#ce-age-old')].filter(Boolean).forEach(function (cb) { cb.checked = false; });
    $all(".ce-risk-cb").forEach(function (cb) { cb.checked = false; });
    ["ce-ref-min","ce-ref-max","ce-chk-min","ce-chk-max","ce-recency"].forEach(function (id) { var el = $("#" + id); if (el) el.value = ""; });
    $("#ce-preview-result").classList.add("hidden");
  }

  function _segmentLabel(s) {
    var map = {
      high_value: "High Value", normal_actual: "Normal Actual",
      active_community_player: "Active Community", low_value: "Low Value",
      voucher_hunter: "Voucher Hunter", ghost: "Ghost", unclassified: "Unclassified"
    };
    return map[s] || s;
  }

  function _campaignTypeLabel(t) {
    var map = {
      vip_campaign: "VIP Campaign", exclusive_voucher: "Exclusive Voucher",
      retention_reward: "Retention Reward", first_bet_campaign: "First-Bet Campaign",
      reload_incentive: "Reload Incentive", referral_campaign: "Referral Campaign",
      xp_campaign: "XP Campaign", community_event: "Community Event",
      turnover_improvement: "Turnover Improvement", task_based_reward: "Task-Based Reward",
      anti_abuse_control: "Anti-Abuse Control", reactivation_campaign: "Reactivation Campaign"
    };
    return map[t] || t || "—";
  }

  function renderCampaignPreview(data) {
    var audience = data.audience || {};
    var historical = data.historical || {};
    var cards = $("#ce-preview-cards");
    var segDist = $("#ce-segment-dist");
    var histEl = $("#ce-historical");

    cards.innerHTML = [
      { label: "Audience Size", value: fmt(audience.audience_size || 0) },
      { label: "Expected Cost (MYR)", value: "MYR " + fmt(audience.expected_voucher_cost || 0) },
      { label: "Snapshot Week", value: esc(audience.snapshot_week || "—") },
    ].map(function (c) {
      return '<div class="card"><div class="card-label">' + esc(c.label) + '</div><div class="card-value">' + c.value + '</div></div>';
    }).join("");

    var dist = audience.segment_distribution || {};
    var distKeys = Object.keys(dist).sort(function (a, b) { return (dist[b].count || 0) - (dist[a].count || 0); });
    if (!distKeys.length) {
      segDist.innerHTML = "<p style='color:var(--muted);font-size:12px;'>No players match these filters in the latest snapshot week.</p>";
    } else {
      segDist.innerHTML = '<table class="mini-table"><thead><tr><th>Segment</th><th class="num">Players</th><th class="num">%</th><th class="num">Avg Bet</th><th class="num">Avg Claims</th><th>Suggested Campaign Types</th></tr></thead><tbody>' +
        distKeys.map(function (seg) {
          var d = dist[seg];
          return '<tr><td>' + esc(_segmentLabel(seg)) + '</td><td class="num">' + fmt(d.count) + '</td><td class="num">' + esc(d.pct) + '%</td><td class="num">' + fmt(d.avg_bet_amount) + '</td><td class="num">' + fmt(d.avg_claim_count) + '</td><td style="font-size:11px;color:var(--muted);">' + esc((d.suggestions || []).join(", ") || "—") + '</td></tr>';
        }).join("") + '</tbody></table>';
    }

    var perf = historical.segment_performance || {};
    var perfKeys = Object.keys(perf);
    var pastCampaigns = historical.past_campaigns || [];
    if (!perfKeys.length && !pastCampaigns.length) {
      histEl.innerHTML = "<p style='color:var(--muted);font-size:12px;'>No historical snapshot data for these filters yet.</p>";
    } else {
      var perfTable = "";
      if (perfKeys.length) {
        perfTable = '<table class="mini-table" style="margin-bottom:12px;"><thead><tr><th>Segment</th><th class="num">Unique Players</th><th class="num">Avg Bet</th><th class="num">Avg Claims</th><th class="num">Avg Referrals</th><th class="num">Weeks w/ Data</th></tr></thead><tbody>' +
          perfKeys.map(function (seg) {
            var p = perf[seg];
            return '<tr><td>' + esc(_segmentLabel(seg)) + '</td><td class="num">' + fmt(p.unique_players) + '</td><td class="num">' + fmt(p.avg_bet_amount) + '</td><td class="num">' + fmt(p.avg_claim_count) + '</td><td class="num">' + fmt(p.avg_referral_count) + '</td><td class="num">' + fmt(p.weeks_with_data) + '</td></tr>';
          }).join("") + '</tbody></table>';
      }
      var pastHtml = "";
      if (pastCampaigns.length) {
        pastHtml = '<div style="font-size:12px;font-weight:600;margin-bottom:6px;">Past Campaigns (same segments)</div>' +
          pastCampaigns.map(function (pc) {
            return '<div style="font-size:12px;padding:4px 0;border-bottom:1px solid var(--border);">' +
              '<span class="pill ' + esc(pc.status) + '">' + esc(pc.status) + '</span> ' +
              '<strong>' + esc(pc.name) + '</strong> · ' + esc(_campaignTypeLabel(pc.campaign_type)) +
              ' <span style="color:var(--muted);float:right;">' + esc(pc.created_at ? pc.created_at.slice(0,10) : "") + '</span></div>';
          }).join("");
      }
      histEl.innerHTML = perfTable + pastHtml;
    }

    if (audience.warning) {
      histEl.innerHTML = '<p style="color:var(--warn);font-size:12px;">' + esc(audience.warning) + '</p>' + histEl.innerHTML;
    }

    $("#ce-preview-result").classList.remove("hidden");
  }

  function loadCampaigns(force) {
    var statusFilter = "";
    var activeBtn = $("#campaigns-status-filter .active");
    if (activeBtn) statusFilter = activeBtn.dataset.status || "";
    var url = "/api/admin/campaigns" + (statusFilter ? "?status=" + encodeURIComponent(statusFilter) : "");
    fetch(url, { credentials: "same-origin" }).then(function (r) { return r.json(); }).then(function (data) {
      var body = $("#campaigns-list-body");
      var items = data.campaigns || [];
      if (!items.length) {
        body.innerHTML = emptyState({
          icon: "📋", title: "No campaigns yet", sub: "Create your first campaign to start targeting players.",
          ctaHtml: '<button class="btn primary" onclick="document.getElementById(\'campaigns-new-btn\').click()">+ Create Campaign</button>',
        });
        return;
      }
      body.innerHTML = items.map(function (c) {
        var segs = ((c.targeting || {}).segments || []).map(_segmentLabel).join(", ") || "All segments";
        return '<div class="campaign-card">' +
          '<div class="campaign-card-header">' +
          '<div class="campaign-card-title">' + esc(c.name) + '</div>' +
          '<span class="pill ' + esc(c.status) + '">' + esc(c.status) + '</span>' +
          '</div>' +
          '<div class="campaign-card-meta">' +
          '<span>' + esc(_campaignTypeLabel(c.campaign_type)) + '</span>' +
          '<span>Targets: ' + esc(segs) + '</span>' +
          (c.voucher_value ? '<span>MYR ' + fmt(c.voucher_value) + ' / voucher</span>' : '') +
          '<span style="margin-left:auto;">' + esc(c.created_at ? c.created_at.slice(0,10) : "") + '</span>' +
          '</div>' +
          (c.description ? '<div style="font-size:12px;color:var(--muted);margin-top:6px;">' + esc(c.description) + '</div>' : '') +
          '<div class="campaign-card-actions">' +
          '<button class="btn" onclick="editCampaign(' + JSON.stringify(c.id) + ')">Edit</button>' +
          '<button class="btn danger" onclick="archiveCampaign(' + JSON.stringify(c.id) + ', ' + JSON.stringify(c.name) + ')">Archive</button>' +
          '</div></div>';
      }).join("");
    }).catch(function (e) {
      banner("Failed to load campaigns: " + e.message, "error");
    });
  }

  window.editCampaign = function (id) {
    fetch("/api/admin/campaigns/" + id, { credentials: "same-origin" })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (data.status !== "ok") { banner("Failed to load campaign", "error"); return; }
        campaignEditorMode = "edit";
        campaignEditorId = id;
        $("#campaigns-editor-title").textContent = "Edit Campaign";
        _cePopulateForm(data.campaign);
        if (data.audience && data.historical) renderCampaignPreview(data);
        $("#campaigns-list-panel").classList.add("hidden");
        $("#campaigns-editor-panel").classList.remove("hidden");
      });
  };

  window.archiveCampaign = function (id, name) {
    if (!confirm("Archive campaign \"" + name + "\"? It will be hidden from the list.")) return;
    fetch("/api/admin/campaigns/" + id, { method: "DELETE", credentials: "same-origin" })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (d.status === "ok") { banner("Campaign archived.", "ok"); loadCampaigns(true); }
        else banner("Error: " + (d.code || "unknown"), "error");
      });
  };

  function bindCampaigns() {
    var newBtn = $("#campaigns-new-btn");
    if (newBtn) newBtn.addEventListener("click", function () {
      campaignEditorMode = "create";
      campaignEditorId = null;
      $("#campaigns-editor-title").textContent = "New Campaign";
      _ceResetForm();
      $("#campaigns-list-panel").classList.add("hidden");
      $("#campaigns-editor-panel").classList.remove("hidden");
    });

    var cancelBtn = $("#ce-cancel-btn");
    if (cancelBtn) cancelBtn.addEventListener("click", function () {
      $("#campaigns-editor-panel").classList.add("hidden");
      $("#campaigns-list-panel").classList.remove("hidden");
    });

    var previewBtn = $("#ce-preview-btn");
    if (previewBtn) previewBtn.addEventListener("click", function () {
      previewBtn.disabled = true;
      previewBtn.textContent = "Loading…";
      var body = {
        targeting: _ceTargeting(),
        voucher_value: parseFloat($("#ce-voucher-value").value) || 0,
      };
      fetch("/api/admin/campaigns/preview", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }).then(function (r) { return r.json(); })
        .then(function (data) {
          if (data.status !== "ok") { banner("Preview error: " + (data.code || "unknown"), "error"); return; }
          renderCampaignPreview(data);
        })
        .catch(function (e) { banner("Preview failed: " + e.message, "error"); })
        .finally(function () { previewBtn.disabled = false; previewBtn.textContent = "Preview Audience"; });
    });

    var saveBtn = $("#ce-save-btn");
    if (saveBtn) saveBtn.addEventListener("click", function () {
      var name = ($("#ce-name").value || "").trim();
      if (!name) { banner("Campaign name is required.", "error"); return; }
      saveBtn.disabled = true;
      saveBtn.textContent = "Saving…";
      var body = {
        name: name,
        description: ($("#ce-description").value || "").trim(),
        campaign_type: $("#ce-type").value || "",
        targeting: _ceTargeting(),
        voucher_value: parseFloat($("#ce-voucher-value").value) || 0,
      };
      var url = campaignEditorMode === "edit"
        ? "/api/admin/campaigns/" + campaignEditorId
        : "/api/admin/campaigns";
      var method = campaignEditorMode === "edit" ? "PUT" : "POST";
      fetch(url, {
        method: method,
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }).then(function (r) { return r.json(); })
        .then(function (d) {
          if (d.status === "ok") {
            banner("Campaign saved.", "ok");
            $("#campaigns-editor-panel").classList.add("hidden");
            $("#campaigns-list-panel").classList.remove("hidden");
            loadCampaigns(true);
          } else {
            banner("Save error: " + (d.code || "unknown"), "error");
          }
        })
        .catch(function (e) { banner("Save failed: " + e.message, "error"); })
        .finally(function () { saveBtn.disabled = false; saveBtn.textContent = "Save Campaign"; });
    });

    $all("#campaigns-status-filter button").forEach(function (b) {
      b.addEventListener("click", function () {
        $all("#campaigns-status-filter button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadCampaigns(true);
      });
    });
  }

  // ---------- Campaign Builder (P2) ----------
  // Authoring layer that compiles into the existing Voucher Drop engine via
  // /api/admin/campaign-builder/*. Never calls claim/eligibility/scheduler
  // endpoints directly.
  var cb = {
    meta: null,
    campaignId: null,
    campaign: null,
    step: 1,
  };

  var CB_TYPE_LABELS = {
    smart_default: "Smart Default", public: "Public", welcome: "Welcome",
    segment: "Segment", affiliate: "Affiliate", personalised: "Personalised",
    fcfs: "FCFS", surprise: "Surprise", test: "Test",
  };
  var CB_AUDIENCE_LABELS = {
    smart_segment_pct: "Smart Segment %", equal_chance: "Equal Chance",
    no_segment_filter: "No Segment Filter", whitelist: "Whitelist",
    vip: "VIP", region: "Region", admin_only: "Admin Only",
  };
  var CB_SEGMENT_LABELS = {
    high_value: "High Value", normal_actual: "Normal Actual",
    active_community_player: "Active Community", low_value: "Low Value",
    voucher_hunter: "Voucher Hunter", ghost: "Ghost", unclassified: "Unclassified",
  };

  function cbApi(path, opts) {
    opts = opts || {};
    var fetchOpts = { credentials: "same-origin", headers: { "Accept": "application/json" } };
    if (opts.method) fetchOpts.method = opts.method;
    if (opts.body !== undefined) {
      fetchOpts.headers["Content-Type"] = "application/json";
      fetchOpts.body = JSON.stringify(opts.body);
    }
    return fetch(path, fetchOpts).then(function (r) {
      if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
      return r.json().then(function (j) { return { ok: r.ok, status: r.status, body: j }; });
    });
  }

  function ensureCbMeta() {
    if (cb.meta) return Promise.resolve(cb.meta);
    return cbApi("/api/admin/campaign-builder/meta").then(function (res) {
      if (res.ok) cb.meta = res.body;
      return cb.meta;
    });
  }

  function loadCampaignBuilder(force) {
    if (force) { cb.campaignId = null; cb.campaign = null; }
    $("#cb-wizard-panel").classList.add("hidden");
    $("#cb-list-panel").classList.remove("hidden");
    ensureCbMeta().then(function () {
      return cbApi("/api/admin/campaign-builder/campaigns?status=draft");
    }).then(function (res) {
      var body = $("#cb-draft-list-body");
      var items = (res.body && res.body.campaigns) || [];
      if (!items.length) {
        body.innerHTML = emptyState({
          icon: "📝", title: "No drafts in progress", sub: "Start the campaign wizard to create one.",
          ctaHtml: '<button class="btn primary" onclick="document.getElementById(\'cb-new-btn\').click()">+ New Campaign</button>',
        });
        return;
      }
      body.innerHTML = items.map(function (c) {
        return '<div class="campaign-card">' +
          '<div class="campaign-card-header">' +
          '<div class="campaign-card-title">' + esc(c.campaign_name) + '</div>' +
          '<span class="pill draft">draft</span>' +
          '</div>' +
          '<div class="campaign-card-meta">' +
          '<span>' + esc(CB_TYPE_LABELS[c.campaign_type] || c.campaign_type) + '</span>' +
          '<span>' + esc(CB_AUDIENCE_LABELS[c.audience_mode] || c.audience_mode) + '</span>' +
          '<span style="margin-left:auto;">' + esc((c.created_at || "").slice(0, 10)) + '</span>' +
          '</div>' +
          '<div class="campaign-card-actions">' +
          '<button class="btn" onclick="cbResume(' + JSON.stringify(c.id) + ')">Resume</button>' +
          '</div></div>';
      }).join("");
    }).catch(function (e) { banner("Failed to load campaign builder: " + e.message, "error"); });
  }

  function loadActiveCampaigns(force) {
    cbApi("/api/admin/campaign-builder/campaigns?status=active").then(function (res) {
      _cbRenderCampaignList("ac-list-body", (res.body && res.body.campaigns) || [], "active");
    });
  }

  function loadDraftCampaigns(force) {
    cbApi("/api/admin/campaign-builder/campaigns?status=draft").then(function (res) {
      _cbRenderCampaignList("dc-list-body", (res.body && res.body.campaigns) || [], "draft");
    });
  }

  function _cbRenderCampaignList(elId, items, status) {
    var body = $("#" + elId);
    if (!items.length) {
      body.innerHTML = status === "active"
        ? emptyState({
            icon: "🚀", title: "No active campaigns", sub: "Create your first campaign to start reaching players.",
            ctaHtml: '<button class="btn primary" onclick="goToViewAndClick(\'campaignBuilder\',\'cb-new-btn\')">+ Create Campaign</button>',
          })
        : emptyState("No " + status + " campaigns.");
      return;
    }
    body.innerHTML = items.map(function (c) {
      var drops = c.compiled_drop_ids || [];
      var isBatch = !!c.release_type;
      return '<div class="campaign-card">' +
        '<div class="campaign-card-header">' +
        '<div class="campaign-card-title">' + esc(c.campaign_name) + '</div>' +
        '<span class="pill ' + esc(status) + '">' + esc(status) + '</span>' +
        (isBatch ? '<span class="pill">batch: ' + esc(c.batch_status || "") + '</span>' : '') +
        '</div>' +
        '<div class="campaign-card-meta">' +
        '<span>' + esc(CB_TYPE_LABELS[c.campaign_type] || c.campaign_type) + '</span>' +
        '<span>' + esc(CB_AUDIENCE_LABELS[c.audience_mode] || c.audience_mode) + '</span>' +
        (drops.length ? '<span>' + drops.length + ' drop(s)</span>' : '') +
        (isBatch ? '<span>' + (c.released_batches || 0) + '/' + (c.batch_count || "?") + ' batches released</span>' : '') +
        '<span style="margin-left:auto;">' + esc((c.created_at || "").slice(0, 10)) + '</span>' +
        '</div>' +
        (isBatch && c.batch_count
          ? '<div class="bar-wrap" style="margin-top:8px;width:100%;"><div class="bar" style="width:' +
            Math.round(100 * Math.min(1, (c.released_batches || 0) / c.batch_count)) + '%;"></div></div>'
          : '') +
        (status === "draft"
          ? '<div class="campaign-card-actions"><button class="btn" onclick="cbResume(' + JSON.stringify(c.id) + ')">Resume</button>' +
            '<button class="btn danger" onclick="cbDelete(' + JSON.stringify(c.id) + ')">Delete</button></div>'
          : '') +
        (isBatch && status === "active"
          ? '<div class="campaign-card-actions">' +
            (c.batch_status === "scheduled" || c.batch_status === "active"
              ? '<button class="btn danger" onclick="cbPauseBatch(this,' + JSON.stringify(c.id) + ')">Pause</button>' +
                '<button class="btn primary" onclick="cbReleaseNextBatch(this,' + JSON.stringify(c.id) + ')">Release Next Now</button>'
              : '') +
            (c.batch_status === "paused" ? '<button class="btn primary" onclick="cbResumeBatch(this,' + JSON.stringify(c.id) + ')">Resume</button>' : '') +
            (["scheduled", "active", "paused"].indexOf(c.batch_status) >= 0
              ? '<button class="btn danger" onclick="cbCancelBatch(this,' + JSON.stringify(c.id) + ')">Cancel</button>'
              : '') +
            '<button class="btn" style="background:transparent;border:1px solid var(--border);" onclick="cbToggleBatchAnalytics(this,' + JSON.stringify(c.id) + ')">Analytics</button>' +
            '</div><div id="cb-analytics-' + esc(c.id) + '" class="hidden" style="margin-top:10px;"></div>'
          : '') +
        '</div>';
    }).join("");
  }

  function cbBtnStart(btn, loadingText) {
    if (!btn || btn.dataset.loading === "1") return false;
    btn.dataset.loading = "1";
    btn.dataset.originalText = btn.textContent;
    btn.disabled = true;
    btn.classList.add("btn-loading");
    btn.innerHTML = '<span class="btn-spinner"></span>' + esc(loadingText);
    return true;
  }

  function cbBtnStop(btn) {
    if (!btn) return;
    btn.dataset.loading = "";
    btn.disabled = false;
    btn.classList.remove("btn-loading");
    btn.textContent = btn.dataset.originalText || btn.textContent;
  }

  function cbErrMsg(res, e) {
    if (e) return e.message;
    return (res.body && (res.body.message || res.body.code)) || "unknown error";
  }

  window.cbPauseBatch = function (btn, id) {
    if (!cbBtnStart(btn, "⏳ Pausing...")) return;
    banner("Pausing campaign...", "ok");
    cbApi("/api/admin/campaign-builder/campaigns/" + id + "/pause", { method: "POST", body: {} }).then(function (res) {
      if (res.ok) { banner("✅ Campaign paused", "ok"); refreshCurrent(true); }
      else { cbBtnStop(btn); banner("❌ Failed to pause campaign: " + cbErrMsg(res), "error"); }
    }).catch(function (e) { cbBtnStop(btn); banner("❌ Failed to pause campaign: " + cbErrMsg(null, e), "error"); });
  };

  window.cbResumeBatch = function (btn, id) {
    if (!cbBtnStart(btn, "⏳ Resuming...")) return;
    cbApi("/api/admin/campaign-builder/campaigns/" + id + "/resume", { method: "POST", body: {} }).then(function (res) {
      if (res.ok) { banner("✅ Campaign resumed", "ok"); refreshCurrent(true); }
      else { cbBtnStop(btn); banner("❌ Failed to resume campaign: " + cbErrMsg(res), "error"); }
    }).catch(function (e) { cbBtnStop(btn); banner("❌ Failed to resume campaign: " + cbErrMsg(null, e), "error"); });
  };

  window.cbCancelBatch = function (btn, id) {
    if (!confirm("Cancel all unreleased batches? Already-released/claimed drops are not affected.")) return;
    if (!cbBtnStart(btn, "⏳ Cancelling...")) return;
    cbApi("/api/admin/campaign-builder/campaigns/" + id + "/cancel", { method: "POST", body: {} }).then(function (res) {
      if (res.ok) { banner("✅ Campaign cancelled", "ok"); refreshCurrent(true); }
      else { cbBtnStop(btn); banner("❌ Failed to cancel campaign: " + cbErrMsg(res), "error"); }
    }).catch(function (e) { cbBtnStop(btn); banner("❌ Failed to cancel campaign: " + cbErrMsg(null, e), "error"); });
  };

  window.cbReleaseNextBatch = function (btn, id) {
    if (!cbBtnStart(btn, "⏳ Releasing...")) return;
    banner("Releasing next batch...", "ok");
    cbApi("/api/admin/campaign-builder/campaigns/" + id + "/release-next", { method: "POST", body: {} }).then(function (res) {
      if (!res.ok) { cbBtnStop(btn); banner("❌ Failed to release batch: " + cbErrMsg(res), "error"); return; }
      if (!res.body.released_drop_id) { cbBtnStop(btn); banner("No unreleased batches left.", "ok"); return; }
      banner("✅ Batch released", "ok");
      refreshCurrent(true);
    }).catch(function (e) { cbBtnStop(btn); banner("❌ Failed to release batch: " + cbErrMsg(null, e), "error"); });
  };

  window.cbToggleBatchAnalytics = function (btn, id) {
    var el = $("#cb-analytics-" + id);
    if (!el) return;
    if (!el.classList.contains("hidden")) { el.classList.add("hidden"); return; }
    if (!cbBtnStart(btn, "⏳ Loading...")) return;
    el.classList.remove("hidden");
    el.innerHTML = '<div style="font-size:12px;color:var(--muted);">Loading analytics…</div>';
    cbApi("/api/admin/campaign-builder/campaigns/" + id + "/analytics").then(function (res) {
      cbBtnStop(btn);
      if (!res.ok) { el.innerHTML = '<div class="banner error">Failed to load analytics.</div>'; banner("❌ Failed to load analytics: " + cbErrMsg(res), "error"); return; }
      var a = res.body.analytics;
      el.innerHTML =
        '<div class="card-grid">' +
        kpiCard("Total Vouchers", a.total_vouchers) +
        kpiCard("Released", a.released_vouchers) +
        kpiCard("Claimed", a.claimed_vouchers) +
        kpiCard("Remaining", a.remaining_vouchers) +
        kpiCard("Completion %", a.completion_pct) +
        '</div>' +
        '<div style="margin-top:8px;font-size:12px;">Next release: ' + esc(a.next_release_at || "—") + ' &nbsp; Batches: ' + a.released_batches + '/' + a.total_batches + '</div>' +
        '<table class="data-table" style="margin-top:8px;"><thead><tr><th>#</th><th>Drop</th><th>Release Time</th><th>Status</th><th>Codes</th><th>Claimed</th><th>Remaining</th></tr></thead><tbody>' +
        (a.child_drops || []).map(function (d) {
          return '<tr><td>' + d.batch_index + '</td><td>' + esc(d.drop_id) + '</td><td>' + esc(d.release_time || "—") + '</td><td>' + esc(d.status) +
            '</td><td>' + fmt(d.total_codes) + '</td><td>' + fmt(d.claimed) + '</td><td>' + fmt(d.remaining) + '</td></tr>';
        }).join("") + '</tbody></table>';
    }).catch(function (e) { cbBtnStop(btn); el.innerHTML = '<div class="banner error">Failed to load analytics.</div>'; banner("❌ Failed to load analytics: " + cbErrMsg(null, e), "error"); });
  };

  function loadCompiledDrops(force) {
    fetch("/v2/miniapp/admin/drops", { credentials: "same-origin" }).then(function (r) { return r.json(); })
      .then(function (data) {
        var items = (data.items || []).filter(function (d) { return !!d.campaign_id; });
        var body = $("#cd-list-body");
        if (!items.length) {
          body.innerHTML = '<p style="color:var(--muted);font-size:13px;padding:16px 0;">No compiled drops yet. Launch a campaign in Campaign Builder to generate one.</p>';
          return;
        }
        body.innerHTML = '<table class="data-table"><thead><tr><th>Drop</th><th>Campaign</th><th>Status</th><th>Type</th><th>Codes</th></tr></thead><tbody>' +
          items.map(function (d) {
            return '<tr><td>' + esc(d.name) + '</td><td>' + esc(d.campaign_name || "") + '</td><td>' + esc(d.status) + '</td><td>' + esc(d.type) +
              '</td><td>' + (d.type === "personalised" ? (fmt(d.claimed) + "/" + fmt(d.assigned)) : (fmt(d.codesFree) + "/" + fmt(d.codesTotal))) + '</td></tr>';
          }).join("") + '</tbody></table>';
      }).catch(function (e) { banner("Failed to load compiled drops: " + e.message, "error"); });
  }

  // ---------- Campaign Performance (P4) ----------
  // Read-only analytics over campaign_builder_campaigns / drops / vouchers /
  // voucher_claims via /api/admin/campaign-builder/performance/*. Never
  // calls a mutating campaign-builder or drop-manager endpoint.
  var cp = { selected: {} };

  var CP_BADGE_CLASS = {
    "High Quality": "pill active", "Good": "pill active",
    "Neutral": "pill draft", "Risky": "pill", "Bad": "pill",
  };

  function cpApi(path) {
    return fetch(path, { credentials: "same-origin", headers: { "Accept": "application/json" } }).then(function (r) {
      if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
      return r.json().then(function (j) { return { ok: r.ok, status: r.status, body: j }; });
    });
  }

  function loadCampaignPerformance(force) {
    if (force) { cp.selected = {}; }
    $("#cp-detail-panel").classList.add("hidden");
    $("#cp-compare-panel").classList.add("hidden");
    var status = $("#cp-status").value || "active";
    var windowVal = $("#cp-window").value || "all";
    var sort = $("#cp-sort").value || "created_at";
    var body = $("#cp-list-body");
    body.innerHTML = '<div style="font-size:12px;color:var(--muted);">Loading…</div>';
    cpApi("/api/admin/campaign-builder/performance?status=" + status + "&window=" + windowVal + "&sort=" + sort)
      .then(function (res) {
        if (!res.ok) { body.innerHTML = '<div class="banner error">Failed to load campaign performance.</div>'; return; }
        var items = (res.body && res.body.campaigns) || [];
        if (!items.length) {
          body.innerHTML = '<p style="color:var(--muted);font-size:13px;padding:16px 0;">No campaigns match this filter.</p>';
          return;
        }
        body.innerHTML = '<table class="data-table"><thead><tr>' +
          '<th></th><th>Campaign</th><th>Status</th><th>Type</th><th>Total</th><th>Released</th><th>Claimed</th>' +
          '<th>Claim Rate</th><th>Voucher Hunter %</th><th>Actual Player %</th><th>Score</th><th>Actions</th>' +
          '</tr></thead><tbody>' +
          items.map(function (c) {
            var badgeCls = CP_BADGE_CLASS[c.badge] || "pill";
            return '<tr>' +
              '<td><input type="checkbox" class="cp-compare-check" data-id="' + esc(c.campaign_id) + '" ' + (cp.selected[c.campaign_id] ? "checked" : "") + ' /></td>' +
              '<td>' + esc(c.campaign_name) + (c.is_batch ? ' <span class="pill">batch</span>' : '') + '</td>' +
              '<td>' + esc(c.status) + '</td>' +
              '<td>' + esc(c.campaign_type) + '</td>' +
              '<td>' + fmt(c.total_vouchers) + '</td>' +
              '<td>' + fmt(c.total_released) + '</td>' +
              '<td>' + fmt(c.total_claimed) + '</td>' +
              '<td>' + (c.claim_rate == null ? "—" : c.claim_rate + "%") + '</td>' +
              '<td>' + (c.voucher_hunter_claim_share_pct == null ? "—" : c.voucher_hunter_claim_share_pct + "%") + '</td>' +
              '<td>' + (c.actual_player_claim_share_pct == null ? "—" : c.actual_player_claim_share_pct + "%") + '</td>' +
              '<td>' + c.campaign_score + ' <span class="' + badgeCls + '">' + esc(c.badge) + '</span></td>' +
              '<td><button class="btn" onclick="cpViewDetails(' + JSON.stringify(c.campaign_id) + ')">View Details</button></td>' +
              '</tr>';
          }).join("") + '</tbody></table>';
        $all(".cp-compare-check").forEach(function (cb) {
          cb.addEventListener("change", function () {
            if (cb.checked) cp.selected[cb.dataset.id] = true; else delete cp.selected[cb.dataset.id];
          });
        });
      }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load campaign performance.</div>'; banner("❌ " + e.message, "error"); });
  }

  function cpSegmentRows(quality) {
    var labels = { high_value: "High Value", normal_actual: "Normal Actual", low_value: "Low Value", voucher_hunter: "Voucher Hunter", ghost: "Ghost", unknown: "Unknown" };
    return Object.keys(labels).map(function (k) {
      return '<tr><td>' + labels[k] + '</td><td>' + fmt(quality[k] || 0) + '</td></tr>';
    }).join("") + (quality.unknown_reason ? '<tr><td colspan="2" style="font-size:11px;color:var(--muted);">unknown reason: ' + esc(quality.unknown_reason) + '</td></tr>' : "");
  }

  function cpNullable(obj) {
    if (obj && typeof obj === "object" && "reason" in obj) return "Data Not Available (" + esc(obj.reason) + ")";
    return fmt(obj);
  }

  function cpRenderDetail(p) {
    var el = $("#cp-detail-panel");
    el.classList.remove("hidden");
    var v = p.volume, s = p.speed, a = p.abuse_risk, conv = p.conversion_proxy;
    el.innerHTML =
      '<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">' +
      '<h3 style="margin:0;">' + esc(p.campaign_name) + '</h3>' +
      '<span class="' + (CP_BADGE_CLASS[p.badge] || "pill") + '">' + esc(p.badge) + ' (score ' + p.campaign_score + ')</span>' +
      '<button class="btn" style="margin-left:auto;background:transparent;border:1px solid var(--border);" onclick="$(\'#cp-detail-panel\').classList.add(\'hidden\')">Close</button>' +
      '</div>' +
      '<div class="card-grid">' +
      kpiCard("Total Vouchers", v.total_vouchers) + kpiCard("Released", v.total_released) +
      kpiCard("Claimed", v.total_claimed) + kpiCard("Remaining", v.total_remaining) +
      kpiCard("Claim Rate", v.claim_rate == null ? null : v.claim_rate + "%", null, v.claim_rate == null) +
      kpiCard("Release Completion", v.release_completion_pct == null ? null : v.release_completion_pct + "%", null, v.release_completion_pct == null) +
      '</div>' +
      '<div class="section-title" style="margin-top:16px;font-size:13px;">Claim Speed</div>' +
      '<div class="card-grid">' +
      kpiCard("Time to First Claim (min)", s.time_to_first_claim_minutes, null, s.time_to_first_claim_minutes == null) +
      kpiCard("Time to 50% Claimed (min)", s.time_to_50pct_claimed_minutes, null, s.time_to_50pct_claimed_minutes == null) +
      kpiCard("Time to Sold Out (min)", s.time_to_sold_out_minutes, null, s.time_to_sold_out_minutes == null) +
      kpiCard("Avg Claim Speed / Batch (min)", s.average_claim_speed_minutes, null, s.average_claim_speed_minutes == null) +
      '</div>' +
      '<div class="section-title" style="margin-top:16px;font-size:13px;">Segment Breakdown (Quality)</div>' +
      '<table class="data-table"><tbody>' + cpSegmentRows(p.quality) + '</tbody></table>' +
      '<div class="section-title" style="margin-top:16px;font-size:13px;">Abuse Risk</div>' +
      '<table class="data-table"><tbody>' +
      '<tr><td>Repeat Claimers</td><td>' + fmt(a.repeat_claimers) + '</td></tr>' +
      '<tr><td>Same IP/Subnet Claims</td><td>' + fmt(a.same_ip_subnet_claims) + ' (' + fmt(a.same_ip_subnet_clusters) + ' clusters)</td></tr>' +
      '<tr><td>Claim Cooldown Hits</td><td>' + cpNullable(a.claim_cooldown_hits) + '</td></tr>' +
      '<tr><td>Voucher Hunter Claim Share</td><td>' + (a.voucher_hunter_claim_share_pct == null ? "—" : a.voucher_hunter_claim_share_pct + "%") + '</td></tr>' +
      '<tr><td>Suspicious Claims</td><td>' + fmt(a.suspicious_claims) + (a.suspicious_claim_pct == null ? "" : " (" + a.suspicious_claim_pct + "%)") + '</td></tr>' +
      '</tbody></table>' +
      '<div class="section-title" style="margin-top:16px;font-size:13px;">Conversion Proxy</div>' +
      '<table class="data-table"><tbody>' +
      '<tr><td>Qualified After Claim</td><td>' + cpNullable(conv.qualified_after_claim) + '</td></tr>' +
      '<tr><td>Referred After Claim</td><td>' + cpNullable(conv.referral_after_claim) + '</td></tr>' +
      '<tr><td>Checked In After Claim</td><td>' + cpNullable(conv.checkin_after_claim) + '</td></tr>' +
      '<tr><td>After Bet / Withdrawal</td><td>' + cpNullable(conv.after_bet_or_withdrawal) + '</td></tr>' +
      '</tbody></table>' +
      '<div class="section-title" style="margin-top:16px;font-size:13px;">Score Explanation</div>' +
      '<table class="data-table"><tbody>' +
      '<tr><td>Quality Score</td><td>' + fmt(p.score_breakdown.quality_score) + '</td></tr>' +
      '<tr><td>Abuse Penalty</td><td>-' + fmt(p.score_breakdown.abuse_penalty) + '</td></tr>' +
      '<tr><td>Conversion Bonus</td><td>+' + fmt(p.score_breakdown.conversion_bonus) + '</td></tr>' +
      '<tr><td><strong>Campaign Score</strong></td><td><strong>' + fmt(p.campaign_score) + '</strong></td></tr>' +
      '</tbody></table>' +
      (p.child_drops && p.child_drops.length > 1
        ? '<div class="section-title" style="margin-top:16px;font-size:13px;">Batch / Drop Breakdown</div>' +
          '<table class="data-table"><thead><tr><th>#</th><th>Drop</th><th>Release Time</th><th>Total</th><th>Claimed</th><th>Remaining</th><th>Claim Rate</th><th>Voucher Hunter %</th></tr></thead><tbody>' +
          p.child_drops.map(function (d) {
            return '<tr><td>' + fmt(d.batch_index) + '</td><td>' + esc(d.drop_name) + '</td><td>' + esc(d.release_time || "—") +
              '</td><td>' + fmt(d.total_codes) + '</td><td>' + fmt(d.claimed) + '</td><td>' + fmt(d.remaining) +
              '</td><td>' + (d.claim_rate == null ? "—" : d.claim_rate + "%") + '</td><td>' + (d.voucher_hunter_share == null ? "—" : d.voucher_hunter_share + "%") + '</td></tr>';
          }).join("") + '</tbody></table>'
        : '');
  }

  window.cpViewDetails = function (id) {
    var windowVal = $("#cp-window").value || "all";
    var el = $("#cp-detail-panel");
    el.classList.remove("hidden");
    el.innerHTML = '<div style="font-size:12px;color:var(--muted);">Loading details…</div>';
    cpApi("/api/admin/campaign-builder/performance/" + id + "?window=" + windowVal).then(function (res) {
      if (!res.ok) { el.innerHTML = '<div class="banner error">Failed to load campaign details.</div>'; return; }
      cpRenderDetail(res.body.performance);
    }).catch(function (e) { el.innerHTML = '<div class="banner error">Failed to load campaign details.</div>'; banner("❌ " + e.message, "error"); });
  };

  (function bindCampaignPerformanceControls() {
    ["cp-status", "cp-window", "cp-sort"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.addEventListener("change", function () { loadCampaignPerformance(false); });
    });
    var refreshBtn = document.getElementById("cp-refresh-btn");
    if (refreshBtn) refreshBtn.addEventListener("click", function () { loadCampaignPerformance(true); });
    var compareBtn = document.getElementById("cp-compare-btn");
    if (compareBtn) compareBtn.addEventListener("click", function () {
      var ids = Object.keys(cp.selected);
      var panel = $("#cp-compare-panel");
      if (ids.length < 2) { banner("Select at least 2 campaigns to compare.", "error"); return; }
      panel.classList.remove("hidden");
      panel.innerHTML = '<div style="font-size:12px;color:var(--muted);">Loading comparison…</div>';
      var windowVal = $("#cp-window").value || "all";
      cpApi("/api/admin/campaign-builder/performance/compare?campaign_ids=" + ids.join(",") + "&window=" + windowVal).then(function (res) {
        if (!res.ok) { panel.innerHTML = '<div class="banner error">Failed to load comparison.</div>'; return; }
        var rows = res.body.campaigns || [];
        panel.innerHTML = '<div class="section-title" style="font-size:13px;margin-bottom:8px;">Comparison</div>' +
          '<table class="data-table"><thead><tr><th>Metric</th>' + rows.map(function (r) { return '<th>' + esc(r.campaign_name) + '</th>'; }).join("") + '</tr></thead><tbody>' +
          [
            ["Status", function (r) { return esc(r.status); }],
            ["Total Vouchers", function (r) { return fmt(r.volume.total_vouchers); }],
            ["Released", function (r) { return fmt(r.volume.total_released); }],
            ["Claimed", function (r) { return fmt(r.volume.total_claimed); }],
            ["Claim Rate", function (r) { return r.volume.claim_rate == null ? "—" : r.volume.claim_rate + "%"; }],
            ["Voucher Hunter Share", function (r) { return r.abuse_risk.voucher_hunter_claim_share_pct == null ? "—" : r.abuse_risk.voucher_hunter_claim_share_pct + "%"; }],
            ["Suspicious Claims", function (r) { return fmt(r.abuse_risk.suspicious_claims); }],
            ["Score", function (r) { return fmt(r.campaign_score) + " (" + esc(r.badge) + ")"; }],
          ].map(function (row) {
            return '<tr><td>' + row[0] + '</td>' + rows.map(function (r) { return '<td>' + row[1](r) + '</td>'; }).join("") + '</tr>';
          }).join("") + '</tbody></table>';
      }).catch(function (e) { panel.innerHTML = '<div class="banner error">Failed to load comparison.</div>'; banner("❌ " + e.message, "error"); });
    });
  })();

  // ---------- Campaign Intelligence (P5) ----------
  // Read-only recommendations layer over /api/admin/campaign-builder/intelligence/*.
  // Never calls a mutating endpoint; nothing here schedules or launches a campaign.
  var ci = { tab: "rankings", campaignId: null };

  function ciBadge(type) {
    return type === "good" ? '<span class="pill active">✓ ' : '<span class="pill">⚠ ';
  }

  function ciInsightList(insights) {
    if (!insights || !insights.length) return '<span style="color:var(--muted);font-size:12px;">No notable signals yet.</span>';
    return insights.map(function (i) { return ciBadge(i.type) + esc(i.text) + '</span>'; }).join(" ");
  }

  function loadCampaignIntelligence(force) {
    if (force) { ci.campaignId = null; }
    var body = $("#ci-tab-body");
    body.innerHTML = '<div style="font-size:12px;color:var(--muted);">Loading…</div>';
    if (ci.tab === "rankings") return ciLoadRankings(body);
    if (ci.tab === "insights") return ciLoadInsights(body);
    if (ci.tab === "recommendations") return ciLoadRecommendations(body);
    if (ci.tab === "segments") return ciLoadSegments(body);
    if (ci.tab === "templates") return ciLoadTemplates(body);
    if (ci.tab === "releases") return ciLoadReleases(body);
    if (ci.tab === "bestTime") return ciLoadBestTime(body);
    if (ci.tab === "playbook") return ciLoadPlaybook(body);
  }

  function ciLoadRankings(body) {
    api("/api/admin/campaign-builder/intelligence/rankings").then(function (res) {
      var rows = res.rankings || [];
      if (!rows.length) { body.innerHTML = '<p style="color:var(--muted);font-size:13px;">No campaigns to rank yet.</p>'; return; }
      body.innerHTML = '<table class="data-table"><thead><tr>' +
        '<th>Rank</th><th>Campaign</th><th>Type</th><th>Score</th><th>Claim Rate</th><th>Actual %</th><th>VH %</th><th>Conv %</th><th>Avg Speed (min)</th><th></th>' +
        '</tr></thead><tbody>' +
        rows.map(function (r) {
          return '<tr>' +
            '<td>' + r.rank + '</td>' +
            '<td>' + esc(r.campaign_name) + '</td>' +
            '<td>' + esc(r.campaign_type) + '</td>' +
            '<td>' + fmt(r.campaign_score) + '</td>' +
            '<td>' + (r.claim_rate == null ? "—" : r.claim_rate + "%") + '</td>' +
            '<td>' + (r.actual_player_pct == null ? "—" : r.actual_player_pct + "%") + '</td>' +
            '<td>' + (r.voucher_hunter_pct == null ? "—" : r.voucher_hunter_pct + "%") + '</td>' +
            '<td>' + (r.conversion_pct == null ? "—" : r.conversion_pct + "%") + '</td>' +
            '<td>' + fmt(r.avg_claim_speed_minutes) + '</td>' +
            '<td><button class="btn" onclick="ciOpenCampaign(' + JSON.stringify(r.campaign_id) + ')">Details</button></td>' +
            '</tr>';
        }).join("") + '</tbody></table>';
    }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load rankings.</div>'; banner("❌ " + e.message, "error"); });
  }

  window.ciOpenCampaign = function (campaignId) {
    ci.campaignId = campaignId;
    ci.tab = "insights";
    $all("#ci-tabs button").forEach(function (b) { b.classList.toggle("active", b.dataset.tab === "insights"); });
    loadCampaignIntelligence(false);
  };

  function ciRequireCampaign(body, onReady) {
    if (!ci.campaignId) {
      body.innerHTML = '<p style="color:var(--muted);font-size:13px;">Pick a campaign from the Rankings tab (click "Details") to see this view.</p>';
      return;
    }
    api("/api/admin/campaign-builder/intelligence/campaign/" + ci.campaignId).then(function (res) {
      onReady(res.campaign);
    }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load campaign detail.</div>'; banner("❌ " + e.message, "error"); });
  }

  function ciLoadInsights(body) {
    ciRequireCampaign(body, function (c) {
      body.innerHTML = '<h3 style="margin-top:0;">' + esc(c.campaign_name) + ' <span style="font-size:12px;color:var(--muted);">(rank #' + fmt(c.rank) + ', score ' + fmt(c.campaign_score) + ')</span></h3>' +
        '<div style="display:flex;flex-wrap:wrap;gap:8px;">' + ciInsightList(c.insights) + '</div>';
    });
  }

  function ciLoadRecommendations(body) {
    ciRequireCampaign(body, function (c) {
      var recs = c.recommendations || [];
      body.innerHTML = '<h3 style="margin-top:0;">' + esc(c.campaign_name) + '</h3>' +
        (recs.length
          ? '<ul>' + recs.map(function (r) { return '<li>' + esc(r) + '</li>'; }).join("") + '</ul>'
          : '<p style="color:var(--muted);font-size:13px;">No changes recommended — this campaign is performing within normal thresholds.</p>');
    });
  }

  function ciSegmentMatrixTable(matrix) {
    return '<table class="data-table"><thead><tr><th>Segment</th><th>Claimed</th><th>Conversion</th><th>Score</th></tr></thead><tbody>' +
      matrix.map(function (r) {
        return '<tr><td>' + esc(r.segment) + '</td><td>' + fmt(r.claimed) + '</td><td>' + (r.conversion_pct == null ? "—" : r.conversion_pct + "%") + '</td><td>' + esc(r.score) + '</td></tr>';
      }).join("") + '</tbody></table>';
  }

  function ciLoadSegments(body) {
    api("/api/admin/campaign-builder/intelligence/segments").then(function (res) {
      body.innerHTML = '<div class="section-title" style="font-size:13px;">Segment ROI (global, all campaigns)</div>' +
        '<table class="data-table"><thead><tr><th>Segment</th><th>Claimed</th><th>Avg Conversion</th><th>ROI</th></tr></thead><tbody>' +
        (res.segment_roi || []).map(function (r) {
          return '<tr><td>' + esc(r.segment) + '</td><td>' + fmt(r.claimed) + '</td><td>' + (r.avg_conversion_pct == null ? "—" : r.avg_conversion_pct + "%") + '</td><td>' + fmt(r.roi) + '</td></tr>';
        }).join("") + '</tbody></table>' +
        '<div style="margin-top:12px;display:flex;gap:24px;flex-wrap:wrap;">' +
        '<div><strong>Prioritize:</strong> ' + (res.recommended_segments || []).map(esc).join(", ") + '</div>' +
        '<div><strong>Limit:</strong> ' + (res.avoid_segments || []).map(esc).join(", ") + '</div>' +
        '</div>' +
        (ci.campaignId ? '<div class="section-title" style="font-size:13px;margin-top:16px;">Per-Campaign Segment Matrix</div><div id="ci-segment-per-campaign"></div>' : '');
      if (ci.campaignId) {
        api("/api/admin/campaign-builder/intelligence/campaign/" + ci.campaignId).then(function (res2) {
          $("#ci-segment-per-campaign").innerHTML = ciSegmentMatrixTable(res2.campaign.segment_matrix);
        });
      }
    }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load segments.</div>'; banner("❌ " + e.message, "error"); });
  }

  function ciLoadTemplates(body) {
    api("/api/admin/campaign-builder/intelligence/templates").then(function (res) {
      var rows = res.templates || [];
      body.innerHTML = '<table class="data-table"><thead><tr><th>Template</th><th>Campaigns</th><th>Avg Score</th><th>Avg Claim Rate</th><th>Avg Conversion</th><th>Avg Abuse</th></tr></thead><tbody>' +
        rows.map(function (r) {
          return '<tr><td>' + esc(r.template) + '</td><td>' + fmt(r.campaign_count) + '</td><td>' + fmt(r.avg_score) + '</td>' +
            '<td>' + (r.avg_claim_rate == null ? "—" : r.avg_claim_rate + "%") + '</td>' +
            '<td>' + (r.avg_conversion_pct == null ? "—" : r.avg_conversion_pct + "%") + '</td>' +
            '<td>' + (r.avg_abuse_pct == null ? "—" : r.avg_abuse_pct + "%") + '</td></tr>';
        }).join("") + '</tbody></table>';
    }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load templates.</div>'; banner("❌ " + e.message, "error"); });
  }

  function ciLoadReleases(body) {
    api("/api/admin/campaign-builder/intelligence/releases").then(function (res) {
      var rows = res.releases || [];
      body.innerHTML = '<table class="data-table"><thead><tr><th>Release Strategy</th><th>Campaigns</th><th>Avg Claim Speed (min)</th><th>Avg Conversion</th><th>Avg Abuse</th><th>Avg Completion</th></tr></thead><tbody>' +
        rows.map(function (r) {
          return '<tr><td>' + esc(r.release_strategy) + '</td><td>' + fmt(r.campaign_count) + '</td><td>' + fmt(r.avg_claim_speed_minutes) + '</td>' +
            '<td>' + (r.avg_conversion_pct == null ? "—" : r.avg_conversion_pct + "%") + '</td>' +
            '<td>' + (r.avg_abuse_pct == null ? "—" : r.avg_abuse_pct + "%") + '</td>' +
            '<td>' + (r.avg_completion_pct == null ? "—" : r.avg_completion_pct + "%") + '</td></tr>';
        }).join("") + '</tbody></table>';
    }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load release strategy ranking.</div>'; banner("❌ " + e.message, "error"); });
  }

  function ciLoadBestTime(body) {
    api("/api/admin/campaign-builder/intelligence/best-time").then(function (res) {
      var rows = res.hours || [];
      body.innerHTML = '<div style="font-size:14px;font-weight:600;margin-bottom:12px;">' + esc(res.recommendation) + '</div>' +
        (rows.length
          ? '<table class="data-table"><thead><tr><th>Hour</th><th>Score</th><th>Sample Size</th></tr></thead><tbody>' +
            rows.map(function (r) { return '<tr><td>' + esc(r.label) + '</td><td>' + fmt(r.score) + '</td><td>' + fmt(r.sample_size) + '</td></tr>'; }).join("") + '</tbody></table>'
          : '');
    }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load best-time analysis.</div>'; banner("❌ " + e.message, "error"); });
  }

  function ciLoadPlaybook(body) {
    var path = "/api/admin/campaign-builder/intelligence/playbook" + (ci.campaignId ? "?campaign_id=" + ci.campaignId : "");
    api(path).then(function (res) {
      var p = res.playbook;
      body.innerHTML = '<div style="font-size:12px;color:var(--muted);margin-bottom:10px;">Based on: ' + esc(p.based_on_campaign_name) + ' — recommendation only, no auto-launch.</div>' +
        '<table class="data-table"><tbody>' +
        '<tr><td>Template</td><td>' + esc(p.template) + '</td></tr>' +
        '<tr><td>Audience</td><td>' + esc(p.audience) + '</td></tr>' +
        '<tr><td>Release</td><td>' + esc(p.release.strategy) + (p.release.rate_per_hour ? ' (' + fmt(p.release.rate_per_hour) + '/hour)' : '') + '</td></tr>' +
        '<tr><td>Voucher Count</td><td>' + fmt(p.voucher_count) + '</td></tr>' +
        '<tr><td>Expected Claim Rate</td><td>' + (p.expected_claim_rate_pct == null ? "—" : p.expected_claim_rate_pct + "%") + '</td></tr>' +
        '<tr><td>Expected Abuse</td><td>' + (p.expected_abuse_pct == null ? "—" : p.expected_abuse_pct + "%") + '</td></tr>' +
        '<tr><td>Confidence</td><td>' + esc(p.confidence) + '</td></tr>' +
        '</tbody></table>' +
        (p.recommendations && p.recommendations.length
          ? '<div class="section-title" style="font-size:13px;margin-top:12px;">Supporting Recommendations</div><ul>' + p.recommendations.map(function (r) { return '<li>' + esc(r) + '</li>'; }).join("") + '</ul>'
          : '');
    }).catch(function (e) { body.innerHTML = '<div class="banner error">Failed to load playbook.</div>'; banner("❌ " + e.message, "error"); });
  }

  (function bindCampaignIntelligenceControls() {
    $all("#ci-tabs button").forEach(function (b) {
      b.addEventListener("click", function () {
        ci.tab = b.dataset.tab;
        $all("#ci-tabs button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadCampaignIntelligence(false);
      });
    });
  })();

  window.cbDelete = function (id) {
    confirmTyped("DELETE", "Delete draft campaign?", "Any drops it already compiled are NOT affected.").then(function (ok) {
      if (!ok) return;
      cbApi("/api/admin/campaign-builder/campaigns/" + id, { method: "DELETE" }).then(function (res) {
        if (res.ok) { banner("✅ Draft deleted", "ok"); refreshCurrent(true); }
        else banner("❌ Delete failed: " + ((res.body && res.body.code) || "unknown"), "error");
      });
    });
  };

  window.cbResume = function (id) {
    cbApi("/api/admin/campaign-builder/campaigns/" + id).then(function (res) {
      if (!res.ok) { banner("Failed to load campaign", "error"); return; }
      cb.campaignId = id;
      cb.campaign = res.body.campaign;
      state.view = "campaignBuilder";
      switchView("campaignBuilder");
      _cbOpenWizard();
    });
  };

  function _cbOpenWizard() {
    $("#cb-list-panel").classList.add("hidden");
    $("#cb-wizard-panel").classList.remove("hidden");
    ensureCbMeta().then(function () {
      _cbRenderTypeOptions();
      _cbRenderAudienceOptions();
      _cbPopulateFromCampaign();
      _cbGoStep(1);
    });
  }

  function _cbNewDraft() {
    cb.campaignId = null;
    cb.campaign = { campaign_type: "smart_default", audience_mode: "smart_segment_pct", audience_params: {}, release_style: "immediate", release_params: {}, reward_type: "voucher_pool", reward_params: { codes: [], pool: "public" } };
    _cbOpenWizard();
  }

  function _cbRenderTypeOptions() {
    var wrap = $("#cb-type-options");
    wrap.innerHTML = (cb.meta.campaign_types || []).map(function (t) {
      return '<button class="btn cb-type-btn" data-type="' + t + '" style="background:transparent;border:1px solid var(--border);">' + esc(CB_TYPE_LABELS[t] || t) + '</button>';
    }).join("");
    $all(".cb-type-btn", wrap).forEach(function (b) {
      b.addEventListener("click", function () {
        cb.campaign.campaign_type = b.dataset.type;
        var defaults = (cb.meta.template_defaults || {})[b.dataset.type] || {};
        if (!cb._touchedAudience) cb.campaign.audience_mode = defaults.audience_mode || cb.campaign.audience_mode;
        if (!cb._touchedRelease) cb.campaign.release_style = defaults.release_style || cb.campaign.release_style;
        if (!cb._touchedReward) cb.campaign.reward_type = defaults.reward_type || cb.campaign.reward_type;
        _cbPopulateFromCampaign();
      });
    });
  }

  function _cbRenderAudienceOptions() {
    var wrap = $("#cb-audience-options");
    wrap.innerHTML = (cb.meta.audience_modes || []).map(function (m) {
      return '<button class="btn cb-audience-btn" data-mode="' + m + '" style="background:transparent;border:1px solid var(--border);">' + esc(CB_AUDIENCE_LABELS[m] || m) + '</button>';
    }).join("");
    $all(".cb-audience-btn", wrap).forEach(function (b) {
      b.addEventListener("click", function () {
        cb._touchedAudience = true;
        cb.campaign.audience_mode = b.dataset.mode;
        _cbRenderAudienceParams();
        _cbHighlight();
      });
    });
  }

  function _cbRenderAudienceParams() {
    var mode = cb.campaign.audience_mode;
    var params = cb.campaign.audience_params || {};
    var el = $("#cb-audience-params");
    if (mode === "whitelist") {
      el.innerHTML = '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Usernames (one per line, or from affiliate export)</label>' +
        '<textarea id="cb-whitelist-usernames" rows="4" style="width:100%;box-sizing:border-box;" placeholder="@alice\n@bob">' + esc((params.usernames || []).join("\n")) + '</textarea>';
      $("#cb-whitelist-usernames").addEventListener("change", function (e) {
        cb.campaign.audience_params.usernames = e.target.value.split("\n").map(function (s) { return s.trim(); }).filter(Boolean);
      });
    } else if (mode === "vip") {
      el.innerHTML = '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Tier</label>' +
        '<input class="filter-input" id="cb-vip-tier" value="' + esc(params.tier || "VIP") + '" style="max-width:200px;" />';
      $("#cb-vip-tier").addEventListener("change", function (e) { cb.campaign.audience_params.tier = e.target.value; });
    } else if (mode === "region") {
      el.innerHTML = '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Regions (comma-separated)</label>' +
        '<input class="filter-input" id="cb-regions" value="' + esc((params.regions || []).join(", ")) + '" style="max-width:300px;" />';
      $("#cb-regions").addEventListener("change", function (e) {
        cb.campaign.audience_params.regions = e.target.value.split(",").map(function (s) { return s.trim(); }).filter(Boolean);
      });
    } else if (mode === "smart_segment_pct" || cb.campaign.campaign_type === "segment") {
      var segs = params.segments || [];
      el.innerHTML = '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Restrict to backend segment(s) (leave empty = no restriction, weighted by global defaults only)</label>' +
        '<div style="display:flex;flex-wrap:wrap;gap:6px;">' +
        (cb.meta.valid_segments || []).map(function (s) {
          return '<label class="ce-chip-label"><input type="checkbox" class="cb-segment-cb" value="' + s + '" ' + (segs.indexOf(s) >= 0 ? "checked" : "") + ' /> ' + esc(CB_SEGMENT_LABELS[s] || s) + '</label>';
        }).join("") + '</div>';
      $all(".cb-segment-cb", el).forEach(function (cbx) {
        cbx.addEventListener("change", function () {
          cb.campaign.audience_params.segments = $all(".cb-segment-cb", el).filter(function (x) { return x.checked; }).map(function (x) { return x.value; });
        });
      });
    } else {
      el.innerHTML = '<div style="font-size:12px;color:var(--muted);">No further configuration needed for this audience mode.</div>';
    }
  }

  function _cbRenderRewardParams() {
    var type = cb.campaign.reward_type;
    var params = cb.campaign.reward_params || {};
    var el = $("#cb-reward-params");
    if (type === "personalised_voucher") {
      el.innerHTML = '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Assignments (username,code — one per line)</label>' +
        '<textarea id="cb-assignments" rows="5" style="width:100%;box-sizing:border-box;" placeholder="@alice,CODE1\n@bob,CODE2">' +
        esc((params.assignments || []).map(function (a) { return a.username + "," + a.code; }).join("\n")) + '</textarea>';
      $("#cb-assignments").addEventListener("change", function (e) {
        cb.campaign.reward_params.assignments = e.target.value.split("\n").map(function (line) {
          var parts = line.split(",");
          return { username: (parts[0] || "").trim(), code: (parts[1] || "").trim() };
        }).filter(function (a) { return a.username && a.code; });
      });
    } else if (type === "xp") {
      el.innerHTML = '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">XP Amount (grant manually via Add/Reduce XP for the resolved audience — no drop is generated)</label>' +
        '<input type="number" class="filter-input" id="cb-xp-amount" value="' + esc(params.xp_amount || "") + '" style="max-width:160px;" />';
      $("#cb-xp-amount").addEventListener("change", function (e) { cb.campaign.reward_params.xp_amount = parseInt(e.target.value, 10) || 0; });
    } else {
      el.innerHTML = '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Voucher Codes (one per line)</label>' +
        '<textarea id="cb-codes" rows="5" style="width:100%;box-sizing:border-box;" placeholder="CODE1\nCODE2\nCODE3">' + esc((params.codes || []).join("\n")) + '</textarea>' +
        '<label style="font-size:12px;font-weight:600;display:block;margin:8px 0 4px;">Pool</label>' +
        '<select id="cb-pool" style="padding:6px;border:1px solid var(--border);border-radius:6px;background:var(--card-bg);color:var(--text);">' +
        '<option value="public"' + (params.pool !== "my" ? " selected" : "") + '>public</option>' +
        '<option value="my"' + (params.pool === "my" ? " selected" : "") + '>my</option></select>';
      $("#cb-codes").addEventListener("change", function (e) {
        cb.campaign.reward_params.codes = e.target.value.split("\n").map(function (s) { return s.trim(); }).filter(Boolean);
      });
      $("#cb-pool").addEventListener("change", function (e) { cb.campaign.reward_params.pool = e.target.value; });
    }
  }

  function _cbHighlight() {
    $all(".cb-type-btn").forEach(function (b) { b.classList.toggle("active", b.dataset.type === cb.campaign.campaign_type); });
    $all(".cb-audience-btn").forEach(function (b) { b.classList.toggle("active", b.dataset.mode === cb.campaign.audience_mode); });
    $all("#cb-release-options button").forEach(function (b) { b.classList.toggle("active", b.dataset.release === cb.campaign.release_style); });
    $all("#cb-reward-options button").forEach(function (b) { b.classList.toggle("active", b.dataset.reward === cb.campaign.reward_type); });
  }

  function _cbPopulateFromCampaign() {
    $("#cb-name").value = cb.campaign.campaign_name || "";
    $("#cb-type-notes").textContent = ((cb.meta.template_defaults || {})[cb.campaign.campaign_type] || {}).notes || "";
    var rp = cb.campaign.release_params || {};
    $("#cb-starts-at").value = rp.startsAtLocal || "";
    $("#cb-ends-at").value = rp.endsAtLocal || "";

    var isBatch = !!cb.campaign.release_type;
    $("#cb-batch-toggle").checked = isBatch;
    $("#cb-batch-params").classList.toggle("hidden", !isBatch);
    $("#cb-total-vouchers").value = cb.campaign.total_vouchers || "";
    $("#cb-batch-size").value = cb.campaign.batch_size || "";
    $("#cb-release-type").value = cb.campaign.release_type || "hourly";
    $("#cb-release-interval-minutes").value = cb.campaign.release_interval_minutes || "";
    $("#cb-custom-schedule").value = (cb.campaign.release_schedule || []).join("\n");
    _cbToggleBatchTypeFields();

    _cbRenderAudienceParams();
    _cbRenderRewardParams();
    _cbHighlight();
  }

  function _cbToggleBatchTypeFields() {
    var rt = $("#cb-release-type").value;
    $("#cb-interval-minutes-wrap").classList.toggle("hidden", rt !== "interval_minutes");
    $("#cb-custom-schedule-wrap").classList.toggle("hidden", rt !== "custom");
  }

  function _cbCollectFromForm() {
    cb.campaign.campaign_name = ($("#cb-name").value || "").trim();
    cb.campaign.release_params = {
      startsAtLocal: ($("#cb-starts-at").value || "").trim(),
      endsAtLocal: ($("#cb-ends-at").value || "").trim(),
    };
    if ($("#cb-batch-toggle").checked) {
      cb.campaign.release_type = $("#cb-release-type").value;
      cb.campaign.total_vouchers = parseInt($("#cb-total-vouchers").value, 10) || 0;
      cb.campaign.batch_size = parseInt($("#cb-batch-size").value, 10) || 0;
      cb.campaign.release_interval_minutes = parseInt($("#cb-release-interval-minutes").value, 10) || null;
      cb.campaign.release_schedule = ($("#cb-custom-schedule").value || "").split("\n").map(function (s) { return s.trim(); }).filter(Boolean);
    } else {
      cb.campaign.release_type = null;
    }
  }

  function _cbGoStep(n) {
    cb.step = n;
    $all("#cb-steps button").forEach(function (b) { b.classList.toggle("active", parseInt(b.dataset.step, 10) === n); });
    $all(".cb-step").forEach(function (p) { p.classList.toggle("hidden", parseInt(p.dataset.stepPanel, 10) !== n); });
    if (n === 1) $("#cb-type-notes").textContent = ((cb.meta.template_defaults || {})[cb.campaign.campaign_type] || {}).notes || "";
  }

  function _cbSaveDraft(thenFn) {
    _cbCollectFromForm();
    if (!cb.campaign.campaign_name) { banner("Campaign name is required.", "error"); return; }
    var body = {
      campaign_name: cb.campaign.campaign_name,
      campaign_type: cb.campaign.campaign_type,
      audience_mode: cb.campaign.audience_mode,
      audience_params: cb.campaign.audience_params,
      release_style: cb.campaign.release_style,
      release_params: cb.campaign.release_params,
      reward_type: cb.campaign.reward_type,
      reward_params: cb.campaign.reward_params,
      release_type: cb.campaign.release_type || null,
    };
    if (cb.campaign.release_type) {
      body.total_vouchers = cb.campaign.total_vouchers;
      body.batch_size = cb.campaign.batch_size;
      body.release_interval_minutes = cb.campaign.release_interval_minutes;
      body.release_schedule = cb.campaign.release_schedule || [];
    }
    var url = cb.campaignId ? "/api/admin/campaign-builder/campaigns/" + cb.campaignId : "/api/admin/campaign-builder/campaigns";
    var method = cb.campaignId ? "PUT" : "POST";
    cbApi(url, { method: method, body: body }).then(function (res) {
      if (!res.ok) { banner("Save failed: " + ((res.body && res.body.code) || "unknown"), "error"); return; }
      cb.campaignId = res.body.campaign.id;
      cb.campaign = res.body.campaign;
      banner("Draft saved.", "ok");
      if (thenFn) thenFn();
    });
  }

  function bindCampaignBuilder() {
    var newBtn = $("#cb-new-btn");
    if (newBtn) newBtn.addEventListener("click", _cbNewDraft);

    var backBtn = $("#cb-back-btn");
    if (backBtn) backBtn.addEventListener("click", function () { loadCampaignBuilder(true); });

    $all("#cb-steps button").forEach(function (b) {
      b.addEventListener("click", function () { _cbGoStep(parseInt(b.dataset.step, 10)); });
    });

    $all("#cb-release-options button").forEach(function (b) {
      b.addEventListener("click", function () {
        cb._touchedRelease = true;
        cb.campaign.release_style = b.dataset.release;
        _cbHighlight();
      });
    });

    $all("#cb-reward-options button").forEach(function (b) {
      b.addEventListener("click", function () {
        cb._touchedReward = true;
        cb.campaign.reward_type = b.dataset.reward;
        _cbRenderRewardParams();
        _cbHighlight();
      });
    });

    var batchToggle = $("#cb-batch-toggle");
    if (batchToggle) batchToggle.addEventListener("change", function () {
      $("#cb-batch-params").classList.toggle("hidden", !batchToggle.checked);
    });
    var releaseTypeSel = $("#cb-release-type");
    if (releaseTypeSel) releaseTypeSel.addEventListener("change", _cbToggleBatchTypeFields);

    var saveDraftBtn = $("#cb-save-draft-btn");
    if (saveDraftBtn) saveDraftBtn.addEventListener("click", function () { _cbSaveDraft(); });

    var previewBtn = $("#cb-preview-btn");
    if (previewBtn) previewBtn.addEventListener("click", function () {
      _cbSaveDraft(function () {
        cbApi("/api/admin/campaign-builder/campaigns/" + cb.campaignId + "/preview", { method: "POST", body: {} }).then(function (res) {
          if (!res.ok) { $("#cb-preview-result").innerHTML = '<div class="banner error">Preview failed: ' + esc((res.body && res.body.code) || "unknown") + '</div>'; return; }
          _cbRenderPreview(res.body.preview);
        });
      });
    });

    var launchBtn = $("#cb-launch-btn");
    if (launchBtn) launchBtn.addEventListener("click", function () {
      if (launchBtn.dataset.loading === "1") return;
      var confirmVal = ($("#cb-launch-confirm").value || "").trim();
      if (confirmVal !== "LAUNCH") { $("#cb-launch-result").innerHTML = '<div class="banner error">Type LAUNCH exactly to confirm.</div>'; return; }
      if (!cb.campaignId) { $("#cb-launch-result").innerHTML = '<div class="banner error">Save the draft first.</div>'; return; }
      cbBtnStart(launchBtn, "⏳ Launching...");
      cbApi("/api/admin/campaign-builder/campaigns/" + cb.campaignId + "/compile", { method: "POST", body: { confirm: "LAUNCH" } })
        .then(function (res) {
          if (!res.ok) {
            var errMsg = cbErrMsg(res);
            $("#cb-launch-result").innerHTML = '<div class="banner error">Compile failed: ' + esc(errMsg) + '</div>';
            banner("❌ Failed to launch campaign: " + errMsg, "error");
            return;
          }
          if (res.body.child_drop_ids) {
            $("#cb-launch-result").innerHTML = '<div class="banner ok">Compiled ' + res.body.child_drop_ids.length + ' child drop(s). Released now: ' +
              ((res.body.released_now || []).length) + '</div>' +
              (res.body.warnings && res.body.warnings.length ? '<div style="margin-top:6px;font-size:12px;color:var(--muted);">' + res.body.warnings.map(esc).join("<br/>") + '</div>' : '');
          } else {
            $("#cb-launch-result").innerHTML = '<div class="banner ok">Compiled ' + (res.body.compiled_drop_ids || []).length + ' drop(s): ' +
              esc((res.body.compiled_drop_ids || []).join(", ")) + '</div>' +
              (res.body.warnings && res.body.warnings.length ? '<div style="margin-top:6px;font-size:12px;color:var(--muted);">' + res.body.warnings.map(esc).join("<br/>") + '</div>' : '');
          }
          banner("✅ Campaign launched", "ok");
        })
        .catch(function (e) {
          $("#cb-launch-result").innerHTML = '<div class="banner error">Compile failed: ' + esc(e.message) + '</div>';
          banner("❌ Failed to launch campaign: " + e.message, "error");
        })
        .finally(function () { cbBtnStop(launchBtn); });
    });
  }

  function _cbRenderPreview(p) {
    var el = $("#cb-preview-result");
    if (p.batch_count !== undefined) {
      el.innerHTML =
        '<div class="card-grid">' +
        kpiCard("Total Vouchers", p.total_vouchers) +
        kpiCard("Batch Size", p.batch_size) +
        kpiCard("Number of Batches", p.batch_count) +
        kpiCard("Estimated Duration (h)", p.estimated_duration_hours) +
        '</div>' +
        '<div style="margin-top:12px;"><div class="section-title" style="font-size:13px;">Release Schedule</div>' +
        '<div>First: ' + esc(p.first_release_at || "—") + ' &nbsp; Last: ' + esc(p.last_release_at || "—") + '</div>' +
        '<div style="max-height:160px;overflow-y:auto;margin-top:6px;font-size:12px;">' +
        (p.release_schedule || []).map(function (t, i) { return '<div>Batch ' + (i + 1) + ': ' + esc(t || "manual — not auto-released") + '</div>'; }).join("") +
        '</div></div>' +
        '<div style="margin-top:12px;font-size:12px;">Audience: ' + esc(p.audience_mode || "") + ' &nbsp; Drop type: ' + esc(p.drop_type || "") +
        (p.region_restriction ? ' &nbsp; Region: ' + esc(p.region_restriction.join(", ")) : '') + '</div>' +
        (p.warnings && p.warnings.length ? '<div style="margin-top:12px;padding:10px;border:1px solid var(--border);border-radius:6px;font-size:12px;color:var(--muted);"><strong>Notes:</strong><br/>' + p.warnings.map(esc).join("<br/>") + '</div>' : '') +
        (!p.launchable ? '<div class="banner error" style="margin-top:8px;">Not launchable — resolve the errors above first.</div>' : '');
      return;
    }
    var safety = p.safety_checks || {};
    var segDist = Object.keys(p.segment_distribution || {}).map(function (k) {
      return '<div>' + esc(CB_SEGMENT_LABELS[k] || k) + ': ' + fmt(p.segment_distribution[k]) + '</div>';
    }).join("");
    el.innerHTML =
      '<div class="card-grid">' +
      kpiCard("Expected Drops", p.expected_drop_count) +
      kpiCard("Estimated Reach", p.estimated_reach) +
      kpiCard("Expected Voucher Count", p.expected_voucher_count) +
      kpiCard("Campaign Duration (h)", safety.campaign_duration_hours) +
      '</div>' +
      '<div style="margin-top:12px;"><div class="section-title" style="font-size:13px;">Compiler Output</div>' +
      '<div>' + (p.expected_drop_names || []).join(", ") + '</div></div>' +
      (segDist ? '<div style="margin-top:12px;"><div class="section-title" style="font-size:13px;">Segment Distribution</div>' + segDist + '</div>' : '') +
      (p.warnings && p.warnings.length ? '<div style="margin-top:12px;padding:10px;border:1px solid var(--border);border-radius:6px;font-size:12px;color:var(--muted);"><strong>Notes:</strong><br/>' + p.warnings.map(esc).join("<br/>") + '</div>' : '');
  }

  // ---------- Voucher Drops (migrated from legacy MiniApp admin panel) ----------
  // Reuses the same /v2/miniapp/admin/drops* endpoints used by static/index.html#admin-panel.
  function toKLLocalString(inputValue) {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(inputValue || "")) return "";
    var parts = inputValue.split("T");
    return parts[0] + " " + parts[1] + ":00";
  }

  function loadDrops(force) {
    statePanel("drops-list-body", "loading", "Loading drops…");
    fetch("/v2/miniapp/admin/drops_v2", { credentials: "same-origin", headers: { "Accept": "application/json" } })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var items = data.items || [];
        var dropSelect = $("#dra-drop-id");
        if (dropSelect) {
          var pooled = items.filter(function (d) { return d.type === "pooled"; });
          dropSelect.innerHTML = '<option value="">— select a pooled drop —</option>' +
            pooled.map(function (d) { return '<option value="' + esc(d.dropId) + '">' + esc(d.name) + " (" + esc(d.status) + ")</option>"; }).join("");
        }
        if (!items.length) {
          statePanel("drops-list-body", "empty", "No drops found.");
          return;
        }
        var rows = items.map(function (d) {
          var codesInfo = d.type === "personalised"
            ? ("Assigned " + fmt(d.assigned) + " / Claimed " + fmt(d.claimed))
            : ("Free " + fmt(d.codesFree) + " / Total " + fmt(d.codesTotal));
          return "<tr>" +
            "<td>" + esc(d.name) + "</td>" +
            "<td>" + esc(d.type) + "</td>" +
            '<td><span class="pill ' + esc(d.status) + '">' + esc(d.status) + "</span></td>" +
            "<td>" + fmt(d.priority) + "</td>" +
            "<td>" + codesInfo + "</td>" +
            "<td>" +
            '<button class="btn" data-drop-op="start_now" data-drop-id="' + esc(d.dropId) + '">Start</button> ' +
            '<button class="btn" data-drop-op="pause" data-drop-id="' + esc(d.dropId) + '">Pause</button> ' +
            '<button class="btn danger" data-drop-op="end_now" data-drop-id="' + esc(d.dropId) + '">End</button>' +
            "</td></tr>";
        }).join("");
        $("#drops-list-body").innerHTML =
          '<table class="data-table"><thead><tr><th>Name</th><th>Type</th><th>Status</th><th>Priority</th><th>Codes</th><th>Actions</th></tr></thead><tbody>' +
          rows + "</tbody></table>";
      })
      .catch(function (e) { statePanel("drops-list-body", "error", "Failed to load drops: " + e.message); });
  }

  function bindDrops() {
    var refreshBtn = $("#drops-refresh-btn");
    if (refreshBtn) refreshBtn.addEventListener("click", function () { loadDrops(true); });

    var typeSel = $("#dr-type");
    function toggleDropTypeUi() {
      var isPersonalised = typeSel && typeSel.value === "personalised";
      var poolRow = $("#dr-pool-row");
      if (poolRow) poolRow.style.display = isPersonalised ? "none" : "block";
      var label = $("#dr-codes-label");
      var codesEl = $("#dr-codes");
      if (label) label.textContent = isPersonalised ? "Assignments (one \"username,code\" pair per line)" : "Codes (one per line)";
      if (codesEl) codesEl.placeholder = isPersonalised ? "username1,CODE1\nusername2,CODE2" : "CODE1\nCODE2\nCODE3";
    }
    if (typeSel) { typeSel.addEventListener("change", toggleDropTypeUi); toggleDropTypeUi(); }

    var createBtn = $("#dr-create-btn");
    if (createBtn) createBtn.addEventListener("click", function () {
      var resultEl = $("#dr-create-result");
      var name = ($("#dr-name").value || "").trim();
      var type = $("#dr-type").value || "pooled";
      var startsAtLocal = toKLLocalString($("#dr-starts").value);
      if (!name || !startsAtLocal) {
        resultEl.textContent = "Name and Starts At are required.";
        return;
      }
      var payload = {
        name: name,
        type: type,
        startsAtLocal: startsAtLocal,
        priority: parseInt($("#dr-priority").value, 10) || 100,
      };
      var endsAtLocal = toKLLocalString($("#dr-ends").value);
      if (endsAtLocal) payload.endsAtLocal = endsAtLocal;

      var codesRaw = ($("#dr-codes").value || "").split(/\r?\n/).map(function (x) { return x.trim(); }).filter(Boolean);
      if (type === "personalised") {
        var assignments = codesRaw.map(function (line) {
          var parts = line.split(/[,\t]/).map(function (s) { return s.trim(); });
          return parts[0] && parts[1] ? { username: parts[0], code: parts[1] } : null;
        }).filter(Boolean);
        if (!assignments.length) { resultEl.textContent = "Please provide username,code pairs."; return; }
        payload.assignments = assignments;
      } else {
        if (!codesRaw.length) { resultEl.textContent = "Please provide codes."; return; }
        payload.codes = codesRaw;
        payload.pool = $("#dr-pool").value || "public";
      }

      createBtn.disabled = true;
      resultEl.textContent = "Creating…";
      fetch("/v2/miniapp/admin/drops", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); })
        .then(function (res) {
          if (!res.ok || res.d.status !== "ok") throw new Error(res.d.code || "unknown");
          resultEl.textContent = "Created drop: " + res.d.dropId;
          toast("✅ Voucher drop created: " + res.d.dropId, "success");
          loadDrops(true);
        })
        .catch(function (e) { resultEl.textContent = "Create failed: " + e.message; toast("❌ Failed to create drop: " + e.message, "error"); })
        .finally(function () { createBtn.disabled = false; });
    });

    var addBtn = $("#dra-add-btn");
    if (addBtn) addBtn.addEventListener("click", function () {
      var resultEl = $("#dra-add-result");
      var dropId = $("#dra-drop-id").value;
      if (!dropId) { resultEl.textContent = "Please select a drop."; return; }
      var codes = ($("#dra-codes").value || "").split(/\r?\n/).map(function (x) { return x.trim(); }).filter(Boolean);
      if (!codes.length) { resultEl.textContent = "Please provide codes."; return; }
      var payload = { type: "pooled", pool: $("#dra-pool").value || "public", codes: codes };

      addBtn.disabled = true;
      resultEl.textContent = "Adding…";
      fetch("/v2/miniapp/admin/drops/" + encodeURIComponent(dropId) + "/codes", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); })
        .then(function (res) {
          if (!res.ok || res.d.status !== "ok") throw new Error(res.d.code || "unknown");
          resultEl.textContent = "Added " + res.d.inserted + " code(s).";
          toast("✅ Added " + res.d.inserted + " code(s)", "success");
          loadDrops(true);
        })
        .catch(function (e) { resultEl.textContent = "Add failed: " + e.message; toast("❌ Failed to add codes: " + e.message, "error"); })
        .finally(function () { addBtn.disabled = false; });
    });

    document.addEventListener("click", function (event) {
      var btn = event.target && event.target.closest && event.target.closest("[data-drop-op]");
      if (!btn) return;
      var op = btn.dataset.dropOp;
      var dropId = btn.dataset.dropId;
      if (op === "end_now" && !confirm("End this drop now?")) return;
      btn.disabled = true;
      fetch("/v2/miniapp/admin/drops/" + encodeURIComponent(dropId) + "/actions", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ op: op }),
      }).then(function (r) { return r.json(); })
        .then(function (d) {
          if (d.status === "ok") { toast("✅ Drop updated", "success"); loadDrops(true); }
          else banner("❌ Drop action failed: " + (d.code || "unknown"), "error");
        })
        .catch(function (e) { banner("❌ Drop action failed: " + e.message, "error"); })
        .finally(function () { btn.disabled = false; });
    });
  }

  // ---------- Affiliate Voucher Pools (migrated from legacy MiniApp admin panel) ----------
  // ---------- Player Campaigns (Campaign Centre / tournament reward integration) ----------

  function gcPill(status) {
    var known = { live: "approved", draft: "neutral", scheduled: "pending", paused: "pending",
      ended: "neutral", archived: "neutral", active: "approved", inactive: "neutral",
      assigned: "approved", out_of_stock: "rejected", rejected: "rejected", pending_review: "pending",
      expired: "rejected" };
    return '<span class="pill ' + (known[status] || "neutral") + '">' + esc(status || "—") + '</span>';
  }

  function loadGcCampaigns(force) {
    statePanel("gc-campaigns-body", "loading", "Loading campaigns…");
    api("/api/admin/gc-campaigns").then(function (data) {
      var items = data.campaigns || [];
      if (!items.length) { $("#gc-campaigns-body").innerHTML = emptyState("No campaigns yet — create one above."); return; }
      var rows = items.map(function (c) {
        var vis = c.effective_visibility || {};
        var visBadge = vis.publicly_visible ? '<span class="pill approved">public</span>' : '<span class="pill pending">admin-only</span>';
        var reasons = (vis.reasons || []).map(function (r) { return '<div class="sub">' + esc(r) + '</div>'; }).join("");
        return '<tr><td>' + esc(c.name || "") + '<div class="sub">' + esc(c.campaign_id) + '</div></td>' +
          '<td>' + esc(c.type || "") + '</td>' +
          '<td>' + gcPill(c.status) + '</td>' +
          '<td>' + visBadge + reasons + '</td>' +
          '<td>' +
          '<button class="btn" data-gc-action="publish" data-id="' + esc(c.campaign_id) + '">Publish</button> ' +
          '<button class="btn" data-gc-action="pause" data-id="' + esc(c.campaign_id) + '">Pause</button> ' +
          '<button class="btn" data-gc-action="archive" data-id="' + esc(c.campaign_id) + '">Archive</button> ' +
          '<button class="btn" data-gc-action="preview" data-id="' + esc(c.campaign_id) + '">Preview</button> ' +
          '<button class="btn" data-gc-action="duplicate" data-id="' + esc(c.campaign_id) + '">Duplicate</button>' +
          (c.mechanic === "mission_pool"
            ? ' <button class="btn" data-gc-action="mission" data-id="' + esc(c.campaign_id) + '">Mission Ops</button>'
            : "") +
          '</td></tr>';
      }).join("");
      $("#gc-campaigns-body").innerHTML = '<table class="data-table"><thead><tr><th>Campaign</th><th>Type</th><th>Status</th><th>Visibility</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }).catch(function (e) { statePanel("gc-campaigns-body", "error", "Failed to load campaigns: " + e.message); });
  }

  // ---------- Mission Reward Pool (Phase 2 admin) ----------
  //
  // Lives inside the existing Campaign Centre / Player Campaigns surface —
  // no separate Mission admin app (§24). Every field maps onto the Phase 1
  // schema; every lifecycle action calls the official Phase 1 endpoint
  // (§30, §31). The UI never writes campaign state itself.

  // Which mission_config fields each mission_type actually supports (§28).
  var MISSION_TYPE_FIELDS = {
    multiple_choice: ["prompt", "options", "correct"],
    single_choice: ["prompt", "options", "correct"],
    keyword: ["prompt", "correct", "case_insensitive"],
    feedback: ["prompt", "min_chars", "max_chars"],
  };

  // mission_config fields Phase 1 freezes once any entry exists (§26).
  var MISSION_CONFIG_INPUT_IDS = [
    "gc-m-type", "gc-m-prompt", "gc-m-options", "gc-m-correct",
    "gc-m-case-insensitive", "gc-m-min-chars", "gc-m-max-chars",
  ];

  var gcMissionSelectedId = null;
  // Monotonic token for Mission Ops loads. Only the most recent load may
  // touch the shared form (see openMissionOps).
  var gcMissionLoadToken = 0;

  // ROOT-CAUSE GUARD. One form serves both "create a new campaign" and "edit
  // the selected one", and every wrong-campaign write found in review came
  // from those two intents being indistinguishable at save time. The mode is
  // now explicit, and each action is refused outright in the wrong mode, so
  // "Save mission changes" can never write a half-typed new campaign onto
  // the campaign that happens to still be selected.
  var gcMissionMode = "create";

  /**
   * Leave edit mode. Invalidates any in-flight Mission Ops load, drops the
   * edit target, and clears every piece of per-campaign state the shared
   * form was carrying (option labels, schedule instants) so none of it can
   * leak into the next create.
   */
  function gcEnterCreateMode() {
    gcMissionMode = "create";
    gcMissionSelectedId = null;
    gcMissionLoadToken++;

    var optionsInput = $("#gc-m-options");
    if (optionsInput) { optionsInput.dataset.labels = "{}"; optionsInput.dataset.labelsFor = ""; }
    ["gc-c-starts", "gc-c-ends"].forEach(function (id) {
      var node = $("#" + id);
      if (node) { node.dataset.originalIso = ""; node.dataset.hydratedValue = ""; }
    });
    // Nothing is frozen while creating.
    MISSION_CONFIG_INPUT_IDS.concat(["gc-c-starts", "gc-c-ends", "gc-c-id"]).forEach(function (id) {
      var node = $("#" + id);
      if (node) node.disabled = false;
    });
    var note = $("#gc-m-lock-note");
    if (note) { note.style.display = "none"; note.textContent = ""; }

    var saveBtn = $("#gc-save-mission-btn");
    if (saveBtn) saveBtn.style.display = "none";
    var createBtn = $("#gc-create-campaign-btn");
    if (createBtn) createBtn.style.display = "";
    var ops = $("#gc-mission-ops");
    if (ops) ops.style.display = "none";
  }

  function gcEnterEditMode(campaignId) {
    gcMissionMode = "edit";
    gcMissionSelectedId = campaignId;
    // The campaign being edited is fixed: retyping the id must not silently
    // retarget the save, and creating from a form loaded for editing is not
    // a thing an operator can do by accident any more.
    var idInput = $("#gc-c-id");
    if (idInput) idInput.disabled = true;
    var createBtn = $("#gc-create-campaign-btn");
    if (createBtn) createBtn.style.display = "none";
  }

  function gcMissionFieldWrap(id) {
    var node = $("#" + id);
    if (!node) return null;
    if (id === "gc-m-case-insensitive") return $("#gc-m-ci-wrap");
    return node;
  }

  function applyMissionTypeVisibility() {
    var type = ($("#gc-m-type") || {}).value || "multiple_choice";
    var allowed = MISSION_TYPE_FIELDS[type] || [];
    var map = {
      prompt: "gc-m-prompt", options: "gc-m-options", correct: "gc-m-correct",
      case_insensitive: "gc-m-case-insensitive", min_chars: "gc-m-min-chars", max_chars: "gc-m-max-chars",
    };
    Object.keys(map).forEach(function (key) {
      var node = gcMissionFieldWrap(map[key]);
      if (node) node.style.display = allowed.indexOf(key) === -1 ? "none" : "";
    });
  }

  function toggleMissionFields() {
    var isMission = (($("#gc-c-type") || {}).value === "mission_pool");
    var wrap = $("#gc-mission-fields");
    if (wrap) wrap.style.display = isMission ? "" : "none";
    // Mission Pool is answered inside the Mini App; it has no external
    // provider or destination path (campaign_centre._ALLOWED_OPEN_MODES_BY_TYPE).
    ["gc-c-provider", "gc-c-path"].forEach(function (id) {
      var node = $("#" + id);
      if (node) node.style.display = isMission ? "none" : "";
    });
    if (isMission) { applyMissionTypeVisibility(); loadMissionPools(); }
  }

  var gcMissionPoolsPromise = null;

  /**
   * The loader MUTATES the shared #gc-m-pool element, so it needs its own
   * guard rather than relying on the caller's stale() check: two Mission Ops
   * opens could each start a request, and a late one rewriting innerHTML
   * would reset the *other* campaign's hydrated pool selection to whatever
   * sorted first — and Save was already revealed. A single in-flight promise
   * is shared by all callers, so the list is written exactly once.
   */
  function loadMissionPools(force) {
    var sel = $("#gc-m-pool");
    if (!sel) return Promise.resolve();
    if (!force && sel.dataset.loaded === "1") return Promise.resolve();
    if (!force && gcMissionPoolsPromise) return gcMissionPoolsPromise;
    // The compatible-pool set is decided by the backend
    // (voucher_pool_service.CAMPAIGN_ALLOCATABLE_SCOPES minus the reserved
    // WELCOME/T1-T5/affiliate pools), never by a list hardcoded here (§29).
    gcMissionPoolsPromise = apiSoft("/api/admin/mission-pool/pools").then(function (r) {
      if (!r || r.status !== "ok") { sel.innerHTML = '<option value="">Failed to load pools</option>'; return; }
      var pools = r.pools || [];
      sel.dataset.loaded = "1";
      sel.innerHTML = pools.length
        ? pools.map(function (p) {
            // The pool's REAL pool_type rides on the option. The processor
            // passes mission_pool.pool_type to voucher_pool_service
            // .allocate_voucher as expected_pool_type, which filters the
            // inventory row on it — so storing a hardcoded "voucher_drop"
            // for a pool registered as tournament_reward/vip would make
            // every allocation miss and mark winners out_of_stock, while
            // the UI still showed stock available.
            return '<option value="' + esc(p.pool_id) + '" data-pool-type="' + esc(p.pool_type || "") + '">' +
              esc(p.pool_id) + " — " + esc(p.name || "") +
              " [" + esc(p.pool_type || "?") + "] (" + ((p.stock || {}).available || 0) + " available)</option>";
          }).join("")
        : '<option value="">No mission-compatible pools — register one in Voucher Centre</option>';
    }).catch(function () {
      sel.innerHTML = '<option value="">Failed to load pools</option>';
    }).then(function () {
      gcMissionPoolsPromise = null;
    });
    return gcMissionPoolsPromise;
  }

  /**
   * Make sure the campaign's stored pool is selectable even if the backend
   * no longer lists it (its scope changed, it was deactivated). Silently
   * falling back to whatever option happens to be first would rewrite the
   * campaign's pool on the next save.
   */
  function ensurePoolOption(poolId, poolType) {
    var sel = $("#gc-m-pool");
    if (!sel || !poolId) return;
    for (var i = 0; i < sel.options.length; i++) {
      if (sel.options[i].value === poolId) { sel.value = poolId; return; }
    }
    var opt = document.createElement("option");
    opt.value = poolId;
    opt.textContent = poolId + " — (not currently listed as mission-compatible)";
    opt.setAttribute("data-pool-type", poolType || "");
    sel.appendChild(opt);
    sel.value = poolId;
  }

  /**
   * Hydrated option labels, valid ONLY while editing the campaign they came
   * from. Without the ownership stamp the map would survive into the create
   * flow (and into a different campaign), where reusing an option id would
   * silently inherit the previous campaign's participant-facing label
   * instead of the documented id-as-label fallback.
   */
  function missionOptionLabels() {
    var input = $("#gc-m-options");
    var data = (input || {}).dataset || {};
    if (!gcMissionSelectedId || data.labelsFor !== gcMissionSelectedId) return {};
    try { return JSON.parse(data.labels || "{}"); } catch (e) { return {}; }
  }

  function missionConfigFromForm() {
    var type = ($("#gc-m-type") || {}).value || "multiple_choice";
    var cfg = { mission_type: type, prompt: ($("#gc-m-prompt").value || "").trim() };
    if (type === "multiple_choice" || type === "single_choice") {
      // ONE OPTION PER LINE. A comma-separated field cannot round-trip an
      // option id containing a comma, which validate_mission_config accepts:
      // {"id": "red,blue"} would be split into "red" and "blue", silently
      // rewriting the mission (or failing validation when it is the correct
      // answer). Newlines are not a legal id character in practice and are
      // rejected below if one somehow appears.
      var knownLabels = missionOptionLabels();
      cfg.options = ($("#gc-m-options").value || "").split("\n")
        .map(function (o) { return o.trim(); })
        .filter(function (o) { return o.length; })
        .map(function (o) { return { id: o, label: knownLabels[o] || o }; });
      cfg.correct_answer = ($("#gc-m-correct").value || "").trim();
    } else if (type === "keyword") {
      cfg.correct_answer = ($("#gc-m-correct").value || "").trim();
      cfg.keyword_case_insensitive = !!($("#gc-m-case-insensitive") || {}).checked;
    } else {
      cfg.min_chars = parseInt($("#gc-m-min-chars").value, 10) || 1;
      cfg.max_chars = parseInt($("#gc-m-max-chars").value, 10) || 500;
    }
    return cfg;
  }

  function selectedPoolType() {
    var sel = $("#gc-m-pool");
    var opt = sel && sel.options[sel.selectedIndex];
    // Fall back to voucher_drop only when nothing is selected — never
    // override a pool's real registered type (see loadMissionPools).
    return (opt && opt.getAttribute("data-pool-type")) || "voucher_drop";
  }

  function missionPoolFromForm() {
    return {
      pool_id: ($("#gc-m-pool") || {}).value || "",
      pool_type: selectedPoolType(),
      winner_count: parseInt($("#gc-m-winners").value, 10) || 0,
      allocation_method: ($("#gc-m-allocation") || {}).value || "random_qualified",
      eligibility_policy: {
        require_correct_answer: !!$("#gc-m-el-correct").checked,
        exclude_voucher_hunter: !!$("#gc-m-el-hunter").checked,
        exclude_multi_account_risk: !!$("#gc-m-el-multi").checked,
        exclude_blocked: !!$("#gc-m-el-blocked").checked,
        require_gaming_account: !!$("#gc-m-el-gaming").checked,
      },
    };
  }

  /**
   * §26: once Phase 1 has frozen mission_config (any entry exists), the
   * fields are disabled PROACTIVELY with an explanation, instead of letting
   * an operator type a change and then surfacing the backend's 409.
   * Schedule fields are reported separately because Phase 1 freezes
   * mission_config only — it has no freeze rule for `schedule` (§27).
   */
  function applyMissionFreeze(state) {
    var locked = !!(state && state.mission_config_locked);
    MISSION_CONFIG_INPUT_IDS.forEach(function (id) {
      var node = $("#" + id);
      if (node) node.disabled = locked;
    });
    var note = $("#gc-m-lock-note");
    if (note) {
      note.style.display = locked ? "" : "none";
      note.textContent = locked
        ? "Mission details can no longer be edited because participants have already submitted entries. (" +
          ((state && state.entries) || 0) + " entries)"
        : "";
    }
    var schedLocked = !(state && state.schedule_editable);
    ["gc-c-starts", "gc-c-ends"].forEach(function (id) {
      var node = $("#" + id);
      if (node) node.disabled = schedLocked;
    });
  }

  // Only the actions valid for the current backend state are offered (§30).
  function missionActionsFor(state) {
    var status = state.campaign_status;
    var out = [];
    if (state.cancelled) { out.push(["resume", "Resume (undo cancel)"]); return out; }
    if (status === "draft" || status === "scheduled") out.push(["publish", "Publish"]);
    if (status === "live") { out.push(["pause", "Pause"]); out.push(["close", "Close Mission"]); out.push(["cancel", "Cancel Mission"]); }
    if (status === "paused") { out.push(["publish", "Resume"]); out.push(["cancel", "Cancel Mission"]); }
    if (status === "ended" || status === "archived") {
      out.push([state.processing_stage === "completed" ? "summary" : "process",
                state.processing_stage === "completed" ? "View Summary"
                  : (state.processing_stage === "pending" ? "Process Campaign" : "Resume Processing")]);
    }
    return out;
  }

  function renderMissionOps(state, summary) {
    var wrap = $("#gc-mission-ops");
    if (!wrap) return;
    wrap.style.display = "";
    $("#gc-mo-name").textContent = state.campaign_id;

    var linkEl = $("#gc-mo-link");
    if (state.mission_link) {
      linkEl.innerHTML = 'Mission Link: <code>' + esc(state.mission_link) + '</code> ' +
        '<button class="btn" id="gc-mo-copy-link">Copy Link</button>';
      var copyBtn = $("#gc-mo-copy-link");
      if (copyBtn) copyBtn.addEventListener("click", function () {
        try { navigator.clipboard.writeText(state.mission_link); toast("✅ Mission link copied", "success"); } catch (e) {}
      });
    } else {
      linkEl.textContent = "Mission Link unavailable (" + (state.mission_link_unavailable_reason || "unknown") + ")";
    }

    $("#gc-mo-actions").innerHTML = missionActionsFor(state).map(function (a) {
      return '<button class="btn" data-mission-action="' + a[0] + '" data-id="' + esc(state.campaign_id) + '">' + esc(a[1]) + "</button>";
    }).join(" ");

    var g = (summary && summary.grains) || {};
    var reasons = (summary && summary.disqualification_reasons) || {};
    var REASON_LABELS = {
      duplicate_identity: "Duplicate identity", duplicate_gaming_account: "Duplicate gaming account",
      voucher_hunter: "Voucher hunter", multi_account_risk: "Multi-account risk",
      incorrect_answer: "Incorrect answer", missing_gaming_account: "Missing gaming account",
      blocked: "Blocked", already_rewarded: "Already rewarded", invalid_submission: "Invalid submission",
      campaign_cancelled: "Campaign cancelled", submitted_after_close: "Submitted after close",
      out_of_stock: "Out of stock", other: "Other",
    };
    var rows = [
      ["Campaign Status", state.campaign_status + (state.cancelled ? " (cancelled)" : "")],
      ["Processing stage", (summary && summary.processing_stage) || state.processing_stage],
      ["Close cutoff (closed_at)", state.closed_at || "—"],
      ["Submissions", g.submissions_telegram_user_grain],
      ["Deduplicated identities", g.deduplicated_identity_grain],
      ["Qualified", g.qualified_identity_grain],
      ["Disqualified", g.disqualified_telegram_user_grain],
      ["Winner target", summary && summary.winner_count_requested],
      ["Winners selected", g.winners_identity_grain],
      ["Rewards allocated", g.rewards_allocated_voucher_grain],
      ["Notifications sent", g.notifications_sent_voucher_grain],
      ["Notification failures", g.notifications_failed_voucher_grain],
    ].map(function (r) {
      return "<tr><td>" + esc(r[0]) + "</td><td>" + esc(r[1] == null ? "—" : String(r[1])) + "</td></tr>";
    }).join("");
    var reasonRows = Object.keys(reasons).map(function (k) {
      return "<tr><td>" + esc(REASON_LABELS[k] || k) + "</td><td>" + esc(String(reasons[k])) + "</td></tr>";
    }).join("");
    $("#gc-mo-summary").innerHTML =
      '<table class="data-table"><tbody>' + rows + "</tbody></table>" +
      (reasonRows ? '<div class="sub" style="margin-top:8px;">Disqualification reasons (admin only)</div>' +
        '<table class="data-table"><tbody>' + reasonRows + "</tbody></table>" : "");
  }

  // api()/apiPost() reject on a non-2xx, so every mission call is caught:
  // an operator must never be left with a stale panel and no explanation.
  function apiSoft(path) {
    return api(path).catch(function (e) { return { status: "error", code: e.message }; });
  }

  // A datetime-local input wants "YYYY-MM-DDTHH:mm[:ss]" in LOCAL time; the
  // API returns ISO-8601. Converting through the epoch keeps the displayed
  // instant identical to the stored one.
  //
  // SECONDS ARE INCLUDED (with step="1" on the inputs) rather than truncated
  // to the minute. The save handler resends the schedule whenever scheduling
  // is editable, so a minute-precision round trip would silently move an
  // API-created boundary back by up to 59s — and `schedule.ends_at` is one of
  // Phase 1's two eligibility cutoffs (mission_pool.close_cutoff), so that
  // could start a mission early or drop valid submissions near its end.
  function isoToLocalInput(iso) {
    if (!iso) return "";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return "";
    var pad = function (n) { return (n < 10 ? "0" : "") + n; };
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) +
      "T" + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
  }

  /**
   * Hydrate a schedule input, remembering both the exact instant the server
   * gave us and the value we displayed. On save, an untouched field resends
   * the ORIGINAL ISO string verbatim, so no conversion can perturb it even
   * by a millisecond; an edited field is converted from what the operator
   * actually typed.
   */
  function setScheduleValue(id, iso) {
    var node = $("#" + id);
    if (!node) return;
    var shown = isoToLocalInput(iso);
    node.value = shown;
    node.dataset.originalIso = iso || "";
    node.dataset.hydratedValue = shown;
  }

  function scheduleValueForSave(id) {
    var node = $("#" + id);
    if (!node) return null;
    var current = node.value || "";
    if (!current) return null;
    if (node.dataset.hydratedValue === current && node.dataset.originalIso) {
      return node.dataset.originalIso;
    }
    var d = new Date(current);
    return isNaN(d.getTime()) ? null : d.toISOString();
  }

  function setValue(id, value) {
    var node = $("#" + id);
    if (node) node.value = value == null ? "" : String(value);
  }

  function setChecked(id, value) {
    var node = $("#" + id);
    if (node) node.checked = !!value;
  }

  /**
   * Load the selected campaign's ACTUAL stored values into the shared
   * create/edit form.
   *
   * Without this the form still holds whatever the create flow last left in
   * it — blank after a page reload, or another campaign's values after
   * switching — and Save would PUT those over the selected campaign's real
   * operator settings (or fail validation with an empty pool_id). Save is
   * therefore only exposed once hydration has actually succeeded.
   */
  function hydrateMissionForm(campaign) {
    var cfg = campaign.mission_config || {};
    var block = campaign.mission_pool || {};
    var schedule = campaign.schedule || {};
    var policy = block.eligibility_policy || {};

    setValue("gc-c-id", campaign.campaign_id);
    setValue("gc-c-name", campaign.name);
    var typeSel = $("#gc-c-type");
    if (typeSel) typeSel.value = "mission_pool";
    toggleMissionFields();

    setValue("gc-m-type", cfg.mission_type || "multiple_choice");
    setValue("gc-m-prompt", cfg.prompt);
    // The options field edits IDs. Hydrating ids-only and rebuilding
    // {id, label} from them on save would silently overwrite every
    // participant-facing label with its id for a campaign created through
    // the API with labels != ids. Remember the id->label map so an option
    // the operator did not touch keeps the label it already had.
    var hydratedOptions = (cfg.options || []);
    var optionsInput = $("#gc-m-options");
    if (optionsInput) {
      optionsInput.value = hydratedOptions.map(function (o) { return o.id; }).join("\n");
      var labelMap = {};
      hydratedOptions.forEach(function (o) { if (o && o.id) labelMap[o.id] = o.label || o.id; });
      try { optionsInput.dataset.labels = JSON.stringify(labelMap); } catch (e) { optionsInput.dataset.labels = "{}"; }
      // Stamped with its owner so it can never be applied to another
      // campaign or to a newly created one.
      optionsInput.dataset.labelsFor = campaign.campaign_id || "";
    }
    setValue("gc-m-correct", cfg.correct_answer);
    setChecked("gc-m-case-insensitive", cfg.keyword_case_insensitive !== false);
    setValue("gc-m-min-chars", cfg.min_chars);
    setValue("gc-m-max-chars", cfg.max_chars);
    applyMissionTypeVisibility();

    setValue("gc-m-winners", block.winner_count);
    setValue("gc-m-allocation", block.allocation_method || "random_qualified");
    ensurePoolOption(block.pool_id, block.pool_type);

    setChecked("gc-m-el-correct", policy.require_correct_answer !== false);
    setChecked("gc-m-el-hunter", policy.exclude_voucher_hunter !== false);
    setChecked("gc-m-el-multi", policy.exclude_multi_account_risk !== false);
    setChecked("gc-m-el-blocked", policy.exclude_blocked !== false);
    setChecked("gc-m-el-gaming", !!policy.require_gaming_account);

    setScheduleValue("gc-c-starts", schedule.starts_at);
    setScheduleValue("gc-c-ends", schedule.ends_at);
  }

  function openMissionOps(campaignId) {
    gcEnterEditMode(campaignId);
    // Requests are not cancellable here, so guard on arrival instead: an
    // earlier campaign's responses can land AFTER a later campaign's. Without
    // this, opening A then B could hydrate the shared form with A's values
    // while gcMissionSelectedId is B — and Save would then PUT A's config
    // onto B. Both the token and the selected id are checked, so a stale
    // load can neither hydrate nor reveal Save.
    var token = ++gcMissionLoadToken;
    var stale = function () {
      return token !== gcMissionLoadToken || gcMissionSelectedId !== campaignId;
    };

    var saveBtn = $("#gc-save-mission-btn");
    // Hide Save until the form actually holds this campaign's values.
    if (saveBtn) saveBtn.style.display = "none";
    // Pools must be listed before the stored pool_id can be selected.
    loadMissionPools().then(function () {
      if (stale()) return null;
      return Promise.all([
        apiSoft("/api/admin/mission-pool/" + encodeURIComponent(campaignId) + "/edit-state"),
        apiSoft("/api/admin/mission-pool/" + encodeURIComponent(campaignId) + "/summary"),
        apiSoft("/api/admin/gc-campaigns/" + encodeURIComponent(campaignId)),
      ]);
    }).then(function (res) {
      if (!res || stale()) return;
      var state = res[0] || {};
      if (state.status !== "ok") { toast("❌ " + (state.code || "not_a_mission_campaign"), "error"); return; }
      var campaignResp = res[2] || {};
      if (campaignResp.status !== "ok" || !campaignResp.campaign) {
        // Render the read-only ops panel, but never enable a Save that would
        // write unhydrated values.
        applyMissionFreeze(state);
        renderMissionOps(state, (res[1] && res[1].status === "ok") ? res[1] : {});
        toast("⚠️ Could not load campaign values — editing disabled", "error");
        return;
      }
      hydrateMissionForm(campaignResp.campaign);
      // applyMissionFreeze runs AFTER hydration: hydration re-enables fields
      // via toggleMissionFields, and the freeze must win.
      applyMissionFreeze(state);
      renderMissionOps(state, (res[1] && res[1].status === "ok") ? res[1] : {});
      // Re-checked immediately before revealing Save: everything above is
      // synchronous, but this is the invariant that actually matters.
      if (stale()) return;
      if (saveBtn) saveBtn.style.display = "";
    });
  }

  // Close and Cancel are irreversible-in-effect, so both confirm first and
  // both go through the official Phase 1 endpoints. Close is idempotent
  // server-side and never moves closed_at on a repeat (§31).
  var MISSION_CONFIRM = {
    close: "Close this mission now?\n\nNew valid entries after the close cutoff will not be eligible for rewards.",
    cancel: "Cancel this mission?\n\nNew submissions and new reward distribution will stop.\n\nRewards already allocated to winners will remain valid.",
  };

  function runMissionAction(action, id) {
    if (MISSION_CONFIRM[action] && !window.confirm(MISSION_CONFIRM[action])) return;
    if (action === "summary") { openMissionOps(id); return; }
    var path = (action === "publish" || action === "pause")
      ? "/api/admin/gc-campaigns/" + encodeURIComponent(id) + "/" + action
      // close / cancel / resume / process -> Phase 1 mission endpoints only.
      // The UI never writes campaign status directly (§31).
      : "/api/admin/mission-pool/" + encodeURIComponent(id) + "/" + action;
    apiPost(path)
      .catch(function (e) { return { status: "error", code: e.message }; })
      .then(function (r) {
        if (!r || r.status !== "ok") { toast("❌ " + ((r && r.code) || "action_failed"), "error"); }
        else { toast("✅ " + action + " ok", "success"); }
        loadGcCampaigns(true); openMissionOps(id);
      });
  }

  function bindGcCampaigns() {
    var newBtn = $("#gc-new-campaign-btn");
    if (newBtn) newBtn.addEventListener("click", function () {
      gcEnterCreateMode();
      ["gc-c-id", "gc-c-name", "gc-m-prompt", "gc-m-options", "gc-m-correct",
        "gc-c-starts", "gc-c-ends"].forEach(function (id) {
        var node = $("#" + id);
        if (node) node.value = "";
      });
      toggleMissionFields();
    });

    var typeSel = $("#gc-c-type");
    if (typeSel) typeSel.addEventListener("change", function () {
      // Switching campaign type is a create-intent action; it must not leave
      // a stale edit target armed behind a still-visible Save button.
      if (gcMissionMode === "edit") gcEnterCreateMode();
      toggleMissionFields();
    });
    var missionTypeSel = $("#gc-m-type");
    if (missionTypeSel) missionTypeSel.addEventListener("change", applyMissionTypeVisibility);
    toggleMissionFields();

    var saveBtn = $("#gc-save-mission-btn");
    if (saveBtn) {
      saveBtn.addEventListener("click", function () {
        // Belt and braces: the button is hidden outside edit mode, but the
        // handler refuses anyway rather than trusting UI state.
        if (gcMissionMode !== "edit" || !gcMissionSelectedId) {
          toast("❌ No mission selected for editing", "error");
          return;
        }
        var body = { mission_pool: missionPoolFromForm() };
        // A frozen mission_config is simply not sent, so an unchanged PUT
        // never trips the backend freeze check.
        if (!$("#gc-m-prompt").disabled) body.mission_config = missionConfigFromForm();
        // The schedule inputs are enabled whenever the backend reports the
        // schedule editable, so the PUT has to carry them — otherwise an
        // operator changes a date, sees "Mission saved", and nothing is
        // persisted (campaign_centre._validate_body leaves `schedule`
        // untouched on a partial update that omits it).
        if (!$("#gc-c-starts").disabled) {
          body.schedule = {
            starts_at: scheduleValueForSave("gc-c-starts"),
            ends_at: scheduleValueForSave("gc-c-ends"),
          };
        }
        apiPutJson("/api/admin/gc-campaigns/" + encodeURIComponent(gcMissionSelectedId), body).then(function (res) {
          var d = res.d || res;
          if (d.status !== "ok") {
            toast(d.code === "mission_config_locked"
              ? "❌ Mission details are frozen — participants have already submitted entries."
              : "❌ " + (d.code || "save_failed"), "error");
            return;
          }
          toast("✅ Mission saved", "success");
          openMissionOps(gcMissionSelectedId);
        });
      });
    }

    var createBtn = $("#gc-create-campaign-btn");
    if (createBtn) {
      createBtn.addEventListener("click", function () {
        if (gcMissionMode === "edit") {
          toast("❌ Editing a mission — use “New campaign” first", "error");
          return;
        }
        var type = $("#gc-c-type").value;
        var body = {
          campaign_id: ($("#gc-c-id").value || "").trim(),
          name: ($("#gc-c-name").value || "").trim(),
          type: type,
          schedule: {
            starts_at: $("#gc-c-starts").value ? new Date($("#gc-c-starts").value).toISOString() : null,
            ends_at: $("#gc-c-ends").value ? new Date($("#gc-c-ends").value).toISOString() : null,
          },
          telegram: { channel_username: ($("#gc-c-channel").value || "").trim() },
          destination: { provider_id: ($("#gc-c-provider").value || "").trim(), path: ($("#gc-c-path").value || "").trim(), open_mode: "telegram_web_app", ready: false },
        };
        if (type === "mission_pool") {
          body.mission_config = missionConfigFromForm();
          body.mission_pool = missionPoolFromForm();
          // Mission Pool has no external destination; campaign_centre only
          // allows telegram_web_app for this type.
          body.destination = { open_mode: "telegram_web_app", ready: false };
        }
        apiPostJson("/api/admin/gc-campaigns", body).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "create_failed"), "error"); return; }
          toast("✅ Campaign created as draft", "success");
          loadGcCampaigns(true);
          if (type === "mission_pool") openMissionOps(body.campaign_id);
          else gcEnterCreateMode();
        });
      });
    }
    document.addEventListener("click", function (e) {
      var missionBtn = e.target && e.target.closest && e.target.closest("[data-mission-action]");
      if (missionBtn) { runMissionAction(missionBtn.dataset.missionAction, missionBtn.dataset.id); return; }
      var btn = e.target && e.target.closest && e.target.closest("[data-gc-action]");
      if (!btn) return;
      var action = btn.dataset.gcAction, id = btn.dataset.id;
      if (action === "publish") apiPost("/api/admin/gc-campaigns/" + id + "/publish").then(function (r) { if (r.status !== "ok") toast("❌ " + r.code, "error"); loadGcCampaigns(true); });
      else if (action === "pause") apiPost("/api/admin/gc-campaigns/" + id + "/pause").then(function () { loadGcCampaigns(true); });
      else if (action === "archive") apiPost("/api/admin/gc-campaigns/" + id + "/archive").then(function () { loadGcCampaigns(true); });
      else if (action === "mission") openMissionOps(id);
      else if (action === "duplicate") apiPost("/api/admin/gc-campaigns/" + id + "/duplicate").then(function (r) {
        if (!r || r.status !== "ok") { toast("❌ " + ((r && r.code) || "duplicate_failed"), "error"); return; }
        toast("✅ Duplicated as draft " + r.campaign_id, "success");
        loadGcCampaigns(true);
      });
      else if (action === "preview") api("/api/admin/gc-campaigns/" + id + "/preview").then(function (r) {
        alert("Card: " + JSON.stringify(r.card, null, 2) + "\n\nBadges: " + (r.admin_badges || []).join(", ") + "\n\nVisibility: " + JSON.stringify(r.effective_visibility));
      });
    });
  }

  // ---------- Event Banner (image-only Mini App top banner, event_banner.py) ----------

  function loadEventBanners(force) {
    statePanel("eb-body", "loading", "Loading event banners…");
    api("/api/admin/event-banners").then(function (data) {
      var items = data.banners || [];
      if (!items.length) { $("#eb-body").innerHTML = emptyState("No event banners yet — create one above."); return; }
      window.__ebBannersById = {};
      var rows = items.map(function (b) {
        window.__ebBannersById[b.event_id] = b;
        var regions = (b.regions || []).length ? esc((b.regions || []).join(", ")) : "All regions";
        return '<tr><td>' + esc(b.event_id) + '<div class="sub">' + esc(b.alt_text || "") + '</div></td>' +
          '<td><img src="' + esc(b.image_url || "") + '" alt="" style="max-width:80px;max-height:36px;object-fit:contain;" /></td>' +
          '<td>' + gcPill(b.effective_status || b.status) + '</td>' +
          '<td>' + esc(b.priority != null ? String(b.priority) : "") + '</td>' +
          '<td class="sub">' + esc((b.starts_at || "").slice(0, 16).replace("T", " ")) + ' → ' + esc((b.ends_at || "").slice(0, 16).replace("T", " ")) + '</td>' +
          '<td class="sub">' + regions + '</td>' +
          '<td>' +
          '<button class="btn" data-eb-action="toggle" data-id="' + esc(b.event_id) + '" data-status="' + esc(b.status) + '">' + (b.status === "active" ? "Deactivate" : "Activate") + '</button> ' +
          '<button class="btn" data-eb-action="edit-schedule" data-id="' + esc(b.event_id) + '">Edit Schedule</button> ' +
          '<button class="btn" data-eb-action="delete" data-id="' + esc(b.event_id) + '">Delete</button>' +
          '</td></tr>';
      }).join("");
      $("#eb-body").innerHTML = '<table class="data-table"><thead><tr><th>Event</th><th>Image</th><th>Status</th><th>Priority</th><th>Window (UTC)</th><th>Regions</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }).catch(function (e) { statePanel("eb-body", "error", "Failed to load event banners: " + e.message); });
  }

  // Kuala Lumpur is a fixed UTC+8 offset (no DST) — a plain string offset
  // suffix on parse/format is enough, no timezone library needed.
  function utcIsoToKlInputValue(iso) {
    if (!iso) return "";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return "";
    var kl = new Date(d.getTime() + 8 * 60 * 60 * 1000);
    return kl.toISOString().slice(0, 16);
  }

  function klInputValueToUtcIso(value) {
    if (!value) return null;
    var d = new Date(value + ":00+08:00");
    if (isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  function openEditScheduleModal(banner) {
    var overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML =
      '<div class="modal-box">' +
      '<h3>Edit Schedule — ' + esc(banner.event_id) + '</h3>' +
      '<p class="sub">Times are in Kuala Lumpur time (GMT+8). Stored as UTC.</p>' +
      '<label class="sub">Start</label>' +
      '<input class="filter-input" id="eb-edit-starts" type="datetime-local" />' +
      '<label class="sub">End</label>' +
      '<input class="filter-input" id="eb-edit-ends" type="datetime-local" />' +
      '<label class="sub">Priority</label>' +
      '<input class="filter-input" id="eb-edit-priority" type="number" />' +
      '<div class="modal-actions">' +
      '<button class="btn" id="eb-edit-cancel">Cancel</button>' +
      '<button class="btn primary" id="eb-edit-save">Save</button>' +
      '</div></div>';
    document.body.appendChild(overlay);
    $("#eb-edit-starts", overlay).value = utcIsoToKlInputValue(banner.starts_at);
    $("#eb-edit-ends", overlay).value = utcIsoToKlInputValue(banner.ends_at);
    $("#eb-edit-priority", overlay).value = banner.priority != null ? banner.priority : 0;

    function close() { overlay.remove(); }
    overlay.querySelector("#eb-edit-cancel").addEventListener("click", close);
    overlay.addEventListener("click", function (e) { if (e.target === overlay) close(); });

    overlay.querySelector("#eb-edit-save").addEventListener("click", function () {
      submitEditSchedule(banner, overlay, false);
    });
  }

  function submitEditSchedule(banner, overlay, confirmed) {
    var startsVal = overlay.querySelector("#eb-edit-starts").value;
    var endsVal = overlay.querySelector("#eb-edit-ends").value;
    var priorityVal = overlay.querySelector("#eb-edit-priority").value;
    var starts_at = klInputValueToUtcIso(startsVal);
    var ends_at = klInputValueToUtcIso(endsVal);
    if (!starts_at || !ends_at) { toast("❌ Start and end are required", "error"); return; }
    if (new Date(ends_at) <= new Date(starts_at)) { toast("❌ End must be after start", "error"); return; }

    var body = { starts_at: starts_at, ends_at: ends_at, priority: Number(priorityVal || 0) };
    if (confirmed) body.confirm = true;

    apiPatchJson("/api/admin/event-banners/" + encodeURIComponent(banner.event_id) + "/schedule", body)
      .then(function (res) {
        if (!res.ok || res.d.status !== "ok") {
          if (res.d && res.d.code === "confirmation_required") {
            confirmSimple(
              "Banner is currently active",
              "\"" + banner.event_id + "\" is active and may be live for users right now. Save the new schedule anyway?"
            ).then(function (ok) {
              if (ok) submitEditSchedule(banner, overlay, true);
            });
            return;
          }
          toast("❌ " + (res.d && res.d.code || "update_failed"), "error");
          return;
        }
        toast("✅ Schedule updated", "success");
        overlay.remove();
        loadEventBanners(true);
      });
  }

  function bindEventBanners() {
    var createBtn = $("#eb-create-btn");
    if (createBtn) {
      createBtn.addEventListener("click", function () {
        var regionsRaw = ($("#eb-regions").value || "").trim();
        var body = {
          event_id: ($("#eb-event-id").value || "").trim(),
          image_url: ($("#eb-image-url").value || "").trim(),
          destination_url: ($("#eb-destination-url").value || "").trim(),
          alt_text: ($("#eb-alt-text").value || "").trim(),
          starts_at: $("#eb-starts").value ? new Date($("#eb-starts").value).toISOString() : null,
          ends_at: $("#eb-ends").value ? new Date($("#eb-ends").value).toISOString() : null,
          priority: Number($("#eb-priority").value || 100),
          status: $("#eb-status").value,
          regions: regionsRaw ? regionsRaw.split(",").map(function (r) { return r.trim(); }).filter(Boolean) : [],
        };
        apiPostJson("/api/admin/event-banners", body).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "create_failed"), "error"); return; }
          toast("✅ Event banner created", "success");
          $("#eb-event-id").value = ""; $("#eb-image-url").value = ""; $("#eb-destination-url").value = "";
          $("#eb-alt-text").value = ""; $("#eb-regions").value = "";
          loadEventBanners(true);
        });
      });
    }
    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-eb-action]");
      if (!btn) return;
      var action = btn.dataset.ebAction, id = btn.dataset.id;
      if (action === "toggle") {
        var nextStatus = btn.dataset.status === "active" ? "inactive" : "active";
        apiPatchJson("/api/admin/event-banners/" + encodeURIComponent(id), { status: nextStatus }).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "update_failed"), "error"); return; }
          loadEventBanners(true);
        });
      } else if (action === "edit-schedule") {
        var banner = (window.__ebBannersById || {})[id];
        if (!banner) return;
        openEditScheduleModal(banner);
      } else if (action === "delete") {
        if (!confirm("Delete event banner \"" + id + "\"?")) return;
        apiDelete("/api/admin/event-banners/" + encodeURIComponent(id)).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "delete_failed"), "error"); return; }
          toast("✅ Event banner deleted", "success");
          loadEventBanners(true);
        });
      }
    });
  }

  // ---------- Lucky Games (admin-managed Mini App game cards, lucky_games.py) ----------

  var LG_VOLATILITY_OPTIONS = ["Low", "Low-Med", "Medium", "High-Med", "High"];
  var lgEditingId = null; // null = create mode; a game id = edit mode. Never both.
  window.__lgGamesById = {};

  function lgFormEl(id) { return document.getElementById(id); }

  function lgReadForm() {
    return {
      name: (lgFormEl("lg-name").value || "").trim(),
      label: (lgFormEl("lg-label").value || "").trim(),
      provider: (lgFormEl("lg-provider").value || "").trim(),
      volatility: lgFormEl("lg-volatility").value,
      max_win: (lgFormEl("lg-max-win").value || "").trim(),
      image_url: (lgFormEl("lg-image-url").value || "").trim(),
      game_url: (lgFormEl("lg-game-url").value || "").trim(),
      sort_order: Number(lgFormEl("lg-sort-order").value || 0),
      is_published: !!lgFormEl("lg-is-published").checked,
    };
  }

  function lgClearForm() {
    ["lg-name", "lg-label", "lg-provider", "lg-max-win", "lg-image-url", "lg-game-url"].forEach(function (id) {
      var el = lgFormEl(id); if (el) el.value = "";
    });
    var sortEl = lgFormEl("lg-sort-order"); if (sortEl) sortEl.value = "0";
    var volEl = lgFormEl("lg-volatility"); if (volEl) volEl.value = "Medium";
    var pubEl = lgFormEl("lg-is-published"); if (pubEl) pubEl.checked = false;
  }

  function lgEnterCreateMode() {
    lgEditingId = null;
    lgClearForm();
    var submitBtn = lgFormEl("lg-submit-btn");
    if (submitBtn) submitBtn.textContent = "Add game";
    var cancelBtn = lgFormEl("lg-cancel-edit-btn");
    if (cancelBtn) cancelBtn.style.display = "none";
    var modeLabel = lgFormEl("lg-form-mode");
    if (modeLabel) modeLabel.textContent = "Create new game";
  }

  function lgEnterEditMode(game) {
    // Explicit, mutually-exclusive edit mode: capture the target game's id
    // once here and never re-derive it from form contents, so editing one
    // game (even after changing its name) can never be submitted as a
    // create or silently overwrite a different game.
    lgEditingId = game.id;
    lgFormEl("lg-name").value = game.name || "";
    lgFormEl("lg-label").value = game.label || "";
    lgFormEl("lg-provider").value = game.provider || "";
    lgFormEl("lg-volatility").value = game.volatility || "Medium";
    lgFormEl("lg-max-win").value = game.max_win || "";
    lgFormEl("lg-image-url").value = game.image_url || "";
    lgFormEl("lg-game-url").value = game.game_url || "";
    lgFormEl("lg-sort-order").value = game.sort_order != null ? game.sort_order : 0;
    lgFormEl("lg-is-published").checked = !!game.is_published;
    var submitBtn = lgFormEl("lg-submit-btn");
    if (submitBtn) submitBtn.textContent = "Save changes";
    var cancelBtn = lgFormEl("lg-cancel-edit-btn");
    if (cancelBtn) cancelBtn.style.display = "inline-block";
    var modeLabel = lgFormEl("lg-form-mode");
    if (modeLabel) modeLabel.textContent = "Editing “" + (game.name || "") + "”";
    var section = document.getElementById("view-luckyGames");
    if (section && section.scrollIntoView) section.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  function loadLuckyGames(force) {
    statePanel("lg-body", "loading", "Loading lucky games…");
    api("/api/admin/lucky-games").then(function (data) {
      var items = data.games || [];
      window.__lgGamesById = {};
      if (!items.length) { $("#lg-body").innerHTML = emptyState("No lucky games yet — add one above."); return; }
      var rows = items.map(function (g) {
        window.__lgGamesById[g.id] = g;
        return '<tr><td>' + esc(g.name) + '<div class="sub">' + esc(g.label || "") + (g.provider ? " · " + esc(g.provider) : "") + '</div></td>' +
          '<td>' + (g.image_url ? '<img src="' + esc(g.image_url) + '" alt="" style="max-width:60px;max-height:36px;object-fit:contain;" />' : '<span class="sub">—</span>') + '</td>' +
          '<td>' + esc(g.volatility || "") + '</td>' +
          '<td>' + esc(g.max_win || "") + '</td>' +
          '<td>' + esc(g.sort_order != null ? String(g.sort_order) : "0") + '</td>' +
          '<td>' + gcPill(g.is_published ? "active" : "inactive") + '</td>' +
          '<td>' +
          '<button class="btn" data-lg-action="edit" data-id="' + esc(g.id) + '">Edit</button> ' +
          '<button class="btn" data-lg-action="toggle" data-id="' + esc(g.id) + '" data-published="' + (g.is_published ? "1" : "0") + '">' + (g.is_published ? "Unpublish" : "Publish") + '</button> ' +
          '<button class="btn" data-lg-action="delete" data-id="' + esc(g.id) + '">Delete</button>' +
          '</td></tr>';
      }).join("");
      $("#lg-body").innerHTML = '<table class="data-table"><thead><tr><th>Game</th><th>Image</th><th>Volatility</th><th>Max Win</th><th>Order</th><th>Status</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
      // An edit in progress can be invalidated by a concurrent delete
      // elsewhere; drop back to create mode rather than submitting a PATCH
      // against a game_id that no longer exists.
      if (lgEditingId && !window.__lgGamesById[lgEditingId]) lgEnterCreateMode();
    }).catch(function (e) { statePanel("lg-body", "error", "Failed to load lucky games: " + e.message); });
  }

  function bindLuckyGames() {
    lgEnterCreateMode();

    var submitBtn = lgFormEl("lg-submit-btn");
    if (submitBtn) {
      submitBtn.addEventListener("click", function () {
        if (submitBtn.disabled) return; // guard against double submission
        var body = lgReadForm();
        if (!body.name) { toast("❌ Game name is required", "error"); return; }

        submitBtn.disabled = true;
        var wasEditing = lgEditingId;
        var request = wasEditing
          ? apiPatchJson("/api/admin/lucky-games/" + encodeURIComponent(wasEditing), body)
          : apiPostJson("/api/admin/lucky-games", body);

        request.then(function (res) {
          if (!res.ok || res.d.status !== "ok") {
            toast("❌ " + (res.d && res.d.code || (wasEditing ? "update_failed" : "create_failed")), "error");
            return;
          }
          toast(wasEditing ? "✅ Game updated" : "✅ Game added", "success");
          lgEnterCreateMode();
          loadLuckyGames(true);
        }).catch(function (e) {
          toast("❌ " + e.message, "error");
        }).finally(function () {
          submitBtn.disabled = false;
        });
      });
    }

    var cancelBtn = lgFormEl("lg-cancel-edit-btn");
    if (cancelBtn) cancelBtn.addEventListener("click", function () { lgEnterCreateMode(); });

    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-lg-action]");
      if (!btn) return;
      var action = btn.dataset.lgAction, id = btn.dataset.id;
      if (action === "edit") {
        var game = (window.__lgGamesById || {})[id];
        if (!game) return;
        lgEnterEditMode(game);
      } else if (action === "toggle") {
        var nextPublished = btn.dataset.published !== "1";
        apiPatchJson("/api/admin/lucky-games/" + encodeURIComponent(id), { is_published: nextPublished }).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "update_failed"), "error"); return; }
          toast(nextPublished ? "✅ Game published" : "✅ Game unpublished", "success");
          loadLuckyGames(true);
        });
      } else if (action === "delete") {
        var name = (window.__lgGamesById[id] || {}).name || id;
        if (!confirm("Delete lucky game \"" + name + "\"? This cannot be undone.")) return;
        apiDelete("/api/admin/lucky-games/" + encodeURIComponent(id)).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "delete_failed"), "error"); return; }
          toast("✅ Game deleted", "success");
          if (lgEditingId === id) lgEnterCreateMode();
          loadLuckyGames(true);
        });
      }
    });
  }

  function loadGcProviders(force) {
    statePanel("gc-providers-body", "loading", "Loading providers…");
    api("/api/admin/providers").then(function (data) {
      var items = data.providers || [];
      if (!items.length) { $("#gc-providers-body").innerHTML = emptyState("No providers yet — create one above."); return; }
      var rows = items.map(function (p) {
        return '<tr><td>' + esc(p.name) + '<div class="sub">' + esc(p.provider_id) + '</div><div class="sub">' + esc(p.base_url || "") + '</div></td>' +
          '<td>' + esc(p.type || "") + '</td>' +
          '<td>' + gcPill(p.active ? "active" : "inactive") + '</td>' +
          '<td>' + (p.secret_configured ? '<span class="pill approved">configured</span>' : '<span class="pill rejected">missing</span>') + '</td>' +
          '<td>' + fmt(p.linked_campaign_count || 0) + '</td>' +
          '<td><button class="btn" data-gcp-action="activate" data-id="' + esc(p.provider_id) + '">Activate</button> ' +
          '<button class="btn" data-gcp-action="deactivate" data-id="' + esc(p.provider_id) + '">Deactivate</button></td></tr>';
      }).join("");
      $("#gc-providers-body").innerHTML = '<table class="data-table"><thead><tr><th>Provider</th><th>Type</th><th>Active</th><th>Secret</th><th>Linked</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }).catch(function (e) { statePanel("gc-providers-body", "error", "Failed to load providers: " + e.message); });
  }

  function bindGcProviders() {
    var createBtn = $("#gc-create-provider-btn");
    if (createBtn) {
      createBtn.addEventListener("click", function () {
        var body = {
          provider_id: ($("#gc-p-id").value || "").trim(),
          name: ($("#gc-p-name").value || "").trim(),
          type: $("#gc-p-type").value,
          base_url: ($("#gc-p-base-url").value || "").trim(),
          url_mode: $("#gc-p-url-mode").value,
          secret_env_var: ($("#gc-p-secret-env").value || "").trim(),
        };
        body.allowed_campaign_types = [body.type];
        apiPostJson("/api/admin/providers", body).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "create_failed"), "error"); return; }
          toast("✅ Provider created (inactive)", "success");
          loadGcProviders(true);
        });
      });
    }
    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-gcp-action]");
      if (!btn) return;
      var action = btn.dataset.gcpAction, id = btn.dataset.id;
      apiPost("/api/admin/providers/" + id + "/" + action).then(function (r) { if (r.status !== "ok") toast("❌ " + r.code, "error"); loadGcProviders(true); });
    });
  }

  function loadGcResults(force) {
    statePanel("gc-results-body", "loading", "Loading tournament results…");
    api("/api/admin/tournament-results").then(function (data) {
      var items = data.results || [];
      if (!items.length) { $("#gc-results-body").innerHTML = emptyState("No tournament result submissions yet."); return; }
      var rows = items.map(function (r) {
        return '<tr><td>' + esc(r.submission_id) + '</td><td>' + esc(r.campaign_id) + '</td>' +
          '<td>' + gcPill(r.status) + '</td><td>' + fmt(r.winner_count) + '</td>' +
          '<td>' + fmt(r.matched_users) + '/' + fmt(r.winner_count) + '</td>' +
          '<td class="sub">' + esc(r.received_at || "") + '</td>' +
          '<td><button class="btn" data-gcr-action="approve" data-id="' + esc(r.submission_id) + '">Approve</button> ' +
          '<button class="btn" data-gcr-action="reject" data-id="' + esc(r.submission_id) + '">Reject</button> ' +
          '<button class="btn" data-gcr-action="retry-allocation" data-id="' + esc(r.submission_id) + '">Retry</button></td></tr>';
      }).join("");
      $("#gc-results-body").innerHTML = '<table class="data-table"><thead><tr><th>Submission</th><th>Campaign</th><th>Status</th><th>Winners</th><th>Matched</th><th>Received</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }).catch(function (e) { statePanel("gc-results-body", "error", "Failed to load results: " + e.message); });
  }

  function bindGcResults() {
    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-gcr-action]");
      if (!btn) return;
      var action = btn.dataset.gcrAction, id = btn.dataset.id;
      if (action === "reject") {
        var reason = prompt("Rejection reason:") || "";
        apiPostJson("/api/admin/tournament-results/" + id + "/reject", { reason: reason }).then(function () { loadGcResults(true); });
      } else {
        apiPost("/api/admin/tournament-results/" + id + "/" + action).then(function (r) {
          toast(r.status === "ok" ? "✅ Done" : "❌ " + (r.code || "failed"), r.status === "ok" ? "success" : "error");
          loadGcResults(true);
        });
      }
    });
  }

  function gcPoolWarnings(p) {
    var warnings = [];
    if (p.status !== "active") warnings.push("inactive pool");
    if (p.allocation_scope === "shared") warnings.push("shared scope — allocatable by multiple subsystems");
    if (p.pool_type === "tournament_reward" && !p.campaign_id) warnings.push("no campaign linked");
    return warnings;
  }

  function loadGcRewards(force) {
    statePanel("gc-rewards-body", "loading", "Loading reward pools…");
    api("/api/admin/reward-pools").then(function (data) {
      var pools = data.pools || [];
      if (!pools.length) { $("#gc-pools-body").innerHTML = emptyState("No reward pools registered yet."); return; }
      var rows = pools.map(function (p) {
        var s = p.stock || {};
        var warnings = gcPoolWarnings(p);
        var warnHtml = warnings.length ? '<div class="sub" style="color:var(--warn);">⚠ ' + esc(warnings.join("; ")) + '</div>' : "";
        return '<tr><td>' + esc(p.name || "") + '<div class="sub">' + esc(p.pool_id) + '</div>' + warnHtml + '</td>' +
          '<td>' + esc(p.pool_type || "") + '</td>' +
          '<td>' + esc(p.allocation_scope || "") + '</td>' +
          '<td>' + esc(p.campaign_id || "—") + '</td>' +
          '<td>' + gcPill(p.status) + '</td>' +
          '<td>' + fmt(s.available || 0) + '</td>' +
          '<td>' + fmt(s.issued || 0) + '</td>' +
          '<td>campaign_centre</td>' +
          '<td><button class="btn" data-gc-upload-pool="' + esc(p.pool_id) + '">Upload codes</button></td></tr>';
      }).join("");
      $("#gc-pools-body").innerHTML = '<table class="data-table"><thead><tr><th>Pool</th><th>Type</th><th>Allocation Scope</th>' +
        '<th>Campaign</th><th>Status</th><th>Available</th><th>Assigned</th><th>Source</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>' +
        '<div class="sub" style="margin-top:6px;">"Reserved" has no separate persisted state in this model — allocation is a single atomic available→assigned transition.</div>';
    }).catch(function (e) { statePanel("gc-pools-body", "error", "Failed to load pools: " + e.message); });

    api("/api/admin/rewards").then(function (data) {
      var items = data.rewards || [];
      if (!items.length) { $("#gc-rewards-body").innerHTML = emptyState("No rewards allocated yet."); return; }
      var rows = items.map(function (r) {
        return '<tr><td>' + esc(r.reward_id) + '</td><td>' + esc(r.category || "tournament") + '</td>' +
          '<td>' + fmt(r.telegram_user_id) + '</td><td>' + fmt(r.rank) + '</td>' +
          '<td>' + esc(r.pool_id || "") + '</td><td>' + gcPill(r.status) + '</td><td>' + esc(r.voucher_code || "—") + '</td></tr>';
      }).join("");
      $("#gc-rewards-body").innerHTML = '<table class="data-table"><thead><tr><th>Reward</th><th>Category</th><th>User</th><th>Rank</th><th>Pool</th><th>Status</th><th>Code</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }).catch(function (e) { statePanel("gc-rewards-body", "error", "Failed to load rewards: " + e.message); });
  }

  function bindGcRewards() {
    var createPoolBtn = $("#gc-create-pool-btn");
    if (createPoolBtn) {
      createPoolBtn.addEventListener("click", function () {
        var body = {
          pool_id: ($("#gc-pool-id").value || "").trim(),
          name: ($("#gc-pool-name").value || "").trim(),
          pool_type: $("#gc-pool-type").value,
          allocation_scope: $("#gc-pool-scope") ? $("#gc-pool-scope").value : "campaign_rewards",
          campaign_id: ($("#gc-pool-campaign").value || "").trim(),
        };
        apiPostJson("/api/admin/reward-pools", body).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "create_failed"), "error"); return; }
          toast("✅ Pool registered", "success");
          loadGcRewards(true);
        });
      });
    }
    var filterBtn = $("#gc-rewards-filter-btn");
    if (filterBtn) {
      filterBtn.addEventListener("click", function () {
        var params = [];
        var uid = ($("#gc-rewards-filter-uid").value || "").trim();
        var status = $("#gc-rewards-filter-status").value;
        if (uid) params.push("telegram_user_id=" + encodeURIComponent(uid));
        if (status) params.push("status=" + encodeURIComponent(status));
        statePanel("gc-rewards-body", "loading", "Loading…");
        api("/api/admin/rewards" + (params.length ? "?" + params.join("&") : "")).then(function (data) {
          var items = data.rewards || [];
          if (!items.length) { $("#gc-rewards-body").innerHTML = emptyState("No matching rewards."); return; }
          var rows = items.map(function (r) {
            return '<tr><td>' + esc(r.reward_id) + '</td><td>' + esc(r.category || "tournament") + '</td>' +
              '<td>' + fmt(r.telegram_user_id) + '</td><td>' + fmt(r.rank) + '</td>' +
              '<td>' + esc(r.pool_id || "") + '</td><td>' + gcPill(r.status) + '</td><td>' + esc(r.voucher_code || "—") + '</td></tr>';
          }).join("");
          $("#gc-rewards-body").innerHTML = '<table class="data-table"><thead><tr><th>Reward</th><th>Category</th><th>User</th><th>Rank</th><th>Pool</th><th>Status</th><th>Code</th></tr></thead><tbody>' + rows + '</tbody></table>';
        });
      });
    }
    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-gc-upload-pool]");
      if (!btn) return;
      var poolId = btn.dataset.gcUploadPool;
      var raw = prompt("Paste voucher codes, one per line:");
      if (!raw) return;
      var codes = raw.split("\n").map(function (s) { return s.trim(); }).filter(Boolean);
      apiPostJson("/api/admin/reward-pools/" + poolId + "/upload-codes", { codes: codes }).then(function (res) {
        toast(res.ok ? "✅ Inserted " + res.d.inserted + "/" + codes.length : "❌ upload failed", res.ok ? "success" : "error");
        loadGcRewards(true);
      });
    });
  }

  var gcEventsPage = 1;

  function loadGcEvents(page) {
    gcEventsPage = page || 1;
    var params = ["page=" + gcEventsPage, "page_size=25"];
    var campaignId = ($("#gc-events-campaign-id") && $("#gc-events-campaign-id").value || "").trim();
    var eventType = $("#gc-events-type") && $("#gc-events-type").value;
    if (campaignId) params.push("campaign_id=" + encodeURIComponent(campaignId));
    if (eventType) params.push("event_type=" + encodeURIComponent(eventType));
    statePanel("gc-events-body", "loading", "Loading activity log…");
    api("/api/admin/campaign-events?" + params.join("&")).then(function (data) {
      var items = data.events || [];
      if (!items.length) { $("#gc-events-body").innerHTML = emptyState("No matching activity yet."); $("#gc-events-pagination").innerHTML = ""; return; }
      var rows = items.map(function (e) {
        return '<tr><td>' + esc(e.event_type) + '</td><td>' + esc(e.campaign_id || "—") + '</td>' +
          '<td>' + fmt(e.telegram_user_id || "") + '</td><td>' + gcPill(e.status) + '</td>' +
          '<td>' + esc(e.reason || "") + '</td><td class="sub">' + esc(e.occurred_at || "") + '</td></tr>';
      }).join("");
      $("#gc-events-body").innerHTML = '<table class="data-table"><thead><tr><th>Event</th><th>Campaign</th><th>User</th><th>Status</th><th>Reason</th><th>Occurred</th></tr></thead><tbody>' + rows + '</tbody></table>';
      var totalPages = Math.max(1, Math.ceil((data.total || 0) / (data.page_size || 25)));
      $("#gc-events-pagination").innerHTML =
        '<button class="btn" id="gc-events-prev"' + (gcEventsPage <= 1 ? " disabled" : "") + '>← Prev</button> ' +
        '<span class="sub">Page ' + gcEventsPage + ' of ' + totalPages + ' (' + fmt(data.total || 0) + ' total)</span> ' +
        '<button class="btn" id="gc-events-next"' + (gcEventsPage >= totalPages ? " disabled" : "") + '>Next →</button>';
      var prevBtn = $("#gc-events-prev"), nextBtn = $("#gc-events-next");
      if (prevBtn) prevBtn.addEventListener("click", function () { loadGcEvents(gcEventsPage - 1); });
      if (nextBtn) nextBtn.addEventListener("click", function () { loadGcEvents(gcEventsPage + 1); });
    }).catch(function (e) { statePanel("gc-events-body", "error", "Failed to load activity log: " + e.message); });
  }

  function loadGcAnalyticsSummary(campaignId) {
    if (!campaignId) { $("#gc-analytics-summary-body").innerHTML = emptyState("Enter a campaign_id and click Load Summary."); return; }
    statePanel("gc-analytics-summary-body", "loading", "Loading summary…");
    api("/api/admin/campaign-analytics/summary?campaign_id=" + encodeURIComponent(campaignId)).then(function (data) {
      var pct = function (v) { return Math.round((v || 0) * 100) + "%"; };
      $("#gc-analytics-summary-body").innerHTML = '<div class="card-grid">' + [
        ["Views", fmt(data.views)], ["Clicks", fmt(data.clicks)], ["CTR", pct(data.click_through_rate)],
        ["Sub. Checks", fmt(data.subscription_checks)], ["Sub. Pass Rate", pct(data.subscription_pass_rate)],
        ["Destination Opens", fmt(data.destination_opens)], ["Dest. Conversion", pct(data.destination_conversion_rate)],
        ["Results Received", fmt(data.leaderboards_received)], ["Rewards Assigned", fmt(data.rewards_assigned)],
        ["Reward Views", fmt(data.rewards_viewed)], ["Voucher Copies", fmt(data.voucher_copies)],
        ["Out of Stock", fmt(data.out_of_stock)],
      ].map(function (kv) {
        return '<div class="kpi"><div class="label">' + esc(kv[0]) + '</div><div class="value">' + kv[1] + '</div></div>';
      }).join("") + '</div>';
    }).catch(function (e) { statePanel("gc-analytics-summary-body", "error", "Failed to load summary: " + e.message); });
  }

  function bindGcActivity() {
    var filterBtn = $("#gc-events-filter-btn");
    if (filterBtn) filterBtn.addEventListener("click", function () { loadGcEvents(1); });
    var summaryBtn = $("#gc-analytics-load-btn");
    if (summaryBtn) {
      summaryBtn.addEventListener("click", function () {
        loadGcAnalyticsSummary(($("#gc-analytics-campaign-id").value || "").trim());
      });
    }
  }

  // ---------------------------------------------------------------------
  // Referral Centre -> Share Content (Caption Hooks / Playback Pool)
  // ---------------------------------------------------------------------
  var rse = {
    startDate: "",
    endDate: "",
    source: "all",
  };

  var rsc = {
    subtab: "hooks",
    selected: { hook: new Set(), playback_link: new Set() },
    counts: { hook: { active: 0, total: 0 }, playback_link: { active: 0, total: 0 } },
    rowStatus: { hook: {}, playback_link: {} },
    rowLabel: { hook: {}, playback_link: {} },
    busy: { hook: false, playback_link: false },
  };

  var RSC_LARGE_BULK_THRESHOLD = 50;

  var rscHookCfg = {
    resourceType: "hook", prefix: "rsc-hooks", label: "hook", labelPlural: "hooks",
    titlePlural: "Hooks", rowActionAttr: "data-rsc-hook-action", reload: function () { return loadShareHooks(true); },
  };
  var rscPlaybackCfg = {
    resourceType: "playback_link", prefix: "rsc-playback", label: "playback link", labelPlural: "playback links",
    titlePlural: "Playback Links", rowActionAttr: "data-rsc-playback-action", reload: function () { return loadSharePlayback(true); },
  };

  function rscSummaryText(counts) {
    return "Active: " + fmt(counts.active) + " / " + fmt(counts.total);
  }

  // Keeps the section-level "select all" checkbox + Apply button + selected
  // count in sync with the current selection Set and busy state. Called
  // after every render and every checkbox toggle.
  function rscUpdateBulkControls(cfg) {
    var selected = rsc.selected[cfg.resourceType];
    var actionSel = $("#" + cfg.prefix + "-bulk-select");
    var applyBtn = $("#" + cfg.prefix + "-bulk-apply-btn");
    var countEl = $("#" + cfg.prefix + "-selected-count");
    var action = actionSel ? actionSel.value : "";
    var busy = rsc.busy[cfg.resourceType];
    if (countEl) countEl.textContent = selected.size ? (fmt(selected.size) + " selected") : "";
    if (applyBtn) applyBtn.disabled = busy || !action || (action === "delete_selected" && selected.size === 0);
    var selectAll = $("#" + cfg.prefix + "-select-all");
    var rowBoxes = $all("#" + cfg.prefix + "-body .rsc-row-check");
    if (selectAll) {
      var total = rowBoxes.length;
      var checked = rowBoxes.filter(function (b) { return b.checked; }).length;
      selectAll.disabled = busy;
      selectAll.checked = total > 0 && checked === total;
      selectAll.indeterminate = checked > 0 && checked < total;
    }
  }

  // Disables every control in a section (checkboxes, bulk controls, and
  // individual row action buttons) while a request for that resource type
  // is in flight, to prevent duplicate/overlapping requests.
  function rscSetBusy(cfg, busy) {
    rsc.busy[cfg.resourceType] = busy;
    var scope = $("#" + cfg.prefix + "-panel");
    if (scope) {
      $all("button, select, input[type=checkbox]", scope).forEach(function (el) { el.disabled = busy; });
    }
    rscUpdateBulkControls(cfg);
  }

  function rscPerformBulkAction(cfg) {
    var actionSel = $("#" + cfg.prefix + "-bulk-select");
    var action = actionSel.value;
    if (!action || rsc.busy[cfg.resourceType]) return;
    if (action === "delete_selected" && !rsc.selected[cfg.resourceType].size) {
      toast("❌ Select at least one item first", "error");
      return;
    }

    // The last-active-item warning (and the large-batch threshold) must be
    // judged against *current* server state, not whatever was on screen
    // when the page/list was last loaded -- so refresh counts/status first,
    // and only then build the confirmation copy from the fresh values.
    rscSetBusy(cfg, true);
    cfg.reload().then(function () {
      var counts = rsc.counts[cfg.resourceType];
      var selectedIds = Array.from(rsc.selected[cfg.resourceType]);

      if (action === "delete_selected" && !selectedIds.length) {
        rscSetBusy(cfg, false);
        toast("❌ Selection is no longer valid -- the item(s) may have already been removed", "error");
        return;
      }

      var proceed;
      if (action === "activate_all") {
        var inactiveCount = Math.max(0, counts.total - counts.active);
        proceed = inactiveCount > RSC_LARGE_BULK_THRESHOLD
          ? confirmSimple("Activate All " + cfg.titlePlural,
              "This will activate " + fmt(inactiveCount) + " " + cfg.labelPlural + ". Continue?")
          : Promise.resolve(true);
      } else if (action === "deactivate_all") {
        proceed = confirmSimple("Deactivate All " + cfg.titlePlural,
          "This will deactivate " + fmt(counts.active) + " active " + cfg.labelPlural + ". " +
          "Creator post generation may become unavailable if no active items remain.")
          .then(function (ok) {
            if (!ok) return false;
            if (counts.active === 0) return true;
            return confirmSimple("Warning: Zero Active " + cfg.titlePlural,
              "This will leave zero active " + cfg.labelPlural + ". Confirm again to proceed.");
          });
      } else if (action === "delete_selected") {
        var labels = rsc.rowLabel[cfg.resourceType];
        var names = selectedIds.slice(0, 10).map(function (id) { return labels[id] || id; });
        var namesText = names.join(", ") + (selectedIds.length > 10 ? ", …" : "");
        var activeSelected = selectedIds.filter(function (id) { return rsc.rowStatus[cfg.resourceType][id] === "active"; }).length;
        proceed = confirmSimple(
          "Delete " + fmt(selectedIds.length) + " " + (selectedIds.length === 1 ? cfg.label : cfg.labelPlural),
          "This permanently deletes: " + namesText + ". This cannot be undone."
        ).then(function (ok) {
          if (!ok) return false;
          if (activeSelected > 0 && activeSelected >= counts.active) {
            return confirmSimple("Warning: Zero Active " + cfg.titlePlural,
              "Deleting this selection will leave zero active " + cfg.labelPlural + ". Confirm again to proceed.");
          }
          return true;
        });
      } else {
        rscSetBusy(cfg, false);
        return;
      }

      proceed.then(function (ok) {
        if (!ok) { rscSetBusy(cfg, false); return; }
        // The backend re-validates every id and computes every count
        // independently regardless of what the client just confirmed --
        // this refresh only makes the confirmation *copy* accurate, it is
        // not a substitute for server-side validation.
        var body = { resource_type: cfg.resourceType, action: action };
        if (action === "delete_selected") body.selected_ids = selectedIds;
        apiPostJson("/api/admin/referral/share-content/bulk-action", body).then(function (res) {
          rscSetBusy(cfg, false);
          if (!res.ok || res.d.status !== "ok") {
            toast("❌ " + (res.d && res.d.code || "bulk_action_failed"), "error");
            return; // preserve selection on failure
          }
          var d = res.d;
          var msg;
          if (action === "delete_selected") {
            msg = fmt(d.deleted_count) + " selected " + (d.deleted_count === 1 ? cfg.label : cfg.labelPlural) + " deleted.";
            rsc.selected[cfg.resourceType].clear();
          } else if (action === "activate_all") {
            msg = fmt(d.modified_count) + " " + cfg.labelPlural + " activated.";
          } else {
            msg = fmt(d.modified_count) + " " + cfg.labelPlural + " deactivated.";
          }
          toast("✅ " + msg, "success");
          actionSel.value = "";
          cfg.reload();
        }).catch(function (e) {
          rscSetBusy(cfg, false);
          toast("❌ " + e.message, "error"); // preserve selection on failure
        });
      });
    }).catch(function (e) {
      rscSetBusy(cfg, false);
      toast("❌ Failed to refresh before confirming: " + e.message, "error");
    });
  }

  function rscBindBulkControls(cfg) {
    var selectAll = $("#" + cfg.prefix + "-select-all");
    if (selectAll) {
      selectAll.addEventListener("change", function () {
        var checked = selectAll.checked;
        $all("#" + cfg.prefix + "-body .rsc-row-check").forEach(function (box) {
          box.checked = checked;
          if (checked) rsc.selected[cfg.resourceType].add(box.dataset.id);
          else rsc.selected[cfg.resourceType].delete(box.dataset.id);
        });
        rscUpdateBulkControls(cfg);
      });
    }
    var bulkSelect = $("#" + cfg.prefix + "-bulk-select");
    if (bulkSelect) bulkSelect.addEventListener("change", function () { rscUpdateBulkControls(cfg); });
    var applyBtn = $("#" + cfg.prefix + "-bulk-apply-btn");
    if (applyBtn) applyBtn.addEventListener("click", function () { rscPerformBulkAction(cfg); });

    document.addEventListener("change", function (e) {
      var box = e.target;
      if (!box.classList || !box.classList.contains("rsc-row-check")) return;
      if (box.closest("#" + cfg.prefix + "-body") === null) return;
      if (box.checked) rsc.selected[cfg.resourceType].add(box.dataset.id);
      else rsc.selected[cfg.resourceType].delete(box.dataset.id);
      rscUpdateBulkControls(cfg);
    });
  }

  function rscStatusPill(status) {
    return '<span class="pill ' + (status === "active" ? "approved" : "neutral") + '">' + esc(status || "—") + '</span>';
  }

  function rscStatusFilter(id) {
    var activeBtn = $("#" + id + " .active");
    return activeBtn ? (activeBtn.dataset.status || "") : "";
  }

  function rscQuery(searchId, filterId) {
    var q = ($("#" + searchId) && $("#" + searchId).value || "").trim();
    var status = rscStatusFilter(filterId);
    var params = [];
    if (q) params.push("q=" + encodeURIComponent(q));
    if (status) params.push("status=" + encodeURIComponent(status));
    return params.length ? "?" + params.join("&") : "";
  }

  function loadReferralShareContent(force) {
    $("#rsc-subtab-hooks").classList.toggle("active", rsc.subtab === "hooks");
    $("#rsc-subtab-playback").classList.toggle("active", rsc.subtab === "playback");
    $("#rsc-subtab-creators").classList.toggle("active", rsc.subtab === "creators");
    $("#rsc-hooks-panel").classList.toggle("hidden", rsc.subtab !== "hooks");
    $("#rsc-playback-panel").classList.toggle("hidden", rsc.subtab !== "playback");
    $("#rsc-creators-panel").classList.toggle("hidden", rsc.subtab !== "creators");
    if (rsc.subtab === "hooks") loadShareHooks(force);
    else if (rsc.subtab === "playback") loadSharePlayback(force);
    else { loadCreatorAccess(force); loadCreatorGroupSettings(force); }
  }

  function loadCreatorGroupSettings() {
    api("/api/admin/referral/creator-settings").then(function (data) {
      var s = data.settings || {};
      $("#rsc-creator-group-chat-id").value = s.creator_group_chat_id != null ? String(s.creator_group_chat_id) : "";
      $("#rsc-creator-group-check-enabled").checked = !!s.membership_check_enabled;
      $("#rsc-creator-group-chat-title").textContent = s.chat_title || "—";
      $("#rsc-creator-group-chat-type").textContent = s.chat_type || "—";
      $("#rsc-creator-group-bot-status").textContent = s.bot_membership_status || "—";
      $("#rsc-creator-group-verified-at").textContent = s.verified_at ? new Date(s.verified_at).toLocaleString() : "—";
      $("#rsc-creator-group-source").textContent = s.source || "—";
      $("#rsc-creator-group-version").textContent = s.config_version != null ? String(s.config_version) : "—";
    }).catch(function (e) { toast("❌ Failed to load creator group settings: " + e.message, "error"); });
  }

  function rscApprovalSourceLabel(source) {
    if (source === "creator_access_chat_membership") return "Chat membership";
    if (source === "bulk_import") return "Bulk import";
    if (source === "manual") return "Manual";
    return "—";
  }

  function loadCreatorAccess() {
    statePanel("rsc-creators-body", "loading", "Loading creators…");
    api("/api/admin/referral/creators" + rscQuery("rsc-creators-search", "rsc-creators-status-filter")).then(function (data) {
      $("#rsc-creators-summary").textContent = "Active creators: " + fmt(data.active_count || 0);
      var items = data.creators || [];
      if (!items.length) { $("#rsc-creators-body").innerHTML = emptyState("No creators yet — chat members appear here after opening The Money Room, or approve one manually above."); return; }
      var rows = items.map(function (c) {
        var actions = '<button class="btn danger" data-rsc-creator-action="remove" data-id="' + esc(c.user_id) + '">Remove</button>';
        if (c.status === "active") {
          actions = '<button class="btn" data-rsc-creator-action="suspend" data-id="' + esc(c.user_id) + '">Suspend</button> ' + actions;
        } else {
          actions = '<button class="btn" data-rsc-creator-action="activate" data-id="' + esc(c.user_id) + '">Activate</button> ' + actions;
        }
        return '<tr><td>' + esc(c.user_id) + '</td>' +
          '<td>' + esc(c.username || "—") + '</td>' +
          '<td>' + esc(c.creator_tier || "—") + '</td>' +
          '<td>' + rscStatusPill(c.status) + '</td>' +
          '<td>' + esc(rscApprovalSourceLabel(c.approval_source)) + '</td>' +
          '<td class="sub">' + (c.approved_at ? new Date(c.approved_at).toLocaleString() : "—") + '</td>' +
          '<td>' + actions + '</td></tr>';
      }).join("");
      $("#rsc-creators-body").innerHTML = '<table class="data-table"><thead><tr><th>User ID</th><th>Username</th><th>Tier</th>' +
        '<th>Status</th><th>Approval Source</th><th>Approved</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }).catch(function (e) { statePanel("rsc-creators-body", "error", "Failed to load creators: " + e.message); });
  }

  function loadShareHooks() {
    statePanel("rsc-hooks-body", "loading", "Loading caption hooks…");
    return api("/api/admin/referral/share-content/hooks" + rscQuery("rsc-hooks-search", "rsc-hooks-status-filter")).then(function (data) {
      var items = data.hooks || [];
      rsc.counts.hook = { active: data.active_count || 0, total: data.total_count || 0 };
      var summaryEl = $("#rsc-hooks-active-summary");
      if (summaryEl) summaryEl.textContent = rscSummaryText(rsc.counts.hook);
      rsc.rowStatus.hook = {};
      rsc.rowLabel.hook = {};
      items.forEach(function (h) { rsc.rowStatus.hook[h.id] = h.status; rsc.rowLabel.hook[h.id] = h.text; });
      var known = {};
      items.forEach(function (h) { known[h.id] = true; });
      rsc.selected.hook.forEach(function (id) { if (!known[id]) rsc.selected.hook.delete(id); });
      if (!items.length) { $("#rsc-hooks-body").innerHTML = emptyState("No caption hooks yet — add one above."); rscUpdateBulkControls(rscHookCfg); return; }
      var rows = items.map(function (h) {
        var checked = rsc.selected.hook.has(h.id) ? "checked" : "";
        return '<tr><td><input type="checkbox" class="rsc-row-check" data-id="' + esc(h.id) + '" ' + checked + ' /></td><td>' + esc(h.text) + '</td>' +
          '<td>' + rscStatusPill(h.status) + '</td>' +
          '<td>' + fmt(h.times_selected || 0) + '</td>' +
          '<td class="sub">' + (h.last_selected_at ? new Date(h.last_selected_at).toLocaleString() : "—") + '</td>' +
          '<td class="sub">' + (h.created_at ? new Date(h.created_at).toLocaleString() : "—") + '</td>' +
          '<td>' +
          '<button class="btn" data-rsc-hook-action="edit" data-id="' + esc(h.id) + '" data-text="' + esc(h.text) + '">Edit</button> ' +
          '<button class="btn" data-rsc-hook-action="' + (h.status === "active" ? "deactivate" : "activate") + '" data-id="' + esc(h.id) + '">' +
          (h.status === "active" ? "Deactivate" : "Activate") + '</button> ' +
          '<button class="btn danger" data-rsc-hook-action="delete" data-id="' + esc(h.id) + '" data-text="' + esc(h.text) + '">Delete</button>' +
          '</td></tr>';
      }).join("");
      $("#rsc-hooks-body").innerHTML = '<table class="data-table"><thead><tr><th></th><th>Hook</th><th>Status</th><th>Times Selected</th>' +
        '<th>Last Selected</th><th>Created</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
      rscUpdateBulkControls(rscHookCfg);
    }).catch(function (e) { statePanel("rsc-hooks-body", "error", "Failed to load caption hooks: " + e.message); });
  }

  function rscRenderBulkResult(elId, body) {
    var el = $(elId);
    if (!el) return;
    var lines = (body.results || []).map(function (r) {
      var tag = r.status === "inserted" ? "approved" : (r.status === "rejected" ? "rejected" : "neutral");
      return '<div><span class="pill ' + tag + '">' + esc(r.status) + '</span> ' + esc(r.line) + (r.reason ? ' <span class="sub">(' + esc(r.reason) + ')</span>' : '') + '</div>';
    }).join("");
    el.innerHTML = '<div class="sub" style="margin:6px 0;">Inserted: ' + fmt(body.inserted) + ' · Skipped: ' + fmt(body.skipped) + ' · Rejected: ' + fmt(body.rejected) + '</div>' + lines;
  }

  // ---------- Referral Centre → Share Engagement ----------
  function rsePct(rate) {
    return Math.round((rate || 0) * 1000) / 10 + "%";
  }

  function rseQuery() {
    var qs = [];
    if (rse.startDate) qs.push("start_date=" + encodeURIComponent(rse.startDate));
    if (rse.endDate) qs.push("end_date=" + encodeURIComponent(rse.endDate));
    qs.push("source=" + encodeURIComponent(rse.source || "all"));
    return qs.length ? "?" + qs.join("&") : "";
  }

  function rseFunnelStep(label, value, assisted) {
    return '<div class="kpi" style="min-width:150px;"><div class="label">' + esc(label) +
      (assisted ? ' <span class="sub" style="color:#c98a2c;">(assisted)</span>' : '') + '</div>' +
      '<div class="value">' + fmt(value) + '</div></div>';
  }

  function loadReferralShareEngagement(force) {
    var startEl = $("#rse-start-date"), endEl = $("#rse-end-date");
    skeletonGrid($("#rse-kpis"), 6);
    api("/api/admin/referral-engagement" + rseQuery()).then(function (data) {
      if (startEl && !startEl.value) startEl.value = data.period.start_date;
      if (endEl && !endEl.value) endEl.value = data.period.end_date;

      var trackingStartedAt = data.period.tracking_started_at ? new Date(data.period.tracking_started_at) : null;
      $("#rse-tracking-banner").innerHTML = trackingStartedAt
        ? '<div class="banner" style="background:rgba(255,122,0,.08);border:1px solid rgba(255,122,0,.25);">' +
          'Tracking available from ' + esc(trackingStartedAt.toLocaleDateString()) +
          '. Periods before this date have no tracking data and are not "0% performance" — they were simply not measured.</div>'
        : '';

      if (!data.has_data) {
        $("#rse-content").classList.add("hidden");
        $("#rse-empty-state").innerHTML = emptyState({
          icon: "📭",
          title: "No tracking data is available for this period.",
          sub: "Try widening the date range, or check back after tracking has been running for a few days.",
        });
        return;
      }
      $("#rse-empty-state").innerHTML = "";
      $("#rse-content").classList.remove("hidden");

      var t = data.totals, r = data.rates, ac = data.assisted_conversion;
      $("#rse-kpis").innerHTML =
        kpiCard("Referral Section Viewers", t.unique_section_viewers) +
        kpiCard("CTA Clickers", t.unique_cta_clickers) +
        kpiCard("Referral CTA CTR", rsePct(r.section_to_click)) +
        kpiCard("Links Generated", t.links_generated) +
        kpiCard("Copy/Share Users", t.unique_copy_or_share_users,
          "Copiers: " + fmt(t.unique_copiers) + " · Sharers: " + fmt(t.unique_sharers)) +
        kpiCard("Engagement → Qualified Referrer Rate", rsePct(ac.engagement_to_qualified_rate), "Referrer-level assisted conversion");

      $("#rse-funnel").innerHTML =
        '<div class="card-grid">' +
        rseFunnelStep("Section Viewed", t.unique_section_viewers) +
        rseFunnelStep("CTA Clicked", t.unique_cta_clickers) +
        rseFunnelStep("Link Generated", t.unique_link_generators) +
        rseFunnelStep("Copied or Shared", t.unique_copy_or_share_users) +
        rseFunnelStep("Referrer Produced Join", ac.engaged_referrers_with_join, true) +
        rseFunnelStep("Referrer Produced Qualified Referral", ac.engaged_referrers_with_qualified_referral, true) +
        '</div>' +
        '<div class="sub" style="margin-top:8px;">Section→Click ' + rsePct(r.section_to_click) +
        ' · Click→Generate ' + rsePct(r.click_to_generate) +
        ' · Generate→Copy/Share ' + rsePct(r.generate_to_copy_or_share) +
        ' · Section→Copy/Share ' + rsePct(r.section_to_copy_or_share) + '</div>' +
        '<div class="sub" style="margin-top:4px;color:#c98a2c;">' +
        '"Referrer Produced Join / Qualified Referral" are referrer-level assisted conversion metrics — ' +
        'they show whether an engaged referrer\'s account later produced a join/qualified referral, ' +
        'not that a specific click caused a specific invite.</div>';

      var bySource = data.by_source || [];
      $("#rse-source-comparison").innerHTML = '<table class="data-table"><thead><tr>' +
        '<th>Source</th><th>Viewers</th><th>Clickers</th><th>CTR</th><th>Generated</th>' +
        '<th>Copy/Share Users</th><th>Assisted Joins</th><th>Assisted Qualified</th></tr></thead><tbody>' +
        bySource.map(function (s) {
          return '<tr><td>' + esc(s.source === "miniapp" ? "Mini App" : "The Money Room") + '</td>' +
            '<td>' + fmt(s.viewers) + '</td><td>' + fmt(s.clickers) + '</td><td>' + rsePct(s.ctr) + '</td>' +
            '<td>' + fmt(s.generated) + '</td><td>' + fmt(s.copy_share_users) + '</td>' +
            '<td>' + fmt(s.assisted_joins) + '</td><td>' + fmt(s.assisted_qualified_referrals) + '</td></tr>';
        }).join("") + '</tbody></table>';

      var daily = data.daily || [];
      $("#rse-daily-trend").innerHTML = !daily.length ? emptyState("No daily data for this period.") :
        '<table class="data-table"><thead><tr><th>Date</th><th>Section Viewed</th><th>CTA Clicked</th>' +
        '<th>Link Generated</th><th>Copied/Shared</th></tr></thead><tbody>' +
        daily.map(function (d) {
          return '<tr><td>' + esc(d.date) + '</td><td>' + fmt(d.section_viewed) + '</td>' +
            '<td>' + fmt(d.cta_clicked) + '</td><td>' + fmt(d.link_generated) + '</td>' +
            '<td>' + fmt(d.copy_or_share) + '</td></tr>';
        }).join("") + '</tbody></table>';

      var topUsers = data.top_users || [];
      $("#rse-top-users").innerHTML = !topUsers.length ? emptyState("No engaged users in this period.") :
        '<table class="data-table"><thead><tr><th>User ID</th><th>Events</th><th>Links Generated</th>' +
        '<th>Copies</th><th>Shares</th></tr></thead><tbody>' +
        topUsers.map(function (u) {
          return '<tr><td>' + fmt(u.user_id) + '</td><td>' + fmt(u.events) + '</td>' +
            '<td>' + fmt(u.links_generated) + '</td><td>' + fmt(u.copies) + '</td><td>' + fmt(u.shares) + '</td></tr>';
        }).join("") + '</tbody></table>';
    }).catch(function (e) {
      $("#rse-empty-state").innerHTML = '<div class="banner error">Failed to load Share Engagement data: ' + esc(e.message) + '</div>';
      $("#rse-content").classList.add("hidden");
    });
  }

  function bindReferralShareEngagement() {
    $("#rse-apply-btn").addEventListener("click", function () {
      rse.startDate = ($("#rse-start-date").value || "").trim();
      rse.endDate = ($("#rse-end-date").value || "").trim();
      loadReferralShareEngagement(true);
    });
    $all("#rse-source-filter button").forEach(function (b) {
      b.addEventListener("click", function () {
        rse.source = b.dataset.source;
        $all("#rse-source-filter button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadReferralShareEngagement(true);
      });
    });
  }

  function bindReferralShareContent() {
    $("#rsc-subtab-hooks").addEventListener("click", function () { rsc.subtab = "hooks"; loadReferralShareContent(true); });
    $("#rsc-subtab-playback").addEventListener("click", function () { rsc.subtab = "playback"; loadReferralShareContent(true); });
    $("#rsc-subtab-creators").addEventListener("click", function () { rsc.subtab = "creators"; loadReferralShareContent(true); });

    $("#rsc-creator-group-verify-btn").addEventListener("click", function () {
      var raw = ($("#rsc-creator-group-chat-id").value || "").trim();
      if (!raw) { toast("❌ Enter a group chat ID first", "error"); return; }
      $("#rsc-creator-group-verify-result").textContent = "Verifying…";
      apiPostJson("/api/admin/referral/creator-settings/verify-group", { creator_group_chat_id: raw }).then(function (res) {
        if (!res.ok || res.d.status !== "ok") {
          $("#rsc-creator-group-verify-result").textContent = "❌ " + (res.d && res.d.code || "verification_failed");
          return;
        }
        var warn = res.d.warning ? " (warning: chat ID doesn't start with -100)" : "";
        $("#rsc-creator-group-verify-result").textContent =
          "✅ Verified: " + (res.d.chat_title || "—") + " [" + res.d.chat_type + "], bot status: " + res.d.bot_membership_status + warn;
      });
    });

    $("#rsc-creator-group-save-btn").addEventListener("click", function () {
      var raw = ($("#rsc-creator-group-chat-id").value || "").trim();
      var body = {
        creator_group_chat_id: raw || null,
        membership_check_enabled: $("#rsc-creator-group-check-enabled").checked,
        force_save: $("#rsc-creator-group-force-save").checked,
      };
      apiPutJson("/api/admin/referral/creator-settings", body).then(function (res) {
        if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "save_failed"), "error"); return; }
        toast("✅ Creator group settings saved", "success");
        $("#rsc-creator-group-verify-result").textContent = "";
        loadCreatorGroupSettings(true);
        loadCreatorAccess(true);
      });
    });

    $("#rsc-creators-search-btn").addEventListener("click", function () { loadCreatorAccess(true); });
    $all("#rsc-creators-status-filter button").forEach(function (b) {
      b.addEventListener("click", function () {
        $all("#rsc-creators-status-filter button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadCreatorAccess(true);
      });
    });
    $("#rsc-creator-add-btn").addEventListener("click", function () {
      var userId = ($("#rsc-creator-user-id").value || "").trim();
      var username = ($("#rsc-creator-username").value || "").trim();
      var tier = ($("#rsc-creator-tier").value || "").trim();
      if (!userId) { toast("❌ Telegram user ID is required", "error"); return; }
      apiPostJson("/api/admin/referral/creators", { user_id: userId, username: username, creator_tier: tier || undefined }).then(function (res) {
        if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "create_failed"), "error"); return; }
        toast("✅ Creator approved", "success");
        $("#rsc-creator-user-id").value = "";
        $("#rsc-creator-username").value = "";
        $("#rsc-creator-tier").value = "";
        loadCreatorAccess(true);
      });
    });
    $("#rsc-creators-bulk-btn").addEventListener("click", function () {
      var lines = $("#rsc-creators-bulk-text").value || "";
      apiPostJson("/api/admin/referral/creators/bulk", { user_ids: lines }).then(function (res) {
        if (!res.ok) { toast("❌ Bulk import failed", "error"); return; }
        rscRenderBulkResult("#rsc-creators-bulk-result", res.d);
        toast("✅ Bulk import done (" + res.d.inserted + " inserted)", "success");
        $("#rsc-creators-bulk-text").value = "";
        loadCreatorAccess(true);
      });
    });
    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-rsc-creator-action]");
      if (!btn) return;
      var action = btn.dataset.rscCreatorAction, id = btn.dataset.id;
      if (action === "suspend" || action === "activate") {
        apiPost("/api/admin/referral/creators/" + id + "/" + action).then(function (r) {
          if (r.status !== "ok") toast("❌ " + r.code, "error");
          loadCreatorAccess(true);
        });
      } else if (action === "remove") {
        if (!confirm("Remove this creator's access?")) return;
        apiDelete("/api/admin/referral/creators/" + id).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "remove_failed"), "error"); return; }
          toast("✅ Creator removed", "success");
          loadCreatorAccess(true);
        });
      }
    });

    $("#rsc-hooks-search-btn").addEventListener("click", function () { loadShareHooks(true); });
    $all("#rsc-hooks-status-filter button").forEach(function (b) {
      b.addEventListener("click", function () {
        $all("#rsc-hooks-status-filter button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadShareHooks(true);
      });
    });
    $("#rsc-hook-add-btn").addEventListener("click", function () {
      var text = ($("#rsc-hook-text").value || "").trim();
      if (!text) { toast("❌ Hook text is required", "error"); return; }
      apiPostJson("/api/admin/referral/share-content/hooks", { text: text }).then(function (res) {
        if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "create_failed"), "error"); return; }
        toast("✅ Hook added", "success");
        $("#rsc-hook-text").value = "";
        loadShareHooks(true);
      });
    });
    $("#rsc-hooks-bulk-btn").addEventListener("click", function () {
      var lines = $("#rsc-hooks-bulk-text").value || "";
      apiPostJson("/api/admin/referral/share-content/hooks/bulk-import", { lines: lines }).then(function (res) {
        if (!res.ok) { toast("❌ Bulk import failed", "error"); return; }
        rscRenderBulkResult("#rsc-hooks-bulk-result", res.d);
        toast("✅ Bulk import done (" + res.d.inserted + " inserted)", "success");
        $("#rsc-hooks-bulk-text").value = "";
        loadShareHooks(true);
      });
    });
    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-rsc-hook-action]");
      if (!btn) return;
      var action = btn.dataset.rscHookAction, id = btn.dataset.id;
      if (action === "edit") {
        var newText = prompt("Edit hook text:", btn.dataset.text || "");
        if (newText === null) return;
        newText = newText.trim();
        if (!newText) { toast("❌ Hook text is required", "error"); return; }
        apiPutJson("/api/admin/referral/share-content/hooks/" + id, { text: newText }).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "update_failed"), "error"); return; }
          toast("✅ Hook updated", "success");
          loadShareHooks(true);
        });
      } else if (action === "activate" || action === "deactivate") {
        if (!btnStart(btn, "Working...")) return;
        apiPost("/api/admin/referral/share-content/hooks/" + id + "/" + action).then(function (r) {
          btnStop(btn);
          if (r.status !== "ok") toast("❌ " + r.code, "error");
          loadShareHooks(true);
        }).catch(function (e) {
          btnStop(btn);
          toast("❌ " + e.message, "error");
        });
      } else if (action === "delete") {
        var hookText = btn.dataset.text || id;
        confirmSimple("Delete Caption Hook",
          'Permanently delete hook "' + hookText + '"? This cannot be undone.').then(function (ok) {
          if (!ok) return;
          if (!btnStart(btn, "Deleting...")) return;
          apiDelete("/api/admin/referral/share-content/hooks/" + id).then(function (res) {
            btnStop(btn);
            if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "delete_failed"), "error"); return; }
            toast("✅ Hook deleted", "success");
            loadShareHooks(true);
          }).catch(function (e) {
            btnStop(btn);
            toast("❌ " + e.message, "error");
          });
        });
      }
    });

    rscBindBulkControls(rscHookCfg);

    $("#rsc-playback-search-btn").addEventListener("click", function () { loadSharePlayback(true); });
    $all("#rsc-playback-status-filter button").forEach(function (b) {
      b.addEventListener("click", function () {
        $all("#rsc-playback-status-filter button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadSharePlayback(true);
      });
    });
    $("#rsc-playback-add-btn").addEventListener("click", function () {
      var url = ($("#rsc-playback-url").value || "").trim();
      var gameName = ($("#rsc-playback-game-name").value || "").trim();
      if (!url) { toast("❌ Playback URL or ID is required", "error"); return; }
      apiPostJson("/api/admin/referral/share-content/playback", { url: url, game_name: gameName }).then(function (res) {
        if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "create_failed"), "error"); return; }
        toast("✅ Playback record added", "success");
        $("#rsc-playback-url").value = "";
        $("#rsc-playback-game-name").value = "";
        loadSharePlayback(true);
      });
    });
    $("#rsc-playback-bulk-btn").addEventListener("click", function () {
      var lines = $("#rsc-playback-bulk-text").value || "";
      apiPostJson("/api/admin/referral/share-content/playback/bulk-import", { lines: lines }).then(function (res) {
        if (!res.ok) { toast("❌ Bulk import failed", "error"); return; }
        rscRenderBulkResult("#rsc-playback-bulk-result", res.d);
        toast("✅ Bulk import done (" + res.d.inserted + " inserted)", "success");
        $("#rsc-playback-bulk-text").value = "";
        loadSharePlayback(true);
      });
    });
    document.addEventListener("click", function (e) {
      var btn = e.target && e.target.closest && e.target.closest("[data-rsc-playback-action]");
      if (!btn) return;
      var action = btn.dataset.rscPlaybackAction, id = btn.dataset.id;
      if (action === "edit") {
        var newUrl = prompt("Edit playback URL or ID:", btn.dataset.playbackId || "");
        if (newUrl === null) return;
        newUrl = newUrl.trim();
        var newGameName = prompt("Edit game name:", btn.dataset.gameName || "");
        if (newGameName === null) return;
        var body = {};
        if (newUrl) body.url = newUrl;
        body.game_name = newGameName.trim();
        apiPutJson("/api/admin/referral/share-content/playback/" + id, body).then(function (res) {
          if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "update_failed"), "error"); return; }
          toast("✅ Playback record updated", "success");
          loadSharePlayback(true);
        });
      } else if (action === "activate" || action === "deactivate") {
        if (!btnStart(btn, "Working...")) return;
        apiPost("/api/admin/referral/share-content/playback/" + id + "/" + action).then(function (r) {
          btnStop(btn);
          if (r.status !== "ok") toast("❌ " + r.code, "error");
          loadSharePlayback(true);
        }).catch(function (e) {
          btnStop(btn);
          toast("❌ " + e.message, "error");
        });
      } else if (action === "delete") {
        var playbackLabel = btn.dataset.playbackId || id;
        confirmSimple("Delete Playback Record",
          'Permanently delete playback record "' + playbackLabel + '"? This cannot be undone.').then(function (ok) {
          if (!ok) return;
          if (!btnStart(btn, "Deleting...")) return;
          apiDelete("/api/admin/referral/share-content/playback/" + id).then(function (res) {
            btnStop(btn);
            if (!res.ok || res.d.status !== "ok") { toast("❌ " + (res.d && res.d.code || "delete_failed"), "error"); return; }
            toast("✅ Playback record deleted", "success");
            loadSharePlayback(true);
          }).catch(function (e) {
            btnStop(btn);
            toast("❌ " + e.message, "error");
          });
        });
      }
    });

    rscBindBulkControls(rscPlaybackCfg);
  }

  function loadSharePlayback() {
    statePanel("rsc-playback-body", "loading", "Loading playback pool…");
    return api("/api/admin/referral/share-content/playback" + rscQuery("rsc-playback-search", "rsc-playback-status-filter")).then(function (data) {
      var items = data.playback || [];
      rsc.counts.playback_link = { active: data.active_count || 0, total: data.total_count || 0 };
      var summaryEl = $("#rsc-playback-active-summary");
      if (summaryEl) summaryEl.textContent = rscSummaryText(rsc.counts.playback_link);
      rsc.rowStatus.playback_link = {};
      rsc.rowLabel.playback_link = {};
      items.forEach(function (p) { rsc.rowStatus.playback_link[p.id] = p.status; rsc.rowLabel.playback_link[p.id] = p.playback_id; });
      var known = {};
      items.forEach(function (p) { known[p.id] = true; });
      rsc.selected.playback_link.forEach(function (id) { if (!known[id]) rsc.selected.playback_link.delete(id); });
      if (!items.length) { $("#rsc-playback-body").innerHTML = emptyState("No playback records yet — add one above."); rscUpdateBulkControls(rscPlaybackCfg); return; }
      var rows = items.map(function (p) {
        var checked = rsc.selected.playback_link.has(p.id) ? "checked" : "";
        return '<tr><td><input type="checkbox" class="rsc-row-check" data-id="' + esc(p.id) + '" ' + checked + ' /></td><td>' + esc(p.playback_url) + '</td>' +
          '<td class="sub">' + esc(p.playback_id) + '</td>' +
          '<td>' + esc(p.game_name || "—") + '</td>' +
          '<td>' + rscStatusPill(p.status) + '</td>' +
          '<td>' + fmt(p.times_selected || 0) + '</td>' +
          '<td class="sub">' + (p.last_selected_at ? new Date(p.last_selected_at).toLocaleString() : "—") + '</td>' +
          '<td class="sub">' + (p.created_at ? new Date(p.created_at).toLocaleString() : "—") + '</td>' +
          '<td>' +
          '<button class="btn" data-rsc-playback-action="edit" data-id="' + esc(p.id) + '" data-playback-id="' + esc(p.playback_id) + '" data-game-name="' + esc(p.game_name || "") + '">Edit</button> ' +
          '<button class="btn" data-rsc-playback-action="' + (p.status === "active" ? "deactivate" : "activate") + '" data-id="' + esc(p.id) + '">' +
          (p.status === "active" ? "Deactivate" : "Activate") + '</button> ' +
          '<button class="btn danger" data-rsc-playback-action="delete" data-id="' + esc(p.id) + '" data-playback-id="' + esc(p.playback_id) + '">Delete</button>' +
          '</td></tr>';
      }).join("");
      $("#rsc-playback-body").innerHTML = '<table class="data-table"><thead><tr><th></th><th>Playback URL</th><th>Playback ID</th><th>Game</th><th>Status</th>' +
        '<th>Times Selected</th><th>Last Selected</th><th>Created</th><th>Actions</th></tr></thead><tbody>' + rows + '</tbody></table>';
      rscUpdateBulkControls(rscPlaybackCfg);
    }).catch(function (e) { statePanel("rsc-playback-body", "error", "Failed to load playback pool: " + e.message); });
  }

  function loadGcVerification(force) {
    statePanel("gc-verification-body", "loading", "Loading provider integration status…");
    api("/api/admin/providers").then(function (data) {
      var items = data.providers || [];
      if (!items.length) { $("#gc-verification-body").innerHTML = emptyState("No providers yet."); return; }
      var rows = items.map(function (p) {
        return '<tr><td>' + esc(p.provider_id) + '</td>' +
          '<td>' + (p.secret_configured ? '<span class="pill approved">configured</span>' : '<span class="pill rejected">missing</span>') + '</td>' +
          '<td>' + gcPill(p.active ? "active" : "inactive") + '</td></tr>';
      }).join("");
      $("#gc-verification-body").innerHTML = '<table class="data-table"><thead><tr><th>Provider</th><th>Secret</th><th>Status</th></tr></thead><tbody>' + rows + '</tbody></table>' +
        '<div class="sub" style="margin-top:8px;">Signature failures / nonce replay counters are recorded per provider in campaign_provider_integration_status and surfaced here once a provider has activity.</div>';
    }).catch(function (e) { statePanel("gc-verification-body", "error", "Failed to load verification status: " + e.message); });
  }

  function loadAffiliatePools(force) {
    statePanel("affiliate-pools-summary-body", "loading", "Loading pool summary…");
    fetch("/v2/miniapp/admin/pools/summary", { credentials: "same-origin", headers: { "Accept": "application/json" } })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var items = data.items || [];
        if (!items.length) {
          $("#affiliate-pools-summary-body").innerHTML = emptyState("No voucher pools configured yet.");
          return;
        }
        $("#affiliate-pools-summary-body").innerHTML = '<div class="card-grid">' + items.map(function (p) {
          // claimable_available is the issuance-authoritative count (same
          // rules _claim_voucher_from_pool applies); raw_available/available
          // is the naive "status: available" row count and is shown only as
          // a diagnostic so this card can never claim "Healthy" when the
          // bot actually sees zero claimable vouchers.
          //
          // claimable_available === null means the claimability check
          // itself failed server-side (blocking_reason
          // "claimability_check_failed") — this must render as Unknown,
          // never fall back to raw_available/available as if it were
          // claimable, or a genuinely-blocked pool could read as Healthy
          // whenever the check errors out.
          var unknown = p.claimable_available === null || p.blocking_reason === "claimability_check_failed";
          var hasClaimable = typeof p.claimable_available === "number";
          var claimable = hasClaimable ? p.claimable_available : 0;
          var raw = typeof p.raw_available === "number" ? p.raw_available : (typeof p.available === "number" ? p.available : null);
          var issued = typeof p.issued === "number" ? p.issued : 0;
          var total = (raw || 0) + issued;
          var pctIssued = total > 0 ? Math.round((issued / total) * 100) : 0;
          var blocked = !unknown && hasClaimable && claimable <= 0 && !!p.blocking_reason;
          var sev = unknown ? "neutral" : (blocked ? "red" : (claimable < 10 ? "red" : (claimable < 50 ? "yellow" : "green")));
          var pillClass = unknown ? "neutral" : (sev === "red" ? "rejected" : sev === "yellow" ? "pending" : "approved");
          var pillLabel = unknown ? "Unknown" : (blocked ? "Blocked" : (sev === "red" ? "Low" : sev === "yellow" ? "Watch" : "Healthy"));
          var mismatchNote = unknown
            ? '<div class="sub" style="margin-top:4px;">' + (raw === null ? "" : fmt(raw) + ' stored · ') + 'claimability check failed</div>'
            : (hasClaimable && raw !== claimable
              ? '<div class="sub" style="margin-top:4px;">' + fmt(raw) + ' stored' + (p.blocking_reason ? " · " + esc(p.blocking_reason) : "") + '</div>'
              : '');
          return '<div class="kpi">' +
            '<div style="display:flex;justify-content:space-between;align-items:center;">' +
            '<div class="label">' + esc(p.pool_id) + '</div>' +
            '<span class="pill ' + pillClass + '">' +
            pillLabel + '</span>' +
            '</div>' +
            '<div class="value">' + (unknown ? "—" : fmt(claimable)) + '</div>' +
            '<div class="sub">claimable · ' + fmt(issued) + ' issued</div>' +
            mismatchNote +
            '<div class="progress-row"><div class="bar-wrap"><div class="bar" style="width:' + pctIssued + '%;"></div></div>' +
            '<div class="progress-label">' + pctIssued + '% used</div></div>' +
            '<div class="sub" style="margin-top:8px;">' + esc(p.display_label || "—") +
            (p.value_hint ? " · " + esc(p.value_hint) : "") + (p.currency ? " " + esc(p.currency) : "") + '</div>' +
            '</div>';
        }).join("") + '</div>';
      })
      .catch(function (e) { statePanel("affiliate-pools-summary-body", "error", "Failed to load pool summary: " + e.message); });
  }

  function bindAffiliatePools() {
    var uploadBtn = $("#ap-upload-btn");
    if (!uploadBtn) return;
    uploadBtn.addEventListener("click", function () {
      var resultEl = $("#ap-upload-result");
      var codesText = $("#ap-codes").value || "";
      if (!codesText.trim()) { resultEl.textContent = "Please provide codes."; return; }
      var payload = {
        pool_id: $("#ap-pool-id").value,
        codes_text: codesText,
        display_label: ($("#ap-display-label").value || "").trim() || null,
        value_hint: ($("#ap-value-hint").value || "").trim() || null,
        currency: ($("#ap-currency").value || "").trim() || null,
      };
      uploadBtn.disabled = true;
      resultEl.textContent = "Uploading…";
      fetch("/v2/miniapp/admin/pools/upload", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); })
        .then(function (res) {
          if (!res.ok || res.d.status !== "ok") throw new Error(res.d.reason || "unknown");
          resultEl.textContent = "Inserted " + res.d.inserted + " / " + res.d.received + " code(s) into " + res.d.pool_id + ".";
          toast("✅ Inserted " + res.d.inserted + "/" + res.d.received + " code(s) into " + res.d.pool_id, "success");
          $("#ap-codes").value = "";
          loadAffiliatePools(true);
        })
        .catch(function (e) { resultEl.textContent = "Upload failed: " + e.message; toast("❌ Upload failed: " + e.message, "error"); })
        .finally(function () { uploadBtn.disabled = false; });
    });
  }

  // ---------- Voucher Batches (scheduled T1-T4 affiliate + WELCOME pools) ----------
  var abItemsCache = {};
  state.abEditingBatchId = null;

  var AFF_BATCH_ERROR_MESSAGES = {
    batch_window_overlap: "This batch's window overlaps an existing scheduled or active batch for this pool. Adjust the start/end time, or edit/disable the conflicting batch first.",
    invalid_start_at: "Start date/time could not be parsed. Please use the date/time picker.",
    invalid_end_at: "End date/time could not be parsed. Please use the date/time picker.",
    end_before_start: "End time must be after the start time.",
    invalid_pool_id: "Choose a valid pool (T1-T4 or WELCOME).",
    invalid_batch_name: "Batch name is required.",
    no_codes: "No valid voucher codes were provided. Paste at least one code.",
    duplicate_codes: "All submitted codes were already in the system — no new codes were inserted.",
    unauthorized: "Your admin session has expired. Please log in again.",
    batch_not_found: "This batch could not be found — it may have been removed.",
    active_batch_edit_restricted: "This batch already has issued vouchers, so its schedule can no longer be changed. You can still rename it, edit notes, or disable distribution.",
    upload_failed: "The upload failed partway through and was marked Failed for review. Use Reconcile to check for any inserted codes, or re-upload.",
    target_batch_failed_cannot_enable: "This batch failed to upload and cannot be re-enabled. Use Reconcile or re-upload instead.",
    batch_disabled: "This batch is disabled. Re-enable it before adding codes.",
    batch_not_ready: "This batch is not ready to accept new codes yet (still uploading or failed). Reconcile it first.",
    batch_expired: "This batch's schedule window has already ended and can no longer accept new codes.",
    database_error: "A database error occurred while adding codes. Codes already inserted before the failure remain saved; please retry with the remaining codes.",
  };

  function abErrorMessage(d) {
    var code = d && d.code;
    return (code && AFF_BATCH_ERROR_MESSAGES[code]) || (d && d.message) || "Something went wrong. Please try again.";
  }

  function abParseCodesPreview(text) {
    var raw = String(text || "").split(/[\r\n,]+/);
    var seen = {};
    var unique = [];
    var duplicates = 0;
    var invalid = 0;
    raw.forEach(function (item) {
      var code = item.trim();
      if (!code) return;
      if (/\s/.test(code)) { invalid++; return; }
      if (seen[code]) { duplicates++; return; }
      seen[code] = true;
      unique.push(code);
    });
    return { unique: unique, duplicates: duplicates, invalid: invalid };
  }

  function abLocalInputToKlString(value) {
    if (!value) return "";
    return value.replace("T", " ") + ":00";
  }

  // T1-T4 are monthly-entitlement affiliate tiers: their claimability rule
  // requires a batch window that exactly matches a full KL calendar month,
  // so the form pins them to an entitlement-month picker instead of free
  // start/end fields — an admin-typed window (even off by a minute, e.g.
  // "00:01"/"23:59") silently fails the full-month-containment check used
  // by get_claimable_pool_inventory/_resolve_monthly_ledger_target and the
  // batch reports 0 claimable despite having stock. WELCOME has no monthly
  // entitlement concept and keeps its existing free-form start/end window.
  // Mirrors affiliate_reward_plans.ENTITLEMENT_MONTH_POOL_IDS exactly.
  // test_affiliate_pool_catalogue.py parses this literal and fails if it
  // drifts from the backend, so a pool can never again reach production
  // with free-form window fields when it requires the month picker.
  var AB_ENTITLEMENT_MONTH_POOLS = { T1: true, T2: true, T3: true, T4: true, T5: true, AFFILIATE_5: true, AFFILIATE_10: true, AFFILIATE_50: true };

  function abIsEntitlementMonthPool(poolId) {
    return !!AB_ENTITLEMENT_MONTH_POOLS[poolId];
  }

  function abUpdatePoolFieldVisibility() {
    var poolId = $("#ab-pool-id").value;
    var useMonth = abIsEntitlementMonthPool(poolId);
    $("#ab-month-row").style.display = useMonth ? "block" : "none";
    $("#ab-window-row").style.display = useMonth ? "none" : "grid";
  }

  function abMonthInputToYyyymm(value) {
    // <input type="month"> yields "YYYY-MM"
    if (!value) return "";
    return value.replace("-", "");
  }

  function abUpdatePreview() {
    var el = $("#ab-preview");
    if (!el) return;
    var codesText = $("#ab-codes").value || "";
    var parsed = abParseCodesPreview(codesText);
    var tier = $("#ab-pool-id").value;
    var useMonth = abIsEntitlementMonthPool(tier);
    var windowLabel;
    if (useMonth) {
      var monthRaw = $("#ab-entitlement-month").value;
      windowLabel = monthRaw
        ? "Entitlement month " + esc(monthRaw) + " (full KL calendar month, start inclusive / end exclusive)"
        : "Entitlement month —";
    } else {
      var startsRaw = $("#ab-starts-at").value;
      var endsRaw = $("#ab-ends-at").value;
      windowLabel =
        "Start " + esc(startsRaw ? startsRaw.replace("T", " ") : "—") + " KL" +
        " · End " + esc(endsRaw ? endsRaw.replace("T", " ") : "—") + " KL";
    }
    var hasWindowInput = useMonth ? !!$("#ab-entitlement-month").value : !!($("#ab-starts-at").value || $("#ab-ends-at").value);
    if (!codesText.trim() && !hasWindowInput) { el.style.display = "none"; return; }
    el.style.display = "block";
    el.innerHTML =
      "<b>Preview:</b> Tier " + esc(tier) +
      " · " + windowLabel +
      " · " + parsed.unique.length + " unique code(s)" +
      (parsed.duplicates ? " · " + parsed.duplicates + " duplicate code(s) in pasted input" : "") +
      (parsed.invalid ? " · " + parsed.invalid + " invalid token(s) will be ignored" : "");
  }

  function abShowFormError(msg) {
    var el = $("#ab-form-error");
    el.textContent = msg;
    el.style.display = "block";
  }

  function abResetForm() {
    state.abEditingBatchId = null;
    $("#ab-form-title").textContent = "Create Batch";
    $("#ab-create-btn").textContent = "Create Batch";
    $("#ab-cancel-edit-btn").style.display = "none";
    $("#ab-batch-name").value = "";
    $("#ab-pool-id").value = "T1";
    $("#ab-pool-id").disabled = false;
    $("#ab-entitlement-month").value = "";
    $("#ab-starts-at").value = "";
    $("#ab-ends-at").value = "";
    $("#ab-codes").value = "";
    $("#ab-codes").disabled = false;
    $("#ab-notes").value = "";
    $("#ab-form-error").style.display = "none";
    $("#ab-preview").style.display = "none";
    abUpdatePoolFieldVisibility();
  }

  function abFillFormForEdit(item) {
    state.abEditingBatchId = item.batch_id;
    $("#ab-form-title").textContent = "Edit Batch Schedule — " + item.batch_name;
    $("#ab-create-btn").textContent = "Save Changes";
    $("#ab-cancel-edit-btn").style.display = "inline-block";
    $("#ab-batch-name").value = item.batch_name || "";
    $("#ab-pool-id").value = item.pool_id;
    $("#ab-pool-id").disabled = true;
    abUpdatePoolFieldVisibility();
    if (abIsEntitlementMonthPool(item.pool_id)) {
      var em = item.entitlement_month || "";
      $("#ab-entitlement-month").value = em.length === 6 ? em.slice(0, 4) + "-" + em.slice(4, 6) : "";
      $("#ab-starts-at").value = "";
      $("#ab-ends-at").value = "";
    } else {
      $("#ab-entitlement-month").value = "";
      $("#ab-starts-at").value = item.starts_at_kl ? item.starts_at_kl.slice(0, 16) : "";
      $("#ab-ends-at").value = item.ends_at_kl ? item.ends_at_kl.slice(0, 16) : "";
    }
    $("#ab-codes").value = "";
    $("#ab-codes").disabled = true;
    $("#ab-notes").value = item.notes || "";
    $("#ab-form-error").style.display = "none";
    $("#ab-preview").style.display = "none";
    var section = $("#view-affiliateBatches");
    if (section) section.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function abSubmit() {
    var btn = $("#ab-create-btn");
    var isEdit = !!state.abEditingBatchId;
    var name = ($("#ab-batch-name").value || "").trim();
    var poolId = $("#ab-pool-id").value;
    var useMonth = abIsEntitlementMonthPool(poolId);
    var monthRaw = $("#ab-entitlement-month").value;
    var startsRaw = $("#ab-starts-at").value;
    var endsRaw = $("#ab-ends-at").value;
    var codesText = $("#ab-codes").value || "";
    var notes = ($("#ab-notes").value || "").trim();

    $("#ab-form-error").style.display = "none";

    if (!name) { abShowFormError("Batch name is required."); return; }
    if (useMonth) {
      if (!monthRaw) { abShowFormError("Entitlement month is required for " + poolId + " batches."); return; }
    } else if (!startsRaw || !endsRaw) {
      abShowFormError("Start and end date/time are required.");
      return;
    }
    if (!isEdit) {
      var parsed = abParseCodesPreview(codesText);
      if (!parsed.unique.length) { abShowFormError("No valid voucher codes were provided. Paste at least one code."); return; }
    }

    var payload = {
      batch_name: name,
      pool_id: poolId,
      timezone: "Asia/Kuala_Lumpur",
      notes: notes || null,
    };
    if (useMonth) {
      payload.entitlement_month = abMonthInputToYyyymm(monthRaw);
    } else {
      payload.starts_at_local = abLocalInputToKlString(startsRaw);
      payload.ends_at_local = abLocalInputToKlString(endsRaw);
    }
    if (!isEdit) payload.codes = codesText;

    var doSubmit = function () {
      if (!btnStart(btn, isEdit ? "Saving…" : "Uploading…")) return;
      var req = isEdit
        ? apiPatchJson("/api/admin/affiliate-voucher-batches/" + encodeURIComponent(state.abEditingBatchId), payload)
        : apiPostJson("/api/admin/affiliate-voucher-batches", payload);
      req.then(function (res) {
        btnStop(btn);
        if (!res.ok || res.d.ok === false) {
          abShowFormError(abErrorMessage(res.d));
          return;
        }
        toast(isEdit ? "✅ Batch schedule updated" : "✅ Batch created (" + ((res.d.counts && res.d.counts.inserted) || 0) + " code(s) inserted)", "success");
        abResetForm();
        loadAffiliateBatches(true);
      }).catch(function (e) {
        btnStop(btn);
        abShowFormError("Network error: " + e.message + " — your form entries have been kept, please retry.");
      });
    };

    // Editing an existing batch's schedule is the one case that gets an
    // explicit extra confirmation, per the "dangerous overlap / editing an
    // active batch" carve-out — everything else submits on one click.
    if (isEdit) {
      if (!window.confirm("Save changes to this batch's schedule/details?")) return;
    }
    doSubmit();
  }

  function abSetDisabled(batchId, disabled) {
    if (disabled && !window.confirm("Disable distribution for this batch? No further vouchers will be issued from it until re-enabled.")) return;
    apiPatchJson("/api/admin/affiliate-voucher-batches/" + encodeURIComponent(batchId), { distribution_disabled: disabled })
      .then(function (res) {
        if (!res.ok || res.d.ok === false) { toast("❌ " + abErrorMessage(res.d), "error"); return; }
        toast(disabled ? "✅ Distribution disabled" : "✅ Distribution re-enabled", "success");
        loadAffiliateBatches(true);
      })
      .catch(function (e) { toast("❌ " + e.message, "error"); });
  }

  function abReconcileBatch(batchId) {
    apiPostJson("/api/admin/affiliate-voucher-batches/" + encodeURIComponent(batchId) + "/reconcile", {})
      .then(function (res) {
        if (!res.ok || res.d.ok === false) { toast("❌ " + abErrorMessage(res.d), "error"); return; }
        var newStatus = res.d.batch && res.d.batch.status;
        toast("✅ Reconciled — now " + (newStatus || "updated"), "success");
        loadAffiliateBatches(true);
      })
      .catch(function (e) { toast("❌ " + e.message, "error"); });
  }

  function abViewBatch(batchId) {
    var existing = $("#ab-detail-" + batchId);
    if (existing) { existing.remove(); return; }
    fetch("/api/admin/affiliate-voucher-batches/" + encodeURIComponent(batchId), { credentials: "same-origin", headers: { "Accept": "application/json" } })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (!d.ok) { toast("❌ " + abErrorMessage(d), "error"); return; }
        var row = $("#ab-row-" + batchId);
        if (!row) return;
        var detail = document.createElement("tr");
        detail.id = "ab-detail-" + batchId;
        detail.className = "row-detail";
        var vouchers = (d.batch && d.batch.vouchers) || [];
        var codesHtml = vouchers.map(function (v) {
          return '<span class="pill ' + (v.status === "issued" ? "issued" : "ok") + '" style="margin:2px;">' + esc(v.code) + (v.status === "issued" ? " (issued)" : "") + '</span>';
        }).join("") || "No codes on this page.";
        detail.innerHTML = '<td colspan="10"><div style="padding:10px;">' +
          '<div style="margin-bottom:6px;font-size:12px;color:var(--muted);">Showing ' + vouchers.length + ' of ' + fmt(d.batch.vouchers_total) + ' code(s) · created by ' + esc(d.batch.created_by) + ' at ' + dt(d.batch.created_at) + '</div>' +
          codesHtml + '</div></td>';
        row.parentNode.insertBefore(detail, row.nextSibling);
      })
      .catch(function (e) { toast("❌ " + e.message, "error"); });
  }

  function abOpenAddCodesModal(batchId) {
    var item = abItemsCache[batchId];
    if (!item) return;
    var overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML =
      '<div class="modal-box" style="max-width:520px;">' +
      "<h3>+ Add Codes — " + esc(item.batch_name) + "</h3>" +
      '<p class="sub">Pool: ' + esc(item.pool_id) + " · Available: " + fmt(item.available_count) + " · Uploaded: " + fmt(item.uploaded_count) + "</p>" +
      '<label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px;">Voucher Codes</label>' +
      '<textarea id="ab-addcodes-codes" rows="8" style="width:100%;padding:8px;border:1px solid var(--border);border-radius:6px;background:var(--card-bg);color:var(--text);box-sizing:border-box;resize:vertical;font-family:monospace;"></textarea>' +
      '<p class="sub" style="margin-top:4px;">One code per line, comma-separated, or pasted CSV column</p>' +
      '<div id="ab-addcodes-error" style="display:none;background:rgba(255,107,107,0.12);border:1px solid var(--bad);color:var(--bad);border-radius:8px;padding:8px 12px;font-size:12px;margin-top:8px;"></div>' +
      '<div class="modal-actions">' +
      '<button class="btn" id="ab-addcodes-cancel">Cancel</button>' +
      '<button class="btn primary" id="ab-addcodes-submit">Add Codes</button>' +
      "</div></div>";
    document.body.appendChild(overlay);
    function done() { overlay.remove(); }
    overlay.querySelector("#ab-addcodes-cancel").addEventListener("click", done);
    overlay.addEventListener("click", function (e) { if (e.target === overlay) done(); });

    var errEl = overlay.querySelector("#ab-addcodes-error");
    function showError(msg) { errEl.textContent = msg; errEl.style.display = "block"; }

    var submitBtn = overlay.querySelector("#ab-addcodes-submit");
    submitBtn.addEventListener("click", function () {
      var codesText = overlay.querySelector("#ab-addcodes-codes").value || "";
      var parsed = abParseCodesPreview(codesText);
      if (!parsed.unique.length) { showError("No valid voucher codes were provided. Paste at least one code."); return; }
      errEl.style.display = "none";
      if (!btnStart(submitBtn, "Adding…")) return;
      apiPostJson("/api/admin/affiliate-voucher-batches/" + encodeURIComponent(batchId) + "/add-codes", { codes: codesText })
        .then(function (res) {
          btnStop(submitBtn);
          var d = res.d || {};
          if (!res.ok || d.ok === false) {
            // A mid-loop DB failure (code: database_error) still inserted
            // some codes before it hit the error — surface that count so
            // the admin doesn't assume the whole submission was a no-op,
            // and refresh the table since inventory already changed.
            var partial = d.inserted_count > 0;
            showError(abErrorMessage(d) + (partial ? " (" + fmt(d.inserted_count) + " code(s) were already inserted before the failure.)" : ""));
            if (partial) loadAffiliateBatches(true);
            return;
          }
          done();
          toast(
            "✅ " + fmt(d.inserted_count) + " codes added to " + item.batch_name + "." +
            (d.duplicate_count ? " " + fmt(d.duplicate_count) + " duplicates skipped." : ""),
            "success"
          );
          loadAffiliateBatches(true);
        })
        .catch(function (e) { btnStop(submitBtn); showError("Network error: " + e.message + " — please retry."); });
    });
  }

  function abStatusPillClass(status) {
    return {
      active: "active", scheduled: "upcoming", exhausted: "paused", expired: "expired",
      disabled: "rejected", uploading: "pending", failed: "rejected", legacy_unbounded: "neutral",
    }[status] || "neutral";
  }

  function abFormatDuration(ms) {
    if (ms <= 0) return "0m";
    var mins = Math.floor(ms / 60000);
    var days = Math.floor(mins / 1440); mins -= days * 1440;
    var hours = Math.floor(mins / 60); mins -= hours * 60;
    var parts = [];
    if (days) parts.push(days + "d");
    if (hours) parts.push(hours + "h");
    if (!days && mins) parts.push(mins + "m");
    return parts.length ? parts.join(" ") : "0m";
  }

  function abTimeRemaining(item, nowIso) {
    var now = nowIso ? new Date(nowIso) : new Date();
    if (item.status === "scheduled" && item.starts_at_utc) {
      return "Starts in " + abFormatDuration(new Date(item.starts_at_utc) - now);
    }
    if ((item.status === "active" || item.status === "exhausted") && item.ends_at_utc) {
      return "Ends in " + abFormatDuration(new Date(item.ends_at_utc) - now);
    }
    if (item.status === "expired") return "Ended";
    return "—";
  }

  function renderAffiliateBatches(data) {
    var items = data.items || [];
    var nowIso = data.server_now_utc;
    var legacy = data.legacy_summary || [];
    var legacyFallback = data.legacy_fallback || [];
    var legacyEl = $("#ab-legacy-summary");

    var fallbackByPool = {};
    legacyFallback.forEach(function (f) { fallbackByPool[f.pool_id] = f; });
    var fallbackHtml = legacyFallback.length
      ? '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px;">' + legacyFallback.map(function (f) {
          return f.entered_scheduled_mode
            ? '<span class="pill rejected">' + esc(f.pool_id) + ': legacy fallback permanently OFF (scheduled-batch mode active)</span>'
            : '<span class="pill active">' + esc(f.pool_id) + ': legacy fallback allowed (transitional)</span>';
        }).join("") + '</div>'
      : "";

    if (!legacy.length) {
      legacyEl.innerHTML = fallbackHtml + emptyState("No legacy undated voucher codes for this filter.");
    } else {
      legacyEl.innerHTML = fallbackHtml + '<div class="card-grid">' + legacy.map(function (l) {
        var blocked = fallbackByPool[l.pool_id] && fallbackByPool[l.pool_id].entered_scheduled_mode;
        return '<div class="kpi"><div class="label">' + esc(l.pool_id) + ' <span class="pill ' + (blocked ? "rejected" : "neutral") + '">' + (blocked ? "legacy blocked" : "legacy") + '</span></div>' +
          '<div class="value">' + fmt(l.available) + '</div><div class="sub">available (undated) · ' + fmt(l.issued) + ' issued' +
          (blocked ? ' · <b>not distributable — scheduled batch mode active</b>' : '') + '</div></div>';
      }).join("") + '</div>';
    }

    abItemsCache = {};
    items.forEach(function (item) { abItemsCache[item.batch_id] = item; });

    if (!items.length) {
      $("#ab-body").innerHTML = emptyState("No batches match these filters.");
      return;
    }
    var rows = items.map(function (item) {
      var needsReconcile = item.status === "uploading" || item.status === "failed";
      // Mirror the backend's add_codes_to_batch guards exactly: disabled,
      // still-uploading, failed, and expired batches all reject a top-up
      // server-side, so gate the button here instead of letting the admin
      // open the modal only to have submission fail.
      var AB_ADD_CODES_BLOCK_REASON = {
        disabled: "Re-enable this batch before adding codes.",
        uploading: "This batch is still uploading.",
        failed: "This batch failed to upload — reconcile it first.",
        expired: "This batch's window has ended and can no longer accept codes.",
      };
      var addCodesBlockReason = AB_ADD_CODES_BLOCK_REASON[item.status];
      var failureDetail = item.status === "failed"
        ? '<div class="sub" style="color:var(--bad);margin-top:4px;">Submitted ' + fmt(item.submitted_count) + ' · Inserted ' + fmt(item.inserted_count) +
          ' · Duplicates ' + fmt(item.duplicate_count) + ' · Invalid ' + fmt(item.invalid_count) +
          (item.upload_error_code ? ' · Reason: ' + esc(item.upload_error_code) : '') + '</div>'
        : "";
      var entitlementLabel = item.pool_id === "WELCOME" ? "Entitlement reference: Welcome eligibility time" : "Entitlement month: " + esc(item.entitlement_month || "—");
      return '<tr id="ab-row-' + esc(item.batch_id) + '">' +
        '<td>' + esc(item.batch_name) + (item.notes ? '<div class="sub">' + esc(item.notes) + '</div>' : '') +
          (item.entitlement_month || item.pool_id === "WELCOME" ? '<div class="sub">' + entitlementLabel + '</div>' : '') +
          failureDetail +
        '</td>' +
        '<td>' + esc(item.pool_id) + '</td>' +
        '<td>' + esc((item.starts_at_kl || "").replace("T", " ").slice(0, 16)) + '</td>' +
        '<td>' + esc((item.ends_at_kl || "").replace("T", " ").slice(0, 16)) + '</td>' +
        '<td><span class="pill ' + abStatusPillClass(item.status) + '">' + esc(item.status) + '</span></td>' +
        '<td class="num">' + fmt(item.uploaded_count) + '</td>' +
        '<td class="num">' + fmt(item.available_count) + '</td>' +
        '<td class="num">' + fmt(item.issued_count) + '</td>' +
        '<td>' + esc(abTimeRemaining(item, nowIso)) + '</td>' +
        '<td>' +
          '<button class="btn" data-ab-view="' + esc(item.batch_id) + '">View</button> ' +
          (addCodesBlockReason
            ? '<button class="btn" disabled title="' + esc(addCodesBlockReason) + '">+ Add Codes</button> '
            : '<button class="btn" data-ab-addcodes="' + esc(item.batch_id) + '">+ Add Codes</button> ') +
          '<button class="btn" data-ab-edit="' + esc(item.batch_id) + '">Edit schedule</button> ' +
          (needsReconcile ? '<button class="btn" data-ab-reconcile="' + esc(item.batch_id) + '">Reconcile</button> ' : '') +
          (item.status === "failed"
            ? ""
            : (item.distribution_disabled
                ? '<button class="btn" data-ab-enable="' + esc(item.batch_id) + '">Re-enable</button>'
                : '<button class="btn" data-ab-disable="' + esc(item.batch_id) + '">Disable</button>')) +
        '</td>' +
        '</tr>';
    }).join("");
    $("#ab-body").innerHTML = '<table class="data-table"><thead><tr>' +
      '<th>Batch</th><th>Pool</th><th>Start (KL)</th><th>End (KL)</th><th>Status</th><th class="num">Uploaded</th><th class="num">Available</th><th class="num">Issued</th><th>Remaining</th><th>Actions</th>' +
      '</tr></thead><tbody>' + rows + '</tbody></table>';
  }

  function loadAffiliateBatches(force) {
    var qs = new URLSearchParams();
    var poolId = $("#ab-filter-pool") ? $("#ab-filter-pool").value : "";
    var status = $("#ab-filter-status") ? $("#ab-filter-status").value : "";
    var month = $("#ab-filter-month") ? ($("#ab-filter-month").value || "").trim() : "";
    var includeExpired = $("#ab-filter-include-expired") ? $("#ab-filter-include-expired").checked : false;
    if (poolId) qs.set("pool_id", poolId);
    if (status) qs.set("status", status);
    if (month) qs.set("month", month);
    if (includeExpired) qs.set("include_expired", "1");
    statePanel("ab-body", "loading", "Loading affiliate voucher batches…");
    fetch("/api/admin/affiliate-voucher-batches" + (qs.toString() ? "?" + qs.toString() : ""), {
      credentials: "same-origin", headers: { "Accept": "application/json" },
    })
      .then(function (r) {
        if (r.status === 401) { window.location.href = "/static/admin-login.html"; throw new Error("unauthorized"); }
        return r.json();
      })
      .then(function (data) {
        if (!data.ok) { statePanel("ab-body", "error", abErrorMessage(data)); return; }
        renderAffiliateBatches(data);
      })
      .catch(function (e) { statePanel("ab-body", "error", "Failed to load batches: " + e.message); });
  }

  function bindAffiliateBatches() {
    var createBtn = $("#ab-create-btn");
    if (!createBtn) return;
    createBtn.addEventListener("click", abSubmit);
    $("#ab-cancel-edit-btn").addEventListener("click", abResetForm);
    ["#ab-codes", "#ab-starts-at", "#ab-ends-at", "#ab-pool-id", "#ab-entitlement-month"].forEach(function (sel) {
      var el = $(sel);
      if (el) el.addEventListener("input", abUpdatePreview);
    });
    $("#ab-pool-id").addEventListener("change", function () {
      abUpdatePoolFieldVisibility();
      abUpdatePreview();
    });
    abUpdatePoolFieldVisibility();
    $("#ab-refresh-btn").addEventListener("click", function () { loadAffiliateBatches(true); });
    ["#ab-filter-pool", "#ab-filter-status", "#ab-filter-include-expired"].forEach(function (sel) {
      var el = $(sel);
      if (el) el.addEventListener("change", function () { loadAffiliateBatches(true); });
    });
    $("#ab-filter-month").addEventListener("change", function () { loadAffiliateBatches(true); });
    $("#ab-body").addEventListener("click", function (e) {
      var viewBtn = e.target.closest("[data-ab-view]");
      if (viewBtn) { abViewBatch(viewBtn.dataset.abView); return; }
      var addCodesBtn = e.target.closest("[data-ab-addcodes]");
      if (addCodesBtn) { abOpenAddCodesModal(addCodesBtn.dataset.abAddcodes); return; }
      var editBtn = e.target.closest("[data-ab-edit]");
      if (editBtn) { var item = abItemsCache[editBtn.dataset.abEdit]; if (item) abFillFormForEdit(item); return; }
      var disableBtn = e.target.closest("[data-ab-disable]");
      if (disableBtn) { abSetDisabled(disableBtn.dataset.abDisable, true); return; }
      var enableBtn = e.target.closest("[data-ab-enable]");
      if (enableBtn) { abSetDisabled(enableBtn.dataset.abEnable, false); return; }
      var reconcileBtn = e.target.closest("[data-ab-reconcile]");
      if (reconcileBtn) { abReconcileBatch(reconcileBtn.dataset.abReconcile); return; }
    });
  }

  // ---------- Pending Affiliate Rewards (migrated from legacy MiniApp admin panel) ----------
  function loadAffiliatePending(force) {
    var activeBtn = $("#affp-status-filter .active");
    var status = (activeBtn && activeBtn.dataset.status) || "PENDING_REVIEW";
    statePanel("affp-body", "loading", "Loading pending affiliate rewards…");
    fetch("/v2/miniapp/admin/affiliate/pending?status=" + encodeURIComponent(status), { credentials: "same-origin", headers: { "Accept": "application/json" } })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var items = data.items || [];
        if (!items.length) {
          statePanel("affp-body", "empty", "No entries for " + status + ".");
          return;
        }
        var rows = items.map(function (it) {
          return "<tr>" +
            "<td>" + fmt(it.user_id) + "</td>" +
            "<td>" + esc(it.tier || "—") + "</td>" +
            "<td>" + esc(it.year_month || "—") + "</td>" +
            "<td>" + esc(it.eligible_tier || "—") + "</td>" +
            "<td>" + fmt(it.qualified_month) + "</td>" +
            "<td>" + esc((it.risk_flags || []).join(", ") || "—") + "</td>" +
            "<td>" +
            '<button class="btn primary" data-affp-op="approve" data-ledger-id="' + esc(it.ledger_id) + '">Approve</button> ' +
            '<button class="btn danger" data-affp-op="reject" data-ledger-id="' + esc(it.ledger_id) + '">Reject</button>' +
            "</td></tr>";
        }).join("");
        $("#affp-body").innerHTML =
          '<table class="data-table"><thead><tr><th>User ID</th><th>Tier</th><th>Month</th><th>Eligible Tier</th><th>Qualified (Month)</th><th>Risk Flags</th><th>Actions</th></tr></thead><tbody>' +
          rows + "</tbody></table>";
      })
      .catch(function (e) { statePanel("affp-body", "error", "Failed to load pending rewards: " + e.message); });
  }

  function bindAffiliatePending() {
    var refreshBtn = $("#affp-refresh-btn");
    if (refreshBtn) refreshBtn.addEventListener("click", function () { loadAffiliatePending(true); });

    $all("#affp-status-filter button").forEach(function (b) {
      b.addEventListener("click", function () {
        $all("#affp-status-filter button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadAffiliatePending(true);
      });
    });

    document.addEventListener("click", function (event) {
      var btn = event.target && event.target.closest && event.target.closest("[data-affp-op]");
      if (!btn) return;
      var op = btn.dataset.affpOp;
      var ledgerId = btn.dataset.ledgerId;
      if (op === "reject") {
        var reason = prompt("Reason for rejection (optional):") || "";
        btn.disabled = true;
        fetch("/v2/miniapp/admin/affiliate/" + encodeURIComponent(ledgerId) + "/reject", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ reason: reason }),
        }).then(function (r) { return r.json(); })
          .then(function (d) {
            if (d.status === "ok") { toast("✅ Affiliate reward rejected", "success"); loadAffiliatePending(true); }
            else banner("❌ Reject failed: " + (d.reason || "unknown"), "error");
          })
          .catch(function (e) { banner("❌ Reject failed: " + e.message, "error"); })
          .finally(function () { btn.disabled = false; });
      } else if (op === "approve") {
        if (!confirm("Approve this affiliate reward? A voucher may be issued immediately.")) return;
        btn.disabled = true;
        fetch("/v2/miniapp/admin/affiliate/" + encodeURIComponent(ledgerId) + "/approve", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
        }).then(function (r) { return r.json(); })
          .then(function (d) {
            if (d.status === "ok") { toast("✅ Affiliate reward approved", "success"); loadAffiliatePending(true); }
            else banner("❌ Approve failed: " + (d.reason || "unknown"), "error");
          })
          .catch(function (e) { banner("❌ Approve failed: " + e.message, "error"); })
          .finally(function () { btn.disabled = false; });
      }
    });
  }

  // ---------- Join Requests (read-only; no manual approve/reject endpoint exists) ----------
  function bindJoinRequests() {
    var btn = $("#jr-refresh-btn");
    if (!btn) return;
    btn.addEventListener("click", function () {
      statePanel("jr-body", "loading", "Loading join requests…");
      fetch("/api/join_requests", { credentials: "same-origin", headers: { "Accept": "application/json" } })
        .then(function (r) { return r.json(); })
        .then(function (data) {
          if (!data.success) throw new Error(data.message || data.error || "unknown");
          var items = data.requests || [];
          if (!items.length) { statePanel("jr-body", "empty", "No pending join requests."); return; }
          var rows = items.map(function (it) {
            return "<tr><td>" + fmt(it.user_id) + "</td><td>" + esc(it.username || "—") + "</td></tr>";
          }).join("");
          $("#jr-body").innerHTML =
            '<table class="data-table"><thead><tr><th>User ID</th><th>Username</th></tr></thead><tbody>' + rows + "</tbody></table>";
        })
        .catch(function (e) { statePanel("jr-body", "error", "Failed to load join requests: " + e.message); });
    });
  }

  // ---------- Add / Reduce XP (migrated from legacy MiniApp admin panel) ----------
  function bindXpAdjust() {
    var btn = $("#xp-submit-btn");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var resultEl = $("#xp-result");
      var username = ($("#xp-username").value || "").trim();
      var amount = parseInt($("#xp-amount").value, 10);
      if (!username || !amount) { resultEl.textContent = "Username and a non-zero XP amount are required."; return; }
      btn.disabled = true;
      resultEl.textContent = "Submitting…";
      fetch("/api/add_xp", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: username, xp: amount }),
      }).then(function (r) { return r.json().then(function (d) { return { ok: r.ok, d: d }; }); })
        .then(function (res) {
          if (!res.ok || !res.d.success) throw new Error(res.d.message || "unknown");
          resultEl.textContent = res.d.message || "XP updated.";
          toast("✅ " + (res.d.message || "XP updated"), "success");
          $("#xp-amount").value = "";
        })
        .catch(function (e) { resultEl.textContent = "Failed: " + e.message; toast("❌ Failed to update XP: " + e.message, "error"); })
        .finally(function () { btn.disabled = false; });
    });
  }

  var VIEWS =["summary", "moduleOverview", "placeholder", "funnel", "abuse", "campaignBuilder", "campaignPerformance", "campaignIntelligence", "activeCampaigns", "draftCampaigns", "compiledDrops", "campaigns", "gcCampaigns", "gcProviders", "gcResults", "gcRewards", "gcVerification", "gcActivity", "eventBanners", "luckyGames", "vouchers", "drops", "referrals", "affiliate", "affiliatePools", "affiliateBatches", "affiliatePending", "reactivation", "audit", "segmentProbabilityConfig", "segmentRoi", "segments", "validation", "backendSegmentEngine", "voucherHunterAudit", "unclassifiedAudit", "segmentRuleSimulator", "voucherHunterQuality", "voucherHunterFalsePositive", "voucherHunterRuleSimulator", "vhPriorityImpact", "uploadPlayerPerformance", "uploadHistory", "rawExplorer", "users", "joinRequests", "xpAdjust", "settings", "referralShareContent", "referralShareEngagement", "ccComposer", "ccCalendar", "ccBoard"];

  // ---------------------------------------------------------------------
  // Information architecture: sidebar Business Modules, each with its own
  // row of top tabs. Every tab points at an existing "view" id (unchanged
  // DOM section + loader function) so no JS logic below this config needs
  // to change. `live: true` marks a tab that mutates production data.
  // `overviewKey` selects which existing summary card-grid (if any) the
  // generic Module Overview clones into its KPI row.
  // ---------------------------------------------------------------------
  var MODULES = [
    { key: "dashboard", icon: "🏠", label: "Dashboard", tabs: [
      { label: "Overview", view: "summary" },
      { label: "Live Activity", view: "audit" },
      { label: "Alerts", view: "abuse" },
      { label: "System Health", external: "/static/runtime-status.html" }
    ]},
    { key: "campaign", icon: "🎯", label: "Campaign Centre", tabs: [
      { label: "Overview", view: "moduleOverview", overviewKey: "campaign" },
      { label: "Running", view: "activeCampaigns" },
      { label: "Scheduled", view: "campaigns" },
      { label: "Drafts", view: "draftCampaigns" },
      { label: "Templates", view: "campaignBuilder", live: true },
      { label: "Performance", view: "campaignPerformance" },
      { label: "Intelligence", view: "campaignIntelligence" }
    ]},
    { key: "growth", icon: "🕹", label: "Player Campaigns", tabs: [
      { label: "Campaigns", view: "gcCampaigns" },
      { label: "Providers", view: "gcProviders" },
      { label: "Tournament Results", view: "gcResults" },
      { label: "Rewards", view: "gcRewards" },
      { label: "Verification", view: "gcVerification" },
      { label: "Activity Log", view: "gcActivity" },
      { label: "Event Banner", view: "eventBanners" },
      { label: "Lucky Games", view: "luckyGames" }
    ]},
    { key: "voucher", icon: "🎟", label: "Voucher Centre", tabs: [
      { label: "Overview", view: "moduleOverview", overviewKey: "voucher" },
      { label: "Active Drops", view: "drops", live: true },
      { label: "Voucher Pools", view: "compiledDrops" },
      { label: "Voucher Codes", view: "vouchers" },
      { label: "Settings", view: "settings", settingsCategory: "voucher_rules" }
    ]},
    { key: "community", icon: "👥", label: "Community Centre", tabs: [
      { label: "Composer", view: "ccComposer", live: true },
      { label: "Calendar", view: "ccCalendar" },
      { label: "Scheduled", view: "ccBoard", ccBoard: "scheduled" },
      { label: "Drafts", view: "ccBoard", ccBoard: "draft" },
      { label: "Pending Approval", view: "ccBoard", ccBoard: "pending_approval" },
      { label: "Published", view: "ccBoard", ccBoard: "published" },
      { label: "Poll Results", view: "ccBoard", ccBoard: "poll_results" },
      { label: "Failed", view: "ccBoard", ccBoard: "failed" },
      { label: "Members", view: "placeholder", ph: { title: "Members", desc: "Community member directory is not yet wired to an admin data source." } },
      { label: "Leaderboard", external: "/static/index.html#admin-panel" }
    ]},
    { key: "affiliate", icon: "🤝", label: "Affiliate Centre", tabs: [
      { label: "Overview", view: "affiliate" },
      { label: "Pending Approval", view: "affiliatePending", live: true },
      { label: "Voucher Pools", view: "affiliatePools", live: true },
      { label: "Voucher Batches", view: "affiliateBatches", live: true },
      { label: "Rewards", view: "placeholder", ph: { title: "Rewards", desc: "Affiliate reward ledger is not yet wired to an admin data source." } },
      { label: "Payouts", view: "placeholder", ph: { title: "Payouts", desc: "Payout batches are not yet wired to an admin data source." } },
      { label: "Analytics", view: "placeholder", ph: { title: "Analytics", desc: "Affiliate analytics is not yet wired to an admin data source." } }
    ]},
    { key: "referral", icon: "🔗", label: "Referral Centre", tabs: [
      { label: "Overview", view: "moduleOverview", overviewKey: "referral" },
      { label: "Performance", view: "referrals" },
      { label: "Share Content", view: "referralShareContent", live: true },
      { label: "Share Engagement", view: "referralShareEngagement", live: true },
      { label: "Pending", view: "placeholder", ph: { title: "Pending", desc: "Pending referral qualification queue is not yet wired to an admin data source." } },
      { label: "Rewards", view: "placeholder", ph: { title: "Rewards", desc: "Referral reward ledger is not yet wired to an admin data source." } },
      { label: "Leaderboard", view: "placeholder", ph: { title: "Leaderboard", desc: "Referral leaderboard is not yet wired to an admin data source." } },
      { label: "Analytics", view: "placeholder", ph: { title: "Analytics", desc: "Referral analytics is not yet wired to an admin data source." } }
    ]},
    { key: "welcome", icon: "🎁", label: "Welcome Journey", tabs: [
      { label: "Overview", view: "moduleOverview", overviewKey: "welcome" },
      { label: "Journey", external: "/static/welcome-journey-runtime.html" },
      { label: "Rewards", view: "placeholder", ph: { title: "Rewards", desc: "Welcome reward ledger is not yet wired to an admin data source." } },
      { label: "Funnel", view: "funnel" },
      { label: "Drop-off", view: "placeholder", ph: { title: "Drop-off", desc: "Drop-off analysis is not yet wired to an admin data source." } },
      { label: "Analytics", view: "placeholder", ph: { title: "Analytics", desc: "Welcome journey analytics is not yet wired to an admin data source." } }
    ]},
    { key: "reactivation", icon: "🔄", label: "Reactivation Centre", tabs: [
      { label: "Overview", view: "moduleOverview", overviewKey: "reactivation" },
      { label: "Campaigns", view: "reactivation", live: true },
      { label: "Eligible Users", view: "placeholder", ph: { title: "Eligible Users", desc: "Eligible-user queue is not yet wired to an admin data source." } },
      { label: "Queue", view: "placeholder", ph: { title: "Queue", desc: "Reactivation send queue is not yet wired to an admin data source." } },
      { label: "Rewards", view: "placeholder", ph: { title: "Rewards", desc: "Reactivation reward ledger is not yet wired to an admin data source." } },
      { label: "Performance", view: "placeholder", ph: { title: "Performance", desc: "Reactivation performance analytics is not yet wired to an admin data source." } }
    ]},
    { key: "segments", icon: "👤", label: "Segments", tabs: [
      { label: "Overview", view: "segments" },
      { label: "All Players", view: "users" },
      { label: "High Value", view: "placeholder", ph: { title: "High Value", desc: "Per-segment drilldown is not yet wired to an admin data source — see Overview for distribution." } },
      { label: "Low Value", view: "placeholder", ph: { title: "Low Value", desc: "Per-segment drilldown is not yet wired to an admin data source — see Overview for distribution." } },
      { label: "Active Community", view: "placeholder", ph: { title: "Active Community", desc: "Per-segment drilldown is not yet wired to an admin data source — see Overview for distribution." } },
      { label: "Ghost", view: "placeholder", ph: { title: "Ghost", desc: "Per-segment drilldown is not yet wired to an admin data source — see Overview for distribution." } },
      { label: "Simulator", view: "segmentRuleSimulator" },
      { label: "VH: Mismatch Audit", view: "voucherHunterAudit" },
      { label: "VH: Unclassified", view: "unclassifiedAudit" },
      { label: "VH: Rule Quality", view: "voucherHunterQuality" },
      { label: "VH: False Positive", view: "voucherHunterFalsePositive" },
      { label: "VH: Rule Simulator", view: "voucherHunterRuleSimulator" },
      { label: "VH: Priority Impact", view: "vhPriorityImpact" },
      { label: "Probability Config", view: "segmentProbabilityConfig" }
    ]},
    { key: "analytics", icon: "📊", label: "Analytics", tabs: [
      { label: "Executive", view: "summary" },
      { label: "Funnels", view: "funnel" },
      { label: "Revenue", view: "placeholder", ph: { title: "Revenue", desc: "Revenue analytics is not yet wired to an admin data source." } },
      { label: "Retention", view: "placeholder", ph: { title: "Retention", desc: "Retention analytics is not yet wired to an admin data source." } },
      { label: "Campaign", view: "campaignPerformance" },
      { label: "ROI", view: "segmentRoi" },
      { label: "Cohorts", view: "backendSegmentEngine" },
      { label: "Data Validation", view: "validation" }
    ]},
    { key: "automation", icon: "🤖", label: "Automation", tabs: [
      { label: "Scheduler", view: "placeholder", ph: { title: "Scheduler", desc: "Scheduler control panel is not yet wired to an admin data source." } },
      { label: "Queue", view: "placeholder", ph: { title: "Queue", desc: "Job queue view is not yet wired to an admin data source." } },
      { label: "Notifications", view: "placeholder", ph: { title: "Notifications", desc: "Notification log is not yet wired to an admin data source." } },
      { label: "Retry Jobs", view: "placeholder", ph: { title: "Retry Jobs", desc: "Retry job view is not yet wired to an admin data source." } },
      { label: "Logs", view: "uploadHistory" },
      { label: "Health", external: "/static/runtime-status.html" },
      { label: "Upload Data", view: "uploadPlayerPerformance", live: true },
      { label: "Raw Explorer", view: "rawExplorer" },
      { label: "Join Requests", view: "joinRequests" }
    ]},
    { key: "settings", icon: "⚙", label: "Settings", tabs: [
      { label: "General", view: "settings", settingsCategory: "general" },
      { label: "Feature Flags", view: "settings", settingsCategory: "feature_flags" },
      { label: "XP", view: "xpAdjust", live: true },
      { label: "Rewards", view: "settings", settingsCategory: "rewards" },
      { label: "Voucher Rules", view: "settings", settingsCategory: "voucher_rules" },
      { label: "Referral", view: "settings", settingsCategory: "referral" },
      { label: "Affiliate", view: "settings", settingsCategory: "affiliate" },
      { label: "Welcome Journey", view: "settings", settingsCategory: "welcome_journey" },
      { label: "Reactivation", view: "settings", settingsCategory: "reactivation" },
      { label: "Security", view: "settings", settingsCategory: "security" },
      { label: "Integrations", view: "settings", settingsCategory: "integrations" },
      { label: "Segment Probability", view: "settings", settingsCategory: "segment_probability" }
    ]}
  ];

  var currentModuleKey = null;
  var currentTabIndex = 0;
  var currentSettingsCategory = null;
  var currentCcBoard = null;

  function moduleByKey(key) {
    for (var i = 0; i < MODULES.length; i++) if (MODULES[i].key === key) return MODULES[i];
    return null;
  }

  // Reverse-lookup so any direct switchView(viewId) call (from empty-state
  // CTAs, cross-links, etc.) still highlights the right module/tab chrome.
  // moduleOverview/placeholder are ambiguous (shared by many tabs) and are
  // intentionally excluded — those are always entered via activateTab().
  function findTabForView(view) {
    if (view === "moduleOverview" || view === "placeholder" || view === "ccBoard") return null;
    for (var i = 0; i < MODULES.length; i++) {
      var mod = MODULES[i];
      for (var j = 0; j < mod.tabs.length; j++) {
        if (mod.tabs[j].view === view) return { moduleKey: mod.key, tabIndex: j };
      }
    }
    return null;
  }

  function renderSidebar() {
    var el = $("#nav-modules");
    if (!el) return;
    var html = "";
    MODULES.forEach(function (m) {
      html += '<button class="module-btn" data-module="' + m.key + '">' +
        '<span class="module-icon">' + m.icon + '</span><span class="module-label">' + esc(m.label) + '</span></button>';
    });
    el.innerHTML = html;
  }

  function renderSidebarActive(moduleKey) {
    $all(".module-btn").forEach(function (b) { b.classList.toggle("active", b.dataset.module === moduleKey); });
  }

  function renderTabBar(moduleKey, activeIndex) {
    var mod = moduleByKey(moduleKey);
    var bar = $("#tab-bar");
    if (!mod || !bar) return;
    var html = "";
    mod.tabs.forEach(function (t, idx) {
      var liveDot = t.live ? '<span class="tab-live-dot" title="Live Control — mutates production data">●</span>' : "";
      if (t.external) {
        html += '<a class="tab-btn tab-btn-external" href="' + t.external + '" target="_blank" rel="noopener">' + esc(t.label) + ' ↗</a>';
      } else {
        html += '<button class="tab-btn' + (idx === activeIndex ? " active" : "") + '" data-tab-idx="' + idx + '">' + esc(t.label) + liveDot + '</button>';
      }
    });
    bar.innerHTML = html;
  }

  function updateBreadcrumb(moduleKey, tabIndex) {
    var mod = moduleByKey(moduleKey);
    var tab = mod && mod.tabs[tabIndex];
    if (!mod || !tab) return;
    var bc = $("#breadcrumb");
    if (bc) bc.textContent = mod.icon + " " + mod.label + "  /  " + tab.label;
    $("#view-title").textContent = tab.label;
  }

  // Several view ids are legitimately reused by more than one tab (e.g.
  // "summary" backs both Dashboard/Overview and Analytics/Executive,
  // "settings" backs both Voucher Centre/Settings and Settings/General).
  // findTabForView() only returns the first config match, so while an
  // explicit activateTab() navigation is in flight we suppress switchView's
  // own auto-sync to avoid it clobbering the module/tab the user actually
  // picked with that first match.
  var inActivateTab = false;

  function activateTab(moduleKey, tabIndex) {
    var mod = moduleByKey(moduleKey);
    if (!mod) return;
    var tab = mod.tabs[tabIndex];
    if (!tab) return;
    currentModuleKey = moduleKey;
    currentTabIndex = tabIndex;
    currentSettingsCategory = tab.settingsCategory || null;
    currentCcBoard = tab.ccBoard || null;
    renderSidebarActive(moduleKey);
    renderTabBar(moduleKey, tabIndex);
    updateBreadcrumb(moduleKey, tabIndex);
    if (tab.external) { window.open(tab.external, "_blank", "noopener"); return; }
    if (tab.view === "placeholder" && tab.ph) {
      $("#placeholder-heading").textContent = tab.ph.title;
      $("#placeholder-desc").textContent = tab.ph.desc;
    }
    inActivateTab = true;
    switchView(tab.view);
    inActivateTab = false;
    if (tab.view === "moduleOverview") renderModuleOverview(moduleKey);
  }

  function selectModule(moduleKey) {
    activateTab(moduleKey, 0);
  }

  // Existing summary card-grid a generated Module Overview can safely reuse
  // (cloned as-is; the source grid is kept populated by loadSummary()).
  var OVERVIEW_CLONE_SOURCE = {
    voucher: "cards-vouchers",
    community: "cards-community",
    referral: "cards-referrals",
    welcome: "cards-community"
  };

  function renderModuleOverview(moduleKey) {
    var mod = moduleByKey(moduleKey);
    if (!mod) return;
    var kpiEl = $("#mod-ov-kpis");
    var qaEl = $("#mod-ov-quick-actions");
    var cloneSrc = OVERVIEW_CLONE_SOURCE[moduleKey];
    if (kpiEl) {
      if (cloneSrc) {
        loadSummary(false);
        setTimeout(function () {
          var src = $("#" + cloneSrc);
          kpiEl.innerHTML = (src && src.innerHTML) ? src.innerHTML : emptyState("No summary data yet.");
        }, 150);
      } else {
        kpiEl.innerHTML = emptyState(mod.label + " does not have aggregate KPIs wired yet — use the tabs below for live data.");
      }
    }
    if (qaEl) {
      var html = "";
      mod.tabs.forEach(function (t, idx) {
        if (t.view === "moduleOverview") return;
        html += '<button class="btn qa-btn" data-qa-idx="' + idx + '">' + esc(t.label) + (t.external ? " ↗" : "") + '</button>';
      });
      qaEl.innerHTML = html;
    }
  }

  function switchView(view) {
    state.view = view;
    VIEWS.forEach(function (v) { $("#view-" + v).classList.toggle("hidden", v !== view); });
    var found = inActivateTab ? null : findTabForView(view);
    if (found) {
      currentModuleKey = found.moduleKey;
      currentTabIndex = found.tabIndex;
      var foundTab = moduleByKey(found.moduleKey).tabs[found.tabIndex];
      currentSettingsCategory = (foundTab && foundTab.settingsCategory) || null;
      renderSidebarActive(found.moduleKey);
      renderTabBar(found.moduleKey, found.tabIndex);
      updateBreadcrumb(found.moduleKey, found.tabIndex);
    }
    var titles = {
      summary: "Executive Summary", moduleOverview: "Overview", placeholder: "Coming Soon",
      funnel: "Activation Funnel", abuse: "Abuse Overview",
      campaignBuilder: "Campaign Builder (P2)", campaignPerformance: "Campaign Performance (P4)",
      campaignIntelligence: "Campaign Intelligence (P5)", activeCampaigns: "Active Campaigns",
      draftCampaigns: "Draft Campaigns", compiledDrops: "Compiled Voucher Drops",
      campaigns: "Campaigns (Legacy Targeting)",
      gcCampaigns: "Player Campaigns", gcProviders: "Providers", gcResults: "Tournament Results",
      gcRewards: "Rewards", gcVerification: "Verification Integrations", gcActivity: "Activity Log",
      eventBanners: "Event Banner",
      luckyGames: "Lucky Games",
      vouchers: "Vouchers", drops: "Voucher Drops", referrals: "Referrals", affiliate: "Affiliate",
      affiliatePools: "Affiliate Voucher Pools", affiliateBatches: "Affiliate Voucher Batches", affiliatePending: "Pending Affiliate Rewards", reactivation: "Reactivation",
      audit: "Audit", segmentProbabilityConfig: "Segment Probability Configuration (Read Only)",
      segmentRoi: "Segment ROI Dashboard",
      segments: "Segment Overview", validation: "Data → Validation / UIM Compare",
      backendSegmentEngine: "Data → Backend Segment Engine (Shadow Mode)",
      voucherHunterAudit: "Data → Voucher Hunter Mismatch Audit (Phase 5B)",
      unclassifiedAudit: "Data → Unclassified Audit (Phase 5C)",
      segmentRuleSimulator: "Data → Segment Rule Simulator (Phase 5D)",
      voucherHunterQuality: "Data → Voucher Hunter Rule Quality Analysis (Phase 5E)",
      voucherHunterFalsePositive: "Data → Voucher Hunter False Positive Analysis (Phase 5E-FP)",
      voucherHunterRuleSimulator: "Data → Voucher Hunter Rule Simulator (Phase 6A)",
      vhPriorityImpact: "Data → VH Priority Impact Analysis (Phase 7C)",
      uploadPlayerPerformance: "Data → Upload Player Performance", uploadHistory: "Data → Upload History",
      rawExplorer: "Data → Raw Data Explorer",
      users: "User Drilldown", joinRequests: "Join Requests", xpAdjust: "Add / Reduce XP",
      settings: "Settings", referralShareContent: "Referral Centre — Share Content",
      referralShareEngagement: "Referral Centre — Share Engagement",
      ccComposer: "Community Centre — Composer", ccCalendar: "Community Centre — Calendar",
      ccBoard: "Community Centre"
    };
    if (!found && !inActivateTab) $("#view-title").textContent = titles[view] || view;
    banner(null);
    refreshCurrent(false);
  }

  function refreshCurrent(force) {
    if (state.view === "summary") loadSummary(force);
    else if (state.view === "funnel") loadFunnel(force);
    else if (state.view === "abuse") loadAbuse(force);
    else if (state.view === "campaignBuilder") loadCampaignBuilder(force);
    else if (state.view === "campaignPerformance") loadCampaignPerformance(force);
    else if (state.view === "campaignIntelligence") loadCampaignIntelligence(force);
    else if (state.view === "activeCampaigns") loadActiveCampaigns(force);
    else if (state.view === "draftCampaigns") loadDraftCampaigns(force);
    else if (state.view === "compiledDrops") loadCompiledDrops(force);
    else if (state.view === "campaigns") loadCampaigns(force);
    else if (state.view === "gcCampaigns") loadGcCampaigns(force);
    else if (state.view === "gcProviders") loadGcProviders(force);
    else if (state.view === "gcResults") loadGcResults(force);
    else if (state.view === "gcRewards") loadGcRewards(force);
    else if (state.view === "gcVerification") loadGcVerification(force);
    else if (state.view === "gcActivity") loadGcEvents(1);
    else if (state.view === "eventBanners") loadEventBanners(force);
    else if (state.view === "luckyGames") loadLuckyGames(force);
    else if (state.view === "referralShareContent") loadReferralShareContent(force);
    else if (state.view === "referralShareEngagement") loadReferralShareEngagement(force);
    else if (state.view === "vouchers") loadVouchers(force);
    else if (state.view === "drops") loadDrops(force);
    else if (state.view === "referrals") loadReferrals(force);
    else if (state.view === "affiliate") loadAffiliate(force);
    else if (state.view === "affiliatePools") loadAffiliatePools(force);
    else if (state.view === "affiliateBatches") loadAffiliateBatches(force);
    else if (state.view === "affiliatePending") loadAffiliatePending(force);
    else if (state.view === "reactivation") { loadReactivation(force); loadReactivationJourneyConfig(); }
    else if (state.view === "audit") loadAudit(force);
    else if (state.view === "segmentProbabilityConfig") loadSegmentProbabilityConfig(force);
    else if (state.view === "segmentRoi") loadSegmentRoi(force);
    else if (state.view === "segments") loadSegments(force);
    else if (state.view === "validation") loadValidation(force);
    else if (state.view === "backendSegmentEngine") {
      var _bseWeekSel = $("#bse-week");
      var _bseDropdownsLoaded = _bseWeekSel && _bseWeekSel.options.length > 1;
      if (!_bseDropdownsLoaded) {
        _populateBsePeriodDropdowns(true).then(function () { loadBackendSegmentEngine(force); });
      } else {
        loadBackendSegmentEngine(force);
      }
    }
    else if (state.view === "voucherHunterAudit") loadVoucherHunterMismatchAudit();
    else if (state.view === "unclassifiedAudit") loadUnclassifiedAudit();
    else if (state.view === "segmentRuleSimulator") { /* loads on Run Simulation click */ }
    else if (state.view === "voucherHunterQuality") { /* loads on Run Analysis click */ }
    else if (state.view === "voucherHunterFalsePositive") { /* loads on Run Analysis click */ }
    else if (state.view === "voucherHunterRuleSimulator") { /* loads on Run Simulation click */ }
    else if (state.view === "vhPriorityImpact") { /* loads on Run Analysis click */ }
    else if (state.view === "uploadPlayerPerformance") { /* upload view loads on submit */ }
    else if (state.view === "uploadHistory") loadUploadHistory(force);
    else if (state.view === "rawExplorer") loadRawExplorer(force);
    else if (state.view === "users") { /* user view loads on search */ }
    else if (state.view === "joinRequests") { /* loads on Load Join Requests click */ }
    else if (state.view === "xpAdjust") { /* form submit only, no load */ }
    else if (state.view === "settings") loadSettings(force);
    else if (state.view === "ccComposer") loadCcComposer(force);
    else if (state.view === "ccCalendar") loadCcCalendar(force);
    else if (state.view === "ccBoard") loadCcBoard(force);
  }

  function bind() {
    renderSidebar();
    $all(".module-btn").forEach(function (b) {
      b.addEventListener("click", function () { selectModule(b.dataset.module); });
    });
    $("#tab-bar").addEventListener("click", function (e) {
      var btn = e.target.closest(".tab-btn[data-tab-idx]");
      if (!btn || !currentModuleKey) return;
      activateTab(currentModuleKey, parseInt(btn.dataset.tabIdx, 10));
    });
    $("#mod-ov-quick-actions").addEventListener("click", function (e) {
      var btn = e.target.closest(".qa-btn[data-qa-idx]");
      if (!btn || !currentModuleKey) return;
      activateTab(currentModuleKey, parseInt(btn.dataset.qaIdx, 10));
    });
    $("#refresh-btn").addEventListener("click", function () {
      var btn = this;
      if (!btnStart(btn, "⏳ Refreshing...")) return;
      refreshCurrent(true);
      setTimeout(function () { btnStop(btn); }, 600);
    });
    $("#logout-btn").addEventListener("click", function () {
      fetch("/api/admin/auth/logout", { method: "POST", credentials: "same-origin" })
        .finally(function () { window.location.href = "/admin"; });
    });
    bindNotifBell();
    bindCampaigns();
    bindCampaignBuilder();
    bindDrops();
    bindAffiliatePools();
    bindAffiliateBatches();
    bindAffiliatePending();
    bindGcCampaigns();
    bindEventBanners();
    bindLuckyGames();
    bindGcProviders();
    bindGcResults();
    bindGcRewards();
    bindGcActivity();
    bindReferralShareContent();
    bindReferralShareEngagement();
    bindCommunityCentre();
    bindJoinRequests();
    bindXpAdjust();
    $("#reactivation-start-btn").addEventListener("click", function () { setReactivation(true, this); });
    $("#reactivation-pause-btn").addEventListener("click", function () { setReactivation(false, this); });
    $all("#funnel-window button").forEach(function (b) {
      b.addEventListener("click", function () {
        state.funnelWindow = b.dataset.window;
        $all("#funnel-window button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadFunnel(false);
      });
    });

    function seg(id, activeBtn) {
      $all("#" + id + " button").forEach(function (x) { x.classList.toggle("active", x === activeBtn); });
    }
    function on(id, evt, sel, fn) {
      var el = $(id);
      if (!el) return;
      el.addEventListener(evt, function (e) {
        var t = e.target && e.target.closest && e.target.closest(sel);
        if (t) fn({ target: t });
      });
    }

    on("#summary-window", "click", "button", function (e) {
      state.summaryWindow = e.target.dataset.window;
      seg("summary-window", e.target);
      loadSummary();
    });

    on("#abuse-window", "click", "button", function (e) {
      state.abuseWindow = e.target.dataset.window;
      seg("abuse-window", e.target);
      loadAbuse();
    });

    $all("#referrals-window button").forEach(function (b) {
      b.addEventListener("click", function () {
        state.referralsWindow = b.dataset.window;
        $all("#referrals-window button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadReferrals(false);
      });
    });
    $all("#vouchers-window button").forEach(function (b) {
      b.addEventListener("click", function () {
        state.voucherWindow = b.dataset.window;
        $all("#vouchers-window button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadVouchers(false);
      });
    });

    [["vouchers-filter", "vouchers-body"], ["referrals-filter", "referrals-body"],
     ["affiliate-filter", "affiliate-body"], ["audit-filter", "audit-body"]].forEach(function (pair) {
      var input = $("#" + pair[0]);
      if (input) input.addEventListener("input", function () { applyFilter(pair[1], input.value); });
    });

    var roiApplyBtn = $("#roi-apply-btn");
    if (roiApplyBtn) roiApplyBtn.addEventListener("click", function () { loadSegmentRoi(true); });
    var roiMonthSel = $("#roi-month");
    if (roiMonthSel) {
      roiMonthSel.addEventListener("change", function () {
        state.roiMonth = roiMonthSel.value || "";
        loadSegmentRoi(false);
      });
    }

    initSegmentMonthOptions();
    on("#segments-mode", "click", "button", function (e) {
      state.segmentsMode = e.target.dataset.mode;
      seg("segments-mode", e.target);
      var monthSelect = $("#segments-month");
      if (monthSelect) monthSelect.value = "";
      loadSegments(false);
    });
    var segMonthSelect = $("#segments-month");
    if (segMonthSelect) {
      segMonthSelect.addEventListener("change", function () {
        state.segmentsMonth = segMonthSelect.value || "";
        if (state.segmentsMonth) {
          state.segmentsMode = "month";
          seg("segments-mode", null);
        }
        loadSegments(false);
      });
    }
    var segFilterInput = $("#segments-filter");
    if (segFilterInput) {
      var segFilterTimer = null;
      segFilterInput.addEventListener("input", function () {
        clearTimeout(segFilterTimer);
        segFilterTimer = setTimeout(function () {
          state.segmentsFilter = (segFilterInput.value || "").trim().toLowerCase();
          loadSegments(false);
        }, 300);
      });
    }

    var valApplyBtn = $("#validation-apply-btn");
    if (valApplyBtn) valApplyBtn.addEventListener("click", function () { loadValidation(true); });

    var bseApplyBtn = $("#bse-apply-btn");
    if (bseApplyBtn) bseApplyBtn.addEventListener("click", function () {
      _populateBsePeriodDropdowns(false).then(function () { loadBackendSegmentEngine(true); });
    });

    var bseDryRunBtn = $("#bse-dry-run-btn");
    if (bseDryRunBtn) bseDryRunBtn.addEventListener("click", function () { runBackendSegmentEngine(true); });
    var bseCommitRunBtn = $("#bse-commit-run-btn");
    if (bseCommitRunBtn) bseCommitRunBtn.addEventListener("click", function () { runBackendSegmentEngine(false); });

    var uimcApplyBtn = $("#uimc-apply-btn");
    if (uimcApplyBtn) uimcApplyBtn.addEventListener("click", function () { loadUimComparison(true); });
    var uimcExportBtn = $("#uimc-export-btn");
    if (uimcExportBtn) uimcExportBtn.addEventListener("click", exportUimComparisonCsv);

    var vhmaApplyBtn = $("#vhma-apply-btn");
    if (vhmaApplyBtn) vhmaApplyBtn.addEventListener("click", loadVoucherHunterMismatchAudit);

    var uncaApplyBtn = $("#unca-apply-btn");
    if (uncaApplyBtn) uncaApplyBtn.addEventListener("click", loadUnclassifiedAudit);

    var srsRunBtn = $("#srs-run-btn");
    if (srsRunBtn) srsRunBtn.addEventListener("click", runSegmentRuleSimulator);

    var vhqaApplyBtn = $("#vhqa-apply-btn");
    if (vhqaApplyBtn) vhqaApplyBtn.addEventListener("click", loadVoucherHunterQuality);

    var vhfpApplyBtn = $("#vhfp-apply-btn");
    if (vhfpApplyBtn) vhfpApplyBtn.addEventListener("click", loadVoucherHunterFalsePositive);

    var vhrsRunBtn = $("#vhrs-run-btn");
    if (vhrsRunBtn) vhrsRunBtn.addEventListener("click", runVoucherHunterRuleSimulator);

    var vhpiRunBtn = $("#vhpi-run-btn");
    if (vhpiRunBtn) vhpiRunBtn.addEventListener("click", runVhPriorityImpact);

    var imaApplyBtn = $("#ima-apply-btn");
    if (imaApplyBtn) imaApplyBtn.addEventListener("click", loadIdentityMatchAudit);

    var uploadSubmitBtn = $("#upload-submit-btn");
    if (uploadSubmitBtn) uploadSubmitBtn.addEventListener("click", uploadPlayerPerformance);
    var uploadHistoryRefreshBtn = $("#upload-history-refresh-btn");
    if (uploadHistoryRefreshBtn) uploadHistoryRefreshBtn.addEventListener("click", function () { loadUploadHistory(true); });

    var explorerApplyBtn = $("#explorer-apply-btn");
    if (explorerApplyBtn) explorerApplyBtn.addEventListener("click", function () { loadRawExplorer(true); });
    var explorerPeriodType = $("#explorer-period-type");
    if (explorerPeriodType) explorerPeriodType.addEventListener("change", function () {
      updateExplorerPeriodOptions({}, explorerPeriodType.value || "weekly", "");
      loadRawExplorer(true);
    });

    var us = $("#user-search"), ub = $("#user-search-btn");
    function doUserSearch() { loadUser((us.value || "").trim()); }
    if (ub) ub.addEventListener("click", doUserSearch);
    if (us) us.addEventListener("keydown", function (e) { if (e.key === "Enter") doUserSearch(); });
  }

  // =========================================================================
  // COMMUNITY CENTRE — Composer / Calendar / Scheduled / Drafts / Pending
  // Approval / Published / Poll Results / Failed
  // =========================================================================

  // ---- Rich-text editor: pure HTML helpers (no DOM dependency) ----------
  // Mirrors the allowlist/normalisation in community_centre_limits.py so the
  // Composer's visual editor never displays or round-trips markup that the
  // authoritative backend sanitizer would silently strip. The backend
  // re-sanitizes on every save regardless — these helpers exist purely so
  // the client can show the same canonical result before submitting.
  var CC_RTE_TAG_ALIASES = { strong: "b", em: "i", ins: "u", strike: "s", del: "s" };
  // "tg-spoiler" is its own standalone tag in Telegram's HTML dialect
  // (equivalent to, but distinct from, <span class="tg-spoiler">) — kept
  // as-is rather than aliased so existing drafts using it round-trip
  // instead of losing their spoiler formatting.
  var CC_RTE_ALLOWED_TAGS = ["b", "i", "u", "s", "code", "pre", "blockquote", "a", "span", "tg-spoiler"];
  var CC_RTE_ALLOWED_ATTRS = { a: ["href"], span: ["class"], blockquote: ["expandable"], code: ["class"] };
  var CC_RTE_NAMED_ENTITIES = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " };

  function ccRteDecodeEntities(text) {
    return String(text || "").replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);/g, function (m, ent) {
      if (ent.charAt(0) === "#") {
        var isHex = ent.charAt(1) === "x" || ent.charAt(1) === "X";
        var code = isHex ? parseInt(ent.slice(2), 16) : parseInt(ent.slice(1), 10);
        if (isNaN(code)) return m;
        try { return String.fromCodePoint(code); } catch (e) { return m; }
      }
      return CC_RTE_NAMED_ENTITIES.hasOwnProperty(ent) ? CC_RTE_NAMED_ENTITIES[ent] : m;
    });
  }

  function ccRteEscapeText(text) {
    return String(text || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function ccRteEscapeAttr(v) {
    return String(v || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function ccRteIsSafeHref(href) {
    var v = String(href || "").trim();
    var low = v.toLowerCase();
    if (!v) return false;
    if (low.indexOf("javascript:") === 0 || low.indexOf("data:") === 0) return false;
    return low.indexOf("https://") === 0 || low.indexOf("http://") === 0 || low.indexOf("tg://") === 0;
  }

  // Validates a URL for the Link toolbar action. Returns null when valid,
  // otherwise a short user-facing error string.
  function ccRteValidateUrl(url) {
    var v = String(url || "").trim();
    if (!v) return "URL is required.";
    var low = v.toLowerCase();
    if (low.indexOf("javascript:") === 0) return "javascript: links are not allowed.";
    if (low.indexOf("data:") === 0) return "data: links are not allowed.";
    if (/^tg:\/\/(resolve|join|user)(\/|\?|$)/i.test(v)) return null;
    if (low.indexOf("https://") === 0 || low.indexOf("http://") === 0) {
      try {
        var u = new URL(v);
        if (u.protocol !== "https:" && u.protocol !== "http:") return "Only https:// links are allowed.";
        if (!u.hostname) return "Enter a valid URL.";
        return null;
      } catch (e) {
        return "Enter a valid URL.";
      }
    }
    return "Enter a valid https:// URL or an approved tg:// link.";
  }

  function ccRteParseAttrs(attrString) {
    var attrs = {};
    if (!attrString) return attrs;
    var re = /([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*(?:=\s*("([^"]*)"|'([^']*)'|[^\s"'=<>`]+))?/g;
    var m;
    while ((m = re.exec(attrString))) {
      var name = m[1].toLowerCase();
      var value = m[3] !== undefined ? m[3] : (m[4] !== undefined ? m[4] : (m[2] !== undefined ? m[2] : ""));
      attrs[name] = ccRteDecodeEntities(value);
    }
    return attrs;
  }

  // Sanitizes/normalizes raw HTML (visual-editor innerHTML, pasted markup,
  // or hand-typed Advanced-mode HTML) down to the canonical Telegram-HTML
  // allowlist: b/i/u/s/code/pre/blockquote/a[href]/span[class=tg-spoiler].
  // <div>/<p>/<br> are converted to literal newlines since Telegram has no
  // block-level line-break tags. Nested <blockquote> is flattened, not
  // rejected. This is a client-side mirror of
  // community_centre_limits.sanitize_telegram_html — the backend remains
  // authoritative and re-sanitizes independently on every save.
  function ccRteSanitizeHtml(raw) {
    var input = String(raw == null ? "" : raw);
    var TAG_RE = /<(\/?)([a-zA-Z][a-zA-Z0-9-]*)((?:[^>"']|"[^"]*"|'[^']*')*)>/g;
    var out = [];
    var stack = []; // { tag, emitted }
    var lastIndex = 0;
    var m;

    function inBlockquote() {
      for (var i = 0; i < stack.length; i++) if (stack[i].tag === "blockquote") return true;
      return false;
    }
    function endsWithNewline() {
      return !out.length || /\n$/.test(out[out.length - 1]);
    }
    function emitText(text) {
      if (!text) return;
      out.push(ccRteEscapeText(ccRteDecodeEntities(text)));
    }
    function emitLineBreak() {
      if (!endsWithNewline()) out.push("\n");
    }

    while ((m = TAG_RE.exec(input))) {
      emitText(input.slice(lastIndex, m.index));
      lastIndex = TAG_RE.lastIndex;
      var closing = m[1] === "/";
      var rawTag = m[2].toLowerCase();

      if (rawTag === "br") { emitLineBreak(); continue; }
      if (rawTag === "div" || rawTag === "p") { emitLineBreak(); continue; }
      // script/style content is dropped entirely below via the disallowed-
      // tag path (content is still emitted as text) — that's intentional
      // for style, but script content must never reach the output even as
      // text, so strip it out explicitly.
      if (rawTag === "script" || rawTag === "style") {
        if (!closing) {
          var closeRe = new RegExp("</" + rawTag + "\\s*>", "i");
          var closeMatch = closeRe.exec(input.slice(lastIndex));
          lastIndex = closeMatch ? lastIndex + closeMatch.index + closeMatch[0].length : input.length;
        }
        continue;
      }

      var tag = CC_RTE_TAG_ALIASES[rawTag] || rawTag;
      if (CC_RTE_ALLOWED_TAGS.indexOf(tag) === -1) continue;

      if (!closing) {
        if (tag === "blockquote" && inBlockquote()) {
          stack.push({ tag: tag, emitted: false });
          continue;
        }
        var attrs = ccRteParseAttrs(m[3]);
        var allowedAttrNames = CC_RTE_ALLOWED_ATTRS[tag] || [];
        var kept = [];
        allowedAttrNames.forEach(function (name) {
          if (!(name in attrs)) return;
          var value = attrs[name];
          if (tag === "a" && name === "href" && !ccRteIsSafeHref(value)) return;
          if (tag === "span" && name === "class" && value.trim() !== "tg-spoiler") return;
          kept.push(name + '="' + ccRteEscapeAttr(value) + '"');
        });
        if (tag === "span" && !kept.length) {
          stack.push({ tag: tag, emitted: false });
          continue;
        }
        out.push("<" + tag + (kept.length ? " " + kept.join(" ") : "") + ">");
        stack.push({ tag: tag, emitted: true });
      } else {
        var idx = -1;
        for (var i = stack.length - 1; i >= 0; i--) { if (stack[i].tag === tag) { idx = i; break; } }
        if (idx === -1) continue;
        while (stack.length > idx) {
          var top = stack.pop();
          if (top.emitted) out.push("</" + top.tag + ">");
        }
      }
    }
    emitText(input.slice(lastIndex));
    while (stack.length) {
      var rest = stack.pop();
      if (rest.emitted) out.push("</" + rest.tag + ">");
    }
    return out.join("").replace(/\n{3,}/g, "\n\n").replace(/^\n+/, "").replace(/\n+$/, "");
  }

  // Visible (rendered) text of a canonical/sanitized Telegram-HTML string,
  // for character counting — Telegram limits are on visible text, not raw
  // markup length.
  function ccRteHtmlToPlainText(html) {
    return ccRteDecodeEntities(String(html || "").replace(/<[^>]+>/g, ""));
  }

  // Whether a toolbar action should apply to the current selection — a
  // no-op on a collapsed/empty selection rather than silently mutating
  // nothing or throwing.
  function ccRteShouldApplyToSelection(selectionText) {
    return !!(selectionText && selectionText.length > 0);
  }

  // Pure decision for the Quote toggle: clicking Quote while already inside
  // a quote removes it instead of nesting another one.
  function ccRteQuoteAction(insideBlockquote) {
    return insideBlockquote ? "unwrap" : "wrap";
  }

  var cc = {
    limits: null,
    destinations: null,
    media: [],
    buttons: [],
    pollOptions: [{ text: "" }, { text: "" }],
    quizCorrect: 0,
    editingId: null,
    editingUpdatedAt: null,
    calendarStart: null,
    textMode: "rich", // "rich" | "html" — Composer message editor mode
  };

  function ccPad2(n) { return (n < 10 ? "0" : "") + n; }

  // Asia/Kuala_Lumpur is UTC+8 year-round (no DST) — fixed-offset conversion
  // is exact, no timezone library needed.
  function ccKlInputToUtcIso(val) {
    if (!val) return null;
    var m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/.exec(val);
    if (!m) return null;
    var ms = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5]) - 8 * 3600 * 1000;
    return new Date(ms).toISOString();
  }

  function ccUtcToKlInputValue(iso) {
    if (!iso) return "";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return "";
    var kl = new Date(d.getTime() + 8 * 3600 * 1000);
    return kl.getUTCFullYear() + "-" + ccPad2(kl.getUTCMonth() + 1) + "-" + ccPad2(kl.getUTCDate()) +
      "T" + ccPad2(kl.getUTCHours()) + ":" + ccPad2(kl.getUTCMinutes());
  }

  function ccUtcToKlDisplay(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return "—";
    var kl = new Date(d.getTime() + 8 * 3600 * 1000);
    return kl.getUTCFullYear() + "-" + ccPad2(kl.getUTCMonth() + 1) + "-" + ccPad2(kl.getUTCDate()) +
      " " + ccPad2(kl.getUTCHours()) + ":" + ccPad2(kl.getUTCMinutes()) + " KL";
  }

  var CC_CONTENT_ICON = { text: "📝", photo: "🖼", animation: "🎞", video: "🎬", media_group: "🗂", poll: "📊", quiz: "🧠" };

  function ccEnsureMeta() {
    var tasks = [];
    if (!cc.limits) tasks.push(api("/api/admin/community/limits").then(function (d) { cc.limits = d.limits; }));
    if (!cc.destinations) tasks.push(api("/api/admin/community/destinations").then(function (d) { cc.destinations = d.destinations || []; }));
    return Promise.all(tasks);
  }

  function ccOpenTelegramSettings() {
    var mod = moduleByKey("settings");
    var idx = mod ? mod.tabs.findIndex(function (t) { return t.settingsCategory === "integrations"; }) : -1;
    if (idx >= 0) activateTab("settings", idx);
  }

  function ccRenderDestinationSelect() {
    var sel = $("#cc-destination");
    if (!sel) return;
    var prev = sel.value;
    var enabled = (cc.destinations || []).filter(function (d) { return d.enabled; });
    var hasValid = enabled.length > 0;
    sel.innerHTML = hasValid
      ? enabled.map(function (d) { return '<option value="' + esc(d.key) + '"' + (d.key === prev ? " selected" : "") + '>' + esc(d.name) + "</option>"; }).join("")
      : '<option value="" disabled selected>No valid Telegram destination configured</option>';
    sel.disabled = !hasValid;
    var warning = $("#cc-destination-warning");
    if (warning) warning.classList.toggle("hidden", hasValid);
    var previewBtn = $("#cc-preview-btn");
    if (previewBtn) previewBtn.disabled = !hasValid;
  }

  // ---------- Composer: content-type-driven field visibility ----------
  function ccUpdateContentTypeVisibility() {
    var ct = $("#cc-content-type").value;
    var isTextish = ct === "text" || ct === "photo" || ct === "animation" || ct === "video" || ct === "media_group";
    var isMedia = ct === "photo" || ct === "animation" || ct === "video" || ct === "media_group";
    var isPoll = ct === "poll" || ct === "quiz";
    $("#cc-text-fields").classList.toggle("hidden", !isTextish);
    $("#cc-media-fields").classList.toggle("hidden", !isMedia);
    $("#cc-poll-fields").classList.toggle("hidden", !isPoll);
    $("#cc-quiz-fields").classList.toggle("hidden", ct !== "quiz");
    $("#cc-poll-multi-wrap").classList.toggle("hidden", ct === "quiz");
    $("#cc-link-preview-wrap").classList.toggle("hidden", ct !== "text");
    if (isMedia && ct !== "media_group" && cc.media.length > 1) cc.media = cc.media.slice(0, 1);
    if (isMedia && cc.media.length === 0) {
      cc.media.push({ type: ct === "media_group" ? "photo" : ct, media_library_id: "", source_url: "", telegram_file_id: "" });
    }
    if (!isMedia) cc.media = [];
    ccRenderMedia();
    var maxLen = ct === "text" ? ((cc.limits && cc.limits.text_max_len) || 4096) : ((cc.limits && cc.limits.caption_max_len) || 1024);
    $("#cc-text").maxLength = maxLen;
    ccRteUpdateVisibility();
    ccUpdateTextCounter();
  }

  function ccUpdateTextCounter() {
    var el = $("#cc-text-counter");
    if (!el) return;
    var ct = $("#cc-content-type").value;
    var maxLen = ct === "text" ? ((cc.limits && cc.limits.text_max_len) || 4096) : ((cc.limits && cc.limits.caption_max_len) || 1024);
    var raw = $("#cc-text").value || "";
    var parseModeEl = $("#cc-parse-mode");
    var parseMode = parseModeEl ? parseModeEl.value : "HTML";
    // Only HTML parse_mode content has markup to strip for the "visible
    // characters" count — MarkdownV2 is stored/sent verbatim, and stripping
    // "<...>"-shaped substrings out of literal Markdown text would miscount.
    var visibleLen = parseMode === "HTML" ? ccRteHtmlToPlainText(raw).length : raw.length;
    el.textContent = visibleLen + " / " + maxLen + " characters";
  }

  // ---------- Composer: rich-text editor (DOM-facing) ----------
  function ccRteEditor() { return $("#cc-rte-editor"); }

  function ccRteUpdateVisibility() {
    var toolbar = $("#cc-rte-toolbar");
    var editor = ccRteEditor();
    var source = $("#cc-text");
    var toggleBtn = $("#cc-rte-source-toggle");
    if (!toolbar || !editor || !source) return;
    var parseModeEl = $("#cc-parse-mode");
    var parseMode = parseModeEl ? parseModeEl.value : "HTML";
    if (parseMode !== "HTML") {
      // MarkdownV2 is stored/sent verbatim (see ccPreviewBubbleHtml) — the
      // Telegram-HTML rich-text toolbar doesn't apply to it.
      toolbar.classList.add("hidden");
      editor.classList.add("hidden");
      source.classList.remove("hidden");
      if (toggleBtn) toggleBtn.classList.add("hidden");
      return;
    }
    if (toggleBtn) toggleBtn.classList.remove("hidden");
    if (cc.textMode === "html") {
      toolbar.classList.add("hidden");
      editor.classList.add("hidden");
      source.classList.remove("hidden");
      if (toggleBtn) toggleBtn.textContent = "Back to Rich Text";
    } else {
      toolbar.classList.remove("hidden");
      editor.classList.remove("hidden");
      source.classList.add("hidden");
      if (toggleBtn) toggleBtn.textContent = "Advanced: Edit Telegram HTML";
    }
  }

  // Recomputes the canonical/sanitized Telegram HTML from the visual
  // editor's current contents into the hidden #cc-text field.
  function ccRteSyncFromEditor() {
    var editor = ccRteEditor();
    if (!editor) return;
    $("#cc-text").value = ccRteSanitizeHtml(editor.innerHTML);
    ccUpdateTextCounter();
  }

  function ccRteCurrentSelectionInsideEditor(editor) {
    var sel = window.getSelection && window.getSelection();
    if (!sel || sel.rangeCount === 0) return false;
    var node = sel.getRangeAt(0).commonAncestorContainer;
    return editor.contains(node.nodeType === 1 ? node : node.parentNode);
  }

  function ccRteAncestorTag(node, tag, editor) {
    while (node && node !== editor) {
      if (node.nodeType === 1 && node.tagName.toLowerCase() === tag) return node;
      node = node.parentNode;
    }
    return null;
  }

  function ccRteExec(cmd) { document.execCommand(cmd, false, null); }

  // Toggles an inline wrapper (code / spoiler) around the current selection.
  function ccRteToggleInline(editor, tag, className) {
    var sel = window.getSelection();
    if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return;
    if (!ccRteShouldApplyToSelection(sel.toString())) return;
    if (!ccRteCurrentSelectionInsideEditor(editor)) return;
    var range = sel.getRangeAt(0);
    var container = range.commonAncestorContainer;
    var existing = ccRteAncestorTag(container.nodeType === 1 ? container : container.parentNode, tag, editor);
    if (existing && (!className || existing.className === className)) {
      var frag = document.createDocumentFragment();
      while (existing.firstChild) frag.appendChild(existing.firstChild);
      existing.parentNode.replaceChild(frag, existing);
    } else {
      var contents = range.extractContents();
      var wrapper = document.createElement(tag);
      if (className) wrapper.className = className;
      wrapper.appendChild(contents);
      range.insertNode(wrapper);
      sel.removeAllRanges();
      var r2 = document.createRange();
      r2.selectNodeContents(wrapper);
      sel.addRange(r2);
    }
    ccRteSyncFromEditor();
  }

  function ccRteBlockAncestor(node, editor) {
    if (!node) return null;
    while (node.parentNode && node.parentNode !== editor) node = node.parentNode;
    return node.parentNode === editor ? node : null;
  }

  // Toggles a <blockquote> around the block(s) covering the current
  // selection. If the selection is already inside a quote, unwraps it
  // instead of nesting — never produces nested <blockquote>.
  function ccRteToggleQuote(editor) {
    var sel = window.getSelection();
    if (!sel || sel.rangeCount === 0) return;
    if (!ccRteCurrentSelectionInsideEditor(editor)) return;
    var range = sel.getRangeAt(0);
    var container = range.commonAncestorContainer;
    var insideQuote = ccRteAncestorTag(container.nodeType === 1 ? container : container.parentNode, "blockquote", editor);
    if (ccRteQuoteAction(!!insideQuote) === "unwrap") {
      var frag = document.createDocumentFragment();
      while (insideQuote.firstChild) frag.appendChild(insideQuote.firstChild);
      insideQuote.parentNode.replaceChild(frag, insideQuote);
      ccRteSyncFromEditor();
      return;
    }
    var startBlock = ccRteBlockAncestor(range.startContainer, editor) || editor.firstChild;
    var endBlock = ccRteBlockAncestor(range.endContainer, editor) || editor.lastChild;
    if (!startBlock) return;
    var nodes = [];
    var child = editor.firstChild;
    var collecting = false;
    while (child) {
      if (child === startBlock) collecting = true;
      if (collecting) nodes.push(child);
      if (child === endBlock) break;
      child = child.nextSibling;
    }
    if (!nodes.length) nodes = [startBlock];
    var wrapper = document.createElement("blockquote");
    nodes[0].parentNode.insertBefore(wrapper, nodes[0]);
    nodes.forEach(function (n) { wrapper.appendChild(n); });
    ccRteSyncFromEditor();
  }

  // Strips all formatting from the selection, leaving plain text.
  function ccRteClearFormatting(editor) {
    var sel = window.getSelection();
    if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return;
    if (!ccRteCurrentSelectionInsideEditor(editor)) return;
    var range = sel.getRangeAt(0);
    var text = range.toString();
    range.deleteContents();
    var textNode = document.createTextNode(text);
    range.insertNode(textNode);
    sel.removeAllRanges();
    var r2 = document.createRange();
    r2.setStartAfter(textNode);
    r2.collapse(true);
    sel.addRange(r2);
    ccRteSyncFromEditor();
  }

  function ccRtePromptLinkUrl() {
    return new Promise(function (resolve) {
      var overlay = document.createElement("div");
      overlay.className = "modal-overlay";
      overlay.innerHTML =
        '<div class="modal-box" style="max-width:420px;">' +
        "<h3>Insert Link</h3>" +
        '<input class="filter-input" id="cc-rte-link-input" placeholder="https://example.com" autocomplete="off" />' +
        '<div class="sub" id="cc-rte-link-error" style="color:var(--bad);min-height:16px;"></div>' +
        '<div class="modal-actions">' +
        '<button class="btn" id="cc-rte-link-cancel">Cancel</button>' +
        '<button class="btn primary" id="cc-rte-link-ok">Insert</button>' +
        "</div></div>";
      document.body.appendChild(overlay);
      var input = overlay.querySelector("#cc-rte-link-input");
      input.focus();
      function done(url) { overlay.remove(); resolve(url); }
      overlay.querySelector("#cc-rte-link-cancel").addEventListener("click", function () { done(null); });
      overlay.addEventListener("click", function (e) { if (e.target === overlay) done(null); });
      function tryOk() {
        var err = ccRteValidateUrl(input.value);
        if (err) { overlay.querySelector("#cc-rte-link-error").textContent = err; return; }
        done(input.value.trim());
      }
      overlay.querySelector("#cc-rte-link-ok").addEventListener("click", tryOk);
      input.addEventListener("keydown", function (e) {
        if (e.key === "Enter") { e.preventDefault(); tryOk(); }
        if (e.key === "Escape") done(null);
      });
    });
  }

  function ccRteApplyLink(editor) {
    var sel = window.getSelection();
    if (!sel || sel.rangeCount === 0 || sel.isCollapsed || !ccRteShouldApplyToSelection(sel.toString())) {
      toast("❌ Select text first", "error");
      return;
    }
    if (!ccRteCurrentSelectionInsideEditor(editor)) return;
    var range = sel.getRangeAt(0).cloneRange();
    ccRtePromptLinkUrl().then(function (url) {
      if (!url) return;
      sel.removeAllRanges();
      sel.addRange(range);
      var contents = range.extractContents();
      var a = document.createElement("a");
      a.setAttribute("href", url);
      a.appendChild(contents);
      range.insertNode(a);
      sel.removeAllRanges();
      var r2 = document.createRange();
      r2.selectNodeContents(a);
      sel.addRange(r2);
      ccRteSyncFromEditor();
    });
  }

  // Sanitizes pasted content (Word/Docs/websites → strip fonts, colours,
  // classes, inline CSS, scripts; keep only the Telegram-supported subset).
  // Plain-text clipboard data stays plain text; line breaks are preserved
  // as visible <br> while the canonical value stores them as "\n".
  function ccRtePasteHandler(e) {
    e.preventDefault();
    var cd = e.clipboardData || window.clipboardData;
    var html = cd && cd.getData && cd.getData("text/html");
    var text = cd && cd.getData && cd.getData("text/plain");
    var canonical = html ? ccRteSanitizeHtml(html) : ccRteEscapeText(String(text || ""));
    var displayHtml = canonical.replace(/\n/g, "<br>");
    document.execCommand("insertHTML", false, displayHtml);
    ccRteSyncFromEditor();
  }

  // Ctrl/Cmd+B/I/U only — everything else is left to the browser.
  function ccRteKeydown(e) {
    var meta = e.ctrlKey || e.metaKey;
    if (!meta) return;
    var key = (e.key || "").toLowerCase();
    if (key === "b") { e.preventDefault(); ccRteExec("bold"); ccRteSyncFromEditor(); }
    else if (key === "i") { e.preventDefault(); ccRteExec("italic"); ccRteSyncFromEditor(); }
    else if (key === "u") { e.preventDefault(); ccRteExec("underline"); ccRteSyncFromEditor(); }
  }

  // Called before Preview / Save Draft / Publish / Schedule: converts the
  // visual editor content into canonical Telegram HTML and re-sanitizes
  // whatever's in the (possibly hand-edited) Advanced HTML field too.
  // MarkdownV2 is stored/sent verbatim (see ccPreviewBubbleHtml) — the
  // HTML editor/sanitizer never touches it, or a MarkdownV2 draft's text
  // would be overwritten by the (unrelated, possibly empty) rich-text
  // editor content.
  function ccRteFinalizeBeforeSubmit() {
    var parseModeEl = $("#cc-parse-mode");
    var parseMode = parseModeEl ? parseModeEl.value : "HTML";
    if (parseMode !== "HTML") return;
    var editor = ccRteEditor();
    if (editor && cc.textMode !== "html") {
      ccRteSyncFromEditor();
    } else {
      $("#cc-text").value = ccRteSanitizeHtml($("#cc-text").value || "");
    }
  }

  function ccRteValidateLimits() {
    var ct = $("#cc-content-type").value;
    var parseModeEl = $("#cc-parse-mode");
    var parseMode = parseModeEl ? parseModeEl.value : "HTML";
    if (parseMode !== "HTML") return null;
    var textish = ct === "text" || ct === "photo" || ct === "animation" || ct === "video" || ct === "media_group";
    if (!textish) return null;
    var maxLen = ct === "text" ? ((cc.limits && cc.limits.text_max_len) || 4096) : ((cc.limits && cc.limits.caption_max_len) || 1024);
    // The backend (validate_post_payload in community_centre.py) validates
    // length on the raw HTML string it receives — i.e. our already-
    // sanitized canonical payload — *before* re-sanitizing, not on the
    // visible/tag-stripped text. Mirror that exactly here so the composer
    // never reports "OK" on something the backend will reject as
    // too_long.
    var rawLen = ($("#cc-text").value || "").length;
    if (rawLen > maxLen) return "Message is too long: " + rawLen + " / " + maxLen + " characters.";
    return null;
  }

  // ---------- Composer: media list ----------
  var CC_MEDIA_TYPE_ICON = { photo: "🖼", animation: "🎞", video: "🎬", document: "📄" };

  function ccRenderMedia() {
    var ct = $("#cc-content-type").value;
    var wrap = $("#cc-media-list");
    if (!wrap) return;
    var isGroup = ct === "media_group";
    wrap.innerHTML = cc.media.map(function (m, i) {
      var typeSel = isGroup
        ? '<select class="filter-input cc-media-type" data-idx="' + i + '" style="max-width:110px;">' +
          '<option value="photo"' + (m.type === "photo" ? " selected" : "") + '>Image</option>' +
          '<option value="video"' + (m.type === "video" ? " selected" : "") + '>Video</option></select>'
        : '<span class="pill neutral">' + esc(ct) + '</span>';

      var librarySummary = m.media_library_id
        ? '<span class="pill ok">' + (CC_MEDIA_TYPE_ICON[m.type] || "📎") + " " + esc(m._libraryLabel || "Media Library item") + '</span>' +
          '<button class="btn" data-cc-media-pick="' + i + '" type="button">Change</button>' +
          '<button class="btn" data-cc-media-clear="' + i + '" type="button">Remove selection</button>'
        : '<button class="btn primary" data-cc-media-pick="' + i + '" type="button">Select from Media Library</button>';

      var advancedOpen = !!m._advancedOpen || (!m.media_library_id && (m.source_url || m.telegram_file_id));
      var advanced = '<div class="cc-media-advanced' + (advancedOpen ? "" : " hidden") + '" data-idx="' + i + '">' +
        '<input class="filter-input cc-media-url" data-idx="' + i + '" placeholder="https://... media URL" value="' + esc(m.source_url || "") + '" style="flex:1;min-width:220px;" />' +
        '<input class="filter-input cc-media-fileid" data-idx="' + i + '" placeholder="or Telegram file_id" value="' + esc(m.telegram_file_id || "") + '" style="min-width:160px;" />' +
        "</div>";

      return '<div class="cc-media-row" data-idx="' + i + '" style="flex-direction:column;align-items:flex-start;">' +
        '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;width:100%;">' + typeSel + librarySummary +
        (isGroup
          ? '<button class="btn" data-cc-media-up="' + i + '" type="button">&uarr;</button>' +
            '<button class="btn" data-cc-media-down="' + i + '" type="button">&darr;</button>' +
            '<button class="btn danger" data-cc-media-remove="' + i + '" type="button">Remove</button>'
          : "") +
        '<button class="btn" data-cc-media-advanced-toggle="' + i + '" type="button" style="margin-left:auto;">Advanced ▾</button>' +
        "</div>" + advanced + "</div>";
    }).join("");
    var addBtn = $("#cc-media-add-btn");
    if (addBtn) addBtn.classList.toggle("hidden", !isGroup);
  }

  function ccSyncMediaFromDom() {
    $all("#cc-media-list .cc-media-row").forEach(function (row) {
      var i = +row.dataset.idx;
      if (!cc.media[i]) return;
      var typeSel = row.querySelector(".cc-media-type");
      if (typeSel) cc.media[i].type = typeSel.value;
      var urlInput = row.querySelector(".cc-media-url");
      var fileIdInput = row.querySelector(".cc-media-fileid");
      if (urlInput) cc.media[i].source_url = (urlInput.value || "").trim();
      if (fileIdInput) cc.media[i].telegram_file_id = (fileIdInput.value || "").trim();
    });
  }

  function ccMediaFormatBytes(n) {
    if (!n && n !== 0) return "—";
    if (n < 1024) return n + " B";
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
    return (n / (1024 * 1024)).toFixed(1) + " MB";
  }

  function ccMediaFormatDuration(sec) {
    if (!sec && sec !== 0) return "";
    var m = Math.floor(sec / 60), s = Math.floor(sec % 60);
    return m + ":" + (s < 10 ? "0" : "") + s;
  }

  // ---------- Media Library modal (select / rename / archive / restore) ----------
  function ccOpenMediaLibraryModal(opts) {
    opts = opts || {};
    var filterType = opts.filterType || "";
    var onSelect = opts.onSelect;
    var overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML =
      '<div class="modal-box" style="max-width:820px;">' +
      "<h3>Community Centre — Media Library</h3>" +
      '<p class="sub">Send media to <strong>@APreferralV1_bot</strong> in a private chat to add it here.</p>' +
      '<div class="cc-grid" style="margin-bottom:8px;">' +
      '<input class="filter-input" id="cml-search" placeholder="Search internal name / filename / caption" />' +
      '<select class="filter-input" id="cml-type-filter"><option value="">All types</option>' +
      '<option value="photo">Photo</option><option value="animation">GIF</option>' +
      '<option value="video">Video</option><option value="document">Document</option></select>' +
      '<select class="filter-input" id="cml-status-filter"><option value="active">Active</option><option value="all">All</option><option value="archived">Archived</option></select>' +
      "</div>" +
      '<div class="seg" id="cml-view-toggle"><button data-value="grid" class="active">Grid</button><button data-value="list">List</button></div>' +
      '<div id="cml-results" style="margin-top:10px;max-height:420px;overflow:auto;"></div>' +
      '<div class="modal-actions"><button class="btn" id="cml-close">Close</button></div>' +
      "</div>";
    document.body.appendChild(overlay);
    if (filterType) overlay.querySelector("#cml-type-filter").value = filterType;

    function done() { overlay.remove(); }
    overlay.querySelector("#cml-close").addEventListener("click", done);
    overlay.addEventListener("click", function (e) { if (e.target === overlay) done(); });

    var view = "grid";
    function load() {
      var status = overlay.querySelector("#cml-status-filter").value;
      var mtype = overlay.querySelector("#cml-type-filter").value;
      var search = overlay.querySelector("#cml-search").value || "";
      var qs = "status=" + encodeURIComponent(status) +
        (mtype ? "&media_type=" + encodeURIComponent(mtype) : "") +
        (search ? "&search=" + encodeURIComponent(search) : "");
      var results = overlay.querySelector("#cml-results");
      results.innerHTML = '<div class="sub">Loading…</div>';
      api("/api/admin/community/media?" + qs).then(function (d) {
        renderResults(d.media || []);
      }).catch(function () {
        results.innerHTML = '<div class="banner error">Failed to load media library.</div>';
      });
    }

    function itemMeta(m) {
      var bits = [];
      bits.push(esc(m.media_type));
      if (m.filename) bits.push(esc(m.filename));
      if (m.file_size) bits.push(ccMediaFormatBytes(m.file_size));
      if (m.duration) bits.push(ccMediaFormatDuration(m.duration));
      bits.push("Uploaded " + ccUtcToKlDisplay(m.uploaded_at_utc));
      bits.push("by " + esc(m.uploaded_by));
      bits.push("used " + (m.usage_count || 0) + "x");
      return bits.join(" · ");
    }

    function renderResults(items) {
      var results = overlay.querySelector("#cml-results");
      if (!items.length) {
        results.innerHTML = '<div class="sub">No media yet. Send a photo, GIF, or video to @APreferralV1_bot to add one.</div>';
        return;
      }
      if (view === "grid") {
        results.className = "cc-grid";
        results.innerHTML = items.map(function (m) {
          return '<div class="card" data-id="' + esc(m.id) + '" style="padding:10px;">' +
            '<div style="font-size:28px;">' + (CC_MEDIA_TYPE_ICON[m.media_type] || "📎") + "</div>" +
            '<div style="font-weight:600;word-break:break-word;">' + esc(m.internal_name || "Untitled") + "</div>" +
            '<div class="sub" style="font-size:11px;">' + itemMeta(m) + "</div>" +
            (m.status === "archived" ? '<span class="pill neutral">Archived</span>' : "") +
            ccMediaItemActions(m) +
            "</div>";
        }).join("");
      } else {
        results.className = "";
        results.innerHTML = '<table class="data-table"><thead><tr><th>Name</th><th>Type</th><th>Filename</th><th>Uploaded</th><th>By</th><th>Size</th><th>Used</th><th>Actions</th></tr></thead><tbody>' +
          items.map(function (m) {
            return "<tr data-id=\"" + esc(m.id) + "\"><td>" + esc(m.internal_name || "Untitled") + (m.status === "archived" ? ' <span class="pill neutral">Archived</span>' : "") + "</td>" +
              "<td>" + (CC_MEDIA_TYPE_ICON[m.media_type] || "") + " " + esc(m.media_type) + "</td>" +
              "<td>" + esc(m.filename || "—") + "</td>" +
              "<td>" + ccUtcToKlDisplay(m.uploaded_at_utc) + "</td>" +
              "<td>" + esc(m.uploaded_by) + "</td>" +
              "<td>" + ccMediaFormatBytes(m.file_size) + "</td>" +
              "<td>" + (m.usage_count || 0) + "</td>" +
              "<td>" + ccMediaItemActions(m) + "</td></tr>";
          }).join("") + "</tbody></table>";
      }
      wireItemActions(items);
    }

    function ccMediaItemActions(m) {
      var actions = [];
      if (onSelect && m.status === "active") actions.push('<button class="btn primary" data-cml-select="' + esc(m.id) + '" type="button">Select</button>');
      actions.push('<button class="btn" data-cml-rename="' + esc(m.id) + '" type="button">Rename</button>');
      actions.push(m.status === "archived"
        ? '<button class="btn" data-cml-restore="' + esc(m.id) + '" type="button">Restore</button>'
        : '<button class="btn danger" data-cml-archive="' + esc(m.id) + '" type="button">Archive</button>');
      actions.push('<button class="btn" data-cml-advanced="' + esc(m.id) + '" type="button">Advanced</button>');
      return '<div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;">' + actions.join("") + "</div>";
    }

    function wireItemActions(items) {
      var byId = {};
      items.forEach(function (m) { byId[m.id] = m; });
      $all("[data-cml-select]", overlay).forEach(function (btn) {
        btn.addEventListener("click", function () {
          var m = byId[btn.dataset.cmlSelect];
          if (m && onSelect) onSelect(m);
          done();
        });
      });
      $all("[data-cml-rename]", overlay).forEach(function (btn) {
        btn.addEventListener("click", function () {
          var m = byId[btn.dataset.cmlRename];
          var name = window.prompt("Rename media", m.internal_name || "");
          if (name === null) return;
          name = name.trim();
          if (!name) { toast("❌ Name cannot be empty", "error"); return; }
          apiPatchJson("/api/admin/community/media/" + m.id, { internal_name: name }).then(function (r) {
            if (!r.ok) { toast("❌ " + ((r.d && r.d.code) || "rename_failed"), "error"); return; }
            toast("✅ Renamed", "success");
            load();
          });
        });
      });
      $all("[data-cml-archive]", overlay).forEach(function (btn) {
        btn.addEventListener("click", function () {
          apiPost("/api/admin/community/media/" + btn.dataset.cmlArchive + "/archive").then(function () {
            toast("✅ Archived", "success"); load();
          });
        });
      });
      $all("[data-cml-restore]", overlay).forEach(function (btn) {
        btn.addEventListener("click", function () {
          apiPost("/api/admin/community/media/" + btn.dataset.cmlRestore + "/restore").then(function () {
            toast("✅ Restored", "success"); load();
          });
        });
      });
      $all("[data-cml-advanced]", overlay).forEach(function (btn) {
        btn.addEventListener("click", function () {
          var m = byId[btn.dataset.cmlAdvanced];
          window.prompt("Telegram file_id (advanced — copy manually)", m.file_id || "");
        });
      });
    }

    var searchTimer = null;
    overlay.querySelector("#cml-search").addEventListener("input", function () {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(load, 300);
    });
    overlay.querySelector("#cml-type-filter").addEventListener("change", load);
    overlay.querySelector("#cml-status-filter").addEventListener("change", load);
    overlay.querySelector("#cml-view-toggle").addEventListener("click", function (e) {
      var btn = e.target.closest("button[data-value]");
      if (!btn) return;
      view = btn.dataset.value;
      $all("#cml-view-toggle button", overlay).forEach(function (b) { b.classList.toggle("active", b === btn); });
      load();
    });

    load();
  }

  // ---------- Composer: poll / quiz options ----------
  function ccRenderPollOptions() {
    var ct = $("#cc-content-type").value;
    var isQuiz = ct === "quiz";
    var wrap = $("#cc-poll-options");
    if (!wrap) return;
    wrap.innerHTML = cc.pollOptions.map(function (o, i) {
      var radio = isQuiz
        ? '<input type="radio" name="cc-quiz-correct" data-idx="' + i + '" ' + (cc.quizCorrect === i ? "checked" : "") + ' title="Correct answer" />'
        : "";
      return '<div class="cc-poll-option-row" data-idx="' + i + '">' + radio +
        '<input class="filter-input cc-poll-option-text" data-idx="' + i + '" value="' + esc(o.text) + '" placeholder="Option text" style="flex:1;" />' +
        '<button class="btn" data-cc-opt-up="' + i + '" type="button">&uarr;</button>' +
        '<button class="btn" data-cc-opt-down="' + i + '" type="button">&darr;</button>' +
        '<button class="btn danger" data-cc-opt-remove="' + i + '" type="button">Remove</button></div>';
    }).join("");
    var max = (cc.limits && cc.limits.poll_options_max) || 10;
    var min = (cc.limits && cc.limits.poll_options_min) || 2;
    $("#cc-poll-options-counter").textContent = cc.pollOptions.length + " / " + max + " options (minimum " + min + ")";
  }

  function ccSyncPollOptionsFromDom() {
    $all("#cc-poll-options .cc-poll-option-row").forEach(function (row) {
      var i = +row.dataset.idx;
      if (!cc.pollOptions[i]) return;
      cc.pollOptions[i].text = (row.querySelector(".cc-poll-option-text").value || "").trim();
    });
    var checked = document.querySelector("input[name='cc-quiz-correct']:checked");
    if (checked) cc.quizCorrect = +checked.dataset.idx;
  }

  // ---------- Composer: inline buttons ----------
  function ccRenderButtons() {
    var wrap = $("#cc-buttons-list");
    if (!wrap) return;
    var allowedCallbacks = (cc.limits && cc.limits.allowed_callback_actions) || ["noop"];
    wrap.innerHTML = cc.buttons.map(function (b, i) {
      var typeOpts = (cc.limits && cc.limits.button_types || ["url", "telegram_link", "webapp", "callback"]).map(function (t) {
        return '<option value="' + t + '"' + (b.type === t ? " selected" : "") + '>' + t + "</option>";
      }).join("");
      var valueField = b.type === "callback"
        ? '<select class="filter-input cc-btn-value" data-idx="' + i + '">' +
          allowedCallbacks.map(function (a) { return '<option value="' + esc(a) + '"' + (b.value === a ? " selected" : "") + '>' + esc(a) + "</option>"; }).join("") +
          "</select>"
        : '<input class="filter-input cc-btn-value" data-idx="' + i + '" placeholder="https://..." value="' + esc(b.value || "") + '" style="flex:1;min-width:200px;" />';
      return '<div class="cc-button-row" data-idx="' + i + '">' +
        '<input class="filter-input cc-btn-text" data-idx="' + i + '" placeholder="Button text" value="' + esc(b.text || "") + '" style="max-width:160px;" />' +
        '<select class="filter-input cc-btn-type" data-idx="' + i + '">' + typeOpts + "</select>" +
        valueField +
        '<input class="filter-input cc-btn-row" data-idx="' + i + '" type="number" min="0" max="7" value="' + (b.row || 0) + '" style="max-width:70px;" title="Row" />' +
        '<button class="btn danger" data-cc-btn-remove="' + i + '" type="button">Remove</button></div>';
    }).join("");
  }

  function ccSyncButtonsFromDom() {
    $all("#cc-buttons-list .cc-button-row").forEach(function (row) {
      var i = +row.dataset.idx;
      if (!cc.buttons[i]) return;
      cc.buttons[i].text = (row.querySelector(".cc-btn-text").value || "").trim();
      cc.buttons[i].type = row.querySelector(".cc-btn-type").value;
      cc.buttons[i].value = (row.querySelector(".cc-btn-value").value || "").trim();
      cc.buttons[i].row = parseInt(row.querySelector(".cc-btn-row").value, 10) || 0;
    });
  }

  function ccResetComposer() {
    cc.editingId = null;
    cc.editingUpdatedAt = null;
    cc.media = [];
    cc.buttons = [];
    cc.pollOptions = [{ text: "" }, { text: "" }];
    cc.quizCorrect = 0;
    $("#cc-title").value = "";
    $("#cc-content-type").value = "text";
    $("#cc-text").value = "";
    cc.textMode = "rich";
    if (ccRteEditor()) ccRteEditor().innerHTML = "";
    $("#cc-parse-mode").value = "HTML";
    $("#cc-disable-preview").checked = false;
    $("#cc-poll-question").value = "";
    $("#cc-poll-anonymous").checked = true;
    $("#cc-poll-multiple").checked = false;
    $("#cc-poll-members-only").checked = false;
    $("#cc-quiz-explanation").value = "";
    $("#cc-poll-close-mode").value = "manual";
    $("#cc-poll-duration-seconds").value = "";
    $("#cc-poll-close-at").value = "";
    $("#cc-disable-notification").checked = false;
    $("#cc-protect-content").checked = false;
    $("#cc-pin-after-send").checked = false;
    $("#cc-unpin-at").value = "";
    $("#cc-campaign-tags").value = "";
    $("#cc-internal-notes").value = "";
    $("#cc-scheduled-at").value = "";
    $("#cc-recurrence-type").value = "daily";
    $all("#cc-recurrence-weekdays input").forEach(function (x) { x.checked = false; });
    $all("#cc-schedule-type button").forEach(function (b) { b.classList.toggle("active", b.dataset.value === "once"); });
    $("#cc-schedule-once").classList.remove("hidden");
    $("#cc-schedule-recurring").classList.add("hidden");
    $("#cc-poll-duration-wrap").classList.add("hidden");
    $("#cc-poll-date-wrap").classList.add("hidden");
    $("#cc-unpin-wrap").classList.add("hidden");
    $("#cc-composer-banner").innerHTML = "";
    ccUpdateContentTypeVisibility();
    ccRenderPollOptions();
    ccRenderButtons();
  }

  function ccLoadPostIntoForm(post) {
    ccResetComposer();
    cc.editingId = post._id;
    cc.editingUpdatedAt = post.updated_at;
    $("#cc-title").value = post.title || "";
    $("#cc-content-type").value = post.content_type;
    $("#cc-destination").value = post.destination_key || "";
    $("#cc-text").value = post.text || "";
    cc.textMode = "rich";
    if (ccRteEditor()) ccRteEditor().innerHTML = ccRteSanitizeHtml(post.text || "");
    $("#cc-parse-mode").value = post.parse_mode || "HTML";
    $("#cc-disable-preview").checked = !!post.disable_web_page_preview;
    cc.media = (post.media || []).map(function (m) {
      return {
        type: m.type,
        media_library_id: m.media_library_id || "",
        source_url: m.storage_key || "",
        telegram_file_id: m.telegram_file_id || "",
        _libraryLabel: m.media_library_id ? (m.filename || "Media Library item") : "",
      };
    });
    if (post.poll) {
      $("#cc-poll-question").value = post.poll.question || "";
      cc.pollOptions = (post.poll.options || []).map(function (o) { return { text: o.text }; });
      cc.quizCorrect = post.poll.correct_option_id || 0;
      $("#cc-poll-anonymous").checked = !!post.poll.is_anonymous;
      $("#cc-poll-multiple").checked = !!post.poll.allows_multiple_answers;
      $("#cc-poll-members-only").checked = !!post.poll.members_only;
      $("#cc-quiz-explanation").value = post.poll.explanation || "";
      $("#cc-poll-close-mode").value = post.poll.close_mode || "manual";
      $("#cc-poll-duration-seconds").value = post.poll.open_period_seconds || "";
      $("#cc-poll-close-at").value = ccUtcToKlInputValue(post.poll.close_at_utc);
    }
    cc.buttons = (post.buttons || []).map(function (b) { return { row: b.row, position: b.position, text: b.text, type: b.type, value: b.value }; });
    $("#cc-disable-notification").checked = !!post.disable_notification;
    $("#cc-protect-content").checked = !!post.protect_content;
    $("#cc-pin-after-send").checked = !!post.pin_after_send;
    $("#cc-unpin-at").value = ccUtcToKlInputValue(post.unpin_at_utc);
    $("#cc-campaign-tags").value = (post.campaign_tags || []).join(", ");
    $("#cc-internal-notes").value = post.internal_notes || "";
    if (post.scheduled_at_utc) $("#cc-scheduled-at").value = ccUtcToKlInputValue(post.scheduled_at_utc);
    if (post.schedule_type === "recurring" && post.recurrence) {
      $all("#cc-schedule-type button").forEach(function (b) { b.classList.toggle("active", b.dataset.value === "recurring"); });
      $("#cc-schedule-once").classList.add("hidden");
      $("#cc-schedule-recurring").classList.remove("hidden");
      $("#cc-recurrence-type").value = post.recurrence.type || "daily";
      var weekdayKeys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
      var recWeekdays = post.recurrence.weekdays || [];
      $all("#cc-recurrence-weekdays input").forEach(function (x) { x.checked = recWeekdays.indexOf(weekdayKeys.indexOf(x.value)) !== -1; });
    }
    ccUpdateContentTypeVisibility();
    ccRenderPollOptions();
    ccRenderButtons();
    $("#cc-poll-duration-wrap").classList.toggle("hidden", $("#cc-poll-close-mode").value !== "duration");
    $("#cc-poll-date-wrap").classList.toggle("hidden", $("#cc-poll-close-mode").value !== "date");
    $("#cc-unpin-wrap").classList.toggle("hidden", !$("#cc-pin-after-send").checked);
    $("#cc-composer-banner").innerHTML = '<div class="banner ok">Editing "' + esc(post.title) + '" (' + esc(post.status) + ')</div>';
  }

  function ccBuildPayload() {
    ccRteFinalizeBeforeSubmit();
    ccSyncMediaFromDom();
    ccSyncPollOptionsFromDom();
    ccSyncButtonsFromDom();
    var ct = $("#cc-content-type").value;
    var scheduleType = ($("#cc-schedule-type .active") || {}).dataset ? $("#cc-schedule-type .active").dataset.value : "once";
    var payload = {
      title: ($("#cc-title").value || "").trim(),
      content_type: ct,
      destination_key: $("#cc-destination").value,
      parse_mode: $("#cc-parse-mode").value,
      disable_web_page_preview: $("#cc-disable-preview").checked,
      disable_notification: $("#cc-disable-notification").checked,
      protect_content: $("#cc-protect-content").checked,
      pin_after_send: $("#cc-pin-after-send").checked,
      unpin_at_utc: $("#cc-pin-after-send").checked ? ccKlInputToUtcIso($("#cc-unpin-at").value) : null,
      campaign_tags: ($("#cc-campaign-tags").value || "").split(",").map(function (s) { return s.trim(); }).filter(Boolean),
      internal_notes: ($("#cc-internal-notes").value || "").trim() || null,
      schedule_type: scheduleType,
      buttons: cc.buttons.map(function (b, i) { return { row: b.row || 0, position: i, text: b.text, type: b.type, value: b.value }; }),
    };
    if (ct === "text" || ct === "photo" || ct === "animation" || ct === "video" || ct === "media_group") {
      payload.text = $("#cc-text").value || "";
    }
    if (ct === "photo" || ct === "animation" || ct === "video" || ct === "media_group") {
      payload.media = cc.media.map(function (m, i) {
        var item = { type: (ct === "media_group" ? m.type : ct), position: i };
        if (m.media_library_id) {
          item.media_library_id = m.media_library_id;
        } else {
          item.source_url = m.source_url;
          item.telegram_file_id = m.telegram_file_id || null;
        }
        return item;
      });
    }
    if (ct === "poll" || ct === "quiz") {
      var closeMode = $("#cc-poll-close-mode").value;
      payload.poll = {
        question: $("#cc-poll-question").value || "",
        options: cc.pollOptions.map(function (o) { return { text: o.text }; }),
        is_anonymous: $("#cc-poll-anonymous").checked,
        allows_multiple_answers: ct === "quiz" ? false : $("#cc-poll-multiple").checked,
        members_only: $("#cc-poll-members-only").checked,
        close_mode: closeMode,
        open_period_seconds: closeMode === "duration" ? (parseInt($("#cc-poll-duration-seconds").value, 10) || null) : null,
        close_at_utc: closeMode === "date" ? ccKlInputToUtcIso($("#cc-poll-close-at").value) : null,
      };
      if (ct === "quiz") {
        payload.poll.correct_option_id = cc.quizCorrect;
        payload.poll.explanation = $("#cc-quiz-explanation").value || null;
      }
    }
    if (scheduleType === "recurring") {
      var weekdayKeys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
      var weekdays = $all("#cc-recurrence-weekdays input:checked").map(function (x) { return x.value; });
      payload.recurrence = { type: $("#cc-recurrence-type").value, weekdays: weekdays };
    }
    return payload;
  }

  function ccSaveDraft(showToast) {
    ccRteFinalizeBeforeSubmit();
    var limitErr = ccRteValidateLimits();
    if (limitErr) { toast("❌ " + limitErr, "error"); return Promise.resolve(null); }
    var payload = ccBuildPayload();
    var req = cc.editingId
      ? apiPatchJson("/api/admin/community/posts/" + cc.editingId, payload)
      : apiPostJson("/api/admin/community/posts", payload);
    return req.then(function (res) {
      if (!res.ok || !res.d.success) {
        toast("❌ " + (res.d && res.d.code || "save_failed"), "error");
        return null;
      }
      cc.editingId = res.d.post._id;
      cc.editingUpdatedAt = res.d.post.updated_at;
      if (showToast) toast("✅ Saved as draft", "success");
      return res.d.post;
    });
  }

  function ccPreviewBubbleHtml(preview) {
    var html = '<div class="cc-preview-bubble">';
    if (preview.is_poll) {
      html += "<strong>" + esc(preview.poll.question) + "</strong>";
      html += '<div class="sub">' + (preview.poll.type === "quiz" ? "Quiz" : "Regular Poll") +
        " · " + (preview.poll.is_anonymous ? "Anonymous" : "Non-anonymous") +
        " · " + (preview.poll.allows_multiple_answers ? "Multiple answers" : "Single answer") +
        (preview.poll.members_only ? " · Members-only" : "") + "</div>";
      preview.poll.options.forEach(function (o, i) {
        var mark = (preview.poll.type === "quiz" && i === preview.poll.correct_option_id) ? " ✅ (admin-only: correct answer)" : "";
        html += '<div style="margin-top:4px;">◻ ' + esc(o.text) + mark + "</div>";
      });
      if (preview.poll.type === "quiz" && preview.poll.explanation) {
        html += '<div class="sub" style="margin-top:6px;">Explanation shown after answering: ' + preview.poll.explanation + "</div>";
      }
      var closing = preview.poll.close_mode === "duration" ? ("Closes " + preview.poll.open_period_seconds + "s after publish")
        : preview.poll.close_mode === "date" ? ("Closes at " + ccUtcToKlDisplay(preview.poll.close_at_utc))
        : "Open until manually stopped";
      html += '<div class="sub" style="margin-top:6px;">' + closing + "</div>";
    } else {
      if (preview.media && preview.media.length) {
        html += '<div class="sub">[' + preview.media.length + " media item(s): " + preview.media.map(function (m) { return m.type; }).join(", ") + "]</div>";
      }
      // Only HTML parse_mode content is sanitized to a safe tag allowlist
      // server-side (see community_centre_limits.sanitize_telegram_html) —
      // MarkdownV2 text is stored verbatim and MUST be escaped before ever
      // touching innerHTML, or a saved draft could run script in this
      // admin-only preview modal.
      var isHtmlContent = preview.parse_mode === "HTML";
      var captionHtml = preview.text ? (isHtmlContent ? preview.text : esc(preview.text)) : "<em>(no text)</em>";
      html += '<div class="cc-preview-caption">' + captionHtml + "</div>";
    }
    if (preview.buttons && preview.buttons.length) {
      var rows = {};
      preview.buttons.forEach(function (b) { (rows[b.row] = rows[b.row] || []).push(b); });
      Object.keys(rows).sort(function (a, b) { return a - b; }).forEach(function (r) {
        html += '<div class="cc-preview-row">' + rows[r].map(function (b) { return '<span class="cc-preview-btn">' + esc(b.text) + "</span>"; }).join("") + "</div>";
      });
      html += '<div class="sub" style="margin-top:6px;">' + esc(preview.button_style_note) + "</div>";
    }
    html += "</div>";
    return html;
  }

  // publish-now/schedule are only valid transitions from draft/approved
  // (see schedule_post/publish_now in community_centre.py) — showing those
  // controls for every status let an admin "Preview" a failed/scheduled
  // post and hit Confirm, which called /schedule with the post's stale
  // original scheduled_at_utc and surfaced schedule_post's "not_schedulable"
  // conflict code as if it were a Retry failure. Gate the controls on status.
  var CC_PREVIEW_PUBLISHABLE_STATUSES = ["draft", "approved"];

  function ccOpenPreviewModal(post) {
    apiPost("/api/admin/community/posts/" + post._id + "/preview").then(function (res) {
      var preview = res.preview;
      var overlay = document.createElement("div");
      overlay.className = "modal-overlay";
      var canPublish = CC_PREVIEW_PUBLISHABLE_STATUSES.indexOf(post.status) !== -1;
      var scheduledAtVal = post.scheduled_at_utc ? ccUtcToKlInputValue(post.scheduled_at_utc) : ($("#cc-scheduled-at") ? $("#cc-scheduled-at").value : "");
      var actionsHtml;
      if (canPublish) {
        actionsHtml =
          '<div style="margin-top:14px;">Publish: <select id="cc-preview-mode" class="filter-input" style="max-width:200px;display:inline-block;">' +
          '<option value="now">Now</option><option value="schedule"' + (scheduledAtVal ? " selected" : "") + ">Scheduled time</option></select></div>" +
          '<div class="modal-actions">' +
          '<button class="btn" id="cc-preview-cancel">Back to edit</button>' +
          '<button class="btn primary" id="cc-preview-confirm">Confirm Publish / Schedule</button>' +
          "</div>";
      } else if (post.status === "failed") {
        var pvRunRef = (post.latest_run_id || "").slice(0, 8) || "—";
        var pvFallback = "Internal publish error. Check worker logs using reference: " + pvRunRef + ".";
        var pvCode = post.last_error_code || "unknown_error";
        var pvMessage = CC_BOT_NOT_READY_CODES[pvCode] ? CC_BOT_NOT_READY_MESSAGE :
          ((pvCode === "unknown_error" || !post.last_error_message) ? pvFallback : post.last_error_message);
        var statusNote =
          "Status: Failed<br>" +
          "Error: " + esc(pvCode) + "<br>" +
          (post.last_failed_step ? "Step: " + esc(post.last_failed_step) + "<br>" : "") +
          "Message: " + esc(pvMessage) + "<br>" +
          "Attempts: " + fmt(post.attempt_count || 0) + "<br>" +
          "Reference: " + esc(pvRunRef) + "<br>" +
          (post.retryable === true
            ? "Use Retry from the Failed list to requeue it."
            : "Not retryable — fix the underlying issue, then Edit Post or Duplicate Post.");
        actionsHtml =
          '<p class="sub" style="margin-top:14px;">' + statusNote + "</p>" +
          '<div class="modal-actions"><button class="btn primary" id="cc-preview-cancel">Close</button></div>';
      } else {
        var statusNote2 = "This post is " + esc(post.status) + " and cannot be published/scheduled from here.";
        actionsHtml =
          '<p class="sub" style="margin-top:14px;">' + statusNote2 + "</p>" +
          '<div class="modal-actions"><button class="btn primary" id="cc-preview-cancel">Close</button></div>';
      }
      overlay.innerHTML =
        '<div class="modal-box" style="max-width:520px;">' +
        "<h3>Preview — " + esc(preview.title) + "</h3>" +
        '<p class="sub">Destination: ' + esc(preview.destination_name) + " · Silent: " + (preview.disable_notification ? "Yes" : "No") +
        " · Pin: " + (preview.pin_after_send ? "Yes" : "No") + "</p>" +
        ccPreviewBubbleHtml(preview) +
        '<p class="sub" style="margin-top:8px;">Formatting preview may differ slightly from the Telegram app.</p>' +
        actionsHtml +
        "</div>";
      document.body.appendChild(overlay);
      function done() { overlay.remove(); }
      overlay.querySelector("#cc-preview-cancel").addEventListener("click", done);
      overlay.addEventListener("click", function (e) { if (e.target === overlay) done(); });
      if (canPublish) {
        overlay.querySelector("#cc-preview-confirm").addEventListener("click", function () {
          var mode = overlay.querySelector("#cc-preview-mode").value;
          var action;
          if (mode === "now") {
            action = apiPost("/api/admin/community/posts/" + post._id + "/publish-now");
          } else {
            var iso = ccKlInputToUtcIso(scheduledAtVal);
            if (!iso) { toast("❌ Choose a scheduled time first", "error"); return; }
            action = apiPostJson("/api/admin/community/posts/" + post._id + "/schedule", { scheduled_at: iso });
          }
          action.then(function (r) {
            var body = r && r.d !== undefined ? r.d : r;
            if (!body || !body.success) { toast("❌ " + ((body && body.code) || "publish_failed"), "error"); return; }
            toast(mode === "now" ? "✅ Queued for immediate publish" : "✅ Scheduled", "success");
            done();
            ccResetComposer();
            if (currentModuleKey === "community") loadCcBoard(true);
          });
        });
      }
    }).catch(function () { toast("❌ Failed to build preview", "error"); });
  }

  function loadCcComposer() {
    ccEnsureMeta().then(function () {
      ccRenderDestinationSelect();
      if (!$("#cc-destination").value && cc.destinations && cc.destinations.length) {
        var enabled = cc.destinations.filter(function (d) { return d.enabled; });
        if (enabled.length) $("#cc-destination").value = enabled[0].key;
      }
      ccUpdateContentTypeVisibility();
    });
  }

  // ---------- Calendar ----------
  function ccStartOfDay(d) { var x = new Date(d); x.setUTCHours(0, 0, 0, 0); return x; }

  function loadCcCalendar() {
    if (!cc.calendarStart) cc.calendarStart = ccStartOfDay(new Date(Date.now() - 86400000));
    var start = cc.calendarStart;
    var end = new Date(start.getTime() + 31 * 86400000);
    statePanel("cc-calendar-body", "loading", "Loading calendar…");
    api("/api/admin/community/calendar?start=" + start.toISOString() + "&end=" + end.toISOString()).then(function (d) {
      var entries = d.entries || [];
      if (!entries.length) { $("#cc-calendar-body").innerHTML = emptyState("Nothing scheduled in this window."); return; }
      entries.sort(function (a, b) { return new Date(a.at_utc) - new Date(b.at_utc); });
      var byDay = {};
      entries.forEach(function (e) {
        var day = ccUtcToKlDisplay(e.at_utc).slice(0, 10);
        (byDay[day] = byDay[day] || []).push(e);
      });
      var html = "";
      Object.keys(byDay).sort().forEach(function (day) {
        html += '<div class="section-title" style="margin-top:14px;">' + day + "</div>";
        byDay[day].forEach(function (e) {
          var kindLabel = e.kind === "scheduled_post" ? "Publish" : e.kind === "poll_closing" ? "Poll closes" : "Auto-unpin";
          html += '<div class="attention-item sev-yellow" data-cc-calendar-open="' + e.id + '" style="cursor:pointer;">' +
            '<span class="attention-dot"></span><div class="attention-text"><strong>' + (CC_CONTENT_ICON[e.content_type] || "•") + " " + esc(e.title) + "</strong>" +
            '<span>' + kindLabel + " · " + ccUtcToKlDisplay(e.at_utc) + " · " + esc(e.destination_name || "") + "</span></div></div>";
        });
      });
      $("#cc-calendar-body").innerHTML = html;
    }).catch(function (e) { statePanel("cc-calendar-body", "error", "Failed to load calendar: " + e.message); });
  }

  // ---------- Shared board (Scheduled / Drafts / Pending Approval / Published / Poll Results / Failed) ----------
  var CC_BOARD_QUERY = {
    scheduled: { status: "scheduled" },
    draft: { status: "draft" },
    pending_approval: { status: "pending_approval" },
    published: { statuses: "published,partially_published" },
    failed: { status: "failed" },
  };

  function ccStatusPill(status) {
    var cls = status === "published" ? "approved" : status === "failed" ? "rejected" : status === "scheduled" || status === "processing" ? "neutral" : "neutral";
    return '<span class="pill ' + cls + '">' + esc(status) + "</span>";
  }

  function ccPostActionsHtml(post) {
    var acts = [];
    acts.push('<button class="btn" data-cc-post-action="preview" data-id="' + post._id + '">Preview</button>');
    if (post.status === "draft" || post.status === "scheduled" || post.status === "pending_approval") {
      acts.push('<button class="btn" data-cc-post-action="edit" data-id="' + post._id + '">Edit</button>');
    }
    acts.push('<button class="btn" data-cc-post-action="duplicate" data-id="' + post._id + '">Duplicate</button>');
    if (post.status === "draft" || post.status === "approved") {
      acts.push('<button class="btn primary" data-cc-post-action="publish-now" data-id="' + post._id + '">Publish Now</button>');
    }
    if (post.status === "pending_approval") {
      acts.push('<button class="btn primary" data-cc-post-action="approve" data-id="' + post._id + '">Approve</button>');
      acts.push('<button class="btn danger" data-cc-post-action="reject" data-id="' + post._id + '">Reject</button>');
    }
    if (["draft", "pending_approval", "approved", "scheduled", "failed"].indexOf(post.status) !== -1) {
      acts.push('<button class="btn danger" data-cc-post-action="cancel" data-id="' + post._id + '">Cancel</button>');
    }
    if (post.status === "failed") {
      acts.push('<button class="btn" data-cc-post-action="retry" data-id="' + post._id + '">Retry</button>');
    }
    if ((post.content_type === "poll" || post.content_type === "quiz") && post.poll_status === "open") {
      acts.push('<button class="btn danger" data-cc-post-action="stop-poll" data-id="' + post._id + '">Stop Poll</button>');
    }
    if ((post.content_type === "poll" || post.content_type === "quiz") && post.telegram_poll_ids && post.telegram_poll_ids.length) {
      acts.push('<button class="btn" data-cc-post-action="view-results" data-id="' + post._id + '">View Results</button>');
    }
    if ((post.status === "published" || post.status === "partially_published") && post.pin_after_send && !post.unpinned_at_utc) {
      acts.push('<button class="btn" data-cc-post-action="unpin" data-id="' + post._id + '">Unpin</button>');
    }
    return acts.join(" ");
  }

  function ccRenderPostsTable(posts, opts) {
    if (!posts.length) return emptyState("Nothing here yet.");
    var showError = !!(opts && opts.showError);
    var rows = posts.map(function (p) {
      var pollClose = p.poll ? (p.poll.close_mode === "date" ? ccUtcToKlDisplay(p.poll.close_at_utc) : p.poll.close_mode === "duration" ? (p.poll.open_period_seconds + "s after publish") : "Manual") : "—";
      var errorCell = "";
      if (showError) {
        if (p.last_error_code) {
          var codeHtml = '<span class="pill rejected" title="' + esc(p.last_error_message || "") + '">' + esc(p.last_error_code) + "</span>";
          var attemptsHtml = '<div class="sub">Attempts: ' + fmt(p.attempt_count || 0) + " · Last: " + ccUtcToKlDisplay(p.updated_at) + "</div>";
          errorCell = "<td>" + codeHtml + attemptsHtml + "</td>";
        } else {
          errorCell = "<td>—</td>";
        }
      }
      return "<tr>" +
        "<td>" + (CC_CONTENT_ICON[p.content_type] || "") + " " + esc(p.title) + "</td>" +
        "<td>" + esc(p.destination_name || "") + "</td>" +
        "<td>" + esc(p.content_type) + "</td>" +
        "<td>" + ccStatusPill(p.status) + (p.poll_status && p.poll_status !== "not_applicable" ? " " + ccStatusPill(p.poll_status) : "") + "</td>" +
        "<td>" + esc(p.schedule_type === "recurring" ? (p.recurrence ? p.recurrence.type : "recurring") : "one-time") + "</td>" +
        "<td>" + pollClose + "</td>" +
        "<td>" + ccUtcToKlDisplay(p.next_run_at_utc || p.scheduled_at_utc || p.published_at || p.updated_at) + "</td>" +
        "<td>" + esc(p.created_by_username || p.created_by || "") + "</td>" +
        errorCell +
        "<td>" + ccPostActionsHtml(p) + "</td>" +
        "</tr>";
    }).join("");
    var errorHeader = showError ? "<th>Error</th>" : "";
    return '<table class="data-table"><thead><tr><th>Title</th><th>Destination</th><th>Type</th><th>Status</th>' +
      "<th>Recurrence</th><th>Poll Closing</th><th>Time (KL)</th><th>Created By</th>" + errorHeader + "<th>Actions</th></tr></thead><tbody>" + rows + "</tbody></table>";
  }

  function ccRenderPollResultsTable(posts) {
    if (!posts.length) return emptyState("No polls yet.");
    var rows = posts.map(function (p) {
      return "<tr>" +
        "<td>" + (CC_CONTENT_ICON[p.content_type] || "") + " " + esc((p.poll && p.poll.question) || p.title) + "</td>" +
        "<td>" + esc(p.destination_name || "") + "</td>" +
        "<td>" + ccUtcToKlDisplay(p.published_at) + "</td>" +
        "<td>" + ccStatusPill(p.poll_status) + "</td>" +
        "<td>" + esc(p.content_type === "quiz" ? "Quiz" : "Regular") + (p.poll && p.poll.is_anonymous ? " · Anonymous" : " · Non-anonymous") + "</td>" +
        "<td>" + ccPostActionsHtml(p) + "</td>" +
        "</tr>";
    }).join("");
    return '<table class="data-table"><thead><tr><th>Question</th><th>Destination</th><th>Published</th><th>Status</th><th>Type</th><th>Actions</th></tr></thead><tbody>' + rows + "</tbody></table>";
  }

  function loadCcBoard() {
    var board = currentCcBoard || "scheduled";
    statePanel("cc-board-body", "loading", "Loading…");
    if (board === "poll_results") {
      api("/api/admin/community/polls?limit=100").then(function (d) {
        $("#cc-board-body").innerHTML = ccRenderPollResultsTable(d.posts || []);
      }).catch(function (e) { statePanel("cc-board-body", "error", "Failed to load: " + e.message); });
      return;
    }
    var q = CC_BOARD_QUERY[board] || CC_BOARD_QUERY.scheduled;
    var qs = Object.keys(q).map(function (k) { return k + "=" + encodeURIComponent(q[k]); }).join("&");
    api("/api/admin/community/posts?limit=100&" + qs).then(function (d) {
      $("#cc-board-body").innerHTML = ccRenderPostsTable(d.posts || [], { showError: board === "failed" });
    }).catch(function (e) { statePanel("cc-board-body", "error", "Failed to load: " + e.message); });
  }

  function ccShowPollResultsModal(postId) {
    api("/api/admin/community/polls/" + postId + "/results").then(function (d) {
      var r = d.results;
      var overlay = document.createElement("div");
      overlay.className = "modal-overlay";
      var optRows = (r.options || []).map(function (o) {
        var correct = r.correct_option_id === o.option_id ? " ✅" : "";
        return "<div>" + esc(o.text) + correct + " — " + fmt(o.voter_count) + " votes (" + o.percentage + "%)</div>";
      }).join("");
      overlay.innerHTML = '<div class="modal-box" style="max-width:480px;">' +
        "<h3>" + esc(r.question || "Poll results") + "</h3>" +
        '<p class="sub">' + esc(r.destination_name || "") + " · " + ccStatusPill(r.poll_status) + " · " +
        (r.is_anonymous ? "Anonymous" : "Non-anonymous") + " · " + (r.allows_multiple_answers ? "Multiple answers" : "Single answer") + "</p>" +
        "<p>Total voters: " + fmt(r.total_voters) + "</p>" + optRows +
        (r.correct_answer_rate !== null && r.correct_answer_rate !== undefined ? "<p style=\"margin-top:8px;\">Correct-answer rate: " + r.correct_answer_rate + "%</p>" : "") +
        '<div class="modal-actions"><button class="btn primary" id="cc-results-close">Close</button></div></div>';
      document.body.appendChild(overlay);
      function done() { overlay.remove(); }
      overlay.querySelector("#cc-results-close").addEventListener("click", done);
      overlay.addEventListener("click", function (e) { if (e.target === overlay) done(); });
    }).catch(function () { toast("❌ Failed to load poll results", "error"); });
  }

  var CC_RETRY_ERROR_MESSAGES = {
    post_not_found: "This post no longer exists.",
    already_published: "This post already published successfully — refreshing lists.",
    already_processing: "Another attempt is currently processing this post. Try again shortly.",
    retry_limit_reached: "Maximum retry attempts reached for this post.",
    non_retryable_failure: "This failure isn't retryable — fix the underlying issue first, then use Edit Post or Duplicate Post.",
    not_failed: "This post is no longer in a failed state.",
    invalid_destination: "The destination is disabled or no longer available.",
  };

  // Per-error-code corrective action shown in the retry modal when a
  // failure is not retryable. `action` maps to an existing post action
  // (ccHandlePostAction) triggered after the modal closes.
  var CC_ERROR_GUIDANCE = {
    telegram_forbidden: { label: "Fix Permissions", hint: "Re-add the bot to the destination with permission to post, then retry.", action: "edit" },
    bot_lacks_permission: { label: "Fix Permissions", hint: "Grant the bot posting rights in this destination, then retry.", action: "edit" },
    bot_lacks_pin_permission: { label: "Fix Permissions", hint: "Grant the bot pin rights in this destination, then retry.", action: "edit" },
    chat_not_found: { label: "Test Destination", hint: "Confirm the destination still exists and the bot can reach it.", action: "edit" },
    invalid_chat_id: { label: "Test Destination", hint: "The destination chat ID looks invalid — verify it.", action: "edit" },
    invalid_media: { label: "Replace Media", hint: "Re-upload the media on this post, then retry.", action: "edit" },
    invalid_media_file_id: { label: "Replace Media", hint: "The saved media reference is stale on Telegram — re-upload the media, then retry.", action: "edit" },
    invalid_poll: { label: "Edit Post", hint: "Telegram rejected the poll configuration — review the poll options.", action: "edit" },
    invalid_url: { label: "Edit Post", hint: "A button URL was rejected by Telegram — review the post's buttons.", action: "edit" },
    message_too_long: { label: "Edit Post", hint: "Content exceeds Telegram's length limit — shorten it.", action: "edit" },
    invalid_request: { label: "Edit Post", hint: "Telegram rejected this request — review the post content.", action: "edit" },
    bot_loop_not_running: { label: "Duplicate Post", hint: "The publishing worker process is not currently running — contact an administrator.", action: "duplicate" },
    bot_not_ready: { label: "Duplicate Post", hint: "The worker's Telegram bot is not ready yet — contact an administrator.", action: "duplicate" },
  };

  // bot_loop_not_running / bot_not_ready both mean the worker's Telegram
  // bot hasn't finished starting up — always show this fixed copy instead
  // of the persisted message, since "Check worker startup and bot-loop
  // status" is more actionable for an admin than the raw local text.
  var CC_BOT_NOT_READY_CODES = { bot_loop_not_running: true, bot_not_ready: true };
  var CC_BOT_NOT_READY_MESSAGE = "Telegram worker is not ready. Check worker startup and bot-loop status.";

  // "23 Jul 2026, 4:03 AM" — the source string is already KL wall-clock
  // time with a +08:00 offset (see _serialize_post_for_admin on the
  // backend), so this parses the components directly instead of routing
  // through Date's UTC accessors, which would re-convert it.
  function ccFormatKlLong(iso) {
    if (!iso) return "—";
    var m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/.exec(iso);
    if (!m) return "—";
    var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    var day = parseInt(m[3], 10);
    var month = months[parseInt(m[2], 10) - 1];
    var hour = parseInt(m[4], 10);
    var ampm = hour >= 12 ? "PM" : "AM";
    var h12 = hour % 12; if (h12 === 0) h12 = 12;
    return day + " " + month + " " + m[1] + ", " + h12 + ":" + m[5] + " " + ampm;
  }

  // Builds the retry modal's inner HTML from the post's real persisted
  // failure fields — never a generic "Unexpected error contacting
  // Telegram." Falls back to the internal-error copy only when the
  // backend didn't persist a code/message (e.g. legacy data).
  function ccBuildRetryModalContent(post) {
    var code = post.last_error_code || "unknown_error";
    var runRef = (post.latest_run_id || "").slice(0, 8) || "—";
    var fallback = "Internal publish error. Check worker logs using reference: " + runRef + ".";
    var message = CC_BOT_NOT_READY_CODES[code] ? CC_BOT_NOT_READY_MESSAGE :
      ((code === "unknown_error" || !post.last_error_message) ? fallback : post.last_error_message);
    var retryable = post.retryable === true;
    var guidance = CC_ERROR_GUIDANCE[code];
    var footerHtml;
    if (retryable) {
      footerHtml = '<p class="sub">This will queue a new attempt now.</p>';
    } else if (guidance) {
      footerHtml = '<p class="sub">' + esc(guidance.hint) + '</p>' +
        '<button type="button" class="btn" id="cc-retry-corrective" data-cc-corrective-action="' + esc(guidance.action) + '">' + esc(guidance.label) + '</button>';
    } else {
      footerHtml = '<p class="sub">Fix the underlying issue before retrying.</p>';
    }
    var detailLines = "";
    if (post.last_failed_step) detailLines += "Step: " + esc(post.last_failed_step) + "<br>";
    if (post.last_exception_class) detailLines += "Exception: " + esc(post.last_exception_class) + "<br>";
    var html = "<h3>Retry failed post?</h3>" +
      '<p><strong>Error:</strong><br>' + esc(code) + '</p>' +
      '<p>' + esc(message) + '</p>' +
      '<p class="sub">' + detailLines +
      'Attempts: ' + fmt(post.attempt_count || 0) + '<br>' +
      'Last attempt: ' + esc(ccFormatKlLong(post.last_attempt_at_kl || post.updated_at)) + '<br>' +
      'Retryable: ' + (retryable ? "Yes" : "No") + '<br>' +
      'Reference: ' + esc(runRef) + '</p>' +
      footerHtml;
    return { html: html, retryable: retryable, guidance: guidance };
  }

  // ---------- Retry failed post modal: shows the real persisted failure
  // detail (never a generic "Unexpected error contacting Telegram") and
  // disables Confirm for non-retryable failures. ----------
  function ccOpenRetryModal(post) {
    return new Promise(function (resolve) {
      var built = ccBuildRetryModalContent(post);
      var overlay = document.createElement("div");
      overlay.className = "modal-overlay";
      overlay.innerHTML =
        '<div class="modal-box">' + built.html +
        '<div class="modal-actions">' +
        '<button type="button" class="btn" id="cc-retry-cancel">Cancel</button>' +
        '<button type="button" class="btn primary" id="cc-retry-confirm"' + (built.retryable ? "" : " disabled") + '>' +
        (built.retryable ? "Confirm" : "Cannot Retry") + '</button>' +
        '</div></div>';
      document.body.appendChild(overlay);
      function done(result) { overlay.remove(); resolve(result); }
      overlay.querySelector("#cc-retry-cancel").addEventListener("click", function () { done(false); });
      overlay.querySelector("#cc-retry-confirm").addEventListener("click", function () {
        if (built.retryable) done(true);
      });
      overlay.addEventListener("click", function (e) { if (e.target === overlay) done(false); });
      var corrective = overlay.querySelector("#cc-retry-corrective");
      if (corrective) {
        corrective.addEventListener("click", function () {
          var actionName = corrective.getAttribute("data-cc-corrective-action");
          done(false);
          if (actionName) ccHandlePostAction(actionName, post._id);
        });
      }
    });
  }

  function ccHandlePostAction(action, id, btnEl) {
    if (action === "retry") {
      api("/api/admin/community/posts/" + id).then(function (d) {
        var post = d.post || {};
        ccOpenRetryModal(post).then(function (ok) {
          if (!ok) return;
          if (btnEl) btnEl.disabled = true;
          apiPost("/api/admin/community/posts/" + id + "/retry").then(function (r) {
            if (!r.success) {
              var code = r.code || "retry_failed";
              toast("❌ " + (CC_RETRY_ERROR_MESSAGES[code] || code), "error");
              // "already_published" means the content actually delivered —
              // the row belongs in Published now, not Failed, so refresh
              // even though the retry call itself reported a conflict.
              if (code === "already_published") loadCcBoard(true);
              return;
            }
            toast("✅ Post queued for retry", "success");
            loadCcBoard(true);
          }).catch(function () {
            toast("❌ Retry failed", "error");
          }).finally(function () {
            if (btnEl) btnEl.disabled = false;
          });
        });
      }).catch(function () { toast("❌ Failed to load post", "error"); });
      return;
    }
    if (action === "preview") {
      api("/api/admin/community/posts/" + id).then(function (d) { ccOpenPreviewModal(d.post); });
      return;
    }
    if (action === "edit") {
      Promise.all([api("/api/admin/community/posts/" + id), ccEnsureMeta()]).then(function (results) {
        var d = results[0];
        activateTab("community", 0);
        ccRenderDestinationSelect();
        ccLoadPostIntoForm(d.post);
      });
      return;
    }
    if (action === "view-results") { ccShowPollResultsModal(id); return; }
    if (action === "reject") {
      var reason = prompt("Reason for rejection (optional):", "") || "";
      apiPostJson("/api/admin/community/posts/" + id + "/reject", { reason: reason }).then(function (r) {
        if (!r.ok) { toast("❌ " + (r.d && r.d.code || "reject_failed"), "error"); return; }
        toast("✅ Returned to draft", "success");
        loadCcBoard(true);
      });
      return;
    }
    if (action === "stop-poll") {
      confirmSimple("Stop this poll?", "Stop this poll now? Users will no longer be able to vote. This action cannot reopen the same poll.").then(function (ok) {
        if (!ok) return;
        apiPost("/api/admin/community/posts/" + id + "/stop-poll").then(function (r) {
          if (!r.success) { toast("❌ " + (r.code || "stop_poll_failed"), "error"); return; }
          toast("✅ Poll stopped", "success");
          loadCcBoard(true);
        }).catch(function () { toast("❌ Failed to stop poll", "error"); });
      });
      return;
    }
    if (action === "cancel") {
      confirmSimple("Cancel this post?", "This post will be moved to Cancelled and will not be published.").then(function (ok) {
        if (!ok) return;
        apiPost("/api/admin/community/posts/" + id + "/cancel")
          .then(function () { toast("✅ Cancelled", "success"); loadCcBoard(true); })
          .catch(function () { toast("❌ Cancel failed", "error"); });
      });
      return;
    }
    // Simple one-call actions: duplicate, publish-now, approve, retry, unpin
    apiPost("/api/admin/community/posts/" + id + "/" + action).then(function (r) {
      if (!r.success) { toast("❌ " + (r.code || (action + "_failed")), "error"); return; }
      toast("✅ Done", "success");
      loadCcBoard(true);
    }).catch(function () { toast("❌ Action failed", "error"); });
  }

  function bindCommunityCentre() {
    $("#cc-content-type").addEventListener("change", ccUpdateContentTypeVisibility);
    $("#cc-text").addEventListener("input", ccUpdateTextCounter);
    $("#cc-parse-mode").addEventListener("change", ccRteUpdateVisibility);

    var rteToolbar = $("#cc-rte-toolbar");
    var rteEditorEl = $("#cc-rte-editor");
    if (rteToolbar && rteEditorEl) {
      try { document.execCommand("styleWithCSS", false, false); } catch (e) { /* unsupported in some browsers — harmless */ }
      rteToolbar.addEventListener("click", function (e) {
        var btn = e.target.closest("button[data-cc-rte-cmd]");
        if (!btn) return;
        var cmd = btn.dataset.ccRteCmd;
        rteEditorEl.focus();
        if (cmd === "bold") { ccRteExec("bold"); ccRteSyncFromEditor(); }
        else if (cmd === "italic") { ccRteExec("italic"); ccRteSyncFromEditor(); }
        else if (cmd === "underline") { ccRteExec("underline"); ccRteSyncFromEditor(); }
        else if (cmd === "strike") { ccRteExec("strikeThrough"); ccRteSyncFromEditor(); }
        else if (cmd === "code") { ccRteToggleInline(rteEditorEl, "code"); }
        else if (cmd === "spoiler") { ccRteToggleInline(rteEditorEl, "span", "tg-spoiler"); }
        else if (cmd === "quote") { ccRteToggleQuote(rteEditorEl); }
        else if (cmd === "link") { ccRteApplyLink(rteEditorEl); }
        else if (cmd === "clear") { ccRteClearFormatting(rteEditorEl); }
      });
      rteEditorEl.addEventListener("input", ccRteSyncFromEditor);
      rteEditorEl.addEventListener("paste", ccRtePasteHandler);
      rteEditorEl.addEventListener("keydown", ccRteKeydown);
    }
    var rteSourceToggle = $("#cc-rte-source-toggle");
    if (rteSourceToggle) {
      rteSourceToggle.addEventListener("click", function () {
        if (cc.textMode === "html") {
          // HTML -> Rich Text: parse only supported Telegram tags.
          var sanitized = ccRteSanitizeHtml($("#cc-text").value || "");
          $("#cc-text").value = sanitized;
          if (rteEditorEl) rteEditorEl.innerHTML = sanitized;
          cc.textMode = "rich";
        } else {
          // Rich Text -> HTML: show canonical sanitized HTML.
          ccRteSyncFromEditor();
          cc.textMode = "html";
        }
        ccRteUpdateVisibility();
        ccUpdateTextCounter();
      });
    }

    $("#cc-media-add-btn").addEventListener("click", function () {
      ccSyncMediaFromDom();
      var ct = $("#cc-content-type").value;
      cc.media.push({ type: ct === "media_group" ? "photo" : ct, media_library_id: "", source_url: "", telegram_file_id: "" });
      ccRenderMedia();
    });
    $("#cc-media-list").addEventListener("click", function (e) {
      var t = e.target;
      if (t.dataset.ccMediaRemove !== undefined) {
        ccSyncMediaFromDom();
        cc.media.splice(+t.dataset.ccMediaRemove, 1);
        ccRenderMedia();
      } else if (t.dataset.ccMediaUp !== undefined) {
        ccSyncMediaFromDom();
        var i = +t.dataset.ccMediaUp;
        if (i > 0) { var tmp = cc.media[i - 1]; cc.media[i - 1] = cc.media[i]; cc.media[i] = tmp; }
        ccRenderMedia();
      } else if (t.dataset.ccMediaDown !== undefined) {
        ccSyncMediaFromDom();
        var j = +t.dataset.ccMediaDown;
        if (j < cc.media.length - 1) { var tmp2 = cc.media[j + 1]; cc.media[j + 1] = cc.media[j]; cc.media[j] = tmp2; }
        ccRenderMedia();
      } else if (t.dataset.ccMediaPick !== undefined) {
        ccSyncMediaFromDom();
        var pickIdx = +t.dataset.ccMediaPick;
        var ct = $("#cc-content-type").value;
        var wantType = ct === "media_group" ? cc.media[pickIdx].type : ct;
        ccOpenMediaLibraryModal({
          filterType: wantType,
          onSelect: function (m) {
            cc.media[pickIdx].media_library_id = m.id;
            cc.media[pickIdx].telegram_file_id = m.file_id;
            cc.media[pickIdx].source_url = "";
            cc.media[pickIdx]._libraryLabel = m.internal_name || m.filename || "Untitled";
            ccRenderMedia();
          },
        });
      } else if (t.dataset.ccMediaClear !== undefined) {
        ccSyncMediaFromDom();
        var clearIdx = +t.dataset.ccMediaClear;
        cc.media[clearIdx].media_library_id = "";
        cc.media[clearIdx]._libraryLabel = "";
        ccRenderMedia();
      } else if (t.dataset.ccMediaAdvancedToggle !== undefined) {
        ccSyncMediaFromDom();
        var advIdx = +t.dataset.ccMediaAdvancedToggle;
        cc.media[advIdx]._advancedOpen = !cc.media[advIdx]._advancedOpen;
        ccRenderMedia();
      }
    });

    $("#cc-poll-option-add-btn").addEventListener("click", function () {
      ccSyncPollOptionsFromDom();
      var max = (cc.limits && cc.limits.poll_options_max) || 10;
      if (cc.pollOptions.length >= max) { toast("❌ Maximum " + max + " options", "error"); return; }
      cc.pollOptions.push({ text: "" });
      ccRenderPollOptions();
    });
    $("#cc-poll-options").addEventListener("click", function (e) {
      var t = e.target;
      var min = (cc.limits && cc.limits.poll_options_min) || 2;
      if (t.dataset.ccOptRemove !== undefined) {
        ccSyncPollOptionsFromDom();
        if (cc.pollOptions.length <= min) { toast("❌ Minimum " + min + " options required", "error"); return; }
        cc.pollOptions.splice(+t.dataset.ccOptRemove, 1);
        ccRenderPollOptions();
      } else if (t.dataset.ccOptUp !== undefined) {
        ccSyncPollOptionsFromDom();
        var i = +t.dataset.ccOptUp;
        if (i > 0) { var tmp = cc.pollOptions[i - 1]; cc.pollOptions[i - 1] = cc.pollOptions[i]; cc.pollOptions[i] = tmp; }
        ccRenderPollOptions();
      } else if (t.dataset.ccOptDown !== undefined) {
        ccSyncPollOptionsFromDom();
        var j = +t.dataset.ccOptDown;
        if (j < cc.pollOptions.length - 1) { var tmp2 = cc.pollOptions[j + 1]; cc.pollOptions[j + 1] = cc.pollOptions[j]; cc.pollOptions[j] = tmp2; }
        ccRenderPollOptions();
      }
    });

    $("#cc-poll-close-mode").addEventListener("change", function () {
      $("#cc-poll-duration-wrap").classList.toggle("hidden", this.value !== "duration");
      $("#cc-poll-date-wrap").classList.toggle("hidden", this.value !== "date");
    });
    $("#cc-poll-fields").addEventListener("click", function (e) {
      var dur = e.target.dataset.ccDuration;
      if (dur) $("#cc-poll-duration-seconds").value = dur;
    });

    $("#cc-button-add-btn").addEventListener("click", function () {
      ccSyncButtonsFromDom();
      var maxTotal = (cc.limits && cc.limits.button_max_total) || 20;
      if (cc.buttons.length >= maxTotal) { toast("❌ Maximum " + maxTotal + " buttons", "error"); return; }
      cc.buttons.push({ row: 0, position: cc.buttons.length, text: "", type: "url", value: "" });
      ccRenderButtons();
    });
    $("#cc-buttons-list").addEventListener("click", function (e) {
      if (e.target.dataset.ccBtnRemove !== undefined) {
        ccSyncButtonsFromDom();
        cc.buttons.splice(+e.target.dataset.ccBtnRemove, 1);
        ccRenderButtons();
      }
    });
    $("#cc-buttons-list").addEventListener("change", function (e) {
      if (e.target.classList.contains("cc-btn-type")) { ccSyncButtonsFromDom(); ccRenderButtons(); }
    });

    $("#cc-pin-after-send").addEventListener("change", function () { $("#cc-unpin-wrap").classList.toggle("hidden", !this.checked); });

    $("#cc-schedule-type").addEventListener("click", function (e) {
      var btn = e.target.closest("button");
      if (!btn) return;
      $all("#cc-schedule-type button").forEach(function (b) { b.classList.toggle("active", b === btn); });
      $("#cc-schedule-once").classList.toggle("hidden", btn.dataset.value !== "once");
      $("#cc-schedule-recurring").classList.toggle("hidden", btn.dataset.value !== "recurring");
    });
    $("#cc-recurrence-type").addEventListener("change", function () {
      $("#cc-recurrence-weekdays").classList.toggle("hidden", this.value !== "weekly");
    });

    $("#cc-save-draft-btn").addEventListener("click", function () { ccSaveDraft(true); });
    $("#cc-clear-btn").addEventListener("click", function () { ccResetComposer(); });
    $("#cc-preview-btn").addEventListener("click", function () {
      var title = ($("#cc-title").value || "").trim();
      var dest = $("#cc-destination").value;
      if (!title) { toast("❌ Internal title is required", "error"); return; }
      if (!dest) { toast("❌ Choose a valid destination first", "error"); return; }
      ccSaveDraft(false).then(function (post) { if (post) ccOpenPreviewModal(post); });
    });
    var ccOpenTelegramSettingsBtn = $("#cc-open-telegram-settings-btn");
    if (ccOpenTelegramSettingsBtn) ccOpenTelegramSettingsBtn.addEventListener("click", ccOpenTelegramSettings);

    $("#cc-calendar-prev").addEventListener("click", function () {
      cc.calendarStart = new Date((cc.calendarStart || ccStartOfDay(new Date())).getTime() - 31 * 86400000);
      loadCcCalendar(true);
    });
    $("#cc-calendar-next").addEventListener("click", function () {
      cc.calendarStart = new Date((cc.calendarStart || ccStartOfDay(new Date())).getTime() + 31 * 86400000);
      loadCcCalendar(true);
    });
    $("#cc-calendar-body").addEventListener("click", function (e) {
      var id = e.target.closest && e.target.closest("[data-cc-calendar-open]");
      if (id) ccHandlePostAction("preview", id.dataset.ccCalendarOpen);
    });

    $("#cc-board-body").addEventListener("click", function (e) {
      var btn = e.target.closest && e.target.closest("[data-cc-post-action]");
      if (!btn) return;
      ccHandlePostAction(btn.dataset.ccPostAction, btn.dataset.id, btn);
    });

    ccResetComposer();
  }

  api("/api/admin/auth/me")
    .then(function (d) {
      var a = d.admin || {};
      $("#admin-chip").textContent = "@" + (a.username || a.id);
      bind();
      selectModule("dashboard");
    })
    .catch(function () { /* api() already redirects on 401 */ });
})();
