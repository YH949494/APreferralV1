/* APReferral Admin Dashboard — browser-only, session-cookie auth.
   No Telegram SDK. Reuses existing /api/admin/* endpoints. Read-only. */
(function () {
  "use strict";

  var state = { view: "summary", funnelWindow: "7d" };

  function $(sel, root) { return (root || document).querySelector(sel); }
  function $all(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function fmt(v) {
    if (v === null || v === undefined) return "—";
    if (typeof v === "number") return v.toLocaleString();
    return String(v);
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

  function banner(msg, kind) {
    var el = $("#global-banner");
    if (!msg) { el.innerHTML = ""; return; }
    el.innerHTML = '<div class="banner ' + (kind || "error") + '">' + msg + "</div>";
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

  // ---------- Summary ----------
  function loadSummary(refresh) {
    ["cards-users", "cards-community", "cards-referrals", "cards-vouchers", "cards-system"].forEach(function (id) {
      skeletonGrid($("#" + id), 4);
    });
    api("/api/admin/dashboard/summary" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
        banner(d.partial_errors ? ("Some metrics failed to load: " + d.partial_errors.join("; ")) : null, "warn");

        var u = d.users;
        // Community Followers card — headline KPI with stale indicator.
        var followersCard = (function() {
          var val = u.community_followers;
          var missing = val === null || val === undefined;
          var staleHtml = u.community_followers_stale
            ? ' <span class="tag heuristic" title="Last updated: ' + (u.community_followers_cached_at ? new Date(u.community_followers_cached_at).toLocaleString() : "unknown") + '">stale</span>'
            : '';
          var sub = u.community_followers_stale
            ? "Telegram API unavailable — last known value"
            : (u.community_followers_cached_at ? ("cached " + new Date(u.community_followers_cached_at).toLocaleTimeString()) : "");
          return '<div class="kpi kpi-headline"><div class="label">COMMUNITY FOLLOWERS' + staleHtml + '</div>' +
            '<div class="' + (missing ? "value missing" : "value") + '">' + (missing ? "Unavailable" : fmt(val)) + '</div>' +
            '<div class="sub">' + sub + '</div></div>';
        })();
        $("#cards-users").innerHTML =
          followersCard +
          kpiCard("Registered Users", u.registered) +
          kpiCard("Active 7d", u.active_7d) +
          kpiCard("Active 30d", u.active_30d) +
          kpiCard("Active Today", u.active_today);

        $("#cards-community").innerHTML =
          kpiCard("Check-ins Today", d.community.checkins_today) +
          kpiCard("Check-ins 7d", d.community.checkins_7d) +
          kpiCard("Welcome Eligible", d.welcome.eligible) +
          kpiCard("Welcome Claimed", d.welcome.claimed,
            d.welcome.conversion_pct !== null ? (d.welcome.conversion_pct + "% conversion") : "");

        $("#cards-referrals").innerHTML =
          kpiCard("Pending Referrals", d.referrals.pending) +
          kpiCard("Qualified (total)", d.referrals.qualified_total, fmt(d.referrals.qualified_7d) + " in 7d") +
          kpiCard("Revoked Referrals", d.referrals.revoked);

        $("#cards-vouchers").innerHTML =
          kpiCard("Active Campaigns", d.vouchers.active_campaigns) +
          kpiCard("Claims Today", d.vouchers.claims_today) +
          kpiCard("Remaining Codes", d.vouchers.remaining_codes) +
          kpiCard("Affiliate Pending", d.affiliate.pending_review) +
          kpiCard("Affiliate Approved (mo)", d.affiliate.approved_this_month);

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
      .catch(function (e) { if (e.message !== "unauthorized") banner("Failed to load summary: " + e.message); });
  }

  // ---------- Funnel ----------
  function loadFunnel(refresh) {
    var body = $("#funnel-body");
    body.innerHTML = '<div class="loading">Loading funnel…</div>';
    api("/api/admin/dashboard/funnel?window=" + encodeURIComponent(state.funnelWindow) + (refresh ? "&refresh=1" : ""))
      .then(function (d) {
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
          '<div class="note">Conversion is relative to the first stage with data. Window: ' + d.window + ".</div>";
      })
      .catch(function (e) { if (e.message !== "unauthorized") body.innerHTML = '<div class="banner error">Failed: ' + e.message + "</div>"; });
  }

  // ---------- Abuse ----------
  function loadAbuse(refresh) {
    var grid = $("#cards-abuse");
    skeletonGrid(grid, 5);
    $("#abuse-notes").innerHTML = "";
    api("/api/admin/dashboard/abuse" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
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
        if (d.partial_errors) $("#abuse-notes").innerHTML = '<div class="banner warn">Partial errors: ' + d.partial_errors.join("; ") + "</div>";
      })
      .catch(function (e) { if (e.message !== "unauthorized") banner("Failed to load abuse data: " + e.message); });
  }

  // ---------- View switching ----------
  function switchView(view) {
    state.view = view;
    $all(".nav-item").forEach(function (b) { b.classList.toggle("active", b.dataset.view === view); });
    ["summary", "funnel", "abuse"].forEach(function (v) {
      $("#view-" + v).classList.toggle("hidden", v !== view);
    });
    var titles = { summary: "Executive Summary", funnel: "Activation Funnel", abuse: "Abuse Overview" };
    $("#view-title").textContent = titles[view] || view;
    banner(null);
    refreshCurrent(false);
  }

  function refreshCurrent(force) {
    if (state.view === "summary") loadSummary(force);
    else if (state.view === "funnel") loadFunnel(force);
    else if (state.view === "abuse") loadAbuse(force);
  }

  function bind() {
    $all(".nav-item[data-view]").forEach(function (b) {
      b.addEventListener("click", function () { switchView(b.dataset.view); });
    });
    $("#refresh-btn").addEventListener("click", function () { refreshCurrent(true); });
    $("#logout-btn").addEventListener("click", function () {
      fetch("/api/admin/auth/logout", { method: "POST", credentials: "same-origin" })
        .finally(function () { window.location.href = "/admin"; });
    });
    $all("#funnel-window button").forEach(function (b) {
      b.addEventListener("click", function () {
        state.funnelWindow = b.dataset.window;
        $all("#funnel-window button").forEach(function (x) { x.classList.toggle("active", x === b); });
        loadFunnel(false);
      });
    });
  }

  // ---------- Boot ----------
  api("/api/admin/auth/me")
    .then(function (d) {
      var a = d.admin || {};
      $("#admin-chip").textContent = "@" + (a.username || a.id);
      bind();
      switchView("summary");
    })
    .catch(function () { /* api() already redirects on 401 */ });
})();
