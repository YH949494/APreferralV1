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
        // Telegram count card with stale indicator. `extraClass` lets the
        // official channel use the headline style while the chatroom stays
        // a secondary-highlight card.
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
          kpiCard("Active 30d", u.active_30d) +
          kpiCard("Active 7d", u.active_7d) +
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

  // ---------- Shared helpers for Phase C panels ----------
  function esc(s) {
    return String(s === null || s === undefined ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }
  function dt(v) { return v ? new Date(v).toLocaleString() : "—"; }
  function pct(v) { return (v === null || v === undefined) ? "—" : v + "%"; }

  // Render a {value, data_quality, note} metric as a KPI card.
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
    $("#" + elId).innerHTML = '<div class="' + kind + '">' + esc(msg) + "</div>";
  }

  // Build an expandable table. rows: [{cells:[], detailHtml, search}]
  function expandTable(headers, rows) {
    if (!rows.length) return '<div class="empty">No records.</div>';
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

  // Wire row expand/collapse + lazy detail loading inside a container.
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

  // ---------- Vouchers ----------
  function loadVouchers(refresh) {
    skeletonGrid($("#cards-voucher-summary"), 6);
    statePanel("vouchers-body", "loading", "Loading campaigns…");
    api("/api/admin/dashboard/vouchers" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
        var s = d.summary;
        $("#cards-voucher-summary").innerHTML =
          dqCard("Active Campaigns", s.active_campaigns) + dqCard("Upcoming", s.upcoming_campaigns) +
          dqCard("Ended", s.ended_campaigns) + dqCard("Total Codes", s.total_codes) +
          dqCard("Claimed Codes", s.claimed_codes) + dqCard("Remaining Codes", s.remaining_codes) +
          dqCard("Claim Rate", s.claim_rate_pct.value === null ? s.claim_rate_pct : { value: s.claim_rate_pct.value + "%", data_quality: s.claim_rate_pct.data_quality }) +
          dqCard("Failed Claims 7d", s.failed_claims_7d) + dqCard("Repeat Claimers 7d", s.repeat_claimers_7d) +
          dqCard("Welcome Claims 7d", s.welcome_claims_7d);

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
      .catch(function (e) { if (e.message !== "unauthorized") statePanel("vouchers-body", "banner error", "Failed: " + e.message); });
  }

  // ---------- Referrals ----------
  function loadReferrals(refresh) {
    skeletonGrid($("#cards-referrals-summary"), 7);
    statePanel("referrals-body", "loading", "Loading referrers…");
    api("/api/admin/dashboard/referrals" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
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
      .catch(function (e) { if (e.message !== "unauthorized") statePanel("referrals-body", "banner error", "Failed: " + e.message); });
  }

  // ---------- Affiliate ----------
  function loadAffiliate(refresh) {
    skeletonGrid($("#cards-affiliate-summary"), 4);
    statePanel("affiliate-body", "loading", "Loading affiliates…");
    $("#affiliate-pools").innerHTML = "";
    api("/api/admin/dashboard/affiliate" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
        var s = d.summary;
        $("#cards-affiliate-summary").innerHTML =
          dqCard("Pending Review", s.pending_review) + dqCard("Approved", s.approved) +
          dqCard("Issued", s.issued) + dqCard("Rejected", s.rejected);

        var pools = (d.pool_availability || []).map(function (p) {
          return "<tr><td>" + esc(p.pool_id) + '</td><td class="num">' + fmt(p.available) + '</td><td class="num">' + fmt(p.issued) + "</td></tr>";
        }).join("");
        var mi = ((d.monthly_issuance || {}).by_status || []).map(function (m) {
          return "<tr><td>" + esc(m.status) + '</td><td class="num">' + fmt(m.count) + "</td></tr>";
        }).join("") || '<tr><td colspan="2">No issuance this month.</td></tr>';
        $("#affiliate-pools").innerHTML = '<div class="detail-grid">' +
          '<div class="detail-block"><h4>Pool Availability</h4><table class="mini-table"><thead><tr><th>Pool</th><th class="num">Available</th><th class="num">Issued</th></tr></thead><tbody>' + pools + "</tbody></table></div>" +
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
      .catch(function (e) { if (e.message !== "unauthorized") statePanel("affiliate-body", "banner error", "Failed: " + e.message); });
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

  // ---------- Audit ----------
  function loadAudit(refresh) {
    skeletonGrid($("#cards-audit-summary"), 6);
    statePanel("audit-body", "loading", "Loading audit trail…");
    api("/api/admin/dashboard/audit" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
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
      .catch(function (e) { if (e.message !== "unauthorized") statePanel("audit-body", "banner error", "Failed: " + e.message); });
  }

  // ---------- User Drilldown ----------
  function loadUser(query) {
    if (!query) { statePanel("user-body", "empty", "Search by Telegram user_id or username to view a user profile."); return; }
    statePanel("user-body", "loading", "Searching…");
    api("/api/admin/dashboard/user?query=" + encodeURIComponent(query))
      .then(function (d) {
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
      .catch(function (e) { if (e.message !== "unauthorized") statePanel("user-body", "banner error", "Failed: " + e.message); });
  }

  // ---------- Settings ----------
  function loadSettings(refresh) {
    statePanel("settings-body", "loading", "Loading configuration…");
    api("/api/admin/dashboard/settings" + (refresh ? "?refresh=1" : ""))
      .then(function (d) {
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
        var titles = {
          voucher_settings: "Voucher Settings", referral_settings: "Referral Settings",
          affiliate_settings: "Affiliate Settings", xp_checkin_settings: "XP & Check-in",
          bot_settings: "Bot Settings", security: "Security"
        };
        $("#settings-body").innerHTML = Object.keys(sec).map(function (k) {
          return '<div class="settings-section"><div class="section-title">' + esc(titles[k] || k) +
            "</div><div class=\"detail-block\">" + render(sec[k]) + "</div></div>";
        }).join("");
      })
      .catch(function (e) { if (e.message !== "unauthorized") statePanel("settings-body", "banner error", "Failed: " + e.message); });
  }

  // ---------- View switching ----------
  var VIEWS = ["summary", "funnel", "abuse", "vouchers", "referrals", "affiliate", "audit", "users", "settings"];
  function switchView(view) {
    state.view = view;
    $all(".nav-item").forEach(function (b) { b.classList.toggle("active", b.dataset.view === view); });
    VIEWS.forEach(function (v) { $("#view-" + v).classList.toggle("hidden", v !== view); });
    var titles = {
      summary: "Executive Summary", funnel: "Activation Funnel", abuse: "Abuse Overview",
      vouchers: "Vouchers", referrals: "Referrals", affiliate: "Affiliate", audit: "Audit",
      users: "User Drilldown", settings: "Settings (Read Only)"
    };
    $("#view-title").textContent = titles[view] || view;
    banner(null);
    refreshCurrent(false);
  }

  function refreshCurrent(force) {
    if (state.view === "summary") loadSummary(force);
    else if (state.view === "funnel") loadFunnel(force);
    else if (state.view === "abuse") loadAbuse(force);
    else if (state.view === "vouchers") loadVouchers(force);
    else if (state.view === "referrals") loadReferrals(force);
    else if (state.view === "affiliate") loadAffiliate(force);
    else if (state.view === "audit") loadAudit(force);
    else if (state.view === "users") { /* user view loads on search */ }
    else if (state.view === "settings") loadSettings(force);
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

    // Phase C: client-side table filters.
    [["vouchers-filter", "vouchers-body"], ["referrals-filter", "referrals-body"],
     ["affiliate-filter", "affiliate-body"], ["audit-filter", "audit-body"]].forEach(function (pair) {
      var input = $("#" + pair[0]);
      if (input) input.addEventListener("input", function () { applyFilter(pair[1], input.value); });
    });

    // User drilldown search.
    var us = $("#user-search"), ub = $("#user-search-btn");
    function doUserSearch() { loadUser((us.value || "").trim()); }
    if (ub) ub.addEventListener("click", doUserSearch);
    if (us) us.addEventListener("keydown", function (e) { if (e.key === "Enter") doUserSearch(); });
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
