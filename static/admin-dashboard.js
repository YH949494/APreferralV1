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

  function banner(msg, kind) {
    var el = $("#global-banner");
    if (!msg) { el.innerHTML = ""; return; }
    el.innerHTML = '<div class="banner ' + (kind || "error") + '">' + msg + "</div>";
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

  // ---------- Summary ----------
  function loadSummary(refresh) {
    setMeta("Window: " + state.summaryWindow + " · Loading…");
    ["cards-users", "cards-community", "cards-referrals", "cards-vouchers", "cards-system"].forEach(function (id) {
      skeletonGrid($("#" + id), 4);
    });
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
    $("#" + elId).innerHTML = '<div class="' + kind + '">' + esc(msg) + "</div>";
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
          kpiCard("XP Awarded", d.xp_awarded) +
          kpiCard("Send Failures", d.send_failures) +
          kpiCard("Skipped Subscribed", d.skipped_already_subscribed);
        $("#reactivation-body").innerHTML =
          '<div class="detail-grid">' +
          kvBlock("Safety Limits", [["Daily Send Limit", d.daily_limit], ["Messages Sent Today", d.messages_sent_today], ["Per-Minute Limit", d.minute_limit]]) +
          kvBlock("Campaign", [["Campaign ID", d.campaign_id], ["Status", d.active ? "Active" : "Paused"], ["Updated", dt(d.updated_at)]]) +
          "</div>";
      })
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          $("#cards-reactivation-summary").innerHTML = '<div class="banner error">Failed: ' + esc(e.message) + "</div>";
          statePanel("reactivation-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  function setReactivation(active) {
    var path = active ? "/api/admin/channel-reactivation/start" : "/api/admin/channel-reactivation/pause";
    apiPost(path)
      .then(function () { loadReactivation(true); })
      .catch(function (e) { if (e.message !== "unauthorized") banner("Failed to update campaign: " + e.message); });
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
          card("New Player Override", "100%", "player_age_type=new_player AND first 3 assignments", "good") +
          card("Segments configured", String((d.rows || []).length), "", "neutral");
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

  function loadSettings(refresh) {
    setMeta("Loading…");
    statePanel("settings-body", "loading", "Loading configuration…");
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
      .catch(function (e) {
        if (e.message !== "unauthorized") {
          setMeta("Failed to update");
          statePanel("settings-body", "banner error", "Failed: " + e.message);
        }
      });
  }

  var VIEWS = ["summary", "funnel", "abuse", "vouchers", "referrals", "affiliate", "reactivation", "audit", "segmentProbabilityConfig", "segmentRoi", "segments", "validation", "backendSegmentEngine", "voucherHunterAudit", "unclassifiedAudit", "segmentRuleSimulator", "voucherHunterQuality", "voucherHunterFalsePositive", "voucherHunterRuleSimulator", "vhPriorityImpact", "uploadPlayerPerformance", "uploadHistory", "rawExplorer", "users", "settings"];
  function switchView(view) {
    state.view = view;
    $all(".nav-item").forEach(function (b) { b.classList.toggle("active", b.dataset.view === view); });
    VIEWS.forEach(function (v) { $("#view-" + v).classList.toggle("hidden", v !== view); });
    var titles = {
      summary: "Executive Summary", funnel: "Activation Funnel", abuse: "Abuse Overview",
      vouchers: "Vouchers", referrals: "Referrals", affiliate: "Affiliate", reactivation: "Reactivation",
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
    else if (state.view === "reactivation") loadReactivation(force);
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
    $("#reactivation-start-btn").addEventListener("click", function () { setReactivation(true); });
    $("#reactivation-pause-btn").addEventListener("click", function () { setReactivation(false); });
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

  api("/api/admin/auth/me")
    .then(function (d) {
      var a = d.admin || {};
      $("#admin-chip").textContent = "@" + (a.username || a.id);
      bind();
      switchView("summary");
    })
    .catch(function () { /* api() already redirects on 401 */ });
})();
