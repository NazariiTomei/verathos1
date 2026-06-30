from __future__ import annotations


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Miner Control Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #eef4f1;
      --panel: #ffffff;
      --panel-2: #f7fafc;
      --text: #172033;
      --muted: #607086;
      --line: #d9e3ea;
      --blue: #2563eb;
      --cyan: #0891b2;
      --teal: #0f766e;
      --green: #16875a;
      --amber: #b45309;
      --gold: #ca8a04;
      --red: #dc2626;
      --rose: #be123c;
      --violet: #7c3aed;
      --ink: #101828;
      --code-bg: #111827;
      --code-fg: #d1d5db;
      --shadow: 0 12px 34px rgba(16, 24, 40, 0.08);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      background:
        linear-gradient(135deg, #e8f5f2 0, #f8f3e6 42%, #eef1fb 100%);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      letter-spacing: 0;
    }

    button, select {
      font: inherit;
    }

    .shell {
      width: min(1500px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 18px 0 28px;
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 20px;
      margin-bottom: 14px;
      background: linear-gradient(135deg, #102033 0, #0e4a54 50%, #5f4320 100%);
      border: 1px solid rgba(255, 255, 255, 0.32);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }

    h1 {
      margin: 0;
      font-size: 26px;
      line-height: 1.15;
      font-weight: 720;
      color: #ffffff;
    }

    h2 {
      margin: 0;
      font-size: 16px;
      line-height: 1.25;
      font-weight: 700;
      color: var(--ink);
    }

    .subtle {
      color: var(--muted);
      font-size: 13px;
    }

    header .subtle {
      color: #dbeafe;
    }

    .stack {
      display: flex;
      flex-direction: column;
      gap: 3px;
      min-width: 0;
    }

    .mini {
      color: var(--muted);
      font-size: 11px;
      line-height: 1.25;
    }

    .toolbar {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    .button {
      min-height: 36px;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--text);
      border-radius: 8px;
      padding: 0 12px;
      cursor: pointer;
    }

    .button:hover {
      border-color: #b8c4d6;
      background: #f9fbfd;
    }

    header .button {
      border-color: rgba(255, 255, 255, 0.55);
      background: rgba(255, 255, 255, 0.14);
      color: #ffffff;
    }

    header .button:hover {
      border-color: rgba(255, 255, 255, 0.82);
      background: rgba(255, 255, 255, 0.22);
    }

    .grid-metrics {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }

    .metric {
      min-height: 86px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      box-shadow: var(--shadow);
      position: relative;
      overflow: hidden;
    }

    .metric::before {
      content: "";
      position: absolute;
      inset: 0 0 auto;
      height: 4px;
      background: var(--blue);
    }

    .metric:nth-child(2)::before { background: var(--teal); }
    .metric:nth-child(3)::before { background: var(--violet); }
    .metric:nth-child(4)::before { background: var(--gold); }
    .metric:nth-child(5)::before { background: var(--rose); }
    .metric:nth-child(6)::before { background: var(--cyan); }
    .metric:nth-child(7)::before { background: var(--green); }

    .metric.good .metric-value { color: var(--green); }
    .metric.warn .metric-value { color: var(--amber); }
    .metric.bad .metric-value { color: var(--red); }
    .metric.info .metric-value { color: var(--blue); }
    .metric.violet .metric-value { color: var(--violet); }
    .metric.cyan .metric-value { color: var(--cyan); }

    .metric-label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      font-weight: 700;
    }

    .metric-value {
      font-size: 28px;
      line-height: 1;
      font-weight: 760;
      color: var(--ink);
    }

    .section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      margin-top: 14px;
      overflow: hidden;
      box-shadow: var(--shadow);
    }

    .section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(90deg, #fbfcfe 0, #eef7f5 100%);
    }

    .section-head h2 {
      display: flex;
      align-items: center;
      gap: 8px;
      padding-left: 10px;
      border-left: 4px solid var(--teal);
    }

    .table-wrap {
      overflow-x: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1180px;
    }

    th, td {
      padding: 10px 12px;
      text-align: left;
      vertical-align: middle;
      border-bottom: 1px solid var(--line);
      white-space: nowrap;
    }

    th {
      color: var(--muted);
      background: #f8fafc;
      font-size: 12px;
      text-transform: uppercase;
      font-weight: 760;
    }

    tr:last-child td {
      border-bottom: 0;
    }

    tbody tr:hover {
      background: #f8fbff;
    }

    tbody tr.row-ok { box-shadow: inset 3px 0 0 var(--green); }
    tbody tr.row-warn { box-shadow: inset 3px 0 0 var(--amber); }
    tbody tr.row-bad { box-shadow: inset 3px 0 0 var(--red); }

    .mono {
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      border-radius: 999px;
      padding: 0 9px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid transparent;
    }

    .ok {
      color: #0f5132;
      background: #dcfce7;
      border-color: #bbf7d0;
    }

    .busy {
      color: #1e40af;
      background: #dbeafe;
      border-color: #bfdbfe;
    }

    .warn {
      color: #7c2d12;
      background: #ffedd5;
      border-color: #fed7aa;
    }

    .bad {
      color: #7f1d1d;
      background: #fee2e2;
      border-color: #fecaca;
    }

    .score {
      display: inline-grid;
      place-items: center;
      width: 42px;
      height: 28px;
      border-radius: 8px;
      font-weight: 760;
      background: #eef2ff;
      color: var(--ink);
    }

    .score.high { color: var(--green); }
    .score.mid { color: var(--amber); }
    .score.low { color: var(--red); }

    .bar {
      width: 116px;
      height: 8px;
      border-radius: 999px;
      background: #e5e7eb;
      overflow: hidden;
    }

    .bar > span {
      display: block;
      height: 100%;
      background: linear-gradient(90deg, var(--cyan), var(--blue));
    }

    .bar.proof > span {
      background: linear-gradient(90deg, var(--green), var(--teal));
    }

    .bar.warn > span {
      background: linear-gradient(90deg, var(--gold), var(--amber));
    }

    .bar.bad > span {
      background: linear-gradient(90deg, var(--rose), var(--red));
    }

    .proof-head {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .epoch-row {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      margin-top: 4px;
      max-width: 260px;
    }

    .epoch-chip {
      display: inline-flex;
      align-items: center;
      min-height: 20px;
      border-radius: 999px;
      padding: 0 7px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid transparent;
    }

    .wrap {
      white-space: normal;
      min-width: 220px;
      max-width: 420px;
    }

    .tight {
      min-width: 980px;
    }

    .log-controls {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }

    select {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      color: var(--text);
      padding: 0 10px;
      min-width: 220px;
    }

    pre {
      margin: 0;
      padding: 14px;
      min-height: 320px;
      max-height: 520px;
      overflow: auto;
      background: var(--code-bg);
      color: var(--code-fg);
      font: 12px/1.55 "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }

    .empty {
      padding: 22px 14px;
      color: var(--muted);
    }

    @media (max-width: 1280px) {
      .grid-metrics {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }
    }

    @media (max-width: 900px) {
      .shell {
        width: min(100vw - 20px, 1500px);
        padding-top: 10px;
      }

      header {
        align-items: flex-start;
        flex-direction: column;
      }

      .toolbar {
        justify-content: flex-start;
      }

      .grid-metrics {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 560px) {
      .grid-metrics {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <h1>Miner Control Dashboard</h1>
        <div class="subtle" id="updated">Waiting for router state</div>
      </div>
      <div class="toolbar">
        <button class="button" id="refresh">Refresh</button>
        <span class="pill" id="router-status">loading</span>
      </div>
    </header>

    <section class="grid-metrics" id="metrics"></section>

    <section class="section">
      <div class="section-head">
        <h2>Leases</h2>
        <span class="subtle" id="lease-summary"></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Watcher</th>
              <th>Slot</th>
              <th>Model Index</th>
              <th>Remaining</th>
              <th>Next Renew</th>
              <th>Expires</th>
              <th>Endpoint</th>
            </tr>
          </thead>
          <tbody id="leases"></tbody>
        </table>
      </div>
    </section>

	    <section class="section">
	      <div class="section-head">
	        <h2>Receipt Integrity</h2>
	        <span class="subtle" id="receipt-summary"></span>
      </div>
      <div class="table-wrap">
        <table class="tight">
          <thead>
            <tr>
              <th>Slot</th>
              <th>Status</th>
              <th>Epoch</th>
              <th>Receipts</th>
              <th>Validators</th>
              <th>Latest Proof</th>
              <th>Duplicates</th>
              <th>Model Index</th>
              <th>Last Receipt</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody id="receipts"></tbody>
        </table>
	      </div>
	    </section>
	
	    <section class="section">
	      <div class="section-head">
	        <h2>Supervisor</h2>
	        <span class="subtle" id="supervisor-summary"></span>
	      </div>
	      <div class="table-wrap">
	        <table class="tight">
	          <thead>
	            <tr>
	              <th>Process</th>
	              <th>Kind</th>
	              <th>Status</th>
	              <th>PID</th>
	              <th>Restart</th>
	              <th>Detail</th>
	            </tr>
	          </thead>
	          <tbody id="supervisor"></tbody>
	        </table>
	      </div>
	    </section>
	
	    <section class="section">
	      <div class="section-head">
	        <h2>Miners</h2>
        <span class="subtle" id="miner-summary"></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Miner</th>
              <th>Hotkey</th>
              <th>Coldkey</th>
              <th>Status</th>
              <th>Chain</th>
	              <th>Verathos Score</th>
	              <th>Slots</th>
	              <th>Process</th>
	              <th>Success / Fail</th>
              <th>Ports</th>
              <th>Organic</th>
            </tr>
          </thead>
          <tbody id="miners"></tbody>
        </table>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <h2>Slots</h2>
        <span class="subtle" id="slot-summary"></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Port</th>
              <th>Miner</th>
	              <th>Slot</th>
	              <th>Status</th>
	              <th>Process</th>
	              <th>Success / Fail</th>
	              <th>Total</th>
              <th>Verathos</th>
              <th>Last Seen</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody id="slots"></tbody>
        </table>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <h2>GPUs</h2>
        <span class="subtle" id="gpu-summary"></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>GPU</th>
              <th>Status</th>
              <th>Score</th>
              <th>Jobs</th>
              <th>Model</th>
              <th>VRAM</th>
              <th>Util</th>
              <th>Temp</th>
              <th>Router S/F</th>
              <th>Worker S/F</th>
            </tr>
          </thead>
          <tbody id="gpus"></tbody>
        </table>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <h2>Logs</h2>
        <div class="log-controls">
          <select id="log-file"></select>
          <button class="button" id="load-log">Load</button>
        </div>
      </div>
      <pre id="logs">No log loaded.</pre>
    </section>
  </main>

  <script>
    const state = {
      selectedLog: "router.log",
      timer: null,
      clockTimer: null,
      data: null
    };

    const el = (id) => document.getElementById(id);

    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char]));
    }

	    function statusClass(status) {
	      if (status === "ok" || status === "active" || status === "running") return "ok";
	      if (status === "busy") return "busy";
	      if (status === "degraded" || status === "registered" || status === "unknown" || status === "warn" || status === "stale" || status === "due" || status === "external" || status === "restarting" || status === "waiting") return "warn";
	      return "bad";
	    }

    function scoreClass(score) {
      const value = Number(score);
      if (!Number.isFinite(value)) return "low";
      if (value >= 5) return "high";
      if (value >= 1) return "mid";
      return "low";
    }

    function formatScore(score) {
      const value = Number(score);
      if (!Number.isFinite(value)) return "unlisted";
      return value.toFixed(value >= 10 ? 1 : 3).replace(/0+$/, "").replace(/\.$/, "");
    }

    function fmtInt(value) {
      const number = Number(value || 0);
      return Number.isFinite(number) ? number.toLocaleString() : "0";
    }

    function fmtGb(mb) {
      const number = Number(mb || 0);
      return Number.isFinite(number) ? `${(number / 1024).toFixed(1)} GB` : "0 GB";
    }

    function fmtGbMaybe(mb) {
      if (mb === null || mb === undefined || mb === "") return "not reported";
      const number = Number(mb);
      return Number.isFinite(number) ? `${(number / 1024).toFixed(1)} GB` : "not reported";
    }

    function fmtPercentMaybe(value) {
      if (value === null || value === undefined || value === "") return "not reported";
      const number = Number(value);
      return Number.isFinite(number) ? `${number.toFixed(number % 1 ? 1 : 0)}%` : "not reported";
    }

    function fmtTempMaybe(value) {
      if (value === null || value === undefined || value === "") return "not reported";
      const number = Number(value);
      return Number.isFinite(number) ? `${number.toFixed(number % 1 ? 1 : 0)} C` : "not reported";
    }

    function shortId(value, size = 10) {
      const text = String(value ?? "");
      if (text.length <= size + 2) return text;
      return `${text.slice(0, size)}...`;
    }

    function asArray(value) {
      return Array.isArray(value) ? value : [];
    }

    function fmtDuration(seconds, showSeconds = false) {
      const value = Number(seconds);
      if (!Number.isFinite(value)) return "";
      const sign = value < 0 ? "-" : "";
      let remaining = Math.abs(Math.floor(value));
      const days = Math.floor(remaining / 86400);
      remaining %= 86400;
      const hours = Math.floor(remaining / 3600);
      remaining %= 3600;
      const minutes = Math.floor(remaining / 60);
      const secondsPart = remaining % 60;
      if (days) return `${sign}${days}d ${hours}h`;
      if (hours) return `${sign}${hours}h ${minutes}m`;
      if (showSeconds && minutes) return `${sign}${minutes}m ${secondsPart}s`;
      if (showSeconds) return `${sign}${secondsPart}s`;
      return `${sign}${minutes}m`;
    }

    function secondsUntilIso(value) {
      const timestamp = Date.parse(value || "");
      if (!Number.isFinite(timestamp)) return null;
      return Math.max(0, (timestamp - Date.now()) / 1000);
    }

    function metric(label, value, note, tone = "") {
      return `
        <div class="metric ${esc(tone)}">
          <div class="metric-label">${esc(label)}</div>
          <div class="metric-value">${esc(value)}</div>
          <div class="subtle">${esc(note)}</div>
        </div>
      `;
    }

    function renderMetrics(data) {
      const h = data.health || {};
	      const v = data.verathos || {};
	      const network = v.network || {};
	      const supervisor = data.supervisor || {};
	      const supervisorSummary = supervisor.summary || {};
	      const routerTopology = data.topology || {};
	      const supervisorTopology = supervisor.topology || {};
	      const lease = data.lease_watcher || {};
	      const receipt = data.receipt_integrity || {};
      const miners = asArray(data.miners);
      const epoch = data.epoch || {};
      const latestProofRequests = Number(receipt.latest_proof_requests || 0);
      const latestProofPasses = Number(receipt.latest_proof_passes ?? Math.max(0, latestProofRequests - Number(receipt.latest_proof_failures || 0)));
      const latestProofFailures = Number(receipt.latest_proof_failures || 0);
      const historicalProofFailures = Number(receipt.historical_proof_failures || 0);
      const epochRemaining = secondsUntilIso(epoch.estimated_next_epoch_at) ?? epoch.remaining_seconds;
      const epochNote = epoch.status === "ok"
        ? `to epoch ${epoch.next_epoch_number ?? "?"} at block ${epoch.next_epoch_block ?? "?"}`
        : (epoch.error || "chain time unavailable");
      const epochValue = epoch.status === "ok"
        ? fmtDuration(epochRemaining, true)
        : "unavailable";
      el("metrics").innerHTML = [
	        metric("Miners", miners.length, `${miners.filter((m) => m.status === "ok").length} ok`, "info"),
	        metric("Slots", `${h.healthy_slots || 0}/${h.total_slots || 0}`, "healthy / total", h.healthy_slots === h.total_slots ? "good" : "warn"),
	        metric("Vast GPUs", `${h.healthy_gpus || 0}/${h.total_gpus || 0}`, `queue ${h.dispatch_queue_length || 0}`, h.healthy_gpus === h.total_gpus ? "violet" : "warn"),
	        metric("Supervisor", supervisor.status || "missing", `${supervisorSummary.running || 0} running, ${supervisorSummary.external || 0} external, ${supervisorSummary.restarting || 0} restarting`, supervisor.status === "ok" ? "good" : "warn"),
	        metric("Topology", `R${routerTopology.reload_count || 0} / S${supervisorTopology.reload_count || 0}`, routerTopology.last_error || supervisorTopology.last_error || "hot reload ready", (routerTopology.last_error || supervisorTopology.last_error) ? "warn" : "cyan"),
	        metric("Lease Guard", lease.status || "missing", lease.next_renew_at_iso ? `renew ${lease.next_renew_at_iso}` : (lease.last_error || "waiting"), lease.status === "ok" ? "good" : "warn"),
        metric("Proof Latest", latestProofRequests ? `${latestProofPasses}/${latestProofRequests}` : "none", `${latestProofFailures} current fail, ${historicalProofFailures} older fail`, latestProofFailures ? "bad" : (historicalProofFailures ? "warn" : "good")),
        metric("Receipts", receipt.status || "missing", `${receipt.ok_slots ?? 0}/${receipt.slot_count ?? 0} ok, ${fmtInt(receipt.total_receipts)} stored`, receipt.status === "ok" ? "good" : "warn"),
        metric("Verathos", v.epoch_number ? `Epoch ${v.epoch_number}` : "unavailable", v.error || `${network.healthy_miners ?? "?"}/${network.total_miners ?? "?"} miners`, v.error ? "bad" : "cyan"),
        metric("Next Epoch", epochValue, epochNote, epoch.status === "ok" ? "cyan" : "warn")
      ].join("");
    }

    function renderLeases(watcher) {
      const slots = asArray(watcher?.slots);
      const status = watcher?.status || "missing";
      const running = watcher?.running ? "running" : "stopped";
      const next = watcher?.next_renew_at_iso ? `next renew ${watcher.next_renew_at_iso}` : "next renew unknown";
      const pid = watcher?.pid ? `pid ${watcher.pid}${watcher.pid_alive === false ? " dead" : ""}` : "no pid";
      const txs = asArray(watcher?.last_transactions);
      el("lease-summary").textContent = `${status} - ${running} - ${pid} - ${next}`;
      if (!slots.length) {
        el("leases").innerHTML = `
          <tr>
            <td><span class="pill ${statusClass(status)}">${esc(status)}</span></td>
            <td colspan="6">${esc(watcher?.last_error || "No lease rows yet")}</td>
          </tr>
        `;
        return;
      }
      el("leases").innerHTML = slots.map((slot, index) => {
        const tx = txs.find((item) => item.slot_id === slot.slot_id);
        const rowStatus = slot.due ? "due" : (slot.active ? "active" : (slot.registered ? "inactive" : "missing"));
        return `
          <tr>
            <td>
              ${index === 0 ? `
                <div class="stack">
                  <span><span class="pill ${statusClass(status)}">${esc(status)}</span></span>
                  <span class="mini">${esc(running)} - every ${esc(fmtDuration(watcher.renew_interval_seconds))}</span>
                  <span class="mini">${esc(pid)}${watcher.stale ? " - stale" : ""}</span>
                  ${watcher.last_error ? `<span class="mini">${esc(watcher.last_error)}</span>` : ""}
                </div>
              ` : ""}
            </td>
            <td>
              <div class="stack">
                <span class="mono">${esc(slot.slot_index)} / ${esc((slot.slot_id || "").slice(0, 8))}</span>
                <span class="mini"><span class="pill ${slot.due ? "warn" : (slot.active ? "ok" : "bad")}">${esc(rowStatus)}</span></span>
              </div>
            </td>
            <td class="mono">${slot.model_index === null || slot.model_index === undefined ? "" : esc(slot.model_index)}</td>
            <td>${esc(fmtDuration(slot.remaining_seconds))}</td>
            <td>
              <div class="stack">
                <span class="mono">${esc(slot.next_renew_at_iso || "")}</span>
                ${tx ? `<span class="mini mono">${esc(tx.tx_hash.slice(0, 18))}</span>` : ""}
              </div>
            </td>
            <td class="mono">${esc(slot.expires_at_iso || "")}</td>
            <td class="mono">${esc(slot.endpoint || "")}</td>
          </tr>
        `;
      }).join("");
    }

	    function renderReceipts(integrity) {
	      const slots = asArray(integrity?.slots);
	      const status = integrity?.status || "missing";
      const warningText = asArray(integrity?.warnings).join(" | ");
      const latestProofRequests = Number(integrity?.latest_proof_requests || 0);
      const latestProofPasses = Number(integrity?.latest_proof_passes ?? Math.max(0, latestProofRequests - Number(integrity?.latest_proof_failures || 0)));
      const latestProofFailures = Number(integrity?.latest_proof_failures || 0);
      const historicalProofFailures = Number(integrity?.historical_proof_failures || 0);
      const failedEpochs = asArray(integrity?.proof_failures_by_epoch)
        .map((item) => `e${item.epoch}:${item.proof_failures}`)
        .join(", ");
      el("receipt-summary").textContent = `${status} - latest proof ${latestProofPasses}/${latestProofRequests} pass - older failed proofs ${historicalProofFailures}${failedEpochs ? ` (${failedEpochs})` : ""} - ${fmtInt(integrity?.total_receipts)} receipts${warningText ? ` - ${warningText}` : ""}`;
      if (!slots.length) {
        el("receipts").innerHTML = `
          <tr>
            <td colspan="10">${esc(integrity?.watcher_issue || integrity?.last_error || "No receipt audit rows yet")}</td>
          </tr>
        `;
        return;
      }
      el("receipts").innerHTML = slots.map((slot) => {
        const validatorCounts = asArray(slot.latest_validator_counts)
          .map((item) => `${shortId(item.validator, 8)}:${item.count}`)
          .join(", ");
        const duplicateTotal = Number(slot.latest_duplicate_signatures || 0) + Number(slot.latest_cross_slot_duplicate_signatures || 0);
        const latestProofRequests = Number(slot.latest_proof_requests || 0);
        const latestProofFailures = Number(slot.latest_proof_failures || 0);
        const latestProofPasses = Number(slot.latest_proof_passes ?? Math.max(0, latestProofRequests - latestProofFailures));
        const historicalProofFailures = Number(slot.historical_proof_failures || Math.max(0, Number(slot.proof_failures || 0) - latestProofFailures));
        const proofPct = latestProofRequests
          ? Math.round(latestProofPasses / latestProofRequests * 100)
          : 100;
        const failedEpochChips = asArray(slot.epoch_proofs)
          .filter((epoch) => Number(epoch.proof_failures || 0) > 0)
          .slice(0, 4)
          .map((epoch) => `<span class="epoch-chip bad">e${esc(epoch.epoch)}: ${esc(epoch.proof_failures)} fail</span>`)
          .join("");
        const proofClass = latestProofFailures ? "bad" : (historicalProofFailures ? "warn" : "ok");
        const notes = [
          slot.last_error,
          slot.stale ? "stale receipts" : "",
          slot.latest_wrong_model_index ? `${slot.latest_wrong_model_index} wrong model-index` : "",
          duplicateTotal ? `${duplicateTotal} duplicate signature(s)` : "",
          latestProofFailures ? `${latestProofFailures} current proof failure(s)` : "",
          historicalProofFailures ? `${historicalProofFailures} older proof failure(s)` : ""
        ].filter(Boolean).join("; ");
        return `
          <tr class="row-${statusClass(slot.status)}">
            <td>
              <div class="stack">
                <span class="mono">${esc(slot.slot_index)} / ${esc(shortId(slot.slot_id, 8))}</span>
                <span class="mini mono">${esc(slot.endpoint || "")}</span>
              </div>
            </td>
            <td><span class="pill ${statusClass(slot.status)}">${esc(slot.status || "unknown")}</span></td>
            <td class="mono">${esc(slot.latest_epoch ?? "")}</td>
            <td>
              <div class="stack">
                <span>${esc(fmtInt(slot.latest_epoch_receipts))} latest</span>
                <span class="mini">${esc(fmtInt(slot.total_receipts))} stored</span>
              </div>
            </td>
            <td>
              <div class="stack">
                <span>${esc(slot.latest_validator_count || 0)} validators</span>
                <span class="mini mono">${esc(validatorCounts)}</span>
              </div>
            </td>
            <td>
              <div class="proof-head">
                <span class="pill ${proofClass}">${esc(latestProofPasses)} / ${esc(latestProofRequests)} pass</span>
                <span>${esc(proofPct)}%</span>
              </div>
              <div class="bar proof ${latestProofFailures ? "bad" : ""}"><span style="width:${Math.min(100, Math.max(0, proofPct))}%"></span></div>
              <div class="epoch-row">
                ${failedEpochChips || `<span class="epoch-chip ok">history clean</span>`}
              </div>
            </td>
            <td>
              <div>${esc(duplicateTotal)} latest</div>
              <div class="mini">${esc(slot.duplicate_signatures || 0)} local, ${esc(slot.cross_slot_duplicate_signatures || 0)} cross</div>
            </td>
            <td>
              <div>expected ${esc(slot.expected_model_index ?? "")}</div>
              <div class="mini">${esc(slot.latest_wrong_model_index || 0)} wrong latest</div>
            </td>
            <td>
              <div class="mono">${esc(slot.last_receipt_at_iso || "")}</div>
              ${slot.stale ? `<span class="pill warn">stale</span>` : ""}
            </td>
            <td class="wrap">${esc(notes || "clean")}</td>
          </tr>
        `;
	      }).join("");
	    }
	
	    function renderSupervisor(supervisor) {
	      const status = supervisor?.status || "missing";
	      const summary = supervisor?.summary || {};
	      const topology = supervisor?.topology || {};
	      const processes = asArray(supervisor?.processes);
	      const detail = supervisor?.last_error ? ` - ${supervisor.last_error}` : "";
	      const topologyText = topology.path
	        ? ` - topology reloads ${topology.reload_count || 0}${topology.last_error ? ` (${topology.last_error})` : ""}`
	        : "";
	      el("supervisor-summary").textContent = `${status} - ${summary.running || 0} running, ${summary.external || 0} external, ${summary.restarting || 0} restarting, ${summary.down || 0} down${topologyText}${detail}`;
	      if (!processes.length) {
	        el("supervisor").innerHTML = `
	          <tr>
	            <td colspan="6">${esc(supervisor?.last_error || "No supervisor status rows yet")}</td>
	          </tr>
	        `;
	        return;
	      }
	      el("supervisor").innerHTML = processes.map((process) => {
	        const restartNote = process.next_restart_seconds !== null && process.next_restart_seconds !== undefined
	          ? `next in ${fmtDuration(process.next_restart_seconds)}`
	          : "";
	        const detailParts = [
	          process.url,
	          process.external_last_seen ? `external seen ${process.external_last_seen}` : "",
	          process.last_error
	        ].filter(Boolean);
	        return `
	          <tr class="row-${statusClass(process.status)}">
	            <td>
	              <div class="stack">
	                <span class="mono">${esc(process.name || "")}</span>
	                ${process.slot_id ? `<span class="mini mono">slot ${esc(process.slot_index || "")} / ${esc(shortId(process.slot_id, 8))}</span>` : ""}
	              </div>
	            </td>
	            <td>${esc(process.kind || "")}</td>
	            <td><span class="pill ${statusClass(process.status)}">${esc(process.status || "unknown")}</span></td>
	            <td>
	              <div class="stack">
	                <span class="mono">${esc(process.pid || "")}</span>
	                <span class="mini">${esc(process.managed_by_supervisor ? "supervised" : (process.external_active ? "external/adopted" : "not owned"))}</span>
	              </div>
	            </td>
	            <td>
	              <div>${esc(process.restart_count || 0)}</div>
	              <span class="mini">${esc(restartNote)}</span>
	            </td>
	            <td class="wrap">${esc(detailParts.join(" - "))}</td>
	          </tr>
	        `;
	      }).join("");
	    }
	
	    function renderMiners(miners) {
        const rows = asArray(miners);
	      el("miner-summary").textContent = `${rows.filter((m) => m.status === "ok").length}/${rows.length} ok`;
	      el("miners").innerHTML = rows.map((miner) => `
        <tr>
          <td class="mono">${esc(miner.miner_id)}</td>
          <td>
            <div class="stack">
              <span class="mono">${esc(miner.hotkey || "")}</span>
              <span class="mini mono">${esc(miner.hotkey_ss58 || "")}</span>
            </div>
          </td>
          <td class="mono">${esc(miner.coldkey || "")}</td>
          <td><span class="pill ${statusClass(miner.status)}">${esc(miner.status)}</span></td>
          <td>
            <div class="stack">
              <span><span class="pill ${statusClass(miner.chain_status)}">${esc(miner.chain_status)}</span></span>
              <span class="mini">${esc(miner.chain_running ? "local process running" : "local process stopped")}${miner.chain_uid !== null && miner.chain_uid !== undefined ? ` - uid ${esc(miner.chain_uid)}` : ""}</span>
            </div>
          </td>
          <td>
            <div class="stack">
              <span class="score ${scoreClass(miner.score)}">${esc(formatScore(miner.score))}</span>
              <span class="mini">${esc(miner.score_source)}${miner.verathos_rows ? ` - ${esc(miner.verathos_rows)} rows` : ""}</span>
            </div>
          </td>
	          <td>${esc(miner.healthy_slots)}/${esc(miner.total_slots)}</td>
	          <td>
	            <div class="stack">
	              <span>${esc(miner.supervised_slots || 0)} supervised</span>
	              <span class="mini">${esc(miner.external_slots || 0)} external, ${esc(miner.restarting_slots || 0)} restarting</span>
	            </div>
	          </td>
	          <td>
	            <div>${esc(fmtInt(miner.success_count))} / ${esc(fmtInt(miner.failure_count))}</div>
            <span class="mini">${esc(miner.count_source === "receipts" ? "receipt-backed" : "live since restart")}</span>
          </td>
          <td class="mono">${esc(asArray(miner.ports).join(", "))}</td>
          <td>
            <div class="stack">
              <span>${esc(fmtInt(miner.verathos_requests))} req</span>
              <span class="mini">${esc(fmtInt(miner.verathos_tokens))} tokens</span>
            </div>
          </td>
        </tr>
      `).join("");
    }

    function renderSlots(slots) {
      const rows = asArray(slots);
      el("slot-summary").textContent = `${rows.filter((slot) => slot.healthy).length}/${rows.length} healthy`;
      el("slots").innerHTML = rows.map((slot) => {
        const receiptBacked = slot.count_source === "receipts";
        const latestEpochNote = receiptBacked
          ? `epoch ${slot.latest_epoch ?? ""}: ${fmtInt(slot.latest_epoch_receipts)} receipts`
          : "live since restart";
        const liveNote = receiptBacked
          ? `live ${fmtInt(slot.live_success_count)} / ${fmtInt(slot.live_failure_count)} of ${fmtInt(slot.live_request_count)}`
          : "";
        return `
          <tr>
            <td class="mono">${esc(slot.port)}</td>
	            <td class="mono">${esc(slot.miner_id)}</td>
	            <td class="mono">${esc(slot.slot_index)} / ${esc(slot.slot_id.slice(0, 8))}</td>
	            <td><span class="pill ${slot.healthy ? "ok" : "bad"}">${slot.healthy ? "ok" : "down"}</span></td>
	            <td>
	              <div class="stack">
	                <span><span class="pill ${statusClass(slot.process_status)}">${esc(slot.process_status || "unknown")}</span></span>
	                <span class="mini">${esc(slot.process_managed_by_supervisor ? `pid ${slot.process_pid}` : (slot.process_external_active ? "external/adopted" : "not owned"))}</span>
	                ${slot.process_restart_count ? `<span class="mini">${esc(slot.process_restart_count)} restarts</span>` : ""}
	              </div>
	            </td>
	            <td>
	              <div>${esc(fmtInt(slot.success_count))} / ${esc(fmtInt(slot.failure_count))}</div>
              <span class="mini">${esc(receiptBacked ? "receipt-backed" : "live since restart")}</span>
              ${liveNote ? `<span class="mini">${esc(liveNote)}</span>` : ""}
            </td>
            <td>
              <div>${esc(fmtInt(slot.request_count))}</div>
              <span class="mini">${esc(latestEpochNote)}</span>
            </td>
            <td>
              <div class="stack">
                <span class="score ${scoreClass(slot.verathos_score)}">${esc(formatScore(slot.verathos_score))}</span>
                <span class="mini">${slot.verathos_rows ? `${slot.verathos_healthy ? "healthy" : "unhealthy"} - ${esc(slot.verathos_rows)} rows` : "not listed"}</span>
                ${slot.verathos_consecutive_failures ? `<span class="mini">${esc(slot.verathos_consecutive_failures)} consecutive failures</span>` : ""}
              </div>
            </td>
            <td class="mono">${esc(slot.last_seen || "")}</td>
            <td>${esc(slot.last_error || "")}</td>
          </tr>
        `;
      }).join("");
    }

    function renderGpus(gpus) {
      const rows = asArray(gpus);
      el("gpu-summary").textContent = `${rows.filter((gpu) => gpu.healthy).length}/${rows.length} healthy`;
      el("gpus").innerHTML = rows.map((gpu) => {
        const memoryPctNumber = Number(gpu.memory_used_percent);
        const memoryPct = Number.isFinite(memoryPctNumber) ? Math.round(memoryPctNumber) : null;
        const hasMemoryUsed = gpu.memory_used_mb !== null && gpu.memory_used_mb !== undefined;
        const hasMemoryFree = gpu.memory_free_mb !== null && gpu.memory_free_mb !== undefined;
        const kvText = gpu.kv_utilization_pct !== null && gpu.kv_utilization_pct !== undefined
          ? `KV ${fmtPercentMaybe(gpu.kv_utilization_pct)} used`
          : "NVIDIA metrics not reported";
        const vramMain = hasMemoryUsed
          ? `${fmtGbMaybe(gpu.memory_used_mb)} / ${fmtGbMaybe(gpu.memory_total_mb)}`
          : `not reported / ${fmtGbMaybe(gpu.memory_total_mb)}`;
        const vramSub = hasMemoryFree
          ? `${fmtGbMaybe(gpu.memory_free_mb)} free`
          : kvText;
        const workerStats = gpu.worker_stats_available
          ? `${fmtInt(gpu.worker_completed_jobs)} / ${fmtInt(gpu.worker_failed_jobs)}`
          : "not reported";
        return `
          <tr>
            <td>
              <div class="stack">
                <span class="mono">${esc(gpu.gpu_id)}</span>
                <span class="mini">${esc(gpu.gpu_name || "")}</span>
                <span class="mini">${esc(gpu.provider || "")}${gpu.instance_id ? ` #${esc(gpu.instance_id)}` : ""}</span>
              </div>
            </td>
            <td><span class="pill ${statusClass(gpu.status)}">${esc(gpu.status)}</span></td>
            <td><span class="score ${scoreClass(gpu.score)}">${esc(gpu.score)}</span></td>
            <td>
              <div class="stack">
                <span>${esc(gpu.active_jobs)}/${esc(gpu.max_jobs)} active</span>
                <span class="mini">${esc(gpu.router_inflight)} router inflight, ${esc(gpu.free_jobs)} free</span>
              </div>
            </td>
            <td>
              <div class="stack">
                <span class="mono">${esc(gpu.model || "")}</span>
                <span class="mini mono">${esc(gpu.url || "")}</span>
              </div>
            </td>
            <td>
              <div>${esc(vramMain)}</div>
              <div class="mini">${esc(vramSub)}</div>
              ${memoryPct === null ? "" : `<div class="bar"><span style="width:${Math.min(100, Math.max(0, memoryPct))}%"></span></div>`}
            </td>
            <td>${esc(fmtPercentMaybe(gpu.utilization_gpu_percent))}</td>
            <td>${esc(fmtTempMaybe(gpu.temperature_gpu_c))}</td>
            <td>${esc(gpu.success_count)} / ${esc(gpu.failure_count)}</td>
            <td>${esc(workerStats)}</td>
          </tr>
        `;
      }).join("");
    }

    function renderLogsList(logs) {
      const select = el("log-file");
      const current = state.selectedLog || logs?.default || "router.log";
      const files = asArray(logs?.files);
      select.innerHTML = files.map((name) => (
        `<option value="${esc(name)}"${name === current ? " selected" : ""}>${esc(name)}</option>`
      )).join("");
      state.selectedLog = select.value || current;
    }

    async function loadLog() {
      const name = el("log-file").value || state.selectedLog;
      state.selectedLog = name;
      const response = await fetch(`/v1/dashboard/logs?name=${encodeURIComponent(name)}&lines=180`);
      if (!response.ok) {
        el("logs").textContent = `Failed to load ${name}: HTTP ${response.status}`;
        return;
      }
      const data = await response.json();
      el("logs").textContent = data.content || "";
    }

    async function refresh() {
      try {
        const response = await fetch("/v1/dashboard", { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
	        const data = await response.json();
	        state.data = data;
	        el("updated").textContent = `Updated ${data.checked_at}`;
	        el("router-status").textContent = data.health.status;
	        el("router-status").className = `pill ${statusClass(data.health.status)}`;
	        renderMetrics(data);
	        renderLeases(data.lease_watcher);
	        renderReceipts(data.receipt_integrity);
	        renderSupervisor(data.supervisor);
	        renderMiners(data.miners);
        renderSlots(data.slots);
        renderGpus(data.gpus);
        renderLogsList(data.logs);
      } catch (error) {
        el("updated").textContent = `Dashboard error: ${error.message}`;
        el("router-status").textContent = "error";
        el("router-status").className = "pill bad";
      }
    }

    el("refresh").addEventListener("click", () => refresh());
    el("load-log").addEventListener("click", () => loadLog());
    el("log-file").addEventListener("change", () => loadLog());

    refresh().then(loadLog);
    state.timer = window.setInterval(refresh, 5000);
    state.clockTimer = window.setInterval(() => {
      if (state.data) renderMetrics(state.data);
    }, 1000);
  </script>
</body>
</html>
"""
