"""
Live log viewer for the sleep health pipeline.

Run:  python log_viewer.py
Open:  http://localhost:8777
"""
import json
import subprocess
import sys
import threading
import time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path

LOG_FILE = Path(__file__).resolve().parent / "pipeline.log"
SUMMARY_FILE = Path(__file__).resolve().parent / "run_summary.json"
PROJECT_DIR = Path(__file__).resolve().parent.parent
PORT = 8777

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pipeline Log Viewer</title>
<style>
  :root {
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #c9d1d9; --muted: #8b949e;
    --blue: #58a6ff; --green: #3fb950; --yellow: #d29922;
    --red: #f85149; --purple: #bc8cff; --cyan: #39d2c0;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }

  /* Dark scrollbars */
  ::-webkit-scrollbar { width: 10px; height: 10px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 5px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--muted); }
  ::-webkit-scrollbar-corner { background: var(--bg); }
  * { scrollbar-width: thin; scrollbar-color: var(--border) var(--bg); }
  body {
    font-family: 'Cascadia Code', 'Fira Code', 'JetBrains Mono', monospace;
    background: var(--bg); color: var(--text);
    display: flex; flex-direction: column; height: 100vh;
  }

  /* Header */
  header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: var(--surface);
    border-bottom: 1px solid var(--border);
  }
  header h1 { font-size: 14px; font-weight: 600; color: var(--text); }
  header h1 span { color: var(--muted); font-weight: 400; }
  .controls { display: flex; gap: 10px; align-items: center; }
  .status {
    display: flex; align-items: center; gap: 6px;
    font-size: 11px; color: var(--muted);
  }
  .status .dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--green); animation: pulse 2s infinite;
  }
  .status.disconnected .dot { background: var(--red); animation: none; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: .4; } }

  button {
    padding: 4px 12px; font-size: 11px; font-family: inherit;
    border: 1px solid var(--border); border-radius: 6px;
    background: var(--surface); color: var(--text); cursor: pointer;
  }
  button:hover { border-color: var(--muted); }
  button.active { border-color: var(--blue); color: var(--blue); }
  button.run-btn { border-color: var(--green); color: var(--green); }
  button.run-btn:hover { background: rgba(63,185,80,.1); }
  button.run-btn.running { border-color: var(--yellow); color: var(--yellow); cursor: wait; }

  /* Filters */
  .filters {
    display: flex; gap: 6px; padding: 8px 20px;
    background: var(--surface); border-bottom: 1px solid var(--border);
  }
  .filters button { font-size: 10px; text-transform: uppercase; letter-spacing: .5px; }
  .filters button.active { background: var(--border); }

  /* Main content: log + summary sidebar */
  .main {
    flex: 1; display: flex; overflow: hidden;
  }

  /* Log area */
  #log {
    flex: 1; overflow-y: auto; padding: 12px 20px;
    font-size: 12.5px; line-height: 1.7;
  }

  /* Execution group */
  .exec-group { margin-bottom: 2px; }
  .exec-header {
    display: flex; align-items: center; gap: 10px;
    padding: 6px 12px; cursor: pointer; user-select: none;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; margin-bottom: 2px;
    font-size: 11px; color: var(--muted);
    transition: background .15s;
  }
  .exec-header:hover { background: #1c2129; }
  .exec-header .chevron {
    display: inline-block; transition: transform .15s;
    font-size: 10px; width: 14px; text-align: center;
  }
  .exec-group.collapsed .chevron { transform: rotate(-90deg); }
  .exec-group:not(.collapsed) .chevron { transform: rotate(0deg); }
  .exec-header .run-id { color: var(--cyan); font-weight: 600; }
  .exec-header .run-time { color: var(--muted); }
  .exec-header .run-status {
    font-weight: 700; font-size: 10px; text-transform: uppercase;
    letter-spacing: .5px; padding: 1px 6px; border-radius: 3px;
    cursor: pointer; position: relative;
  }
  .exec-header .run-status:hover { filter: brightness(1.3); }
  .exec-header .run-status.success { color: var(--green); background: rgba(63,185,80,.12); }
  .exec-header .run-status.failed { color: var(--red); background: rgba(248,81,73,.12); }
  .exec-header .run-status.running { color: var(--yellow); background: rgba(210,153,34,.12); }
  .exec-header .run-status.dry_run { color: var(--blue); background: rgba(88,166,255,.12); }
  .exec-header .run-status.unknown { color: var(--muted); background: rgba(139,148,158,.12); }
  .exec-header .run-status.selected { outline: 2px solid var(--blue); outline-offset: 1px; }
  .exec-header .line-count { color: var(--muted); margin-left: auto; }
  .exec-body { padding: 0 0 0 4px; }
  .exec-group.collapsed .exec-body { display: none; }

  /* Layer sub-groups */
  .layer-group { margin-bottom: 1px; }
  .layer-header {
    display: flex; align-items: center; gap: 8px;
    padding: 3px 10px; cursor: pointer; user-select: none;
    background: rgba(255,255,255,.02); border-left: 3px solid var(--muted);
    border-radius: 0 4px 4px 0; margin-bottom: 1px;
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .5px; color: var(--muted);
    transition: background .15s;
  }
  .layer-header:hover { background: rgba(255,255,255,.04); }
  .layer-header .chevron {
    display: inline-block; transition: transform .15s;
    font-size: 9px; width: 12px; text-align: center;
  }
  .layer-group.collapsed .chevron { transform: rotate(-90deg); }
  .layer-group:not(.collapsed) .chevron { transform: rotate(0deg); }
  .layer-header .layer-line-count { margin-left: auto; color: var(--muted); font-weight: 400; }
  .layer-body { padding: 0 0 0 6px; }
  .layer-group.collapsed .layer-body { display: none; }

  .layer-header.bronze  { border-left-color: #cd7f32; color: #cd7f32; }
  .layer-header.silver  { border-left-color: #c0c0c0; color: #c0c0c0; }
  .layer-header.gold    { border-left-color: #ffd700; color: #ffd700; }
  .layer-header.general { border-left-color: var(--muted); color: var(--muted); }

  .line {
    display: flex; gap: 0; white-space: nowrap;
    border-bottom: 1px solid transparent;
    padding: 1px 0;
  }
  .line:hover { background: rgba(88,166,255,.06); border-radius: 3px; }

  .ts   { color: var(--muted); min-width: 170px; }
  .lvl  { min-width: 70px; font-weight: 600; text-transform: uppercase; }
  .mod  { color: var(--purple); min-width: 260px; overflow: hidden; text-overflow: ellipsis; }
  .fn   { color: var(--cyan); min-width: 120px; }
  .msg  { color: var(--text); white-space: pre-wrap; flex: 1; }

  .lvl.debug    { color: var(--muted); }
  .lvl.info     { color: var(--green); }
  .lvl.warning  { color: var(--yellow); }
  .lvl.error, .lvl.critical { color: var(--red); }

  .line.error, .line.critical {
    background: rgba(248,81,73,.08); border-radius: 3px;
  }

  /* Run Summary sidebar */
  #summary {
    width: 300px; min-width: 300px;
    background: var(--surface); border-left: 1px solid var(--border);
    padding: 16px; overflow-y: auto; font-size: 11.5px;
  }
  #summary h2 {
    font-size: 12px; font-weight: 600; color: var(--text);
    margin-bottom: 14px; text-transform: uppercase; letter-spacing: .5px;
  }
  #summary .empty { color: var(--muted); font-style: italic; }

  .summary-section { margin-bottom: 16px; }
  .summary-section h3 {
    font-size: 10px; font-weight: 600; color: var(--muted);
    text-transform: uppercase; letter-spacing: .5px;
    margin-bottom: 6px; padding-bottom: 4px;
    border-bottom: 1px solid var(--border);
  }
  .summary-row {
    display: flex; justify-content: space-between;
    padding: 3px 0; color: var(--text);
  }
  .summary-row .label { color: var(--muted); }
  .summary-row .value { font-weight: 600; }

  .status-badge {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .5px;
  }
  .status-badge.success { background: rgba(63,185,80,.15); color: var(--green); }
  .status-badge.failed  { background: rgba(248,81,73,.15); color: var(--red); }
  .status-badge.started { background: rgba(210,153,34,.15); color: var(--yellow); }
  .status-badge.dry_run { background: rgba(88,166,255,.15); color: var(--blue); }

  /* Stats bar */
  .stats {
    display: flex; gap: 16px; padding: 6px 20px;
    font-size: 11px; color: var(--muted);
    background: var(--surface); border-top: 1px solid var(--border);
  }
  .stats span strong { color: var(--text); }

  /* Search */
  #search {
    padding: 4px 10px; font-size: 11px; font-family: inherit;
    border: 1px solid var(--border); border-radius: 6px;
    background: var(--bg); color: var(--text); width: 180px;
  }
  #search:focus { outline: none; border-color: var(--blue); }
</style>
</head>
<body>
  <header>
    <h1>Pipeline Log Viewer <span>/ logs/pipeline.log</span></h1>
    <div class="controls">
      <button id="runBtn" class="run-btn" onclick="runPipeline()">Run Pipeline</button>
      <input id="search" type="text" placeholder="Filter messages...">
      <button id="collapseBtn" onclick="toggleDefaultCollapse()">Expand all</button>
      <button id="scrollBtn" class="active" onclick="toggleScroll()">Auto-scroll</button>
      <button onclick="clearLog()">Clear</button>
      <div id="status" class="status"><div class="dot"></div><span>Connected</span></div>
    </div>
  </header>
  <div class="filters">
    <button class="active" data-level="all" onclick="setFilter('all',this)">All</button>
    <button data-level="debug" onclick="setFilter('debug',this)">Debug</button>
    <button data-level="info" onclick="setFilter('info',this)">Info</button>
    <button data-level="warning" onclick="setFilter('warning',this)">Warning</button>
    <button data-level="error" onclick="setFilter('error',this)">Error</button>
  </div>
  <div class="main">
    <div id="log"></div>
    <div id="summary">
      <h2>Run Summary</h2>
      <div id="summaryContent"><span class="empty">Click a run status badge to view its summary</span></div>
    </div>
  </div>
  <div class="stats">
    <span>Lines: <strong id="statTotal">0</strong></span>
    <span>DEBUG: <strong id="statDebug">0</strong></span>
    <span>INFO: <strong id="statInfo">0</strong></span>
    <span>WARN: <strong id="statWarn">0</strong></span>
    <span>ERROR: <strong id="statError">0</strong></span>
    <span>Runs: <strong id="statRuns">0</strong></span>
  </div>

<script>
const logEl = document.getElementById('log');
const statusEl = document.getElementById('status');
const summaryEl = document.getElementById('summaryContent');
let autoScroll = true;
let levelFilter = 'all';
let searchTerm = '';
let counts = { total: 0, debug: 0, info: 0, warning: 0, error: 0 };

// All run summaries keyed by run_id (short 8-char)
let summariesByRunId = {};
let selectedRunId = null;

// Execution groups
let execGroups = {};
let execOrder = [];
let defaultCollapsed = true;

function parseLine(raw) {
  const parts = raw.split(' | ');
  if (parts.length >= 7) {
    // New format: ts | level | run_id | layer | module | func | msg
    return {
      ts: parts[0].trim(),
      level: parts[1].trim().toLowerCase(),
      runId: parts[2].trim(),
      layer: parts[3].trim().toLowerCase(),
      module: parts[4].trim(),
      func: parts[5].trim(),
      msg: parts.slice(6).join(' | ').trim()
    };
  }
  if (parts.length >= 6) {
    // Previous format: ts | level | run_id | module | func | msg
    return {
      ts: parts[0].trim(),
      level: parts[1].trim().toLowerCase(),
      runId: parts[2].trim(),
      layer: 'general',
      module: parts[3].trim(),
      func: parts[4].trim(),
      msg: parts.slice(5).join(' | ').trim()
    };
  }
  if (parts.length >= 5) {
    // Old format: ts | level | module | func | msg
    const msg = parts.slice(4).join(' | ').trim();
    const m = msg.match(/\[run_id=([a-f0-9]+)\]/);
    const layerMatch = msg.match(/^\[(Bronze|Silver|Gold)\]/i);
    return {
      ts: parts[0].trim(),
      level: parts[1].trim().toLowerCase(),
      runId: m ? m[1] : null,
      layer: layerMatch ? layerMatch[1].toLowerCase() : 'general',
      module: parts[2].trim(),
      func: parts[3].trim(),
      msg: msg
    };
  }
  return null;
}

function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function renderLine(p) {
  const div = document.createElement('div');
  div.className = 'line ' + p.level;
  div.dataset.level = p.level;
  div.innerHTML =
    `<span class="ts">${esc(p.ts)}</span>` +
    `<span class="lvl ${p.level}">${esc(p.level.toUpperCase().padEnd(7))}</span>` +
    `<span class="mod">${esc(p.module)}</span>` +
    `<span class="fn">${esc(p.func)}</span>` +
    `<span class="msg">${esc(p.msg)}</span>`;
  if (levelFilter !== 'all' && p.level !== levelFilter) div.style.display = 'none';
  if (searchTerm && !p.msg.toLowerCase().includes(searchTerm)) div.style.display = 'none';
  return div;
}

function detectStatus(msg) {
  if (/Pipeline complete/i.test(msg)) return 'success';
  if (/Pipeline failed/i.test(msg)) return 'failed';
  if (/\[Dry-run\]/i.test(msg)) return 'dry_run';
  return null;
}

function getOrCreateGroup(runId, ts) {
  if (execGroups[runId]) return execGroups[runId];

  const group = document.createElement('div');
  group.className = 'exec-group';

  const header = document.createElement('div');
  header.className = 'exec-header';
  header.innerHTML =
    `<span class="chevron">&#9660;</span>` +
    `<span class="run-id">${esc(runId)}</span>` +
    `<span class="run-time">${esc(ts)}</span>` +
    `<span class="run-status running" data-run-id="${esc(runId)}">running</span>` +
    `<span class="line-count">0 lines</span>`;

  header.onclick = (e) => {
    if (e.target.classList.contains('run-status')) {
      e.stopPropagation();
      selectRunSummary(e.target.dataset.runId);
      return;
    }
    group.classList.toggle('collapsed');
  };

  const body = document.createElement('div');
  body.className = 'exec-body';

  group.appendChild(header);
  group.appendChild(body);
  logEl.appendChild(group);

  // currentLayer: the active layer sub-group (changes when layer changes)
  const info = { el: group, bodyEl: body, headerEl: header, lineCount: 0, firstTs: ts, lastTs: ts, status: 'running', currentLayer: null, currentLayerName: null };
  execGroups[runId] = info;
  execOrder.push(runId);

  applyCollapsePolicy();
  document.getElementById('statRuns').textContent = execOrder.length;
  return info;
}

function getOrCreateLayerGroup(execInfo, layer) {
  // If same layer as current, reuse it
  if (execInfo.currentLayerName === layer && execInfo.currentLayer) {
    return execInfo.currentLayer;
  }

  // New sequential layer group
  const lg = document.createElement('div');
  lg.className = 'layer-group';

  const header = document.createElement('div');
  header.className = 'layer-header ' + layer;
  header.innerHTML =
    `<span class="chevron">&#9660;</span>` +
    `<span>${esc(layer)}</span>` +
    `<span class="layer-line-count">0 lines</span>`;
  header.onclick = () => lg.classList.toggle('collapsed');

  const body = document.createElement('div');
  body.className = 'layer-body';

  lg.appendChild(header);
  lg.appendChild(body);
  execInfo.bodyEl.appendChild(lg);

  const info = { el: lg, bodyEl: body, headerEl: header, lineCount: 0 };
  execInfo.currentLayer = info;
  execInfo.currentLayerName = layer;
  return info;
}

function applyCollapsePolicy() {
  for (let i = 0; i < execOrder.length; i++) {
    const g = execGroups[execOrder[i]];
    const isLast = i === execOrder.length - 1;
    if (defaultCollapsed) {
      if (isLast) g.el.classList.remove('collapsed');
      else g.el.classList.add('collapsed');
    } else {
      g.el.classList.remove('collapsed');
    }
  }
}

function updateGroupStatus(info, status) {
  info.status = status;
  const badge = info.headerEl.querySelector('.run-status');
  badge.className = 'run-status ' + status;
  if (selectedRunId === badge.dataset.runId) badge.classList.add('selected');
  badge.textContent = status === 'dry_run' ? 'dry run' : status;
}

function updateGroupMeta(info, ts) {
  info.lineCount++;
  info.lastTs = ts;
  info.headerEl.querySelector('.line-count').textContent = info.lineCount + ' lines';
  info.headerEl.querySelector('.run-time').textContent = info.firstTs + '  \u2192  ' + ts;
}

function updateStats(level) {
  counts.total++;
  if (level === 'debug') counts.debug++;
  else if (level === 'info') counts.info++;
  else if (level === 'warning') counts.warning++;
  else if (level === 'error' || level === 'critical') counts.error++;
  document.getElementById('statTotal').textContent = counts.total;
  document.getElementById('statDebug').textContent = counts.debug;
  document.getElementById('statInfo').textContent = counts.info;
  document.getElementById('statWarn').textContent = counts.warning;
  document.getElementById('statError').textContent = counts.error;
}

let lastSeenRunId = null;

function addLine(raw) {
  const p = parseLine(raw);
  if (!p) return;

  let runId = p.runId || lastSeenRunId || 'no-id';
  if (p.runId) lastSeenRunId = p.runId;

  const group = getOrCreateGroup(runId, p.ts);
  const layerGroup = getOrCreateLayerGroup(group, p.layer);
  const el = renderLine(p);
  layerGroup.bodyEl.appendChild(el);
  layerGroup.lineCount++;
  layerGroup.headerEl.querySelector('.layer-line-count').textContent = layerGroup.lineCount + ' lines';
  updateGroupMeta(group, p.ts);
  updateStats(p.level);

  const st = detectStatus(p.msg);
  if (st) updateGroupStatus(group, st);

  if (autoScroll) logEl.scrollTop = logEl.scrollHeight;
}

// --- Run Summary ---
function selectRunSummary(runId) {
  // Clear previous selection highlight
  document.querySelectorAll('.run-status.selected').forEach(el => el.classList.remove('selected'));

  selectedRunId = runId;

  // Highlight the clicked badge
  const badge = document.querySelector(`.run-status[data-run-id="${runId}"]`);
  if (badge) badge.classList.add('selected');

  // Find matching summary (match short 8-char id)
  const summary = summariesByRunId[runId];
  if (summary) {
    renderSummary(summary);
  } else {
    summaryEl.innerHTML = '<span class="empty">No summary data for this run</span>';
  }
}

function renderSummary(s) {
  if (!s || !s.run_id) {
    summaryEl.innerHTML = '<span class="empty">No summary yet</span>';
    return;
  }
  let html = '';

  html += '<div class="summary-section">';
  html += '<h3>Run Info</h3>';
  html += summaryRow('Status', `<span class="status-badge ${s.status}">${s.status}</span>`);
  html += summaryRow('Run ID', s.run_id.substring(0, 8));
  html += summaryRow('Time', formatTimestamp(s.timestamp));
  html += summaryRow('Duration', s.elapsed_seconds != null ? s.elapsed_seconds + 's' : '-');
  html += summaryRow('Source', s.source_file || '-');
  html += '</div>';

  if (s.layers) {
    if (s.layers.bronze) {
      html += '<div class="summary-section">';
      html += '<h3>Bronze</h3>';
      html += summaryRow('Rows in', num(s.layers.bronze.rows_in));
      html += summaryRow('Rows out', num(s.layers.bronze.rows_out));
      html += '</div>';
    }
    if (s.layers.silver) {
      html += '<div class="summary-section">';
      html += '<h3>Silver</h3>';
      html += summaryRow('Rows in', num(s.layers.silver.rows_in));
      html += summaryRow('Valid', `<span style="color:var(--green)">${num(s.layers.silver.rows_valid)}</span>`);
      const q = s.layers.silver.rows_quarantined || 0;
      const qColor = q > 0 ? 'var(--yellow)' : 'var(--green)';
      html += summaryRow('Quarantined', `<span style="color:${qColor}">${num(q)}</span>`);
      html += '</div>';
    }
    if (s.layers.gold) {
      html += '<div class="summary-section">';
      html += '<h3>Gold</h3>';
      for (const [name, count] of Object.entries(s.layers.gold)) {
        const label = name.replace('gold_', '').replace(/_/g, ' ');
        html += summaryRow(label, num(count) + ' rows');
      }
      html += '</div>';
    }
  }

  summaryEl.innerHTML = html;
}

function summaryRow(label, value) {
  return `<div class="summary-row"><span class="label">${label}</span><span class="value">${value}</span></div>`;
}

function num(n) { return n != null ? n.toLocaleString() : '-'; }

function formatTimestamp(ts) {
  if (!ts) return '-';
  try {
    const d = new Date(ts);
    return d.toLocaleTimeString() + ' ' + d.toLocaleDateString();
  } catch { return ts; }
}

function loadSummaries() {
  fetch('/summary')
    .then(r => {
      if (r.status !== 200) return null;
      return r.json();
    })
    .then(data => {
      if (!data) return;
      // Handle both old single-object and new array format
      const arr = Array.isArray(data) ? data : [data];
      for (const s of arr) {
        if (s.run_id) {
          summariesByRunId[s.run_id.substring(0, 8)] = s;
        }
      }
      // If a run is currently selected, refresh its display
      if (selectedRunId && summariesByRunId[selectedRunId]) {
        renderSummary(summariesByRunId[selectedRunId]);
      }
    })
    .catch(err => console.error('Summary fetch error:', err));
}

// --- Run Pipeline ---
let pipelineRunning = false;

function runPipeline() {
  if (pipelineRunning) return;
  const btn = document.getElementById('runBtn');
  const prevRuns = execOrder.length;
  btn.textContent = 'Running...';
  btn.classList.add('running');
  pipelineRunning = true;

  fetch('/run', { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      console.log('Pipeline triggered:', data);
      // Poll until a new run appears in the log, or timeout after 30s
      let elapsed = 0;
      const check = setInterval(() => {
        elapsed += 500;
        if (execOrder.length > prevRuns || elapsed >= 30000) {
          clearInterval(check);
          btn.textContent = 'Run Pipeline';
          btn.classList.remove('running');
          pipelineRunning = false;
        }
      }, 500);
    })
    .catch(err => {
      console.error('Run pipeline error:', err);
      btn.textContent = 'Run Pipeline';
      btn.classList.remove('running');
      pipelineRunning = false;
    });
}

// --- Controls ---
function toggleDefaultCollapse() {
  defaultCollapsed = !defaultCollapsed;
  const btn = document.getElementById('collapseBtn');
  btn.textContent = defaultCollapsed ? 'Expand all' : 'Collapse all';
  btn.className = defaultCollapsed ? '' : 'active';
  for (const runId of execOrder) {
    const g = execGroups[runId];
    const isLast = runId === execOrder[execOrder.length - 1];
    if (defaultCollapsed) {
      if (isLast) g.el.classList.remove('collapsed');
      else g.el.classList.add('collapsed');
    } else {
      g.el.classList.remove('collapsed');
    }
  }
}

function toggleScroll() {
  autoScroll = !autoScroll;
  document.getElementById('scrollBtn').className = autoScroll ? 'active' : '';
  if (autoScroll) logEl.scrollTop = logEl.scrollHeight;
}

function clearLog() {
  logEl.innerHTML = '';
  execGroups = {};
  execOrder = [];
  lastSeenRunId = null;
  selectedRunId = null;
  counts = { total: 0, debug: 0, info: 0, warning: 0, error: 0 };
  document.getElementById('statTotal').textContent = '0';
  document.getElementById('statDebug').textContent = '0';
  document.getElementById('statInfo').textContent = '0';
  document.getElementById('statWarn').textContent = '0';
  document.getElementById('statError').textContent = '0';
  document.getElementById('statRuns').textContent = '0';
  summaryEl.innerHTML = '<span class="empty">Click a run status badge to view its summary</span>';
}

function setFilter(level, btn) {
  levelFilter = level;
  document.querySelectorAll('.filters button').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  logEl.querySelectorAll('.line').forEach(el => {
    const show = (level === 'all' || el.dataset.level === level) &&
                 (!searchTerm || el.querySelector('.msg').textContent.toLowerCase().includes(searchTerm));
    el.style.display = show ? '' : 'none';
  });
}

document.getElementById('search').addEventListener('input', (e) => {
  searchTerm = e.target.value.toLowerCase();
  logEl.querySelectorAll('.line').forEach(el => {
    const matchLevel = levelFilter === 'all' || el.dataset.level === levelFilter;
    const matchText = !searchTerm || el.querySelector('.msg').textContent.toLowerCase().includes(searchTerm);
    el.style.display = (matchLevel && matchText) ? '' : 'none';
  });
});

// --- SSE connection ---
function connect() {
  const es = new EventSource('/stream');
  es.onmessage = (e) => {
    const data = JSON.parse(e.data);
    data.lines.forEach(addLine);
  };
  es.onopen = () => {
    statusEl.className = 'status';
    statusEl.querySelector('span').textContent = 'Connected';
  };
  es.onerror = () => {
    statusEl.className = 'status disconnected';
    statusEl.querySelector('span').textContent = 'Reconnecting...';
  };
}

connect();
loadSummaries();
setInterval(loadSummaries, 3000);
</script>
</body>
</html>"""


class LogHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML.encode("utf-8"))

        elif self.path == "/summary":
            self._serve_summary()

        elif self.path == "/stream":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self._stream_log()

        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/run":
            self._run_pipeline()
        else:
            self.send_error(404)

    def _serve_summary(self):
        if SUMMARY_FILE.exists():
            try:
                data = SUMMARY_FILE.read_text(encoding="utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception:
                self.send_error(500)
        else:
            self.send_response(204)
            self.end_headers()

    def _run_pipeline(self):
        def _run_in_background():
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "pipeline.main"],
                    cwd=str(PROJECT_DIR),
                    capture_output=True,
                    text=True,
                    timeout=300,
                    env={**__import__("os").environ, "PYTHONPATH": str(PROJECT_DIR)},
                )
                if result.returncode != 0:
                    print(f"[run] Pipeline failed (rc={result.returncode}): {result.stderr}")
            except Exception as e:
                print(f"[run] Error: {e}")

        threading.Thread(target=_run_in_background, daemon=True).start()

        body = json.dumps({"status": "started"}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _stream_log(self):
        try:
            if LOG_FILE.exists():
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    lines = f.read().splitlines()
                if lines:
                    chunk = json.dumps({"lines": lines})
                    self.wfile.write(f"data: {chunk}\n\n".encode("utf-8"))
                    self.wfile.flush()
                pos = LOG_FILE.stat().st_size
            else:
                pos = 0

            while True:
                time.sleep(0.5)
                if not LOG_FILE.exists():
                    continue
                size = LOG_FILE.stat().st_size
                if size < pos:
                    pos = 0
                if size > pos:
                    with open(LOG_FILE, "r", encoding="utf-8") as f:
                        f.seek(pos)
                        new = f.read()
                    pos = size
                    new_lines = [l for l in new.splitlines() if l.strip()]
                    if new_lines:
                        chunk = json.dumps({"lines": new_lines})
                        self.wfile.write(f"data: {chunk}\n\n".encode("utf-8"))
                        self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, format, *args):
        pass


def main():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), LogHandler)
    server.daemon_threads = True
    print(f"Log viewer running at http://localhost:{PORT}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()


if __name__ == "__main__":
    main()
