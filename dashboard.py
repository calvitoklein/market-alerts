#!/usr/bin/env python3
"""
Dashboard web — Twitter Signal Monitor
Corre en: http://localhost:5000
Lanzar:   python dashboard.py
"""

import os, json, csv, sys
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template_string

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

app = Flask(__name__)
DIR = os.path.dirname(os.path.abspath(__file__))

# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _read_csv(path):
    try:
        rows = []
        with open(path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(row)
        return rows
    except Exception:
        return []

# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    open_pos  = _read_json(os.path.join(DIR, "open_positions.json"), {})
    perf      = _read_json(os.path.join(DIR, "performance.json"), {})
    log_file  = os.path.join(DIR, "bot_output.log")
    last_line = ""
    try:
        with open(log_file, encoding="utf-8", errors="replace") as f:
            lines = [l.strip() for l in f if l.strip()]
            last_line = lines[-1] if lines else ""
    except Exception:
        pass

    trades_csv = _read_csv(os.path.join(DIR, "positions_log.csv"))
    today      = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_rows = [r for r in trades_csv if r.get("Fecha","").startswith(today)]
    total_rows = [r for r in trades_csv if r.get("Modo","") not in ("SKIP","LOG","INFO")]

    wins   = sum(1 for r in total_rows if "TP" in r.get("Estado",""))
    losses = sum(1 for r in total_rows if "SL" in r.get("Estado",""))
    total_closed = wins + losses
    win_rate = round(wins / total_closed * 100) if total_closed > 0 else None

    return jsonify({
        "mode":         "PAPER" if os.environ.get("TRADE_ENABLED","false").lower() != "true" else "LIVE",
        "open_positions": open_pos,
        "today_alerts": len(today_rows),
        "total_trades": len(total_rows),
        "wins":         wins,
        "losses":       losses,
        "win_rate":     win_rate,
        "last_log":     last_line,
        "now_utc":      datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
    })

@app.route("/api/events")
def api_events():
    from bot_events import get_events
    return jsonify(get_events(80))

@app.route("/api/ranking")
def api_ranking():
    stats = _read_json(os.path.join(DIR, "trader_stats.json"), {})
    traders = []
    for handle, s in stats.items():
        if s.get("signals", 0) < 2:
            continue
        traders.append({
            "handle":   handle,
            "name":     s.get("name", handle),
            "signals":  s.get("signals", 0),
            "wins":     s.get("wins", 0),
            "losses":   s.get("losses", 0),
            "win_rate": round(s.get("win_rate", 0) * 100, 1),
        })
    traders.sort(key=lambda x: (-x["win_rate"], -x["signals"]))
    return jsonify(traders[:30])

@app.route("/api/trades")
def api_trades():
    rows = _read_csv(os.path.join(DIR, "positions_log.csv"))
    real = [r for r in rows if r.get("Modo","") not in ("SKIP",)]
    return jsonify(real[-100:][::-1])

@app.route("/api/chart/<symbol>")
def api_chart(symbol):
    """OHLCV data for chart — 1h candles, last 100."""
    sym = symbol.upper()
    try:
        import ccxt
        if sym in ("BTC", "ETH"):
            ex   = ccxt.binance({"options": {"defaultType": "spot"}})
            pair = f"{sym}/USDT"
        elif sym == "XAU":
            ex   = ccxt.bitget({"options": {"defaultType": "swap"}})
            pair = "XAU/USDT:USDT"
        else:
            ex   = ccxt.bitget({"options": {"defaultType": "swap"}})
            pair = f"{sym}/USDT:USDT"

        ohlcv = ex.fetch_ohlcv(pair, "1h", limit=100)
        candles = [{"time": int(c[0]/1000), "open": c[1], "high": c[2],
                    "low": c[3], "close": c[4]} for c in ohlcv]
        return jsonify({"symbol": sym, "candles": candles})
    except Exception as e:
        return jsonify({"symbol": sym, "candles": [], "error": str(e)})

@app.route("/api/prices")
def api_prices():
    symbols = [
        ("BTC",  "BTC/USDT",       "binance", "spot"),
        ("ETH",  "ETH/USDT",       "binance", "spot"),
        ("XAU",  "XAU/USDT:USDT",  "bitget",  "swap"),
        ("NVDA", "NVDA/USDT:USDT", "bitget",  "swap"),
        ("AAPL", "AAPL/USDT:USDT", "bitget",  "swap"),
    ]
    result = {}
    import ccxt
    for name, pair, exid, dtype in symbols:
        try:
            ex   = getattr(ccxt, exid)({"options": {"defaultType": dtype}})
            tick = ex.fetch_ticker(pair)
            pct  = round(tick.get("percentage") or 0, 2)
            result[name] = {"price": tick["last"], "pct": pct}
        except Exception:
            result[name] = {"price": None, "pct": 0}
    return jsonify(result)

# ── HTML Dashboard ────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Signal Monitor Dashboard</title>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
  :root {
    --bg: #0f1117; --bg2: #1a1d27; --bg3: #22263a;
    --border: #2d3148; --text: #e2e8f0; --muted: #6b7280;
    --green: #10b981; --red: #ef4444; --blue: #60a5fa;
    --purple: #a78bfa; --orange: #fb923c; --yellow: #f59e0b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; font-size: 13px; }

  /* ── Header ── */
  .header { display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: var(--bg2); border-bottom: 1px solid var(--border); }
  .header-left { display: flex; align-items: center; gap: 12px; }
  .logo { font-size: 18px; font-weight: 700; color: var(--blue); }
  .badge { padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; letter-spacing: 1px; }
  .badge-paper { background: #1e3a5f; color: var(--blue); }
  .badge-live  { background: #1f2d1f; color: var(--green); }
  .clock { color: var(--muted); font-size: 12px; }

  /* ── Stats row ── */
  .stats { display: grid; grid-template-columns: repeat(5, 1fr); gap: 1px;
    background: var(--border); border-bottom: 1px solid var(--border); }
  .stat { background: var(--bg2); padding: 14px 18px; text-align: center; }
  .stat-val { font-size: 26px; font-weight: 700; line-height: 1; }
  .stat-lbl { color: var(--muted); font-size: 11px; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
  .val-green { color: var(--green); }
  .val-blue  { color: var(--blue); }
  .val-purple{ color: var(--purple); }
  .val-yellow{ color: var(--yellow); }

  /* ── Prices bar ── */
  .prices-bar { display: flex; gap: 20px; padding: 8px 20px;
    background: var(--bg2); border-bottom: 1px solid var(--border); flex-wrap: wrap; }
  .price-item { display: flex; align-items: center; gap: 8px; }
  .price-sym { color: var(--muted); font-size: 11px; font-weight: 600; }
  .price-val { font-weight: 600; font-size: 13px; }
  .price-pct { font-size: 11px; }
  .up { color: var(--green); } .down { color: var(--red); }

  /* ── Main grid ── */
  .main { display: grid; grid-template-columns: 1fr 1fr; gap: 1px;
    background: var(--border); min-height: calc(100vh - 200px); }
  .panel { background: var(--bg2); display: flex; flex-direction: column; }
  .panel-full { grid-column: 1 / -1; }
  .panel-title { padding: 10px 16px; font-size: 11px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 1px; color: var(--muted);
    border-bottom: 1px solid var(--border); }

  /* ── Chart ── */
  .chart-tabs { display: flex; gap: 0; border-bottom: 1px solid var(--border); }
  .chart-tab { padding: 8px 16px; cursor: pointer; font-size: 12px; font-weight: 600;
    color: var(--muted); border-bottom: 2px solid transparent; transition: all 0.15s; }
  .chart-tab:hover { color: var(--text); }
  .chart-tab.active { color: var(--blue); border-bottom-color: var(--blue); }
  #chart-container { height: 280px; position: relative; }

  /* ── Positions ── */
  .pos-list { padding: 10px; flex: 1; }
  .pos-card { background: var(--bg3); border: 1px solid var(--border);
    border-radius: 8px; padding: 10px 14px; margin-bottom: 8px; }
  .pos-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
  .pos-sym { font-weight: 700; font-size: 14px; }
  .pos-dir { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }
  .dir-long  { background: #0d2d1f; color: var(--green); }
  .dir-short { background: #2d1212; color: var(--red); }
  .pos-levels { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; margin-top: 6px; }
  .pos-level { background: var(--bg2); border-radius: 4px; padding: 4px 8px; text-align: center; }
  .pos-level-lbl { color: var(--muted); font-size: 10px; }
  .pos-level-val { font-size: 12px; font-weight: 600; }
  .empty-msg { color: var(--muted); text-align: center; padding: 30px; }

  /* ── Event feed ── */
  .feed { flex: 1; overflow-y: auto; max-height: 420px; }
  .event { display: flex; gap: 10px; padding: 7px 14px;
    border-bottom: 1px solid var(--border); align-items: flex-start; }
  .event:hover { background: var(--bg3); }
  .ev-dot { width: 7px; height: 7px; border-radius: 50%; margin-top: 5px; flex-shrink: 0; }
  .ev-time { color: var(--muted); font-size: 11px; white-space: nowrap; margin-top: 1px; min-width: 46px; }
  .ev-type { font-size: 10px; font-weight: 700; padding: 1px 6px; border-radius: 3px;
    background: var(--bg3); white-space: nowrap; }
  .ev-msg { color: var(--text); line-height: 1.4; }

  /* ── Ranking ── */
  .ranking-table { width: 100%; border-collapse: collapse; }
  .ranking-table th { color: var(--muted); font-size: 10px; text-transform: uppercase;
    padding: 6px 12px; text-align: left; border-bottom: 1px solid var(--border); }
  .ranking-table td { padding: 7px 12px; border-bottom: 1px solid var(--border); }
  .ranking-table tr:hover td { background: var(--bg3); }
  .rank-num { color: var(--muted); font-weight: 700; text-align: center; }
  .rank-handle { color: var(--blue); font-weight: 600; }
  .rank-wr { font-weight: 700; }
  .wr-bar { height: 4px; background: var(--bg3); border-radius: 2px; margin-top: 3px; width: 80px; }
  .wr-fill { height: 100%; border-radius: 2px; background: var(--green); }

  /* ── Trades history ── */
  .trades-table { width: 100%; border-collapse: collapse; }
  .trades-table th { color: var(--muted); font-size: 10px; text-transform: uppercase;
    padding: 6px 12px; text-align: left; border-bottom: 1px solid var(--border); position: sticky; top: 0; background: var(--bg2); }
  .trades-table td { padding: 6px 12px; border-bottom: 1px solid var(--border); }
  .trades-table tr:hover td { background: var(--bg3); }
  .trades-scroll { max-height: 300px; overflow-y: auto; }
  .mode-paper  { color: var(--blue); }
  .mode-live   { color: var(--green); }
  .mode-insider{ color: var(--orange); }
  .mode-skip   { color: var(--muted); }

  .refresh-bar { background: var(--bg3); padding: 6px 20px; text-align: right;
    font-size: 11px; color: var(--muted); border-top: 1px solid var(--border); }
  .dot-live { width: 8px; height: 8px; background: var(--green); border-radius: 50%;
    display: inline-block; animation: pulse 2s infinite; margin-right: 6px; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <div class="header-left">
    <span class="logo">📡 Signal Monitor</span>
    <span class="badge badge-paper" id="mode-badge">PAPER</span>
    <span id="header-status" style="color:var(--muted);font-size:12px">Cargando...</span>
  </div>
  <div style="display:flex;align-items:center;gap:16px">
    <span><span class="dot-live"></span><span style="color:var(--green);font-size:12px">EN VIVO</span></span>
    <span class="clock" id="clock">--:-- UTC</span>
  </div>
</div>

<!-- Stats -->
<div class="stats">
  <div class="stat"><div class="stat-val val-blue"  id="s-open">—</div><div class="stat-lbl">Posiciones abiertas</div></div>
  <div class="stat"><div class="stat-val val-yellow" id="s-today">—</div><div class="stat-lbl">Alertas hoy</div></div>
  <div class="stat"><div class="stat-val val-purple" id="s-total">—</div><div class="stat-lbl">Total operaciones</div></div>
  <div class="stat"><div class="stat-val val-green"  id="s-wr">—</div><div class="stat-lbl">Win rate</div></div>
  <div class="stat"><div class="stat-val" id="s-wl" style="font-size:16px">— / —</div><div class="stat-lbl">Wins / Losses</div></div>
</div>

<!-- Prices bar -->
<div class="prices-bar" id="prices-bar">
  <span style="color:var(--muted);font-size:12px">Cargando precios...</span>
</div>

<!-- Main grid -->
<div class="main">

  <!-- Chart (full width) -->
  <div class="panel panel-full">
    <div style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border)">
      <div class="chart-tabs" id="chart-tabs">
        <div class="chart-tab active" data-sym="BTC" onclick="loadChart('BTC',this)">BTC</div>
        <div class="chart-tab" data-sym="XAU" onclick="loadChart('XAU',this)">XAU/Gold</div>
        <div class="chart-tab" data-sym="ETH" onclick="loadChart('ETH',this)">ETH</div>
        <div class="chart-tab" data-sym="NVDA" onclick="loadChart('NVDA',this)">NVDA</div>
        <div class="chart-tab" data-sym="AAPL" onclick="loadChart('AAPL',this)">AAPL</div>
      </div>
      <span style="padding:0 14px;color:var(--muted);font-size:11px">1H · últimas 100 velas</span>
    </div>
    <div id="chart-container"></div>
  </div>

  <!-- Posiciones abiertas -->
  <div class="panel">
    <div class="panel-title">📊 Posiciones abiertas</div>
    <div class="pos-list" id="positions">
      <div class="empty-msg">Sin posiciones abiertas</div>
    </div>
  </div>

  <!-- Feed en tiempo real -->
  <div class="panel">
    <div class="panel-title">⚡ Feed en tiempo real</div>
    <div class="feed" id="feed">
      <div class="empty-msg">Sin eventos aún...</div>
    </div>
  </div>

  <!-- Ranking de traders (full width) -->
  <div class="panel panel-full">
    <div class="panel-title">🏆 Ranking de traders</div>
    <div style="overflow-x:auto">
      <table class="ranking-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Trader</th>
            <th>Win Rate</th>
            <th>Señales</th>
            <th>Wins</th>
            <th>Losses</th>
            <th>Rendimiento</th>
          </tr>
        </thead>
        <tbody id="ranking-body">
          <tr><td colspan="7" style="color:var(--muted);padding:20px;text-align:center">Sin datos de traders aún</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- Historial de operaciones (full width) -->
  <div class="panel panel-full">
    <div class="panel-title">📋 Historial de operaciones</div>
    <div class="trades-scroll">
      <table class="trades-table">
        <thead>
          <tr>
            <th>Fecha</th><th>Hora</th><th>Fuente</th>
            <th>Instrumento</th><th>Dir.</th>
            <th>Entrada</th><th>TP</th><th>SL</th>
            <th>USD</th><th>Lev.</th><th>Modo</th><th>Estado</th>
          </tr>
        </thead>
        <tbody id="trades-body">
          <tr><td colspan="12" style="color:var(--muted);padding:20px;text-align:center">Sin operaciones registradas</td></tr>
        </tbody>
      </table>
    </div>
  </div>

</div>

<div class="refresh-bar">
  Actualización automática cada 15s · <span id="next-refresh">15</span>s
</div>

<script>
// ── Chart ─────────────────────────────────────────────────────────────────────
let chart = null;
let candleSeries = null;
let currentSym = 'BTC';

function initChart() {
  const container = document.getElementById('chart-container');
  chart = LightweightCharts.createChart(container, {
    width: container.offsetWidth,
    height: 280,
    layout: { background: {color:'#1a1d27'}, textColor:'#e2e8f0' },
    grid: { vertLines:{color:'#2d3148'}, horzLines:{color:'#2d3148'} },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor:'#2d3148' },
    timeScale: { borderColor:'#2d3148', timeVisible:true },
  });
  candleSeries = chart.addCandlestickSeries({
    upColor:'#10b981', downColor:'#ef4444',
    borderUpColor:'#10b981', borderDownColor:'#ef4444',
    wickUpColor:'#10b981', wickDownColor:'#ef4444',
  });
  window.addEventListener('resize', () => {
    chart.applyOptions({ width: container.offsetWidth });
  });
}

function loadChart(sym, tabEl) {
  currentSym = sym;
  document.querySelectorAll('.chart-tab').forEach(t => t.classList.remove('active'));
  if (tabEl) tabEl.classList.add('active');
  fetch('/api/chart/' + sym)
    .then(r => r.json())
    .then(d => {
      if (d.candles && d.candles.length > 0) {
        candleSeries.setData(d.candles);
        chart.timeScale().fitContent();
      }
      // Draw open position lines
      updateChartLines(sym);
    })
    .catch(console.error);
}

function updateChartLines(sym) {
  fetch('/api/status')
    .then(r => r.json())
    .then(s => {
      const pos = s.open_positions && s.open_positions[sym];
      if (!pos) return;
      // Entry line
      if (pos.entry) {
        chart.addLineSeries({ color:'#60a5fa', lineWidth:1, lineStyle:2 }).setData([]);
      }
    });
}

// ── Status ────────────────────────────────────────────────────────────────────
function updateStatus() {
  fetch('/api/status').then(r => r.json()).then(s => {
    document.getElementById('s-open').textContent = Object.keys(s.open_positions || {}).length;
    document.getElementById('s-today').textContent = s.today_alerts;
    document.getElementById('s-total').textContent = s.total_trades;
    document.getElementById('s-wr').textContent = s.win_rate !== null ? s.win_rate + '%' : 'N/A';
    document.getElementById('s-wl').textContent = s.wins + ' / ' + s.losses;

    const badge = document.getElementById('mode-badge');
    badge.textContent = s.mode;
    badge.className = 'badge ' + (s.mode === 'LIVE' ? 'badge-live' : 'badge-paper');

    document.getElementById('header-status').textContent =
      s.last_log ? s.last_log.slice(0, 80) : '';

    // Positions
    const posEl = document.getElementById('positions');
    const positions = s.open_positions || {};
    if (Object.keys(positions).length === 0) {
      posEl.innerHTML = '<div class="empty-msg">Sin posiciones abiertas</div>';
    } else {
      posEl.innerHTML = Object.entries(positions).map(([inst, p]) => {
        const isLong = p.direction === 'LARGO';
        const entry = p.entry ? Number(p.entry).toFixed(2) : '?';
        const tp    = p.tp    ? Number(p.tp).toFixed(2)    : 'N/A';
        const sl    = p.sl    ? Number(p.sl).toFixed(2)    : 'N/A';
        const since = p.opened_at ? new Date(p.opened_at).toUTCString().slice(17,22) + ' UTC' : '';
        return `<div class="pos-card">
          <div class="pos-header">
            <span class="pos-sym">${inst}</span>
            <div style="display:flex;gap:6px;align-items:center">
              <span class="pos-dir ${isLong?'dir-long':'dir-short'}">${p.direction}</span>
              <span style="color:var(--muted);font-size:11px">${since}</span>
            </div>
          </div>
          <div class="pos-levels">
            <div class="pos-level"><div class="pos-level-lbl">ENTRADA</div><div class="pos-level-val">${entry}</div></div>
            <div class="pos-level"><div class="pos-level-lbl">TP</div><div class="pos-level-val" style="color:var(--green)">${tp}</div></div>
            <div class="pos-level"><div class="pos-level-lbl">SL</div><div class="pos-level-val" style="color:var(--red)">${sl}</div></div>
          </div>
          <div style="margin-top:6px;font-size:11px;color:var(--muted)">${p.horizonte||''} · ${p.leverage||'?'}x lev · $${p.tamanio||'?'} margen</div>
        </div>`;
      }).join('');
    }
  }).catch(console.error);
}

// ── Prices ────────────────────────────────────────────────────────────────────
function updatePrices() {
  fetch('/api/prices').then(r => r.json()).then(prices => {
    const bar = document.getElementById('prices-bar');
    bar.innerHTML = Object.entries(prices).map(([sym, d]) => {
      if (!d.price) return '';
      const pct = d.pct || 0;
      const cls = pct >= 0 ? 'up' : 'down';
      const arrow = pct >= 0 ? '▲' : '▼';
      const fmt = d.price >= 1000 ? d.price.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})
                                   : d.price.toFixed(2);
      return `<div class="price-item">
        <span class="price-sym">${sym}</span>
        <span class="price-val">$${fmt}</span>
        <span class="price-pct ${cls}">${arrow} ${Math.abs(pct).toFixed(2)}%</span>
      </div>`;
    }).join('');
  }).catch(console.error);
}

// ── Events feed ───────────────────────────────────────────────────────────────
function updateFeed() {
  fetch('/api/events').then(r => r.json()).then(events => {
    const feed = document.getElementById('feed');
    if (!events.length) {
      feed.innerHTML = '<div class="empty-msg">Sin eventos aún...</div>';
      return;
    }
    feed.innerHTML = events.map(e => {
      const t = new Date(e.ts).toUTCString().slice(17,22);
      return `<div class="event">
        <div class="ev-dot" style="background:${e.color}"></div>
        <div class="ev-time">${t}</div>
        <div class="ev-type" style="color:${e.color}">${e.type}</div>
        <div class="ev-msg">${escHtml(e.msg)}</div>
      </div>`;
    }).join('');
  }).catch(console.error);
}

// ── Ranking ───────────────────────────────────────────────────────────────────
function updateRanking() {
  fetch('/api/ranking').then(r => r.json()).then(traders => {
    const tbody = document.getElementById('ranking-body');
    if (!traders.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="color:var(--muted);padding:20px;text-align:center">Sin datos aún — el ranking se construye con el tiempo</td></tr>';
      return;
    }
    tbody.innerHTML = traders.map((t, i) => {
      const medal = i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1;
      const wr    = t.win_rate;
      const wrCol = wr>=60?'var(--green)':wr>=45?'var(--yellow)':'var(--red)';
      return `<tr>
        <td class="rank-num">${medal}</td>
        <td class="rank-handle">@${t.handle}</td>
        <td class="rank-wr" style="color:${wrCol}">${wr}%
          <div class="wr-bar"><div class="wr-fill" style="width:${wr}%;background:${wrCol}"></div></div>
        </td>
        <td>${t.signals}</td>
        <td style="color:var(--green)">${t.wins}</td>
        <td style="color:var(--red)">${t.losses}</td>
        <td>
          <div style="height:3px;background:var(--bg3);border-radius:2px;width:60px">
            <div style="height:100%;width:${wr}%;background:${wrCol};border-radius:2px"></div>
          </div>
        </td>
      </tr>`;
    }).join('');
  }).catch(console.error);
}

// ── Trades ────────────────────────────────────────────────────────────────────
function updateTrades() {
  fetch('/api/trades').then(r => r.json()).then(rows => {
    const tbody = document.getElementById('trades-body');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="12" style="color:var(--muted);padding:20px;text-align:center">Sin operaciones registradas</td></tr>';
      return;
    }
    tbody.innerHTML = rows.slice(0,50).map(r => {
      const mode  = r['Modo'] || '';
      const modeC = mode==='PAPER'?'mode-paper':mode==='LIVE'?'mode-live':mode==='INSIDER'?'mode-insider':'mode-skip';
      const dir   = r['Direccion']||'';
      const dirC  = dir==='LARGO'?'val-green':'val-red' ;
      return `<tr>
        <td>${r['Fecha']||''}</td>
        <td>${r['Hora UTC']||''}</td>
        <td style="color:var(--muted)">${r['Fuente']||''}</td>
        <td style="font-weight:600">${r['Instrumento']||''}</td>
        <td class="${dirC}" style="font-weight:700">${dir}</td>
        <td>${r['Entrada']||''}</td>
        <td style="color:var(--green)">${r['TP']||''}</td>
        <td style="color:var(--red)">${r['SL']||''}</td>
        <td>$${r['Tamanio USD']||''}</td>
        <td>${r['Leverage']||''}x</td>
        <td class="${modeC}" style="font-weight:700">${mode}</td>
        <td style="color:var(--muted);max-width:200px;overflow:hidden;text-overflow:ellipsis">${r['Estado']||''}</td>
      </tr>`;
    }).join('');
  }).catch(console.error);
}

// ── Clock ─────────────────────────────────────────────────────────────────────
function updateClock() {
  const now = new Date();
  const h = String(now.getUTCHours()).padStart(2,'0');
  const m = String(now.getUTCMinutes()).padStart(2,'0');
  const s = String(now.getUTCSeconds()).padStart(2,'0');
  document.getElementById('clock').textContent = `${h}:${m}:${s} UTC`;
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Init & refresh loop ───────────────────────────────────────────────────────
initChart();
loadChart('BTC', document.querySelector('.chart-tab.active'));

function refreshAll() {
  updateStatus();
  updateFeed();
  updateRanking();
  updateTrades();
}
function refreshPrices() { updatePrices(); }

refreshAll();
refreshPrices();

setInterval(updateClock, 1000);
setInterval(refreshAll, 15000);
setInterval(refreshPrices, 30000);
setInterval(() => loadChart(currentSym), 300000); // chart cada 5 min

// Countdown
let countdown = 15;
setInterval(() => {
  countdown--;
  if (countdown <= 0) countdown = 15;
  document.getElementById('next-refresh').textContent = countdown;
}, 1000);
</script>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(HTML)

if __name__ == "__main__":
    print("Dashboard en: http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
