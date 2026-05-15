"""
Backtest bar-a-bar: Fixed Range Volume Profile (FRVP) Mean Reversion — v3

Filtros balanceados (v2 era demasiado restrictivo):
  1. Sesiones London+NY solo para PAXG/oro; BTC y ETH operan 24/7
  2. Zona mínima: VAH-VAL > MIN_ZONE_PCT del precio (descarta rangos estrechos)
  3. RSI moderado: < 45 LONG / > 55 SHORT
  4. Volumen fuerte: vela señal con volumen > 1.5× avg últimas 20 velas
  5. Max 1 señal activa por instrumento (no solapamiento)

Estrategia base:
  - FRVP sobre últimas LOOKBACK velas 5m (ventana deslizante)
  - LONG : precio toca VAL + filtros → TP=POC, SL bajo VAL
  - SHORT: precio toca VAH + filtros → TP=POC, SL sobre VAH
  - R:R mínimo 1.5

Instrumentos: BTC/USDT, ETH/USDT, PAXG/USDT (proxy oro)
"""

import sys, time, statistics
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import ccxt
from datetime import datetime, timezone, timedelta

SYMBOLS = {
    "BTC":  "BTC/USDT",
    "ETH":  "ETH/USDT",
    "PAXG": "PAXG/USDT",
}

LOOKBACK       = 50      # velas 5m para calcular FRVP
N_BINS         = 200     # bins de precio
VA_PCT         = 0.70    # value area = 70% del volumen
TOL_MULT       = 2.0     # tolerancia = TOL_MULT × bin_size
SL_MULT        = 3.0     # SL = VAL - SL_MULT × bin_size (LONG) o VAH + ... (SHORT)
MIN_RR         = 1.5     # R:R mínimo
MAX_HOLD       = 48      # máx velas (4h en 5m)
RSI_PERIOD     = 14
VOL_AVG_BARS   = 20      # ventana para media de volumen
VOL_MIN_MULT   = 1.5     # volumen señal debe ser > 1.5× media
MIN_ZONE_PCT   = 0.0025  # zona mínima: VAH-VAL > 0.25% del precio
RSI_LONG_MAX   = 45      # RSI máximo para LONG
RSI_SHORT_MIN  = 55      # RSI mínimo para SHORT
DAYS           = 365

# Sesiones en UTC solo para PAXG/oro (BTC/ETH son 24/7)
SESSIONS = [(7, 0, 10, 0), (13, 30, 16, 30)]

ex = ccxt.binance({"options": {"defaultType": "spot"}})


# ── Sesión ───────────────────────────────────────────────────────────────────

def _in_session(ts_ms: int) -> bool:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    tm = dt.hour * 60 + dt.minute
    return any(sh * 60 + sm <= tm < eh * 60 + em for sh, sm, eh, em in SESSIONS)


# ── FRVP ────────────────────────────────────────────────────────────────────

def _frvp(bars: list) -> dict | None:
    lo_all = min(b[3] for b in bars)
    hi_all = max(b[2] for b in bars)
    if hi_all <= lo_all:
        return None

    bin_size = (hi_all - lo_all) / N_BINS
    bins = [0.0] * N_BINS

    for b in bars:
        h, l, v = b[2], b[3], b[5]
        if h <= l or v <= 0:
            continue
        b_lo = max(0, int((l - lo_all) / bin_size))
        b_hi = min(N_BINS - 1, int((h - lo_all) / bin_size))
        n_touched = b_hi - b_lo + 1
        vol_per = v / n_touched
        for i in range(b_lo, b_hi + 1):
            bins[i] += vol_per

    poc_idx  = bins.index(max(bins))
    poc      = lo_all + (poc_idx + 0.5) * bin_size

    # Expandir desde POC hasta capturar VA_PCT del volumen total
    total  = sum(bins)
    target = total * VA_PCT
    lo_idx = hi_idx = poc_idx
    captured = bins[poc_idx]

    while captured < target:
        lo_v = bins[lo_idx - 1] if lo_idx > 0 else 0.0
        hi_v = bins[hi_idx + 1] if hi_idx < N_BINS - 1 else 0.0
        if lo_v == 0 and hi_v == 0:
            break
        if lo_v >= hi_v and lo_idx > 0:
            lo_idx  -= 1
            captured += bins[lo_idx]
        elif hi_idx < N_BINS - 1:
            hi_idx  += 1
            captured += bins[hi_idx]
        elif lo_idx > 0:
            lo_idx  -= 1
            captured += bins[lo_idx]
        else:
            break

    vah = lo_all + (hi_idx + 1) * bin_size
    val = lo_all + lo_idx * bin_size

    return {"poc": poc, "vah": vah, "val": val, "bin_size": bin_size}


# ── RSI ──────────────────────────────────────────────────────────────────────

def _rsi(closes: list, period: int = RSI_PERIOD) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for g, l in zip(gains[period:], losses[period:]):
        ag = (ag * (period - 1) + g) / period
        al = (al * (period - 1) + l) / period
    return 50.0 if al == 0 else 100 - 100 / (1 + ag / al)


# ── Fetch ────────────────────────────────────────────────────────────────────

def fetch_ohlcv(symbol: str, tf: str, days: int) -> list:
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    all_c = []
    while True:
        try:
            batch = ex.fetch_ohlcv(symbol, tf, since=since, limit=1000)
        except Exception as e:
            print(f"  Error {symbol} {tf}: {e}")
            break
        if not batch:
            break
        all_c.extend(batch)
        since = batch[-1][0] + 1
        if len(batch) < 1000:
            break
        time.sleep(0.15)
    return all_c


# ── Backtest ─────────────────────────────────────────────────────────────────

def backtest(name: str, symbol: str, session_only: bool = False):
    print(f"\n{'='*60}")
    print(f"  FRVP Mean Reversion v3 — {name}  ({symbol})")
    print(f"  Lookback={LOOKBACK} | VA={VA_PCT:.0%} | Tol={TOL_MULT}×bin | Min R:R {MIN_RR}")
    ses_txt = "London+NY" if session_only else "24/7"
    print(f"  Filtros: {ses_txt} | Zona>{MIN_ZONE_PCT:.2%} | RSI<{RSI_LONG_MAX}/<{RSI_SHORT_MIN} | Vol>{VOL_MIN_MULT}×avg")
    print(f"{'='*60}")

    print(f"  Descargando 5m ({DAYS}d)...")
    bars = fetch_ohlcv(symbol, "5m", DAYS)
    if len(bars) < LOOKBACK + MAX_HOLD + 10:
        print("  Datos insuficientes")
        return

    print(f"  {len(bars)} velas 5m")

    trades      = []
    in_trade    = False
    trade_end_i = 0

    for i in range(LOOKBACK, len(bars) - MAX_HOLD - 1):
        if in_trade and i < trade_end_i:
            continue
        in_trade = False

        # Filtro 1: sesión (solo London+NY para PAXG; 24/7 para BTC/ETH)
        if session_only and not _in_session(bars[i][0]):
            continue

        window = bars[i - LOOKBACK: i]
        fp = _frvp(window)
        if not fp:
            continue

        poc, vah, val, bsz = fp["poc"], fp["vah"], fp["val"], fp["bin_size"]
        tol   = TOL_MULT * bsz
        price = bars[i][4]

        # Filtro 2: zona mínima VAH-VAL > MIN_ZONE_PCT del precio
        if (vah - val) < price * MIN_ZONE_PCT:
            continue

        closes  = [b[4] for b in window]
        rsi     = _rsi(closes)

        # Filtro 3: RSI moderado + Filtro 4: volumen fuerte
        vol_bar = bars[i][5]
        vol_avg = sum(b[5] for b in bars[i - VOL_AVG_BARS: i]) / VOL_AVG_BARS

        direction = None
        if val - tol <= price <= val + tol and rsi < RSI_LONG_MAX and vol_bar > vol_avg * VOL_MIN_MULT:
            direction = "LONG"
        elif vah - tol <= price <= vah + tol and rsi > RSI_SHORT_MIN and vol_bar > vol_avg * VOL_MIN_MULT:
            direction = "SHORT"
        if not direction:
            continue

        # Puntos de entrada (open siguiente vela), TP, SL
        if i + 1 >= len(bars):
            continue
        entry = bars[i + 1][1]   # open next bar

        if direction == "LONG":
            tp_p = poc
            sl_p = val - SL_MULT * bsz
        else:
            tp_p = poc
            sl_p = vah + SL_MULT * bsz

        risk   = abs(entry - sl_p)
        reward = abs(entry - tp_p)
        if risk <= 0 or reward / risk < MIN_RR:
            continue

        # POC del lado equivocado → descartar
        if direction == "LONG" and tp_p <= entry:
            continue
        if direction == "SHORT" and tp_p >= entry:
            continue

        # Simular resultado bar-a-bar
        result = "TIMEOUT"
        exit_p = entry
        for j in range(i + 1, min(i + 1 + MAX_HOLD, len(bars))):
            h, l = bars[j][2], bars[j][3]
            if direction == "LONG":
                if l <= sl_p:
                    exit_p, result = sl_p, "SL"; break
                if h >= tp_p:
                    exit_p, result = tp_p, "TP"; break
            else:
                if h >= sl_p:
                    exit_p, result = sl_p, "SL"; break
                if l <= tp_p:
                    exit_p, result = tp_p, "TP"; break
        if result == "TIMEOUT":
            exit_p = bars[min(i + MAX_HOLD, len(bars) - 1)][4]

        pnl = (exit_p - entry) / entry * (1 if direction == "LONG" else -1)
        dt  = datetime.fromtimestamp(bars[i][0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

        trades.append({
            "dt": dt, "dir": direction, "result": result,
            "pnl": round(pnl * 100, 4), "entry": round(entry, 4),
            "poc": round(poc, 4), "vah": round(vah, 4), "val": round(val, 4),
        })
        in_trade    = True
        trade_end_i = i + 1 + MAX_HOLD

    if not trades:
        print("  Sin trades generados.")
        return

    wins     = [t for t in trades if t["result"] == "TP"]
    losses   = [t for t in trades if t["result"] == "SL"]
    timeouts = [t for t in trades if t["result"] == "TIMEOUT"]
    pnls     = [t["pnl"] for t in trades]
    wr       = len(wins) / len(trades)
    avg      = statistics.mean(pnls)
    tot      = sum(pnls)
    be_wr    = 1 / (1 + MIN_RR)

    print(f"\n  RESULTADOS ({len(trades)} trades | {DAYS}d):")
    print(f"  Win rate:   {wr:.1%}   (break-even ≈ {be_wr:.0%})")
    print(f"  {len(wins)}W / {len(losses)}L / {len(timeouts)} timeout")
    print(f"  PnL medio:  {avg:+.4f}%")
    print(f"  PnL total:  {tot:+.2f}%")
    print(f"  EV/trade:   {avg:+.4f}%  {'✅' if avg > 0 else '❌'}")

    # Por año
    by_year = {}
    for t in trades:
        y = t["dt"][:4]
        by_year.setdefault(y, []).append(t)
    print(f"\n  Por año:")
    for y, yt in sorted(by_year.items()):
        yw = sum(1 for t in yt if t["result"] == "TP")
        yp = sum(t["pnl"] for t in yt)
        print(f"    {y}: {len(yt):4d} trades | WR {yw/len(yt):.0%} | PnL {yp:+.2f}%")

    # Por dirección
    longs  = [t for t in trades if t["dir"] == "LONG"]
    shorts = [t for t in trades if t["dir"] == "SHORT"]
    lw = sum(1 for t in longs  if t["result"] == "TP")
    sw = sum(1 for t in shorts if t["result"] == "TP")
    print(f"\n  LONGs  ({len(longs):4d}): WR {lw/max(len(longs),1):.0%} | "
          f"PnL {sum(t['pnl'] for t in longs):+.2f}%")
    print(f"  SHORTs ({len(shorts):4d}): WR {sw/max(len(shorts),1):.0%} | "
          f"PnL {sum(t['pnl'] for t in shorts):+.2f}%")

    print(f"\n  {'✅ EDGE POSITIVO — implementar' if avg > 0 else '❌ EV NEGATIVO — ajustar parámetros'}")

    # Sugerencia R:R si WR es buena pero EV negativo
    if wr > 0.50 and avg < 0:
        rr_min = wr / (1 - wr)
        print(f"  💡 WR {wr:.0%} → R:R mínimo rentable ≈ 1:{rr_min:.2f}")

    # Muestra algunos trades de ejemplo
    print(f"\n  Últimos 5 trades:")
    for t in trades[-5:]:
        print(f"    {t['dt']} {t['dir']:5s} @ {t['entry']} "
              f"POC={t['poc']} → {t['result']:7s} {t['pnl']:+.3f}%")


if __name__ == "__main__":
    for name, symbol in SYMBOLS.items():
        try:
            backtest(name, symbol, session_only=(name == "PAXG"))
        except Exception as e:
            print(f"\n  ERROR en {name}: {e}")
        time.sleep(1)
