"""
Backtest FRVP v5 — Regla del 80% + CVD Divergencia

SEÑALES (las que realmente usan traders con 70%+ WR):

  SEÑAL 1: REGLA DEL 80%
  ─────────────────────
  Si el precio ENTRA al value area desde fuera y se queda 2 barras consecutivas
  dentro, tiene ~80% de probabilidad de rotar completamente al OTRO lado del VA.
  (Documentada por Jim Dalton en "Mind Over Markets", usada en CME/CBOT)

    LONG : precio estaba bajo VAL → entra VA → 2 barras dentro → TP=VAH, SL=VAL-buf
    SHORT: precio estaba sobre VAH → entra VA → 2 barras dentro → TP=VAL, SL=VAH+buf

  SEÑAL 2: CVD DIVERGENCIA EN VAH/VAL
  ─────────────────────────────────────
  Precio rompe por debajo de VAL pero el CVD (delta acumulado de compradores-vendedores)
  no lo sigue → los compradores están absorbiendo → rebote inminente.
  CVD aproximado desde OHLCV: delta = vol × (2×close - high - low) / (high - low)

    LONG : precio < VAL + tol Y CVD última barra > CVD de N barras atrás (divergencia)
    SHORT: precio > VAH - tol Y CVD última barra < CVD de N barras atrás

  Ambas señales filtradas por:
    - EMA50 en 15m (tendencia estructural)
    - Primer toque del nivel en la sesión actual (first-touch only)
    - Solo apertura London (07:00-08:30 UTC) y NY (13:30-15:00 UTC)

FRVP anclado a la sesión anterior (ayer) — niveles más respetados que ventana rolling.
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

# ── Parámetros ────────────────────────────────────────────────────────────────
N_BINS         = 300     # resolución del perfil de volumen
VA_PCT         = 0.70    # value area = 70% del volumen
N_CONFIRM      = 2       # barras consecutivas dentro del VA para regla 80%
SL_BUFFER_PCT  = 0.0004  # buffer del SL más allá de VAH/VAL (0.04%)
MIN_RR         = 1.5     # R:R mínimo
MAX_HOLD       = 36      # máx barras (3h en 5m)
EMA_PERIOD     = 50      # EMA50 en 15m para tendencia
CVD_LOOKBACK   = 10      # barras para comparar CVD divergencia
TOL_PCT        = 0.0005  # tolerancia 0.05% para detectar precio "en el nivel"
DAYS           = 365

# Ventanas de sesión UTC: solo primera hora de London y NY
SESSION_WINDOWS = [(7, 0, 8, 30), (13, 30, 15, 0)]

ex = ccxt.binance({"options": {"defaultType": "spot"}})


# ── FRVP ─────────────────────────────────────────────────────────────────────

def _frvp(bars: list) -> dict | None:
    if not bars:
        return None
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
        vol_per = v / (b_hi - b_lo + 1)
        for i in range(b_lo, b_hi + 1):
            bins[i] += vol_per

    poc_idx  = bins.index(max(bins))
    poc      = lo_all + (poc_idx + 0.5) * bin_size
    total    = sum(bins)
    target   = total * VA_PCT
    lo_idx   = hi_idx = poc_idx
    captured = bins[poc_idx]

    while captured < target:
        lo_v = bins[lo_idx - 1] if lo_idx > 0 else 0.0
        hi_v = bins[hi_idx + 1] if hi_idx < N_BINS - 1 else 0.0
        if lo_v == 0 and hi_v == 0:
            break
        if lo_v >= hi_v and lo_idx > 0:
            lo_idx -= 1; captured += bins[lo_idx]
        elif hi_idx < N_BINS - 1:
            hi_idx += 1; captured += bins[hi_idx]
        elif lo_idx > 0:
            lo_idx -= 1; captured += bins[lo_idx]
        else:
            break

    return {
        "poc": poc,
        "vah": lo_all + (hi_idx + 1) * bin_size,
        "val": lo_all + lo_idx * bin_size,
    }


# ── CVD ───────────────────────────────────────────────────────────────────────

def _cvd_series(bars: list) -> list:
    """CVD acumulado: delta = vol × (2×close - high - low) / (high - low)"""
    result, cum = [], 0.0
    for b in bars:
        h, l, c, v = b[2], b[3], b[4], b[5]
        d = v * (2 * c - h - l) / (h - l) if h != l else 0.0
        cum += d
        result.append(cum)
    return result


# ── EMA ───────────────────────────────────────────────────────────────────────

def _ema_last(closes: list, period: int) -> float:
    k = 2 / (period + 1)
    e = closes[0]
    for v in closes[1:]:
        e = v * k + e * (1 - k)
    return e


# ── Sesión ────────────────────────────────────────────────────────────────────

def _in_session(ts_ms: int) -> bool:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    tm = dt.hour * 60 + dt.minute
    return any(sh * 60 + sm <= tm < eh * 60 + em
               for sh, sm, eh, em in SESSION_WINDOWS)

def _day_key(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_ohlcv(symbol: str, tf: str, days: int) -> list:
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    all_c = []
    while True:
        try:
            batch = ex.fetch_ohlcv(symbol, tf, since=since, limit=1000)
        except Exception as e:
            print(f"  Error {symbol} {tf}: {e}"); break
        if not batch: break
        all_c.extend(batch)
        since = batch[-1][0] + 1
        if len(batch) < 1000: break
        time.sleep(0.15)
    return all_c


# ── Backtest ──────────────────────────────────────────────────────────────────

def backtest(name: str, symbol: str):
    print(f"\n{'='*64}")
    print(f"  FRVP v5 — Regla 80% + CVD Divergencia — {name}  ({symbol})")
    print(f"  VA {VA_PCT:.0%} | {N_CONFIRM} barras confirm | SL buf {SL_BUFFER_PCT:.2%} | Min R:R {MIN_RR}")
    print(f"  Sesiones: London 07-08:30 UTC | NY 13:30-15:00 UTC")
    print(f"{'='*64}")

    print(f"  Descargando 5m ({DAYS}d)...")
    bars5  = fetch_ohlcv(symbol, "5m",  DAYS)
    print(f"  Descargando 15m ({DAYS}d)...")
    bars15 = fetch_ohlcv(symbol, "15m", DAYS)

    if len(bars5) < 500 or len(bars15) < EMA_PERIOD + 5:
        print("  Datos insuficientes"); return

    print(f"  {len(bars5)} velas 5m | {len(bars15)} velas 15m")

    # Índice 15m por timestamp
    ts15 = [b[0] for b in bars15]

    # Pre-agrupar 5m por día para calcular FRVP del día anterior
    bars_by_day = {}
    for b in bars5:
        dk = _day_key(b[0])
        bars_by_day.setdefault(dk, []).append(b)

    sorted_days = sorted(bars_by_day.keys())

    trades      = []
    in_trade    = False
    trade_end_i = 0

    # Estado por sesión: first-touch tracking
    touched_val = set()   # "YYYY-MM-DD_session" que ya tocó VAL
    touched_vah = set()   # idem para VAH

    for i in range(N_CONFIRM + CVD_LOOKBACK + 2, len(bars5) - MAX_HOLD - 1):
        if in_trade and i < trade_end_i:
            continue
        in_trade = False

        if not _in_session(bars5[i][0]):
            continue

        # ── FRVP del día anterior ────────────────────────────────────────────
        today = _day_key(bars5[i][0])
        today_idx = sorted_days.index(today) if today in sorted_days else -1
        if today_idx < 1:
            continue
        prev_day = sorted_days[today_idx - 1]
        prev_bars = bars_by_day.get(prev_day, [])
        if len(prev_bars) < 20:
            continue

        fp = _frvp(prev_bars)
        if not fp:
            continue
        poc, vah, val = fp["poc"], fp["vah"], fp["val"]
        va_size = vah - val
        if va_size <= 0:
            continue

        # ── Tendencia: EMA50 en 15m ──────────────────────────────────────────
        ts_now = bars5[i][0]
        idx15  = next((j for j in range(len(ts15) - 1, -1, -1) if ts15[j] <= ts_now), None)
        if idx15 is None or idx15 < EMA_PERIOD:
            continue
        cl15      = [bars15[k][4] for k in range(idx15 - EMA_PERIOD, idx15 + 1)]
        ema50     = _ema_last(cl15, EMA_PERIOD)
        price_now = bars5[i][4]
        trend_up  = price_now > ema50

        # ── CVD en ventana reciente ──────────────────────────────────────────
        cvd_window = bars5[max(0, i - CVD_LOOKBACK - 2): i + 1]
        cvd_vals   = _cvd_series(cvd_window)
        cvd_now    = cvd_vals[-1]
        cvd_prev   = cvd_vals[-CVD_LOOKBACK - 1] if len(cvd_vals) > CVD_LOOKBACK else cvd_vals[0]

        # ──────────────────────────────────────────────────────────────────────
        # SEÑAL 1: REGLA DEL 80%
        # Necesitamos: precio fuera del VA N_CONFIRM+1 barras atrás,
        # luego N_CONFIRM barras consecutivas DENTRO del VA → entrada para rotación
        # ──────────────────────────────────────────────────────────────────────

        window_for_rule = bars5[i - N_CONFIRM - 1: i]
        bar_before      = window_for_rule[0]           # debe estar FUERA del VA
        bars_inside     = window_for_rule[1:]          # deben estar DENTRO

        bb_close = bar_before[4]
        all_inside = all(val <= b[4] <= vah for b in bars_inside)

        direction = None

        # LONG 80%: antes estaba bajo VAL, luego N_CONFIRM barras dentro
        if (bb_close < val
                and all_inside
                and trend_up
                and val <= price_now <= vah):
            # Sesión & first-touch
            sess_key_val = f"{today}_L" if bars5[i][0] < (int(datetime.now(timezone.utc).replace(hour=12, minute=0, second=0).timestamp()) * 1000) else f"{today}_NY"
            if sess_key_val not in touched_val:
                touched_val.add(sess_key_val)
                direction = "LONG_80"
                sl_p = val * (1 - SL_BUFFER_PCT)
                tp_p = vah

        # SHORT 80%: antes estaba sobre VAH, luego N_CONFIRM barras dentro
        elif (bb_close > vah
                and all_inside
                and not trend_up
                and val <= price_now <= vah):
            sess_key_vah = f"{today}_L_vah" if bars5[i][0] < (int(datetime.now(timezone.utc).replace(hour=12, minute=0, second=0).timestamp()) * 1000) else f"{today}_NY_vah"
            if sess_key_vah not in touched_vah:
                touched_vah.add(sess_key_vah)
                direction = "SHORT_80"
                sl_p = vah * (1 + SL_BUFFER_PCT)
                tp_p = val

        # ──────────────────────────────────────────────────────────────────────
        # SEÑAL 2: CVD DIVERGENCIA en VAH/VAL
        # ──────────────────────────────────────────────────────────────────────

        if not direction:
            tol = va_size * TOL_PCT + val * TOL_PCT

            prev_b  = bars5[i - 1]
            curr_b  = bars5[i]
            curr_cl = curr_b[4]
            curr_op = curr_b[1]

            # LONG CVD: precio perforó VAL pero CVD sigue subiendo (absorción)
            if (trend_up
                    and prev_b[3] < val           # wick bajo perforó VAL
                    and curr_cl > val              # cierre de vuelta dentro
                    and curr_cl > curr_op          # vela alcista
                    and cvd_now > cvd_prev         # CVD divergente (compradores absorbiendo)
                    and price_now <= val + va_size * 0.35):  # cerca de VAL, no en medio del VA
                direction = "LONG_CVD"
                sl_p = min(prev_b[3], curr_b[3]) * (1 - SL_BUFFER_PCT)
                tp_p = poc   # TP conservador en POC para CVD (menor que 80% rule)

            # SHORT CVD: precio perforó VAH pero CVD sigue bajando (distribución)
            elif (not trend_up
                    and prev_b[2] > vah
                    and curr_cl < vah
                    and curr_cl < curr_op
                    and cvd_now < cvd_prev
                    and price_now >= vah - va_size * 0.35):
                direction = "SHORT_CVD"
                sl_p = max(prev_b[2], curr_b[2]) * (1 + SL_BUFFER_PCT)
                tp_p = poc

        if not direction:
            continue

        entry  = bars5[i][4]
        risk   = abs(entry - sl_p)
        reward = abs(tp_p - entry)

        if risk <= 0 or reward / risk < MIN_RR:
            continue

        is_long = direction.startswith("LONG")
        if is_long  and tp_p <= entry: continue
        if not is_long and tp_p >= entry: continue

        # Simular resultado
        result = "TIMEOUT"
        exit_p = entry
        for j in range(i + 1, min(i + 1 + MAX_HOLD, len(bars5))):
            h, l = bars5[j][2], bars5[j][3]
            if is_long:
                if l <= sl_p: exit_p, result = sl_p, "SL"; break
                if h >= tp_p: exit_p, result = tp_p, "TP"; break
            else:
                if h >= sl_p: exit_p, result = sl_p, "SL"; break
                if l <= tp_p: exit_p, result = tp_p, "TP"; break
        if result == "TIMEOUT":
            exit_p = bars5[min(i + MAX_HOLD, len(bars5) - 1)][4]

        pnl    = (exit_p - entry) / entry * (1 if is_long else -1)
        rr_act = round(reward / risk, 2)
        dt     = datetime.fromtimestamp(bars5[i][0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

        trades.append({
            "dt": dt, "dir": direction, "result": result,
            "pnl": round(pnl * 100, 4), "rr": rr_act,
            "entry": round(entry, 4),
            "tp": round(tp_p, 4), "sl": round(sl_p, 4),
        })
        in_trade    = True
        trade_end_i = i + 1 + MAX_HOLD

    if not trades:
        print("  Sin trades generados."); return

    wins     = [t for t in trades if t["result"] == "TP"]
    losses   = [t for t in trades if t["result"] == "SL"]
    timeouts = [t for t in trades if t["result"] == "TIMEOUT"]
    pnls     = [t["pnl"] for t in trades]
    wr       = len(wins) / len(trades)
    avg      = statistics.mean(pnls)
    tot      = sum(pnls)
    avg_rr   = statistics.mean(t["rr"] for t in trades)
    be_wr    = 1 / (1 + MIN_RR)

    # Por señal
    rule80 = [t for t in trades if "80"  in t["dir"]]
    cvd_t  = [t for t in trades if "CVD" in t["dir"]]

    print(f"\n  RESULTADOS ({len(trades)} trades | {DAYS}d):")
    print(f"  Win rate:   {wr:.1%}   (break-even ≈ {be_wr:.0%} con R:R {MIN_RR})")
    print(f"  {len(wins)}W / {len(losses)}L / {len(timeouts)} timeout")
    print(f"  R:R medio real: {avg_rr:.2f}")
    print(f"  PnL medio:  {avg:+.4f}%")
    print(f"  PnL total:  {tot:+.2f}%")
    print(f"  EV/trade:   {avg:+.4f}%  {'✅' if avg > 0 else '❌'}")

    if rule80:
        r80_wr = sum(1 for t in rule80 if t["result"] == "TP") / len(rule80)
        r80_pnl = sum(t["pnl"] for t in rule80)
        print(f"\n  Regla 80%  ({len(rule80):3d} trades): WR {r80_wr:.0%} | PnL {r80_pnl:+.2f}%")
    if cvd_t:
        cvd_wr  = sum(1 for t in cvd_t  if t["result"] == "TP") / len(cvd_t)
        cvd_pnl = sum(t["pnl"] for t in cvd_t)
        print(f"  CVD Diverg ({len(cvd_t):3d} trades): WR {cvd_wr:.0%} | PnL {cvd_pnl:+.2f}%")

    by_year = {}
    for t in trades:
        by_year.setdefault(t["dt"][:4], []).append(t)
    print(f"\n  Por año:")
    for y, yt in sorted(by_year.items()):
        yw  = sum(1 for t in yt if t["result"] == "TP")
        yp  = sum(t["pnl"] for t in yt)
        print(f"    {y}: {len(yt):4d} trades | WR {yw/len(yt):.0%} | PnL {yp:+.2f}%")

    longs  = [t for t in trades if t["dir"].startswith("LONG")]
    shorts = [t for t in trades if t["dir"].startswith("SHORT")]
    lw = sum(1 for t in longs  if t["result"] == "TP")
    sw = sum(1 for t in shorts if t["result"] == "TP")
    print(f"\n  LONGs  ({len(longs):4d}): WR {lw/max(len(longs),1):.0%} | "
          f"PnL {sum(t['pnl'] for t in longs):+.2f}%")
    print(f"  SHORTs ({len(shorts):4d}): WR {sw/max(len(shorts),1):.0%} | "
          f"PnL {sum(t['pnl'] for t in shorts):+.2f}%")

    verdict = ("✅ EDGE SÓLIDO"    if avg > 0 and wr >= 0.55 else
               "✅ EDGE POSITIVO"  if avg > 0 and wr >= 0.40 else
               "⚠️  EDGE MARGINAL"  if avg > 0 else
               "❌ EV NEGATIVO")
    print(f"\n  {verdict}  WR {wr:.1%} | EV {avg:+.4f}%/trade")

    print(f"\n  Últimos 6 trades:")
    for t in trades[-6:]:
        print(f"    {t['dt']} {t['dir']:9s} @ {t['entry']} "
              f"SL={t['sl']} TP={t['tp']} R:R={t['rr']} → {t['result']:7s} {t['pnl']:+.3f}%")


if __name__ == "__main__":
    for name, symbol in SYMBOLS.items():
        try:
            backtest(name, symbol)
        except Exception as e:
            import traceback
            print(f"\n  ERROR en {name}: {e}")
            traceback.print_exc()
        time.sleep(1)
