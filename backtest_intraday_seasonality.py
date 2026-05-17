"""
Backtest: BTC Intraday Hour Seasonality
Fuente: QuantPedia — "The Seasonality of Bitcoin" (2015-2023)

Concepto:
  El NYSE cierra a las 21:00 UTC (16:00 ET). Esto retira la fuente de
  liquidez dominante del mercado, creando un patrón de precio identificable.
  Estrategia documentada: LONG BTC a las 21:00 UTC, cierre a las 23:00 UTC.
  - Retorno anual documentado: 37-40%
  - Max Drawdown documentado: -22.7% (vs -70% buy-and-hold BTC)
  - Calmar ratio: 1.79-1.97

Variantes testeadas:
  1. Base: cada día 21h→23h
  2. Con filtro de alta volatilidad (ATR > media): solo operar días volátiles
  3. Con filtro tendencia (EMA50 diaria): solo longs si precio > EMA50
  4. Short inverso: los peores horarios (ventana negativa)

Timeframe: 1h bars
"""

import sys, math, statistics, time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import ccxt
from datetime import datetime, timezone, timedelta

ex = ccxt.binance({"options": {"defaultType": "spot"}})

DAYS = 730  # 2 años


def fetch(symbol: str, days: int) -> list:
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    out = []
    while True:
        try:
            batch = ex.fetch_ohlcv(symbol, "1h", since=since, limit=1000)
        except Exception as e:
            print(f"  Error {symbol}: {e}"); break
        if not batch: break
        out.extend(batch)
        since = batch[-1][0] + 1
        if len(batch) < 1000: break
        time.sleep(0.15)
    return out


def _ema(series: list, period: int) -> list:
    k = 2 / (period + 1)
    ema = [series[0]]
    for v in series[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def _atr(bars: list, period: int = 14) -> list:
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i][2], bars[i][3], bars[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    # simple average ATR
    atrs = []
    for i in range(len(trs)):
        window = trs[max(0, i - period + 1): i + 1]
        atrs.append(statistics.mean(window))
    return atrs  # length = len(bars) - 1


# ── Backtest por ventana horaria ──────────────────────────────────────────────

def backtest_window(bars: list, entry_hour: int, exit_hour: int,
                    trend_filter: bool = False,
                    vol_filter: bool = False) -> dict:
    """
    Compra al OPEN de la vela entry_hour UTC.
    Cierra al CLOSE de la vela (exit_hour - 1) UTC
    (equivale al open de exit_hour, que es el close de la anterior).
    """
    # Indexar por timestamp
    bar_map = {b[0]: b for b in bars}
    ts_list  = sorted(bar_map.keys())

    # EMA50 diaria (usando cierres horarios, aproximado)
    closes = [bar_map[t][4] for t in ts_list]
    ema50  = _ema(closes, 50 * 24)  # EMA50 en barras diarias ≈ 1200h

    # ATR 14 días (en barras horarias = 336)
    atr_vals = _atr(bars, period=14 * 24)
    atr_mean = statistics.mean(atr_vals) if atr_vals else 1.0

    trades = []
    for idx, ts in enumerate(ts_list):
        bar = bar_map[ts]
        dt  = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

        if dt.hour != entry_hour:
            continue

        # Buscar vela de cierre (exit_hour - 1)
        hold_h = exit_hour - entry_hour if exit_hour > entry_hour else (24 - entry_hour + exit_hour)
        exit_ts = ts + (hold_h * 3_600_000)
        if exit_ts not in bar_map:
            continue

        exit_bar = bar_map[exit_ts]

        # Filtro tendencia: precio > EMA50 al momento de entrada
        if trend_filter and closes[idx] < ema50[idx]:
            continue

        # Filtro volatilidad: ATR de hoy > media
        if vol_filter and idx > 0 and atr_vals[idx - 1] < atr_mean:
            continue

        entry_price = bar[1]    # open de la vela de entrada
        exit_price  = exit_bar[1]  # open de la vela de salida = cierre efectivo

        pnl = (exit_price - entry_price) / entry_price * 100
        trades.append({
            "dt":  dt.strftime("%Y-%m-%d"),
            "pnl": round(pnl, 4),
            "entry": entry_price,
            "exit":  exit_price,
        })

    if not trades:
        return {}

    pnls   = [t["pnl"] for t in trades]
    wins   = sum(1 for p in pnls if p > 0)
    avg    = statistics.mean(pnls)
    tot    = sum(pnls)
    sharpe = (avg / statistics.stdev(pnls) * math.sqrt(365)) if len(pnls) > 1 else 0

    equity = [0.0]
    for p in pnls:
        equity.append(equity[-1] + p)
    peak   = equity[0]
    max_dd = 0.0
    for v in equity:
        peak   = max(peak, v)
        max_dd = min(max_dd, v - peak)

    return {
        "n": len(trades), "wr": wins / len(trades),
        "avg": avg, "tot": tot, "sharpe": sharpe, "max_dd": max_dd,
        "trades": trades,
    }


def print_r(r: dict, label: str):
    if not r:
        print(f"  {label:50s} | Sin datos")
        return
    verdict = ("✅ SÓLIDO"   if r["avg"] > 0 and r["wr"] >= 0.55 and r["sharpe"] >= 1.5 else
               "✅ POSITIVO" if r["avg"] > 0 and r["wr"] >= 0.50 else
               "⚠️  MARGINAL" if r["avg"] > 0 else "❌ NEGATIVO")
    print(f"  {label:50s} | {r['n']:4d} ops | WR {r['wr']:.0%} | "
          f"EV {r['avg']:+.4f}% | Total {r['tot']:+.1f}% | "
          f"Sharpe {r['sharpe']:.2f} | DD {r['max_dd']:+.1f}%  {verdict}")


if __name__ == "__main__":
    print("\nDescargando datos BTC 2 años (1h)...")
    bars = fetch("BTC/USDT", DAYS)
    print(f"  {len(bars)} barras\n")

    print("=" * 115)
    print("  ESTRATEGIA: BTC INTRADAY HOUR SEASONALITY  (fuente: QuantPedia)")
    print("=" * 115)

    # ── 1. Ventana documentada: 21h → 23h ────────────────────────────────────
    print("\n--- Ventana documentada: LONG 21:00→23:00 UTC ---")
    r = backtest_window(bars, 21, 23)
    print_r(r, "21h→23h base")
    r_tf = backtest_window(bars, 21, 23, trend_filter=True)
    print_r(r_tf, "21h→23h + filtro EMA50")
    r_vf = backtest_window(bars, 21, 23, vol_filter=True)
    print_r(r_vf, "21h→23h + filtro alta volatilidad")
    r_both = backtest_window(bars, 21, 23, trend_filter=True, vol_filter=True)
    print_r(r_both, "21h→23h + EMA50 + alta volatilidad")

    # ── 2. Scan todas las ventanas de 2h para encontrar la mejor ─────────────
    print("\n--- Scan completo: todas las ventanas de 2h ---")
    best = None
    best_label = ""
    for h in range(24):
        exit_h = (h + 2) % 24
        r = backtest_window(bars, h, exit_h)
        label = f"{h:02d}h→{exit_h:02d}h"
        print_r(r, label)
        if r and r["avg"] > 0 and (best is None or r["sharpe"] > best["sharpe"]):
            best = r
            best_label = label

    # ── 3. Ventanas de 3h ─────────────────────────────────────────────────────
    print("\n--- Scan: ventanas de 3h ---")
    for h in range(0, 24, 3):
        exit_h = (h + 3) % 24
        r = backtest_window(bars, h, exit_h)
        print_r(r, f"{h:02d}h→{exit_h:02d}h (3h hold)")

    # ── 4. Resultado óptimo ───────────────────────────────────────────────────
    if best:
        print(f"\n{'='*115}")
        print(f"  MEJOR VENTANA DE 2H: {best_label}")
        print(f"{'='*115}")
        pnls = [t["pnl"] for t in best["trades"]]
        wins = [t for t in best["trades"] if t["pnl"] > 0]
        loss = [t for t in best["trades"] if t["pnl"] <= 0]
        print(f"  Total ops: {best['n']} | WR {best['wr']:.1%} | EV {best['avg']:+.4f}%/op")
        if wins:  print(f"  Avg win:  {statistics.mean(t['pnl'] for t in wins):+.4f}%")
        if loss:  print(f"  Avg loss: {statistics.mean(t['pnl'] for t in loss):+.4f}%")
        print(f"  Total acumulado: {best['tot']:+.1f}%")
        print(f"  Sharpe anualizado: {best['sharpe']:.2f}")
        print(f"  Max Drawdown: {best['max_dd']:+.2f}%")

        by_year = {}
        for t in best["trades"]:
            by_year.setdefault(t["dt"][:4], []).append(t["pnl"])
        print(f"\n  Por año:")
        for y, yp in sorted(by_year.items()):
            yw = sum(1 for p in yp if p > 0)
            print(f"    {y}: {len(yp):4d} ops | WR {yw/len(yp):.0%} | Total {sum(yp):+.1f}%")

        # Con filtros sobre la mejor ventana
        print(f"\n  Con filtros sobre {best_label}:")
        h_in  = int(best_label[:2])
        h_out = int(best_label[4:6])
        for tf, vf, lbl in [(True, False, "+ EMA50"), (False, True, "+ alta vol"),
                             (True, True, "+ EMA50 + alta vol")]:
            rb = backtest_window(bars, h_in, h_out, trend_filter=tf, vol_filter=vf)
            print_r(rb, lbl)
