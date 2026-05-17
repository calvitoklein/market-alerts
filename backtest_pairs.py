"""
Backtest: BTC/ETH Statistical Pairs Trading

Concepto:
  BTC y ETH están cointegrados a largo plazo (se mueven juntos).
  Cuando el spread log(BTC/ETH) se aleja de su media histórica (Z-score alto),
  revertirá. Aprovechamos esa reversión:

  Z > +ENTRY_Z  → BTC caro vs ETH  → SHORT BTC + LONG  ETH
  Z < -ENTRY_Z  → BTC barato vs ETH → LONG  BTC + SHORT ETH

  Salida cuando el Z-score vuelve a EXIT_Z (cerca de la media).

  Ventajas sobre estrategias direccionales:
  - Market neutral: no depende de si el mercado sube o baja
  - Sharpe documentado: 2.45 / WR documentado: 64.7%
  - Funciona 100% desde OHLCV, sin tick data

Parámetros explorados en sweep: LOOKBACK, ENTRY_Z, EXIT_Z
Timeframe: 1h (más limpio que 5m para spreads)
"""

import sys, time, math, statistics
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import ccxt
from datetime import datetime, timezone, timedelta

ex = ccxt.binance({"options": {"defaultType": "spot"}})

DAYS        = 730    # 2 años para tener muestra estadística sólida
MAX_HOLD    = 168    # máx barras a mantener (168h = 1 semana)


# ── Fetch ─────────────────────────────────────────────────────────────────────

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


# ── Spread & Z-score ──────────────────────────────────────────────────────────

def _log_spread(btc_p: float, eth_p: float) -> float:
    return math.log(btc_p) - math.log(eth_p)

def _zscore(series: list) -> float:
    """Z-score del último elemento respecto a la media/std de la serie."""
    if len(series) < 5:
        return 0.0
    mu  = statistics.mean(series)
    sig = statistics.stdev(series)
    return (series[-1] - mu) / sig if sig > 0 else 0.0


# ── Backtest ──────────────────────────────────────────────────────────────────

def backtest(lookback: int, entry_z: float, exit_z: float,
             btc_bars: list, eth_bars: list, verbose: bool = True) -> dict:
    """
    Simula el pairs trade bar a bar.
    Retorna dict con métricas.
    """
    # Alinear por timestamp (inner join)
    btc_map = {b[0]: b[4] for b in btc_bars}
    eth_map = {b[0]: b[4] for b in eth_bars}
    ts_common = sorted(set(btc_map) & set(eth_map))

    if len(ts_common) < lookback + MAX_HOLD + 10:
        return {}

    btc_prices = [btc_map[t] for t in ts_common]
    eth_prices = [eth_map[t] for t in ts_common]
    spreads    = [_log_spread(b, e) for b, e in zip(btc_prices, eth_prices)]

    trades      = []
    in_trade    = False
    trade_end_i = 0
    direction   = None   # "BTC_OVER" o "ETH_OVER"
    entry_btc   = entry_eth = 0.0
    entry_i     = 0

    for i in range(lookback, len(ts_common) - 1):
        if in_trade:
            if i >= trade_end_i:
                # Timeout forzado
                result = "TIMEOUT"
                _close(trades, direction, entry_btc, entry_eth,
                       btc_prices[i], eth_prices[i], result,
                       ts_common[entry_i], i - entry_i)
                in_trade = False
            else:
                # Evaluar salida
                z_now = _zscore(spreads[i - lookback: i + 1])
                close_now = False
                if direction == "BTC_OVER" and z_now <= exit_z:
                    close_now = True
                elif direction == "ETH_OVER" and z_now >= -exit_z:
                    close_now = True

                if close_now:
                    _close(trades, direction, entry_btc, entry_eth,
                           btc_prices[i], eth_prices[i], "EXIT_Z",
                           ts_common[entry_i], i - entry_i)
                    in_trade = False
            continue

        # Evaluar entrada
        window  = spreads[i - lookback: i + 1]
        z       = _zscore(window)

        if z > entry_z:       # BTC caro vs ETH → SHORT BTC + LONG ETH
            direction = "BTC_OVER"
            entry_btc, entry_eth = btc_prices[i], eth_prices[i]
            entry_i       = i
            in_trade      = True
            trade_end_i   = i + MAX_HOLD
        elif z < -entry_z:    # ETH caro vs BTC → LONG BTC + SHORT ETH
            direction = "ETH_OVER"
            entry_btc, entry_eth = btc_prices[i], eth_prices[i]
            entry_i       = i
            in_trade      = True
            trade_end_i   = i + MAX_HOLD

    if not trades:
        return {}

    pnls   = [t["pnl"] for t in trades]
    wins   = sum(1 for t in trades if t["pnl"] > 0)
    wr     = wins / len(trades)
    avg    = statistics.mean(pnls)
    tot    = sum(pnls)
    holds  = [t["bars"] for t in trades]

    # Sharpe anualizado (1h bars → 8760 bars/año)
    sharpe = (avg / statistics.stdev(pnls) * math.sqrt(8760)) if len(pnls) > 1 else 0

    # Max drawdown en equity curve
    equity = [0.0]
    for t in trades:
        equity.append(equity[-1] + t["pnl"])
    peak   = equity[0]
    max_dd = 0.0
    for v in equity:
        peak   = max(peak, v)
        max_dd = min(max_dd, v - peak)

    return {
        "lookback": lookback, "entry_z": entry_z, "exit_z": exit_z,
        "n": len(trades), "wr": wr, "avg": avg, "tot": tot,
        "sharpe": sharpe, "max_dd": max_dd,
        "avg_hold": statistics.mean(holds),
    }


def _close(trades: list, direction: str, entry_btc: float, entry_eth: float,
           exit_btc: float, exit_eth: float, result: str,
           ts_entry: int, bars_held: int):
    """
    PnL combinado del par (dollar-neutral, $1 en cada pierna):
    BTC_OVER: SHORT BTC + LONG ETH
      pnl = -(exit_btc/entry_btc - 1) + (exit_eth/entry_eth - 1)
           = ETH_return - BTC_return
    ETH_OVER: LONG BTC + SHORT ETH
      pnl = (exit_btc/entry_btc - 1) - (exit_eth/entry_eth - 1)
           = BTC_return - ETH_return
    """
    btc_ret = exit_btc / entry_btc - 1
    eth_ret = exit_eth / entry_eth - 1

    if direction == "BTC_OVER":
        pnl = eth_ret - btc_ret    # queremos ETH suba más / BTC baje más
    else:
        pnl = btc_ret - eth_ret

    dt = datetime.fromtimestamp(ts_entry / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    trades.append({
        "dt": dt, "dir": direction, "result": result,
        "pnl": round(pnl * 100, 4), "bars": bars_held,
    })


# ── Parameter sweep ───────────────────────────────────────────────────────────

def print_result(r: dict, label: str = ""):
    if not r:
        print("  Sin datos")
        return
    verdict = ("✅ SÓLIDO"   if r["avg"] > 0 and r["wr"] >= 0.55 and r["sharpe"] >= 1.5 else
               "✅ POSITIVO" if r["avg"] > 0 and r["wr"] >= 0.45 else
               "⚠️  MARGINAL" if r["avg"] > 0 else "❌ NEGATIVO")
    print(f"  {label:40s} | {r['n']:4d} trades | WR {r['wr']:.0%} | "
          f"EV {r['avg']:+.3f}% | PnL {r['tot']:+.1f}% | "
          f"Sharpe {r['sharpe']:.2f} | DD {r['max_dd']:+.1f}% | "
          f"Hold {r['avg_hold']:.0f}h  {verdict}")


if __name__ == "__main__":
    print("\nDescargando datos (2 años, 1h)...")
    btc = fetch("BTC/USDT", DAYS)
    eth = fetch("ETH/USDT", DAYS)
    print(f"  BTC: {len(btc)} barras 1h | ETH: {len(eth)} barras 1h\n")

    # ── Mejor configuración primero ───────────────────────────────────────────
    print("=" * 100)
    print("  SWEEP: LOOKBACK × ENTRY_Z × EXIT_Z")
    print("=" * 100)

    best = None
    results = []

    for lookback in [100, 200, 500]:
        for entry_z in [1.5, 2.0, 2.5]:
            for exit_z in [0.0, 0.3, 0.5]:
                label = f"LB={lookback:3d} Ez={entry_z:.1f} Xz={exit_z:.1f}"
                r = backtest(lookback, entry_z, exit_z, btc, eth, verbose=False)
                if r:
                    results.append(r)
                    print_result(r, label)
                    if best is None or (r["avg"] > 0 and r["sharpe"] > best.get("sharpe", 0)):
                        best = r

    # ── Resultado óptimo detallado ────────────────────────────────────────────
    if best:
        print(f"\n{'='*100}")
        print(f"  MEJOR CONFIGURACIÓN: LB={best['lookback']} "
              f"Ez={best['entry_z']} Xz={best['exit_z']}")
        print(f"{'='*100}")

        lb, ez, xz = best["lookback"], best["entry_z"], best["exit_z"]

        # Re-run con detalle por año
        btc_map = {b[0]: b[4] for b in btc}
        eth_map = {b[0]: b[4] for b in eth}
        ts_c    = sorted(set(btc_map) & set(eth_map))
        btc_p   = [btc_map[t] for t in ts_c]
        eth_p   = [eth_map[t] for t in ts_c]
        spreads = [_log_spread(b, e) for b, e in zip(btc_p, eth_p)]

        trades      = []
        in_trade    = False
        trade_end_i = 0
        direction   = None
        entry_btc   = entry_eth = 0.0
        entry_i     = 0

        for i in range(lb, len(ts_c) - 1):
            if in_trade:
                if i >= trade_end_i:
                    _close(trades, direction, entry_btc, entry_eth,
                           btc_p[i], eth_p[i], "TIMEOUT", ts_c[entry_i], i - entry_i)
                    in_trade = False
                else:
                    z_now = _zscore(spreads[i - lb: i + 1])
                    if ((direction == "BTC_OVER" and z_now <= xz) or
                            (direction == "ETH_OVER" and z_now >= -xz)):
                        _close(trades, direction, entry_btc, entry_eth,
                               btc_p[i], eth_p[i], "EXIT_Z", ts_c[entry_i], i - entry_i)
                        in_trade = False
                continue

            w = spreads[i - lb: i + 1]
            z = _zscore(w)
            if z > ez:
                direction = "BTC_OVER"; entry_btc, entry_eth = btc_p[i], eth_p[i]
                entry_i = i; in_trade = True; trade_end_i = i + MAX_HOLD
            elif z < -ez:
                direction = "ETH_OVER"; entry_btc, entry_eth = btc_p[i], eth_p[i]
                entry_i = i; in_trade = True; trade_end_i = i + MAX_HOLD

        if trades:
            wins = [t for t in trades if t["pnl"] > 0]
            loss = [t for t in trades if t["pnl"] <= 0]
            pnls = [t["pnl"] for t in trades]

            print(f"\n  Total trades: {len(trades)} | "
                  f"WR {len(wins)/len(trades):.1%} | "
                  f"EV {statistics.mean(pnls):+.3f}%/trade")
            print(f"  Avg win: {statistics.mean(t['pnl'] for t in wins):+.3f}%  |  "
                  f"Avg loss: {statistics.mean(t['pnl'] for t in loss):+.3f}%")
            print(f"  Sharpe anualizado: {best['sharpe']:.2f}")
            print(f"  Max drawdown: {best['max_dd']:+.2f}%")
            print(f"  Hold medio: {best['avg_hold']:.0f}h")

            by_dir = {}
            for t in trades:
                by_dir.setdefault(t["dir"], []).append(t)
            print(f"\n  Por dirección:")
            for d, dt in by_dir.items():
                dw = sum(1 for t in dt if t["pnl"] > 0)
                dp = sum(t["pnl"] for t in dt)
                print(f"    {d:12s}: {len(dt):3d} trades | WR {dw/len(dt):.0%} | PnL {dp:+.2f}%")

            by_year = {}
            for t in trades:
                by_year.setdefault(t["dt"][:4], []).append(t)
            print(f"\n  Por año:")
            for y, yt in sorted(by_year.items()):
                yw = sum(1 for t in yt if t["pnl"] > 0)
                yp = sum(t["pnl"] for t in yt)
                print(f"    {y}: {len(yt):4d} trades | WR {yw/len(yt):.0%} | PnL {yp:+.2f}%")

            print(f"\n  Últimos 8 trades:")
            for t in trades[-8:]:
                print(f"    {t['dt']} {t['dir']:12s} → {t['result']:7s} "
                      f"{t['pnl']:+.3f}%  ({t['bars']}h)")

            verdict = ("✅ IMPLEMENTAR — edge sólido y consistente"
                       if best["avg"] > 0 and best["wr"] >= 0.55 and best["sharpe"] >= 1.5
                       else "✅ IMPLEMENTAR — EV positivo"
                       if best["avg"] > 0
                       else "❌ No implementar")
            print(f"\n  {verdict}")
