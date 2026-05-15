"""
GreenMood Strategy — inspirada en el trader #7 de Lucid Trading
Stats reales: 83.6% WR | 12,737 trades | $150,376 ganancia | PF 1.23

Decodificando los stats:
  - Avg win $78 / avg loss $288 = ratio 1:3.67 → TP ajustado, SL amplio
  - 83.6% WR + alta frecuencia → scalper de momentum con filtros muy selectivos
  - GreenMood en TradingView usa MACD Zero Lag → señal principal inferida

Implementación:
  - MACD Zero Lag (12/26/9) en 5m para momentum
  - RSI 14 como filtro de extremos
  - EMA 50 en 15m para tendencia estructural
  - TP = 0.4 × ATR (pequeño, alta WR), SL = 1.5 × ATR (amplio)
  - Sesiones: 24/7 (BTC/ETH) — a diferencia de XSignals que solo London/NY
  - Solo entra cuando hay confirmación de 3 condiciones simultáneas
"""

import os
import hashlib
from datetime import datetime, timezone

DIR    = os.path.dirname(os.path.abspath(__file__))
HANDLE = "GreenMoodBot"
NAME   = "GreenMood Strategy"

# Instrumentos: BTC y ETH 24/7 para complementar XSignals (Gold London/NY)
PAIRS = [
    {"symbol": "BTC/USDT:USDT", "instr": "BTC", "atr_mult_tp": 1.5, "atr_mult_sl": 1.5},
    {"symbol": "ETH/USDT:USDT", "instr": "ETH", "atr_mult_tp": 1.5, "atr_mult_sl": 1.5},
]

MACD_FAST   = 12
MACD_SLOW   = 26
MACD_SIGNAL = 9
RSI_PERIOD  = 14
EMA_TREND   = 50
MIN_BARS    = 60   # mínimo de velas 5m para calcular indicadores


# ── Indicadores ───────────────────────────────────────────────────────────────

def _ema(values: list, period: int) -> list:
    k   = 2 / (period + 1)
    ema = values[0]
    out = [ema]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
        out.append(ema)
    return out

def _zero_lag_ema(values: list, period: int) -> list:
    """ZL-EMA = EMA + (EMA − EMA(EMA))  — elimina el lag del EMA estándar."""
    e1 = _ema(values, period)
    e2 = _ema(e1, period)
    return [a + (a - b) for a, b in zip(e1, e2)]

def _macd_zl(closes: list):
    """Retorna (macd, signal_line, histogram) con Zero Lag EMA."""
    zl_fast = _zero_lag_ema(closes, MACD_FAST)
    zl_slow = _zero_lag_ema(closes, MACD_SLOW)
    macd    = [f - s for f, s in zip(zl_fast, zl_slow)]
    sig     = _zero_lag_ema(macd, MACD_SIGNAL)
    hist    = [m - s for m, s in zip(macd, sig)]
    return macd, sig, hist

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
    if al == 0:
        return 100.0
    return round(100 - 100 / (1 + ag / al), 2)

def _atr(bars: list, period: int = 14) -> float:
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i][2], bars[i][3], bars[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


# ── Exchange ──────────────────────────────────────────────────────────────────

def _get_exchange():
    env = {}
    path = os.path.join(DIR, ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip()
    try:
        import ccxt
        cls = getattr(ccxt, env.get("EXCHANGE_ID", "bitget"))
        return cls({
            "apiKey":   env.get("EXCHANGE_API_KEY",    ""),
            "secret":   env.get("EXCHANGE_API_SECRET",  ""),
            "password": env.get("EXCHANGE_PASSPHRASE",  ""),
            "options":  {"defaultType": "swap"},
        })
    except Exception:
        return None

def _ohlcv(ex, symbol: str, tf: str, limit: int) -> list:
    try:
        return ex.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
    except Exception:
        return []


# ── Señal principal ───────────────────────────────────────────────────────────

def _signal_for_pair(ex, pair: dict, now: datetime) -> dict | None:
    symbol = pair["symbol"]
    instr  = pair["instr"]

    bars5  = _ohlcv(ex, symbol, "5m",  MIN_BARS)
    bars15 = _ohlcv(ex, symbol, "15m", EMA_TREND + 10)

    if len(bars5) < MIN_BARS or len(bars15) < EMA_TREND + 5:
        return None

    closes5  = [b[4] for b in bars5]
    closes15 = [b[4] for b in bars15]
    price    = closes5[-1]

    # Tendencia estructural (15m) — solo operar a favor
    ema50_15 = _ema(closes15, EMA_TREND)
    trend_up = closes15[-1] > ema50_15[-1]

    # MACD Zero Lag en 5m
    _, _, hist = _macd_zl(closes5)
    if len(hist) < 3:
        return None

    # Cruce del histograma: negativo→positivo (LONG) o positivo→negativo (SHORT)
    cross_bull = hist[-2] < 0 and hist[-1] > 0
    cross_bear = hist[-2] > 0 and hist[-1] < 0

    # RSI como filtro de extremos
    rsi = _rsi(closes5)

    # ATR para sizing de TP/SL
    atr = _atr(bars5)
    if atr <= 0:
        return None

    tp_dist = round(atr * pair["atr_mult_tp"], 2)
    sl_dist = round(atr * pair["atr_mult_sl"], 2)

    # Requiere mínimo de movimiento razonable
    if tp_dist < price * 0.001 or sl_dist < price * 0.002:
        return None

    direction = None
    if cross_bull and trend_up and rsi < 65:
        direction = "LARGO"
    elif cross_bear and not trend_up and rsi > 35:
        direction = "CORTO"

    if not direction:
        return None

    entrada = round(price, 2)
    if direction == "LARGO":
        sl = round(entrada - sl_dist, 2)
        tp = round(entrada + tp_dist, 2)
    else:
        sl = round(entrada + sl_dist, 2)
        tp = round(entrada - tp_dist, 2)

    rr = round(tp_dist / sl_dist, 2)

    sig_id = "gm_" + hashlib.md5(
        f"{instr}_{direction}_{now.strftime('%Y%m%d%H%M')}".encode()
    ).hexdigest()[:12]

    macd_str = f"{hist[-1]:.4f}"
    return {
        "ts":               now.isoformat(),
        "id":               sig_id,
        "handle":           HANDLE,
        "name":             NAME,
        "link":             "https://www.tradingview.com/u/GreenMood/",
        "img":              "",
        "text": (
            f"[GreenMood MACD-ZL] {instr} {direction} @ {entrada} | "
            f"SL {sl} | TP {tp} | RR 1:{1/rr:.1f} | RSI {rsi} | MACD hist {macd_str}"
        ),
        "INSTRUMENTO":      instr,
        "DIRECCION":        direction,
        "ENTRADA":          str(entrada),
        "TP":               str(tp),
        "SL":               str(sl),
        "CONFIANZA_TRADER": "ALTA",
        "HORIZONTE":        "SCALP",
        "RESUMEN":          f"GreenMood MACD-ZL {direction} {instr}",
    }


def generate_signals() -> list:
    """
    Genera señales GreenMood (MACD Zero Lag) para BTC y ETH.
    Activo 24/7 — complementa XSignals que solo opera London/NY en Gold.
    """
    now = datetime.now(timezone.utc)
    ex  = _get_exchange()
    if not ex:
        return []

    results = []
    for pair in PAIRS:
        try:
            sig = _signal_for_pair(ex, pair, now)
            if sig:
                results.append(sig)
        except Exception as e:
            print(f"  [GREENMOOD] Error {pair['instr']}: {e}")
    return results
