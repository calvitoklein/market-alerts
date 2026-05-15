"""
FRVP Strategy — Fixed Range Volume Profile Mean Reversion
Instrumentos : BTC y ETH (24/7)
Señal        : Pullback al Value Area a favor de tendencia
  - Precio en uptrend (EMA50 15m) + perfora VAL + cierra de vuelta dentro → LONG
  - Precio en downtrend              + perfora VAH + cierra de vuelta dentro → SHORT
  - Confirmación: vela alcista/bajista con cuerpo real + volumen > 1.2× media
  - SL: bajo el mínimo de la perforación | TP: POC
  - R:R mínimo 2.0 | 1 señal por sesión por instrumento
"""

import os
import json
import hashlib
from datetime import datetime, timezone

DIR = os.path.dirname(os.path.abspath(__file__))

HANDLE = "FRVPStrategy"
NAME   = "FRVP Judeadas"

PAIRS = [
    {"symbol": "BTC/USDT:USDT", "instr": "BTC"},
    {"symbol": "ETH/USDT:USDT", "instr": "ETH"},
]

LOOKBACK      = 96      # 8h de velas 5m para el perfil
N_BINS        = 300
VA_PCT        = 0.70
SL_BUFFER_PCT = 0.0003
MIN_RR        = 2.0
EMA_PERIOD    = 50      # EMA50 en 15m
VOL_AVG_BARS  = 20
VOL_MIN_MULT  = 1.2
BODY_MIN_PCT  = 0.30    # cuerpo de vela confirmación > 30% del rango

SESSION_LOG = os.path.join(DIR, "frvp_sessions.json")


# ── Session limiter ───────────────────────────────────────────────────────────

def _session_key(now: datetime, instr: str) -> str:
    tag = "L" if now.hour < 12 else "NY"
    return f"{now.strftime('%Y%m%d')}_{instr}_{tag}"

def _already_fired(key: str) -> bool:
    try:
        if os.path.exists(SESSION_LOG):
            with open(SESSION_LOG) as f:
                return key in json.load(f).get("fired", [])
    except Exception:
        pass
    return False

def _mark_fired(key: str):
    try:
        data = {"fired": []}
        if os.path.exists(SESSION_LOG):
            with open(SESSION_LOG) as f:
                data = json.load(f)
        fired = data.get("fired", [])
        if key not in fired:
            fired.append(key)
        data["fired"] = fired[-40:]
        with open(SESSION_LOG, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


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


# ── Indicadores ───────────────────────────────────────────────────────────────

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

def _ema_last(closes: list, period: int) -> float:
    k = 2 / (period + 1)
    e = closes[0]
    for v in closes[1:]:
        e = v * k + e * (1 - k)
    return e


# ── Señal por par ─────────────────────────────────────────────────────────────

def _signal_for_pair(ex, pair: dict, now: datetime) -> dict | None:
    symbol = pair["symbol"]
    instr  = pair["instr"]

    sess = _session_key(now, instr)
    if _already_fired(sess):
        return None

    bars5  = _ohlcv(ex, symbol, "5m",  LOOKBACK + 5)
    bars15 = _ohlcv(ex, symbol, "15m", EMA_PERIOD + 10)

    if len(bars5) < LOOKBACK + 3 or len(bars15) < EMA_PERIOD + 2:
        return None

    # Tendencia: EMA50 en 15m
    closes15 = [b[4] for b in bars15]
    ema50    = _ema_last(closes15, EMA_PERIOD)
    trend_up = bars5[-1][4] > ema50

    # FRVP en las últimas LOOKBACK velas (excluyendo las 2 últimas — señal)
    fp = _frvp(bars5[-(LOOKBACK + 2): -2])
    if not fp:
        return None

    poc, vah, val = fp["poc"], fp["vah"], fp["val"]

    # Velas de señal
    prev = bars5[-2]   # vela que perforó el nivel
    curr = bars5[-1]   # vela de confirmación (la más reciente)

    prev_lo, prev_hi = prev[3], prev[2]
    curr_op, curr_cl = curr[1], curr[4]
    curr_hi, curr_lo = curr[2], curr[3]
    curr_range       = curr_hi - curr_lo

    # Filtro pin bar: cuerpo ≥ BODY_MIN_PCT del rango
    curr_body = abs(curr_cl - curr_op)
    if curr_range > 0 and curr_body / curr_range < BODY_MIN_PCT:
        return None

    # Filtro volumen
    vol_curr = curr[5]
    vol_avg  = sum(b[5] for b in bars5[-VOL_AVG_BARS - 2: -2]) / VOL_AVG_BARS
    if vol_curr < vol_avg * VOL_MIN_MULT:
        return None

    direction = None
    sl        = None

    # LONG: uptrend + vela anterior perforó VAL + vela actual cierra dentro alcista
    if (trend_up
            and prev_lo < val
            and curr_cl > val
            and curr_cl > curr_op):
        direction = "LARGO"
        sl        = round(prev_lo * (1 - SL_BUFFER_PCT), 2)

    # SHORT: downtrend + vela anterior perforó VAH + vela actual cierra dentro bajista
    elif (not trend_up
            and prev_hi > vah
            and curr_cl < vah
            and curr_cl < curr_op):
        direction = "CORTO"
        sl        = round(prev_hi * (1 + SL_BUFFER_PCT), 2)

    if not direction:
        return None

    entrada  = round(curr_cl, 2)
    tp       = round(poc, 2)
    sl_dist  = abs(entrada - sl)
    tp_dist  = abs(tp - entrada)

    if sl_dist <= 0 or tp_dist / sl_dist < MIN_RR:
        return None

    if direction == "LARGO"  and tp <= entrada: return None
    if direction == "CORTO"  and tp >= entrada: return None

    rr_actual = round(tp_dist / sl_dist, 2)

    sig_id = "frvp_" + hashlib.md5(
        f"{instr}_{direction}_{now.strftime('%Y%m%d%H%M')}".encode()
    ).hexdigest()[:12]

    _mark_fired(sess)

    return {
        "ts":               now.isoformat(),
        "id":               sig_id,
        "handle":           HANDLE,
        "name":             NAME,
        "link":             "",
        "img":              "",
        "text": (
            f"[FRVP] {instr} {direction} @ {entrada} | "
            f"SL {sl} | TP {tp} (POC) | R:R {rr_actual} | "
            f"VAH={round(vah,2)} VAL={round(val,2)}"
        ),
        "INSTRUMENTO":      instr,
        "DIRECCION":        direction,
        "ENTRADA":          str(entrada),
        "TP":               str(tp),
        "SL":               str(sl),
        "CONFIANZA_TRADER": "MEDIA",
        "HORIZONTE":        "SCALP",
        "RESUMEN":          f"FRVP pullback {direction} {instr} POC={round(poc,2)}",
        # Niveles FRVP para el chart generator
        "_poc": poc,
        "_vah": vah,
        "_val": val,
    }


# ── Entry point ───────────────────────────────────────────────────────────────

def generate_signals() -> list:
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
            print(f"  [FRVP] Error {pair['instr']}: {e}")
    return results
