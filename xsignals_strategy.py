"""
XSignals Judeadas — Estrategia SMC estilo Fabio Valentini
Instrumento : XAU (Oro)
Timeframes  : 15m para tendencia estructural / 5m para entrada
Sesiones    : London 07:00-10:00 UTC  |  Nueva York 13:30-16:30 UTC
Conceptos   : Market Structure (HH/HL, LH/LL) + FVG + Order Block
"""

import os
import hashlib
from datetime import datetime, timezone

DIR    = os.path.dirname(os.path.abspath(__file__))
HANDLE = "XSignalsJudeadas"
NAME   = "X Signals Judeadas"
SYMBOL = "XAU/USDT:USDT"
INSTR  = "XAU"

# Sesiones activas (UTC): (hora_inicio, min_inicio, hora_fin, min_fin)
SESSIONS = [(7, 0, 10, 0), (13, 30, 16, 30)]

# Gestión de riesgo
SL_BUFFER   = 0.80   # USD de margen extra bajo/sobre la zona para el SL
RR_RATIO    = 2.5    # Risk:Reward objetivo
MIN_SL_DIST = 1.5    # SL mínimo en USD desde la entrada
MAX_SL_DIST = 12.0   # SL máximo en USD desde la entrada
ZONE_TOL    = 0.5    # USD de tolerancia para considerar precio "en la zona"

# Lookbacks
TREND_BARS  = 50     # velas 15m para estructura
ENTRY_BARS  = 25     # velas 5m para OB/FVG
SWING_WIN   = 4      # ventana de velas a cada lado para swing H/L


# ── Sesión ────────────────────────────────────────────────────────────────────

def _in_session(dt: datetime = None) -> bool:
    dt  = dt or datetime.now(timezone.utc)
    tm  = dt.hour * 60 + dt.minute
    return any(sh * 60 + sm <= tm < eh * 60 + em for sh, sm, eh, em in SESSIONS)


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


def _ohlcv(ex, tf: str, limit: int) -> list:
    try:
        return ex.fetch_ohlcv(SYMBOL, timeframe=tf, limit=limit)
    except Exception:
        return []


# ── Tendencia (15m) ───────────────────────────────────────────────────────────

def _detect_trend(bars: list) -> str:
    """
    HH + HL → UP  |  LH + LL → DOWN  |  otro → NEUTRAL
    Fallback: EMA20 vs precio si no hay suficientes swings.
    """
    if len(bars) < SWING_WIN * 2 + 6:
        return "NEUTRAL"

    highs = [b[2] for b in bars]
    lows  = [b[3] for b in bars]
    n     = len(bars)

    def is_sh(i):
        return (i >= SWING_WIN and i < n - SWING_WIN and
                all(highs[i] > highs[i - j] for j in range(1, SWING_WIN + 1)) and
                all(highs[i] > highs[i + j] for j in range(1, SWING_WIN + 1)))

    def is_sl(i):
        return (i >= SWING_WIN and i < n - SWING_WIN and
                all(lows[i] < lows[i - j] for j in range(1, SWING_WIN + 1)) and
                all(lows[i] < lows[i + j] for j in range(1, SWING_WIN + 1)))

    shs = [highs[i] for i in range(n) if is_sh(i)]
    sls = [lows[i]  for i in range(n) if is_sl(i)]

    if len(shs) >= 2 and len(sls) >= 2:
        if shs[-1] > shs[-2] and sls[-1] > sls[-2]:
            return "UP"
        if shs[-1] < shs[-2] and sls[-1] < sls[-2]:
            return "DOWN"
        return "NEUTRAL"

    # Fallback EMA20
    k   = 2 / 21
    ema = bars[0][4]
    for b in bars[1:]:
        ema = b[4] * k + ema * (1 - k)
    avg5 = sum(b[4] for b in bars[-5:]) / 5
    if avg5 > ema * 1.0015:
        return "UP"
    if avg5 < ema * 0.9985:
        return "DOWN"
    return "NEUTRAL"


# ── Fair Value Gap (FVG) ──────────────────────────────────────────────────────

def _find_fvgs(bars: list) -> list:
    """
    Bullish FVG: bars[i-2].high < bars[i].low   (gap alcista — precio puede retroceder a llenarlo)
    Bearish FVG: bars[i-2].low  > bars[i].high  (gap bajista)
    """
    result = []
    for i in range(2, len(bars)):
        h2  = bars[i - 2][2]
        l2  = bars[i - 2][3]
        h0  = bars[i][2]
        l0  = bars[i][3]

        if h2 < l0 and (l0 - h2) >= 0.3:          # bullish FVG, mínimo 0.30 USD de tamaño
            result.append({"type": "BULL", "top": l0, "bottom": h2,
                           "mid": (l0 + h2) / 2, "idx": i, "_zone_type": "FVG"})

        elif l2 > h0 and (l2 - h0) >= 0.3:        # bearish FVG
            result.append({"type": "BEAR", "top": l2, "bottom": h0,
                           "mid": (l2 + h0) / 2, "idx": i, "_zone_type": "FVG"})
    return result


# ── Order Block (OB) ─────────────────────────────────────────────────────────

def _find_order_blocks(bars: list) -> list:
    """
    Bullish OB: última vela bajista antes de un impulso alcista de 3+ velas que hace BOS.
    Bearish OB: última vela alcista antes de un impulso bajista de 3+ velas que hace BOS.
    """
    result = []
    n = len(bars)

    for i in range(n - 4):
        o, h, lo, cl = bars[i][1], bars[i][2], bars[i][3], bars[i][4]
        impulse       = bars[i + 1: i + 4]

        if cl < o:  # vela bajista → candidato a OB alcista
            if (all(c[4] > c[1] for c in impulse) and      # 3 velas alcistas seguidas
                    impulse[-1][4] > h):                    # BOS: superan el high del OB
                result.append({"type": "BULL", "top": h, "bottom": lo,
                               "mid": (h + lo) / 2, "idx": i, "_zone_type": "OB"})

        elif cl > o:  # vela alcista → candidato a OB bajista
            if (all(c[4] < c[1] for c in impulse) and
                    impulse[-1][4] < lo):                   # BOS: rompen el low del OB
                result.append({"type": "BEAR", "top": h, "bottom": lo,
                               "mid": (h + lo) / 2, "idx": i, "_zone_type": "OB"})
    return result


# ── Señal principal ───────────────────────────────────────────────────────────

def generate_signals() -> list:
    """
    Genera señales SMC (FVG + OB + estructura 15m) para XAU.
    Retorna lista de señales con el formato del buffer del bot.
    Solo activa durante sesiones London / NY.
    """
    now = datetime.now(timezone.utc)
    if not _in_session(now):
        return []

    ex = _get_exchange()
    if not ex:
        return []

    bars15 = _ohlcv(ex, "15m", TREND_BARS)
    bars5  = _ohlcv(ex, "5m",  ENTRY_BARS)

    if len(bars15) < 20 or len(bars5) < 10:
        return []

    price = bars5[-1][4]   # último cierre 5m = precio actual
    trend = _detect_trend(bars15)

    if trend == "NEUTRAL":
        return []

    # Detectar zonas en las últimas 15 velas de 5m
    recent = bars5[-15:]
    zones = []
    for z in _find_fvgs(recent) + _find_order_blocks(recent):
        if (trend == "UP"   and z["type"] == "BULL") or \
           (trend == "DOWN" and z["type"] == "BEAR"):
            zones.append(z)

    if not zones:
        return []

    # Zona más reciente con precio dentro (o muy cerca)
    active = None
    for z in sorted(zones, key=lambda x: x["idx"], reverse=True):
        rng = max(z["top"] - z["bottom"], 0.1)
        tol = max(rng * 0.15, ZONE_TOL)   # 15% del tamaño de la zona o mín 0.5 USD
        if z["bottom"] - tol <= price <= z["top"] + tol:
            active = z
            break

    if not active:
        return []

    # Calcular entrada, SL y TP
    # Entrada = precio actual de mercado (no el mid histórico de la zona)
    # El precio ya está confirmado dentro de la zona por _price_near_zone
    if trend == "UP":
        direction = "LARGO"
        entrada   = round(price, 2)
        sl        = round(active["bottom"] - SL_BUFFER, 2)
    else:
        direction = "CORTO"
        entrada   = round(price, 2)
        sl        = round(active["top"] + SL_BUFFER, 2)

    sl_dist = abs(entrada - sl)
    if not (MIN_SL_DIST <= sl_dist <= MAX_SL_DIST):
        return []

    tp = round(entrada + sl_dist * RR_RATIO * (1 if trend == "UP" else -1), 2)

    session   = "London" if now.hour < 12 else "NY"
    zone_type = active.get("_zone_type", "SMC")
    rr_actual = round(sl_dist * RR_RATIO, 2)

    # ID único por dirección + zona + hora — evita duplicados en el mismo ciclo
    sig_id = "xsig_" + hashlib.md5(
        f"{direction}_{active['mid']:.2f}_{now.strftime('%Y%m%d%H')}".encode()
    ).hexdigest()[:12]

    return [{
        "ts":               now.isoformat(),
        "id":               sig_id,
        "handle":           HANDLE,
        "name":             NAME,
        "link":             "https://www.tradingview.com/u/judeadasva78265/",
        "img":              "",
        "text": (
            f"[SMC Scalp] XAU {direction} @ {entrada} | "
            f"SL {sl} | TP {tp} (+{rr_actual} USD) | "
            f"{session} session | {zone_type} + estructura 15m"
        ),
        "INSTRUMENTO":      INSTR,
        "DIRECCION":        direction,
        "ENTRADA":          str(entrada),
        "TP":               str(tp),
        "SL":               str(sl),
        "CONFIANZA_TRADER": "ALTA",
        "HORIZONTE":        "SCALP",
        "RESUMEN":          f"SMC {direction} XAU {zone_type} {session}",
    }]
