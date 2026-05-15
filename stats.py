"""
Tracker de aciertos por trader.
- Guarda cada señal en pending_results.json
- Tras EVAL_HOURS evalúa si el trade habría ganado o perdido
- Mantiene leaderboard en trader_stats.json
- Manda ranking a Telegram cada LEADERBOARD_HOURS horas
"""

import os
import json
from datetime import datetime, timezone, timedelta

DIR               = os.path.dirname(os.path.abspath(__file__))
PENDING_FILE      = os.path.join(DIR, "pending_results.json")
STATS_FILE        = os.path.join(DIR, "trader_stats.json")
LAST_LB_FILE      = os.path.join(DIR, "last_leaderboard.txt")

EVAL_HOURS        = 6     # horas hasta evaluar resultado
LEADERBOARD_HOURS = 12    # cada cuántas horas mandar ranking a Telegram
MAX_PENDING       = 500

# yfinance symbols para instrumentos no-crypto
YFINANCE_MAP = {
    # Metales
    "XAU": "GC=F", "GOLD": "GC=F", "GC": "GC=F",
    "XAG": "SI=F", "SILVER": "SI=F",
    "COPPER": "HG=F",
    "NATGAS": "NG=F",
    "XPT": "PL=F",   # Platinum
    "XPD": "PA=F",   # Palladium
    # Índices
    "NQ":  "NQ=F",
    "ES":  "ES=F",
    "CL":  "CL=F",
    "DXY": "DX-Y.NYB",
    # Acciones (evaluación de señales)
    "NVDA": "NVDA", "TSLA": "TSLA", "AAPL": "AAPL",
    "META": "META", "GOOGL": "GOOGL", "MSFT": "MSFT",
    "AMD": "AMD", "COIN": "COIN", "MSTR": "MSTR",
    "AMZN": "AMZN", "NFLX": "NFLX", "PLTR": "PLTR",
}

# ── Persistencia ──────────────────────────────────────────────────────────────

def load_pending() -> list:
    try:
        with open(PENDING_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_pending(pending: list):
    with open(PENDING_FILE, "w", encoding="utf-8") as f:
        json.dump(pending[-MAX_PENDING:], f, ensure_ascii=False)

def load_stats() -> dict:
    try:
        with open(STATS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_stats(stats: dict):
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def _last_leaderboard_ts() -> datetime:
    try:
        with open(LAST_LB_FILE, encoding="utf-8") as f:
            return datetime.fromisoformat(f.read().strip())
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

def _save_leaderboard_ts():
    with open(LAST_LB_FILE, "w", encoding="utf-8") as f:
        f.write(datetime.now(timezone.utc).isoformat())

# ── Precio actual ─────────────────────────────────────────────────────────────

def _price_crypto(symbol: str):
    """BTC/ETH via ccxt public endpoint (no auth)."""
    try:
        import ccxt
        ex = ccxt.binance({"options": {"defaultType": "spot"}})
        t  = ex.fetch_ticker(f"{symbol}/USDT")
        return t["last"]
    except Exception:
        return None

def _price_yfinance(yf_sym: str):
    try:
        import yfinance as yf
        t = yf.Ticker(yf_sym)
        h = t.history(period="1d", interval="5m")
        if not h.empty:
            return float(h["Close"].iloc[-1])
    except Exception:
        pass
    return None

def get_current_price(instrument: str):
    inst = instrument.upper()
    if inst in ("BTC", "ETH"):
        p = _price_crypto(inst)
        if p:
            return p
    yf_sym = YFINANCE_MAP.get(inst)
    if yf_sym:
        return _price_yfinance(yf_sym)
    return None

def get_range_since(instrument: str, since: datetime):
    """
    Devuelve (high, low, current) desde 'since' hasta ahora.
    Necesario para detectar si se tocó TP o SL.
    """
    inst   = instrument.upper()
    is_crypto = inst in ("BTC", "ETH")

    try:
        if is_crypto:
            import ccxt
            ex       = ccxt.binance({"options": {"defaultType": "spot"}})
            since_ms = int(since.timestamp() * 1000)
            ohlcv    = ex.fetch_ohlcv(f"{inst}/USDT", "1h", since=since_ms, limit=48)
            if not ohlcv:
                return None
            highs   = [c[2] for c in ohlcv]
            lows    = [c[3] for c in ohlcv]
            current = ohlcv[-1][4]
            return max(highs), min(lows), current
        else:
            yf_sym = YFINANCE_MAP.get(inst)
            if not yf_sym:
                return None
            import yfinance as yf
            t = yf.Ticker(yf_sym)
            # Use 5d to cover signals up to 14 days old
            age_days = max(3, int((datetime.now(timezone.utc) - since).total_seconds() / 86400) + 1)
            h = t.history(period=f"{min(age_days, 14)}d", interval="1h")
            if h.empty:
                return None
            # Normalize index to UTC for comparison
            if h.index.tz is not None:
                idx = h.index.tz_convert("UTC")
            else:
                idx = h.index.tz_localize("UTC")
            h = h[idx >= since]
            if h.empty:
                return None
            return float(h["High"].max()), float(h["Low"].min()), float(h["Close"].iloc[-1])
    except Exception:
        return None

# ── Evaluación de resultado ───────────────────────────────────────────────────

def _parse_price(s: str):
    if not s:
        return None
    import re
    s = s.strip().replace(",", "").replace("$", "").replace(" ", "")
    s = re.sub(r"(\d+(?:\.\d+)?)k", lambda m: str(float(m.group(1)) * 1000), s, flags=re.I)
    if re.match(r"^\d+(\.\d+)?-\d+(\.\d+)?$", s):
        parts = s.split("-")
        try:
            return (float(parts[0]) + float(parts[1])) / 2
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None

def evaluate_signal(signal: dict) -> str:
    """
    Devuelve WIN, LOSS o NEUTRAL.
    Si ambos TP y SL se tocaron en la ventana, usa la magnitud del movimiento
    para estimar cuál llegó primero (más conservador que asumir siempre WIN).
    """
    instrument = signal.get("INSTRUMENTO", "").upper()
    direction  = signal.get("DIRECCION", "").upper()
    ts         = datetime.fromisoformat(signal["ts"])
    entry      = _parse_price(signal.get("ENTRADA", ""))
    tp         = _parse_price(signal.get("TP", ""))
    sl         = _parse_price(signal.get("SL", ""))

    rng = get_range_since(instrument, ts)
    if not rng:
        return "NEUTRAL"

    high, low, current = rng

    if direction == "LARGO":
        hit_tp = bool(tp and high >= tp)
        hit_sl = bool(sl and low  <= sl)
        if hit_tp and hit_sl:
            # Ambos tocados — estima qué llegó primero por magnitud
            # Si el recorrido al TP (desde entrada) es menor que al SL, más probable que TP llegó antes
            if entry:
                dist_tp = abs(tp - entry)
                dist_sl = abs(sl - entry)
                return "WIN" if dist_tp <= dist_sl else "LOSS"
            return "NEUTRAL"
        if hit_tp:    return "WIN"
        if hit_sl:    return "LOSS"
        if entry:     return "WIN" if current > entry * 1.002 else "LOSS"

    elif direction == "CORTO":
        hit_tp = bool(tp and low  <= tp)
        hit_sl = bool(sl and high >= sl)
        if hit_tp and hit_sl:
            if entry:
                dist_tp = abs(tp - entry)
                dist_sl = abs(sl - entry)
                return "WIN" if dist_tp <= dist_sl else "LOSS"
            return "NEUTRAL"
        if hit_tp:    return "WIN"
        if hit_sl:    return "LOSS"
        if entry:     return "WIN" if current < entry * 0.998 else "LOSS"

    return "NEUTRAL"

# ── Blacklist automática por mal historial ────────────────────────────────────

BLACKLIST_MIN_SIGNALS = 20   # mínimo de señales para considerar blacklist
BLACKLIST_MAX_WR      = 0.38  # WR por debajo de este umbral → blacklisted

def is_channel_blacklisted(handle: str, stats: dict) -> bool:
    """True si el canal tiene historial suficiente y WR sistemáticamente malo."""
    s = stats.get(handle, {})
    evaluated = s.get("wins", 0) + s.get("losses", 0)
    if evaluated < BLACKLIST_MIN_SIGNALS:
        return False
    return s.get("win_rate", 1.0) < BLACKLIST_MAX_WR


# ── Leaderboard ───────────────────────────────────────────────────────────────

def _update_stats(handle: str, name: str, result: str, stats: dict):
    if handle not in stats:
        stats[handle] = {"name": name, "signals": 0,
                         "wins": 0, "losses": 0, "neutral": 0,
                         "win_rate": 0.0, "last_signal": ""}
    s = stats[handle]
    s["signals"] += 1
    if result == "WIN":      s["wins"]    += 1
    elif result == "LOSS":   s["losses"]  += 1
    else:                    s["neutral"] += 1
    evaluated = s["wins"] + s["losses"]
    s["win_rate"]    = round(s["wins"] / evaluated, 3) if evaluated else 0.0
    s["last_signal"] = datetime.now(timezone.utc).isoformat()

def build_leaderboard_message(stats: dict) -> str:
    ranked = sorted(
        [(h, s) for h, s in stats.items()
         if s.get("wins", 0) + s.get("losses", 0) >= 3],
        key=lambda x: x[1]["win_rate"],
        reverse=True,
    )
    if not ranked:
        return "📊 <b>RANKING</b> — sin suficientes señales evaluadas aún (mín. 3)"

    medals = ["🥇", "🥈", "🥉"]
    lines  = [f"📊 <b>RANKING DE TRADERS</b> · {datetime.now(timezone.utc).strftime('%d/%m %H:%M UTC')}\n"]

    for i, (handle, s) in enumerate(ranked[:10]):
        medal    = medals[i] if i < 3 else f"  {i+1}."
        wr       = int(s["win_rate"] * 100)
        evaluated = s["wins"] + s["losses"]
        lines.append(f"{medal} @{handle} <b>{wr}%</b>  ({s['wins']}✅ {s['losses']}❌ / {evaluated} ops)")

    # Peor trader (mín 3 evaluadas)
    if len(ranked) > 1:
        worst_h, worst_s = ranked[-1]
        worst_wr = int(worst_s["win_rate"] * 100)
        lines.append(f"\n⚠️ <b>Peor:</b> @{worst_h} — {worst_wr}%  ({worst_s['wins']}✅ {worst_s['losses']}❌)")

    return "\n".join(lines)

# ── Ciclo principal ───────────────────────────────────────────────────────────

def add_pending_signal(signal: dict):
    pending = load_pending()
    pending.append(signal)
    save_pending(pending)

def run_stats_cycle(tg_token: str, tg_chat: str, send_telegram_fn) -> int:
    pending      = load_pending()
    stats        = load_stats()
    now          = datetime.now(timezone.utc)
    eval_cutoff  = now - timedelta(hours=EVAL_HOURS)

    evaluated    = 0
    still_pending = []

    for sig in pending:
        ts = datetime.fromisoformat(sig["ts"])
        if ts > eval_cutoff:
            still_pending.append(sig)
            continue
        result = evaluate_signal(sig)
        _update_stats(sig.get("handle", "?"), sig.get("name", "?"), result, stats)
        evaluated += 1
        print(f"  [STATS] @{sig.get('handle','?')} {sig.get('INSTRUMENTO','?')} "
              f"{sig.get('DIRECCION','?')} -> {result}")

    save_pending(still_pending)
    save_stats(stats)

    # Mandar leaderboard cada LEADERBOARD_HOURS
    if (now - _last_leaderboard_ts()).total_seconds() >= LEADERBOARD_HOURS * 3600:
        msg = build_leaderboard_message(stats)
        send_telegram_fn(tg_token, tg_chat, msg)
        _save_leaderboard_ts()

    return evaluated
