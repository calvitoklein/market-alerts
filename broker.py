"""
Broker connector para señales de confluencia.
- Soporta cualquier exchange compatible con ccxt (BitGet, Binance, Bybit, OKX...)
- TRADE_ENABLED=false por defecto. Pon true en .env para operar en vivo.
- Solo ejecuta BTC y ETH (futuros USDT-M). NQ/ES/XAU se ignoran.
- Solo ejecuta señales con CALIDAD: ALTA.
- Sizing y apalancamiento ajustados automáticamente por horizonte temporal.
"""

import os
import re

# ── Config base ───────────────────────────────────────────────────────────────

EXCHANGE_ID   = os.environ.get("EXCHANGE_ID",   "bitget").lower()
API_KEY       = os.environ.get("EXCHANGE_API_KEY",    "")
API_SECRET    = os.environ.get("EXCHANGE_API_SECRET",  "")
API_PASS      = os.environ.get("EXCHANGE_PASSPHRASE",  "")
TRADE_ENABLED = os.environ.get("TRADE_ENABLED", "false").lower() == "true"

# ── Sizing por horizonte temporal ─────────────────────────────────────────────
#
#  SCALP   — minutos/horas     → poco dinero, apalancamiento alto (riesgo acotado)
#  DIA     — hasta 24h         → tamaño medio, apalancamiento medio
#  SWING   — días/semanas      → más dinero, apalancamiento bajo
#  POSICION — semanas/meses    → más dinero, sin apalancamiento
#
HORIZON_CFG = {
    "SCALP":    {"usdt": float(os.environ.get("SIZE_SCALP",    "10")),
                 "lev":  int(  os.environ.get("LEV_SCALP",     "5"))},
    "DIA":      {"usdt": float(os.environ.get("SIZE_DIA",      "20")),
                 "lev":  int(  os.environ.get("LEV_DIA",       "3"))},
    "SWING":    {"usdt": float(os.environ.get("SIZE_SWING",    "50")),
                 "lev":  int(  os.environ.get("LEV_SWING",     "2"))},
    "POSICION": {"usdt": float(os.environ.get("SIZE_POSICION", "100")),
                 "lev":  int(  os.environ.get("LEV_POSICION",  "1"))},
}

SYMBOL_MAP = {
    "BTC": "BTC/USDT:USDT",
    "ETH": "ETH/USDT:USDT",
}

# ── Exchange ──────────────────────────────────────────────────────────────────

def _get_exchange():
    if not API_KEY:
        return None
    try:
        import ccxt
        ex = getattr(ccxt, EXCHANGE_ID)({
            "apiKey":   API_KEY,
            "secret":   API_SECRET,
            "password": API_PASS,
            "options":  {"defaultType": "swap"},
        })
        return ex
    except Exception:
        return None

# ── Parseo de precio ──────────────────────────────────────────────────────────

def parse_price(s: str):
    if not s:
        return None
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

# ── Ejecución de señal ────────────────────────────────────────────────────────

def execute_signal(instrument: str, direction: str,
                   entrada: str, tp: str, sl: str,
                   calidad: str, horizonte: str = "DIA") -> str:

    symbol = SYMBOL_MAP.get(instrument.upper())
    if not symbol:
        return f"skip — {instrument} no soportado en broker (solo BTC/ETH)"

    if calidad.upper() != "ALTA":
        return f"skip — calidad {calidad} insuficiente (requiere ALTA)"

    cfg       = HORIZON_CFG.get(horizonte.upper(), HORIZON_CFG["DIA"])
    max_usdt  = cfg["usdt"]
    leverage  = cfg["lev"]

    side      = "buy"  if direction.upper() == "LARGO" else "sell"
    entry_p   = parse_price(entrada) if entrada.upper() not in ("MERCADO","N/A","") else None
    tp_p      = parse_price(tp)  if tp  and tp.upper()  != "N/A" else None
    sl_p      = parse_price(sl)  if sl  and sl.upper()  != "N/A" else None
    is_market = entrada.upper() == "MERCADO" or entry_p is None

    # ── Paper mode ─────────────────────────────────────────────────────────
    if not TRADE_ENABLED:
        p_str = f"@ {entry_p}" if entry_p else "@ MERCADO"
        return (f"[PAPER] {horizonte} | {side.upper()} {symbol} {p_str} "
                f"| TP {tp_p or 'N/A'} | SL {sl_p or 'N/A'} "
                f"| ${max_usdt} x{leverage}lev")

    # ── Live mode ───────────────────────────────────────────────────────────
    ex = _get_exchange()
    if not ex:
        return "ERROR — exchange no configurado (revisa EXCHANGE_API_KEY en .env)"

    try:
        ticker    = ex.fetch_ticker(symbol)
        use_price = entry_p if entry_p else ticker["last"]
        qty       = round(max_usdt * leverage / use_price, 6)

        try:
            ex.set_leverage(leverage, symbol)
        except Exception:
            pass

        if is_market:
            order = ex.create_market_order(symbol, side, qty)
        else:
            order = ex.create_limit_order(symbol, side, qty, entry_p)

        order_id = order.get("id", "?")

        if tp_p:
            tp_side = "sell" if side == "buy" else "buy"
            try:
                ex.create_order(symbol, "limit", tp_side, qty, tp_p,
                                {"reduceOnly": True})
            except Exception:
                pass

        if sl_p:
            sl_side = "sell" if side == "buy" else "buy"
            try:
                ex.create_order(symbol, "stop_market", sl_side, qty, None,
                                {"stopPrice": sl_p, "reduceOnly": True})
            except Exception:
                pass

        price_str = "MERCADO" if is_market else str(entry_p)
        return (f"ORDEN — {horizonte} | {side.upper()} {qty} {symbol} @ {price_str} "
                f"| TP {tp_p or 'N/A'} | SL {sl_p or 'N/A'} "
                f"| ${max_usdt} x{leverage}lev | id {order_id}")

    except Exception as e:
        return f"ERROR broker — {e}"
