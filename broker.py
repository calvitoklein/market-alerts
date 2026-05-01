"""
Broker connector para señales de confluencia.
- Soporta cualquier exchange compatible con ccxt (BitGet, Binance, Bybit, OKX...)
- TRADE_ENABLED=false por defecto — solo loguea. Pon true en .env para operar en vivo.
- Solo ejecuta BTC y ETH (futuros USDT-M). NQ/ES se ignoran.
- Solo ejecuta señales con CALIDAD: ALTA.
"""

import os
import re

# ── Config desde .env ─────────────────────────────────────────────────────────

EXCHANGE_ID    = os.environ.get("EXCHANGE_ID", "bitget").lower()
API_KEY        = os.environ.get("EXCHANGE_API_KEY", "")
API_SECRET     = os.environ.get("EXCHANGE_API_SECRET", "")
API_PASS       = os.environ.get("EXCHANGE_PASSPHRASE", "")   # BitGet / OKX lo requieren
TRADE_ENABLED  = os.environ.get("TRADE_ENABLED", "false").lower() == "true"
MAX_USDT       = float(os.environ.get("MAX_TRADE_USDT", "20"))
LEVERAGE       = int(os.environ.get("TRADE_LEVERAGE", "1"))

# Mapeo instrumento → symbol ccxt (futuros perpetuos USDT-M)
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
        exchange_class = getattr(ccxt, EXCHANGE_ID)
        ex = exchange_class({
            "apiKey":   API_KEY,
            "secret":   API_SECRET,
            "password": API_PASS,
            "options":  {"defaultType": "swap"},   # futuros perpetuos
        })
        return ex
    except Exception as e:
        return None

# ── Parseo de precio ──────────────────────────────────────────────────────────

def parse_price(s: str):
    """
    Convierte string de precio a float.
    Acepta: '67000', '$67k', '66000-67000' (devuelve el punto medio), 'MERCADO'.
    Devuelve None si no puede parsear.
    """
    if not s:
        return None
    s = s.strip().replace(",", "").replace("$", "").replace(" ", "")
    # "k" → miles
    s = re.sub(r'(\d+(?:\.\d+)?)k', lambda m: str(float(m.group(1)) * 1000), s, flags=re.I)
    # rango "A-B" → punto medio
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
                   entrada: str, tp: str, sl: str, calidad: str) -> str:
    """
    Ejecuta (o simula) una orden a partir de una señal de confluencia.
    Devuelve string de estado para loguear y mandar a Telegram.
    """

    symbol = SYMBOL_MAP.get(instrument.upper())
    if not symbol:
        return f"skip — {instrument} no es crypto soportado (solo BTC/ETH)"

    if calidad.upper() not in ("ALTA",):
        return f"skip — calidad {calidad} insuficiente (requiere ALTA)"

    side       = "buy"  if direction.upper() == "LARGO" else "sell"
    entry_p    = parse_price(entrada) if entrada.upper() not in ("MERCADO", "N/A", "") else None
    tp_p       = parse_price(tp)  if tp  and tp.upper()  != "N/A" else None
    sl_p       = parse_price(sl)  if sl  and sl.upper()  != "N/A" else None
    is_market  = entrada.upper() == "MERCADO" or entry_p is None

    # ── Paper mode ──────────────────────────────────────────────────────────
    if not TRADE_ENABLED:
        price_str = f"@ {entry_p}" if entry_p else "@ MERCADO"
        tp_str    = f"TP {tp_p}"   if tp_p    else "TP N/A"
        sl_str    = f"SL {sl_p}"   if sl_p    else "SL N/A"
        return f"[PAPER] {side.upper()} {symbol} {price_str} | {tp_str} | {sl_str} | ${MAX_USDT} max"

    # ── Live mode ───────────────────────────────────────────────────────────
    ex = _get_exchange()
    if not ex:
        return "ERROR — exchange no configurado. Revisa EXCHANGE_API_KEY en .env"

    try:
        # Precio actual para calcular cantidad
        ticker     = ex.fetch_ticker(symbol)
        use_price  = entry_p if entry_p else ticker["last"]
        qty        = round(MAX_USDT / use_price, 6)

        # Apalancamiento
        try:
            ex.set_leverage(LEVERAGE, symbol)
        except Exception:
            pass

        # Orden principal
        if is_market:
            order = ex.create_market_order(symbol, side, qty)
        else:
            order = ex.create_limit_order(symbol, side, qty, entry_p)

        order_id = order.get("id", "?")

        # Take profit
        if tp_p:
            tp_side = "sell" if side == "buy" else "buy"
            try:
                ex.create_order(symbol, "limit", tp_side, qty, tp_p,
                                {"reduceOnly": True})
            except Exception:
                pass

        # Stop loss
        if sl_p:
            sl_side = "sell" if side == "buy" else "buy"
            try:
                ex.create_order(symbol, "stop_market", sl_side, qty, None,
                                {"stopPrice": sl_p, "reduceOnly": True})
            except Exception:
                pass

        return (f"ORDEN ENVIADA — {side.upper()} {qty} {symbol} "
                f"{'@ MERCADO' if is_market else f'@ {entry_p}'} | "
                f"TP {tp_p or 'N/A'} | SL {sl_p or 'N/A'} | id {order_id}")

    except Exception as e:
        return f"ERROR broker — {e}"
