"""
Broker connector para señales de confluencia + insiders.
- Soporta cualquier exchange compatible con ccxt (BitGet, Binance, Bybit, OKX...)
- TRADE_ENABLED=false por defecto. Pon true en .env para operar en vivo.
- Ejecuta: BTC, ETH, XAU (Gold), y acciones como futuros (NVDA, AAPL, TSLA, MSFT, GOOGL, META, COIN, MSTR)
- Solo ejecuta señales de confluencia con CALIDAD: ALTA.
- Señales de insiders/congresistas se ejecutan con lógica propia (SIZE_INSIDER / LEV_INSIDER).
- Todas las posiciones quedan registradas en positions_log.csv
- Seguimiento de posiciones abiertas en open_positions.json
"""

import os
import re
import csv
import json
from datetime import datetime, timezone

# Cargar .env si existe
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

# ── Config ────────────────────────────────────────────────────────────────────

EXCHANGE_ID   = os.environ.get("EXCHANGE_ID",   "bitget").lower()
API_KEY       = os.environ.get("EXCHANGE_API_KEY",    "")
API_SECRET    = os.environ.get("EXCHANGE_API_SECRET",  "")
API_PASS      = os.environ.get("EXCHANGE_PASSPHRASE",  "")
TRADE_ENABLED = os.environ.get("TRADE_ENABLED", "false").lower() == "true"

DIR              = os.path.dirname(os.path.abspath(__file__))
POSITIONS_LOG    = os.path.join(DIR, "positions_log.csv")
OPEN_POS_FILE    = os.path.join(DIR, "open_positions.json")

HORIZON_CFG = {
    "SCALP":    {"usdt": float(os.environ.get("SIZE_SCALP",    "10")),
                 "lev":  int(  os.environ.get("LEV_SCALP",     "5"))},
    "DIA":      {"usdt": float(os.environ.get("SIZE_DIA",      "20")),
                 "lev":  int(  os.environ.get("LEV_DIA",       "3"))},
    "SWING":    {"usdt": float(os.environ.get("SIZE_SWING",    "20")),
                 "lev":  int(  os.environ.get("LEV_SWING",     "2"))},
    "POSICION": {"usdt": float(os.environ.get("SIZE_POSICION", "20")),
                 "lev":  int(  os.environ.get("LEV_POSICION",  "1"))},
    # Señales de insiders/congresistas — horizonte days-to-weeks, leverage conservador
    "INSIDER":  {"usdt": float(os.environ.get("SIZE_INSIDER",  "10")),
                 "lev":  int(  os.environ.get("LEV_INSIDER",   "2"))},
}

# ── Instrumentos soportados ───────────────────────────────────────────────────
def _u(sym): return f"{sym}/USDT:USDT"

SYMBOL_MAP = {
    # ── Crypto L1 / L2 principales ───────────────────────────────────────────
    "BTC": _u("BTC"), "BITCOIN": _u("BTC"),
    "ETH": _u("ETH"), "ETHEREUM": _u("ETH"),
    "SOL": _u("SOL"), "SOLANA": _u("SOL"),
    "XRP": _u("XRP"), "RIPPLE": _u("XRP"),
    "ADA": _u("ADA"), "CARDANO": _u("ADA"),
    "AVAX": _u("AVAX"), "AVALANCHE": _u("AVAX"),
    "DOT": _u("DOT"), "POLKADOT": _u("DOT"),
    "LINK": _u("LINK"), "CHAINLINK": _u("LINK"),
    "MATIC": _u("MATIC"), "POLYGON": _u("MATIC"),
    "POL": _u("POL"),
    "UNI": _u("UNI"),
    "ATOM": _u("ATOM"),
    "LTC": _u("LTC"), "LITECOIN": _u("LTC"),
    "BCH": _u("BCH"),
    "ETC": _u("ETC"),
    "FIL": _u("FIL"),
    "NEAR": _u("NEAR"),
    "APT": _u("APT"),
    "ARB": _u("ARB"),
    "OP": _u("OP"),
    "SUI": _u("SUI"),
    "SEI": _u("SEI"),
    "INJ": _u("INJ"),
    "TIA": _u("TIA"),
    "TON": _u("TON"),
    "BNB": _u("BNB"),
    "TRX": _u("TRX"),
    "STRK": _u("STRK"),

    # ── DeFi / ecosistema ─────────────────────────────────────────────────────
    "AAVE": _u("AAVE"),
    "CRV": _u("CRV"),
    "MKR": _u("MKR"),
    "SNX": _u("SNX"),
    "COMP": _u("COMP"),
    "LDO": _u("LDO"),
    "RUNE": _u("RUNE"),
    "GRT": _u("GRT"),
    "IMX": _u("IMX"),
    "PENDLE": _u("PENDLE"),
    "ENA": _u("ENA"),
    "EIGEN": _u("EIGEN"),
    "ZK": _u("ZK"),
    "MOVE": _u("MOVE"),
    "JTO": _u("JTO"),
    "PYTH": _u("PYTH"),
    "JUP": _u("JUP"),
    "W": _u("W"),
    "ONDO": _u("ONDO"),

    # ── AI / Tech tokens ──────────────────────────────────────────────────────
    "TAO": _u("TAO"),
    "FET": _u("FET"),
    "RNDR": _u("RNDR"), "RENDER": _u("RNDR"),
    "WLD": _u("WLD"),
    "AGIX": _u("AGIX"),
    "OCEAN": _u("OCEAN"),

    # ── Memecoins ─────────────────────────────────────────────────────────────
    "DOGE": _u("DOGE"), "DOGECOIN": _u("DOGE"),
    "SHIB": _u("SHIB"), "SHIBA": _u("SHIB"),
    "PEPE": _u("PEPE"),
    "FLOKI": _u("FLOKI"),
    "BONK": _u("BONK"),
    "WIF": _u("WIF"),
    "BOME": _u("BOME"),
    "POPCAT": _u("POPCAT"),
    "MEW": _u("MEW"),
    "NEIRO": _u("NEIRO"),
    "TURBO": _u("TURBO"),
    "NOT": _u("NOT"),
    "DOGS": _u("DOGS"),
    "HMSTR": _u("HMSTR"),
    "JASMY": _u("JASMY"),
    "GALA": _u("GALA"),

    # ── Gaming / NFT / Metaverso ──────────────────────────────────────────────
    "SAND": _u("SAND"),
    "MANA": _u("MANA"),
    "AXS": _u("AXS"),
    "CHZ": _u("CHZ"),
    "ENJ": _u("ENJ"),
    "BLUR": _u("BLUR"),
    "PIXEL": _u("PIXEL"),

    # ── Gold — todos los aliases que puede devolver el AI ─────────────────────
    "XAU": _u("XAU"), "GOLD": _u("XAU"), "GC": _u("XAU"),
    "ORO": _u("XAU"), "XAUUSD": _u("XAU"), "XAUUSDT": _u("XAU"),
    "GLD": _u("XAU"), "XAUUSD:USDT": _u("XAU"),

    # ── Silver (Plata) — XAGUSDT en BitGet ────────────────────────────────────
    "XAG": _u("XAG"), "SILVER": _u("XAG"), "PLATA": _u("XAG"),
    "XAGUSD": _u("XAG"), "XAGUSDT": _u("XAG"), "XAGUSDT:USDT": _u("XAG"),
    "SI": _u("XAG"),

    # ── Acciones — perpetuos USDT en BitGet ───────────────────────────────────
    # Tech
    "NVDA": _u("NVDA"), "NVIDIA": _u("NVDA"),
    "AAPL": _u("AAPL"), "APPLE": _u("AAPL"),
    "TSLA": _u("TSLA"), "TESLA": _u("TSLA"),
    "MSFT": _u("MSFT"), "MICROSOFT": _u("MSFT"),
    "GOOGL": _u("GOOGL"), "GOOG": _u("GOOGL"), "GOOGLE": _u("GOOGL"),
    "META": _u("META"),
    "AMZN": _u("AMZN"), "AMAZON": _u("AMZN"),
    "NFLX": _u("NFLX"), "NETFLIX": _u("NFLX"),
    "AMD": _u("AMD"),
    "INTC": _u("INTC"), "INTEL": _u("INTC"),
    "COIN": _u("COIN"), "COINBASE": _u("COIN"),
    "MSTR": _u("MSTR"), "MICROSTRATEGY": _u("MSTR"),
    "PLTR": _u("PLTR"), "PALANTIR": _u("PLTR"),
    # Meme / retail
    "GME": _u("GME"),
    "HOOD": _u("HOOD"),
    "SOFI": _u("SOFI"),
    "RIVN": _u("RIVN"),
    # Finanzas / otros
    "JPM": _u("JPM"),
    "BAC": _u("BAC"),
    "V": _u("V"),
    "MA": _u("MA"),
    "BABA": _u("BABA"),
    "NIO": _u("NIO"),
    "BA": _u("BA"),
    "DIS": _u("DIS"), "DISNEY": _u("DIS"),
    "UBER": _u("UBER"),
    "ABNB": _u("ABNB"), "AIRBNB": _u("ABNB"),
    "SNAP": _u("SNAP"),
    "SPOT": _u("SPOT"), "SPOTIFY": _u("SPOT"),
    "RBLX": _u("RBLX"), "ROBLOX": _u("RBLX"),
}

# Acciones que van con LEV_INSIDER (no con HORIZON_CFG general)
INSIDER_STOCKS = {
    "NVDA","AAPL","TSLA","MSFT","GOOGL","GOOG","META","AMZN","NFLX",
    "AMD","INTC","COIN","MSTR","PLTR","GME","HOOD","SOFI","RIVN",
    "JPM","BAC","V","MA","BABA","NIO","BA","DIS","UBER","ABNB",
    "SNAP","SPOT","RBLX",
}

# TP y SL automáticos para insiders (sin precio objetivo explícito)
INSIDER_TP_PCT = float(os.environ.get("INSIDER_TP_PCT", "5.0"))   # +5% take profit
INSIDER_SL_PCT = float(os.environ.get("INSIDER_SL_PCT", "3.0"))   # -3% stop loss

CSV_HEADERS = [
    "Fecha", "Hora UTC", "Fuente", "Instrumento", "Simbolo",
    "Direccion", "Entrada", "TP", "SL",
    "Tamanio USD", "Leverage", "Horizonte",
    "Calidad", "Modo", "Order ID", "Estado", "Notas"
]

# ── Registro CSV de posiciones ────────────────────────────────────────────────

def _init_csv():
    if not os.path.exists(POSITIONS_LOG):
        with open(POSITIONS_LOG, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADERS)

def log_position(fuente, instrumento, simbolo, direccion,
                 entrada, tp, sl, tamanio, leverage, horizonte,
                 calidad, modo, order_id, estado, notas=""):
    _init_csv()
    now = datetime.now(timezone.utc)
    row = [
        now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S"),
        fuente, instrumento, simbolo, direccion,
        str(entrada) if entrada else "MERCADO",
        str(tp) if tp else "N/A",
        str(sl) if sl else "N/A",
        f"{tamanio:.2f}", str(leverage), horizonte,
        calidad, modo, order_id, estado, notas
    ]
    with open(POSITIONS_LOG, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

# ── Seguimiento de posiciones abiertas ───────────────────────────────────────

def _load_open_positions() -> dict:
    try:
        with open(OPEN_POS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_open_positions(positions: dict):
    with open(OPEN_POS_FILE, "w", encoding="utf-8") as f:
        json.dump(positions, f, indent=2, ensure_ascii=False)

def has_open_position(instrument: str) -> bool:
    return instrument.upper() in _load_open_positions()

def _record_open_position(instrument, symbol, direction, entry, tp, sl,
                           tamanio, leverage, horizonte, order_id):
    positions = _load_open_positions()
    positions[instrument.upper()] = {
        "symbol":    symbol,
        "direction": direction,
        "entry":     entry,
        "tp":        tp,
        "sl":        sl,
        "tamanio":   tamanio,
        "leverage":  leverage,
        "horizonte": horizonte,
        "order_id":  order_id,
        "opened_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_open_positions(positions)

def check_position_exits() -> list:
    """
    Verifica si alguna posicion abierta alcanzo TP o SL.
    Devuelve lista de dicts con info de las posiciones cerradas.
    Funciona en paper mode usando precios publicos de Binance.
    """
    positions = _load_open_positions()
    if not positions:
        return []

    closed = []
    still_open = {}

    for instrument, pos in positions.items():
        try:
            import ccxt
            # Precio actual — crypto via Binance público, resto via BitGet público
            sym = SYMBOL_MAP.get(instrument)
            if not sym:
                still_open[instrument] = pos
                continue
            if instrument in ("BTC", "ETH"):
                ex   = ccxt.binance({"options": {"defaultType": "spot"}})
                tick = ex.fetch_ticker(f"{instrument}/USDT")
            else:
                ex   = ccxt.bitget({"options": {"defaultType": "swap"}})
                tick = ex.fetch_ticker(sym)
            price = tick["last"]

            tp    = pos.get("tp")
            sl    = pos.get("sl")
            entry = pos.get("entry", price)
            direc = pos.get("direction", "LARGO")

            hit = None
            if direc == "LARGO":
                if tp and price >= tp: hit = "TP"
                elif sl and price <= sl: hit = "SL"
            else:
                if tp and price <= tp: hit = "TP"
                elif sl and price >= sl: hit = "SL"

            if hit:
                pnl_pct = ((price - entry) / entry * 100) if entry else 0
                if direc == "CORTO":
                    pnl_pct = -pnl_pct
                closed.append({
                    "instrument": instrument,
                    "direction":  direc,
                    "entry":      entry,
                    "exit_price": price,
                    "hit":        hit,
                    "pnl_pct":   round(pnl_pct, 2),
                    "horizonte":  pos.get("horizonte", "?"),
                })
                log_position(
                    fuente="cierre_auto", instrumento=instrument,
                    simbolo=pos.get("symbol", instrument),
                    direccion=direc, entrada=entry, tp=tp, sl=sl,
                    tamanio=pos.get("tamanio", 0),
                    leverage=pos.get("leverage", 1),
                    horizonte=pos.get("horizonte", "?"),
                    calidad="CIERRE", modo="PAPER" if not TRADE_ENABLED else "LIVE",
                    order_id=pos.get("order_id", ""),
                    estado=f"{hit} @ {price:.2f} | PnL: {pnl_pct:+.2f}%",
                )
            else:
                still_open[instrument] = pos

        except Exception:
            still_open[instrument] = pos

    _save_open_positions(still_open)
    return closed

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
                   calidad: str, horizonte: str = "DIA",
                   fuente: str = "confluencia") -> str:

    # Normalizar: quitar paréntesis y texto extra ("XAU (oro)" → "XAU")
    import re as _re
    instrument_key = _re.sub(r'\s*\(.*?\)', '', instrument).strip().split()[0].upper() if instrument.strip() else ""
    symbol = SYMBOL_MAP.get(instrument_key) or SYMBOL_MAP.get(instrument.upper())
    instrument = instrument_key or instrument.upper()
    if not symbol:
        status = f"skip — {instrument} no soportado en broker"
        log_position(fuente, instrument, instrument, direction,
                     entrada, tp, sl, 0, 0, horizonte, calidad, "SKIP", "", status)
        return status

    if calidad.upper() != "ALTA":
        status = f"skip — calidad {calidad} insuficiente (requiere ALTA)"
        log_position(fuente, instrument, symbol, direction,
                     entrada, tp, sl, 0, 0, horizonte, calidad, "SKIP", "", status)
        return status

    # ── Bloquear si ya hay posicion abierta en este instrumento ──────────────
    if has_open_position(instrument):
        status = f"skip — ya hay posicion abierta en {instrument}"
        log_position(fuente, instrument, symbol, direction,
                     entrada, tp, sl, 0, 0, horizonte, calidad, "SKIP", "", status)
        return status

    cfg      = HORIZON_CFG.get(horizonte.upper(), HORIZON_CFG["DIA"])
    max_usdt = cfg["usdt"]
    leverage = cfg["lev"]

    side      = "buy"  if direction.upper() == "LARGO" else "sell"
    entry_p   = parse_price(entrada) if entrada.upper() not in ("MERCADO","N/A","") else None
    tp_p      = parse_price(tp)  if tp  and tp.upper()  != "N/A" else None
    sl_p      = parse_price(sl)  if sl  and sl.upper()  != "N/A" else None
    is_market = entrada.upper() == "MERCADO" or entry_p is None

    # ── Paper mode ─────────────────────────────────────────────────────────
    if not TRADE_ENABLED:
        p_str  = f"@ {entry_p}" if entry_p else "@ MERCADO"
        status = (f"[PAPER] {horizonte} | {side.upper()} {symbol} {p_str} "
                  f"| TP {tp_p or 'N/A'} | SL {sl_p or 'N/A'} "
                  f"| ${max_usdt} x{leverage}lev")
        log_position(fuente, instrument, symbol, direction,
                     entry_p, tp_p, sl_p, max_usdt, leverage, horizonte,
                     calidad, "PAPER", "", "OK", status)
        _record_open_position(instrument, symbol, direction,
                              entry_p or 0, tp_p, sl_p,
                              max_usdt, leverage, horizonte, "paper")
        return status

    # ── Live mode ───────────────────────────────────────────────────────────
    ex = _get_exchange()
    if not ex:
        status = "ERROR — exchange no configurado"
        log_position(fuente, instrument, symbol, direction,
                     entry_p, tp_p, sl_p, max_usdt, leverage, horizonte,
                     calidad, "ERROR", "", status)
        return status

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
        fill_p   = order.get("average") or order.get("price") or use_price

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
        status = (f"ORDEN — {horizonte} | {side.upper()} {qty} {symbol} @ {price_str} "
                  f"| TP {tp_p or 'N/A'} | SL {sl_p or 'N/A'} "
                  f"| ${max_usdt} x{leverage}lev | id {order_id}")
        log_position(fuente, instrument, symbol, direction,
                     fill_p, tp_p, sl_p, max_usdt, leverage, horizonte,
                     calidad, "LIVE", order_id, "OK", status)
        _record_open_position(instrument, symbol, direction,
                              fill_p, tp_p, sl_p,
                              max_usdt, leverage, horizonte, order_id)
        return status

    except Exception as e:
        err = f"ERROR broker — {e}"
        log_position(fuente, instrument, symbol, direction,
                     entry_p, tp_p, sl_p, max_usdt, leverage, horizonte,
                     calidad, "ERROR", "", err)
        return err

# ── Ejecución de señal insider (congresistas + cluster buys) ─────────────────

def execute_insider_signal(ticker: str, trader: str, amount: str,
                            trade_date: str, notes: str = "") -> str:
    """
    Ejecuta un LARGO en BitGet basado en compra de insider/congresista.
    - Usa INSIDER horizon: SIZE_INSIDER / LEV_INSIDER
    - TP automático: +INSIDER_TP_PCT% | SL automático: -INSIDER_SL_PCT%
    - Solo ejecuta tickers soportados en SYMBOL_MAP
    - Bloquea si ya hay posición abierta en ese ticker
    """
    import re as _re
    instrument = _re.sub(r'\s*\(.*?\)', '', ticker).strip().split()[0].upper() if ticker.strip() else ticker.upper()
    symbol = SYMBOL_MAP.get(instrument)
    if not symbol:
        log_insider_trade("insider_skip", ticker, trader, amount, trade_date,
                          f"ticker no soportado en BitGet — {notes}")
        return f"skip — {ticker} no tiene futures en BitGet"

    if has_open_position(instrument):
        return f"skip — ya hay posicion abierta en {ticker}"

    cfg      = HORIZON_CFG["INSIDER"]
    max_usdt = cfg["usdt"]
    leverage = cfg["lev"]

    # Log informativo siempre (independiente del modo paper/live)
    log_insider_trade("insider_trade", ticker, trader, amount, trade_date, notes)

    if not TRADE_ENABLED:
        # Paper mode — simular con TP/SL automáticos
        status = (f"[PAPER] INSIDER LARGO {symbol} @ MERCADO "
                  f"| TP +{INSIDER_TP_PCT}% | SL -{INSIDER_SL_PCT}% "
                  f"| ${max_usdt} x{leverage}lev | {trader}")
        log_position("insider", instrument, symbol, "LARGO",
                     "MERCADO", f"+{INSIDER_TP_PCT}%", f"-{INSIDER_SL_PCT}%",
                     max_usdt, leverage, "INSIDER", "ALTA", "PAPER", "", "OK", status)
        _record_open_position(instrument, symbol, "LARGO",
                              0, None, None, max_usdt, leverage, "INSIDER", "paper")
        return status

    ex = _get_exchange()
    if not ex:
        return "ERROR — exchange no configurado"

    try:
        ticker_data = ex.fetch_ticker(symbol)
        price = ticker_data["last"]
        tp_p  = round(price * (1 + INSIDER_TP_PCT / 100), 2)
        sl_p  = round(price * (1 - INSIDER_SL_PCT / 100), 2)
        qty   = round(max_usdt * leverage / price, 4)

        try:
            ex.set_leverage(leverage, symbol)
        except Exception:
            pass

        order    = ex.create_market_order(symbol, "buy", qty)
        order_id = order.get("id", "?")
        fill_p   = order.get("average") or order.get("price") or price

        # TP limit
        try:
            ex.create_order(symbol, "limit", "sell", qty, tp_p, {"reduceOnly": True})
        except Exception:
            pass

        # SL stop
        try:
            ex.create_order(symbol, "stop_market", "sell", qty, None,
                            {"stopPrice": sl_p, "reduceOnly": True})
        except Exception:
            pass

        status = (f"ORDEN INSIDER — LARGO {qty} {symbol} @ {fill_p:.2f} "
                  f"| TP {tp_p} | SL {sl_p} | ${max_usdt} x{leverage}lev "
                  f"| {trader} | id {order_id}")
        log_position("insider", instrument, symbol, "LARGO",
                     fill_p, tp_p, sl_p, max_usdt, leverage, "INSIDER",
                     "ALTA", "LIVE", order_id, "OK", status)
        _record_open_position(instrument, symbol, "LARGO",
                              fill_p, tp_p, sl_p, max_usdt, leverage, "INSIDER", order_id)
        return status

    except Exception as e:
        return f"ERROR broker insider — {e}"


# ── Registro de operación insider (solo log, sin ejecutar orden) ──────────────

def log_insider_trade(fuente, ticker, trader, amount, trade_date, notes=""):
    _init_csv()
    log_position(
        fuente=fuente, instrumento=ticker, simbolo=ticker,
        direccion="LARGO", entrada="MERCADO",
        tp=None, sl=None, tamanio=0, leverage=1, horizonte="INSIDER",
        calidad="INFO", modo="LOG", order_id="",
        estado=f"{trader} compró {amount} el {trade_date}", notas=notes,
    )
    return f"[LOG] Insider {ticker} registrado en positions_log.csv"
