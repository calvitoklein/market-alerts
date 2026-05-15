"""
Contexto de mercado: Fear & Greed Index + señales de sentimiento macro.

Fear & Greed (alternative.me):
  0-24   Extreme Fear    → solo LONGs (mercado deprimido, rebotes esperados)
  25-44  Fear            → LONGs preferidos
  45-55  Neutral         → sin filtro
  56-74  Greed           → SHORTs preferidos
  75-100 Extreme Greed   → solo SHORTs (mercado eufórico, correcciones esperadas)

Uso:
  ctx = get_market_context()
  ok  = is_signal_ok_for_context(direction, ctx)
"""

import os, time, json, requests
from datetime import datetime, timezone, timedelta

DIR       = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(DIR, "_fng_cache.json")
CACHE_TTL  = 3600  # refrescar máximo cada 1h

FNG_URL = "https://api.alternative.me/fng/?limit=1"

# Umbrales — ajustados para no ser demasiado restrictivos
BLOCK_LONG_ABOVE  = 78   # F&G > 78 → no ejecutar LONGs (euforia extrema)
BLOCK_SHORT_BELOW = 22   # F&G < 22 → no ejecutar SHORTs (pánico extremo)

# Funding rate: si BTC funding > umbral, los longs están sobrecargados → suprimir LONGs
FUNDING_BLOCK_LONG_ABOVE  =  0.05 / 100   # 0.05% por periodo 8h
FUNDING_BLOCK_SHORT_BELOW = -0.04 / 100   # -0.04% (backtest insuficiente — umbral conservador)


def get_fear_greed() -> dict | None:
    """
    Devuelve {'value': int, 'label': str, 'ts': str} o None si falla.
    Usa caché de 1h para no saturar la API.
    """
    # Leer caché
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            cached = json.load(f)
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached["fetched_at"])).total_seconds()
        if age < CACHE_TTL:
            return cached
    except Exception:
        pass

    # Fetch fresco
    try:
        r = requests.get(FNG_URL, timeout=8)
        if not r.ok:
            return None
        data  = r.json()["data"][0]
        result = {
            "value":      int(data["value"]),
            "label":      data["value_classification"],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(result, f)
        return result
    except Exception:
        return None


def get_btc_funding_rate() -> float | None:
    """Devuelve el funding rate actual de BTC/USDT perpetuo en Binance."""
    try:
        import ccxt
        ex = ccxt.binance({"options": {"defaultType": "swap"}})
        fr = ex.fetch_funding_rate("BTC/USDT:USDT")
        return fr.get("fundingRate")
    except Exception:
        return None


def get_market_context() -> dict:
    """
    Devuelve contexto consolidado de mercado.
    Siempre devuelve algo (defaults neutros si falla la API).
    """
    fng = get_fear_greed()
    if fng:
        value = fng["value"]
        label = fng["label"]
    else:
        value, label = 50, "Neutral"

    funding = get_btc_funding_rate()

    # Funding rate overrides F&G para crypto
    funding_block_longs  = bool(funding and funding > FUNDING_BLOCK_LONG_ABOVE)
    funding_block_shorts = bool(funding and funding < FUNDING_BLOCK_SHORT_BELOW)

    return {
        "fng_value":     value,
        "fng_label":     label,
        "funding_rate":  funding,
        "block_longs":   value > BLOCK_LONG_ABOVE  or funding_block_longs,
        "block_shorts":  value < BLOCK_SHORT_BELOW or funding_block_shorts,
        "block_reason_long":  (
            f"F&G={value} ({label})" if value > BLOCK_LONG_ABOVE else
            f"Funding={funding*100:.3f}%" if funding_block_longs else ""
        ),
        "block_reason_short": (
            f"F&G={value} ({label})" if value < BLOCK_SHORT_BELOW else
            f"Funding={funding*100:.3f}%" if funding_block_shorts else ""
        ),
    }


def is_signal_ok_for_context(direction: str, ctx: dict) -> tuple[bool, str]:
    """
    Returns (ok, reason).
    ok=True  → la señal es consistente con el contexto macro.
    ok=False → el mercado está en extremo opuesto al trade.
    """
    direc = direction.upper()
    fng   = ctx.get("fng_value", 50)
    label = ctx.get("fng_label", "Neutral")
    fr    = ctx.get("funding_rate")
    fr_str = f" | Funding={fr*100:.3f}%" if fr is not None else ""

    if direc == "LARGO" and ctx.get("block_longs"):
        reason = ctx.get("block_reason_long", f"F&G={fng}")
        return False, f"{reason}{fr_str}"
    if direc == "CORTO" and ctx.get("block_shorts"):
        reason = ctx.get("block_reason_short", f"F&G={fng}")
        return False, f"{reason}{fr_str}"

    return True, f"F&G={fng} ({label}){fr_str}"


if __name__ == "__main__":
    ctx = get_market_context()
    print(f"Fear & Greed: {ctx['fng_value']} — {ctx['fng_label']}")
    print(f"Bloquea LONGs: {ctx['block_longs']}  |  Bloquea SHORTs: {ctx['block_shorts']}")
    for d in ("LARGO", "CORTO"):
        ok, reason = is_signal_ok_for_context(d, ctx)
        print(f"  {d:5s}: {'OK' if ok else 'BLOQUEADO'} — {reason}")
