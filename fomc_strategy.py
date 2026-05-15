"""
FOMC Straddle Strategy — BTC

Backtest 2022-2025 (26 fechas):
  Win rate: 46%  |  PnL medio: +0.508%/trade  |  Solo 1 SL en 26 fechas
  Mismo mecanismo que CPI straddle: bracket de stops T-10 min antes del anuncio.

Anuncios FOMC: 18:00 UTC (14:00 ET) — días variables, ~8/año
"""

from datetime import datetime, timezone, timedelta

# Calendario FOMC 2025-2026 (18:00 UTC)
FOMC_SCHEDULE = [
    (2025, 6, 18, 18, 0),
    (2025, 7, 30, 18, 0),
    (2025, 9, 17, 18, 0),
    (2025, 10, 29, 18, 0),
    (2025, 12, 10, 18, 0),
    (2026, 1, 28, 18, 0),
    (2026, 3, 18, 18, 0),
    (2026, 4, 29, 18, 0),
    (2026, 6, 17, 18, 0),
    (2026, 7, 29, 18, 0),
    (2026, 9, 16, 18, 0),
    (2026, 10, 28, 18, 0),
    (2026, 12, 9,  18, 0),
]

BTC_STOP_PCT  = 0.006   # ±0.6% (idéntico al CPI straddle validado)
BTC_TP1_MULT  = 2.0     # TP1 = 2R = 1.2%
WINDOW_OPEN   = 10      # minutos antes del anuncio
WINDOW_CLOSE  = 5       # minutos — si ya pasó el dato, no entrar
TIMEOUT_MIN   = 90


def next_fomc_dt() -> datetime | None:
    now = datetime.now(timezone.utc)
    for y, mo, d, h, mn in sorted(FOMC_SCHEDULE):
        dt = datetime(y, mo, d, h, mn, tzinfo=timezone.utc)
        if dt > now:
            return dt
    return None


def is_straddle_window() -> tuple[bool, datetime | None]:
    """True si estamos entre T-10 y T-5 minutos antes del FOMC."""
    now = datetime.now(timezone.utc)
    for y, mo, d, h, mn in FOMC_SCHEDULE:
        dt = datetime(y, mo, d, h, mn, tzinfo=timezone.utc)
        mins_to = (dt - now).total_seconds() / 60
        if WINDOW_CLOSE <= mins_to <= WINDOW_OPEN:
            return True, dt
    return False, None


def build_straddle(price_cache: dict) -> list:
    """
    Genera dos señales (LARGO stop + CORTO stop) para BTC.
    Devuelve lista de señales en el mismo formato que el buffer de tweet_monitor.
    """
    in_win, fomc_dt = is_straddle_window()
    if not in_win or not fomc_dt:
        return []

    btc_price = price_cache.get("BTC")
    if not btc_price:
        return []

    fomc_tag  = fomc_dt.strftime("%Y%m%d")
    buy_stop  = round(btc_price * (1 + BTC_STOP_PCT), 2)
    sell_stop = round(btc_price * (1 - BTC_STOP_PCT), 2)
    tp_long   = round(buy_stop  * (1 + BTC_STOP_PCT * BTC_TP1_MULT), 2)
    tp_short  = round(sell_stop * (1 - BTC_STOP_PCT * BTC_TP1_MULT), 2)

    signals = []
    for direc, entrada, tp, sl in [
        ("LARGO", buy_stop,  tp_long,  sell_stop),
        ("CORTO", sell_stop, tp_short, buy_stop),
    ]:
        signals.append({
            "id":             f"fomc_BTC_{direc}_{fomc_tag}",
            "ts":             datetime.now(timezone.utc).isoformat(),
            "handle":         "fomc_straddle",
            "name":           "FOMC Straddle",
            "INSTRUMENTO":    "BTC",
            "DIRECCION":      direc,
            "ENTRADA":        str(entrada),
            "TP":             str(tp),
            "SL":             str(sl),
            "HORIZONTE":      "SCALP",
            "CONFIANZA_TRADER": "ALTA",
            "RESUMEN":        f"FOMC straddle BTC {direc} — {fomc_dt.strftime('%H:%M UTC')}",
            "_fomc_straddle": True,
        })
    return signals


def minutes_to_next_fomc() -> float | None:
    dt = next_fomc_dt()
    if not dt:
        return None
    return (dt - datetime.now(timezone.utc)).total_seconds() / 60
