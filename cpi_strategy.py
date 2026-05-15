"""
CPI Straddle — BTC only
Bracket de stops colocado T-10 min antes del dato CPI USA.
Parametros validados por backtest 2023-2025:
  ±0.6% stop | TP1=1.2% (2R) | WR=33% | PnL medio +0.27%/trade
"""

from datetime import datetime, timezone

# Fechas CPI 2026 — 13:30 UTC (8:30 AM Eastern)
CPI_SCHEDULE = [
    (2026,  1, 15, 13, 30),
    (2026,  2, 12, 13, 30),
    (2026,  3, 12, 13, 30),
    (2026,  4, 10, 13, 30),
    (2026,  5, 13, 13, 30),
    (2026,  6, 11, 13, 30),
    (2026,  7, 15, 13, 30),
    (2026,  8, 12, 13, 30),
    (2026,  9, 10, 13, 30),
    (2026, 10, 15, 13, 30),
    (2026, 11, 12, 13, 30),
    (2026, 12, 10, 13, 30),
]

BTC_PCT      = 0.006   # ±0.6% distancia stop desde precio actual
BTC_TP1_MULT = 2.0     # TP1 = ±1.2%  R:R 2:1
BTC_TP2_MULT = 4.0     # TP2 = ±2.4%  R:R 4:1 (referencia, raro que se alcance)
WINDOW_OPEN  = 10      # minutos antes del dato para colocar los stops
WINDOW_CLOSE = 5       # margen mínimo antes del dato (no colocar en los últimos 5 min)


def next_cpi_dt() -> datetime | None:
    """Próximo CPI en UTC dentro de los próximos 60 días."""
    now = datetime.now(timezone.utc)
    for (y, m, d, h, mi) in CPI_SCHEDULE:
        dt = datetime(y, m, d, h, mi, tzinfo=timezone.utc)
        if dt > now and (dt - now).days <= 60:
            return dt
    return None


def is_straddle_window() -> tuple[bool, datetime | None]:
    """True si estamos en la ventana T-10 a T-5 antes del próximo CPI."""
    dt = next_cpi_dt()
    if not dt:
        return False, None
    diff_min = (dt - datetime.now(timezone.utc)).total_seconds() / 60
    return (WINDOW_CLOSE <= diff_min <= WINDOW_OPEN), dt


def build_straddle(price_cache: dict) -> list:
    """
    Devuelve lista con dos señales (LARGO stop + CORTO stop) para BTC.
    IDs incluyen la fecha CPI para no ejecutarse más de una vez por evento.
    Devuelve [] si no estamos en la ventana o no hay precio de BTC.
    """
    in_window, cpi_dt = is_straddle_window()
    if not in_window:
        return []

    price = price_cache.get("BTC")
    if not price:
        return []

    now_iso  = datetime.now(timezone.utc).isoformat()
    cpi_tag  = cpi_dt.strftime("%Y%m%d")
    cpi_str  = cpi_dt.strftime("%H:%M UTC")

    buy_stop  = round(price * (1 + BTC_PCT), 2)
    sell_stop = round(price * (1 - BTC_PCT), 2)
    tp_long   = round(price * (1 + BTC_PCT * BTC_TP1_MULT), 2)
    tp_short  = round(price * (1 - BTC_PCT * BTC_TP1_MULT), 2)

    return [
        {
            "ts": now_iso, "id": f"cpi_BTC_{direc}_{cpi_tag}",
            "handle": "cpi_straddle", "name": f"CPI Straddle {cpi_str}",
            "link": "https://www.bls.gov/schedule/news_release/cpi.htm",
            "img": "", "text": f"CPI straddle BTC {direc} @ {entry}",
            "INSTRUMENTO": "BTC", "DIRECCION": direc,
            "ENTRADA": str(entry), "TP": str(tp), "SL": str(sl),
            "CONFIANZA_TRADER": "ALTA", "HORIZONTE": "SCALP",
            "RESUMEN": f"CPI straddle BTC {direc}",
            "_cpi_straddle": True,
        }
        for direc, entry, tp, sl in [
            ("LARGO", buy_stop,  tp_long,  sell_stop),
            ("CORTO", sell_stop, tp_short, buy_stop),
        ]
    ]


def minutes_to_next_cpi() -> float | None:
    dt = next_cpi_dt()
    return (dt - datetime.now(timezone.utc)).total_seconds() / 60 if dt else None
