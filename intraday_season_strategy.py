"""
BTC Intraday Hour Seasonality
Fuente: QuantPedia — "The Seasonality of Bitcoin" (2015-2023)

Lógica: LONG BTC a las 21:00 UTC (cierre NYSE), salida a las 23:00 UTC.
Sharpe 2.23 | DD -7.6% | Total +56.9% en 2 años | WR 54.5%

Gestión de estado: season_state.json
  - 1 operación por día máximo
  - SL de seguridad a -1.5% (para crashs nocturnos)
  - Salida primaria: tiempo (23:00 UTC)
"""

import os
import json
import hashlib
import ccxt

from datetime import datetime, timezone

DIR        = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(DIR, "season_state.json")

SYMBOL      = "BTC/USDT:USDT"
INSTR       = "BTC"
ENTRY_H     = 21    # UTC
EXIT_H      = 23    # UTC
ENTRY_WIN   = 6     # minutos desde ENTRY_H en los que se puede entrar
SL_PCT      = 0.015 # -1.5% stop de seguridad

HANDLE = "SeasonBTC"
NAME   = "BTC 21h-23h UTC Seasonality"


# ── Estado ────────────────────────────────────────────────────────────────────

def _load() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save(s: dict):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(s, f, indent=2)
    except Exception:
        pass


# ── Lógica de ventana ─────────────────────────────────────────────────────────

def should_enter(now: datetime) -> bool:
    if now.hour != ENTRY_H or now.minute >= ENTRY_WIN:
        return False
    s = _load()
    # Ya operamos hoy o hay trade abierto
    if s.get("open") or s.get("date") == now.strftime("%Y-%m-%d"):
        return False
    return True

def should_exit(now: datetime) -> bool:
    s = _load()
    if not s.get("open"):
        return False
    return now.hour >= EXIT_H

def mark_closed():
    s = _load()
    s["open"] = False
    _save(s)


# ── Generador de señal ────────────────────────────────────────────────────────

def get_entry_signal(now: datetime) -> dict | None:
    # Obtener precio actual para calcular SL
    try:
        ex = ccxt.binance({"options": {"defaultType": "spot"}})
        price = ex.fetch_ticker("BTC/USDT")["last"]
        sl_val = str(round(price * (1 - SL_PCT), 2))
    except Exception:
        sl_val = "N/A"

    sig_id = "season_" + hashlib.md5(
        f"BTC_LARGO_{now.strftime('%Y%m%d')}".encode()
    ).hexdigest()[:12]

    _save({
        "open":       True,
        "date":       now.strftime("%Y-%m-%d"),
        "entry_time": now.isoformat(),
        "sig_id":     sig_id,
    })

    return {
        "ts":               now.isoformat(),
        "id":               sig_id,
        "handle":           HANDLE,
        "name":             NAME,
        "link":             "",
        "img":              "",
        "text":             (
            f"[SEASONALITY] BTC LARGO @ MERCADO | "
            f"Ventana 21h→23h UTC | Sharpe 2.23 | SL {sl_val}"
        ),
        "INSTRUMENTO":      INSTR,
        "DIRECCION":        "LARGO",
        "ENTRADA":          "MERCADO",
        "TP":               "N/A",       # salida primaria por tiempo a las 23h
        "SL":               sl_val,
        "CONFIANZA_TRADER": "ALTA",
        "HORIZONTE":        "SCALP",
        "RESUMEN":          "Seasonality BTC 21h→23h UTC — cierre en 2h",
    }


# ── Entry point (diagnóstico) ─────────────────────────────────────────────────

def generate_signals() -> list:
    now = datetime.now(timezone.utc)
    if not should_enter(now):
        return []
    sig = get_entry_signal(now)
    return [sig] if sig else []
