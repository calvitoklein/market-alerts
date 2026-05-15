"""
Generador de gráficas de velas para señales del bot.
Devuelve PNG bytes listos para enviar a Telegram.
"""

import io
import os
import time

import ccxt
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch

CHART_BARS    = 60    # velas 5m a mostrar
TF            = "5m"
CHART_W       = 12
CHART_H       = 7
BG_COLOR      = "#0d1117"
GRID_COLOR    = "#21262d"
TEXT_COLOR    = "#c9d1d9"

# Mapa instrumento → símbolo OHLCV en Binance spot
SYMBOL_MAP = {
    "BTC":  "BTC/USDT",
    "ETH":  "ETH/USDT",
    "BNB":  "BNB/USDT",
    "SOL":  "SOL/USDT",
    "XRP":  "XRP/USDT",
    "XAU":  "PAXG/USDT",   # proxy oro
    "PAXG": "PAXG/USDT",
}


def _get_ex():
    try:
        return ccxt.binance({"options": {"defaultType": "spot"}})
    except Exception:
        return None


def _fetch_bars(instrument: str, bars: list = None) -> list:
    """Devuelve CHART_BARS velas 5m. Usa bars si ya están disponibles."""
    if bars and len(bars) >= CHART_BARS:
        return bars[-CHART_BARS:]
    symbol = SYMBOL_MAP.get(instrument.upper())
    if not symbol:
        return []
    ex = _get_ex()
    if not ex:
        return []
    try:
        return ex.fetch_ohlcv(symbol, TF, limit=CHART_BARS + 5)[-CHART_BARS:]
    except Exception:
        return []


def _draw_candles(ax, bars: list):
    for i, b in enumerate(bars):
        ts, o, h, l, c = b[0], b[1], b[2], b[3], b[4]
        color = "#26a641" if c >= o else "#f85149"
        # cuerpo
        body_lo = min(o, c)
        body_hi = max(o, c)
        ax.add_patch(plt.Rectangle(
            (i - 0.35, body_lo), 0.70, body_hi - body_lo,
            color=color, zorder=3,
        ))
        # mecha
        ax.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=2)


def generate_signal_chart(
    instrument: str,
    direction: str,
    entrada: float,
    tp: float,
    sl: float,
    poc: float = None,
    vah: float = None,
    val: float = None,
    bars: list = None,
    title_extra: str = "",
) -> bytes | None:
    """
    Genera una gráfica de velas con los niveles de la señal superpuestos.
    Devuelve bytes PNG o None si falla.
    """
    try:
        raw_bars = _fetch_bars(instrument, bars)
        if not raw_bars or len(raw_bars) < 5:
            return None

        entrada = float(entrada)
        tp      = float(tp)
        sl      = float(sl)

        fig, (ax, ax_vol) = plt.subplots(
            2, 1, figsize=(CHART_W, CHART_H),
            gridspec_kw={"height_ratios": [4, 1]},
            facecolor=BG_COLOR,
        )
        ax.set_facecolor(BG_COLOR)
        ax_vol.set_facecolor(BG_COLOR)

        _draw_candles(ax, raw_bars)

        n = len(raw_bars)
        x_lo, x_hi = -0.5, n - 0.5

        # ── Líneas de precio ──────────────────────────────────────────────────
        entry_color = "#58a6ff"
        tp_color    = "#3fb950"
        sl_color    = "#f85149"
        poc_color   = "#e3b341"
        va_color    = "#a371f7"

        def hline(y, color, label, lw=1.2, ls="--", zorder=4):
            ax.hlines(y, x_lo, x_hi, colors=color, linewidths=lw, linestyles=ls, zorder=zorder)
            ax.text(x_hi + 0.1, y, f" {label}: {y:.2f}",
                    color=color, fontsize=7, va="center", zorder=5)

        # Zona TP/SL sombreada
        tp_side = max(entrada, tp)
        sl_side = min(entrada, sl)
        if direction.upper() in ("LARGO", "LONG"):
            ax.fill_between([x_lo, x_hi], entrada, tp, alpha=0.07, color=tp_color, zorder=1)
            ax.fill_between([x_lo, x_hi], sl, entrada, alpha=0.07, color=sl_color, zorder=1)
        else:
            ax.fill_between([x_lo, x_hi], tp, entrada, alpha=0.07, color=tp_color, zorder=1)
            ax.fill_between([x_lo, x_hi], entrada, sl, alpha=0.07, color=sl_color, zorder=1)

        hline(entrada, entry_color, "Entry", ls="-",  lw=1.5)
        hline(tp,      tp_color,    "TP",    ls="--")
        hline(sl,      sl_color,    "SL",    ls="--")

        if poc is not None:
            hline(poc, poc_color, "POC", ls=":", lw=1.0)
        if vah is not None:
            hline(vah, va_color, "VAH", ls=":", lw=0.9)
        if val is not None:
            hline(val, va_color, "VAL", ls=":", lw=0.9)

        # ── Estilo ejes ───────────────────────────────────────────────────────
        all_prices = [b[2] for b in raw_bars] + [b[3] for b in raw_bars] + [entrada, tp, sl]
        if poc: all_prices += [poc]
        if vah: all_prices += [vah]
        if val: all_prices += [val]
        pad = (max(all_prices) - min(all_prices)) * 0.08
        ax.set_ylim(min(all_prices) - pad, max(all_prices) + pad)
        ax.set_xlim(x_lo, x_hi + 2.5)
        ax.tick_params(colors=TEXT_COLOR, labelsize=7)
        ax.yaxis.tick_right()
        for spine in ax.spines.values():
            spine.set_color(GRID_COLOR)
        ax.grid(axis="y", color=GRID_COLOR, linewidth=0.5, zorder=0)
        ax.set_xticks([])

        # ── Título ────────────────────────────────────────────────────────────
        direc_label = "🟢 LARGO" if direction.upper() in ("LARGO", "LONG") else "🔴 CORTO"
        rr = abs(tp - entrada) / abs(entrada - sl) if abs(entrada - sl) > 0 else 0
        title = f"{instrument} {direc_label} @ {entrada:.2f}  |  TP {tp:.2f}  SL {sl:.2f}  R:R {rr:.1f}"
        if title_extra:
            title += f"\n{title_extra}"
        ax.set_title(title, color=TEXT_COLOR, fontsize=9, pad=6, loc="left")

        # ── Volumen ───────────────────────────────────────────────────────────
        vols   = [b[5] for b in raw_bars]
        colors = ["#26a641" if raw_bars[i][4] >= raw_bars[i][1] else "#f85149" for i in range(n)]
        ax_vol.bar(range(n), vols, color=colors, alpha=0.7, width=0.7)
        ax_vol.set_xlim(x_lo, x_hi + 2.5)
        ax_vol.set_xticks([])
        ax_vol.tick_params(colors=TEXT_COLOR, labelsize=6)
        ax_vol.yaxis.tick_right()
        for spine in ax_vol.spines.values():
            spine.set_color(GRID_COLOR)
        ax_vol.set_facecolor(BG_COLOR)
        ax_vol.set_ylabel("Vol", color=TEXT_COLOR, fontsize=7)

        # Timestamps eje x en el gráfico de volumen (cada 10 velas)
        tick_pos = list(range(0, n, 10))
        tick_labels = []
        for p in tick_pos:
            ts_ms = raw_bars[p][0]
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            tick_labels.append(dt.strftime("%H:%M"))
        ax_vol.set_xticks(tick_pos)
        ax_vol.set_xticklabels(tick_labels, color=TEXT_COLOR, fontsize=6)

        plt.tight_layout(pad=0.5)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                    facecolor=BG_COLOR, edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    except Exception as e:
        print(f"  [CHART] Error generando gráfica: {e}")
        return None


def send_chart_to_telegram(
    token: str,
    chat_id: str,
    instrument: str,
    direction: str,
    entrada,
    tp,
    sl,
    caption: str,
    poc: float = None,
    vah: float = None,
    val: float = None,
    bars: list = None,
    title_extra: str = "",
) -> bool:
    """Genera y envía gráfica de señal al chat de Telegram. Devuelve True si OK."""
    import requests as _req
    png = generate_signal_chart(
        instrument=instrument,
        direction=direction,
        entrada=entrada,
        tp=tp,
        sl=sl,
        poc=poc,
        vah=vah,
        val=val,
        bars=bars,
        title_extra=title_extra,
    )
    if not png:
        return False
    try:
        r = _req.post(
            f"https://api.telegram.org/bot{token}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption[:1024]},
            files={"photo": ("chart.png", png, "image/png")},
            timeout=15,
        )
        return r.ok
    except Exception as e:
        print(f"  [CHART] Error enviando a Telegram: {e}")
        return False
