"""
Señales de operaciones con información privilegiada pública (100% legal).

Fuentes:
  1. Congresistas USA (STOCK Act) — QuiverQuant free API
     Obligados a declarar operaciones en 45 días. Nancy Pelosi, etc.
  2. Insiders corporativos (Form 4 SEC) — OpenInsider cluster buys
     CEOs/directores deben declarar en 2 días hábiles. Cluster = 3+ insiders mismo ticker.

Uso standalone:
    python insider_signals.py            # últimas 3 días
    python insider_signals.py --days 7   # última semana
"""

import os, sys, re, json, time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from typing import Optional

DIR = os.path.dirname(os.path.abspath(__file__))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Sectores / tickers relevantes para nuestros instrumentos
RELEVANT_SECTORS = {
    "semiconductors", "electronic computers", "computer", "software",
    "telecommunications", "pharmaceutical", "biotechnology",
    "crude petroleum", "oil", "natural gas", "gold", "mining",
    "investment trusts", "finance", "banking",
    "defense", "aerospace", "national security",
}

RELEVANT_TICKERS = {
    # Tech / NQ proxies
    "NVDA", "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "TSLA",
    "AMD", "INTC", "AVGO", "QCOM", "ORCL", "CRM", "ADBE", "NFLX",
    "ASML", "TSM", "MU", "AMAT", "LRCX", "KLAC", "PANW", "SNOW",
    "QQQ", "SPY", "IWM", "TQQQ", "SQQQ",
    # BTC proxies
    "COIN", "MSTR", "RIOT", "MARA", "CLSK", "HUT",
    # Gold / XAU proxies
    "GLD", "IAU", "NEM", "GOLD", "AEM", "RGLD", "WPM",
    # Energy
    "XOM", "CVX", "OXY", "COP", "SLB",
    # Defense
    "LMT", "RTX", "NOC", "GD", "BA", "HII",
    # Finance / macro
    "JPM", "GS", "BAC", "MS", "BRK-B", "V", "MA", "SPGI",
    # Indices / ETFs
    "VOO", "VTI", "XLK", "XLF", "SMH",
}

# Congresistas conocidos por sus buenos resultados
STAR_TRADERS = {
    "Nancy Pelosi", "Paul Pelosi",
    "Dan Crenshaw", "Michael McCaul",
    "Ro Khanna", "Austin Scott",
    "Tommy Tuberville", "Markwayne Mullin",
}

# ── QuiverQuant: operaciones de congresistas ──────────────────────────────────

QUIVER_URL = "https://api.quiverquant.com/beta/live/congresstrading"

def fetch_congress_trades(days: int = 3, min_amount: float = 1_000) -> list[dict]:
    """
    Devuelve operaciones de congresistas reportadas en los últimos `days` días.
    Filtra por compras (Purchase) y tickers/sectores relevantes.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        r = requests.get(QUIVER_URL, headers={"User-Agent": "monitor@example.com",
                                               "Accept": "application/json"}, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []

    results = []
    for trade in data:
        # Solo compras (purchases son más predictivas que ventas)
        tx = trade.get("Transaction", "")
        if "Purchase" not in tx and "purchase" not in tx:
            continue

        # Fecha del reporte (cuando fue declarado)
        try:
            report_dt = datetime.strptime(trade["ReportDate"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if report_dt < cutoff:
            continue

        ticker = trade.get("Ticker", "").upper()
        amount = _parse_range(trade.get("Range", ""))
        if amount < min_amount:
            continue

        # Filtrar tickers relevantes
        is_relevant = ticker in RELEVANT_TICKERS

        results.append({
            "_source":      "congress",
            "_author":      "congress_trade",
            "_ts":          report_dt,
            "title":        _congress_summary(trade),
            "summary":      _congress_summary(trade),
            "representative": trade.get("Representative", "?"),
            "party":        trade.get("Party", "?"),
            "ticker":       ticker,
            "transaction":  tx,
            "amount":       _parse_range(trade.get("Range", "")),
            "amount_raw":   trade.get("Range", "?"),
            "report_date":  trade.get("ReportDate", "?"),
            "trade_date":   trade.get("TransactionDate", "?"),
            "house":        trade.get("House", "?"),
            "is_relevant":  is_relevant,
            "is_star":      any(s in trade.get("Representative","") for s in STAR_TRADERS),
            "excess_return": trade.get("ExcessReturn"),
        })

    return results

def _congress_summary(t: dict) -> str:
    rep  = t.get("Representative", "?")
    tx   = t.get("Transaction", "?")
    tick = t.get("Ticker", "?")
    rng  = t.get("Range", "?")
    rd   = t.get("ReportDate", "?")
    td   = t.get("TransactionDate", "?")
    return f"CONGRESS {tx}: {rep} compró {tick} ({rng}) — trade {td}, reportado {rd}"

# ── OpenInsider: cluster buys ─────────────────────────────────────────────────

OPENINSIDER_CLUSTER_URL = "http://openinsider.com/latest-cluster-buys"
OPENINSIDER_SCREENER_URL = (
    "http://openinsider.com/screener?"
    "s=&o=&pl=5&ph=&ll=&lh=&fd=2&xs=1&vl=100"
    "&isofficer=1&isceo=1&iscfo=1&isdirector=1"
    "&action=0"
)

def fetch_insider_cluster_buys(days: int = 2, min_insiders: int = 2) -> list[dict]:
    """
    Devuelve cluster buys de OpenInsider: grupos de 2+ insiders comprando el mismo ticker.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        r = requests.get(OPENINSIDER_CLUSTER_URL, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table", class_="tinytable")
        if not table:
            return []
        rows = table.find_all("tr")
    except Exception:
        return []

    results = []
    headers_row = rows[0] if rows else None
    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 9:
            continue

        # Columns: X | Filing Date | Trade Date | Ticker | Company | Industry | Ins | Type | Price | Qty
        try:
            filing_date_str = cells[1]        # e.g. "2026-05-01 16:34:45"
            trade_date_str  = cells[2]        # e.g. "2026-04-30"
            ticker          = cells[3].upper()
            company         = cells[4]
            industry        = cells[5].lower()
            n_insiders      = int(cells[6]) if cells[6].isdigit() else 0
            trade_type      = cells[7]
            price_str       = cells[8].replace(",", "").replace("$", "")
            qty_str         = cells[9].replace(",", "").replace("+", "") if len(cells) > 9 else ""

            # Parse date
            dt = datetime.strptime(filing_date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        if dt < cutoff:
            continue
        if n_insiders < min_insiders:
            continue
        if "P - Purchase" not in trade_type and "purchase" not in trade_type.lower():
            continue

        try:
            price = float(price_str) if price_str else None
            qty   = int(float(qty_str)) if qty_str else None
            value = round(price * qty, 0) if price and qty else None
        except Exception:
            price = qty = value = None

        is_relevant = ticker in RELEVANT_TICKERS or any(
            s in industry for s in RELEVANT_SECTORS
        )

        summary = (
            f"CLUSTER BUY: {n_insiders} insiders compraron {ticker} ({company}) "
            f"@ ${price_str} x{qty_str} = ${value:,.0f}" if value else
            f"CLUSTER BUY: {n_insiders} insiders compraron {ticker} ({company}) @ ${price_str}"
        )

        results.append({
            "_source":     "insider_cluster",
            "_author":     "openinsider",
            "_ts":         dt,
            "title":       summary,
            "summary":     summary,
            "ticker":      ticker,
            "company":     company,
            "industry":    industry,
            "n_insiders":  n_insiders,
            "trade_type":  trade_type,
            "price":       price,
            "qty":         qty,
            "value":       value,
            "trade_date":  trade_date_str,
            "is_relevant": is_relevant,
        })

    return results

def fetch_insider_purchases(days: int = 1, min_value_k: float = 100) -> list[dict]:
    """
    Compras individuales significativas de CEOs/directores (>$100k).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        r = requests.get(OPENINSIDER_SCREENER_URL, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table", class_="tinytable")
        if not table:
            return []
        rows = table.find_all("tr")
    except Exception:
        return []

    results = []
    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 12:
            continue
        try:
            # Columns: X|FilingDate|TradeDate|Ticker|Company|Insider|Title|TradeType|Price|Qty|Owned|ΔOwn|Value|...
            filing_str = cells[1]
            ticker     = cells[3].upper()
            company    = cells[4]
            insider    = cells[5]
            title      = cells[6]
            trade_type = cells[7]
            price_str  = cells[8].replace(",","").replace("$","")
            qty_str    = cells[9].replace(",","").replace("+","")
            value_str  = cells[12].replace(",","").replace("$","").replace("+","") if len(cells) > 12 else ""

            dt = datetime.strptime(filing_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            value_k = abs(float(value_str)) / 1000 if value_str else 0
        except Exception:
            continue

        if dt < cutoff:
            continue
        if "P - Purchase" not in trade_type:
            continue
        if value_k < min_value_k:
            continue
        if ticker not in RELEVANT_TICKERS:
            continue

        results.append({
            "_source":   "insider_purchase",
            "_author":   "openinsider",
            "_ts":       dt,
            "title":     f"INSIDER BUY: {title} de {company} ({ticker}) compró ${value_k:.0f}k",
            "summary":   f"INSIDER BUY: {insider} ({title}) compró {ticker} ({company}) ${value_k:.0f}k @ ${price_str}",
            "ticker":    ticker,
            "company":   company,
            "insider":   insider,
            "role":      title,
            "value_k":   value_k,
            "is_relevant": True,
        })

    return results

# ── Combinar fuentes ──────────────────────────────────────────────────────────

def fetch_all_insiders(days: int = 2) -> list[dict]:
    """Devuelve todas las señales insider de las últimas `days` días."""
    results = []

    congress = fetch_congress_trades(days=days)
    results.extend(congress)

    clusters = fetch_insider_cluster_buys(days=days)
    results.extend(clusters)

    purchases = fetch_insider_purchases(days=days)
    results.extend(purchases)

    return results

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_range(range_str: str) -> float:
    """Devuelve el punto medio de un rango como '$15,001 - $50,000'."""
    if not range_str:
        return 0
    nums = re.findall(r"[\d,]+", range_str)
    vals = []
    for n in nums:
        try:
            vals.append(float(n.replace(",", "")))
        except Exception:
            pass
    if len(vals) >= 2:
        return (vals[0] + vals[1]) / 2
    return vals[0] if vals else 0

# ── Construcción de mensajes Telegram ────────────────────────────────────────

def build_congress_alert(trade: dict) -> str:
    rep    = trade.get("representative", "?")
    party  = trade.get("party", "?")
    ticker = trade.get("ticker", "?")
    amount = trade.get("amount_raw", "?")
    tdate  = trade.get("trade_date", "?")
    rdate  = trade.get("report_date", "?")
    excess = trade.get("excess_return")
    star   = "⭐ " if trade.get("is_star") else ""
    party_emoji = "🔵" if party == "D" else "🔴" if party == "R" else "⚪"

    excess_str = ""
    if excess is not None:
        sign = "+" if excess >= 0 else ""
        excess_str = f"\n📈 Retorno excess vs SPY: <b>{sign}{excess:.1f}%</b>"

    return (
        f"🏛️ <b>CONGRESISTA COMPRA</b>\n\n"
        f"{star}{party_emoji} <b>{rep}</b>\n"
        f"🎯 Compró <b>${ticker}</b> — {amount}\n"
        f"📅 Operación: {tdate} | Declarado: {rdate}"
        f"{excess_str}"
    )

def build_cluster_alert(trade: dict) -> str:
    ticker    = trade.get("ticker", "?")
    company   = trade.get("company", "?")
    n         = trade.get("n_insiders", "?")
    value     = trade.get("value")
    price     = trade.get("price")
    tdate     = trade.get("trade_date", "?")
    val_str   = f"${value:,.0f}" if value else "?"
    price_str = f"${price:,.2f}" if price else "?"

    return (
        f"🏢 <b>CLUSTER BUY DE INSIDERS</b>\n\n"
        f"<b>{n} ejecutivos</b> compraron <b>${ticker}</b> ({company})\n"
        f"💰 Total: <b>{val_str}</b> @ {price_str}\n"
        f"📅 Operación: {tdate}"
    )

# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--source", default="all", choices=["all","congress","insider"])
    args = parser.parse_args()

    print(f"Fetching insider signals (últimos {args.days} días)...\n")

    if args.source in ("all", "congress"):
        print("=== CONGRESISTAS ===")
        trades = fetch_congress_trades(days=args.days)
        relevant = [t for t in trades if t["is_relevant"]]
        others   = [t for t in trades if not t["is_relevant"]]
        print(f"  Total: {len(trades)} | Relevantes (nuestros tickers): {len(relevant)}")
        for t in relevant[:10]:
            star = "⭐ " if t["is_star"] else "   "
            print(f"  {star}{t['representative']} ({t['party']}) — {t['ticker']} {t['amount_raw']} [{t['trade_date']}]")
        if others:
            print(f"  ... y {len(others)} compras en otros tickers")
        print()

    if args.source in ("all", "insider"):
        print("=== CLUSTER BUYS (INSIDERS) ===")
        clusters = fetch_insider_cluster_buys(days=args.days)
        print(f"  Total: {len(clusters)}")
        for c in clusters[:10]:
            rel = "✓" if c["is_relevant"] else " "
            val = f"${c['value']:,.0f}" if c.get("value") else "?"
            print(f"  [{rel}] {c['n_insiders']} insiders | {c['ticker']} ({c['company'][:30]}) | {val} | {c['trade_date']}")
        print()

        print("=== COMPRAS INDIVIDUALES GRANDES (CEO/DIR >$100k) ===")
        purch = fetch_insider_purchases(days=args.days)
        print(f"  Total: {len(purch)}")
        for p in purch[:10]:
            print(f"  {p['ticker']} | {p['role']} | ${p['value_k']:.0f}k")
