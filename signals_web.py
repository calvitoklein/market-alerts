"""
Fuentes web de señales de trading (complemento a Twitter).

Fuentes implementadas:
  1. TradingView Ideas RSS  — ideas publicadas por la comunidad TV
  2. StockTwits             — mensajes con sentimiento por símbolo
  3. Telegram público       — canales públicos con señales (t.me/s/CANAL)

Cada fuente devuelve mensajes normalizados al mismo formato que twitter_api.py:
  {
    "title":   str,           # texto del mensaje
    "summary": str,
    "_ts":     datetime UTC,
    "_source": str,           # "tradingview" | "stocktwits" | "telegram"
    "_author": str,           # handle / nombre del autor
    "_images": list[str],     # URLs de imágenes (si las hay)
  }

Uso standalone:
    python signals_web.py          # imprime muestras de cada fuente
    python signals_web.py --all    # muestra todos los mensajes
"""

import os, sys, json, time, re
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from typing import Iterator

DIR = os.path.dirname(os.path.abspath(__file__))

# ── Configuración de instrumentos ─────────────────────────────────────────────

# TradingView RSS por instrumento (syminfo.ticker format)
TV_SYMBOLS = {
    "BTC":  "COINBASE:BTCUSD",
    "ETH":  "COINBASE:ETHUSD",
    "NQ":   "CME_MINI:NQ1!",
    "ES":   "CME_MINI:ES1!",
    "XAU":  "COMEX:GC1!",
    "XAG":  "COMEX:SI1!",
    "DXY":  "TVC:DXY",
    "OIL":  "NYMEX:CL1!",
}

# StockTwits symbols
ST_SYMBOLS = {
    "BTC":  "BTC.X",
    "ETH":  "ETH.X",
    "NQ":   "QQQ",       # proxy NQ
    "ES":   "SPY",       # proxy ES
    "XAU":  "GLD",       # proxy XAU
    "GOLD": "GLD",
    "TSLA": "TSLA",
    "NVDA": "NVDA",
}

# Canales públicos de Telegram por categoria de par en BitGet
TELEGRAM_CHANNELS = {

    # ══════════════════════════════════════════════════════════════════════════
    # METALES — XAU (Gold) · XAG (Silver) · COPPER · NATGAS · PAXG · XPT · XPD
    # ══════════════════════════════════════════════════════════════════════════
    "FX Leaders XAU":        "FXLeaders",
    "Metatrader Signals":    "metatrader_signals",
    "Biaheza Signals":       "biaheza_signals",
    "ICT Traders":           "ict_traders",
    "XAUUSD Signals":        "xauusd_signals",
    "Gold Forex VIP":        "gold_forex_signals_vip",
    "Gold Signal Channel":   "goldsignalchannel",
    "XAU Free Signals":      "xauusd_free_signals",
    "Gold Trade Signals":    "goldtradesignals",
    "Smart Money Gold":      "smartmoneygoldsignals",
    "ICT Gold":              "ict_gold_signals",
    "FX Gold Trade":         "fxgoldtrade",
    "XAU Analysis":          "xauusd_analysis",
    "Gold Pips":             "goldpipssignals",
    "XAU VIP":               "xauvipsignals",
    "Gold SMC Signals":      "goldsmc_signals",
    "XAU Daily Setup":       "xaudailysetup",
    "Silver Signals":        "silvertradesignals",
    "XAG Traders":           "xagtraders",
    "Metals Trading":        "metalstrading",
    "Commodities Signals":   "commoditiessignals",

    # ══════════════════════════════════════════════════════════════════════════
    # L1 CRYPTO — BTC · ETH · SOL · BNB · XRP · ADA · AVAX · TON · TRX · DOT
    # ══════════════════════════════════════════════════════════════════════════
    "BTC Signals":           "btcsignalschannel",
    "Crypto Signals":        "Cryptosignals",
    "Binance Killers":       "BinanceKillers",
    "Crypto Futures":        "cryptofuturessignals",
    "ETH Signals":           "eth_signals_free",
    "Crypto VIP":            "cryptovipsignals",
    "BTC ETH Alerts":        "btcethalerts",
    "BTC Scalp Signals":     "btcscalpsignals",
    "BTC Whale Signals":     "btcwhalesignals",
    "ETH BTC Scalp":         "ethbtcscalp",
    "Crypto Entry Signals":  "cryptoentrysignals",
    "Bitcoin Trading":       "bitcoin_trading_signals",
    "ETH Whales":            "eth_whale_signals",
    "SOL Signals":           "solana_signals_free",
    "Solana Trading":        "solanatrading",
    "SOL Daily Entries":     "soldailyentries",
    "BNB Signals":           "bnbsignalsfree",
    "XRP Signals":           "xrpsignalsfree",
    "XRP Trading":           "xrptradingsignals",
    "ADA Signals":           "adasignalsfree",
    "AVAX Signals":          "avaxsignals",
    "TON Signals":           "tonsignalsfree",
    "TRX Trading":           "trxtradingsignals",

    # ══════════════════════════════════════════════════════════════════════════
    # L2 / INFRAESTRUCTURA — ARB · OP · SUI · SEI · INJ · NEAR · APT · TIA · ZK
    # ══════════════════════════════════════════════════════════════════════════
    "ARB Signals":           "arbitrumsignals",
    "OP Signals":            "optimismsignals",
    "SUI Trading":           "suitradingsignals",
    "INJ Signals":           "injectivesignals",
    "Layer2 Signals":        "layer2tradingsignals",
    "SEI Signals":           "seisignalsfree",
    "NEAR Signals":          "nearsignals",
    "APT Signals":           "aptossignals",
    "L2 Scalp":              "l2scalptrading",

    # ══════════════════════════════════════════════════════════════════════════
    # DEFI — AAVE · UNI · LINK · CRV · GMX · JUP · PENDLE · DYDX · RUNE
    # ══════════════════════════════════════════════════════════════════════════
    "DeFi Signals":          "defisignalsfree",
    "LINK Signals":          "chainlinksignals",
    "AAVE Trading":          "aavetrading",
    "DeFi Scalp":            "defiscalptrading",
    "GMX Signals":           "gmxtradingsignals",
    "Altcoin Signals":       "altcoinsignals_free",
    "Altcoin Daily Sig":     "altcoindailysignals",
    "Altcoin Entries":       "altcoinentriessignals",

    # ══════════════════════════════════════════════════════════════════════════
    # AI TOKENS — TAO · FET · RENDER · WLD · VIRTUAL · GRT · IO · CGPT
    # ══════════════════════════════════════════════════════════════════════════
    "AI Crypto Signals":     "aicryptosignals",
    "TAO Signals":           "bittensor_signals",
    "AI Token Trading":      "aitokentrade",
    "WLD Signals":           "worldcoinsignals",
    "RENDER Trading":        "rendertokensignals",
    "AI Altcoins":           "aialtcoinsignals",

    # ══════════════════════════════════════════════════════════════════════════
    # MEMECOINS — DOGE · SHIB · PEPE · WIF · BONK · FLOKI · BRETT · PNUT
    # ══════════════════════════════════════════════════════════════════════════
    "Meme Signals":          "memecoin_signals",
    "PEPE Signals":          "pepesignalsfree",
    "DOGE Signals":          "dogesignalsfree",
    "WIF Signals":           "wifsignals",
    "Meme Scalp":            "memescalptrading",
    "Degen Signals":         "degensignalsfree",
    "Meme Pumps":            "memepumptrading",

    # ══════════════════════════════════════════════════════════════════════════
    # SOL ECOSYSTEM — JUP · RAY · DRIFT · PYTH · ORCA · KMNO
    # ══════════════════════════════════════════════════════════════════════════
    "Solana Ecosystem":      "solecosystemtrading",
    "JUP Signals":           "jupitersignals",
    "Raydium Signals":       "raydiumtrading",
    "Sol DeFi Signals":      "soldefi_signals",

    # ══════════════════════════════════════════════════════════════════════════
    # ACCIONES PERPETUAS BitGet — NVDA · TSLA · AAPL · META · GOOGL · MSFT
    #   AMD · COIN · MSTR · PLTR · AMZN · NFLX · QQQ · SPY · TQQQ
    # ══════════════════════════════════════════════════════════════════════════
    "Stock Perps Signals":   "stockperpetuals",
    "Crypto Stocks Trading": "cryptostockstrading",
    "NVDA Trading":          "nvdasignalsfree",
    "TSLA Signals":          "tslasignalsfree",
    "Tech Stocks Signals":   "techstockssignals",
    "Mag7 Trading":          "mag7tradingsignals",
    "Stock Scalp Crypto":    "stockscalpcrypto",

    # ══════════════════════════════════════════════════════════════════════════
    # GENERAL ALTCOINS (covers 400+ otros pares en BitGet)
    # ══════════════════════════════════════════════════════════════════════════
    "Crypto Quant":          "CryptoQuantSignals",
    "Crypto Premium Sig":    "cryptopremiumsignalsfree",
    "Quant Signals":         "quant_signals",
    "United Signals FX":     "UnitedSignalsFX",
    "Signal Factory":        "SignalFactory",
    "100x Gems":             "hundredxgems",
    "Altcoin Gems":          "altcoingemssignals",
    "Low Cap Gems":          "lowcapgemsfree",
    "Binance Altcoins":      "binancealtcoinsignals",
    "Crypto Pump Signals":   "cryptopumpsignalsfree",

    # ══════════════════════════════════════════════════════════════════════════
    # INDICES NQ / ES (alertas contextuales — no ejecutan en BitGet)
    # ══════════════════════════════════════════════════════════════════════════
    "ES Signals":            "es_signals",
    "Nasdaq Traders":        "NasdaqTraders",
    "NQ Scalp":              "nqscalp",

    # ── Indices NQ / ES ──────────────────────────────────────────────────────
    "Quant Signals":        "quant_signals",
    "ES Signals":           "es_signals",
    "Nasdaq Traders":       "NasdaqTraders",
    "Futures Traders":      "futurestraders",
    "Day Trade Signals":    "daytradesignals",
    "NQ Scalp":             "nqscalp",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── TradingView RSS ───────────────────────────────────────────────────────────

def _parse_tv_date(entry) -> datetime | None:
    t = entry.get("published_parsed")
    if t:
        return datetime(*t[:6], tzinfo=timezone.utc)
    try:
        import email.utils
        return email.utils.parsedate_to_datetime(entry["published"]).astimezone(timezone.utc)
    except Exception:
        return None

def fetch_tradingview(symbols: list[str] = None, max_per_symbol: int = 30) -> list[dict]:
    symbols = symbols or list(TV_SYMBOLS.keys())
    results = []
    seen_ids = set()
    base_url = "https://www.tradingview.com/feed/?section=ideasfeed"

    for sym in symbols:
        tv_sym = TV_SYMBOLS.get(sym.upper())
        # TV RSS feed with symbol filter
        url = f"https://www.tradingview.com/feed/?section=ideas&sym={tv_sym}" if tv_sym else base_url
        try:
            feed = feedparser.parse(url, request_headers=HEADERS)
            for entry in feed.entries[:max_per_symbol]:
                eid = entry.get("id") or entry.get("link", "")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)

                title   = entry.get("title", "")
                content = (entry.get("content") or [{}])[0].get("value", "") or entry.get("summary", "")
                ts      = _parse_tv_date(entry)
                link    = entry.get("link", "")

                # Extract author from URL: .../chart/SYMBOL/ID-AUTHOR-slug/
                author_match = re.search(r"/chart/[^/]+/[^-]+-([^/]+?)(?:-\d+)?/", link)
                author = author_match.group(1) if author_match else entry.get("author", "tv_user")

                # Extract image URL from content
                images = re.findall(r'https://www\.tradingview\.com/x/\S+?(?:\.png|/)', content)

                results.append({
                    "title":   title,
                    "summary": f"{title}\n{_clean_html(content[:800])}",
                    "_ts":     ts,
                    "_source": "tradingview",
                    "_author": author,
                    "_images": images,
                    "_link":   link,
                    "_symbol": sym,
                })
        except Exception:
            pass
        time.sleep(0.3)

    return results

# ── StockTwits ────────────────────────────────────────────────────────────────

def fetch_stocktwits(symbols: list[str] = None, max_per_symbol: int = 30) -> list[dict]:
    symbols = symbols or list(ST_SYMBOLS.keys())
    results = []
    base = "https://api.stocktwits.com/api/2/streams/symbol"

    for sym in symbols:
        st_sym = ST_SYMBOLS.get(sym.upper())
        if not st_sym:
            continue
        try:
            r = requests.get(
                f"{base}/{st_sym}.json?filter=all&limit={max_per_symbol}",
                headers=HEADERS, timeout=12
            )
            if r.status_code != 200:
                continue
            data = r.json()
            for msg in data.get("messages", []):
                try:
                    ts_str = msg.get("created_at", "")
                    ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                except Exception:
                    ts = None
                sentiment = msg.get("entities", {}).get("sentiment", {})
                sent_label = sentiment.get("basic", "") if sentiment else ""
                body = msg.get("body", "")
                username = msg.get("user", {}).get("username", "st_user")
                images = [
                    m.get("direct_url", "")
                    for m in msg.get("entities", {}).get("chart", [])
                    if m.get("direct_url")
                ]
                results.append({
                    "title":   body,
                    "summary": body,
                    "_ts":     ts,
                    "_source": "stocktwits",
                    "_author": username,
                    "_images": images,
                    "_sentiment": sent_label,
                    "_symbol": sym,
                })
        except Exception:
            pass
        time.sleep(0.5)

    return results

# ── Telegram público ──────────────────────────────────────────────────────────

def fetch_telegram_channel(slug: str, max_messages: int = 20) -> list[dict]:
    try:
        r = requests.get(f"https://t.me/s/{slug}", headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")

        # Pair each message text with its timestamp via the data-post attribute
        results = []
        msg_divs = soup.find_all("div", class_="tgme_widget_message")
        for div in msg_divs[-max_messages:]:
            text_el = div.find("div", class_="tgme_widget_message_text")
            text = text_el.get_text("\n", strip=True) if text_el else ""
            if not text or len(text) < 10:
                continue

            # Timestamp from the date link inside this message div
            time_el = div.find("time", class_="time")
            ts = None
            if time_el and time_el.get("datetime"):
                try:
                    ts = datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
                except Exception:
                    pass

            # Images
            images = []
            for img in div.find_all("img"):
                src = img.get("src", "")
                if src and "cdn" in src:
                    images.append(src)

            results.append({
                "title":   text[:400],
                "summary": text,
                "_ts":     ts,
                "_source": "telegram",
                "_author": slug,
                "_images": images,
            })
        return results
    except Exception:
        return []

def fetch_telegram(channels: dict = None, max_per_channel: int = 20) -> list[dict]:
    channels = channels or TELEGRAM_CHANNELS
    results = []
    for name, slug in channels.items():
        msgs = fetch_telegram_channel(slug, max_per_channel)
        for m in msgs:
            m["_channel_name"] = name
        results.extend(msgs)
        time.sleep(0.5)
    return results

# ── Combinar fuentes ──────────────────────────────────────────────────────────

def fetch_all_web(
    use_tv: bool = True,
    use_st: bool = True,
    use_tg: bool = True,
    max_age_days: int = 7,
) -> list[dict]:
    """Devuelve todos los mensajes de fuentes web, filtrados por fecha."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    all_msgs = []

    if use_tv:
        tv = fetch_tradingview()
        print(f"  [TV]  TradingView: {len(tv)} ideas")
        all_msgs.extend(tv)

    if use_st:
        st = fetch_stocktwits()
        print(f"  [ST]  StockTwits:  {len(st)} mensajes")
        all_msgs.extend(st)

    if use_tg:
        tg = fetch_telegram()
        print(f"  [TG]  Telegram:    {len(tg)} mensajes")
        all_msgs.extend(tg)

    # Filtrar por fecha
    filtered = [m for m in all_msgs if m.get("_ts") and m["_ts"] >= cutoff]
    return filtered

# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean_html(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html).strip()

# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--source", default="all", choices=["all","tv","st","tg"])
    args = parser.parse_args()

    print("Fetching web signals...\n")
    msgs = fetch_all_web(
        use_tv=(args.source in ("all","tv")),
        use_st=(args.source in ("all","st")),
        use_tg=(args.source in ("all","tg")),
    )
    print(f"\nTotal tras filtro de fecha: {len(msgs)}\n")

    limit = None if args.all else 5
    for src in ("tradingview", "stocktwits", "telegram"):
        src_msgs = [m for m in msgs if m["_source"] == src]
        print(f"=== {src.upper()} ({len(src_msgs)}) ===")
        for m in src_msgs[:limit]:
            ts_str = m["_ts"].strftime("%m-%d %H:%M") if m["_ts"] else "?"
            author = m.get("_author", "?")
            text = m["title"][:80].replace("\n", " ")
            print(f"  [{ts_str}] @{author}: {text}")
        print()
