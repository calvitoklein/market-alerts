"""
Twitter/X Market Alert Monitor + Signal Confluence Tracker
- Sin argumentos : un solo ciclo (GitHub Actions cada 5 min)
- --loop          : corre continuamente en tu PC (revisa cada 2 min)
"""

import os
import re
import sys
import json
import time
import feedparser
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from groq import Groq
from bot_events import log_event

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Cargar .env automáticamente si existe
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

# ── Config ────────────────────────────────────────────────────────────────────

DIR                  = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_FILE        = os.path.join(DIR, "accounts.json")
SIGNAL_ACCOUNTS_FILE = os.path.join(DIR, "signal_accounts.json")
SEEN_FILE            = os.path.join(DIR, "seen_tweets.json")
SIGNALS_BUFFER_FILE  = os.path.join(DIR, "signals_buffer.json")
CONFLUENCED_FILE     = os.path.join(DIR, "confluenced_keys.json")

CHECK_INTERVAL       = 300    # segundos entre ciclos — 5 min para listas grandes
SEEN_MAX             = 5000
GROQ_MODEL           = "llama-3.1-8b-instant"
GROQ_CALL_DELAY      = 1      # segundos entre llamadas Groq
FETCH_WORKERS        = 25     # hilos paralelos para fetch de RSS
TWEETS_PER_ACCOUNT   = 4      # tweets a revisar por cuenta
SIGNAL_WINDOW_HOURS  = 4      # ventana default (fallback)
MIN_CONFLUENCE       = 2      # mínimo de traders distintos para disparar alerta
MIN_CONFLUENCE_WEIGHT = 2.5   # suma de pesos (traders buenos valen más)

# Ventana de validez por horizonte — señal scalp de hace 2h ya no sirve
HORIZONTE_WINDOW = {
    "SCALP":    0.5,   # 30 minutos
    "DIA":      2.0,   # 2 horas
    "SWING":    6.0,   # 6 horas
    "POSICION": 12.0,  # 12 horas
}

# Heartbeat: aviso de "bot vivo" a Telegram cada N horas
HEARTBEAT_HOURS = 6

# Trader STAR: ejecuta señal individual sin esperar confluencia
STAR_WR_MIN      = 0.65   # win rate mínimo para ser STAR
STAR_SIGNALS_MIN = 5      # necesita al menos 5 señales en historial
HEARTBEAT_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "last_heartbeat.txt")

# Pre-filtro: noticias de mercado
MARKET_KEYWORDS = {
    "tariff","tariffs","trade","sanction","sanctions","rate","rates","interest",
    "inflation","gdp","recession","deficit","debt","fed","fomc","powell",
    "reserve","treasury","fiscal","monetary","tax","taxes","stimulus","bailout",
    "budget","economy","economic","market","stock","stocks","nasdaq","s&p",
    "sp500","dow","futures","equity","earnings","revenue","profit","ipo",
    "buyback","bitcoin","btc","crypto","ethereum","stablecoin","blockchain",
    "oil","gold","dollar","yuan","currency","war","iran","china","russia",
    "opec","nuclear","ban","restrict","regulation","sec","cftc","policy",
    "executive","billion","trillion","deal","merger","acquisition","layoff",
    "chip","semiconductor","nvidia","openai","tarifa","arancel","economia",
}

# Cuentas políticas/influencers: skip en el ciclo de noticias por ahora
POLITICAL_SKIP_CATEGORIES = {"politica"}

# Palabras puramente políticas: si aparecen SIN keywords financieras fuertes → skip
POLITICAL_BLOCK_KEYWORDS = {
    "trump", "white house", "executive order", "cuba", "uss lincoln",
    "iran sanctions", "russia sanctions", "ukraine", "deportation",
    "immigration", "border wall", "democrat", "republican", "senate vote",
    "congress", "house passed", "senate passed", "bill signed", "nato",
    "election", "campaña", "presidente firmó", "decreto ejecutivo",
}

FINANCIAL_STRONG_KEYWORDS = {
    "fed", "fomc", "powell", "rate", "inflation", "gdp", "recession",
    "earnings", "profit", "revenue", "ipo", "yield", "nasdaq", "s&p",
    "sp500", "bitcoin", "btc", "crypto", "oil", "gold", "dollar",
    "interest rate", "treasury", "futures", "market", "stocks",
}

# Pre-filtro: señales de trading
SIGNAL_KEYWORDS = {
    "long", "short", "buy", "sell", "entry", "entrada", "target", "tp", "sl",
    "stop", "scalp", "swing", "trade", "signal", "setup", "breakout",
    "breakdown", "support", "resistance", "bullish", "bearish", "position",
    "holding", "loaded", "calls", "puts", "alert", "watch", "level",
    "largo", "corto", "compra", "venta",
    # Índices
    "nq", "nasdaq", "es", "sp500", "s&p",
    # BTC / ETH
    "btc", "bitcoin", "eth", "ethereum",
    # Metales
    "xau", "gold", "oro", "silver", "plata", "metals", "metales", "gc", "xag",
    # L1 Crypto
    "sol", "solana", "bnb", "xrp", "ripple", "ada", "cardano", "avax",
    "avalanche", "dot", "polkadot", "atom", "ltc", "trx", "ton",
    # L2 / Infra
    "arb", "arbitrum", "op", "optimism", "sui", "sei", "inj", "injective",
    "near", "apt", "aptos", "tia", "zk", "strk",
    # DeFi
    "aave", "uni", "uniswap", "link", "chainlink", "crv", "curve", "gmx",
    "jup", "jupiter", "pendle", "dydx", "rune", "sushi", "cake", "ldo",
    # AI tokens
    "tao", "bittensor", "fet", "render", "wld", "worldcoin", "virtual",
    "grt", "io", "cgpt",
    # Memecoins
    "doge", "dogecoin", "shib", "shiba", "pepe", "wif", "bonk", "floki",
    "brett", "pnut", "moodeng", "fartcoin", "trump", "turbo",
    # Acciones perpetuas BitGet
    "nvda", "nvidia", "tsla", "tesla", "aapl", "apple", "meta", "googl",
    "msft", "microsoft", "amd", "coin", "coinbase", "mstr", "pltr",
    "amzn", "amazon", "nflx", "netflix",
}

NITTER_INSTANCES = [
    "nitter.net",
    "nitter.privacydev.net",
    "nitter.poast.org",
    "nitter.1d4.us",
    "nitter.kavin.rocks",
]

# ── Persistencia ──────────────────────────────────────────────────────────────

def load_seen() -> set:
    try:
        with open(SEEN_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen(seen: set):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(list(seen)[-SEEN_MAX:], f)

def load_signals_buffer() -> list:
    try:
        with open(SIGNALS_BUFFER_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_signals_buffer(buf: list):
    with open(SIGNALS_BUFFER_FILE, "w", encoding="utf-8") as f:
        json.dump(buf, f, ensure_ascii=False)

def load_confluenced() -> set:
    try:
        with open(CONFLUENCED_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_confluenced(keys: set):
    # solo guarda claves de las últimas 24h para no crecer indefinidamente
    with open(CONFLUENCED_FILE, "w", encoding="utf-8") as f:
        json.dump(list(keys)[-500:], f)

# ── Fetch tweets via Nitter RSS ───────────────────────────────────────────────

def fetch_tweets(handle: str) -> list:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; RSS)"}
    for instance in NITTER_INSTANCES:
        try:
            feed = feedparser.parse(f"https://{instance}/{handle}/rss",
                                    request_headers=headers)
            if feed.entries:
                return feed.entries[:TWEETS_PER_ACCOUNT]
        except Exception:
            continue
    return []

def fetch_all_parallel(accounts: list) -> dict:
    """Fetches tweets for all accounts in parallel. Returns {handle: (acc, tweets)}."""
    results = {}
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        future_map = {ex.submit(fetch_tweets, acc["handle"]): acc for acc in accounts}
        for future in as_completed(future_map):
            acc = future_map[future]
            try:
                tweets = future.result(timeout=12)
            except Exception:
                tweets = []
            results[acc["handle"]] = (acc, tweets)
    return results

# ── Análisis de noticias con Groq ─────────────────────────────────────────────

NEWS_PROMPT = """Eres un analista experto en mercados financieros (NQ, SP500, BTC).
Analiza este tweet de {name} (@{handle}) y determina su impacto en los mercados.

Tweet: "{text}"

Responde EXACTAMENTE en este formato (sin texto extra, todo en ESPANOL):
RELEVANTE: SI o NO
SEVERIDAD: BAJA o MEDIA o ALTA o EXTREMA
TEMA: [max 5 palabras en espanol]
RESUMEN: [resumen en espanol, max 20 palabras, claro y directo]
DIRECCION: ALCISTA o BAJISTA o NEUTRO
NQ_PUNTOS: [ej: +80 a +150] o 0
SP500_PUNTOS: [ej: -20 a -40] o 0
BTC_PCT: [ej: +3% a +6%] o 0%
RAZON: [max 12 palabras en espanol explicando el impacto]

Guia de severidad (SOLO marca ALTA o EXTREMA si realmente mueve mercado):
- BAJA: comentario rutinario, no mueve mercado significativamente
- MEDIA: noticia relevante pero impacto limitado, <100pts NQ
- ALTA: noticia importante que mueve mercado, 100-400pts NQ
- EXTREMA: shock de mercado, >400pts NQ (aranceles, crisis, colapsos, guerras)"""

def analyze_tweet(text: str, name: str, handle: str, client: Groq) -> dict | None:
    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content":
                NEWS_PROMPT.format(name=name, handle=handle, text=text[:700])}],
            max_tokens=160,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        result = {}
        for line in raw.split("\n"):
            if ":" in line:
                k, _, v = line.partition(":")
                result[k.strip()] = v.strip()
        return result
    except Exception as e:
        print(f"    Groq error: {e}")
        return None

# ── Extraccion de señales de trading ─────────────────────────────────────────

SIGNAL_EXTRACT_PROMPT = """Analiza este mensaje del trader {name} (@{handle}).

Mensaje: "{text}"

Responde SENAL: NO si es cualquiera de estos casos:
- Comentario general, opinion, analisis sin accion concreta
- Precio historico o prediccion a largo plazo (meses/años)
- Referencia a operacion ya cerrada ("TP hit", "closed", "resultado")
- Noticia de mercado sin señal de entrada
- No dice explicitamente long/short/buy/sell/largo/corto/compra/venta

Responde SENAL: SI si el trader da una señal ACTIVA y ACCIONABLE AHORA.

Notaciones de precio que DEBES reconocer:
- "@4727" o "@ 4727" o "at 4727" = ENTRADA 4727
- "entry: 4727" o "Entry 4727" o "E: 4727" = ENTRADA 4727
- "buy 4710-4706" o "sell 4722-4727" = ENTRADA rango
- "SL: 4739" o "sl 4739" o "Stop 4739" o "stop loss 4739" = SL
- "TP: 4720" o "tp1: 4720" o "target 4720" = TP (usa el primer objetivo)

Formato si es SI:
SENAL: SI
INSTRUMENTO: XAU o BTC o ETH o NQ o ES o SOL o BNB o XRP o GOLD o SILVER o otro
DIRECCION: LARGO o CORTO
ENTRADA: [precio numerico o rango como 4710-4706. Si genuinamente no hay precio: N/A]
TP: [precio objetivo o N/A]
SL: [stop loss o N/A]
CONFIANZA_TRADER: BAJA o MEDIA o ALTA
HORIZONTE: SCALP (minutos-horas) o DIA (hasta 24h) o SWING (dias-semanas) o POSICION (semanas-meses)
RESUMEN: [max 8 palabras en espanol]

Guia HORIZONTE: scalp/quick/immediate/now=SCALP, intraday/today/hoy=DIA, swing/weekly/dias=SWING, position/hold/months=POSICION"""

def extract_signal(text: str, name: str, handle: str, client: Groq) -> dict | None:
    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content":
                SIGNAL_EXTRACT_PROMPT.format(name=name, handle=handle, text=text[:700])}],
            max_tokens=120,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        result = {}
        for line in raw.split("\n"):
            if ":" in line:
                k, _, v = line.partition(":")
                result[k.strip()] = v.strip()
        return result
    except Exception as e:
        print(f"    Groq error (signal): {e}")
        return None

# ── Evaluacion de confluencia ─────────────────────────────────────────────────

CONFLUENCE_EVAL_PROMPT = """Varios traders publicaron señales de {instrument} {direction} en las ultimas {hours} horas:

{signals_text}

Evalua y responde EXACTAMENTE en este formato (sin texto extra):
INSTRUMENTO: {instrument}
DIRECCION: {direction}
TRADERS: {n} de {n}
CALIDAD: BAJA o MEDIA o ALTA
ENTRADA: [precio o rango consensuado entre los traders]
TP: [precio objetivo consensuado o N/A]
SL: [stop loss consensuado o N/A]
RAZON: [max 15 palabras en espanol]
RECOMENDACION: OPERAR o ESPERAR o EVITAR"""

def evaluate_confluence(signals: list, client: Groq) -> dict | None:
    instrument = signals[0].get("INSTRUMENTO", "?")
    direction  = signals[0].get("DIRECCION", "?")
    lines = []
    for s in signals:
        entrada = s.get("ENTRADA", "N/A")
        tp      = s.get("TP", "N/A")
        sl      = s.get("SL", "N/A")
        conf    = s.get("CONFIANZA_TRADER", "?")
        lines.append(
            f"- @{s['handle']} ({s['name']}): Entrada {entrada} | TP {tp} | SL {sl} | Confianza {conf}"
        )
    signals_text = "\n".join(lines)
    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content":
                CONFLUENCE_EVAL_PROMPT.format(
                    instrument=instrument,
                    direction=direction,
                    hours=SIGNAL_WINDOW_HOURS,
                    signals_text=signals_text,
                    n=len(signals),
                )}],
            max_tokens=180,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        result = {}
        for line in raw.split("\n"):
            if ":" in line:
                k, _, v = line.partition(":")
                result[k.strip().upper()] = v.strip()
        return result
    except Exception as e:
        print(f"    Groq error (confluence): {e}")
        return None

# ── Mensajes Telegram ─────────────────────────────────────────────────────────

DIR_EMOJI = {"ALCISTA": "🟢", "BAJISTA": "🔴", "NEUTRO": "🟡"}
SEV_EMOJI = {"BAJA": "🔵", "MEDIA": "🟡", "ALTA": "🟠", "EXTREMA": "🔴🔴"}
SEV_LABEL = {"BAJA": "Baja", "MEDIA": "Media", "ALTA": "ALTA", "EXTREMA": "EXTREMA"}
DIR_SIGNAL = {"LARGO": "🟢 LARGO", "CORTO": "🔴 CORTO"}
QUAL_EMOJI = {"BAJA": "🔵", "MEDIA": "🟡", "ALTA": "🟠"}
REC_EMOJI  = {"OPERAR": "✅", "ESPERAR": "⏳", "EVITAR": "❌"}

def build_news_message(name, handle, text, a, link):
    now     = datetime.now(timezone.utc).strftime("%H:%M UTC")
    de      = DIR_EMOJI.get(a.get("DIRECCION", ""), "")
    se      = SEV_EMOJI.get(a.get("SEVERIDAD", ""), "")
    sev     = SEV_LABEL.get(a.get("SEVERIDAD", ""), a.get("SEVERIDAD", ""))
    tema    = a.get("TEMA", "")
    resumen = a.get("RESUMEN", "")
    nq      = a.get("NQ_PUNTOS", "?")
    sp      = a.get("SP500_PUNTOS", "?")
    btc     = a.get("BTC_PCT", "?")
    razon   = a.get("RAZON", "")
    return (
        f"{de}{se} <b>{name}</b> · {now}\n\n"
        f"<b>{resumen}</b>\n\n"
        f"<i>Original: {text[:200]}{'...' if len(text)>200 else ''}</i>\n\n"
        f"<b>{tema}</b> — Severidad: <b>{sev}</b>\n"
        f"<pre>"
        f"NQ    {nq:>18} pts\n"
        f"SP500 {sp:>18} pts\n"
        f"BTC   {btc:>18}"
        f"</pre>\n"
        f"<i>{razon}</i>\n"
        f"<a href='{link}'>Ver tweet original</a>"
    )

HORIZONTE_EMOJI = {"SCALP": "⚡", "DIA": "📅", "SWING": "📆", "POSICION": "📌"}
HORIZONTE_LABEL = {"SCALP": "Scalp (min-h)", "DIA": "Intraday (24h)",
                   "SWING": "Swing (dias-semanas)", "POSICION": "Posicion (semanas-meses)"}

def build_star_signal_message(name: str, handle: str, sig: dict, stats: dict, link: str) -> str:
    now      = datetime.now(timezone.utc).strftime("%H:%M UTC")
    s        = stats.get(handle, {})
    wr       = s.get("win_rate", 0)
    total    = s.get("signals", 0)
    instr    = sig.get("INSTRUMENTO", "?")
    direc    = sig.get("DIRECCION", "?")
    entrada  = sig.get("ENTRADA", "N/A")
    tp       = sig.get("TP", "N/A")
    sl       = sig.get("SL", "N/A")
    horizonte = sig.get("HORIZONTE", "DIA").upper()
    hor_em   = HORIZONTE_EMOJI.get(horizonte, "")
    hor_lbl  = HORIZONTE_LABEL.get(horizonte, horizonte)
    dir_str  = DIR_SIGNAL.get(direc, direc)
    conf     = sig.get("CONFIANZA_TRADER", "?")
    return (
        f"⭐ <b>SEÑAL STAR</b> · @{handle} · {now}\n\n"
        f"<b>{instr} {dir_str}</b>\n"
        f"{hor_em} <i>{hor_lbl}</i>\n\n"
        f"<pre>"
        f"Entrada  {entrada:>20}\n"
        f"TP       {tp:>20}\n"
        f"SL       {sl:>20}"
        f"</pre>\n"
        f"Confianza: <b>{conf}</b> | WR: <b>{wr:.0%}</b> ({total} señales)\n\n"
        f"<a href='{link}'>Ver señal original</a>"
    )

def build_confluence_message(signals: list, ev: dict) -> str:
    now        = datetime.now(timezone.utc).strftime("%H:%M UTC")
    instrument = ev.get("INSTRUMENTO", signals[0].get("INSTRUMENTO", "?"))
    direction  = ev.get("DIRECCION",   signals[0].get("DIRECCION", "?"))
    acuerdo    = ev.get("TRADERS",     f"{len(signals)} de {len(signals)}")
    calidad    = ev.get("CALIDAD",     "MEDIA")
    entrada    = ev.get("ENTRADA",     "ver traders")
    tp         = ev.get("TP",          "N/A")
    sl         = ev.get("SL",          "N/A")
    razon      = ev.get("RAZON",       "")
    rec        = ev.get("RECOMENDACION","ESPERAR")

    # Horizonte mayoritario entre las señales
    horizontes  = [s.get("HORIZONTE", "DIA").upper() for s in signals]
    horizonte   = max(set(horizontes), key=horizontes.count)
    hor_em      = HORIZONTE_EMOJI.get(horizonte, "")
    hor_label   = HORIZONTE_LABEL.get(horizonte, horizonte)

    dir_str  = DIR_SIGNAL.get(direction, direction)
    qual_em  = QUAL_EMOJI.get(calidad, "")
    rec_em   = REC_EMOJI.get(rec, "")

    traders_lines = "\n".join(
        f"• @{s['handle']} [{s.get('HORIZONTE','?')}] — "
        f"Entrada {s.get('ENTRADA','?')} | TP {s.get('TP','N/A')} | SL {s.get('SL','N/A')}"
        for s in signals
    )
    signal_links = "  ".join(
        f"<a href='{s.get('link','')}'>@{s['handle']}</a>" for s in signals
    )

    return (
        f"⚡{qual_em} <b>CONFLUENCIA DE SEÑALES</b> · {now}\n\n"
        f"<b>{instrument} {dir_str}</b> — {acuerdo} traders\n"
        f"{hor_em} <i>{hor_label}</i>\n\n"
        f"<pre>"
        f"Entrada  {entrada:>20}\n"
        f"TP       {tp:>20}\n"
        f"SL       {sl:>20}"
        f"</pre>\n"
        f"Calidad: <b>{calidad}</b> | {rec_em} <b>{rec}</b>\n\n"
        f"<i>{razon}</i>\n\n"
        f"<b>Traders:</b>\n{traders_lines}\n\n"
        f"{signal_links}"
    )

# ── Heartbeat ─────────────────────────────────────────────────────────────────

def _should_heartbeat() -> bool:
    try:
        with open(HEARTBEAT_FILE) as f:
            last = datetime.fromisoformat(f.read().strip())
        return (datetime.now(timezone.utc) - last).total_seconds() > HEARTBEAT_HOURS * 3600
    except Exception:
        return True

def _save_heartbeat():
    with open(HEARTBEAT_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())

def send_heartbeat(tg_token: str, tg_chat: str, cycle: int, alerts_today: int):
    from broker import _load_open_positions
    positions = _load_open_positions()
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    if positions:
        pos_lines = "\n".join(
            f"  • {inst}: {p['direction']} @ {p.get('entry', '?')} "
            f"| TP {p.get('tp','N/A')} | SL {p.get('sl','N/A')}"
            for inst, p in positions.items()
        )
    else:
        pos_lines = "  Ninguna"
    msg = (
        f"💚 <b>Bot activo</b> · {now}\n\n"
        f"Ciclo #{cycle} | Alertas hoy: {alerts_today}\n\n"
        f"<b>Posiciones abiertas:</b>\n{pos_lines}"
    )
    send_telegram(tg_token, tg_chat, msg)
    _save_heartbeat()
    log_event("HEARTBEAT", f"Bot activo — ciclo #{cycle}, alertas hoy: {alerts_today}", {
        "cycle": cycle, "alerts_today": alerts_today,
        "open_positions": list(positions.keys()),
    })

# ── Peso y clasificación de trader ───────────────────────────────────────────

def _is_star_trader(handle: str, stats: dict) -> bool:
    s = stats.get(handle, {})
    return s.get("signals", 0) >= STAR_SIGNALS_MIN and s.get("win_rate", 0) >= STAR_WR_MIN

def _trader_weight(handle: str, stats: dict) -> float:
    s = stats.get(handle, {})
    total = s.get("signals", 0)
    if total < 5:
        return 1.0  # sin datos suficientes, peso neutro
    wr = s.get("win_rate", 0.5)
    # 50% WR → 1.0 | 70% WR → 1.4 | 30% WR → 0.6 | cap [0.3, 2.0]
    return max(0.3, min(2.0, wr / 0.5))

# ── Insiders ──────────────────────────────────────────────────────────────────

INSIDER_SEEN_FILE = os.path.join(DIR, "insider_seen.json")

def load_insider_seen() -> set:
    try:
        with open(INSIDER_SEEN_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_insider_seen(seen: set):
    with open(INSIDER_SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(list(seen)[-2000:], f)

def send_telegram(token: str, chat_id: str, text: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        return r.ok
    except Exception:
        return False

# ── Ciclo de noticias de mercado ──────────────────────────────────────────────

def run_news_cycle(accounts, client, tg_token, tg_chat, seen) -> int:
    alerts = 0
    fetched = fetch_all_parallel(accounts)
    for handle, (acc, tweets) in fetched.items():
        # Skip political/influencer accounts for now
        if acc.get("categoria") in POLITICAL_SKIP_CATEGORIES:
            continue

        name = acc["name"]
        for tw in tweets:
            tid = tw.get("id") or tw.get("link", "")
            if not tid or tid in seen:
                continue
            seen.add(tid)

            text = (tw.get("title") or tw.get("summary") or "").strip()
            link = tw.get("link", "")
            if not text or len(text) < 15:
                continue

            text_lower = text.lower()
            if not any(kw in text_lower for kw in MARKET_KEYWORDS):
                print(f"  @{handle}: sin keywords — skip")
                continue

            # Block purely political tweets without real financial impact
            if any(kw in text_lower for kw in POLITICAL_BLOCK_KEYWORDS):
                if not any(kw in text_lower for kw in FINANCIAL_STRONG_KEYWORDS):
                    print(f"  @{handle}: tweet político sin impacto financiero — skip")
                    continue

            print(f"  @{handle}: {text[:65]}...")
            time.sleep(GROQ_CALL_DELAY)
            analysis = analyze_tweet(text, name, handle, client)
            if not analysis:
                continue

            if analysis.get("RELEVANTE", "NO").upper() != "SI":
                print(f"    -> no relevante")
                continue

            sev = analysis.get("SEVERIDAD", "BAJA").upper()
            if sev not in ("ALTA", "EXTREMA"):
                print(f"    -> relevante pero severidad {sev} - ignorado")
                continue

            msg  = build_news_message(name, handle, text, analysis, link)
            sent = send_telegram(tg_token, tg_chat, msg)
            print(f"    -> {'ALERTA enviada' if sent else 'ERROR Telegram'} [{sev}]")
            if sent:
                alerts += 1
                log_event("NEWS", f"@{handle}: {analysis.get('RESUMEN', text[:60])}", {
                    "handle": handle, "severidad": sev,
                    "tema": analysis.get("TEMA", ""), "direccion": analysis.get("DIRECCION", ""),
                    "nq": analysis.get("NQ_PUNTOS", ""), "link": link,
                })

    return alerts

# ── Ciclo de señales de traders ───────────────────────────────────────────────

def extract_image_url(tw_entry) -> str | None:
    for m in tw_entry.get("media_content", []):
        if m.get("type", "").startswith("image"):
            return m.get("url")
    for e in tw_entry.get("enclosures", []):
        if e.get("type", "").startswith("image"):
            return e.get("href") or e.get("url")
    # Nitter pone imágenes como <img src="..."> en el summary HTML
    summary = tw_entry.get("summary", "")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', summary, re.IGNORECASE)
    return m.group(1) if m else None

def send_telegram_photo(token: str, chat_id: str, photo_url: str, caption: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendPhoto",
            json={"chat_id": chat_id, "photo": photo_url,
                  "caption": caption, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=15,
        )
        return r.ok
    except Exception:
        return False

def clean_buffer(buf: list) -> list:
    now = datetime.now(timezone.utc)
    def _valid(s):
        h      = s.get("HORIZONTE", "DIA").upper()
        window = HORIZONTE_WINDOW.get(h, SIGNAL_WINDOW_HOURS)
        return datetime.fromisoformat(s["ts"]) >= now - timedelta(hours=window)
    return [s for s in buf if _valid(s)]

def get_confluence_key(instrument: str, direction: str, signals: list) -> str:
    handles = "_".join(sorted(s["handle"] for s in signals))
    return f"{instrument}_{direction}_{handles}"

def _fetch_signals_all(signal_accounts: list) -> dict:
    """
    Fetches signals from: twitter_api (if cookies) or Nitter RSS, plus web sources
    (Telegram channels, TradingView, StockTwits). Returns {handle: (acc, tweets)}.
    """
    # Primary: twitter_api with browser cookies (local), or Nitter RSS (fallback)
    try:
        from twitter_api import is_configured, TwitterAPI
        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
        if is_configured():
            api = TwitterAPI.from_file()
            fetched = {}
            def _fetch_one(acc):
                try:
                    return acc, api.user_tweets(acc["handle"], limit=5)
                except Exception:
                    return acc, []
            with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
                futs = {ex.submit(_fetch_one, a): a for a in signal_accounts}
                for f in _as_completed(futs):
                    acc, tweets = f.result()
                    fetched[acc["handle"]] = (acc, tweets)
        else:
            fetched = fetch_all_parallel(signal_accounts)
    except Exception:
        fetched = fetch_all_parallel(signal_accounts)

    # Also pull Telegram + TradingView + StockTwits (works in GitHub Actions too)
    try:
        from signals_web import fetch_all_web
        web_msgs = fetch_all_web(use_tv=True, use_st=False, use_tg=True, max_age_days=1)
        for msg in web_msgs:
            author = msg.get("_author", "web")
            src    = msg.get("_source", "web")
            if author not in fetched:
                fetched[author] = ({"handle": author, "name": f"{src}:{author}"}, [])
            fetched[author][1].append(msg)
    except Exception as e:
        print(f"  [web] Error en fuentes web: {e}")

    return fetched


# Máxima desviación permitida entre entrada extraída y precio actual
STALE_THRESHOLD = {
    "SCALP":    0.010,   # 1.0%  — señal tiene minutos de vida
    "DIA":      0.030,   # 3.0%  — señal válida hasta 2h
    "SWING":    0.070,   # 7.0%  — señal válida hasta 6h
    "POSICION": 0.150,   # 15.0% — señal válida hasta 12h
}

# Instrumentos principales a cachear (público, sin auth)
_CACHE_SYMBOLS = ["XAU","XAG","BTC","ETH","SOL","BNB","XRP","ADA","AVAX",
                  "DOGE","PEPE","WIF","NVDA","TSLA","AAPL","META"]

def _build_price_cache() -> dict:
    """Obtiene precios actuales de los instrumentos más comunes. Una llamada por ciclo."""
    cache = {}
    try:
        import ccxt
        from broker import SYMBOL_MAP
        ex = ccxt.bitget({"options": {"defaultType": "swap"}})
        for inst in _CACHE_SYMBOLS:
            sym = SYMBOL_MAP.get(inst)
            if not sym:
                continue
            try:
                t = ex.fetch_ticker(sym)
                p = t.get("last") or t.get("close")
                if p:
                    cache[inst] = float(p)
            except Exception:
                pass
    except Exception:
        pass
    return cache


def _is_entry_stale(instrument: str, entrada_str: str, horizonte: str,
                    price_cache: dict) -> str | None:
    """
    Compara la entrada extraída del tweet con el precio actual.
    Si la diferencia supera el umbral del horizonte, devuelve el precio actual (string).
    Si es válida o no se puede verificar, devuelve None.
    """
    inst = instrument.upper()
    current = price_cache.get(inst)
    if not current:
        return None   # sin datos → no bloqueamos

    # Parsear entrada (puede ser rango "4700-4720" → usa el centro)
    try:
        s = entrada_str.strip().replace(",", "").replace("$", "")
        if "-" in s and s.count("-") == 1 and not s.startswith("-"):
            parts = s.split("-")
            entry_p = (float(parts[0]) + float(parts[1])) / 2
        else:
            entry_p = float(s)
    except Exception:
        return None   # no se puede parsear → no bloqueamos

    diff = abs(current - entry_p) / current
    threshold = STALE_THRESHOLD.get(horizonte.upper(), 0.05)

    if diff > threshold:
        return f"{current:.2f}"
    return None


def run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                     seen, signals_buffer, confluenced_keys) -> int:
    alerts = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    # Verificar si alguna posicion abierta ya alcanzo TP o SL
    from broker import check_position_exits
    closed = check_position_exits()
    for c in closed:
        emoji = "✅" if c["hit"] == "TP" else "🛑"
        msg = (
            f"{emoji} <b>Posicion cerrada</b> — {c['instrument']} {c['direction']}\n"
            f"Entrada: {c['entry']} → Salida: {c['exit_price']:.2f}\n"
            f"<b>{c['hit']}</b> | PnL: <b>{c['pnl_pct']:+.2f}%</b> | {c['horizonte']}"
        )
        send_telegram(tg_token, tg_chat, msg)
        print(f"  [{c['hit']}] {c['instrument']} cerrado @ {c['exit_price']:.2f} ({c['pnl_pct']:+.2f}%)")
        log_event("CLOSE", f"{c['instrument']} {c['direction']} — {c['hit']} {c['pnl_pct']:+.2f}%", {
            "instrument": c["instrument"], "direction": c["direction"],
            "entry": c["entry"], "exit": c["exit_price"],
            "hit": c["hit"], "pnl_pct": c["pnl_pct"], "horizonte": c.get("horizonte", ""),
        })

    # Cargar win rates para ponderacion
    from stats import load_stats
    trader_stats = load_stats()

    # ── Caché de precios actuales (una sola llamada al exchange por ciclo) ────
    price_cache = _build_price_cache()

    fetched = _fetch_signals_all(signal_accounts)
    for handle, (acc, tweets) in fetched.items():
        name = acc["name"]
        for tw in tweets:
            # Generar ID único: Twitter/RSS tienen 'id' o 'link'; fuentes web usan hash de contenido
            _raw_id = tw.get("id") or tw.get("link") or tw.get("_link") or ""
            if not _raw_id:
                _content = (tw.get("title") or tw.get("summary") or "")[:80]
                _raw_id = f"{handle}_{abs(hash(_content)):x}"
            tid = f"sig_{_raw_id}"
            if tid in seen:
                continue
            seen.add(tid)

            text = (tw.get("title") or tw.get("summary") or "").strip()
            link = tw.get("link") or tw.get("_link") or ""
            if not text or len(text) < 15:
                continue

            text_lower = text.lower()
            if not any(kw in text_lower for kw in SIGNAL_KEYWORDS):
                continue

            print(f"  [SIGNAL] @{handle}: {text[:60]}...")
            time.sleep(GROQ_CALL_DELAY)
            sig = extract_signal(text, name, handle, client)
            if not sig or sig.get("SENAL", "NO").upper() != "SI":
                print(f"    -> no es señal")
                continue

            entrada = sig.get("ENTRADA", "N/A").strip()
            # Descartar señales sin precio concreto (debe contener al menos un dígito)
            if not re.search(r'\d', entrada):
                print(f"    -> señal sin precio de entrada — skip")
                continue

            # Descartar instrumentos no soportados en BitGet (ej: Forex spot EUR/USD)
            _instr_up = sig.get("INSTRUMENTO", "").upper()
            try:
                from broker import SYMBOL_MAP as _SM
                _supported = _instr_up in _SM
            except Exception:
                _supported = True   # si no se puede importar, dejar pasar
            if not _supported:
                print(f"    -> {_instr_up} no está en BitGet — skip")
                continue

            # Descartar señales con precio obsoleto (el mercado se movió demasiado)
            _stale = _is_entry_stale(
                sig.get("INSTRUMENTO", ""), entrada,
                sig.get("HORIZONTE", "DIA"), price_cache,
            )
            if _stale:
                print(f"    -> entrada {entrada} obsoleta (precio actual: {_stale}) — skip")
                continue

            print(f"    -> SEÑAL {sig.get('INSTRUMENTO','?')} {sig.get('DIRECCION','?')} entrada={entrada}")
            log_event("SIGNAL", f"@{handle}: {sig.get('INSTRUMENTO','?')} {sig.get('DIRECCION','?')} entrada={entrada}", {
                "handle": handle, "name": name,
                "instrumento": sig.get("INSTRUMENTO", ""), "direccion": sig.get("DIRECCION", ""),
                "entrada": entrada, "tp": sig.get("TP", ""), "sl": sig.get("SL", ""),
                "horizonte": sig.get("HORIZONTE", ""), "confianza": sig.get("CONFIANZA_TRADER", ""),
            })

            img_url = extract_image_url(tw)
            entry = {
                "ts":       now_iso,
                "id":       tid,
                "handle":   handle,
                "name":     name,
                "link":     link,
                "img":      img_url or "",
                "text":     text[:300],
                **{k: sig.get(k, "") for k in
                   ["INSTRUMENTO","DIRECCION","ENTRADA","TP","SL",
                    "CONFIANZA_TRADER","HORIZONTE","RESUMEN"]},
            }
            signals_buffer.append(entry)

            # Registrar en tracker de aciertos
            from stats import add_pending_signal
            add_pending_signal(entry)

            # ── Ejecución directa si es trader STAR (sin esperar confluencia) ──
            if (_is_star_trader(handle, trader_stats)
                    and sig.get("CONFIANZA_TRADER", "").upper() in ("MEDIA", "ALTA")):
                wr  = trader_stats.get(handle, {}).get("win_rate", 0)
                tot = trader_stats.get(handle, {}).get("signals", 0)
                print(f"    -> STAR trader @{handle} ({wr:.0%} WR, {tot} señales) — ejecutando directamente")
                star_msg = build_star_signal_message(name, handle, sig, trader_stats, link)
                sent_star = send_telegram(tg_token, tg_chat, star_msg)
                if sent_star:
                    from broker import execute_signal as _exec_star
                    star_status = _exec_star(
                        instrument = sig.get("INSTRUMENTO", ""),
                        direction  = sig.get("DIRECCION", ""),
                        entrada    = sig.get("ENTRADA", "N/A"),
                        tp         = sig.get("TP", "N/A"),
                        sl         = sig.get("SL", "N/A"),
                        calidad    = sig.get("CONFIANZA_TRADER", "MEDIA"),
                        horizonte  = sig.get("HORIZONTE", "DIA"),
                    )
                    print(f"    -> [STAR] BROKER: {star_status}")
                    log_event("BROKER", f"Star @{handle}: {sig.get('INSTRUMENTO','?')} {sig.get('DIRECCION','?')} — {star_status[:60]}", {
                        "handle": handle, "star": True,
                        "instrument": sig.get("INSTRUMENTO", ""),
                        "direction": sig.get("DIRECCION", ""),
                        "wr": round(wr, 2), "status": star_status,
                    })
                    send_telegram(tg_token, tg_chat,
                        f"⭐ <b>Broker [STAR]:</b> <code>{star_status}</code>")
                    alerts += 1

    # Limpiar señales antiguas
    signals_buffer[:] = clean_buffer(signals_buffer)

    # ── Señales propias: XSignals Judeadas (estrategia SMC Fabio Valentini) ───
    try:
        from xsignals_strategy import generate_signals as _xsig_gen
        for _xs in _xsig_gen():
            if _xs["id"] not in seen:
                seen.add(_xs["id"])
                signals_buffer.append(_xs)
                from stats import add_pending_signal
                add_pending_signal(_xs)
                print(f"  [XSIG] {_xs['INSTRUMENTO']} {_xs['DIRECCION']} @ {_xs['ENTRADA']} | {_xs['RESUMEN']}")
                log_event("SIGNAL", f"@{_xs['handle']}: {_xs['INSTRUMENTO']} {_xs['DIRECCION']} entrada={_xs['ENTRADA']}", {
                    "handle": _xs["handle"], "name": _xs["name"],
                    "instrumento": _xs["INSTRUMENTO"], "direccion": _xs["DIRECCION"],
                    "entrada": _xs["ENTRADA"], "tp": _xs["TP"], "sl": _xs["SL"],
                    "horizonte": _xs["HORIZONTE"], "confianza": _xs["CONFIANZA_TRADER"],
                })
    except Exception as _e:
        print(f"  [XSIG] Error: {_e}")

    # Detectar confluencia por instrumento + dirección
    groups = defaultdict(list)
    for s in signals_buffer:
        instr = s.get("INSTRUMENTO", "").upper()
        direc = s.get("DIRECCION", "").upper()
        if instr and direc:
            groups[f"{instr}_{direc}"].append(s)

    for group_key, sigs in groups.items():
        # Deduplicar: un solo voto por trader (el más reciente)
        latest_per_handle: dict = {}
        for s in sorted(sigs, key=lambda x: x["ts"]):
            latest_per_handle[s["handle"]] = s
        unique_sigs = list(latest_per_handle.values())

        # Ponderacion por win rate — traders con buen historial pesan más
        weighted_score = sum(_trader_weight(s["handle"], trader_stats) for s in unique_sigs)
        for s in unique_sigs:
            w = _trader_weight(s["handle"], trader_stats)
            s["_weight"] = round(w, 2)  # guardar para mostrarlo en el mensaje

        if len(unique_sigs) < MIN_CONFLUENCE or weighted_score < MIN_CONFLUENCE_WEIGHT:
            continue

        instrument = unique_sigs[0].get("INSTRUMENTO", "?")
        direction  = unique_sigs[0].get("DIRECCION", "?")
        ckey = get_confluence_key(instrument, direction, unique_sigs)

        if ckey in confluenced_keys:
            continue  # ya alertado para esta combinacion exacta

        print(f"  >> CONFLUENCIA {instrument} {direction} — {len(unique_sigs)} traders distintos")
        time.sleep(GROQ_CALL_DELAY)
        ev = evaluate_confluence(unique_sigs, client)
        if not ev:
            continue

        msg  = build_confluence_message(unique_sigs, ev)

        # Buscar imagen de algún trader del grupo para adjuntar
        img_url = next((s["img"] for s in unique_sigs if s.get("img")), None)
        if img_url:
            sent = send_telegram_photo(tg_token, tg_chat, img_url, msg[:1024])
        else:
            sent = send_telegram(tg_token, tg_chat, msg)

        print(f"    -> {'CONFLUENCIA enviada' if sent else 'ERROR Telegram'} ({len(unique_sigs)} traders){' +img' if img_url else ''}")
        if sent:
            confluenced_keys.add(ckey)
            alerts += 1
            log_event("CONFLUENCE", f"{instrument} {direction} — {len(unique_sigs)} traders (peso={weighted_score:.1f})", {
                "instrument": instrument, "direction": direction,
                "traders": [s["handle"] for s in unique_sigs],
                "weighted_score": round(weighted_score, 2),
                "calidad": ev.get("CALIDAD", ""), "entrada": ev.get("ENTRADA", ""),
                "tp": ev.get("TP", ""), "sl": ev.get("SL", ""),
                "recomendacion": ev.get("RECOMENDACION", ""),
            })

            # Horizonte mayoritario para sizing
            horizontes = [s.get("HORIZONTE", "DIA").upper() for s in unique_sigs]
            horizonte  = max(set(horizontes), key=horizontes.count)

            # Ejecutar orden en broker
            from broker import execute_signal
            broker_status = execute_signal(
                instrument = ev.get("INSTRUMENTO", instrument),
                direction  = ev.get("DIRECCION",  direction),
                entrada    = ev.get("ENTRADA",    "N/A"),
                tp         = ev.get("TP",         "N/A"),
                sl         = ev.get("SL",         "N/A"),
                calidad    = ev.get("CALIDAD",    "MEDIA"),
                horizonte  = horizonte,
            )
            print(f"    -> BROKER: {broker_status}")
            log_event("BROKER", f"Confluencia {instrument} {direction}: {broker_status[:80]}", {
                "instrument": instrument, "direction": direction,
                "horizonte": horizonte, "status": broker_status,
            })
            send_telegram(tg_token, tg_chat,
                f"🤖 <b>Broker:</b> <code>{broker_status}</code>")

    return alerts

# ── Ciclo de insiders (congresistas + Form 4) ─────────────────────────────────

def run_insider_cycle(tg_token: str, tg_chat: str, insider_seen: set) -> int:
    """
    Revisa operaciones de congresistas (QuiverQuant) y cluster buys de insiders
    (OpenInsider). Solo alerta sobre tickers relevantes para nuestros instrumentos.
    """
    alerts = 0
    try:
        from insider_signals import (
            fetch_congress_trades, fetch_insider_cluster_buys,
            build_congress_alert, build_cluster_alert,
        )
    except Exception as e:
        print(f"  [insider] import error: {e}")
        return 0

    from broker import log_insider_trade, execute_insider_signal

    # ── Congresistas ────────────────────────────────────────────────────────
    try:
        congress = fetch_congress_trades(days=2)
        relevant = [t for t in congress if t["is_relevant"]]
        for t in relevant:
            key = f"cong_{t['representative']}_{t['ticker']}_{t['trade_date']}"
            if key in insider_seen:
                continue
            insider_seen.add(key)
            msg  = build_congress_alert(t)
            sent = send_telegram(tg_token, tg_chat, msg)
            star = "⭐ " if t.get("is_star") else ""
            print(f"  [CONGRESS] {star}{t['representative']} compró {t['ticker']} {t.get('amount_raw','?')} — {'OK' if sent else 'ERR'}")
            if sent:
                log_event("INSIDER", f"{star}{t['representative']} compró ${t['ticker']} {t.get('amount_raw','?')}", {
                    "type": "congress", "trader": t["representative"],
                    "ticker": t["ticker"], "amount": t.get("amount_raw", ""),
                    "is_star": t.get("is_star", False), "party": t.get("party", ""),
                })

            # Ejecutar en BitGet solo si es STAR trader o compra grande (>$50k)
            if t.get("is_star") or t.get("amount", 0) >= 50000:
                broker_status = execute_insider_signal(
                    ticker     = t["ticker"],
                    trader     = t["representative"],
                    amount     = t.get("amount_raw", "?"),
                    trade_date = t.get("trade_date", "?"),
                    notes      = f"CONGRESISTA STAR | partido={t.get('party','?')}",
                )
                print(f"    -> BROKER: {broker_status}")
                log_event("BROKER", f"Insider {t['ticker']} ({t['representative']}): {broker_status[:80]}", {
                    "instrument": t["ticker"], "trader": t["representative"], "status": broker_status,
                })
                send_telegram(tg_token, tg_chat,
                    f"🤖 <b>Broker insider:</b> <code>{broker_status}</code>")
            else:
                log_insider_trade("congresista", t["ticker"], t["representative"],
                                  t.get("amount_raw","?"), t.get("trade_date","?"),
                                  f"partido={t.get('party','?')} | no ejecutado (no star)")
            if sent:
                alerts += 1
    except Exception as e:
        print(f"  [insider] congress error: {e}")

    # ── Cluster buys ────────────────────────────────────────────────────────
    try:
        clusters = fetch_insider_cluster_buys(days=2, min_insiders=3)
        relevant_clusters = [c for c in clusters if c["is_relevant"] or c["n_insiders"] >= 5]
        for c in relevant_clusters:
            key = f"cluster_{c['ticker']}_{c['trade_date']}_{c['n_insiders']}"
            if key in insider_seen:
                continue
            insider_seen.add(key)
            msg  = build_cluster_alert(c)
            sent = send_telegram(tg_token, tg_chat, msg)
            val  = f"${c['value']:,.0f}" if c.get("value") else "?"
            print(f"  [CLUSTER] {c['n_insiders']} insiders {c['ticker']} {val} — {'OK' if sent else 'ERR'}")
            if sent:
                log_event("INSIDER", f"Cluster {c['n_insiders']} insiders ${c['ticker']} {val}", {
                    "type": "cluster", "ticker": c["ticker"],
                    "n_insiders": c["n_insiders"], "value": val,
                    "company": c.get("company", ""), "industry": c.get("industry", ""),
                })

            # Cluster buys siempre se intentan ejecutar (señal más fuerte)
            broker_status = execute_insider_signal(
                ticker     = c["ticker"],
                trader     = f"{c['n_insiders']} ejecutivos de {c.get('company','?')}",
                amount     = val,
                trade_date = c.get("trade_date", "?"),
                notes      = f"CLUSTER {c['n_insiders']} insiders | sector={c.get('industry','?')}",
            )
            print(f"    -> BROKER: {broker_status}")
            log_event("BROKER", f"Cluster {c['ticker']} ({c['n_insiders']} insiders): {broker_status[:80]}", {
                "instrument": c["ticker"], "n_insiders": c["n_insiders"], "status": broker_status,
            })
            send_telegram(tg_token, tg_chat,
                f"🤖 <b>Broker cluster:</b> <code>{broker_status}</code>")
            if sent:
                alerts += 1
    except Exception as e:
        print(f"  [insider] cluster error: {e}")

    return alerts


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    loop_mode = "--loop" in sys.argv

    groq_key   = os.environ.get("GROQ_API_KEY")
    tg_token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat    = os.environ.get("TELEGRAM_CHAT_ID")

    if not all([groq_key, tg_token, tg_chat]):
        print("ERROR: faltan GROQ_API_KEY, TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")
        sys.exit(1)

    try:
        with open(ACCOUNTS_FILE) as f:
            accounts = json.load(f)
    except Exception:
        accounts = []

    signal_accounts = []
    if os.path.exists(SIGNAL_ACCOUNTS_FILE):
        try:
            with open(SIGNAL_ACCOUNTS_FILE) as f:
                signal_accounts = json.load(f)
        except Exception:
            signal_accounts = []

    client          = Groq(api_key=groq_key)
    seen            = load_seen()
    signals_buffer  = load_signals_buffer()
    confluenced_keys = load_confluenced()
    insider_seen    = load_insider_seen()

    if loop_mode:
        send_telegram(tg_token, tg_chat,
            f"🚀 Monitor arrancado — {len(accounts)} noticias + {len(signal_accounts)} traders\n"
            f"Ciclo cada {CHECK_INTERVAL//60} min | {FETCH_WORKERS} hilos paralelos\n"
            f"Mejoras activas: ventana dinámica · ponderación WR · tracking posiciones · heartbeat {HEARTBEAT_HOURS}h\n"
            f"⭐ STAR traders: WR>={STAR_WR_MIN:.0%} ejecutan señal individual sin confluencia")
        log_event("INFO", f"Bot arrancado — {len(accounts)} noticias + {len(signal_accounts)} traders, ciclo {CHECK_INTERVAL}s", {
            "accounts": len(accounts), "signal_accounts": len(signal_accounts),
            "interval": CHECK_INTERVAL, "workers": FETCH_WORKERS,
        })
        print(f"=== Modo continuo | {len(accounts)} noticias + {len(signal_accounts)} señales | cada {CHECK_INTERVAL}s | {FETCH_WORKERS} workers ===")
        cycle        = 0
        alerts_today = 0
        while True:
            cycle += 1
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"\n[{ts}] Ciclo #{cycle}")
            try:
                n_news  = run_news_cycle(accounts, client, tg_token, tg_chat, seen)
                n_sigs  = run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                                           seen, signals_buffer, confluenced_keys)
                n_ins   = run_insider_cycle(tg_token, tg_chat, insider_seen)
                from stats import run_stats_cycle
                n_eval  = run_stats_cycle(tg_token, tg_chat, send_telegram)
                alerts_today += n_news + n_sigs + n_ins

                # Heartbeat cada HEARTBEAT_HOURS horas
                if _should_heartbeat():
                    send_heartbeat(tg_token, tg_chat, cycle, alerts_today)

                save_seen(seen)
                save_signals_buffer(signals_buffer)
                save_confluenced(confluenced_keys)
                save_insider_seen(insider_seen)
                if n_news or n_sigs or n_eval or n_ins:
                    print(f"  {n_news} noticias | {n_sigs} confluencias | {n_ins} insiders | {n_eval} señales evaluadas")
            except Exception as e:
                print(f"  Error: {e}")
                log_event("ERROR", f"Error en ciclo #{cycle}: {e}", {"cycle": cycle})
            time.sleep(CHECK_INTERVAL)
    else:
        print(f"=== Ciclo unico | {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")
        n_news = run_news_cycle(accounts, client, tg_token, tg_chat, seen)
        n_sigs = run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                                  seen, signals_buffer, confluenced_keys)
        n_ins  = run_insider_cycle(tg_token, tg_chat, insider_seen)
        save_seen(seen)
        save_signals_buffer(signals_buffer)
        save_confluenced(confluenced_keys)
        save_insider_seen(insider_seen)
        print(f"=== Fin: {n_news} noticias + {n_sigs} confluencias + {n_ins} insiders ===")

if __name__ == "__main__":
    main()
