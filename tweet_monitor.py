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
SIGNAL_WINDOW_HOURS  = 4      # ventana de tiempo para confluencia
MIN_CONFLUENCE       = 3      # mínimo de traders DISTINTOS para disparar alerta

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
    "nq", "nasdaq", "es", "sp500", "s&p", "btc", "bitcoin", "eth",
    "largo", "corto", "compra", "venta",
    "xau", "gold", "oro", "silver", "plata", "metals", "metales", "gc",
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
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen(seen: set):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen)[-SEEN_MAX:], f)

def load_signals_buffer() -> list:
    try:
        with open(SIGNALS_BUFFER_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def save_signals_buffer(buf: list):
    with open(SIGNALS_BUFFER_FILE, "w") as f:
        json.dump(buf, f, ensure_ascii=False)

def load_confluenced() -> set:
    try:
        with open(CONFLUENCED_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_confluenced(keys: set):
    # solo guarda claves de las últimas 24h para no crecer indefinidamente
    with open(CONFLUENCED_FILE, "w") as f:
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

SIGNAL_EXTRACT_PROMPT = """Analiza este tweet del trader {name} (@{handle}).

Tweet: "{text}"

Responde SENAL: NO si el tweet es cualquiera de estos casos:
- Comentario general, opinion, analisis sin accion concreta
- Precio historico o prediccion a largo plazo (meses/años)
- Referencia a operacion ya cerrada
- Noticia de mercado sin señal de entrada
- No dice explicitamente long/short/buy/sell/largo/corto/compra/venta

Responde SENAL: SI SOLO si el trader esta dando una señal ACTIVA Y ACCIONABLE AHORA con direccion clara.

Formato si es SI:
SENAL: SI
INSTRUMENTO: NQ o ES o BTC o ETH o XAU o GC o CL o DXY o otro
DIRECCION: LARGO o CORTO
ENTRADA: [precio numerico o rango, ej: 19500 o 19500-19550. Si no hay precio: N/A]
TP: [precio numerico objetivo, o N/A]
SL: [precio numerico stop loss, o N/A]
CONFIANZA_TRADER: BAJA o MEDIA o ALTA
HORIZONTE: SCALP (minutos-horas) o DIA (hasta 24h) o SWING (dias-semanas) o POSICION (semanas-meses)
RESUMEN: [max 8 palabras en espanol]

Guia HORIZONTE: scalp/quick/immediate=SCALP, intraday/today/hoy=DIA, swing/weekly/dias=SWING, position/hold/months=POSICION
Si no hay precio de entrada especifico, es muy probable que NO sea una señal valida."""

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
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SIGNAL_WINDOW_HOURS)
    return [s for s in buf if datetime.fromisoformat(s["ts"]) >= cutoff]

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


def run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                     seen, signals_buffer, confluenced_keys) -> int:
    alerts = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    fetched = _fetch_signals_all(signal_accounts)
    for handle, (acc, tweets) in fetched.items():
        name = acc["name"]
        for tw in tweets:
            tid = f"sig_{tw.get('id') or tw.get('link', '')}"
            if not tid or tid in seen:
                continue
            seen.add(tid)

            text = (tw.get("title") or tw.get("summary") or "").strip()
            link = tw.get("link", "")
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
            # Descartar señales sin precio concreto
            if entrada.upper() in ("N/A", "", "?", "NONE", "NO"):
                print(f"    -> señal sin precio de entrada — skip")
                continue

            print(f"    -> SEÑAL {sig.get('INSTRUMENTO','?')} {sig.get('DIRECCION','?')} entrada={entrada}")

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

    # Limpiar señales antiguas
    signals_buffer[:] = clean_buffer(signals_buffer)

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

        if len(unique_sigs) < MIN_CONFLUENCE:
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
            send_telegram(tg_token, tg_chat,
                f"🤖 <b>Broker:</b> <code>{broker_status}</code>")

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

    if loop_mode:
        send_telegram(tg_token, tg_chat,
            f"Monitor arrancado — {len(accounts)} noticias + {len(signal_accounts)} traders | cada {CHECK_INTERVAL//60} min | {FETCH_WORKERS} hilos paralelos")
        print(f"=== Modo continuo | {len(accounts)} noticias + {len(signal_accounts)} señales | cada {CHECK_INTERVAL}s | {FETCH_WORKERS} workers ===")
        cycle = 0
        while True:
            cycle += 1
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"\n[{ts}] Ciclo #{cycle}")
            try:
                n_news  = run_news_cycle(accounts, client, tg_token, tg_chat, seen)
                n_sigs  = run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                                           seen, signals_buffer, confluenced_keys)
                from stats import run_stats_cycle
                n_eval  = run_stats_cycle(tg_token, tg_chat, send_telegram)
                save_seen(seen)
                save_signals_buffer(signals_buffer)
                save_confluenced(confluenced_keys)
                if n_news or n_sigs or n_eval:
                    print(f"  {n_news} noticias | {n_sigs} confluencias | {n_eval} señales evaluadas")
            except Exception as e:
                print(f"  Error: {e}")
            time.sleep(CHECK_INTERVAL)
    else:
        print(f"=== Ciclo unico | {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")
        n_news = run_news_cycle(accounts, client, tg_token, tg_chat, seen)
        n_sigs = run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                                  seen, signals_buffer, confluenced_keys)
        save_seen(seen)
        save_signals_buffer(signals_buffer)
        save_confluenced(confluenced_keys)
        print(f"=== Fin: {n_news} noticias + {n_sigs} confluencias ===")

if __name__ == "__main__":
    main()
