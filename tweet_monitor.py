"""
Twitter/X Market Alert Monitor + Signal Confluence Tracker
- Sin argumentos : un solo ciclo (GitHub Actions cada 5 min)
- --loop          : corre continuamente en tu PC (revisa cada 2 min)
"""

import os
import sys
import json
import time
import feedparser
import requests
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

CHECK_INTERVAL       = 120    # segundos entre ciclos (modo --loop)
SEEN_MAX             = 3000
GROQ_MODEL           = "llama-3.1-8b-instant"
GROQ_CALL_DELAY      = 2      # segundos entre llamadas Groq
SIGNAL_WINDOW_HOURS  = 3      # ventana de tiempo para confluencia
MIN_CONFLUENCE       = 2      # mínimo de traders para disparar alerta

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

# Pre-filtro: señales de trading
SIGNAL_KEYWORDS = {
    "long", "short", "buy", "sell", "entry", "entrada", "target", "tp", "sl",
    "stop", "scalp", "swing", "trade", "signal", "setup", "breakout",
    "breakdown", "support", "resistance", "bullish", "bearish", "position",
    "holding", "loaded", "calls", "puts", "alert", "watch", "level",
    "nq", "nasdaq", "es", "sp500", "s&p", "btc", "bitcoin", "eth",
    "largo", "corto", "compra", "venta",
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
                return feed.entries[:8]
        except Exception:
            continue
    return []

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

SIGNAL_EXTRACT_PROMPT = """Analiza este tweet del trader {name} (@{handle}) y extrae la señal de trading si la contiene.

Tweet: "{text}"

Si NO es una señal de trading con direccion concreta (largo/corto/compra/venta), responde solo:
SENAL: NO

Si ES una señal de trading, responde EXACTAMENTE en este formato:
SENAL: SI
INSTRUMENTO: [NQ / ES / BTC / ETH / GC / CL / DXY / otro]
DIRECCION: LARGO o CORTO
ENTRADA: [precio o rango, ej: 19500 o 19500-19550, o MERCADO]
TP: [precio objetivo o N/A]
SL: [stop loss o N/A]
CONFIANZA_TRADER: BAJA o MEDIA o ALTA
RESUMEN: [max 10 palabras en espanol describiendo la señal]"""

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

Evalua la confluencia y calidad de esta operacion potencial:
INSTRUMENTO: [instrumento]
DIRECCION: LARGO o CORTO
TRADERS_ACUERDO: [X de Y]
CALIDAD: BAJA o MEDIA o ALTA
ENTRADA_CONSENSO: [precio o rango consensuado]
TP_CONSENSO: [precio objetivo consensuado o N/A]
SL_CONSENSO: [stop consensuado o N/A]
RAZON: [max 15 palabras en espanol explicando la confluencia]
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
                )}],
            max_tokens=180,
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

def build_confluence_message(signals: list, ev: dict) -> str:
    now        = datetime.now(timezone.utc).strftime("%H:%M UTC")
    instrument = ev.get("INSTRUMENTO", signals[0].get("INSTRUMENTO", "?"))
    direction  = ev.get("DIRECCION", signals[0].get("DIRECCION", "?"))
    acuerdo    = ev.get("TRADERS_ACUERDO", f"{len(signals)}/{len(signals)}")
    calidad    = ev.get("CALIDAD", "?")
    entrada    = ev.get("ENTRADA_CONSENSO", "?")
    tp         = ev.get("TP_CONSENSO", "N/A")
    sl         = ev.get("SL_CONSENSO", "N/A")
    razon      = ev.get("RAZON", "")
    rec        = ev.get("RECOMENDACION", "ESPERAR")

    dir_str  = DIR_SIGNAL.get(direction, direction)
    qual_em  = QUAL_EMOJI.get(calidad, "")
    rec_em   = REC_EMOJI.get(rec, "")

    traders_lines = "\n".join(
        f"• @{s['handle']} — Entrada {s.get('ENTRADA','?')} | TP {s.get('TP','N/A')} | SL {s.get('SL','N/A')}"
        for s in signals
    )
    signal_links = "  ".join(
        f"<a href='{s.get('link','')}'>@{s['handle']}</a>" for s in signals
    )

    return (
        f"⚡{qual_em} <b>CONFLUENCIA DE SEÑALES</b> · {now}\n\n"
        f"<b>{instrument} {dir_str}</b> — {acuerdo} traders de acuerdo\n\n"
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
    for acc in accounts:
        handle = acc["handle"]
        name   = acc["name"]
        tweets = fetch_tweets(handle)

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

def clean_buffer(buf: list) -> list:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SIGNAL_WINDOW_HOURS)
    return [s for s in buf if datetime.fromisoformat(s["ts"]) >= cutoff]

def get_confluence_key(instrument: str, direction: str, signals: list) -> str:
    handles = "_".join(sorted(s["handle"] for s in signals))
    return f"{instrument}_{direction}_{handles}"

def run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                     seen, signals_buffer, confluenced_keys) -> int:
    alerts = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for acc in signal_accounts:
        handle = acc["handle"]
        name   = acc["name"]
        tweets = fetch_tweets(handle)

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

            print(f"    -> SEÑAL {sig.get('INSTRUMENTO','?')} {sig.get('DIRECCION','?')} entrada={sig.get('ENTRADA','?')}")

            entry = {
                "ts":       now_iso,
                "id":       tid,
                "handle":   handle,
                "name":     name,
                "link":     link,
                "text":     text[:300],
                **{k: sig.get(k, "") for k in
                   ["INSTRUMENTO","DIRECCION","ENTRADA","TP","SL","CONFIANZA_TRADER","RESUMEN"]},
            }
            signals_buffer.append(entry)

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
        if len(sigs) < MIN_CONFLUENCE:
            continue

        instrument = sigs[0].get("INSTRUMENTO", "?")
        direction  = sigs[0].get("DIRECCION", "?")
        ckey = get_confluence_key(instrument, direction, sigs)

        if ckey in confluenced_keys:
            continue  # ya alertado para esta combinacion exacta

        print(f"  >> CONFLUENCIA {instrument} {direction} — {len(sigs)} traders")
        time.sleep(GROQ_CALL_DELAY)
        ev = evaluate_confluence(sigs, client)
        if not ev:
            continue

        msg  = build_confluence_message(sigs, ev)
        sent = send_telegram(tg_token, tg_chat, msg)
        print(f"    -> {'CONFLUENCIA enviada' if sent else 'ERROR Telegram'} ({len(sigs)} traders)")
        if sent:
            confluenced_keys.add(ckey)
            alerts += 1

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

    with open(ACCOUNTS_FILE) as f:
        accounts = json.load(f)

    signal_accounts = []
    if os.path.exists(SIGNAL_ACCOUNTS_FILE):
        with open(SIGNAL_ACCOUNTS_FILE) as f:
            signal_accounts = json.load(f)

    client          = Groq(api_key=groq_key)
    seen            = load_seen()
    signals_buffer  = load_signals_buffer()
    confluenced_keys = load_confluenced()

    if loop_mode:
        send_telegram(tg_token, tg_chat,
            f"Monitor arrancado — {len(accounts)} cuentas noticias + {len(signal_accounts)} traders de señales | cada {CHECK_INTERVAL//60} min")
        print(f"=== Modo continuo | {len(accounts)} noticias + {len(signal_accounts)} señales | cada {CHECK_INTERVAL}s ===")
        cycle = 0
        while True:
            cycle += 1
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"\n[{ts}] Ciclo #{cycle}")
            try:
                n_news = run_news_cycle(accounts, client, tg_token, tg_chat, seen)
                n_sigs = run_signal_cycle(signal_accounts, client, tg_token, tg_chat,
                                          seen, signals_buffer, confluenced_keys)
                save_seen(seen)
                save_signals_buffer(signals_buffer)
                save_confluenced(confluenced_keys)
                total = n_news + n_sigs
                if total:
                    print(f"  {n_news} noticias + {n_sigs} confluencias")
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
