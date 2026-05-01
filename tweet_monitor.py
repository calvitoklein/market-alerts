"""
Twitter/X Market Alert Monitor
- Sin argumentos : un solo ciclo (GitHub Actions cada 5 min)
- --loop          : corre continuamente en tu PC (revisa cada 2 min)
"""

import os
import sys
import json
import time
import feedparser
import requests
from datetime import datetime, timezone
from groq import Groq

# ── Config ────────────────────────────────────────────────────────────────────

DIR            = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_FILE  = os.path.join(DIR, "accounts.json")
SEEN_FILE      = os.path.join(DIR, "seen_tweets.json")
CHECK_INTERVAL = 120   # segundos entre ciclos (modo --loop)
SEEN_MAX        = 3000  # max IDs guardados
GROQ_MODEL      = "llama-3.1-8b-instant"
GROQ_CALL_DELAY = 2     # segundos entre llamadas Groq (evita rate limit)

# Pre-filtro: solo manda a Groq tweets con alguna de estas palabras
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

NITTER_INSTANCES = [
    "nitter.net",
    "nitter.privacydev.net",
    "nitter.poast.org",
    "nitter.1d4.us",
    "nitter.kavin.rocks",
]

# ── Seen tweets (evita duplicados) ───────────────────────────────────────────

def load_seen() -> set:
    try:
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen(seen: set):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen)[-SEEN_MAX:], f)

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

# ── Análisis con Groq Llama ───────────────────────────────────────────────────

PROMPT_TEMPLATE = """Eres un analista experto en mercados financieros (NQ, SP500, BTC).
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
                PROMPT_TEMPLATE.format(name=name, handle=handle, text=text[:700])}],
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

# ── Mensaje Telegram ──────────────────────────────────────────────────────────

DIR_EMOJI = {"ALCISTA": "🟢", "BAJISTA": "🔴", "NEUTRO": "🟡"}
SEV_EMOJI = {"BAJA": "🔵", "MEDIA": "🟡", "ALTA": "🟠", "EXTREMA": "🔴🔴"}
SEV_LABEL = {"BAJA": "Baja", "MEDIA": "Media", "ALTA": "ALTA", "EXTREMA": "EXTREMA"}

def build_message(name, handle, text, a, link):
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

# ── Ciclo de chequeo ─────────────────────────────────────────────────────────

def run_cycle(accounts, client, tg_token, tg_chat, seen) -> int:
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

            # Pre-filtro keywords — evita llamadas Groq innecesarias
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

            msg  = build_message(name, handle, text, analysis, link)
            sent = send_telegram(tg_token, tg_chat, msg)
            print(f"    -> {'ALERTA enviada' if sent else 'ERROR Telegram'} [{sev}]")
            if sent:
                alerts += 1

    return alerts

# ── Main ─────────────────────────────────────────────────────────────────────

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

    client = Groq(api_key=groq_key)
    seen   = load_seen()

    if loop_mode:
        send_telegram(tg_token, tg_chat,
            f"Monitor arrancado — {len(accounts)} cuentas, revisando cada {CHECK_INTERVAL//60} min")
        print(f"=== Modo continuo | {len(accounts)} cuentas | cada {CHECK_INTERVAL}s ===")
        cycle = 0
        while True:
            cycle += 1
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"\n[{ts}] Ciclo #{cycle}")
            try:
                n = run_cycle(accounts, client, tg_token, tg_chat, seen)
                save_seen(seen)
                if n:
                    print(f"  {n} alerta(s)")
            except Exception as e:
                print(f"  Error: {e}")
            time.sleep(CHECK_INTERVAL)
    else:
        # Single run (GitHub Actions)
        print(f"=== Ciclo unico | {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")
        n = run_cycle(accounts, client, tg_token, tg_chat, seen)
        save_seen(seen)
        print(f"=== Fin: {n} alerta(s) ===")

if __name__ == "__main__":
    main()
