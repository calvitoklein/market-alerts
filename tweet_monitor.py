"""
Twitter/X Market Alert Monitor
Corre cada 15 min via GitHub Actions.
Solo avisa si el tweet es relevante para mercados financieros (Groq Llama, gratis).
Alertas por Telegram.
"""

import os
import json
import feedparser
import requests
from datetime import datetime, timezone, timedelta
from groq import Groq

# ── Config ────────────────────────────────────────────────────────────────────

ACCOUNTS_FILE  = os.path.join(os.path.dirname(__file__), "accounts.json")
MAX_AGE_MIN    = 20   # solo tweets de los últimos 20 min (cron cada 15 min)
GROQ_MODEL     = "llama-3.1-8b-instant"

NITTER_INSTANCES = [
    "nitter.privacydev.net",
    "nitter.poast.org",
    "nitter.net",
    "nitter.1d4.us",
    "nitter.kavin.rocks",
]

# ── Fetch tweets via Nitter RSS ───────────────────────────────────────────────

def fetch_tweets(handle: str) -> list:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; RSS reader)"}
    for instance in NITTER_INSTANCES:
        try:
            url  = f"https://{instance}/{handle}/rss"
            feed = feedparser.parse(url, request_headers=headers)
            if feed.entries:
                print(f"    [{instance}] OK — {len(feed.entries)} tweets")
                return feed.entries[:10]
        except Exception as e:
            print(f"    [{instance}] fallo: {e}")
            continue
    return []

def is_recent(entry) -> bool:
    if not getattr(entry, "published_parsed", None):
        return True
    pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - pub < timedelta(minutes=MAX_AGE_MIN)

# ── Relevancia con Groq Llama (gratis) ───────────────────────────────────────

def check_relevance(text: str, client: Groq) -> tuple[bool, str]:
    prompt = (
        "Analyze this tweet. Is it relevant to financial markets? "
        "Consider: stocks, crypto, economy, trade policy, tariffs, interest rates, "
        "inflation, geopolitics affecting markets, company earnings, sanctions, debt.\n\n"
        f"Tweet: {text[:600]}\n\n"
        "Reply ONLY with:\n"
        "YES: [reason in max 12 words]\n"
        "or\n"
        "NO: [reason in max 12 words]"
    )
    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60,
            temperature=0.1,
        )
        result = resp.choices[0].message.content.strip()
        relevant = result.upper().startswith("YES")
        reason   = result[4:].strip() if len(result) > 3 else ""
        return relevant, reason
    except Exception as e:
        print(f"    Groq error: {e}")
        return False, ""

# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram(token: str, chat_id: str, text: str) -> bool:
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = requests.post(url, json=data, timeout=10)
        return r.ok
    except Exception as e:
        print(f"    Telegram error: {e}")
        return False

def build_message(name: str, handle: str, tweet_text: str, reason: str, link: str) -> str:
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    return (
        f"🚨 <b>{name}</b> <code>@{handle}</code> · {now}\n\n"
        f"{tweet_text}\n\n"
        f"📊 <i>{reason}</i>\n"
        f"🔗 {link}"
    )

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    groq_key   = os.environ.get("GROQ_API_KEY")
    tg_token   = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not all([groq_key, tg_token, tg_chat_id]):
        print("ERROR: Faltan variables de entorno. Necesitas:")
        print("  GROQ_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID")
        return

    with open(ACCOUNTS_FILE) as f:
        accounts = json.load(f)

    groq_client = Groq(api_key=groq_key)
    alerts_sent = 0

    print(f"=== Monitor iniciado — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} ===")
    print(f"Monitoreando {len(accounts)} cuentas | Ventana: últimos {MAX_AGE_MIN} min\n")

    for account in accounts:
        handle = account["handle"]
        name   = account["name"]
        print(f">> @{handle} ({name})")

        tweets = fetch_tweets(handle)
        if not tweets:
            print(f"    Sin tweets disponibles\n")
            continue

        new_tweets = [t for t in tweets if is_recent(t)]
        print(f"    {len(new_tweets)} tweets recientes (de {len(tweets)} totales)")

        for tweet in new_tweets:
            text = (tweet.get("title") or tweet.get("summary") or "").strip()
            link = tweet.get("link", "")

            if not text or len(text) < 10:
                continue

            print(f"    Analizando: {text[:70]}...")
            relevant, reason = check_relevance(text, groq_client)

            if relevant:
                msg  = build_message(name, handle, text, reason, link)
                sent = send_telegram(tg_token, tg_chat_id, msg)
                status = "✅ ALERTA enviada" if sent else "❌ Error Telegram"
                print(f"    {status}")
                if sent:
                    alerts_sent += 1
            else:
                print(f"    ⏭  No relevante: {reason}")

        print()

    print(f"=== Fin: {alerts_sent} alerta(s) enviada(s) ===")

if __name__ == "__main__":
    main()
