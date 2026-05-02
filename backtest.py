"""
Backtesting de señales recientes (ultimas 1-2 semanas).
Fuentes de datos (en orden de preferencia):
  1. twscrape  — scraping real de Twitter/X via cookies del navegador
                 Configurar primero con: python setup_cookies.py
  2. Nitter RSS — fallback si twscrape no esta disponible

- Obtiene hasta 20 tweets por trader
- Extrae señales con Groq (mismo prompt que el monitor live)
- Evalua WIN/LOSS con precios historicos (yfinance/ccxt)
- Acumula resultados en trader_stats.json

Uso:
    python backtest.py                  # todos los traders
    python backtest.py --limit 20       # solo primeros 20 traders
    python backtest.py --delay 1.5      # delay entre llamadas Groq
    python backtest.py --source rss     # forzar RSS aunque twscrape este disponible
    python backtest.py --source twscrape
"""

import os, sys, json, time, argparse
import feedparser
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from groq import Groq

DIR = os.path.dirname(os.path.abspath(__file__))

# ── Cargar .env ───────────────────────────────────────────────────────────────
def load_env(path=os.path.join(DIR, ".env")):
    env = {}
    try:
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
                os.environ.setdefault(k.strip(), v.strip())
    except Exception:
        pass
    return env

load_env()

GROQ_MODEL  = "llama-3.1-8b-instant"
MIN_AGE_H   = 6     # señal debe tener >6h para evaluar resultado
MAX_AGE_D   = 14    # ignorar tweets de hace >14 días
FETCH_W     = 20    # hilos paralelos para RSS

DB_PATH = os.path.join(DIR, "twscrape_accounts.db")

NITTER_INSTANCES = [
    "nitter.net", "nitter.privacydev.net", "nitter.poast.org",
    "nitter.1d4.us", "nitter.kavin.rocks",
]

SIGNAL_KEYWORDS = {
    "long","short","buy","sell","entry","entrada","target","tp","sl","stop",
    "scalp","swing","trade","signal","setup","breakout","breakdown","support",
    "resistance","bullish","bearish","position","holding","loaded","alert",
    "nq","nasdaq","es","sp500","s&p","btc","bitcoin","eth","xau","gold",
    "largo","corto","compra","venta","gc","metals",
}

SIGNAL_EXTRACT_PROMPT = """Analiza este tweet del trader {name} (@{handle}).

Tweet: "{text}"

Responde SENAL: NO si el tweet es cualquiera de estos casos:
- Comentario general, opinion, analisis sin accion concreta
- Precio historico o prediccion a largo plazo (meses/anios)
- Referencia a operacion ya cerrada
- No dice explicitamente long/short/buy/sell/largo/corto/compra/venta

Responde SENAL: SI SOLO si el trader esta dando una senal ACTIVA Y ACCIONABLE con direccion clara.

Formato si es SI:
SENAL: SI
INSTRUMENTO: NQ o ES o BTC o ETH o XAU o GC o CL o DXY o otro
DIRECCION: LARGO o CORTO
ENTRADA: [precio numerico o rango. Si no hay precio concreto: N/A]
TP: [precio numerico o N/A]
SL: [precio numerico o N/A]
CONFIANZA_TRADER: BAJA o MEDIA o ALTA
HORIZONTE: SCALP o DIA o SWING o POSICION
RESUMEN: [max 8 palabras en espanol]"""

# ── Fetch via Nitter RSS (fallback) ──────────────────────────────────────────

def fetch_tweets_rss(handle: str) -> list:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; RSS)"}
    for inst in NITTER_INSTANCES:
        try:
            feed = feedparser.parse(f"https://{inst}/{handle}/rss",
                                    request_headers=headers)
            if feed.entries:
                return feed.entries[:20]
        except Exception:
            continue
    return []

def fetch_all_rss(accounts: list) -> dict:
    results = {}
    with ThreadPoolExecutor(max_workers=FETCH_W) as ex:
        fut = {ex.submit(fetch_tweets_rss, a["handle"]): a for a in accounts}
        for f in as_completed(fut):
            acc = fut[f]
            try:
                results[acc["handle"]] = (acc, f.result(timeout=12))
            except Exception:
                results[acc["handle"]] = (acc, [])
    return results

# ── Fetch via twitter_api (primary) ──────────────────────────────────────────

def _twitter_api_available() -> bool:
    from twitter_api import is_configured
    return is_configured()

def _fetch_one_twitter_api(handle: str, api) -> list:
    try:
        return api.user_tweets(handle, limit=20)
    except Exception:
        return []

def fetch_all_twitter_api(accounts: list) -> dict:
    from twitter_api import TwitterAPI
    api = TwitterAPI.from_file()
    results = {}
    with ThreadPoolExecutor(max_workers=FETCH_W) as ex:
        fut = {ex.submit(_fetch_one_twitter_api, a["handle"], api): a for a in accounts}
        for f in as_completed(fut):
            acc = fut[f]
            try:
                results[acc["handle"]] = (acc, f.result(timeout=20))
            except Exception:
                results[acc["handle"]] = (acc, [])
    return results

def fetch_all(accounts: list, source: str = "auto") -> dict:
    if source == "rss":
        print("  [src] Nitter RSS")
        return fetch_all_rss(accounts)
    if source == "twitter_api" or (source == "auto" and _twitter_api_available()):
        print("  [src] Twitter GraphQL API (cookies del navegador)")
        return fetch_all_twitter_api(accounts)
    print("  [src] Nitter RSS (fallback)")
    return fetch_all_rss(accounts)

# ── Parse fecha tweet ─────────────────────────────────────────────────────────

def parse_tweet_date(tw) -> datetime | None:
    # twscrape tweets already have _ts set
    if "_ts" in tw:
        return tw["_ts"]
    import email.utils
    for field in ("published", "updated"):
        val = tw.get(field)
        if val:
            try:
                t = tw.get(f"{field}_parsed")
                if t:
                    return datetime(*t[:6], tzinfo=timezone.utc)
                ts = email.utils.parsedate_to_datetime(val)
                return ts.astimezone(timezone.utc)
            except Exception:
                pass
    return None

# ── Groq: extraer señal ───────────────────────────────────────────────────────

def extract_signal(text: str, name: str, handle: str, client: Groq) -> dict | None:
    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content":
                SIGNAL_EXTRACT_PROMPT.format(name=name, handle=handle, text=text[:700])}],
            max_tokens=130,
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
        return None

# ── Evaluar resultado historico ───────────────────────────────────────────────

def evaluate_historical(instrument: str, direction: str,
                        entrada: str, tp: str, sl: str,
                        since: datetime) -> str:
    from stats import get_range_since, _parse_price
    rng = get_range_since(instrument, since)
    if not rng:
        return "NEUTRAL"
    high, low, current = rng
    entry = _parse_price(entrada)
    tp_p  = _parse_price(tp)  if tp  and tp.upper()  != "N/A" else None
    sl_p  = _parse_price(sl)  if sl  and sl.upper()  != "N/A" else None

    if direction == "LARGO":
        if tp_p  and high    >= tp_p:  return "WIN"
        if sl_p  and low     <= sl_p:  return "LOSS"
        if entry and current >  entry * 1.002: return "WIN"
        if entry and current <  entry * 0.998: return "LOSS"
    elif direction == "CORTO":
        if tp_p  and low     <= tp_p:  return "WIN"
        if sl_p  and high    >= sl_p:  return "LOSS"
        if entry and current <  entry * 0.998: return "WIN"
        if entry and current >  entry * 1.002: return "LOSS"
    return "NEUTRAL"

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",  type=int,   default=0,    help="Max traders a procesar (0=todos)")
    parser.add_argument("--delay",  type=float, default=1.2,  help="Delay entre llamadas Groq (s)")
    parser.add_argument("--source", default="auto",           help="Fuente: auto | twscrape | rss")
    args = parser.parse_args()

    groq_key = os.environ.get("GROQ_API_KEY")
    if not groq_key:
        print("ERROR: falta GROQ_API_KEY en .env")
        sys.exit(1)

    with open(os.path.join(DIR, "signal_accounts.json")) as f:
        accounts = json.load(f)
    if args.limit:
        accounts = accounts[:args.limit]

    client = Groq(api_key=groq_key)

    from stats import load_stats, save_stats, _update_stats

    now       = datetime.now(timezone.utc)
    min_age   = now - timedelta(hours=MIN_AGE_H)
    max_age   = now - timedelta(days=MAX_AGE_D)

    print(f"\n=== BACKTEST — {len(accounts)} traders | {MAX_AGE_D}d ventana ===\n")
    print(f"[1/3] Fetching tweets en paralelo ({FETCH_W} workers)...")
    t0      = time.time()
    fetched = fetch_all(accounts, source=args.source)
    total_f = sum(len(tw) for _, tw in fetched.values())
    print(f"      {total_f} tweets en {time.time()-t0:.1f}s\n")
    if total_f == 0:
        print("[!] No se obtuvieron tweets.")
        print("    Si no tienes twscrape configurado, ejecuta primero:")
        print("        python setup_cookies.py")
        print("    (requiere cookies de tu navegador con sesion en Twitter/X)")
        sys.exit(0)

    # ── Fuentes web (TradingView, StockTwits, Telegram) ─────────────────────
    if args.source != "rss":
        print("[1b/3] Fetching fuentes web (TV/StockTwits/Telegram)...")
        try:
            from signals_web import fetch_all_web
            web_msgs = fetch_all_web(max_age_days=MAX_AGE_D)
            # Convert to same handle→(acc,tweets) format
            for msg in web_msgs:
                author  = msg.get("_author", "web_source")
                src     = msg.get("_source", "web")
                pseudo_acc = {"handle": author, "name": f"{src}:{author}"}
                if author not in fetched:
                    fetched[author] = (pseudo_acc, [])
                fetched[author][1].append(msg)
            total_web = sum(len(v[1]) for k, v in fetched.items() if k not in {a["handle"] for a in accounts})
            print(f"      {total_web} mensajes web\n")
        except Exception as e:
            print(f"      [!] Error en fuentes web: {e}\n")

    print("[2/3] Filtrando y extrayendo señales con Groq...")
    stats        = load_stats()
    total_tweets = 0
    total_kw     = 0
    total_sigs   = 0
    total_eval   = 0
    wins = losses = neutrals = 0

    for handle, (acc, tweets) in fetched.items():
        name = acc["name"]
        account_sigs = 0

        for tw in tweets:
            total_tweets += 1
            text = (tw.get("title") or tw.get("summary") or "").strip()
            if not text or len(text) < 15:
                continue

            # Filtro fecha
            ts = parse_tweet_date(tw)
            if not ts:
                continue
            if ts > min_age:
                continue   # demasiado reciente, el live monitor lo cubre
            if ts < max_age:
                continue   # demasiado antiguo, precio histórico poco fiable

            # Filtro keywords
            if not any(kw in text.lower() for kw in SIGNAL_KEYWORDS):
                continue
            total_kw += 1

            # Groq
            time.sleep(args.delay)
            sig = extract_signal(text, name, handle, client)
            if not sig or sig.get("SENAL", "NO").upper() != "SI":
                continue

            entrada = sig.get("ENTRADA", "N/A").strip()
            entrada_up = entrada.upper()
            if not entrada_up or entrada_up.startswith("N/A") or entrada_up in ("?", "NONE", "NO"):
                continue

            total_sigs += 1
            account_sigs += 1
            instrument = sig.get("INSTRUMENTO", "?")
            direction  = sig.get("DIRECCION", "?")
            tp         = sig.get("TP", "N/A")
            sl         = sig.get("SL", "N/A")

            # Evaluar resultado histórico
            result = evaluate_historical(instrument, direction, entrada, tp, sl, ts)
            _update_stats(handle, name, result, stats)
            total_eval += 1
            if result == "WIN":    wins     += 1
            elif result == "LOSS": losses   += 1
            else:                  neutrals += 1

            age_h = (now - ts).total_seconds() / 3600
            src_tag = f"[{tw.get('_source','tw')}]"
            line = (f"  {src_tag} @{handle} | {instrument} {direction} @ {entrada} "
                    f"| {age_h:.0f}h ago | {result}")
            print(line.encode("ascii", "replace").decode())

        if account_sigs:
            print(f"  -- @{handle}: {account_sigs} senal(es)")

    save_stats(stats)

    print(f"\n[3/3] Resultados guardados en trader_stats.json")
    print(f"\n{'='*50}")
    print(f"  Tweets revisados   : {total_tweets}")
    print(f"  Pasaron keywords   : {total_kw}")
    print(f"  Senales extraidas  : {total_sigs}")
    print(f"  Evaluadas          : {total_eval}")
    print(f"  WIN / LOSS / NEUTRAL: {wins} / {losses} / {neutrals}")
    if total_eval:
        wr = wins / (wins + losses) * 100 if (wins + losses) else 0
        print(f"  Win rate global    : {wr:.1f}%")

    # Top 10 traders
    print(f"\n{'='*50}")
    print("  TOP TRADERS (min 2 evaluadas):")
    ranked = sorted(
        [(h, s) for h, s in stats.items()
         if s.get("wins", 0) + s.get("losses", 0) >= 2],
        key=lambda x: x[1]["win_rate"], reverse=True
    )
    medals = ["1.", "2.", "3."]
    for i, (h, s) in enumerate(ranked[:10]):
        ev  = s["wins"] + s["losses"]
        wr  = int(s["win_rate"] * 100)
        m   = medals[i] if i < 3 else f"{i+1}."
        print(f"  {m} @{h} — {wr}%  ({s['wins']}W / {s['losses']}L / {ev} ops)")

    if ranked:
        worst = ranked[-1]
        print(f"\n  Peor: @{worst[0]} — {int(worst[1]['win_rate']*100)}%")

    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
