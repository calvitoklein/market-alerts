"""
Configura twscrape usando cookies del navegador (bypass Cloudflare).

Instrucciones:
1. Abre twitter.com / x.com en tu navegador (sesion iniciada con judeadasva78265)
2. Abre DevTools (F12) > Application > Cookies > https://x.com
3. Copia los valores de: auth_token  y  ct0
4. Ejecuta este script y pegalos cuando se pida

Uso:
    python setup_cookies.py
    python setup_cookies.py --auth_token XXX --ct0 YYY
"""

import argparse, sys, os

DIR = os.path.dirname(os.path.abspath(__file__))

def load_env(path=os.path.join(DIR, ".env")):
    try:
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())
    except Exception:
        pass

load_env()

TWITTER_USER  = os.environ.get("TWITTER_USER",  "judeadasva78265")
TWITTER_PASS  = os.environ.get("TWITTER_PASS",  "")
TWITTER_EMAIL = os.environ.get("TWITTER_EMAIL", "")

DB_PATH = os.path.join(DIR, "twscrape_accounts.db")

def setup(auth_token: str, ct0: str):
    from twitter_api import TwitterAPI, save_cookies

    print(f"\n[+] Guardando cookies...")
    save_cookies(auth_token, ct0)

    print(f"[+] Probando conexion (tweets de @RektCapital)...")
    api = TwitterAPI(auth_token, ct0)
    tweets = api.user_tweets("RektCapital", limit=3)

    if tweets:
        for tw in tweets:
            text = (tw["title"] or "")[:80].encode("ascii", "replace").decode()
            print(f"    [{tw['_ts']}] {text}")
        print(f"\n[OK] Twitter API configurada. {len(tweets)} tweets obtenidos.")
        print(f"\nAhora puedes ejecutar:")
        print(f"    python backtest.py --limit 10")
    else:
        print(f"\n[!] No se obtuvieron tweets. Las cookies pueden estar expiradas.")
        print(f"    Actualiza auth_token y ct0 desde el navegador (F12 > Application > Cookies).")

def check_existing():
    from twitter_api import is_configured, COOKIES_FILE
    if is_configured():
        print(f"Cookies configuradas en {COOKIES_FILE}")
        from twitter_api import TwitterAPI
        api = TwitterAPI.from_file()
        tweets = api.user_tweets("RektCapital", limit=1)
        print(f"Test: {'OK - ' + str(len(tweets)) + ' tweet(s)' if tweets else 'FAIL - cookies expiradas'}")
    else:
        print("No hay cookies configuradas. Ejecuta: python setup_cookies.py --auth_token XXX --ct0 YYY")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--auth_token", default="", help="Valor de auth_token de las cookies")
    parser.add_argument("--ct0",        default="", help="Valor de ct0 de las cookies")
    parser.add_argument("--check",      action="store_true", help="Solo verificar cookies existentes")
    args = parser.parse_args()

    if args.check:
        check_existing()
        return

    if args.auth_token and args.ct0:
        setup(args.auth_token, args.ct0)
        return

    # Modo interactivo
    print("=" * 60)
    print("  CONFIGURACION twscrape via Cookies del Navegador")
    print("=" * 60)
    print("""
Pasos para obtener las cookies:

1. Abre https://x.com en Chrome/Edge/Firefox
2. Inicia sesion con la cuenta judeadasva78265
3. Abre DevTools: F12 (o Ctrl+Shift+I)
4. Ve a: Application (Chrome) / Storage (Firefox) > Cookies > https://x.com
5. Busca y copia el VALOR (no el nombre) de:
     - auth_token   (una cadena larga de letras y numeros)
     - ct0          (otra cadena hexadecimal)
""")
    auth_token = input("Pega el valor de auth_token: ").strip()
    ct0        = input("Pega el valor de ct0:        ").strip()

    if not auth_token or not ct0:
        print("ERROR: necesitas proporcionar ambos valores.")
        sys.exit(1)

    asyncio.run(setup(auth_token, ct0))

if __name__ == "__main__":
    main()
