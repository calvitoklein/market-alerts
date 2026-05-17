"""
Servidor HTTP temporal para generar sesión Telethon desde la IP de Railway.

Flujo:
  GET  /start  → envía código SMS al teléfono
  POST /verify → body=CODIGO → devuelve TELEGRAM_SESSION=...

Uso:
  1. railway variables set START_CMD="python session_server.py"
  2. railway up  (deploy temporal)
  3. curl https://<tu-app>.railway.app/start
  4. curl -X POST https://<tu-app>.railway.app/verify -d "12345"
  5. Copiar TELEGRAM_SESSION del response
  6. railway variables set TELEGRAM_SESSION="..." START_CMD="python tweet_monitor.py --loop"
  7. railway up  (deploy real)
"""

import asyncio, os, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID   = 31765380
API_HASH = "6e7ecbf9655afce6016bc25bd59fc3ef"
PHONE    = "+31619239901"
PORT     = int(os.environ.get("PORT", 8080))

# Estado compartido entre requests
state = {"hash": None, "session": None, "done": False}


async def _send_code():
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    if await client.is_user_authorized():
        session_str = client.session.save()
        await client.disconnect()
        state["done"] = True
        state["session"] = session_str
        return "YA_AUTORIZADO"
    result = await client.send_code_request(PHONE)
    state["hash"]    = result.phone_code_hash
    state["session"] = client.session.save()
    await client.disconnect()
    return "OK"


async def _verify(code: str):
    client = TelegramClient(StringSession(state["session"]), API_ID, API_HASH)
    await client.connect()
    try:
        await client.sign_in(PHONE, code, phone_code_hash=state["hash"])
    except Exception as e:
        if "password" in str(e).lower() or "2fa" in str(e).lower():
            await client.disconnect()
            return f"ERROR_2FA: necesita contraseña 2FA — usa /verify2fa"
        await client.disconnect()
        return f"ERROR: {e}"
    session_str = client.session.save()
    await client.disconnect()
    state["done"]    = True
    state["session"] = session_str
    return f"TELEGRAM_SESSION={session_str}"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _respond(self, code: int, body: str):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        if self.path == "/start":
            result = asyncio.run(_send_code())
            if result == "YA_AUTORIZADO":
                self._respond(200, f"Ya autorizado.\nTELEGRAM_SESSION={state['session']}\n")
            else:
                self._respond(200, f"Código enviado a {PHONE}.\nAhora: POST /verify con el código.\n")
        elif self.path == "/":
            self._respond(200, "GET /start para enviar código | POST /verify con el código\n")
        else:
            self._respond(404, "Not found\n")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        code   = self.rfile.read(length).decode().strip()
        if self.path == "/verify":
            if not state.get("hash"):
                self._respond(400, "Primero haz GET /start\n")
                return
            result = asyncio.run(_verify(code))
            self._respond(200, result + "\n")
        else:
            self._respond(404, "Not found\n")


if __name__ == "__main__":
    print(f"Session server escuchando en :{PORT}")
    print("GET  /start  → envía código SMS")
    print("POST /verify → body=CODIGO → devuelve TELEGRAM_SESSION")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
