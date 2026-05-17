"""
Telethon real-time listener — recibe mensajes en el momento en que se publican.

Arquitectura:
  - Un thread background corre el event loop de Telethon.
  - Cuando llega un mensaje nuevo en cualquier canal monitorizado,
    se añade a una deque thread-safe.
  - El main loop llama a listener.drain() cada ciclo para procesar lo acumulado.

Ventaja sobre polling:
  - Señal llega <1s después de publicarse → precio siempre actual.
  - No necesita MAX_MSG_AGE_HOURS ni filtros de stale agresivos.
  - Consume mucho menos recursos (sin fetch masivo cada 5 min).
"""

import os
import io
import asyncio
import threading
import time
from collections import deque
from datetime import datetime, timezone

DIR = os.path.dirname(os.path.abspath(__file__))

_env_path = os.path.join(DIR, ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

TELEGRAM_API_ID   = int(os.environ.get("TELEGRAM_API_ID",  "0") or "0")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION  = os.environ.get("TELEGRAM_SESSION",  "")

RECONNECT_DELAY = 120  # segundos entre intentos de reconexión (Telegram necesita tiempo para liberar sesión)


class TelegramListener:
    """
    Escucha mensajes nuevos en los canales dados y los pone en una queue.
    Corre en un thread separado — thread-safe para un productor y un consumidor.
    """

    def __init__(self, channels: dict):
        self.channels  = channels           # {nombre: handle_o_id}
        self._queue    = deque(maxlen=1000)
        self._thread   = None
        self._loop     = None
        self._running  = False
        self._ready    = threading.Event()  # se activa cuando la conexión está lista
        self._n_chats  = 0

    # ── API pública ────────────────────────────────────────────────────────────

    def start(self):
        if not (TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_SESSION):
            print("  [RT] Telethon no configurado — listener desactivado")
            return
        self._running = True
        self._thread  = threading.Thread(target=self._run, name="TelegramRT", daemon=True)
        self._thread.start()

    def wait_ready(self, timeout: float = 60.0) -> bool:
        """Bloquea hasta que el listener esté conectado y escuchando, o timeout."""
        return self._ready.wait(timeout)

    def drain(self) -> list:
        """
        Devuelve todos los mensajes pendientes y limpia la queue.
        Formato: (channel_name, handle, text, image_bytes|None, msg_id, msg_ts)
        """
        items = list(self._queue)
        self._queue.clear()
        return items

    def is_running(self) -> bool:
        return self._running and self._thread is not None and self._thread.is_alive()

    @property
    def n_chats(self) -> int:
        return self._n_chats

    # ── Internos ───────────────────────────────────────────────────────────────

    def _run(self):
        """Entry point del thread — reconecta automáticamente si se cae."""
        while self._running:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            # Silenciar los warnings de "Task was destroyed" y errores internos de Telethon
            loop.set_exception_handler(lambda l, ctx: None)
            self._loop = loop
            try:
                loop.run_until_complete(self._listen())
            except Exception as e:
                print(f"  [RT] Error: {e} — reconectando en {RECONNECT_DELAY}s")
                self._ready.clear()
            finally:
                # Cancelar tareas pendientes antes de cerrar el loop
                try:
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
                try:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                except Exception:
                    pass
                loop.close()
            if self._running:
                time.sleep(RECONNECT_DELAY)

    async def _listen(self):
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession

        client = TelegramClient(
            StringSession(TELEGRAM_SESSION),
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH,
            connection_retries=5,
            timeout=30,
        )

        try:
            await client.connect()
        except Exception as e:
            print(f"  [RT] connect error: {e}")
            raise

        if not await client.is_user_authorized():
            print("  [RT] Sesión Telethon expirada — ejecuta setup_telegram.py")
            self._running = False
            await client.disconnect()
            return

        try:
            # Poblar caché de entidades (necesario para acceder a canales privados por ID)
            print("  [RT] Cargando diálogos...")
            try:
                await client.get_dialogs(limit=300)
            except Exception as e:
                print(f"  [RT] get_dialogs warning: {e}")

            # Resolver entidades de los canales
            chat_entities = []
            for name, handle in self.channels.items():
                try:
                    h = str(handle)
                    if h.startswith("id:"):
                        entity = await client.get_entity(int(h[3:]))
                    else:
                        entity = await client.get_entity(f"@{handle}")
                    chat_entities.append(entity)
                except BaseException:
                    pass   # canal no accesible o CancelledError — skip silencioso

            self._n_chats = len(chat_entities)
            print(f"  [RT] Listener activo — {self._n_chats}/{len(self.channels)} canales")
            self._ready.set()

            @client.on(events.NewMessage(chats=chat_entities))
            async def _handler(event):
                try:
                    msg  = event.message
                    text = msg.text or msg.message or ""

                    img_bytes = None
                    if msg.photo:
                        buf = io.BytesIO()
                        await client.download_media(msg.photo, buf)
                        img_bytes = buf.getvalue()

                    if not (text or img_bytes):
                        return

                    # Resolver nombre y handle del chat
                    chat = await event.get_chat()
                    username = getattr(chat, "username", None)
                    handle   = f"@{username}" if username else f"id:{chat.id}"
                    ch_name  = getattr(chat, "title", handle)

                    msg_ts = msg.date.replace(tzinfo=timezone.utc) if msg.date.tzinfo is None else msg.date

                    self._queue.append((ch_name, handle, text, img_bytes, msg.id, msg_ts))

                except Exception as e:
                    print(f"  [RT] handler error: {e}")

            await client.run_until_disconnected()
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass


# ── Singleton global ───────────────────────────────────────────────────────────

_listener: TelegramListener | None = None

def get_listener(channels: dict) -> TelegramListener:
    """Devuelve (y arranca si es necesario) el listener singleton."""
    global _listener
    if _listener is None:
        _listener = TelegramListener(channels)
        _listener.start()
    return _listener
