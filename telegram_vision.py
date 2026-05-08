"""
Telethon + Google Gemini Vision — análisis de imágenes de señales Telegram.

Flujo:
  1. Telethon conecta como usuario real y descarga fotos de los canales.
  2. Gemini 2.0 Flash (gratis: 1500 req/día, 15 RPM) analiza cada imagen.
  3. Señales extraídas van al mismo buffer que las señales de texto.

Setup (una sola vez en local):
  python setup_telegram.py
  → te da TELEGRAM_SESSION que metes en .env y en Railway variables.
"""

import os
import io
import asyncio
import base64
import time

DIR = os.path.dirname(os.path.abspath(__file__))

TELEGRAM_API_ID   = int(os.environ.get("TELEGRAM_API_ID", "0") or "0")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION  = os.environ.get("TELEGRAM_SESSION", "")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY", "")

MAX_MSGS_PER_CHANNEL = 15   # mensajes a revisar por canal por ciclo
MAX_IMAGES_PER_CYCLE = 25   # máx. llamadas Gemini (límite free: 15 RPM)
GEMINI_CALL_DELAY    = 4.5  # segundos entre llamadas Gemini (15 RPM = 4s mín)

VISION_PROMPT = """Analyze this image from a Telegram trading signal channel.

If this image shows an ACTIVE trade setup with a clear direction and at least one price level, respond EXACTLY in this format:
SENAL: SI
INSTRUMENTO: [XAU/BTC/ETH/SOL/NQ/ES/BNB/XRP/ADA/AVAX/DOGE/PEPE/SUI/MATIC/LINK or other symbol visible]
DIRECCION: LARGO or CORTO
ENTRADA: [entry price or range like 4710-4706. If only SL/TP visible, use N/A]
TP: [take profit price, or N/A]
SL: [stop loss price, or N/A]
HORIZONTE: SCALP or DIA or SWING

Rules:
- LARGO = BUY/LONG/COMPRA. CORTO = SELL/SHORT/VENTA.
- If the chart shows a "TP hit" / "resultado" / closed trade → SENAL: NO
- If the image is just a chart analysis without a specific trade call → SENAL: NO
- Only extract prices that are CLEARLY visible as numbers in the image.

If NOT an actionable signal, respond only:
SENAL: NO"""

# ── Gemini ─────────────────────────────────────────────────────────────────────

_gemini_model = None

def _get_gemini_model():
    global _gemini_model
    if _gemini_model:
        return _gemini_model
    if not GEMINI_API_KEY:
        return None
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        _gemini_model = genai.GenerativeModel("gemini-2.0-flash")
        return _gemini_model
    except Exception as e:
        print(f"  [VISION] Gemini init error: {e}")
        return None


def analyze_image(image_bytes: bytes) -> dict | None:
    """
    Envía imagen a Gemini Flash y extrae señal de trading.
    Devuelve dict con claves INSTRUMENTO/DIRECCION/ENTRADA/TP/SL/HORIZONTE,
    o None si no hay señal clara.
    """
    model = _get_gemini_model()
    if not model or not image_bytes:
        return None
    try:
        import google.generativeai as genai
        img_part = genai.types.Part.from_bytes(
            data=image_bytes,
            mime_type="image/jpeg",
        )
        resp  = model.generate_content([VISION_PROMPT, img_part])
        raw   = resp.text.strip()
        result = {}
        for line in raw.split("\n"):
            if ":" in line:
                k, _, v = line.partition(":")
                result[k.strip()] = v.strip()

        if result.get("SENAL", "NO").upper() != "SI":
            return None
        # Requiere al menos dirección e instrumento legibles
        if not result.get("INSTRUMENTO") or not result.get("DIRECCION"):
            return None
        return result
    except Exception as e:
        print(f"  [VISION] Gemini error: {e}")
        return None


# ── Telethon async ─────────────────────────────────────────────────────────────

async def _fetch_async(channels: dict, limit: int = MAX_MSGS_PER_CHANNEL) -> list:
    """
    Descarga mensajes + fotos de los canales Telegram via Telethon.
    Devuelve lista de (channel_name, handle, text, image_bytes | None, msg_id).
    """
    if not (TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_SESSION):
        return []

    from telethon import TelegramClient
    from telethon.sessions import StringSession

    results = []
    client  = TelegramClient(
        StringSession(TELEGRAM_SESSION),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        connection_retries=2,
        timeout=20,
    )

    try:
        await client.connect()
        if not await client.is_user_authorized():
            print("  [TELETHON] Sesión expirada — ejecuta setup_telegram.py de nuevo")
            return []

        for name, handle in channels.items():
            try:
                entity = await client.get_entity(f"@{handle}")
                async for msg in client.iter_messages(entity, limit=limit):
                    text      = msg.text or msg.message or ""
                    img_bytes = None
                    if msg.photo:
                        buf = io.BytesIO()
                        await client.download_media(msg.photo, buf)
                        img_bytes = buf.getvalue()
                    if text or img_bytes:
                        results.append((name, handle, text, img_bytes, msg.id))
            except Exception:
                pass  # canal no disponible — skip silencioso

    except Exception as e:
        print(f"  [TELETHON] Error conexión: {e}")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    return results


def fetch_channels_with_images(channels: dict) -> list:
    """Wrapper síncrono para _fetch_async."""
    if not TELEGRAM_SESSION:
        return []
    try:
        return asyncio.run(_fetch_async(channels))
    except RuntimeError:
        # Si ya hay un event loop corriendo (pytest, Jupyter), usar nest_asyncio
        try:
            import nest_asyncio
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(_fetch_async(channels))
        except Exception as e:
            print(f"  [TELETHON] asyncio error: {e}")
            return []
    except Exception as e:
        print(f"  [TELETHON] Error: {e}")
        return []


def is_configured() -> bool:
    """True si Telethon y Gemini están configurados."""
    return bool(TELEGRAM_SESSION and TELEGRAM_API_ID and TELEGRAM_API_HASH and GEMINI_API_KEY)
