"""
Telethon + Gemini Flash Vision — análisis de imágenes y texto de señales Telegram.

Flujo:
  1. Telethon conecta como usuario real y descarga fotos de los canales.
  2. Gemini Flash (multimodal) analiza imagen + texto juntos en una sola llamada.
  3. Señales extraídas van al mismo buffer que las señales de texto.

Setup (una sola vez en local):
  python setup_telegram.py
  → te da TELEGRAM_SESSION que metes en .env y en Railway variables.
"""

import os
import io
import asyncio
import time
import re
from datetime import datetime, timezone

DIR = os.path.dirname(os.path.abspath(__file__))

TELEGRAM_API_ID   = int(os.environ.get("TELEGRAM_API_ID", "0") or "0")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION  = os.environ.get("TELEGRAM_SESSION", "")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY", "")

MAX_MSGS_PER_CHANNEL = 15   # mensajes a revisar por canal por ciclo
MAX_IMAGES_PER_CYCLE = 30   # máx. llamadas Gemini por ciclo (free: 15 RPM)
VISION_CALL_DELAY    = 4.0  # segundos entre llamadas (free tier: 15 RPM)
MAX_MSG_AGE_HOURS    = 20   # ignorar mensajes de más de 20 horas


# ── Gemini Flash ─────────────────────────────────────────────────────────────────

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
        _gemini_model = genai.GenerativeModel("gemini-1.5-flash")
        return _gemini_model
    except Exception as e:
        print(f"  [VISION] Gemini init error: {e}")
        return None


def _clean_symbol(raw: str) -> str:
    s = raw.strip().upper()
    s = re.split(r'[/\s,]', s)[0]
    s = re.split(r'[.\-]', s)[0]
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s[:12]


_EXTRACTION_PROMPT = """\
You are a professional trading signal analyst. Analyze this image (and any message text provided) from a trading signals channel.

Step 1 — Is there a clear, ACTIVE (not closed) trading signal?
- Must have a buy/long or sell/short direction
- Must have at least one specific price level (entry, TP, or SL)
- If the image shows a CLOSED trade, profit announcement, or past result → answer NO

Step 2 — If YES, extract the signal details in this EXACT format:
SIGNAL: YES
DIRECTION: LONG or SHORT
SYMBOL: single ticker (e.g. BTCUSDT, ETHUSDT, XAUUSD, SOLUSDT, EURUSD)
ENTRY: entry price number or N/A
TP: take profit price or N/A
SL: stop loss price or N/A

If no clear active signal: reply with exactly:
SIGNAL: NO
"""


def analyze_image(image_bytes: bytes, text_context: str = "") -> dict | None:
    """
    Envía imagen (+ texto opcional del mensaje) a Gemini Flash y extrae señal.
    Devuelve dict con claves INSTRUMENTO/DIRECCION/ENTRADA/TP/SL/HORIZONTE,
    o None si no hay señal clara.
    """
    model = _get_gemini_model()
    if not model or not image_bytes:
        return None
    try:
        from PIL import Image
        import google.generativeai as genai

        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")

        prompt_parts = [_EXTRACTION_PROMPT]
        if text_context and text_context.strip():
            prompt_parts.append(f"\nMessage text from channel:\n{text_context.strip()[:500]}")
        prompt_parts.append(img)

        response = model.generate_content(prompt_parts)
        raw = response.text.strip() if response.text else ""

        if not raw or "SIGNAL: NO" in raw.upper():
            return None

        result = {}
        for line in raw.split("\n"):
            if ":" in line:
                k, _, v = line.partition(":")
                result[k.strip().upper()] = v.strip()

        if result.get("SIGNAL", "").upper() != "YES":
            return None

        direction = result.get("DIRECTION", "").upper()
        instr     = _clean_symbol(result.get("SYMBOL", ""))

        if not instr or len(instr) > 10:
            return None
        if direction not in ("LONG", "SHORT"):
            return None

        sl = result.get("SL", "N/A")
        # Rechazar señales sin SL — broker lo requiere
        if sl == "N/A" or not sl:
            return None

        return {
            "SENAL":       "SI",
            "INSTRUMENTO": instr,
            "DIRECCION":   "LARGO" if direction == "LONG" else "CORTO",
            "ENTRADA":     result.get("ENTRY", "N/A"),
            "TP":          result.get("TP",    "N/A"),
            "SL":          sl,
            "HORIZONTE":   "SCALP",
        }
    except Exception as e:
        err = str(e)
        if "429" in err or "quota" in err.lower() or "rate" in err.lower():
            print(f"  [VISION] Gemini rate limit — skip imagen")
        elif "safety" in err.lower() or "block" in err.lower():
            pass  # bloqueo de safety filter — skip silencioso
        else:
            print(f"  [VISION] Gemini error: {err[:120]}")
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

        try:
            await client.get_dialogs(limit=300)
        except Exception:
            pass

        for name, handle in channels.items():
            try:
                if str(handle).startswith("id:"):
                    entity = await client.get_entity(int(handle[3:]))
                else:
                    entity = await client.get_entity(f"@{handle}")
                async for msg in client.iter_messages(entity, limit=limit):
                    if msg.date:
                        msg_ts  = msg.date.replace(tzinfo=timezone.utc) if msg.date.tzinfo is None else msg.date.astimezone(timezone.utc)
                        age_h   = (datetime.now(timezone.utc) - msg_ts).total_seconds() / 3600
                        if age_h > MAX_MSG_AGE_HOURS:
                            continue

                    text      = msg.text or msg.message or ""
                    img_bytes = None
                    if msg.photo:
                        buf = io.BytesIO()
                        await client.download_media(msg.photo, buf)
                        img_bytes = buf.getvalue()
                    if text or img_bytes:
                        results.append((name, handle, text, img_bytes, msg.id))
            except Exception:
                pass

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
