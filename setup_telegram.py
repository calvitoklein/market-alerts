"""
Setup de sesión Telethon — ejecutar UNA SOLA VEZ en tu PC local.

Pasos previos:
  1. Ve a https://my.telegram.org/apps
  2. Crea una aplicación → anota API ID y API Hash
  3. Ejecuta: python setup_telegram.py

Al final te da TELEGRAM_SESSION (string largo).
Cópialo a tu .env y a Railway → Variables.
"""

import asyncio
import os
import sys


async def _setup():
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except ImportError:
        print("Falta telethon. Ejecuta: pip install telethon")
        return

    print("\n" + "═" * 60)
    print("  Telegram Session Setup")
    print("═" * 60)
    print("\nNecesitas API ID y API Hash de https://my.telegram.org/apps\n")

    api_id_str = input("API ID   : ").strip()
    api_hash   = input("API Hash : ").strip()
    phone      = input("Teléfono (+34612...): ").strip()

    if not api_id_str.isdigit():
        print("API ID debe ser un número.")
        return

    api_id = int(api_id_str)
    client = TelegramClient(StringSession(), api_id, api_hash)

    print("\nConectando a Telegram...")
    await client.connect()

    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        code = input("Código de verificación recibido en Telegram: ").strip()
        try:
            await client.sign_in(phone, code)
        except Exception as e:
            if "2FA" in str(e) or "password" in str(e).lower():
                pwd = input("Contraseña 2FA: ").strip()
                await client.sign_in(password=pwd)
            else:
                raise

    session_str = client.session.save()
    await client.disconnect()

    print("\n" + "═" * 60)
    print("✅  Sesión generada correctamente")
    print("═" * 60)
    print("\nAñade estas líneas a tu .env y a Railway → Variables:\n")
    print(f"TELEGRAM_API_ID={api_id}")
    print(f"TELEGRAM_API_HASH={api_hash}")
    print(f"TELEGRAM_SESSION={session_str}")
    print("\nTambién necesitas GEMINI_API_KEY (gratis en https://aistudio.google.com)\n")

    # Guardar en .env automáticamente si existe
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        save = input("¿Guardar automáticamente en .env? [s/N]: ").strip().lower()
        if save == "s":
            with open(env_path, "a") as f:
                f.write(f"\nTELEGRAM_API_ID={api_id}\n")
                f.write(f"TELEGRAM_API_HASH={api_hash}\n")
                f.write(f"TELEGRAM_SESSION={session_str}\n")
            print("✅  Guardado en .env")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(_setup())
