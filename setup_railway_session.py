"""
Genera una sesión Telethon desde la IP de Railway.
Ejecutar con: railway run python setup_railway_session.py
"""
import asyncio, os

API_ID   = 31765380
API_HASH = "6e7ecbf9655afce6016bc25bd59fc3ef"
PHONE    = "+31619239901"

async def main():
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    print("Conectando a Telegram desde Railway...")
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        session_str = client.session.save()
        print(f"\nTELEGRAM_SESSION={session_str}\n")
        await client.disconnect()
        return

    result = await client.send_code_request(PHONE)
    print(f"Código enviado a {PHONE}. Introdúcelo aquí:")

    code = input("Código: ").strip()

    try:
        await client.sign_in(PHONE, code, phone_code_hash=result.phone_code_hash)
    except Exception as e:
        if "password" in str(e).lower() or "2fa" in str(e).lower():
            pwd = input("Contraseña 2FA: ").strip()
            await client.sign_in(password=pwd)
        else:
            print(f"Error: {e}")
            await client.disconnect()
            return

    session_str = client.session.save()
    await client.disconnect()
    print(f"\nTELEGRAM_SESSION={session_str}\n")

asyncio.run(main())
