import asyncio
from telethon import TelegramClient

api_id = os.getenv("TELEGRAM_API_ID")
api_hash = os.getenv("TELEGRAM_API_HASH")


async def main():
    client = TelegramClient("my_session", api_id, api_hash)
    await client.start()
    print("✅ You are now connected to Telegram")

asyncio.run(main())
