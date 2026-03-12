import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient

# ───────── CONFIG ─────────

api_id = os.getenv("TELEGRAM_API_ID")
api_hash = os.getenv("TELEGRAM_API_HASH")

CHANNEL = "https://t.me/breachdetect"

STATE_FILE = "state.json"
RAW_FILE = "raw_messages.json"

BOOTSTRAP_DAYS = 15  # first run only

def state_exists():
    return os.path.exists(STATE_FILE)

def load_state():
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def save_state(last_id):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_message_id": last_id}, f)

def load_existing_messages():
    if os.path.exists(RAW_FILE):
        with open(RAW_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_messages(messages):
    with open(RAW_FILE, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=2)

# ───────── MAIN ─────────

async def main():
    client = TelegramClient("my_session", api_id, api_hash)
    await client.start()

    channel = await client.get_entity(CHANNEL)

    new_messages = []
    max_id_seen = 0

    # ───────── FIRST RUN (BOOTSTRAP) ─────────
    if not state_exists():
        print("🔰 First run detected — fetching last 15 days only")

        start_date = datetime.now(timezone.utc) - timedelta(days=BOOTSTRAP_DAYS)

        async for msg in client.iter_messages(channel):
            if msg.date and msg.date < start_date:
                break

            new_messages.append({
                "id": msg.id,
                "date": msg.date.isoformat() if msg.date else None,
                "text": msg.text,
                "views": msg.views,
                "forwards": msg.forwards,
            })

            if msg.id > max_id_seen:
                max_id_seen = msg.id

    # ───────── NORMAL INCREMENTAL MODE ─────────
    else:
        state = load_state()
        last_id = state["last_message_id"]

        print(f"📌 Fetching messages newer than ID {last_id}")

        async for msg in client.iter_messages(channel, min_id=last_id):
            new_messages.append({
                "id": msg.id,
                "date": msg.date.isoformat() if msg.date else None,
                "text": msg.text,
                "views": msg.views,
                "forwards": msg.forwards,
            })

            if msg.id > max_id_seen:
                max_id_seen = msg.id

        if max_id_seen == 0:
            print("✅ No new messages found.")
            return

    # ───────── SAVE RESULTS ─────────

    existing = load_existing_messages()
    combined = existing + new_messages

    save_messages(combined)
    save_state(max_id_seen)

    print(f"✅ Added {len(new_messages)} messages.")
    print(f"📌 Updated last_message_id to {max_id_seen}")

asyncio.run(main())
