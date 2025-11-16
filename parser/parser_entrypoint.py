import asyncio
import os

from dotenv import load_dotenv
from tg_connector.client_connection import TelegramParser


load_dotenv()


async def main():
    parser = TelegramParser(
        session_name="analyze_tg_news",
        api_id=os.getenv("api_id", 0),
        api_hash=os.getenv("api_hash", ""),
        device_model="NS685U",
        app_version="6.2.4",
        lang_code="en",
        channels=[
            "dva_majors",
            "toporlive",
            "milinfolive",
            "u_now",
            "topor",
            "opersvodki",
            "novosty937",
        ],
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        kafka_topic="raw_telegram_channels_messages",
    )

    await parser.start()


if __name__ == "__main__":
    asyncio.run(main())
