import logging
from parser.message_producer.kafka_connector import KafkaProducer
from parser.models import NewMessage
from typing import List

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors.rpcerrorlist import (
    ChannelInvalidError,
    ChannelPrivateError,
)


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TelegramParser:
    def __init__(
        self,
        session_name: str = None,
        api_id: int = 0,
        api_hash: str = "",
        device_model: str = "",
        app_version: str = "",
        lang_code: str = "en",
        channels: List[str] = None,
        kafka_bootstrap_servers: str = "",
        kafka_topic: str = None,
    ):

        self.client = TelegramClient(
            session=session_name,
            api_id=api_id,
            api_hash=api_hash,
            device_model=device_model,
            app_version=app_version,
            lang_code=lang_code,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers
        )
        self.topic = kafka_topic

        self.channels = channels
        self._register_handlers()

    def _register_handlers(self):
        @self.client.on(events.NewMessage(chats=self.channels))
        async def on_new_message(event):
            print("🎯 ПОЛУЧЕНО НОВОЕ СООБЩЕНИЕ!")
            await self._handle_new_message(event)

    # TODO: handle updates (not real-time message handling for some channels)
    # e.g. INFO:telethon.client.updates:Got difference for channel 1743216360 updates

    async def _handle_new_message(self, event):
        try:
            channel = await event.get_chat()
            message_data = self._parse_new_message(event, channel)

            self._produce_message_to_kafka_topic(message_data)

            logger.info(
                f"📨 Sent to Kafka: {message_data.url} from {channel.title}"
            )

        except Exception as e:
            logger.exception(f"❌ Error processing message: {e}")

    def _parse_new_message(self, event, channel) -> NewMessage:
        return NewMessage(
            timestamp=str(event.message.date),
            url=f"https://t.me/{channel.username}/{event.message.id}",
            channel=channel.title,
            channel_id=channel.id,
            text=event.message.message,
        )

    def _produce_message_to_kafka_topic(self, message_data: NewMessage):
        try:
            message = message_data.model_dump()

            self.producer.send_message(topic=self.topic, message=message)
        except Exception as e:
            logger.exception(f"❌ Failed to send message to Kafka: {e}")

    async def start(self):
        try:
            self.producer.start()
            await self.client.start()

            for channel in self.channels:
                try:
                    entity = await self.client.get_entity(channel)
                    logger.info(f"✅ {channel} -> {entity.title}")
                except ChannelInvalidError:
                    logger.exception(f"Invalid channel username: {channel}")
                    self.channels.remove(channel)
                except ChannelPrivateError:
                    logger.exception(
                        f"You must be a member of channel {channel }to be able to parse data from it"
                    )
                    self.channels.remove(channel)
                except Exception as e:
                    logger.exception(f"❌ {channel} -> Caught error: {e}")
                    self.channels.remove(channel)

            print(f"👂 Listening to new messages from: {self.channels}")
            await self.client.run_until_disconnected()

        except Exception:
            logger.exception("Caught unknown error", exc_info=True)
            await self.stop()

    async def stop(self):
        if self.client.is_connected():
            await self.client.disconnect()
