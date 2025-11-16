import json
import logging

from confluent_kafka import Producer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, bootstrap_servers: str = None):
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "telegram-parser",
            "acks": "all",
            "retries": 3,
        }
        self.producer = None

    def start(self):
        self.producer = Producer(self.config)

    def stop(self):
        if self.producer:
            self.producer.flush(30)

    def delivery_report(self, error, message):
        if error:
            logger.error(f"Delivery failed: {error}")
        else:
            logger.info(
                f"Delivered successfully: {message.value().decode('utf-8')}"
            )

    def send_message(self, topic: str = None, message: dict = None):
        if not self.producer:
            raise RuntimeError("Producer not started")

        try:
            payload = json.dumps(message).encode("utf-8")
            self.producer.produce(
                topic=topic, value=payload, callback=self.delivery_report
            )
            self.producer.poll(0)

            logger.info(
                f"📤 Message sent to {topic}: {message.get('text', 'unknown')}"
            )

        except Exception as e:
            logger.exception(f"Failed to send message: {e}")
            raise
