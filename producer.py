import json
import os
import uuid
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from dotenv import load_dotenv

from log_handler import logger, shutdown_telemetry

load_dotenv()


def _build_config() -> dict[str, Any]:
    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        "client.id": os.getenv("KAFKA_PRODUCER_CLIENT_ID", "purelens-events-documents-producer"),
        "acks": os.getenv("KAFKA_PRODUCER_ACKS", "all"),
        "enable.idempotence": os.getenv("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", "true").lower() == "true",
    }


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        return
    logger.info("Delivered to %s [%s] @ offset %s", msg.topic(), msg.partition(), msg.offset())


def produce(producer: Producer, topic: str, value: dict[str, Any], key: str | None = None) -> None:
    producer.produce(
        topic,
        key=key.encode() if key else None,
        value=json.dumps(value).encode(),
        on_delivery=_on_delivery,
    )
    # Serve delivery callbacks from earlier produce() calls
    producer.poll(0)


def main() -> None:
    topic = os.environ["KAFKA_TOPIC"]
    producer = Producer(_build_config())

    try:
        # TODO: replace with real document events
        document_id = str(uuid.uuid4())
        produce(producer, topic, {"document_id": document_id, "event": "created"}, key=document_id)
    finally:
        remaining = producer.flush(10)
        if remaining:
            logger.warning("%s message(s) not delivered before flush timeout", remaining)
        shutdown_telemetry()


if __name__ == "__main__":
    main()
