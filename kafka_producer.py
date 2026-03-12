import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import settings
from typing import Any, Dict

logger = logging.getLogger(__name__)

producer: KafkaProducer = None


def get_producer() -> KafkaProducer:
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
        )
    return producer


def send_notification_message(business_id: str, message: Dict[str, Any]) -> bool:
    try:
        producer = get_producer()
        future = producer.send(
            settings.kafka_topic,
            key=business_id,
            value=message
        )
        future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f"Failed to send message to Kafka: {e}")
        return False


def send_to_dlq_topic(business_id: str, message: Dict[str, Any], error: str = "") -> bool:
    try:
        dlq_message = {
            **message,
            "dlq_reason": error,
            "dlq_time": time.time()
        }
        producer = get_producer()
        future = producer.send(
            settings.kafka_dlq_topic,
            key=business_id,
            value=dlq_message
        )
        future.get(timeout=10)
        logger.info(f"Message {business_id} sent to DLQ")
        return True
    except KafkaError as e:
        print(f"Failed to send message to DLQ: {e}")
        return False


def close_producer():
    global producer
    if producer:
        producer.close()
        producer = None
