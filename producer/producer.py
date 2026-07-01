"""
Example Kafka producer for learning purposes.

Sends synthetic "events" keyed by user_id, so that messages for the same
user always land on the same partition (demonstrating key-based ordering).
Most Kafka producer config knobs are exposed as env vars so you can change
them in docker-compose.yml and observe the effect (throughput, batching,
durability) without touching code.
"""

import json
import logging
import os
import random
import signal
import time
import uuid

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "login", "logout"]

running = True


def handle_shutdown(signum, frame):
    global running
    log.info("Received signal %s, shutting down...", signum)
    running = False


def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        log.info(
            "Delivered key=%s to %s [partition %d] @ offset %d",
            msg.key().decode() if msg.key() else None,
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def build_producer_config() -> dict:
    return {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        # acks=all + min.insync.replicas (set on the topic) is what actually
        # gives you durability guarantees across broker failures.
        "acks": os.environ.get("ACKS", "all"),
        "enable.idempotence": os.environ.get("ENABLE_IDEMPOTENCE", "true").lower() == "true",
        # Batching knobs: higher linger.ms / batch.size trade latency for
        # throughput and better compression ratios.
        "linger.ms": int(os.environ.get("LINGER_MS", "50")),
        "batch.size": int(os.environ.get("BATCH_SIZE", "16384")),
        "compression.type": os.environ.get("COMPRESSION_TYPE", "snappy"),
        "retries": 5,
        "client.id": f"producer-{uuid.uuid4().hex[:8]}",
    }


def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    topic = os.environ.get("TOPIC", "events")
    messages_per_second = float(os.environ.get("MESSAGES_PER_SECOND", "5"))
    key_cardinality = int(os.environ.get("KEY_CARDINALITY", "20"))
    delay = 1.0 / messages_per_second if messages_per_second > 0 else 0

    config = build_producer_config()
    log.info("Starting producer with config: %s", {k: v for k, v in config.items()})
    producer = Producer(config)

    sent = 0
    while running:
        user_id = f"user-{random.randint(1, key_cardinality)}"
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_type": random.choice(EVENT_TYPES),
            "timestamp": time.time(),
            "value": round(random.uniform(1, 100), 2),
        }

        # Keying by user_id sends every event for that user to the same
        # partition, so per-key ordering is preserved.
        producer.produce(
            topic,
            key=user_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % 50 == 0:
            log.info("Sent %d messages so far", sent)

        if delay:
            time.sleep(delay)

    log.info("Flushing remaining messages...")
    producer.flush(10)
    log.info("Producer stopped after sending %d messages", sent)


if __name__ == "__main__":
    main()
