"""
Clickstream event producer with DELIBERATE chaos injection.

This does NOT just simulate a clean e-commerce event stream. It reproduces,
on purpose, every trap that real production streaming systems hit:

  - DUPLICATE events      (client retry on ack timeout)
  - OUT-OF-ORDER events   (mobile client buffers offline, flushes late)
  - LATE events           (arrive after their session has already closed)
  - MALFORMED events      (broken client SDK sending invalid bytes)
  - SCHEMA-INVALID events (business-rule violations that still pass Avro,
                            e.g. negative price, purchase with no product_id)
  - BURSTY traffic        (flash-sale style spike)

Every chaos type is independently toggleable and logged, so when you're
demoing this you can point at a specific line of output and explain exactly
what's about to break downstream — and then show the pipeline handle it.
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from faker import Faker

fake = Faker()

TOPIC = "clickstream.raw.v1"
SCHEMA_REGISTRY_URL = "http://localhost:18081"
BOOTSTRAP_SERVERS = "localhost:19092"

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "REMOVE_FROM_CART", "PURCHASE"]
CATEGORIES = ["electronics", "apparel", "home", "beauty", "sports", "books"]
DEVICES = ["WEB", "IOS", "ANDROID"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(BASE_DIR, "..", "schemas", "click_event_v1.avsc")

with open(SCHEMA_PATH) as f:
    CLICK_EVENT_SCHEMA = f.read()


def now_ms(offset_seconds: int = 0) -> int:
    return int((datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).timestamp() * 1000)


def make_event(user_id: str, session_id: str, event_time_offset_s: int = 0,
               force_invalid: bool = False) -> dict:
    """Build one otherwise-realistic event. force_invalid produces an event
    that IS valid Avro (so it passes the schema registry) but violates a
    business rule — this is the case the schema registry CANNOT catch for
    you, and has to be handled by validation logic downstream."""
    event_type = random.choice(EVENT_TYPES)
    product_id = f"prod-{random.randint(1000, 9999)}"
    price = round(random.uniform(5, 500), 2)
    quantity = random.randint(1, 3)

    if force_invalid:
        # e.g. a purchase with a negative price, or missing product_id —
        # passes Avro's type check fine, but is nonsense business-wise
        price = -abs(price)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "category": random.choice(CATEGORIES),
        "price": price,
        "quantity": quantity,
        "event_time": now_ms(event_time_offset_s),
        "ingest_time": now_ms(),
        "device_type": random.choice(DEVICES),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"  [DELIVERY FAILED] {err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=5.0, help="events/sec baseline")
    parser.add_argument("--duplicate-rate", type=float, default=0.05,
                         help="fraction of events re-sent as duplicates (retry simulation)")
    parser.add_argument("--late-rate", type=float, default=0.08,
                         help="fraction of events with event_time in the past (buffered/offline client)")
    parser.add_argument("--malformed-rate", type=float, default=0.02,
                         help="fraction of messages sent as raw garbage bytes, bypassing Avro entirely")
    parser.add_argument("--invalid-rate", type=float, default=0.03,
                         help="fraction of events that are schema-valid but business-invalid")
    parser.add_argument("--burst-every", type=int, default=60,
                         help="seconds between simulated flash-sale bursts (0 to disable)")
    parser.add_argument("--burst-multiplier", type=int, default=15)
    parser.add_argument("--burst-duration", type=int, default=10)
    args = parser.parse_args()

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr_client, CLICK_EVENT_SCHEMA)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    users = [str(uuid.uuid4()) for _ in range(200)]
    sessions = {u: str(uuid.uuid4()) for u in users}

    print(f"Producing to '{TOPIC}' — Ctrl+C to stop")
    print(f"Chaos config: dup={args.duplicate_rate} late={args.late_rate} "
          f"malformed={args.malformed_rate} invalid={args.invalid_rate}\n")

    last_burst = time.time()
    in_burst_until = 0

    while True:
        current_rate = args.rate
        if args.burst_every and time.time() - last_burst > args.burst_every:
            in_burst_until = time.time() + args.burst_duration
            last_burst = time.time()
            print(f"  ⚡ BURST STARTED — {args.rate * args.burst_multiplier:.0f} events/sec for {args.burst_duration}s")

        if time.time() < in_burst_until:
            current_rate = args.rate * args.burst_multiplier

        user_id = random.choice(users)
        session_id = sessions[user_id]

        roll = random.random()

        if roll < args.malformed_rate:
            # POISON PILL: not valid Avro at all — simulates a broken/old
            # client SDK sending garbage. Downstream MUST dead-letter this,
            # not crash the whole consumer.
            garbage = json.dumps({"this": "is not avro", "broken": True}).encode("utf-8")
            producer.produce(TOPIC, key=session_id.encode(), value=garbage,
                              callback=delivery_report)
            print(f"  [MALFORMED] sent raw non-Avro bytes for session {session_id[:8]}")

        elif roll < args.malformed_rate + args.invalid_rate:
            event = make_event(user_id, session_id, force_invalid=True)
            serialized = avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE))
            producer.produce(TOPIC, key=session_id.encode(), value=serialized,
                              callback=delivery_report)
            print(f"  [INVALID] business-rule violation (negative price) event_id={event['event_id'][:8]}")

        elif roll < args.malformed_rate + args.invalid_rate + args.late_rate:
            # LATE / OUT-OF-ORDER: event_time is minutes in the past,
            # simulating an offline mobile client flushing a buffer
            delay = random.randint(60, 900)  # 1–15 min late
            event = make_event(user_id, session_id, event_time_offset_s=-delay)
            serialized = avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE))
            producer.produce(TOPIC, key=session_id.encode(), value=serialized,
                              callback=delivery_report)
            print(f"  [LATE] event_time {delay}s in the past, event_id={event['event_id'][:8]}")

        else:
            event = make_event(user_id, session_id)
            serialized = avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE))
            producer.produce(TOPIC, key=session_id.encode(), value=serialized,
                              callback=delivery_report)

            if random.random() < args.duplicate_rate:
                # DUPLICATE: simulate a producer retry after a slow/failed ack
                # — same event_id sent again, exactly what happens in real
                # at-least-once delivery
                time.sleep(0.05)
                producer.produce(TOPIC, key=session_id.encode(), value=serialized,
                                  callback=delivery_report)
                print(f"  [DUPLICATE] re-sent event_id={event['event_id'][:8]}")

        producer.poll(0)
        time.sleep(1.0 / current_rate)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
