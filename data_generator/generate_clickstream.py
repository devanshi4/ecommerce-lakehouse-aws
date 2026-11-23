"""Stream simulated user activity (views, carts, purchases) to Kinesis, a few events per second."""
import boto3
import json
import random
import time
import uuid
from datetime import datetime
from faker import Faker

import config

# Same users and products as the orders, so the two datasets join.
from generate_orders import PRODUCTS, USER_IDS

fake = Faker()


EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase",
]

# Funnel-shaped: many views, fewer carts, fewest purchases.
EVENT_WEIGHTS = [30, 25, 20, 5, 12, 8]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
REFERRERS = [None, "google.com", "facebook.com", "instagram.com", "email", "direct"]


kinesis_client = boto3.client("kinesis", region_name=config.AWS_REGION)


def generate_single_event(session_id: str, user_id: str) -> dict:
    """Creates one clickstream event for a given user session."""
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    is_product_event = event_type in ["product_view", "add_to_cart", "remove_from_cart", "purchase"]
    product = random.choice(PRODUCTS) if is_product_event else None

    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product["product_id"] if product else None,
        "product_name": product["product_name"] if product else None,
        "category": product["category"] if product else None,
        "device_type": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "referrer": random.choice(REFERRERS),
        "country_code": fake.country_code(),
        "ip_address": fake.ipv4_public(),
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
    }


def send_to_kinesis(event: dict):
    """Sends a single event to the Kinesis stream."""
    kinesis_client.put_record(
        StreamName=config.KINESIS_STREAM,
        Data=json.dumps(event),
        # Same session -> same shard, so a visit's events stay in order.
        PartitionKey=event["session_id"],
    )


def stream_events(duration_seconds: int = 120):
    """Continuously generates and sends clickstream events to Kinesis."""
    print("\nStarting clickstream stream...")
    print(f"   Sending to: {config.KINESIS_STREAM}")
    print(f"   Rate: {config.CLICKSTREAM_EVENTS_PER_SECOND} events/second")
    print(f"   Duration: {duration_seconds} seconds")
    print("   Press Ctrl+C at any time to stop early\n")

    start_time = time.time()
    total_sent = 0

    active_sessions = {}

    while time.time() - start_time < duration_seconds:
        if len(active_sessions) < 5 or (len(active_sessions) < 15 and random.random() > 0.85):
            new_session_id = str(uuid.uuid4())
            new_user_id = random.choice(USER_IDS)
            active_sessions[new_session_id] = new_user_id

        session_id = random.choice(list(active_sessions.keys()))
        user_id = active_sessions[session_id]

        event = generate_single_event(session_id, user_id)

        try:
            send_to_kinesis(event)
            total_sent += 1

            if total_sent % 20 == 0:
                elapsed = time.time() - start_time
                remaining = duration_seconds - elapsed
                print(f"{total_sent} events sent | "
                      f"{elapsed:.0f}s elapsed | "
                      f"{remaining:.0f}s remaining | "
                      f"Last: {event['event_type']} from {user_id}")

        except Exception as e:
            print(f"Failed to send event: {e}")

        if len(active_sessions) > 10 and random.random() > 0.95:
            session_to_close = list(active_sessions.keys())[0]
            del active_sessions[session_to_close]

        time.sleep(1 / config.CLICKSTREAM_EVENTS_PER_SECOND)

    total_time = time.time() - start_time
    print("\nStream complete.")
    print(f"   Total events sent: {total_sent}")
    print(f"   Total time: {total_time:.0f} seconds")
    print(f"   Actual rate: {total_sent/total_time:.1f} events/second")
    print("\n   Lambda writes these events to S3 in batches.")


if __name__ == "__main__":
    stream_events(duration_seconds=120)
