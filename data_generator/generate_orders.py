"""Simulate a nightly order export: generate a day of orders and upload them as CSV to S3 bronze.

The product catalog and user IDs are shared with the clickstream generator, so orders
and clickstream events can be joined on user_id and product_id.
"""
import boto3
import pandas
import random
import io
from datetime import datetime, timedelta
from faker import Faker
import config


fake = Faker()
random.seed(42)  # fixed seed: the same catalog and users every run


CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty"]

def build_product_catalog(num_products=config.NUM_PRODUCTS):
    """Creates a list of fake products with IDs, names, categories, and prices."""
    products = []
    for i in range(1, num_products + 1):
        products.append({
            "product_id": f"PROD-{i:04d}",
            "product_name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "base_price": round(random.uniform(5.0, 500.0), 2),
        })
    return products

PRODUCTS = build_product_catalog()

USER_IDS = [f"USR-{i:05d}" for i in range(1, config.NUM_USERS + 1)]

# Weighted toward completed, roughly like a real store.
ORDER_STATUSES = [
    "completed", "completed", "completed",
    "pending",
    "cancelled",
    "refunded"
]


def generate_single_order(order_id: int, order_timestamp: datetime) -> dict:
    """Creates one realistic order record."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    unit_price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)

    has_discount = random.random() > 0.7
    discount_pct = round(random.uniform(0.05, 0.25), 2) if has_discount else 0.0

    total_price = round(unit_price * quantity * (1 - discount_pct), 2)

    return {
        "order_id": f"ORD-{order_id:08d}",
        "user_id": random.choice(USER_IDS),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_pct": discount_pct,
        "total_price": total_price,
        "currency": "USD",
        "status": random.choice(ORDER_STATUSES),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
        "shipping_country": fake.country_code(),
        "order_timestamp": order_timestamp.isoformat(),
        "ingestion_timestamp": datetime.utcnow().isoformat(),
    }


def generate_and_upload_orders(run_date: datetime = None, num_orders: int = config.ORDERS_PER_RUN):
    """Generates a full batch of orders for a given date and uploads to S3."""
    if run_date is None:
        run_date = datetime.utcnow()

    print(f"\nGenerating {num_orders} orders for {run_date.strftime('%Y-%m-%d')}...")

    orders = []

    base_order_id = int(run_date.strftime("%Y%m%d")) * 10000

    for i in range(num_orders):
        seconds_offset = random.randint(0, 86399)
        order_time = run_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=seconds_offset)

        orders.append(generate_single_order(base_order_id + i, order_time))

    df = pandas.DataFrame(orders)

    date_str = run_date.strftime("%Y-%m-%d")
    s3_key = f"{config.BRONZE_ORDERS_PREFIX}/date={date_str}/orders_{date_str}.csv"

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client = boto3.client("s3", region_name=config.AWS_REGION)

    s3_client.put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )

    print(f"Uploaded → s3://{config.S3_BUCKET}/{s3_key}")
    print(f"   Orders generated: {num_orders}")
    print(f"   Sample order ID: {orders[0]['order_id']}")
    return df


def backfill_historical_orders(days_back: int = 7):
    """Generates orders for the past N days."""
    print(f"Starting backfill for the past {days_back} days...")

    for i in range(days_back, 0, -1):
        past_date = datetime.utcnow() - timedelta(days=i)
        generate_and_upload_orders(run_date=past_date)

    print(f"\nBackfill complete — {days_back} days of orders now in S3.")


if __name__ == "__main__":
    backfill_historical_orders(days_back=7)

    generate_and_upload_orders()

    print("\nDone.")
