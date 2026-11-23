"""Shared settings for the data generators: AWS resources, S3 layout and data volumes."""
# AWS resources
S3_BUCKET = "ecommerce-data-platform-dp"

KINESIS_STREAM = "ecommerce-clickstream"

AWS_REGION = "us-east-1"


# Medallion layout in S3
BRONZE_ORDERS_PREFIX = "bronze/orders"
BRONZE_CLICKSTREAM_PREFIX = "bronze/clickstream"

SILVER_ORDERS_PREFIX = "silver/orders"
SILVER_CLICKSTREAM_PREFIX = "silver/clickstream"

GOLD_DAILY_REVENUE_PREFIX = "gold/daily_revenue"
GOLD_FUNNEL_PREFIX = "gold/funnel_conversion"


# Data generation
NUM_USERS = 500
NUM_PRODUCTS = 100
ORDERS_PER_RUN = 200
CLICKSTREAM_EVENTS_PER_SECOND = 5
