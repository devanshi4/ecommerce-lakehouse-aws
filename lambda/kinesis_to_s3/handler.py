"""Lambda triggered by the Kinesis clickstream stream: write each batch of events to S3 bronze."""
import base64
import json
import os
from datetime import datetime

import boto3

s3_client = boto3.client("s3")
S3_BUCKET = os.environ["S3_BUCKET"]


def lambda_handler(event, context):
    print(f"Lambda triggered: {len(event['Records'])} records")

    # Kinesis delivers records in batches. Writing the whole batch as one file avoids
    # thousands of tiny objects, which slow down Spark and Athena.
    all_events = [
        json.loads(base64.b64decode(record["kinesis"]["data"]))
        for record in event["Records"]
    ]

    # Hive-style date/hour partitions, so Athena and Spark can skip irrelevant folders.
    now = datetime.utcnow()
    s3_key = (
        f"bronze/clickstream/"
        f"date={now.strftime('%Y-%m-%d')}/"
        f"hour={now.strftime('%H')}/"
        f"events_{now.strftime('%Y%m%d_%H%M%S')}.jsonl"
    )

    # JSON Lines: one event per line, which Spark can split and read in parallel.
    jsonl_content = "\n".join(json.dumps(evt) for evt in all_events)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=jsonl_content.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    print(f"Wrote {len(all_events)} events to s3://{S3_BUCKET}/{s3_key}")

    return {
        "statusCode": 200,
        "records_processed": len(all_events),
    }
