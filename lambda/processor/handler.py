"""
sqs-message-processor
---------------------
Triggered by the main-queue event source mapping (batch_size=1).

Behaviour:
  - Parses each SQS record.
  - Randomly fails FAILURE_RATE (default 20 %) of messages by raising an
    exception — SQS increments ApproximateReceiveCount and retries.
  - After 3 failed attempts the redrive policy moves the message to the DLQ.
  - Uses ReportBatchItemFailures so only the failing message is retried,
    not the entire batch.
  - Logs every attempt as structured JSON for easy CloudWatch Insights queries.
"""

import json
import logging
import os
import random
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.2"))


# ── Entry point ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    batch_item_failures: list[dict] = []

    for record in event["Records"]:
        message_id = record["messageId"]
        receive_count = int(
            record["attributes"].get("ApproximateReceiveCount", "1")
        )
        body_raw = record["body"]

        logger.info(
            json.dumps(
                {
                    "event": "processing_attempt",
                    "message_id": message_id,
                    "receive_count": receive_count,
                    "queue": record["eventSourceARN"].split(":")[-1],
                    "body_preview": body_raw[:120],
                }
            )
        )

        try:
            _process(message_id, body_raw, receive_count)

            logger.info(
                json.dumps(
                    {
                        "event": "processing_success",
                        "message_id": message_id,
                        "receive_count": receive_count,
                    }
                )
            )

        except Exception as exc:
            will_go_to_dlq = receive_count >= 3
            logger.error(
                json.dumps(
                    {
                        "event": "processing_failure",
                        "message_id": message_id,
                        "receive_count": receive_count,
                        "error": str(exc),
                        "will_go_to_dlq": will_go_to_dlq,
                    }
                )
            )
            # Return this message ID in batchItemFailures so SQS knows to
            # retry it (or send it to the DLQ on the 3rd failure).
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}


# ── Processing logic ──────────────────────────────────────────────────────────

def _process(message_id: str, body_raw: str, receive_count: int) -> None:
    """Parse the message and simulate work with a random failure injection."""

    try:
        data = json.loads(body_raw)
    except json.JSONDecodeError:
        data = {"raw": body_raw}

    # Simulate variable processing time (50–150 ms)
    time.sleep(random.uniform(0.05, 0.15))

    # 20 % random failure — same probability on every attempt so some messages
    # will fail all 3 times and legitimately end up in the DLQ.
    if random.random() < FAILURE_RATE:
        raise RuntimeError(
            f"Simulated random failure processing message {message_id} "
            f"(attempt {receive_count})"
        )

    logger.info(
        json.dumps(
            {
                "event": "message_processed",
                "message_id": message_id,
                "index": data.get("index"),
                "payload": str(data.get("payload", ""))[:80],
                "replayed": "_replayed_at" in data,
            }
        )
    )
