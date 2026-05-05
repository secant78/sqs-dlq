"""
lambda/processor/handler.py — sqs-message-processor
=====================================================

PURPOSE
-------
This is the primary consumer of the main-queue. AWS Lambda invokes this
function automatically whenever messages appear in the queue (via the event
source mapping defined in terraform/main.tf).

PROCESSING FLOW
---------------
1. Lambda receives an event containing one SQS record (batch_size = 1).
2. The handler logs the attempt with the current ApproximateReceiveCount so
   we can see in CloudWatch exactly which attempt number this is.
3. _process() simulates real work (a short sleep) then randomly raises an
   exception 20 % of the time via the FAILURE_RATE environment variable.
4a. SUCCESS → the message is automatically deleted from the queue by SQS
    because the Lambda returned without errors and the message ID was not
    included in batchItemFailures.
4b. FAILURE → the message ID is added to batchItemFailures. SQS puts the
    message back in the queue after the visibility timeout expires and
    increments ApproximateReceiveCount. After 3 failed attempts the redrive
    policy routes it to the failed-messages DLQ.

RETRY MATH
----------
With a 20 % failure rate, the probability of failing all 3 attempts is:
    P(fail) = 0.2 × 0.2 × 0.2 = 0.008  (≈ 0.8 %)

So roughly 1 in 125 messages ends up in the DLQ when sending 100 messages.

BATCHITEMFAILURES
-----------------
Returning {"batchItemFailures": [{"itemIdentifier": message_id}]} tells SQS
which specific messages failed. SQS only retries those messages — it does NOT
penalise other messages in the same batch. With batch_size = 1 this makes no
practical difference, but the pattern is included as best practice for when
batch sizes are increased in real workloads.

ENVIRONMENT VARIABLES
---------------------
FAILURE_RATE  float (0.0–1.0)  Injected failure probability. Set in
              terraform/main.tf under the Lambda resource's environment block.
              Defaults to 0.2 (20 %) if the variable is missing.
"""

import json
import logging
import os
import random
import time

# Lambda creates a logger automatically. Setting the level to INFO means all
# logger.info() calls appear in CloudWatch Logs; DEBUG calls are suppressed.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Read the failure rate from the environment once at module load time.
# Module-level code runs during the Lambda cold start and is reused across
# warm invocations, so this is more efficient than reading os.environ per call.
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.2"))


# ── Entry point ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Main Lambda handler. Called once per SQS polling cycle.

    Parameters
    ----------
    event : dict
        SQS event payload. Structure:
        {
            "Records": [
                {
                    "messageId": "...",
                    "receiptHandle": "...",
                    "body": "...",           # the actual message content
                    "attributes": {
                        "ApproximateReceiveCount": "1",   # 1 on first attempt
                        "SentTimestamp": "...",
                        ...
                    },
                    "eventSourceARN": "arn:aws:sqs:...:main-queue"
                }
            ]
        }

    context : LambdaContext
        Runtime metadata (function name, remaining time, etc.). Not used here.

    Returns
    -------
    dict
        {"batchItemFailures": [...]} — any message IDs that should be retried.
        An empty list (or omitting the key entirely) signals full success.
    """
    batch_item_failures: list[dict] = []

    for record in event["Records"]:
        message_id = record["messageId"]

        # ApproximateReceiveCount starts at "1" on the first delivery attempt.
        # SQS increments it each time the message becomes visible again after
        # a failed attempt. At count = 3 with maxReceiveCount = 3, the next
        # failure routes the message to the DLQ.
        receive_count = int(
            record["attributes"].get("ApproximateReceiveCount", "1")
        )
        body_raw = record["body"]

        # Log the attempt BEFORE processing so the log entry exists even if
        # the function crashes partway through _process().
        logger.info(
            json.dumps(
                {
                    "event": "processing_attempt",
                    "message_id": message_id,
                    "receive_count": receive_count,
                    # Extract just the queue name from the full ARN for readability.
                    "queue": record["eventSourceARN"].split(":")[-1],
                    # Truncate to 120 chars to keep logs scannable.
                    "body_preview": body_raw[:120],
                }
            )
        )

        try:
            _process(message_id, body_raw, receive_count)

            # If _process() returned without raising, the message was handled
            # successfully. SQS will delete it automatically.
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
            # will_go_to_dlq is true when this is the 3rd attempt and the
            # redrive policy's maxReceiveCount is 3. It's informational only —
            # SQS makes the routing decision, not the Lambda.
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

            # Report this message as failed so SQS retries it (or sends it to
            # the DLQ if maxReceiveCount is exhausted).
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}


# ── Processing logic ──────────────────────────────────────────────────────────

def _process(message_id: str, body_raw: str, receive_count: int) -> None:
    """
    Simulate message processing with an injected random failure.

    In a real application this function would do meaningful work:
    write to a database, call a downstream API, trigger a workflow, etc.
    Here it just sleeps briefly and then randomly raises to demonstrate how
    the retry and DLQ machinery behaves.

    Parameters
    ----------
    message_id   : str   SQS message ID (used in log messages).
    body_raw     : str   Raw message body string (JSON or plain text).
    receive_count: int   How many times SQS has delivered this message.

    Raises
    ------
    RuntimeError  20 % of the time (controlled by FAILURE_RATE).
    """

    # Parse the body — messages from send_messages.py are JSON objects, but
    # we handle plain strings too (e.g. messages sent manually from the Console).
    try:
        data = json.loads(body_raw)
    except json.JSONDecodeError:
        data = {"raw": body_raw}

    # Simulate variable processing time (50–150 ms).
    # In production this would be I/O time (DB write, HTTP call, etc.).
    time.sleep(random.uniform(0.05, 0.15))

    # ── Failure injection ─────────────────────────────────────────────────────
    # random.random() returns a float in [0.0, 1.0). If it falls below
    # FAILURE_RATE the message is considered "failed" for this attempt.
    # The same probability applies on every attempt — there is no exponential
    # back-off or "give up after N retries" logic here; that is handled
    # entirely by SQS via the redrive policy.
    if random.random() < FAILURE_RATE:
        raise RuntimeError(
            f"Simulated random failure processing message {message_id} "
            f"(attempt {receive_count})"
        )

    # If we reach here the message was processed successfully.
    logger.info(
        json.dumps(
            {
                "event": "message_processed",
                "message_id": message_id,
                "index": data.get("index"),                     # position in the 100-message batch
                "payload": str(data.get("payload", ""))[:80],  # truncated for log readability
                # True if this message was previously in the DLQ and was replayed.
                "replayed": "_replayed_at" in data,
            }
        )
    )
