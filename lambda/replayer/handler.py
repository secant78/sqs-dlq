"""
sqs-dlq-replayer
----------------
Invoked manually (or via a scheduled event / script) to replay messages from
the DLQ back to the main queue so they can be processed again.

Invocation payload (all fields optional):
{
    "max_messages": 100,   # cap on how many messages to move (default: 1000)
    "dry_run": false,      # if true, log what would happen but don't move anything
    "reset_attempts": true # if true, strip _replayed_at so processor sees it fresh
}

The function:
  1. Polls the DLQ in pages of 10.
  2. Re-sends each message to the main queue with replay metadata appended.
  3. Deletes the message from the DLQ on success.
  4. Returns a summary of replayed / errored counts.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")

MAIN_QUEUE_URL = os.environ["MAIN_QUEUE_URL"]
DLQ_URL = os.environ["DLQ_URL"]


# ── Entry point ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    max_messages: int = int(event.get("max_messages", 1000))
    dry_run: bool = bool(event.get("dry_run", False))
    reset_attempts: bool = bool(event.get("reset_attempts", False))

    logger.info(
        json.dumps(
            {
                "event": "replay_started",
                "max_messages": max_messages,
                "dry_run": dry_run,
                "reset_attempts": reset_attempts,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
    )

    replayed = 0
    errors = 0

    while replayed + errors < max_messages:
        remaining = max_messages - replayed - errors
        page = _poll_dlq(min(10, remaining))

        if not page:
            logger.info(json.dumps({"event": "dlq_empty", "replayed": replayed}))
            break

        for msg in page:
            success = _replay_one(msg, dry_run, reset_attempts)
            if success:
                replayed += 1
            else:
                errors += 1

    result = {
        "replayed": replayed,
        "errors": errors,
        "dry_run": dry_run,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(json.dumps({"event": "replay_complete", **result}))
    return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _poll_dlq(max_count: int) -> list[dict]:
    """Receive up to max_count messages from the DLQ (long-poll 2 s)."""
    response = sqs.receive_message(
        QueueUrl=DLQ_URL,
        MaxNumberOfMessages=max_count,
        WaitTimeSeconds=2,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    )
    return response.get("Messages", [])


def _replay_one(msg: dict, dry_run: bool, reset_attempts: bool) -> bool:
    """
    Re-enqueue a single DLQ message onto the main queue.

    Returns True on success, False on error.
    """
    message_id = msg["MessageId"]
    receipt = msg["ReceiptHandle"]

    try:
        # Enrich the body with replay provenance metadata
        try:
            body = json.loads(msg["Body"])
        except json.JSONDecodeError:
            body = {"raw": msg["Body"]}

        if isinstance(body, dict):
            if reset_attempts:
                # Remove previous replay timestamp so the processor treats it
                # as a brand-new message rather than a known bad actor.
                body.pop("_replayed_at", None)
                body.pop("_original_message_id", None)
            body["_replayed_at"] = datetime.now(timezone.utc).isoformat()
            body["_original_message_id"] = message_id

        send_body = json.dumps(body) if isinstance(body, dict) else msg["Body"]

        if not dry_run:
            # Send to main queue
            sqs.send_message(
                QueueUrl=MAIN_QUEUE_URL,
                MessageBody=send_body,
                MessageAttributes={
                    "ReplayedAt": {
                        "StringValue": datetime.now(timezone.utc).isoformat(),
                        "DataType": "String",
                    },
                    "OriginalMessageId": {
                        "StringValue": message_id,
                        "DataType": "String",
                    },
                },
            )

            # Delete from DLQ only after the send succeeded
            sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=receipt)

        logger.info(
            json.dumps(
                {
                    "event": "message_replayed",
                    "original_message_id": message_id,
                    "dry_run": dry_run,
                }
            )
        )
        return True

    except Exception as exc:
        logger.error(
            json.dumps(
                {
                    "event": "replay_error",
                    "message_id": message_id,
                    "error": str(exc),
                }
            )
        )
        return False
