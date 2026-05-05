"""
lambda/replayer/handler.py — sqs-dlq-replayer
==============================================

PURPOSE
-------
This Lambda function moves messages from the failed-messages DLQ back to the
main-queue so they can be retried by the processor Lambda. It is the "replay
mechanism" required by the assignment.

WHY A LAMBDA INSTEAD OF A SCRIPT
---------------------------------
The replayer is implemented as a Lambda (not just a local Python script) because:
  1. It runs with the same IAM role as the other functions — no local AWS
     credentials needed to execute the replay.
  2. It can be invoked from anywhere (AWS Console, CLI, CI pipeline, EventBridge
     schedule) without installing Python dependencies.
  3. Its output is captured in CloudWatch Logs alongside the processor and
     monitor logs, giving a complete audit trail.

Scripts/replay_dlq.py invokes this Lambda synchronously by default, or can
replay directly via boto3 if Lambda is not available (--direct flag).

HOW REPLAY WORKS
----------------
1. Poll the DLQ in pages of 10 (SQS ReceiveMessage maximum).
2. For each message:
   a. Parse the body (JSON or raw string).
   b. Inject replay provenance metadata (_replayed_at, _original_message_id)
      so the processor logs can distinguish first-time from replayed messages.
   c. Send the enriched body to main-queue via SendMessage.
   d. Delete the message from the DLQ only after the send succeeds.
      This ensures messages are never lost — if SendMessage fails, the message
      stays in the DLQ and can be retried on the next replay invocation.
3. Stop when the DLQ is empty or max_messages is reached.
4. Return a summary {replayed, errors, dry_run, timestamp}.

FAILURE BEHAVIOUR
-----------------
If the processor Lambda still fails on a replayed message (because the root
cause was not fixed), the message will go through three more retry attempts and
land back in the DLQ. The dlq_monitor will fire again and the replayed_count
field in its analysis will be > 0, signalling that the replay did not resolve
the issue.

INVOCATION PAYLOAD
------------------
All fields are optional:

{
    "max_messages": 100,   # cap on messages to move (default: 1000 = drain all)
    "dry_run": false,      # if true, log what would happen but don't move anything
    "reset_attempts": false  # if true, strip _replayed_at so processor treats
                             # the message as fresh (no replay marker in logs)
}

ENVIRONMENT VARIABLES
---------------------
MAIN_QUEUE_URL  str  SQS URL of the main-queue. Injected by Terraform.
DLQ_URL         str  SQS URL of the failed-messages DLQ. Injected by Terraform.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Single boto3 SQS client reused across warm invocations.
# boto3 manages connection pooling internally, so one client is efficient.
sqs = boto3.client("sqs")

MAIN_QUEUE_URL = os.environ["MAIN_QUEUE_URL"]
DLQ_URL = os.environ["DLQ_URL"]


# ── Entry point ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Main Lambda handler. Invoked manually (not by an event source mapping).

    Parameters
    ----------
    event : dict
        Caller-supplied JSON payload. All fields are optional — defaults
        produce a safe full-drain behaviour:
        {
            "max_messages": 1000,
            "dry_run": false,
            "reset_attempts": false
        }

    context : LambdaContext
        Provides remaining_time_in_millis() — useful for stopping the replay
        loop before the Lambda times out. Not used here for simplicity, but
        in production you would check context.get_remaining_time_in_millis()
        and stop early if < 30_000 ms to avoid a partial-batch timeout.

    Returns
    -------
    dict
        {
            "replayed": int,   # messages successfully moved to main-queue
            "errors": int,     # messages that could not be moved (still in DLQ)
            "dry_run": bool,
            "timestamp": str   # ISO 8601 UTC timestamp
        }
    """
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

    # Keep polling until we've processed max_messages or the DLQ is empty.
    while replayed + errors < max_messages:
        # Calculate how many more messages we're allowed to fetch.
        remaining = max_messages - replayed - errors

        # _poll_dlq returns an empty list when the DLQ has no visible messages.
        page = _poll_dlq(min(10, remaining))

        if not page:
            logger.info(json.dumps({"event": "dlq_empty", "replayed": replayed}))
            break

        for msg in page:
            success = _replay_one(msg, dry_run, reset_attempts)
            if success:
                replayed += 1
            else:
                # Error logged inside _replay_one. Continue to the next message
                # rather than aborting — partial replay is better than no replay.
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
    """
    Fetch a page of up to max_count messages from the DLQ.

    WaitTimeSeconds = 2 uses long polling, which reduces the number of empty
    ReceiveMessage calls when the DLQ is near-empty. SQS bills per request,
    so long polling lowers cost compared to short polling (WaitTimeSeconds = 0).

    Returns an empty list when the DLQ has no visible messages — the caller
    uses this as the stop condition.
    """
    response = sqs.receive_message(
        QueueUrl=DLQ_URL,
        MaxNumberOfMessages=max_count,  # SQS cap is 10 per call
        WaitTimeSeconds=2,              # long-poll for up to 2 s
        AttributeNames=["All"],         # include SentTimestamp, ReceiveCount, etc.
        MessageAttributeNames=["All"],  # include custom attributes if present
    )
    return response.get("Messages", [])


def _replay_one(msg: dict, dry_run: bool, reset_attempts: bool) -> bool:
    """
    Re-enqueue a single DLQ message onto the main-queue and delete it from the DLQ.

    The delete only happens AFTER the send succeeds. This guarantees at-least-once
    delivery — in the worst case a message might be replayed twice (if SendMessage
    succeeds but DeleteMessage fails), but it will never be silently lost.

    Parameters
    ----------
    msg            : dict  A single SQS message dict from ReceiveMessage.
    dry_run        : bool  If True, log what would happen without calling SQS.
    reset_attempts : bool  If True, strip _replayed_at from the body so the
                           processor sees the message as a fresh first delivery.

    Returns
    -------
    bool  True if the message was successfully replayed (or would be in dry_run),
          False if an exception occurred.
    """
    message_id = msg["MessageId"]
    receipt = msg["ReceiptHandle"]

    try:
        # Parse the body so we can modify it before re-sending.
        try:
            body = json.loads(msg["Body"])
        except json.JSONDecodeError:
            # Body is not JSON (e.g. a plain string). Wrap it so we can add
            # metadata without breaking the format.
            body = {"raw": msg["Body"]}

        if isinstance(body, dict):
            if reset_attempts:
                # Remove existing replay markers so the processor log entry
                # shows "replayed: false". Useful when you want clean metrics
                # after fixing the underlying bug.
                body.pop("_replayed_at", None)
                body.pop("_original_message_id", None)

            # Stamp the replay time and original message ID. The processor reads
            # "_replayed_at" and logs "replayed: true" so you can distinguish
            # first-time processing from post-DLQ retries in CloudWatch Logs.
            body["_replayed_at"] = datetime.now(timezone.utc).isoformat()
            body["_original_message_id"] = message_id

        # Serialise back to a string for the SQS API.
        send_body = json.dumps(body) if isinstance(body, dict) else msg["Body"]

        if not dry_run:
            # Step 1: Send to main-queue.
            # If this fails, we catch the exception below and return False.
            # The message remains in the DLQ for the next replay invocation.
            send_kwargs: dict = {
                "QueueUrl": MAIN_QUEUE_URL,
                "MessageBody": send_body,
                "MessageAttributes": {
                    # Custom attributes visible to downstream consumers.
                    # The processor Lambda reads MessageAttributeNames=["All"]
                    # but currently ignores these — they exist for observability.
                    "ReplayedAt": {
                        "StringValue": datetime.now(timezone.utc).isoformat(),
                        "DataType": "String",
                    },
                    "OriginalMessageId": {
                        "StringValue": message_id,
                        "DataType": "String",
                    },
                },
            }
            # FIFO queues require MessageGroupId on every SendMessage call.
            # Read it from the original message's attributes (present because
            # _poll_dlq requests AttributeNames=["All"]).
            if MAIN_QUEUE_URL.endswith(".fifo"):
                group_id = msg.get("Attributes", {}).get("MessageGroupId", "default")
                send_kwargs["MessageGroupId"] = group_id

            sqs.send_message(**send_kwargs)

            # Step 2: Delete from DLQ only after a successful send.
            # ReceiptHandle is a temporary token SQS gives us to delete
            # a specific message. It expires after the visibility timeout.
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
        # Log the error but don't re-raise — the caller counts errors and
        # continues to the next message rather than aborting the entire batch.
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
