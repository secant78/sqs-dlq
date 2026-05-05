#!/usr/bin/env python3
"""
scripts/replay_dlq.py
=====================

PURPOSE
-------
Moves messages from the failed-messages DLQ back to the main-queue so they can
be retried by the sqs-message-processor Lambda. This is the "replay mechanism"
required by the assignment.

TWO MODES
---------
Lambda mode (default):
    Invokes the sqs-dlq-replayer Lambda synchronously via the Lambda API.
    The replay logic runs in Lambda, and this script just passes parameters
    and prints the result. Best for normal day-to-day use — it's consistent
    with production behaviour and the output is also captured in CloudWatch Logs.

    Usage:
        python scripts/replay_dlq.py                     # replay all DLQ messages
        python scripts/replay_dlq.py --dry-run           # preview without moving
        python scripts/replay_dlq.py --max 50            # cap at 50 messages
        python scripts/replay_dlq.py --reset-attempts    # strip replay metadata

Direct mode (--direct):
    Replays using boto3 directly from this script, without invoking Lambda.
    Useful when the Lambda is not deployed yet or during local development.
    The output is only printed to this terminal — it does NOT appear in
    CloudWatch Logs.

    Usage:
        python scripts/replay_dlq.py --direct
        python scripts/replay_dlq.py --direct --max 10 --dry-run
        python scripts/replay_dlq.py --direct \\
            --dlq-name failed-messages.fifo \\
            --main-queue-name main-queue.fifo

REPLAY METADATA
---------------
Every replayed message has two fields added to its JSON body:
    _replayed_at        : ISO 8601 UTC timestamp of this replay
    _original_message_id: SQS message ID from the DLQ

The processor Lambda logs "replayed: true" when it sees _replayed_at, making
it easy to distinguish first-time processing from post-DLQ retries in CloudWatch.

DRY-RUN SAFETY
--------------
--dry-run logs what would be replayed without making any SQS API calls.
Always run a dry-run first on large backlogs to confirm the message count and
content before moving them.

AFTER REPLAYING
---------------
Run monitor_queues.py to watch the replayed messages being processed:
    python scripts/monitor_queues.py

If replayed messages fail again (and land back in the DLQ), the dlq_monitor
Lambda fires with replayed_count > 0 in its analysis — a signal that the
root cause has not been fixed.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3


# ── Lambda-mode replay ────────────────────────────────────────────────────────

def invoke_lambda_replayer(
    function_name: str,
    max_messages: int,
    dry_run: bool,
    reset_attempts: bool,
    region: str,
) -> dict:
    """
    Invoke the sqs-dlq-replayer Lambda synchronously and return its result.

    InvocationType="RequestResponse" waits for the Lambda to finish before
    returning. With timeout=300s on the Lambda, this script blocks for up to
    5 minutes while a large backlog drains.

    Parameters
    ----------
    function_name   : str   Lambda function name (not ARN).
    max_messages    : int   Cap on messages to replay.
    dry_run         : bool  If True, Lambda logs but does not move messages.
    reset_attempts  : bool  If True, Lambda strips _replayed_at from bodies.
    region          : str   AWS region.

    Returns
    -------
    dict  The Lambda return value: {replayed, errors, dry_run, timestamp}.
    """
    lam = boto3.client("lambda", region_name=region)

    payload = {
        "max_messages": max_messages,
        "dry_run": dry_run,
        "reset_attempts": reset_attempts,
    }

    print(f"Invoking Lambda : {function_name}")
    print(f"Payload         : {json.dumps(payload)}")
    print()

    response = lam.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",   # synchronous — wait for result
        Payload=json.dumps(payload),
    )

    # FunctionError is set if the Lambda threw an unhandled exception.
    # This is distinct from a boto3 / network error, which raises an exception.
    if response.get("FunctionError"):
        print("[ERROR] Lambda returned a function error:", file=sys.stderr)
        error_body = json.loads(response["Payload"].read())
        print(json.dumps(error_body, indent=2), file=sys.stderr)
        sys.exit(1)

    result = json.loads(response["Payload"].read())
    print(json.dumps(result, indent=2))
    return result


# ── Direct boto3 replay ───────────────────────────────────────────────────────

def direct_replay(
    sqs,
    dlq_url: str,
    main_queue_url: str,
    max_messages: int,
    dry_run: bool,
    reset_attempts: bool,
) -> dict:
    """
    Replay messages from the DLQ to the main-queue directly via boto3.

    This mirrors the logic inside lambda/replayer/handler.py but runs locally
    without requiring a Lambda deployment. Output goes to stdout only — it is
    NOT captured in CloudWatch Logs.

    The same send-then-delete pattern is used: a message is only deleted from
    the DLQ after it is successfully sent to the main-queue, guaranteeing
    at-least-once delivery even if the script is interrupted mid-run.

    Parameters
    ----------
    sqs             : boto3 SQS client
    dlq_url         : str   URL of the DLQ to drain.
    main_queue_url  : str   URL of the destination queue.
    max_messages    : int   Maximum messages to move.
    dry_run         : bool  If True, only log; don't call SQS.
    reset_attempts  : bool  If True, strip _replayed_at from bodies.

    Returns
    -------
    dict  {replayed, errors, dry_run, timestamp}
    """
    replayed = 0
    errors = 0
    now = datetime.now(timezone.utc)

    print(f"DLQ            : {dlq_url}")
    print(f"Main queue     : {main_queue_url}")
    print(f"Max messages   : {max_messages}")
    print(f"Dry run        : {dry_run}")
    print(f"Reset attempts : {reset_attempts}")
    print()

    while replayed + errors < max_messages:
        remaining = max_messages - replayed - errors

        # Long-poll for up to 2 seconds. Returns empty list when DLQ is drained.
        response = sqs.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=min(10, remaining),  # SQS batch limit = 10
            WaitTimeSeconds=2,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )

        messages = response.get("Messages", [])
        if not messages:
            print("DLQ is empty — nothing more to replay.")
            break

        for msg in messages:
            mid = msg["MessageId"]
            try:
                # Parse and enrich the body with replay metadata.
                try:
                    body = json.loads(msg["Body"])
                except json.JSONDecodeError:
                    body = {"raw": msg["Body"]}

                if isinstance(body, dict):
                    if reset_attempts:
                        body.pop("_replayed_at", None)
                        body.pop("_original_message_id", None)
                    body["_replayed_at"] = now.isoformat()
                    body["_original_message_id"] = mid

                send_body = json.dumps(body) if isinstance(body, dict) else msg["Body"]

                if not dry_run:
                    # Send first, then delete — never delete before confirming send.
                    sqs.send_message(QueueUrl=main_queue_url, MessageBody=send_body)
                    sqs.delete_message(
                        QueueUrl=dlq_url, ReceiptHandle=msg["ReceiptHandle"]
                    )

                action = "[DRY-RUN] Would replay" if dry_run else "Replayed    "
                print(f"  {action}  {mid}")
                replayed += 1

            except Exception as exc:
                print(f"  [ERROR]              {mid}  —  {exc}", file=sys.stderr)
                errors += 1

    result = {
        "replayed": replayed,
        "errors": errors,
        "dry_run": dry_run,
        "timestamp": now.isoformat(),
    }

    print()
    print(f"{'─' * 42}")
    print(f"Replayed : {replayed}")
    print(f"Errors   : {errors}")
    return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_queue_url(sqs, name: str) -> str:
    """Resolve a queue name to its full URL via the SQS API."""
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay DLQ messages back to the main queue for reprocessing"
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Use direct boto3 instead of invoking the replayer Lambda",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=1000,
        help="Maximum number of messages to replay (default: 1000 = drain all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be replayed without making any SQS changes",
    )
    parser.add_argument(
        "--reset-attempts",
        action="store_true",
        help="Strip _replayed_at metadata so the processor logs show 'replayed: false'",
    )
    parser.add_argument(
        "--dlq-name",
        default="failed-messages",
        help="Name of the DLQ to drain (default: failed-messages)",
    )
    parser.add_argument(
        "--main-queue-name",
        default="main-queue",
        help="Name of the destination queue (default: main-queue)",
    )
    parser.add_argument(
        "--lambda-name",
        default="sqs-dlq-replayer",
        help="Lambda function name to invoke in Lambda mode (default: sqs-dlq-replayer)",
    )
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    if args.direct:
        # Direct boto3 mode — no Lambda invocation.
        sqs = boto3.client("sqs", region_name=args.region)
        dlq_url = get_queue_url(sqs, args.dlq_name)
        main_url = get_queue_url(sqs, args.main_queue_name)
        direct_replay(sqs, dlq_url, main_url, args.max, args.dry_run, args.reset_attempts)
    else:
        # Lambda mode — invoke sqs-dlq-replayer and print its JSON result.
        invoke_lambda_replayer(
            args.lambda_name,
            args.max,
            args.dry_run,
            args.reset_attempts,
            args.region,
        )


if __name__ == "__main__":
    main()
