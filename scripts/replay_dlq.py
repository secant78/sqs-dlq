#!/usr/bin/env python3
"""
replay_dlq.py
-------------
Move messages from the DLQ back to the main queue so they can be retried.

Two modes:
  Lambda mode  (default) — invokes sqs-dlq-replayer synchronously.
               All replay logic runs in Lambda; you see the result JSON.

  Direct mode  (--direct) — replays using boto3 directly from this script.
               Useful when you want to test locally without re-deploying Lambda.

Usage:
    # Invoke the replayer Lambda (default)
    python scripts/replay_dlq.py

    # Dry-run via Lambda — show what would be replayed without moving anything
    python scripts/replay_dlq.py --dry-run

    # Direct boto3 replay of up to 50 messages
    python scripts/replay_dlq.py --direct --max 50

    # Reset replay metadata so processor treats messages as fresh
    python scripts/replay_dlq.py --reset-attempts

    # Replay from FIFO DLQ to FIFO main queue (direct mode only)
    python scripts/replay_dlq.py --direct --dlq-name failed-messages.fifo \\
        --main-queue-name main-queue.fifo
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
    lam = boto3.client("lambda", region_name=region)
    payload = {
        "max_messages": max_messages,
        "dry_run": dry_run,
        "reset_attempts": reset_attempts,
    }
    print(f"Invoking Lambda: {function_name}")
    print(f"Payload        : {json.dumps(payload)}")
    print()

    response = lam.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )

    if response.get("FunctionError"):
        print(f"[ERROR] Lambda returned an error:", file=sys.stderr)
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
        response = sqs.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=min(10, remaining),
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
                # Build enriched body
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
                    sqs.send_message(
                        QueueUrl=main_queue_url,
                        MessageBody=send_body,
                        MessageAttributes={
                            "ReplayedAt": {
                                "StringValue": now.isoformat(),
                                "DataType": "String",
                            },
                        },
                    )
                    sqs.delete_message(
                        QueueUrl=dlq_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )

                action = "[DRY-RUN]" if dry_run else "Replayed"
                print(f"  {action}  {mid}")
                replayed += 1

            except Exception as exc:
                print(f"  [ERROR]   {mid}  —  {exc}", file=sys.stderr)
                errors += 1

    result = {
        "replayed": replayed,
        "errors": errors,
        "dry_run": dry_run,
        "timestamp": now.isoformat(),
    }
    print()
    print(f"{'─' * 40}")
    print(f"Replayed : {replayed}")
    print(f"Errors   : {errors}")
    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

def get_queue_url(sqs, name: str) -> str:
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay DLQ messages to main queue")
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Use direct boto3 instead of the replayer Lambda",
    )
    parser.add_argument("--max", type=int, default=1000, help="Max messages to replay")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be replayed without moving anything",
    )
    parser.add_argument(
        "--reset-attempts",
        action="store_true",
        help="Strip replay metadata so the processor sees messages as fresh",
    )
    parser.add_argument("--dlq-name", default="failed-messages")
    parser.add_argument("--main-queue-name", default="main-queue")
    parser.add_argument("--lambda-name", default="sqs-dlq-replayer")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    if args.direct:
        sqs = boto3.client("sqs", region_name=args.region)
        dlq_url = get_queue_url(sqs, args.dlq_name)
        main_url = get_queue_url(sqs, args.main_queue_name)
        direct_replay(sqs, dlq_url, main_url, args.max, args.dry_run, args.reset_attempts)
    else:
        invoke_lambda_replayer(
            args.lambda_name,
            args.max,
            args.dry_run,
            args.reset_attempts,
            args.region,
        )


if __name__ == "__main__":
    main()
