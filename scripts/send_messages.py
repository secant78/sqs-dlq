#!/usr/bin/env python3
"""
send_messages.py
----------------
Send N test messages to the SQS main queue (standard or FIFO).

Usage examples:
    # Send 100 messages to standard main-queue
    python send_messages.py

    # Send 50 messages
    python send_messages.py --count 50

    # Send to FIFO queue
    python send_messages.py --fifo

    # Send to a specific queue URL
    python send_messages.py --queue-url https://sqs.us-east-1.amazonaws.com/123456789/main-queue

    # Different region
    python send_messages.py --region eu-west-1
"""

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone

import boto3


def get_queue_url(sqs, queue_name: str) -> str:
    response = sqs.get_queue_url(QueueName=queue_name)
    return response["QueueUrl"]


def send_batch(
    sqs,
    queue_url: str,
    messages: list[dict],
    fifo: bool,
    group_count: int = 5,
) -> tuple[int, int]:
    """
    Send a batch of up to 10 messages. Returns (sent, failed).

    FIFO queues require MessageGroupId. We spread messages across
    group_count groups to allow parallel consumption within each group.
    """
    entries = []
    for i, msg in enumerate(messages):
        entry: dict = {
            "Id": str(i),
            "MessageBody": json.dumps(msg),
        }
        if fifo:
            # Assign to a group so ordering is preserved per group
            entry["MessageGroupId"] = f"group-{(msg['index'] % group_count) + 1}"
            # Unique dedup ID prevents FIFO from silently dropping duplicates
            # within the 5-minute deduplication window.
            entry["MessageDeduplicationId"] = f"msg-{msg['index']}-{uuid.uuid4()}"
        entries.append(entry)

    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    sent = len(response.get("Successful", []))
    failures = response.get("Failed", [])
    if failures:
        for f in failures:
            print(
                f"  [WARN] Message {f['Id']} failed: {f.get('Message', f.get('Code', '?'))}",
                file=sys.stderr,
            )
    return sent, len(failures)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send test messages to an SQS queue"
    )
    parser.add_argument(
        "--queue-url",
        help="Full SQS queue URL. Resolved from --queue-name if omitted.",
    )
    parser.add_argument(
        "--queue-name",
        default="main-queue",
        help="Queue name to resolve (default: main-queue)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of messages to send (default: 100)",
    )
    parser.add_argument(
        "--fifo",
        action="store_true",
        help="Target the FIFO queue (main-queue.fifo) instead of the standard one",
    )
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    sqs = boto3.client("sqs", region_name=args.region)

    if args.queue_url:
        queue_url = args.queue_url
    else:
        queue_name = "main-queue.fifo" if args.fifo else args.queue_name
        queue_url = get_queue_url(sqs, queue_name)

    print(f"Target queue : {queue_url}")
    print(f"Messages     : {args.count}")
    print(f"FIFO         : {args.fifo}")
    print()

    batch_id = str(uuid.uuid4())[:8]
    total_sent = 0
    total_failed = 0
    batch: list[dict] = []

    for i in range(1, args.count + 1):
        msg = {
            "index": i,
            "payload": f"Task payload #{i} — process and acknowledge",
            "batch_id": batch_id,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        batch.append(msg)

        # SQS batch limit is 10 messages
        if len(batch) == 10 or i == args.count:
            sent, failed = send_batch(sqs, queue_url, batch, args.fifo)
            total_sent += sent
            total_failed += failed
            print(
                f"  Batch [{i - len(batch) + 1:>3}–{i:<3}]  "
                f"sent={sent}  failed={failed}  "
                f"cumulative={total_sent}"
            )
            batch = []

    print()
    print(f"{'─' * 40}")
    print(f"Total sent  : {total_sent}")
    print(f"Total failed: {total_failed}")
    print()
    print("Now watch:")
    print("  • CloudWatch Logs  → /aws/lambda/sqs-message-processor")
    print("  • Monitor script   → python scripts/monitor_queues.py")
    print(
        f"  • Expected DLQ ~  {round(total_sent * 0.2 ** 3 * 100)}% "  # P(fail 3 times) ≈ 0.8%
        "(~0.8 % of messages if failure rate is 20 %)"
    )


if __name__ == "__main__":
    main()
