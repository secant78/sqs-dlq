#!/usr/bin/env python3
"""
scripts/send_messages.py
========================

PURPOSE
-------
Sends N test messages to the SQS main-queue (standard or FIFO) to kick off
the DLQ processing demo. Each message contains a unique index, a sample
payload string, a shared batch ID, and a UTC timestamp.

HOW IT WORKS
------------
1. Resolves the queue URL from the queue name (or uses --queue-url directly).
2. Builds message dicts with an index (1..N), payload, batch_id, and sent_at.
3. Calls SendMessageBatch in groups of 10 (the SQS API maximum per batch call).
4. Prints per-batch progress and a final summary.

AFTER SENDING
-------------
The sqs-message-processor Lambda is triggered automatically by the SQS event
source mapping — you do NOT need to do anything else to start processing.
Run scripts/monitor_queues.py in a second terminal to watch queue depths live.

EXPECTED OUTCOME (20 % failure rate, 100 messages)
----------------------------------------------------
- ~99.2 % of messages are processed successfully on the first or second attempt.
- ~0.8 % fail all 3 attempts and land in the DLQ (≈ 0–2 messages from 100).
- The dlq-monitor Lambda fires for every DLQ batch and sends an SNS alert.

FIFO MODE
---------
Pass --fifo to target main-queue.fifo instead. FIFO requires:
  MessageGroupId: determines ordering partition (5 groups used here)
  MessageDeduplicationId: prevents exact duplicate sends within 5 minutes
                          (even though content_based_deduplication is enabled,
                          we set an explicit ID to guarantee uniqueness)

USAGE
-----
    python scripts/send_messages.py                    # 100 messages, standard
    python scripts/send_messages.py --count 50         # 50 messages
    python scripts/send_messages.py --fifo             # FIFO queue
    python scripts/send_messages.py --region eu-west-1 # different region
"""

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone

import boto3


def get_queue_url(sqs, queue_name: str) -> str:
    """Resolve a queue name to its full HTTPS URL via the SQS API."""
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
    Send a batch of up to 10 messages to SQS and return (sent, failed) counts.

    Parameters
    ----------
    sqs         : boto3 SQS client
    queue_url   : str   Full queue URL
    messages    : list  Message dicts to send (max 10 per SQS batch limit)
    fifo        : bool  True if targeting a FIFO queue
    group_count : int   Number of MessageGroupId partitions for FIFO queues.
                        Messages are distributed across groups by index modulo
                        group_count, creating ordered streams in parallel.

    Returns
    -------
    tuple[int, int]  (number sent successfully, number failed)
    """
    entries = []
    for i, msg in enumerate(messages):
        # The "Id" field is a batch-local identifier used to correlate responses
        # with requests. It must be unique within the batch but is not stored
        # by SQS — it is only used in the response to match success/failure.
        entry: dict = {
            "Id": str(i),
            "MessageBody": json.dumps(msg),
        }

        if fifo:
            # MessageGroupId partitions the FIFO stream. Messages within the
            # same group are delivered in strict FIFO order. Using multiple
            # groups allows parallel Lambda invocations (one per group).
            entry["MessageGroupId"] = f"group-{(msg['index'] % group_count) + 1}"

            # A unique MessageDeduplicationId prevents SQS from treating two
            # messages with identical bodies as duplicates within the 5-minute
            # deduplication window. Without this, sending the same 100 messages
            # twice would only result in 100 unique deliveries, not 200.
            entry["MessageDeduplicationId"] = f"msg-{msg['index']}-{uuid.uuid4()}"

        entries.append(entry)

    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)

    sent = len(response.get("Successful", []))
    failures = response.get("Failed", [])

    # Log individual failures to stderr so they don't clutter stdout progress.
    for f in failures:
        print(
            f"  [WARN] Message Id={f['Id']} failed: "
            f"{f.get('Message', f.get('Code', 'unknown error'))}",
            file=sys.stderr,
        )

    return sent, len(failures)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send test messages to an SQS queue to demo DLQ processing"
    )
    parser.add_argument(
        "--queue-url",
        help="Full SQS queue URL. If omitted, --queue-name is resolved via API.",
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
        help="Target the FIFO queue (main-queue.fifo) with MessageGroupId and deduplication",
    )
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    sqs = boto3.client("sqs", region_name=args.region)

    # Resolve the queue URL — either from the CLI arg or by name lookup.
    if args.queue_url:
        queue_url = args.queue_url
    else:
        queue_name = "main-queue.fifo" if args.fifo else args.queue_name
        queue_url = get_queue_url(sqs, queue_name)

    print(f"Target queue : {queue_url}")
    print(f"Messages     : {args.count}")
    print(f"FIFO         : {args.fifo}")
    print()

    # batch_id ties all messages in this run together. Useful for filtering
    # CloudWatch Logs to a specific send_messages.py invocation.
    batch_id = str(uuid.uuid4())[:8]
    total_sent = 0
    total_failed = 0
    batch: list[dict] = []

    for i in range(1, args.count + 1):
        msg = {
            "index": i,                           # 1-based position in this run
            "payload": f"Task payload #{i} — process and acknowledge",
            "batch_id": batch_id,                 # ties all messages to this run
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        batch.append(msg)

        # Flush when batch is full (10 messages) or when we're on the last message.
        if len(batch) == 10 or i == args.count:
            sent, failed = send_batch(sqs, queue_url, batch, args.fifo)
            total_sent += sent
            total_failed += failed
            print(
                f"  Batch [{i - len(batch) + 1:>3}–{i:<3}]  "
                f"sent={sent}  failed={failed}  "
                f"cumulative={total_sent}"
            )
            batch = []  # reset for the next batch

    # Final summary
    print()
    print(f"{'─' * 42}")
    print(f"Total sent  : {total_sent}")
    print(f"Total failed: {total_failed}")
    print()
    print("Next steps:")
    print("  Watch queue depths : python scripts/monitor_queues.py")
    print("  Watch processor log: make logs-processor")
    # P(fail all 3 attempts) = failure_rate^3 = 0.2^3 = 0.008 = 0.8 %
    # Expected DLQ messages from 100 sends = 100 × 0.008 ≈ 0–2 messages
    print(
        f"  Expected DLQ msgs  : ~{round(total_sent * 0.008)} "
        "(≈0.8 % at 20 % failure rate)"
    )


if __name__ == "__main__":
    main()
