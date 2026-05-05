#!/usr/bin/env python3
"""
scripts/monitor_queues.py
=========================

PURPOSE
-------
Polls all four SQS queues every N seconds and prints a formatted depth table,
giving a live view of how messages flow through the main-queue → DLQ pipeline.

WHAT YOU'RE WATCHING
--------------------
Visible     — messages sitting in the queue waiting to be picked up by Lambda.
              This number drops as the processor Lambda consumes messages.

In-flight   — messages currently held by a Lambda invocation (hidden from other
              consumers until the visibility timeout expires or the Lambda deletes
              them). A sustained high in-flight count means Lambda is busy or stuck.

WHAT NORMAL LOOKS LIKE
-----------------------
Just after sending 100 messages:
  Main Queue     visible=100   in-flight=0

While Lambda processes them:
  Main Queue     visible=73    in-flight=10

After main-queue drains (all processed or DLQ'd):
  Main Queue     visible=0     in-flight=0
  DLQ            visible=1     in-flight=0   ← ⚠  replay needed

STOP CONDITIONS
---------------
The script exits automatically when both main queues (standard and FIFO) have
zero visible and zero in-flight messages. It then reports the DLQ depth and
suggests next steps. Press Ctrl-C to stop early at any time.

USAGE
-----
    python scripts/monitor_queues.py                # poll every 10 s
    python scripts/monitor_queues.py --interval 5   # poll every 5 s
    python scripts/monitor_queues.py --once         # single snapshot, then exit
    python scripts/monitor_queues.py --timeout 120  # stop after 2 minutes
    make monitor                                    # shortcut
"""

import argparse
import sys
import time
from datetime import datetime, timezone

import boto3


# Queue names to monitor and their display labels.
# Ordered: standard main → standard DLQ → FIFO main → FIFO DLQ.
QUEUES = {
    "main-queue":         "Main Queue",
    "failed-messages":    "DLQ (failed-messages)",
    "main-queue.fifo":    "Main FIFO",
    "failed-messages.fifo": "DLQ FIFO",
}


def get_depth(sqs, queue_url: str) -> tuple[int, int]:
    """
    Fetch the visible and in-flight message counts for a single queue.

    ApproximateNumberOfMessages        — visible (ready to be consumed)
    ApproximateNumberOfMessagesNotVisible — in-flight (being processed)

    Both are approximate — SQS does not guarantee exact counts, especially
    for large queues. For a demo with 100 messages the numbers are reliable.

    Returns
    -------
    tuple[int, int]  (visible, in_flight)
    """
    resp = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )
    attrs = resp["Attributes"]
    visible = int(attrs.get("ApproximateNumberOfMessages", 0))
    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    return visible, in_flight


def resolve_urls(sqs) -> dict[str, str]:
    """
    Attempt to resolve each queue name to its URL.

    Queues that don't exist yet (e.g. FIFO queues if --fifo was never used)
    are silently skipped rather than crashing the script.

    Returns
    -------
    dict[str, str]  {queue_name: queue_url} for queues that exist.
    """
    urls: dict[str, str] = {}
    for name in QUEUES:
        try:
            urls[name] = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        except sqs.exceptions.QueueDoesNotExist:
            # Queue not deployed yet — skip it silently.
            pass
    return urls


def print_snapshot(sqs, urls: dict[str, str]) -> dict[str, tuple[int, int]]:
    """
    Print a formatted depth table for all known queues and return the raw depth data.

    The ⚠ marker appears next to any DLQ with visible messages, drawing
    attention to queues that need action (replay or investigation).

    Returns
    -------
    dict[str, tuple[int, int]]  {queue_name: (visible, in_flight)}
    """
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    print(f"\n{'─' * 62}")
    print(f"  Queue depths  —  {now}")
    print(f"{'─' * 62}")
    print(f"  {'Queue':<30}  {'Visible':>8}  {'In-flight':>9}")
    print(f"  {'─' * 30}  {'─' * 8}  {'─' * 9}")

    depths: dict[str, tuple[int, int]] = {}
    for name, label in QUEUES.items():
        if name not in urls:
            continue  # queue doesn't exist in this deployment

        visible, in_flight = get_depth(sqs, urls[name])
        depths[name] = (visible, in_flight)

        # Warn visually when a DLQ has messages — these need manual attention.
        marker = " ⚠" if name.startswith("failed") and visible > 0 else ""
        print(f"  {label:<30}  {visible:>8}  {in_flight:>9}{marker}")

    print(f"{'─' * 62}")
    return depths


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Live SQS queue depth monitor for the DLQ processing demo"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Seconds between polls (default: 10)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Stop after this many seconds even if queues are not empty (default: 600)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Print a single snapshot and exit immediately",
    )
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    sqs = boto3.client("sqs", region_name=args.region)
    urls = resolve_urls(sqs)

    if not urls:
        print("No recognised queues found. Run 'terraform apply' first.")
        sys.exit(1)

    if args.once:
        # Single snapshot mode — useful for scripting and quick checks.
        print_snapshot(sqs, urls)
        return

    print(f"Polling every {args.interval}s  (Ctrl-C to stop, auto-stops when queues empty)")
    start = time.time()

    try:
        while time.time() - start < args.timeout:
            depths = print_snapshot(sqs, urls)

            # Sum visible + in-flight across both main queues (standard + FIFO).
            # We stop only when ALL messages have been either processed or DLQ'd.
            main_visible = (
                depths.get("main-queue", (0, 0))[0]
                + depths.get("main-queue.fifo", (0, 0))[0]
            )
            main_inflight = (
                depths.get("main-queue", (0, 0))[1]
                + depths.get("main-queue.fifo", (0, 0))[1]
            )

            if main_visible == 0 and main_inflight == 0:
                # Both main queues are empty — all messages have been processed
                # (deleted by Lambda) or routed to the DLQ.
                dlq_visible = (
                    depths.get("failed-messages", (0, 0))[0]
                    + depths.get("failed-messages.fifo", (0, 0))[0]
                )
                print(
                    f"\n  ✓ Main queues empty. DLQ contains {dlq_visible} message(s)."
                )
                if dlq_visible == 0:
                    # Perfect outcome — every message was processed successfully.
                    print("  ✓ All messages processed successfully. No DLQ replay needed.")
                else:
                    # Some messages exceeded the retry limit and need replaying.
                    print(
                        "  ⚠ Replay DLQ messages with:\n"
                        "    python scripts/replay_dlq.py\n"
                        "    OR: make replay"
                    )
                break  # exit the polling loop

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")


if __name__ == "__main__":
    main()
