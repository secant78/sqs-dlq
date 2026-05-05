#!/usr/bin/env python3
"""
monitor_queues.py
-----------------
Poll SQS queue depths every N seconds and print a live dashboard.
Exits when both queues are empty OR after --timeout seconds.

Usage:
    python scripts/monitor_queues.py
    python scripts/monitor_queues.py --interval 5 --timeout 300
    python scripts/monitor_queues.py --once          # single snapshot
"""

import argparse
import sys
import time
from datetime import datetime, timezone

import boto3


QUEUES = {
    "main-queue": "Main Queue",
    "failed-messages": "DLQ (failed-messages)",
    "main-queue.fifo": "Main FIFO",
    "failed-messages.fifo": "DLQ FIFO",
}


def get_depth(sqs, queue_url: str) -> tuple[int, int]:
    """Return (visible, in_flight) message counts for a queue URL."""
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


def resolve_urls(sqs, region: str) -> dict[str, str]:
    """Build a {queue_name: queue_url} map, skipping queues that don't exist."""
    urls: dict[str, str] = {}
    for name in QUEUES:
        try:
            urls[name] = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        except sqs.exceptions.QueueDoesNotExist:
            pass
    return urls


def print_snapshot(sqs, urls: dict[str, str]) -> dict[str, tuple[int, int]]:
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    print(f"\n{'─' * 62}")
    print(f"  Queue depths  —  {now}")
    print(f"{'─' * 62}")
    print(f"  {'Queue':<30}  {'Visible':>8}  {'In-flight':>9}")
    print(f"  {'─'*30}  {'─'*8}  {'─'*9}")

    depths: dict[str, tuple[int, int]] = {}
    for name, label in QUEUES.items():
        if name not in urls:
            continue
        visible, in_flight = get_depth(sqs, urls[name])
        depths[name] = (visible, in_flight)
        marker = " ⚠" if name.startswith("failed") and visible > 0 else ""
        print(f"  {label:<30}  {visible:>8}  {in_flight:>9}{marker}")

    print(f"{'─' * 62}")
    return depths


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor SQS queue depths")
    parser.add_argument("--interval", type=int, default=10, help="Seconds between polls (default: 10)")
    parser.add_argument("--timeout", type=int, default=600, help="Stop after this many seconds (default: 600)")
    parser.add_argument("--once", action="store_true", help="Print a single snapshot and exit")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    sqs = boto3.client("sqs", region_name=args.region)
    urls = resolve_urls(sqs, args.region)

    if not urls:
        print("No recognised queues found. Have you run `terraform apply` yet?")
        sys.exit(1)

    if args.once:
        print_snapshot(sqs, urls)
        return

    print(f"Polling every {args.interval}s  (Ctrl-C to stop)")
    start = time.time()

    try:
        while time.time() - start < args.timeout:
            depths = print_snapshot(sqs, urls)

            # Stop automatically if both main queues are drained
            main_visible = (
                depths.get("main-queue", (0, 0))[0]
                + depths.get("main-queue.fifo", (0, 0))[0]
            )
            main_inflight = (
                depths.get("main-queue", (0, 0))[1]
                + depths.get("main-queue.fifo", (0, 0))[1]
            )

            if main_visible == 0 and main_inflight == 0:
                dlq_visible = (
                    depths.get("failed-messages", (0, 0))[0]
                    + depths.get("failed-messages.fifo", (0, 0))[0]
                )
                print(
                    f"\n  ✓ Main queues empty. "
                    f"DLQ contains {dlq_visible} message(s)."
                )
                if dlq_visible == 0:
                    print("  ✓ All messages processed successfully.")
                else:
                    print(
                        "  ⚠ Run the replayer to move DLQ messages back:\n"
                        "    python scripts/replay_dlq.py"
                    )
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")


if __name__ == "__main__":
    main()
