"""
sqs-dlq-monitor
---------------
Triggered whenever messages arrive in the failed-messages DLQ
(event source mapping, batch_size=10).

Behaviour:
  - Receives a batch of DLQ messages.
  - Analyses failure patterns: receive-count distribution, age, error types.
  - Logs a structured summary to CloudWatch.
  - Publishes a formatted SNS alert so the team is notified immediately.

NOTE: The DLQ monitor does NOT delete the messages — it just reads them via
the event source mapping. SQS deletes them automatically after the Lambda
returns successfully. If you want to keep messages for manual inspection,
disable the event source mapping and poll manually.
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DLQ_URL = os.environ["DLQ_URL"]


# ── Entry point ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    records = event["Records"]
    now = datetime.now(timezone.utc)

    analysis = _analyse(records, now)

    logger.info(
        json.dumps(
            {
                "event": "dlq_batch_received",
                "timestamp": now.isoformat(),
                "message_count": len(records),
                "analysis": analysis,
            }
        )
    )

    _send_alert(analysis, records, now)

    return {
        "statusCode": 200,
        "messagesAnalyzed": len(records),
        "analysis": analysis,
    }


# ── Failure analysis ──────────────────────────────────────────────────────────

def _analyse(records: list[dict], now: datetime) -> dict:
    """
    Compute statistics across all DLQ messages in this batch.

    Tracked fields
    --------------
    total_messages        - how many messages arrived in this batch
    receive_count_*       - distribution of ApproximateReceiveCount values
    oldest_age_seconds    - age of the oldest message
    error_types           - breakdown by error_type field (if present in body)
    replayed_count        - messages that had already been replayed at least once
    """
    receive_counts: list[int] = []
    send_timestamps: list[float] = []
    error_types: dict[str, int] = defaultdict(int)
    replayed_count = 0

    for record in records:
        attrs = record.get("attributes", {})

        rc = int(attrs.get("ApproximateReceiveCount", "0"))
        receive_counts.append(rc)

        ts = attrs.get("SentTimestamp")
        if ts:
            send_timestamps.append(int(ts) / 1000)

        try:
            body = json.loads(record["body"])
            if isinstance(body, dict):
                error_types[body.get("error_type", "simulated_random_failure")] += 1
                if "_replayed_at" in body:
                    replayed_count += 1
        except (json.JSONDecodeError, KeyError):
            error_types["unparseable_body"] += 1

    oldest_age = (
        int(now.timestamp() - min(send_timestamps)) if send_timestamps else 0
    )

    return {
        "total_messages": len(records),
        "receive_count_avg": (
            round(sum(receive_counts) / len(receive_counts), 1) if receive_counts else 0
        ),
        "receive_count_max": max(receive_counts, default=0),
        "oldest_message_age_seconds": oldest_age,
        "error_types": dict(error_types),
        "replayed_count": replayed_count,
    }


# ── SNS alert ─────────────────────────────────────────────────────────────────

def _send_alert(analysis: dict, records: list[dict], now: datetime) -> None:
    subject = (
        f"[DLQ ALERT] {analysis['total_messages']} failed message(s) "
        "in failed-messages queue"
    )

    lines = [
        "=" * 55,
        "   DEAD LETTER QUEUE ALERT",
        "=" * 55,
        f"Timestamp : {now.isoformat()}",
        f"Batch size: {analysis['total_messages']} message(s)",
        "",
        "── Failure statistics ──────────────────────────────────",
        f"Avg receive count : {analysis['receive_count_avg']}",
        f"Max receive count : {analysis['receive_count_max']}",
        f"Oldest message age: {analysis['oldest_message_age_seconds']} seconds",
        f"Replayed messages : {analysis['replayed_count']}",
        "",
        "── Error type breakdown ────────────────────────────────",
    ]

    for error_type, count in analysis["error_types"].items():
        lines.append(f"  {error_type}: {count}")

    lines += [
        "",
        "── Sample messages (first 3) ───────────────────────────",
    ]
    for record in records[:3]:
        lines.append(f"  MessageId : {record['messageId']}")
        lines.append(f"  Body      : {record['body'][:100]}")
        lines.append("")

    lines += [
        "── Recommended actions ─────────────────────────────────",
        "  1. Review CloudWatch Logs for /aws/lambda/sqs-message-processor",
        "  2. Invoke sqs-dlq-replayer to replay messages back to main-queue",
        "     aws lambda invoke --function-name sqs-dlq-replayer \\",
        '       --payload \'{"max_messages": 100}\' response.json',
        f"  3. DLQ URL: {DLQ_URL}",
        "=" * 55,
    ]

    message_body = "\n".join(lines)

    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message_body,
    )

    logger.info(
        json.dumps(
            {
                "event": "sns_alert_sent",
                "sns_message_id": response["MessageId"],
                "subject": subject,
            }
        )
    )
