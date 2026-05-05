"""
lambda/dlq_monitor/handler.py — sqs-dlq-monitor
================================================

PURPOSE
-------
This Lambda function acts as an automated incident detector for the DLQ.
It is triggered automatically by the event source mapping in terraform/main.tf
whenever messages arrive in the failed-messages queue.

WHAT IT DOES
------------
1. Receives a batch of up to 10 DLQ messages in a single invocation.
2. Calls _analyse() to compute statistics across the batch:
     - How many messages are in the batch
     - Average and max ApproximateReceiveCount (always 3 for standard DLQ flow)
     - Age of the oldest message in the batch
     - Breakdown of error types found in the message bodies
     - Count of messages that were previously replayed
3. Logs the analysis as structured JSON to CloudWatch Logs so it can be
   queried with CloudWatch Insights (e.g. "show me all DLQ batches from today").
4. Calls _send_alert() to publish a human-readable summary to the dlq-alerts
   SNS topic, which notifies any subscribers (email, Slack webhook, PagerDuty).

MESSAGE LIFECYCLE IN THE DLQ
-----------------------------
Messages land in the DLQ after failing 3 processing attempts in the main queue.
By the time this monitor sees them, ApproximateReceiveCount in the DLQ reflects
how many times the MONITOR has delivered them — it resets to 1 on DLQ entry.
The original attempt count in the main queue is not preserved in the message
metadata, but the body may contain _replayed_at if the message was previously
replayed.

IMPORTANT: DO NOT DELETE MESSAGES HERE
---------------------------------------
The event source mapping deletes messages from the DLQ automatically after
this Lambda returns successfully. Do NOT call sqs.delete_message() inside this
function — that would remove the message before the replayer can move it back
to the main queue, making replay impossible.

If you want to preserve messages for manual inspection, disable the event source
mapping and poll the DLQ manually instead.

ENVIRONMENT VARIABLES
---------------------
SNS_TOPIC_ARN  str  ARN of the dlq-alerts SNS topic. Injected by Terraform.
DLQ_URL        str  URL of the failed-messages queue. Included in alert text
                    so recipients can navigate to it directly.
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialise the SNS client at module level so it is reused across warm
# Lambda invocations. boto3 clients are thread-safe and connection-pooling
# is handled internally, so a single instance is fine here.
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DLQ_URL = os.environ["DLQ_URL"]


# ── Entry point ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Main Lambda handler. Invoked by the SQS event source mapping on the DLQ.

    Parameters
    ----------
    event : dict
        Same SQS event structure as the processor Lambda, but Records come from
        the failed-messages queue rather than the main-queue. Each record
        represents a message that exhausted its retry attempts.

    context : LambdaContext
        Not used directly, but available for remaining_time_in_millis() etc.

    Returns
    -------
    dict
        Status summary returned to the SQS event source mapping.
        No batchItemFailures key — if this Lambda fails, SQS will retry the
        entire batch (all 10 messages) up to the DLQ's own maxReceiveCount.
    """
    records = event["Records"]
    now = datetime.now(timezone.utc)

    # Analyse failure patterns across the whole batch before logging or alerting.
    # This gives a richer picture than logging each message individually.
    analysis = _analyse(records, now)

    # Structured JSON log — queryable with CloudWatch Insights.
    # Example query: fields @timestamp, analysis.total_messages
    #   | filter event = "dlq_batch_received"
    #   | sort @timestamp desc
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

    # Publish the SNS alert. This runs after logging so the CloudWatch entry
    # exists even if SNS.publish() raises an exception.
    _send_alert(analysis, records, now)

    return {
        "statusCode": 200,
        "messagesAnalyzed": len(records),
        "analysis": analysis,
    }


# ── Failure analysis ──────────────────────────────────────────────────────────

def _analyse(records: list[dict], now: datetime) -> dict:
    """
    Compute aggregate statistics across all DLQ messages in this batch.

    The goal is to answer: "Is this a one-off blip or a systematic failure?"
    Key signals:
      - If all messages have receive_count = 3, they all hit the retry limit
        (normal for a random-failure scenario).
      - If receive_count varies, some messages may have been injected directly
        into the DLQ (receive_count = 1) rather than failing through the main queue.
      - error_types breakdown reveals whether failures are diverse (random)
        or concentrated (a specific bug in the processor).
      - replayed_count > 0 means a previous replay pass did not fully resolve
        the issue — those messages are failing again after retry.

    Parameters
    ----------
    records : list[dict]  SQS event records from the DLQ batch.
    now     : datetime    Current UTC time, passed in so all calculations use
                          the same reference point.

    Returns
    -------
    dict  Serialisable analysis summary included in logs and the SNS alert.
    """
    receive_counts: list[int] = []
    send_timestamps: list[float] = []
    # defaultdict means we don't need to check "if key in dict" before incrementing.
    error_types: dict[str, int] = defaultdict(int)
    replayed_count = 0

    for record in records:
        attrs = record.get("attributes", {})

        # ApproximateReceiveCount in the DLQ reflects deliveries in the DLQ,
        # not the original main-queue failure count.
        rc = int(attrs.get("ApproximateReceiveCount", "0"))
        receive_counts.append(rc)

        # SentTimestamp is milliseconds since epoch — convert to seconds for
        # standard Unix timestamp arithmetic.
        ts = attrs.get("SentTimestamp")
        if ts:
            send_timestamps.append(int(ts) / 1000)

        # Inspect the message body for error metadata and replay markers.
        try:
            body = json.loads(record["body"])
            if isinstance(body, dict):
                # The processor Lambda stores error_type in the body when it
                # raises a known exception. For simulated failures the type
                # defaults to "simulated_random_failure".
                error_types[body.get("error_type", "simulated_random_failure")] += 1

                # _replayed_at is stamped by the replayer Lambda when a message
                # is moved from the DLQ back to the main-queue and fails again.
                if "_replayed_at" in body:
                    replayed_count += 1
        except (json.JSONDecodeError, KeyError):
            # Body is not valid JSON (e.g. a plain string sent manually).
            error_types["unparseable_body"] += 1

    # Age of the oldest message = now - earliest SentTimestamp.
    # A large value suggests the DLQ has been accumulating for a while without
    # the monitor firing (e.g. the event source mapping was disabled).
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
        # Number of messages that previously went through the replayer and
        # still failed — indicates the root cause has not been fixed.
        "replayed_count": replayed_count,
    }


# ── SNS alert ─────────────────────────────────────────────────────────────────

def _send_alert(analysis: dict, records: list[dict], now: datetime) -> None:
    """
    Publish a formatted plain-text alert to the dlq-alerts SNS topic.

    SNS delivers this message to all active subscribers — email addresses,
    HTTP/HTTPS endpoints, SQS queues, Lambda functions, mobile push, etc.
    The format is plain text (not JSON) so it reads naturally in an email client.

    Parameters
    ----------
    analysis : dict        Output of _analyse().
    records  : list[dict]  Raw SQS records (used to include sample message bodies).
    now      : datetime    Timestamp used in the alert header.
    """
    subject = (
        f"[DLQ ALERT] {analysis['total_messages']} failed message(s) "
        "in failed-messages queue"
    )

    # Build the alert body as a list of lines then join at the end.
    # This is easier to read and maintain than a large f-string.
    lines = [
        "=" * 55,
        "   DEAD LETTER QUEUE ALERT",
        "=" * 55,
        f"Timestamp : {now.isoformat()}",
        f"Batch size: {analysis['total_messages']} message(s)",
        "",
        "── Failure statistics ──────────────────────────────────",
        # receive_count_avg is almost always 3.0 here because messages arrive
        # in the DLQ only after 3 failed main-queue delivery attempts.
        f"Avg receive count : {analysis['receive_count_avg']}",
        f"Max receive count : {analysis['receive_count_max']}",
        f"Oldest message age: {analysis['oldest_message_age_seconds']} seconds",
        # Non-zero replayed_count is a red flag — the root cause is still active.
        f"Replayed messages : {analysis['replayed_count']}",
        "",
        "── Error type breakdown ────────────────────────────────",
    ]

    # List each distinct error type and how many messages had it.
    for error_type, count in analysis["error_types"].items():
        lines.append(f"  {error_type}: {count}")

    # Include sample bodies so the recipient has context without opening the
    # AWS Console. Limit to 3 samples to keep the email a manageable length.
    lines += ["", "── Sample messages (first 3) ───────────────────────────"]
    for record in records[:3]:
        lines.append(f"  MessageId : {record['messageId']}")
        # Truncate body to 100 chars — full bodies can be thousands of characters.
        lines.append(f"  Body      : {record['body'][:100]}")
        lines.append("")

    lines += [
        "── Recommended actions ─────────────────────────────────",
        "  1. Review CloudWatch Logs for /aws/lambda/sqs-message-processor",
        "     to understand why these messages failed.",
        "  2. Fix the root cause if it is a code or config bug.",
        "  3. Invoke sqs-dlq-replayer to replay messages back to main-queue:",
        "     aws lambda invoke --function-name sqs-dlq-replayer \\",
        '       --payload \'{"max_messages": 100}\' response.json',
        "     OR run: python scripts/replay_dlq.py",
        f"  4. DLQ URL: {DLQ_URL}",
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
                # MessageId returned by SNS can be used to trace the alert in
                # the SNS delivery status logs if the email doesn't arrive.
                "sns_message_id": response["MessageId"],
                "subject": subject,
            }
        )
    )
