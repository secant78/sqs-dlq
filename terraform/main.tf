# ── main.tf ───────────────────────────────────────────────────────────────────
#
# PURPOSE:
#   Declares every AWS resource needed for the SQS DLQ processing demo:
#     1. Standard SQS queue (main-queue) + Dead Letter Queue (failed-messages)
#     2. FIFO versions of both queues with content-based deduplication
#     3. SNS topic for DLQ alerts + optional email subscription
#     4. IAM role shared by all three Lambda functions
#     5. CloudWatch Log Groups with explicit retention policies
#     6. Three Lambda functions: processor, dlq_monitor, replayer
#     7. Event source mappings that wire queues → Lambdas automatically
#     8. CloudWatch alarm that fires when DLQ depth exceeds zero
#
# RESOURCE FLOW:
#   send_messages.py
#        │
#        ▼
#   main-queue ──(on message)──▶ sqs-message-processor Lambda
#        │                           │ (fails 20 % randomly)
#        │ after 3 failures          │
#        ▼ (redrive policy)          │ raises exception → SQS retries
#   failed-messages (DLQ) ◀─────────┘
#        │
#        ├──(on message)──▶ sqs-dlq-monitor Lambda ──▶ SNS dlq-alerts
#        │
#        └──(manual)──────▶ sqs-dlq-replayer Lambda ──▶ back to main-queue
# ─────────────────────────────────────────────────────────────────────────────


# ── Dead Letter Queue (must be created before main-queue) ─────────────────────
#
# The DLQ is a plain SQS queue that serves as the destination for messages that
# could not be processed after maxReceiveCount attempts. It uses a long retention
# period (14 days) so failed messages are not auto-deleted before the team can
# investigate or replay them.
#
# IMPORTANT: The DLQ must exist before the main-queue references it in the
# redrive_policy, which is why it is declared first in this file.

resource "aws_sqs_queue" "dlq" {
  name = "failed-messages"

  # Matches the main queue's visibility timeout. If the DLQ monitor Lambda
  # ever takes longer than this to acknowledge a message, SQS will re-deliver
  # it — consistent timeouts prevent that scenario.
  visibility_timeout_seconds = 30

  # 14 days gives the team maximum investigation time before messages expire.
  # The AWS maximum retention period is 1,209,600 seconds (14 days).
  message_retention_seconds = 1209600

  tags = var.common_tags
}


# ── Main Queue ─────────────────────────────────────────────────────────────────
#
# The primary intake queue. Producers (send_messages.py) write here; the
# sqs-message-processor Lambda reads from here via an event source mapping.
#
# KEY SETTINGS:
#   visibility_timeout_seconds — how long a message is hidden from other
#     consumers while a Lambda is processing it. Must be >= Lambda timeout
#     to prevent a message from becoming visible again mid-processing, which
#     would cause duplicate delivery. Both are 30 s here.
#
#   redrive_policy — after maxReceiveCount (3) failed delivery attempts,
#     SQS automatically moves the message to the deadLetterTargetArn.
#     "Failed" means the Lambda raised an exception OR the visibility timeout
#     expired before the Lambda deleted the message.

resource "aws_sqs_queue" "main" {
  name                       = "main-queue"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400 # 1 day is sufficient for the main queue

  # The redrive_policy is stored as a JSON string.
  # maxReceiveCount: 3 means SQS allows 3 total delivery attempts before
  # routing the message to the DLQ. The counter is tracked by SQS via the
  # ApproximateReceiveCount message attribute.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })

  tags = var.common_tags
}

# Allow the main queue to write to the DLQ (required by AWS since 2023).
# Without this policy, SQS cannot move messages to the DLQ even if the
# redrive_policy is set correctly — the delivery will silently fail.
resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.url
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main.arn]
  })
}


# ── FIFO Dead Letter Queue ─────────────────────────────────────────────────────
#
# FIFO queues can only redrive to other FIFO queues — a standard DLQ would
# be rejected. This is the DLQ for the main-queue.fifo FIFO queue.
#
# content_based_deduplication: SQS computes a SHA-256 of the message body and
# uses it as the deduplication ID automatically, so callers don't need to
# provide one explicitly.

resource "aws_sqs_queue" "dlq_fifo" {
  name                        = "failed-messages.fifo"
  fifo_queue                  = true  # FIFO queues must have the .fifo suffix
  content_based_deduplication = true
  visibility_timeout_seconds  = 30
  message_retention_seconds   = 1209600
  tags                        = var.common_tags
}


# ── FIFO Main Queue ────────────────────────────────────────────────────────────
#
# The FIFO (First-In-First-Out) variant of the main queue. Use this when:
#   1. Message ORDER matters (e.g. sequential state transitions)
#   2. EXACTLY-ONCE delivery is required within the 5-minute dedup window
#
# FIFO guarantees:
#   - Messages within the same MessageGroupId are delivered in send order
#   - Duplicate messages (same body within 5 min) are delivered only once
#   - Throughput is capped at 300 msg/s (or 3,000 with batching)
#
# content_based_deduplication replaces explicit MessageDeduplicationId fields.
# SQS hashes the body — if the same body is sent twice within 5 minutes,
# the second send is silently dropped (acknowledged but not queued).

resource "aws_sqs_queue" "main_fifo" {
  name                        = "main-queue.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 30
  message_retention_seconds   = 86400

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq_fifo.arn
    maxReceiveCount     = 3
  })

  tags = var.common_tags
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq_fifo" {
  queue_url = aws_sqs_queue.dlq_fifo.url
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main_fifo.arn]
  })
}


# ── SNS Topic for DLQ Alerts ──────────────────────────────────────────────────
#
# The sqs-dlq-monitor Lambda publishes to this topic every time it receives a
# batch of DLQ messages. Subscribers (email, HTTP endpoints, other Lambdas,
# etc.) are notified immediately so the team can act before messages expire.

resource "aws_sns_topic" "dlq_alerts" {
  name = "dlq-alerts"
  tags = var.common_tags
}

# Optional email subscription. The count meta-argument creates this resource
# only when alert_email is non-empty — count = 0 means "don't create it".
# After `terraform apply`, AWS sends a confirmation email to the address;
# the subscription is only active after the recipient clicks the confirm link.
resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.dlq_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}


# ── IAM Role for Lambda Functions ─────────────────────────────────────────────
#
# All three Lambda functions (processor, dlq_monitor, replayer) share this
# single IAM role. The role has two policies attached:
#   1. AWSLambdaBasicExecutionRole — managed policy that grants permission to
#      write logs to CloudWatch. Required for ANY Lambda function.
#   2. lambda_sqs_sns (inline) — custom policy granting the specific SQS and
#      SNS permissions this project needs, scoped to only our queue ARNs.
#
# WHY A SHARED ROLE:
#   All three functions need identical permissions (SQS + SNS + CloudWatch).
#   A single role is simpler and avoids IAM resource sprawl for a demo project.
#   In production, each function would have its own least-privilege role.

data "aws_iam_policy_document" "lambda_assume" {
  # Trust policy — tells AWS which service is allowed to assume this role.
  # Only the Lambda service can use this role; no human or other service can.
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "sqs-dlq-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = var.common_tags
}

# Attach the AWS-managed basic execution policy.
# This grants: logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents
# Without it, any logger.info() call inside Lambda silently discards output.
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Inline policy for SQS and SNS access.
# Resource ARNs are scoped to only the four queues in this project — if a new
# queue is added, this policy must be updated to include its ARN.
resource "aws_iam_role_policy" "lambda_sqs_sns" {
  name = "sqs-sns-access"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "SQSAccess"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",       # read messages from a queue
          "sqs:DeleteMessage",        # acknowledge (delete) after processing
          "sqs:GetQueueAttributes",   # read depth / other metadata
          "sqs:SendMessage",          # replayer needs this to re-enqueue messages
          "sqs:ChangeMessageVisibility", # extend visibility timeout if needed
          "sqs:GetQueueUrl",          # resolve queue name → URL
        ]
        # Explicitly limited to only our four queues; wildcards avoided.
        Resource = [
          aws_sqs_queue.main.arn,
          aws_sqs_queue.dlq.arn,
          aws_sqs_queue.main_fifo.arn,
          aws_sqs_queue.dlq_fifo.arn,
        ]
      },
      {
        Sid      = "SNSPublish"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.dlq_alerts.arn]
      }
    ]
  })
}


# ── CloudWatch Log Groups ──────────────────────────────────────────────────────
#
# Lambda creates log groups automatically, but without an explicit Terraform
# resource the retention period defaults to "never expire" — logs accumulate
# forever and incur unbounded storage costs. Declaring the groups here lets
# us set a short 7-day retention that is appropriate for a demo project.
#
# The depends_on on each Lambda ensures the log group exists before the first
# invocation writes to it, preventing a race condition on cold starts.

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/aws/lambda/sqs-message-processor"
  retention_in_days = 7
  tags              = var.common_tags
}

resource "aws_cloudwatch_log_group" "dlq_monitor" {
  name              = "/aws/lambda/sqs-dlq-monitor"
  retention_in_days = 7
  tags              = var.common_tags
}

resource "aws_cloudwatch_log_group" "replayer" {
  name              = "/aws/lambda/sqs-dlq-replayer"
  retention_in_days = 7
  tags              = var.common_tags
}


# ── Lambda: Message Processor ─────────────────────────────────────────────────
#
# WHAT IT DOES:
#   Reads messages from main-queue one at a time (batch_size = 1), simulates
#   processing with a 20 % random failure rate, and logs every attempt.
#   Failures cause SQS to increment the ApproximateReceiveCount. After 3
#   failures the redrive policy moves the message to the DLQ.
#
# WHY batch_size = 1:
#   When batch_size > 1 and the Lambda raises an exception, ALL messages in the
#   batch have their receive count incremented even if only one actually failed.
#   batch_size = 1 isolates failures to individual messages so the receive
#   count accurately reflects how many times THAT message failed.
#   (ReportBatchItemFailures would allow selective failure reporting with larger
#   batches, but batch_size=1 is the simplest and clearest approach here.)
#
# PACKAGING:
#   data.archive_file zips handler.py on every `terraform plan/apply`.
#   source_code_hash detects if the zip changed and triggers a Lambda update.

data "archive_file" "processor" {
  type        = "zip"
  source_file = "${path.module}/../lambda/processor/handler.py"
  output_path = "${path.module}/../lambda/processor/handler.zip"
}

resource "aws_lambda_function" "processor" {
  filename         = data.archive_file.processor.output_path
  source_code_hash = data.archive_file.processor.output_base64sha256
  function_name    = "sqs-message-processor"
  role             = aws_iam_role.lambda.arn

  # handler = "<filename_without_.py>.<function_name>"
  handler = "handler.lambda_handler"
  runtime = "python3.12"

  # timeout must be <= visibility_timeout_seconds on the queue (both are 30 s).
  # If a Lambda invocation exceeds the timeout, the message becomes visible
  # again in the queue before the Lambda finishes — causing a double-delivery.
  timeout = 30

  environment {
    variables = {
      # Passed in from variables.tf so the failure rate can be changed at
      # deploy time without modifying source code.
      FAILURE_RATE = var.failure_rate
    }
  }

  depends_on = [aws_cloudwatch_log_group.processor]
  tags       = var.common_tags
}

# Event source mapping — tells Lambda to poll main-queue and invoke the
# processor function automatically when messages arrive.
# SQS handles the polling loop; no cron or trigger rule is needed.
resource "aws_lambda_event_source_mapping" "processor" {
  event_source_arn = aws_sqs_queue.main.arn
  function_name    = aws_lambda_function.processor.arn

  # One message per Lambda invocation keeps failure isolation clean.
  batch_size = 1

  # ReportBatchItemFailures tells SQS to only retry the specific messages that
  # the Lambda reported as failed, rather than the entire batch. With batch_size
  # = 1 this has no effect, but it is included as best practice so the setting
  # is already in place if batch_size is increased later.
  function_response_types = ["ReportBatchItemFailures"]

  enabled = true
}


# ── Lambda: DLQ Monitor ────────────────────────────────────────────────────────
#
# WHAT IT DOES:
#   Triggered automatically whenever messages land in the failed-messages DLQ.
#   Reads up to 10 messages per invocation, analyses failure patterns across
#   the batch (receive count distribution, error types, message age), logs a
#   structured JSON summary to CloudWatch, and publishes a formatted alert to
#   the dlq-alerts SNS topic.
#
# NOTE: The monitor does NOT delete messages itself. The event source mapping
#   handles deletion — SQS deletes the messages after the Lambda returns
#   successfully. If the monitor Lambda itself fails, the messages remain in
#   the DLQ and SQS retries the delivery.

data "archive_file" "dlq_monitor" {
  type        = "zip"
  source_file = "${path.module}/../lambda/dlq_monitor/handler.py"
  output_path = "${path.module}/../lambda/dlq_monitor/handler.zip"
}

resource "aws_lambda_function" "dlq_monitor" {
  filename         = data.archive_file.dlq_monitor.output_path
  source_code_hash = data.archive_file.dlq_monitor.output_base64sha256
  function_name    = "sqs-dlq-monitor"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.dlq_alerts.arn
      # Included in alert messages so recipients know exactly which queue
      # to inspect in the AWS Console without having to look it up.
      DLQ_URL = aws_sqs_queue.dlq.url
    }
  }

  depends_on = [aws_cloudwatch_log_group.dlq_monitor]
  tags       = var.common_tags
}

# Wire the DLQ to the monitor Lambda.
# batch_size = 10: the monitor benefits from seeing multiple failures at once
# so it can aggregate patterns (e.g. "8 out of 10 messages failed with the
# same error type"). A larger batch gives richer analysis.
resource "aws_lambda_event_source_mapping" "dlq_monitor" {
  event_source_arn = aws_sqs_queue.dlq.arn
  function_name    = aws_lambda_function.dlq_monitor.arn
  batch_size       = 10
  enabled          = true
}


# ── Lambda: DLQ Replayer ───────────────────────────────────────────────────────
#
# WHAT IT DOES:
#   Invoked manually (via the CLI, scripts/replay_dlq.py, or AWS Console) to
#   move messages from the DLQ back to the main-queue so they can be retried.
#   Supports dry-run mode (inspect without moving) and reset-attempts mode
#   (strip replay metadata so the processor sees the message as fresh).
#
# TIMEOUT = 300 s (5 minutes):
#   The replayer may need to drain a large DLQ backlog. Each iteration polls
#   10 messages, sends them, and deletes them — roughly 50–100 ms per message.
#   5 minutes allows replaying ~3,000 messages in a single invocation.
#
# NOT wired to an event source:
#   The replayer is triggered on-demand, not automatically, because replaying
#   messages is an intentional human action that should not happen without
#   understanding WHY those messages failed.

data "archive_file" "replayer" {
  type        = "zip"
  source_file = "${path.module}/../lambda/replayer/handler.py"
  output_path = "${path.module}/../lambda/replayer/handler.zip"
}

resource "aws_lambda_function" "replayer" {
  filename         = data.archive_file.replayer.output_path
  source_code_hash = data.archive_file.replayer.output_base64sha256
  function_name    = "sqs-dlq-replayer"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 300

  environment {
    variables = {
      MAIN_QUEUE_URL = aws_sqs_queue.main.url
      DLQ_URL        = aws_sqs_queue.dlq.url
    }
  }

  depends_on = [aws_cloudwatch_log_group.replayer]
  tags       = var.common_tags
}


# ── CloudWatch Alarm: DLQ Depth ───────────────────────────────────────────────
#
# This alarm is a secondary alert path that fires if the DLQ accumulates
# messages and the depth stays > 0. It catches the edge case where the
# sqs-dlq-monitor Lambda itself is broken — in that scenario the monitor
# would never send an SNS alert, but this alarm fires independently.
#
# EVALUATION:
#   evaluation_periods = 1 + period = 60 s means the alarm fires as soon as
#   the first 60-second datapoint shows ApproximateNumberOfMessagesVisible > 0.
#   treat_missing_data = "notBreaching" prevents false alarms during periods
#   when the queue has no traffic and CloudWatch emits no datapoints.

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "dlq-messages-visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60   # check once per minute
  statistic           = "Sum"
  threshold           = 0    # any message in the DLQ triggers the alarm
  alarm_description   = "One or more messages are present in the failed-messages DLQ."
  alarm_actions       = [aws_sns_topic.dlq_alerts.arn]
  ok_actions          = [aws_sns_topic.dlq_alerts.arn] # notify when DLQ drains
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  tags = var.common_tags
}
