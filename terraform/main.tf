# ── Dead Letter Queue ─────────────────────────────────────────────────────────

resource "aws_sqs_queue" "dlq" {
  name                       = "failed-messages"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 1209600 # 14 days — plenty of time to investigate
  tags                       = var.common_tags
}

# ── Main Queue ────────────────────────────────────────────────────────────────
# Messages that fail processing 3 times are automatically moved to the DLQ.

resource "aws_sqs_queue" "main" {
  name                       = "main-queue"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400 # 1 day

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })

  tags = var.common_tags
}

# Allow the main queue to send to the DLQ (required for redrive)
resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.url
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main.arn]
  })
}

# ── FIFO Dead Letter Queue ────────────────────────────────────────────────────

resource "aws_sqs_queue" "dlq_fifo" {
  name                        = "failed-messages.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 30
  message_retention_seconds   = 1209600
  tags                        = var.common_tags
}

# ── FIFO Main Queue (with deduplication) ──────────────────────────────────────
# FIFO queues guarantee exactly-once delivery within the deduplication window
# and preserve ordering within each MessageGroupId.

resource "aws_sqs_queue" "main_fifo" {
  name                        = "main-queue.fifo"
  fifo_queue                  = true
  content_based_deduplication = true # SHA-256 of body = deduplication ID
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

# ── SNS Topic for DLQ alerts ──────────────────────────────────────────────────

resource "aws_sns_topic" "dlq_alerts" {
  name = "dlq-alerts"
  tags = var.common_tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.dlq_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── IAM Role shared by all three Lambda functions ─────────────────────────────

data "aws_iam_policy_document" "lambda_assume" {
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

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

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
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:SendMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueUrl",
        ]
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

# ── CloudWatch Log Groups (explicit so retention is set) ──────────────────────

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
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30

  environment {
    variables = {
      FAILURE_RATE = var.failure_rate
    }
  }

  depends_on = [aws_cloudwatch_log_group.processor]
  tags       = var.common_tags
}

# batch_size = 1 means each message is its own invocation — a failure on one
# message cannot cause another message's receive count to increment.
resource "aws_lambda_event_source_mapping" "processor" {
  event_source_arn        = aws_sqs_queue.main.arn
  function_name           = aws_lambda_function.processor.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  enabled                 = true
}

# ── Lambda: DLQ Monitor ───────────────────────────────────────────────────────

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
      DLQ_URL       = aws_sqs_queue.dlq.url
    }
  }

  depends_on = [aws_cloudwatch_log_group.dlq_monitor]
  tags       = var.common_tags
}

# Triggers whenever messages land in the DLQ — batch up to 10 at once so the
# monitor can identify failure patterns across multiple messages.
resource "aws_lambda_event_source_mapping" "dlq_monitor" {
  event_source_arn = aws_sqs_queue.dlq.arn
  function_name    = aws_lambda_function.dlq_monitor.arn
  batch_size       = 10
  enabled          = true
}

# ── Lambda: DLQ Replayer ──────────────────────────────────────────────────────

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
  timeout          = 300 # 5 minutes to drain large DLQ backlogs

  environment {
    variables = {
      MAIN_QUEUE_URL = aws_sqs_queue.main.url
      DLQ_URL        = aws_sqs_queue.dlq.url
    }
  }

  depends_on = [aws_cloudwatch_log_group.replayer]
  tags       = var.common_tags
}

# ── CloudWatch Alarm: DLQ depth ───────────────────────────────────────────────
# Secondary alert path — fires if DLQ message count stays elevated, which means
# the DLQ monitor Lambda itself may have a problem.

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "dlq-messages-visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Messages present in failed-messages DLQ"
  alarm_actions       = [aws_sns_topic.dlq_alerts.arn]
  ok_actions          = [aws_sns_topic.dlq_alerts.arn]
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  tags = var.common_tags
}
