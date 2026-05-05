# ── outputs.tf ────────────────────────────────────────────────────────────────
#
# PURPOSE:
#   Exposes key resource attributes after `terraform apply` so they can be
#   used immediately by the scripts (send_messages.py, replay_dlq.py, etc.)
#   without requiring a manual trip to the AWS Console.
#
# HOW TO READ THEM:
#   terraform output                    # print all outputs
#   terraform output -raw main_queue_url  # print just one value (no quotes)
#   terraform output -json              # machine-readable JSON
#
# HOW THE SCRIPTS USE THEM:
#   The Python scripts resolve queue URLs by name via get_queue_url(), so they
#   don't need to read these outputs directly. The outputs are primarily for
#   human reference and for wiring into CI/CD pipelines.
# ─────────────────────────────────────────────────────────────────────────────

# ── Queue URLs and ARNs ───────────────────────────────────────────────────────
# URLs are needed to call SQS API operations (send, receive, delete).
# ARNs are needed for IAM policies and event source mapping references.

output "main_queue_url" {
  description = "URL of the standard main-queue. Pass to --queue-url in send_messages.py."
  value       = aws_sqs_queue.main.url
}

output "main_queue_arn" {
  description = "ARN of the standard main-queue. Used in IAM policies."
  value       = aws_sqs_queue.main.arn
}

output "dlq_url" {
  description = "URL of the failed-messages DLQ. Used by the replayer to read and delete messages."
  value       = aws_sqs_queue.dlq.url
}

output "dlq_arn" {
  description = "ARN of the failed-messages DLQ."
  value       = aws_sqs_queue.dlq.arn
}

output "main_fifo_queue_url" {
  description = "URL of the FIFO main-queue. Pass --fifo to send_messages.py to target this queue."
  value       = aws_sqs_queue.main_fifo.url
}

output "dlq_fifo_url" {
  description = "URL of the FIFO dead letter queue."
  value       = aws_sqs_queue.dlq_fifo.url
}

# ── SNS ───────────────────────────────────────────────────────────────────────

output "sns_topic_arn" {
  description = "ARN of the dlq-alerts SNS topic. Add subscribers here to receive DLQ alerts."
  value       = aws_sns_topic.dlq_alerts.arn
}

# ── Lambda function names ─────────────────────────────────────────────────────
# Function names (not ARNs) are what the AWS CLI and scripts use to invoke
# Lambda directly: `aws lambda invoke --function-name <name> ...`

output "processor_function_name" {
  description = "Name of the message processor Lambda (consumes main-queue)."
  value       = aws_lambda_function.processor.function_name
}

output "dlq_monitor_function_name" {
  description = "Name of the DLQ monitor Lambda (triggered by failed-messages)."
  value       = aws_lambda_function.dlq_monitor.function_name
}

output "replayer_function_name" {
  description = "Name of the DLQ replayer Lambda (invoked manually to replay messages)."
  value       = aws_lambda_function.replayer.function_name
}

# ── CloudWatch Log Group names ────────────────────────────────────────────────
# Used with `aws logs tail <name> --follow` or `make logs-*` shortcuts.

output "processor_log_group" {
  description = "CloudWatch Log Group for the processor Lambda. Shows per-message attempt logs."
  value       = aws_cloudwatch_log_group.processor.name
}

output "dlq_monitor_log_group" {
  description = "CloudWatch Log Group for the DLQ monitor Lambda. Shows failure analysis summaries."
  value       = aws_cloudwatch_log_group.dlq_monitor.name
}

output "replayer_log_group" {
  description = "CloudWatch Log Group for the replayer Lambda. Shows replay progress per message."
  value       = aws_cloudwatch_log_group.replayer.name
}
