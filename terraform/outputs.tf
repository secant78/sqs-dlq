output "main_queue_url" {
  description = "URL of the standard main queue"
  value       = aws_sqs_queue.main.url
}

output "main_queue_arn" {
  value = aws_sqs_queue.main.arn
}

output "dlq_url" {
  description = "URL of the standard dead letter queue"
  value       = aws_sqs_queue.dlq.url
}

output "dlq_arn" {
  value = aws_sqs_queue.dlq.arn
}

output "main_fifo_queue_url" {
  description = "URL of the FIFO main queue"
  value       = aws_sqs_queue.main_fifo.url
}

output "dlq_fifo_url" {
  description = "URL of the FIFO dead letter queue"
  value       = aws_sqs_queue.dlq_fifo.url
}

output "sns_topic_arn" {
  description = "ARN of the DLQ alert SNS topic"
  value       = aws_sns_topic.dlq_alerts.arn
}

output "processor_function_name" {
  value = aws_lambda_function.processor.function_name
}

output "dlq_monitor_function_name" {
  value = aws_lambda_function.dlq_monitor.function_name
}

output "replayer_function_name" {
  value = aws_lambda_function.replayer.function_name
}

output "processor_log_group" {
  value = aws_cloudwatch_log_group.processor.name
}

output "dlq_monitor_log_group" {
  value = aws_cloudwatch_log_group.dlq_monitor.name
}

output "replayer_log_group" {
  value = aws_cloudwatch_log_group.replayer.name
}
