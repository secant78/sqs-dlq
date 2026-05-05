# ── variables.tf ──────────────────────────────────────────────────────────────
#
# PURPOSE:
#   Centralises every configurable value so callers can override them at
#   `terraform apply` time without editing resource definitions.
#   All variables have sensible defaults, meaning `terraform apply` works
#   with zero extra flags for a standard demo run.
#
# USAGE EXAMPLES:
#   # Use defaults (us-east-1, no email, 20 % failure rate)
#   terraform apply
#
#   # Enable SNS email alerts
#   terraform apply -var='alert_email=you@example.com'
#
#   # Crank up the failure rate to stress-test the retry logic
#   terraform apply -var='failure_rate=0.5'
#
#   # Deploy to a different region
#   terraform apply -var='aws_region=eu-west-1'
# ─────────────────────────────────────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region to deploy all resources into."
  type        = string
  default     = "us-east-1"
}

variable "alert_email" {
  description = <<-EOT
    Email address to subscribe to the dlq-alerts SNS topic.
    When non-empty, AWS sends a confirmation email; click the link to activate
    the subscription. Leave empty (the default) to skip email alerting —
    the SNS topic is still created and the CloudWatch alarm still fires,
    but no emails are delivered.
  EOT
  type    = string
  default = ""
}

variable "failure_rate" {
  description = <<-EOT
    Probability (0.0 – 1.0) that the processor Lambda will intentionally fail
    a given message on any single attempt, simulating real-world processing
    errors. Passed to the Lambda as the FAILURE_RATE environment variable.

    At 0.2 (20 %): P(fail all 3 attempts) = 0.2³ ≈ 0.8 %
    At 0.5 (50 %): P(fail all 3 attempts) = 0.5³ = 12.5 %

    Higher values push more messages to the DLQ, which is useful for testing
    the monitor Lambda and replay mechanism without sending thousands of messages.
  EOT
  type    = string
  default = "0.2"
}

variable "common_tags" {
  description = <<-EOT
    Key/value tags applied to every AWS resource in this project.
    Tags make it easy to filter costs in AWS Cost Explorer and to identify
    which resources belong to this assignment.
  EOT
  type = map(string)
  default = {
    Project = "assignment-17-sqs-dlq"
    Owner   = "sean"
  }
}
