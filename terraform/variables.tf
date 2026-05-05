variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "alert_email" {
  description = "Email address to receive DLQ SNS alerts (leave empty to skip)"
  type        = string
  default     = ""
}

variable "failure_rate" {
  description = "Simulated failure rate for the processor Lambda (0.0 – 1.0)"
  type        = string
  default     = "0.2"
}

variable "common_tags" {
  description = "Tags applied to every resource"
  type        = map(string)
  default = {
    Project = "assignment-17-sqs-dlq"
    Owner   = "sean"
  }
}
