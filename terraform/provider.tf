# ── provider.tf ───────────────────────────────────────────────────────────────
#
# PURPOSE:
#   Declares which version of Terraform and which provider plugins this project
#   requires. Terraform reads this file first during `terraform init` to download
#   the correct plugins before any resources are planned or applied.
#
# WHY A SEPARATE FILE:
#   Keeping provider configuration in its own file makes it easy to update
#   versions without touching resource definitions in main.tf.
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  # Enforce a minimum Terraform CLI version. The ~> 1.5 constraint syntax used
  # in required_providers was stabilised in 1.5, so we pin to that floor.
  required_version = ">= 1.5"

  required_providers {
    # hashicorp/aws provides every "aws_*" resource (SQS, Lambda, SNS, IAM…).
    # ~> 5.0 means "any 5.x release" — allows patch/minor upgrades automatically
    # but blocks a breaking major-version bump until we explicitly update this.
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }

    # hashicorp/archive is used in main.tf to zip the Lambda source files on the
    # fly so Terraform can upload them to Lambda without a separate build step.
    # The data "archive_file" resource reads handler.py and outputs handler.zip.
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

# Configure the AWS provider with the region variable defined in variables.tf.
# Credentials are read from the environment (AWS_PROFILE, AWS_ACCESS_KEY_ID,
# or an IAM role) — no secrets are hardcoded here.
provider "aws" {
  region = var.aws_region
}
