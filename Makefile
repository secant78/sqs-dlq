# ── Makefile ──────────────────────────────────────────────────────────────────
#
# PURPOSE:
#   Provides short, memorable shortcuts for every step of the SQS DLQ demo.
#   Instead of remembering long AWS CLI commands or Python flags, you run
#   `make send` or `make replay`.
#
# VARIABLES (override on the command line):
#   REGION   AWS region (default: us-east-1)
#   COUNT    Number of messages to send (default: 100)
#   MAX      Max messages to replay from DLQ (default: 1000 = drain all)
#
# EXAMPLES:
#   make apply                   # deploy all infrastructure
#   make send                    # send 100 messages to main-queue
#   make send COUNT=50           # send 50 messages
#   make monitor                 # live queue-depth dashboard
#   make replay                  # replay DLQ → main-queue via Lambda
#   make logs-processor          # tail the processor Lambda's CloudWatch logs
#   make destroy                 # tear down all AWS resources
#
# HOW `##` COMMENTS WORK:
#   The `help` target uses grep + awk to extract lines matching
#   `target: ## description` and prints them in a formatted table.
#   Targets without a `##` comment are hidden from `make help`.
# ─────────────────────────────────────────────────────────────────────────────

# ── Default variable values ────────────────────────────────────────────────────
# ?= means "set only if not already set in the environment or command line"
REGION   ?= us-east-1
COUNT    ?= 100
MAX      ?= 1000

# := means "evaluate right now" (not lazily). Used for constants.
TF_DIR   := terraform

# .PHONY tells Make these are not real file targets — it should always run the
# recipe even if a file with that name exists in the directory.
.PHONY: init plan apply destroy send send-fifo monitor monitor-once \
        replay replay-dry replay-direct replay-fifo \
        logs-processor logs-dlq logs-replayer demo help

# ── Help ──────────────────────────────────────────────────────────────────────

help: ## Show this help message with all available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Terraform ──────────────────────────────────────────────────────────────────

init: ## Download Terraform providers (run once after cloning)
	cd $(TF_DIR) && terraform init

plan: ## Preview infrastructure changes without applying them
	cd $(TF_DIR) && terraform plan

apply: ## Deploy all AWS infrastructure (queues, Lambdas, SNS, IAM, CloudWatch)
	cd $(TF_DIR) && terraform apply

destroy: ## Tear down all AWS resources created by terraform apply
	cd $(TF_DIR) && terraform destroy

# ── Send messages ─────────────────────────────────────────────────────────────

send: ## Send COUNT messages to main-queue (standard). Default: COUNT=100
	python scripts/send_messages.py --count $(COUNT) --region $(REGION)

send-fifo: ## Send COUNT messages to main-queue.fifo (FIFO with deduplication)
	python scripts/send_messages.py --count $(COUNT) --fifo --region $(REGION)

# ── Monitor ───────────────────────────────────────────────────────────────────

monitor: ## Live queue-depth dashboard — polls every 5 s (Ctrl-C to stop)
	python scripts/monitor_queues.py --interval 5 --region $(REGION)

monitor-once: ## Print a single queue-depth snapshot and exit
	python scripts/monitor_queues.py --once --region $(REGION)

# ── Replay ────────────────────────────────────────────────────────────────────

replay: ## Replay all DLQ messages to main-queue via the replayer Lambda
	python scripts/replay_dlq.py --max $(MAX) --region $(REGION)

replay-dry: ## Dry-run: show what would be replayed without moving any messages
	python scripts/replay_dlq.py --dry-run --region $(REGION)

replay-direct: ## Replay directly via boto3 (bypasses Lambda, useful for debugging)
	python scripts/replay_dlq.py --direct --max $(MAX) --region $(REGION)

replay-fifo: ## Replay FIFO DLQ (failed-messages.fifo) → FIFO main queue
	python scripts/replay_dlq.py --direct \
		--dlq-name failed-messages.fifo \
		--main-queue-name main-queue.fifo \
		--max $(MAX) --region $(REGION)

# ── CloudWatch Logs ───────────────────────────────────────────────────────────
# `aws logs tail --follow` streams new log events as they arrive.
# Press Ctrl-C to stop tailing.

logs-processor: ## Stream live logs from the message processor Lambda
	aws logs tail /aws/lambda/sqs-message-processor --follow --region $(REGION)

logs-dlq: ## Stream live logs from the DLQ monitor Lambda (shows failure analysis)
	aws logs tail /aws/lambda/sqs-dlq-monitor --follow --region $(REGION)

logs-replayer: ## Stream live logs from the DLQ replayer Lambda
	aws logs tail /aws/lambda/sqs-dlq-replayer --follow --region $(REGION)

# ── Full demo run ──────────────────────────────────────────────────────────────
# Chains: apply → send → monitor in sequence.
# Run `make replay` manually after the monitor reports the main queue is empty.

demo: apply send monitor ## Full demo: deploy infra, send 100 messages, then monitor
	@echo ""
	@echo "Main queue is empty. Run 'make replay' to drain any DLQ messages."
