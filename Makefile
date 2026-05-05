REGION   ?= us-east-1
COUNT    ?= 100
MAX      ?= 1000
TF_DIR   := terraform

.PHONY: init plan apply destroy send monitor replay replay-dry replay-direct \
        replay-fifo logs-processor logs-dlq logs-replayer help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Terraform ──────────────────────────────────────────────────────────────────

init: ## terraform init
	cd $(TF_DIR) && terraform init

plan: ## terraform plan
	cd $(TF_DIR) && terraform plan

apply: ## terraform apply (deploys all infrastructure)
	cd $(TF_DIR) && terraform apply

destroy: ## terraform destroy (tears everything down)
	cd $(TF_DIR) && terraform destroy

# ── Workflow steps ─────────────────────────────────────────────────────────────

send: ## Send COUNT messages to main-queue (default: 100)
	python scripts/send_messages.py --count $(COUNT) --region $(REGION)

send-fifo: ## Send COUNT messages to main-queue.fifo
	python scripts/send_messages.py --count $(COUNT) --fifo --region $(REGION)

monitor: ## Live queue-depth dashboard (Ctrl-C to stop)
	python scripts/monitor_queues.py --interval 5 --region $(REGION)

monitor-once: ## Single queue-depth snapshot
	python scripts/monitor_queues.py --once --region $(REGION)

replay: ## Replay all DLQ messages via the replayer Lambda
	python scripts/replay_dlq.py --max $(MAX) --region $(REGION)

replay-dry: ## Dry-run replay — log what would happen without moving messages
	python scripts/replay_dlq.py --dry-run --region $(REGION)

replay-direct: ## Replay directly via boto3 (bypasses Lambda)
	python scripts/replay_dlq.py --direct --max $(MAX) --region $(REGION)

replay-fifo: ## Replay FIFO DLQ → FIFO main queue (direct)
	python scripts/replay_dlq.py --direct \
		--dlq-name failed-messages.fifo \
		--main-queue-name main-queue.fifo \
		--max $(MAX) --region $(REGION)

# ── CloudWatch Logs ───────────────────────────────────────────────────────────

logs-processor: ## Tail processor Lambda logs
	aws logs tail /aws/lambda/sqs-message-processor --follow --region $(REGION)

logs-dlq: ## Tail DLQ monitor Lambda logs
	aws logs tail /aws/lambda/sqs-dlq-monitor --follow --region $(REGION)

logs-replayer: ## Tail replayer Lambda logs
	aws logs tail /aws/lambda/sqs-dlq-replayer --follow --region $(REGION)

# ── Full demo run ──────────────────────────────────────────────────────────────

demo: apply send monitor ## Deploy infra, send 100 messages, then monitor
	@echo "Run 'make replay' once the main queue is empty to drain the DLQ."
