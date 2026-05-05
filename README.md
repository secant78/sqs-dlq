# Assignment 17: SQS Dead Letter Queue Processing

Build robust message processing with retry logic, dead letter queue handling, failure analysis, and a replay mechanism — all running on AWS Lambda + SQS + SNS.

---

## Assignment Requirements

### Task
Build robust message processing with retry and DLQ handling.

### What to Do
- Create standard SQS queue **main-queue**
- Create DLQ **failed-messages**
- Configure main queue with:
  - Max receive count: 3
  - DLQ redrive policy
  - Visibility timeout: 30 seconds
- Create Lambda consumer that:
  - Processes messages
  - Randomly fails 20% of messages (simulated)
  - Logs processing attempts
- Create second Lambda for DLQ monitoring:
  - Triggers when messages arrive in DLQ
  - Analyzes failure patterns
  - Logs to CloudWatch
  - Sends SNS alert
- Send 100 messages to main queue
- Monitor how many end up in DLQ
- Implement replay mechanism from DLQ
- Add message deduplication for FIFO queue version

### Success Criteria
- Failed messages move to DLQ after 3 attempts
- DLQ monitoring Lambda triggers correctly
- Can replay messages from DLQ
- All messages eventually processed successfully

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Standard Queue Flow                      │
│                                                                 │
│  send_messages.py                                               │
│       │                                                         │
│       ▼                                                         │
│  ┌──────────┐   attempt 1,2,3    ┌───────────────────────┐     │
│  │ main-    │ ──────────────────▶│  sqs-message-         │     │
│  │ queue    │                    │  processor (Lambda)   │     │
│  │          │◀── retry on fail ──│  20% random failure   │     │
│  └──────────┘                    └───────────────────────┘     │
│       │                                                         │
│       │ after 3 failures (redrive policy)                       │
│       ▼                                                         │
│  ┌──────────┐                    ┌───────────────────────┐     │
│  │ failed-  │ ──────────────────▶│  sqs-dlq-monitor      │     │
│  │ messages │   triggers on      │  (Lambda)             │     │
│  │  (DLQ)   │   new messages     │  analyse + SNS alert  │     │
│  └──────────┘                    └───────────────────────┘     │
│       │                                  │                      │
│       │ replay_dlq.py                    ▼                      │
│       │                          ┌───────────────┐             │
│       └─── sqs-dlq-replayer ────▶│  SNS Topic    │             │
│            (Lambda)              │  dlq-alerts   │             │
│                 │                └───────────────┘             │
│                 └──────────────▶ main-queue (retry)            │
└─────────────────────────────────────────────────────────────────┘
```

**Resources created:**

| Resource | Name | Purpose |
|---|---|---|
| SQS Queue | `main-queue` | Primary message intake |
| SQS Queue | `failed-messages` | Dead letter queue |
| SQS Queue | `main-queue.fifo` | FIFO variant with deduplication |
| SQS Queue | `failed-messages.fifo` | FIFO dead letter queue |
| Lambda | `sqs-message-processor` | Consumes main-queue; fails 20% randomly |
| Lambda | `sqs-dlq-monitor` | Triggered by DLQ; sends SNS alert |
| Lambda | `sqs-dlq-replayer` | Moves DLQ messages back to main-queue |
| SNS Topic | `dlq-alerts` | Alert channel for DLQ events |
| CloudWatch Alarm | `dlq-messages-visible` | Fires when DLQ depth > 0 |

---

## Prerequisites

- AWS CLI configured (`aws configure`)
- Terraform >= 1.5 installed
- Python >= 3.10 with `boto3` installed (`pip install boto3`)
- An AWS IAM user or role with permissions to create SQS, Lambda, SNS, IAM, and CloudWatch resources

---

## Step 1 — Clone the repository

```bash
git clone https://github.com/secant78/sqs-dlq.git
cd sqs-dlq
```

---

## Step 2 — Deploy infrastructure with Terraform

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

Terraform will create both queues, all three Lambda functions, the SNS topic, the CloudWatch alarm, and the IAM role. Confirm with `yes` when prompted.

**Optional — receive email alerts when messages hit the DLQ:**

```bash
terraform apply -var='alert_email=you@example.com'
```

You will receive a subscription confirmation email from AWS; click the link to activate it.

**Optional — change the failure rate (default 20 %):**

```bash
terraform apply -var='failure_rate=0.5'   # 50% failure rate
```

After apply, verify the outputs:

```bash
terraform output
```

Expected output:

```
dlq_monitor_function_name   = "sqs-dlq-monitor"
dlq_url                     = "https://sqs.us-east-1.amazonaws.com/..."
main_queue_url               = "https://sqs.us-east-1.amazonaws.com/..."
processor_function_name      = "sqs-message-processor"
replayer_function_name       = "sqs-dlq-replayer"
sns_topic_arn                = "arn:aws:sns:us-east-1:..."
```

---

## Step 3 — Send 100 messages to the main queue

```bash
cd ..   # back to project root
python scripts/send_messages.py
```

The script sends messages in batches of 10 and prints progress:

```
Target queue : https://sqs.us-east-1.amazonaws.com/.../main-queue
Messages     : 100
FIFO         : False

  Batch [  1–10 ]  sent=10  failed=0  cumulative=10
  Batch [ 11–20 ]  sent=10  failed=0  cumulative=20
  ...
  Batch [ 91–100]  sent=10  failed=0  cumulative=100

────────────────────────────────────────
Total sent  : 100
Total failed: 0
```

**Options:**

```bash
python scripts/send_messages.py --count 50          # send 50 messages
python scripts/send_messages.py --fifo              # send to FIFO queue
python scripts/send_messages.py --region eu-west-1  # different region
```

---

## Step 4 — Monitor queue depths in real time

Open a second terminal and run:

```bash
python scripts/monitor_queues.py
```

This polls every 10 seconds and prints a live table:

```
──────────────────────────────────────────────────────────────
  Queue depths  —  14:32:05 UTC
──────────────────────────────────────────────────────────────
  Queue                           Visible   In-flight
  ──────────────────────────────  ────────  ─────────
  Main Queue                           73          10
  DLQ (failed-messages)                 0           0
  Main FIFO                             0           0
  DLQ FIFO                              0           0
──────────────────────────────────────────────────────────────
```

Once the main queue drains, the monitor reports how many messages ended up in the DLQ:

```
  ✓ Main queues empty. DLQ contains 1 message(s).
  ⚠ Run the replayer to move DLQ messages back:
    python scripts/replay_dlq.py
```

**Single snapshot:**

```bash
python scripts/monitor_queues.py --once
```

---

## Step 5 — Observe the DLQ monitor Lambda

Each time messages land in the DLQ, `sqs-dlq-monitor` fires automatically and:

1. Logs a structured JSON failure analysis to CloudWatch
2. Publishes a formatted SNS alert

**View the logs:**

```bash
aws logs tail /aws/lambda/sqs-dlq-monitor --follow
```

Sample log output:

```json
{
  "event": "dlq_batch_received",
  "timestamp": "2025-01-15T14:32:44Z",
  "message_count": 1,
  "analysis": {
    "total_messages": 1,
    "receive_count_avg": 3.0,
    "receive_count_max": 3,
    "oldest_message_age_seconds": 94,
    "error_types": { "simulated_random_failure": 1 },
    "replayed_count": 0
  }
}
```

**View the processor logs** to see every processing attempt:

```bash
aws logs tail /aws/lambda/sqs-message-processor --follow
```

You will see entries for each of the three attempts before a message is sent to the DLQ:

```json
{ "event": "processing_attempt", "message_id": "abc123", "receive_count": 1 }
{ "event": "processing_failure",  "message_id": "abc123", "receive_count": 1, "will_go_to_dlq": false }
{ "event": "processing_attempt", "message_id": "abc123", "receive_count": 2 }
{ "event": "processing_failure",  "message_id": "abc123", "receive_count": 2, "will_go_to_dlq": false }
{ "event": "processing_attempt", "message_id": "abc123", "receive_count": 3 }
{ "event": "processing_failure",  "message_id": "abc123", "receive_count": 3, "will_go_to_dlq": true }
```

---

## Step 6 — Replay messages from the DLQ

Once the main queue is empty, replay any messages that ended up in the DLQ:

```bash
python scripts/replay_dlq.py
```

This invokes `sqs-dlq-replayer` synchronously and prints the result:

```json
{
  "replayed": 1,
  "errors": 0,
  "dry_run": false,
  "timestamp": "2025-01-15T14:35:01Z"
}
```

The replayer:
- Reads messages from `failed-messages`
- Adds `_replayed_at` and `_original_message_id` metadata to the body
- Re-sends to `main-queue`
- Deletes from the DLQ only after a successful send

The processor then retries the replayed messages. Because the 20% failure rate applies on every attempt, most will succeed on the replay pass.

**Dry-run first (recommended before large replays):**

```bash
python scripts/replay_dlq.py --dry-run
```

**Replay without Lambda (direct boto3):**

```bash
python scripts/replay_dlq.py --direct --max 50
```

**Reset metadata so messages look fresh to the processor:**

```bash
python scripts/replay_dlq.py --reset-attempts
```

---

## Step 7 — Test the FIFO queue with deduplication

```bash
# Send 100 messages to the FIFO queue
python scripts/send_messages.py --fifo

# Try sending the same 100 messages again within 5 minutes
# SQS will silently deduplicate them — nothing new enters the queue
python scripts/send_messages.py --fifo

# Monitor
python scripts/monitor_queues.py --once

# Replay FIFO DLQ → FIFO main queue
python scripts/replay_dlq.py --direct \
    --dlq-name failed-messages.fifo \
    --main-queue-name main-queue.fifo
```

FIFO queues add two guarantees:
- **Ordering** — messages within a `MessageGroupId` are processed in the order they were sent
- **Deduplication** — identical messages sent within a 5-minute window are delivered only once (using content-based SHA-256 deduplication)

---

## Step 8 — Verify success criteria

| Criterion | How to verify |
|---|---|
| Failed messages move to DLQ after 3 attempts | Check processor logs for `"receive_count": 3, "will_go_to_dlq": true`; confirm DLQ depth > 0 in monitor |
| DLQ monitoring Lambda triggers correctly | Check CloudWatch Logs for `/aws/lambda/sqs-dlq-monitor`; SNS email (if configured) |
| Can replay messages from DLQ | Run `replay_dlq.py`; DLQ depth returns to 0; messages reappear in main-queue |
| All messages eventually processed | After one replay pass, all messages succeed (monitor shows both queues at 0) |

---

## Makefile shortcuts

```bash
make apply          # terraform apply
make send           # send 100 messages
make send COUNT=50  # send 50 messages
make send-fifo      # send to FIFO queue
make monitor        # live depth dashboard
make monitor-once   # single snapshot
make replay         # replay DLQ via Lambda
make replay-dry     # dry-run preview
make replay-direct  # replay via direct boto3
make replay-fifo    # replay FIFO DLQ
make logs-processor # tail processor logs
make logs-dlq       # tail DLQ monitor logs
make logs-replayer  # tail replayer logs
make destroy        # tear everything down
```

---

## Tear down

```bash
cd terraform
terraform destroy
```

This removes all queues, Lambda functions, the SNS topic, the CloudWatch alarm, and the IAM role.
