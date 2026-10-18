# AGENTS.md — backend-worker

## Overview

`backend-worker` is Finperk's background service (Go 1.26+). It consumes async jobs from NATS JetStream (`finperk.sync.request` and `finperk.notify.send`), runs a periodic 12h scheduler for active bank accounts, fetches movements and balances from GoCardless, deduplicates and reconciles pending-to-booked transactions, detects internal transfers, classifies transactions into global categories using a **3-tier pipeline powered by Jev**, persists everything atomically in MongoDB, evaluates budget/consent alerts, and delivers email (SMTP) and push (FCM/APNs) notifications.

## Architecture & Packages

```
backend-worker/
├── cmd/worker/main.go             # Entrypoint, JetStream consumers, 12h scheduler, healthz/readyz
└── internal/
    ├── ai/jev.go                  # Jev decision/classification client + 3-tier categorization pipeline
    ├── config/config.go           # Env configuration loader
    ├── dedup/dedup.go             # Pending->booked reconciliation, fuzzy match, internal transfer detection
    ├── notify/notify.go           # SMTP email + FCM/APNs push dispatcher
    ├── repository/repository.go   # MongoDB ACID persistence, user manual history lookup, alert dedup
    └── worker/worker.go           # Sync orchestration, per-account lock, rate-limit backoff
```

## Key Architectural Rules

1. **Monetary Amounts in Cents (`int64`)**:
   - All transaction amounts, account balances, spending summaries, and budget thresholds are integer cents (`int64`). Convert GoCardless string amounts via `gocardless.ParseAmountToCents`.
2. **Per-Account Concurrency Lock & Rate Limit Backoff**:
   - Never run two concurrent syncs for the same `account_id`. If locked, NAK the NATS message with delay.
   - Regular syncs only call GoCardless `transactions` and `balances` endpoints (never `details`, preserving the ~4/day per-endpoint rate limit).
   - Always persist `next_allowed_sync_at` from `HTTP_X_RATELIMIT_ACCOUNT_SUCCESS_RESET`. On HTTP `429`, NAK with delay until reset.
3. **Deduplication & Pending-to-Booked Reconciliation** (`internal/dedup`):
   - **Booked**: Dedup by `gocardless_id`. When a new booked transaction arrives, reconcile and delete any previously stored `is_pending: true` transaction matching either by `gocardless_id` or by **fuzzy match** (`account_id` + same `amount` + `booking_date` within $\pm 4$ days + normalized description prefix).
   - **Pending**: Use `gocardless.DeterministicPendingID` if GoCardless omits `transactionId`. Pending transactions are stored with `is_pending: true` and **excluded** from `spending_summaries` and budget alerts until booked.
4. **Internal Transfer Detection** (`internal/dedup`):
   - Detect mirror transactions across two accounts belonging to the same user (`amount_A == -amount_B` within $\pm 2$ days) or matching self-transfer patterns (`traspaso propio`, `bizum a mi mismo`). Mark `is_internal_transfer: true` and assign category `transferencias` so they do not inflate income/expense totals.
5. **3-Tier Categorization Pipeline with Jev (Native or OpenRouter)** (`internal/ai/jev.go`):
   - **Tier 1 (`manual`)**: Exact match on normalized description or creditor against the user's prior manual recategorizations in MongoDB (`category_source: "manual"`).
   - **Tier 2 (`keyword`)**: Deterministic match against global category `keywords` (`category_source: "keyword"`).
   - **Tier 3 (`jev`)**: Calls **Jev** either via its native decisions API (`JEV_PROVIDER=jev`) or via **OpenRouter** (`JEV_PROVIDER=openrouter`, `JEV_MODEL`, `https://openrouter.ai/api/v1/chat/completions`) passing constrained global category IDs. Stores `category_id: nil` for retry if unreachable.
6. **Atomic MongoDB Persistence, Alert Deduplication & Optional Notifications** (`internal/repository`, `internal/notify`):
   - Save new transactions, remove reconciled pending transactions, update account balance, and rebuild `spending_summaries` inside a single MongoDB ACID session.
   - Budget alerts (`budget_threshold` at 80%, `budget_exceeded` at 100%) are deduplicated by `user_id + budget_id + type + period (YYYY-MM)` and always saved in MongoDB for in-app viewing.
   - External notifications (SMTP email, FCM Android, APNs iOS) are strictly optional (`NOTIFICATIONS_ENABLED=false` by default) and each channel activates only if its credentials are provided.

## Commands

```bash
make test    # go test -v -race ./...
make vet     # go vet ./...
make fmt     # gofmt -s -w .
make lint    # golangci-lint run ./...
make run     # go run ./cmd/worker/main.go
```
