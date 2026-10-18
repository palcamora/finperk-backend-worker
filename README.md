# Finperk — backend-worker

Go background worker and periodic scheduler for **Finperk**.

## Overview

`backend-worker` handles asynchronous bank synchronization, AI transaction classification, and notification delivery:
- **NATS JetStream Consumers**:
  - `finperk-sync` (`finperk.sync.request`): Processes manual and scheduled bank account syncs with per-account locking and GoCardless rate-limit backoff.
  - `finperk-notify` (`finperk.notify.send`): Delivers email (SMTP) and push (FCM/APNs) notifications.
- **Periodic Scheduler**: Wakes up on a configurable interval (`SYNC_SCHEDULER_INTERVAL`, default `12h`) and enqueues sync jobs for active accounts whose `next_allowed_sync_at` has passed, plus checks for PSD2 consents expiring within 7 days.
- **Pending Reconciliation & Internal Transfers**: Reconciles `pending` into `booked` movements (via exact `gocardless_id` or $\pm 4$-day fuzzy match) and marks self-transfers (`is_internal_transfer: true`) so they don't distort budgets.
- **3-Tier Categorization with Jev**:
  1. User's own historical manual rules (`categorization_source: "manual"`)
  2. Global category keyword matching (`categorization_source: "keyword"`)
  3. **Jev** calibrated decision/classification model (`categorization_source: "jev"`)

## Prerequisites

- **Go** 1.26+
- **MongoDB 7.0+** running as a **Replica Set** (`rs0`)
- **NATS 2.10+** with **JetStream** enabled

## Environment Variables

Copy `.env.example` to `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `HEALTH_PORT` | `8081` | HTTP port for `/healthz` and `/readyz` |
| `MONGODB_URI` | `mongodb://localhost:27017/finperk?replicaSet=rs0` | MongoDB Replica Set URI |
| `MONGODB_DB` | `finperk` | Database name |
| `NATS_URL` | `nats://localhost:4222` | NATS JetStream URL |
| `GOCARDLESS_SECRET_ID` | — | GoCardless API Secret ID |
| `GOCARDLESS_SECRET_KEY` | — | GoCardless API Secret Key |
| `JEV_API_URL` | `https://api.typesafe.ai` | Jev classification API base URL |
| `JEV_API_KEY` | — | Jev API key |
| `SYNC_SCHEDULER_INTERVAL` | `12h` | Cron interval for automatic account syncs |
| `SMTP_HOST` / `SMTP_PORT` | `localhost` / `1025` | SMTP server for email alerts |

## Development Commands

```bash
make run     # Start background worker (health probes on :8081)
make test    # Run unit tests with race detector
make vet     # Run go vet
make fmt     # Format code with gofmt
make lint    # Run golangci-lint
```
