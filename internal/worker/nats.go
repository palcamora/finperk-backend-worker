package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"backend-worker/internal/notify"
	"github.com/nats-io/nats.go"
	"pkg/gocardless"
)

const (
	StreamSync         = "FINPERK_SYNC"
	SubjectSyncRequest = "finperk.sync.request"
	ConsumerSyncWorker = "sync-worker"

	StreamNotify         = "FINPERK_NOTIFY"
	SubjectNotifyRequest = "finperk.notify.request"
	ConsumerNotifyWorker = "notify-worker"
)

// NATSWorkerBus manages the NATS JetStream consumers and publishers for backend-worker.
type NATSWorkerBus struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	syncSub   *nats.Subscription
	notifySub *nats.Subscription
}

// ConnectNATSBus connects to NATS, initializes JetStream streams, and returns a NATSWorkerBus.
func ConnectNATSBus(natsURL string) (*NATSWorkerBus, error) {
	nc, err := nats.Connect(natsURL,
		nats.Name("finperk-backend-worker"),
		nats.Timeout(5*time.Second),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}

	ensureStream(js, StreamSync, []string{"finperk.sync.*"})
	ensureStream(js, StreamNotify, []string{"finperk.notify.*"})

	return &NATSWorkerBus{nc: nc, js: js}, nil
}

func ensureStream(js nats.JetStreamContext, name string, subjects []string) {
	if _, err := js.StreamInfo(name); err != nil {
		_, _ = js.AddStream(&nats.StreamConfig{
			Name:      name,
			Subjects:  subjects,
			Retention: nats.WorkQueuePolicy,
			Storage:   nats.FileStorage,
			MaxAge:    7 * 24 * time.Hour,
		})
	}
}

// JetStream returns the underlying nats.JetStreamContext (used for the shared finperk-tokens KV bucket).
func (b *NATSWorkerBus) JetStream() nats.JetStreamContext {
	return b.js
}

// PublishSyncRequest publishes a scheduled sync job to finperk.sync.request.
func (b *NATSWorkerBus) PublishSyncRequest(ctx context.Context, req SyncRequestPayload) error {
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = b.js.Publish(SubjectSyncRequest, raw, nats.Context(ctx))
	return err
}

// StartConsumers subscribes durable JetStream consumers for sync and notification jobs.
func (b *NATSWorkerBus) StartConsumers(ctx context.Context, eng *Engine) error {
	syncSub, err := b.js.QueueSubscribe(
		SubjectSyncRequest,
		ConsumerSyncWorker,
		func(msg *nats.Msg) {
			var payload SyncRequestPayload
			if err := json.Unmarshal(msg.Data, &payload); err != nil {
				slog.Error("invalid sync request payload", "error", err)
				_ = msg.Term()
				return
			}

			err := eng.ProcessSyncRequest(ctx, payload, nil)
			if err != nil {
				if errors.Is(err, ErrAccountSyncInProgress) {
					slog.Warn("sync already in progress for account, NAKing with 30s delay", "account_id", payload.AccountID)
					_ = msg.NakWithDelay(30 * time.Second)
					return
				}
				var apiErr *gocardless.APIError
				if errors.As(err, &apiErr) && apiErr.IsRateLimited() {
					delay := 1 * time.Hour
					if apiErr.RateLimits.AccountSuccessReset > 0 {
						delay = time.Duration(apiErr.RateLimits.AccountSuccessReset) * time.Second
					}
					slog.Warn("GoCardless rate limited (429), NAKing with reset delay", "account_id", payload.AccountID, "delay", delay.String())
					_ = msg.NakWithDelay(delay)
					return
				}
				slog.Error("sync execution failed", "account_id", payload.AccountID, "error", err)
			}
			_ = msg.Ack()
		},
		nats.Durable(ConsumerSyncWorker),
		nats.ManualAck(),
		nats.AckWait(2*time.Minute),
		nats.MaxDeliver(5),
	)
	if err != nil {
		return err
	}
	b.syncSub = syncSub

	notifySub, err := b.js.QueueSubscribe(
		SubjectNotifyRequest,
		ConsumerNotifyWorker,
		func(msg *nats.Msg) {
			var req notify.NotificationRequest
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				_ = msg.Term()
				return
			}
			eng.notifier.Deliver(ctx, req)
			_ = msg.Ack()
		},
		nats.Durable(ConsumerNotifyWorker),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(3),
	)
	if err != nil {
		return err
	}
	b.notifySub = notifySub

	return nil
}

// Close drains subscriptions and closes the NATS connection.
func (b *NATSWorkerBus) Close() {
	if b == nil {
		return
	}
	if b.syncSub != nil {
		_ = b.syncSub.Drain()
	}
	if b.notifySub != nil {
		_ = b.notifySub.Drain()
	}
	if b.nc != nil {
		_ = b.nc.Drain()
		b.nc.Close()
	}
}
