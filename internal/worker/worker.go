package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"backend-worker/internal/ai"
	"backend-worker/internal/notify"
	"backend-worker/internal/repository"
	"pkg/gocardless"
)

// ErrAccountSyncInProgress is returned when a sync is already running for the same account.
// The NATS JetStream consumer responds by sending a NAK with delay for redelivery.
var ErrAccountSyncInProgress = errors.New("worker: sync already in progress for this account")

// SyncRequestPayload represents the JSON payload on the finperk.sync.request subject.
type SyncRequestPayload struct {
	SyncID                  string `json:"sync_id"`
	UserID                  string `json:"user_id"`
	AccountID               string `json:"account_id"`
	GoCardlessRequisitionID string `json:"gocardless_requisition_id"`
	GoCardlessAccountID     string `json:"gocardless_account_id"`
}

// AccountLockManager manages per-account concurrency locks so two syncs for the same
// GoCardless account never run simultaneously.
type AccountLockManager struct {
	mu     sync.Mutex
	active map[string]bool
}

// NewAccountLockManager creates a new AccountLockManager.
func NewAccountLockManager() *AccountLockManager {
	return &AccountLockManager{
		active: make(map[string]bool),
	}
}

// TryLock attempts to acquire the lock for accountID. Returns false if already locked.
func (m *AccountLockManager) TryLock(accountID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active[accountID] {
		return false
	}
	m.active[accountID] = true
	return true
}

// Unlock releases the lock for accountID.
func (m *AccountLockManager) Unlock(accountID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.active, accountID)
}

// Engine coordinates NATS JetStream sync/notify consumers and the periodic 12h scheduler.
type Engine struct {
	gcClient          *gocardless.Client
	jevClient         *ai.JevClient
	notifier          *notify.Dispatcher
	locks             *AccountLockManager
	schedulerInterval time.Duration
}

// NewEngine constructs a new worker Engine.
func NewEngine(
	gcClient *gocardless.Client,
	jevClient *ai.JevClient,
	notifier *notify.Dispatcher,
	schedulerInterval time.Duration,
) *Engine {
	if schedulerInterval <= 0 {
		schedulerInterval = 12 * time.Hour
	}
	return &Engine{
		gcClient:          gcClient,
		jevClient:         jevClient,
		notifier:          notifier,
		locks:             NewAccountLockManager(),
		schedulerInterval: schedulerInterval,
	}
}

// ProcessSyncRequest handles an incoming finperk.sync.request message with per-account locking.
func (e *Engine) ProcessSyncRequest(ctx context.Context, req SyncRequestPayload, fn func(ctx context.Context, req SyncRequestPayload) error) error {
	if !e.locks.TryLock(req.AccountID) {
		return ErrAccountSyncInProgress
	}
	defer e.locks.Unlock(req.AccountID)

	slog.Info("processing account sync request",
		"sync_id", req.SyncID,
		"user_id", req.UserID,
		"account_id", req.AccountID,
	)
	if fn != nil {
		return fn(ctx, req)
	}
	return nil
}

// EvaluateScheduledAccount checks whether an active account needs a scheduled sync or a 90-day PSD2 consent alert.
type ScheduledAction struct {
	AccountID    string
	TriggerSync  bool
	MarkExpired  bool
	AlertExpiring bool
}

// PlanScheduledActions inspects active accounts for the periodic 12-hour worker scheduler.
func PlanScheduledActions(now time.Time, accounts []repository.ActiveAccount) []ScheduledAction {
	var actions []ScheduledAction
	for _, acc := range accounts {
		if acc.Status != "enabled" {
			continue
		}
		var act ScheduledAction
		act.AccountID = acc.ID

		// Check 90-day PSD2 consent expiration
		if !acc.AccessExpiresAt.IsZero() {
			if !now.Before(acc.AccessExpiresAt) {
				act.MarkExpired = true
				actions = append(actions, act)
				continue
			}
			if acc.AccessExpiresAt.Sub(now) <= 7*24*time.Hour {
				act.AlertExpiring = true
			}
		}

		// Schedule automatic sync if last sync was >= 12 hours ago and rate limit allows
		if (acc.LastSyncAt.IsZero() || now.Sub(acc.LastSyncAt) >= 12*time.Hour) && acc.RemainingCalls > 0 {
			act.TriggerSync = true
		}

		if act.TriggerSync || act.AlertExpiring || act.MarkExpired {
			actions = append(actions, act)
		}
	}
	return actions
}

// Run loop starts the background scheduler until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.schedulerInterval)
	defer ticker.Stop()

	slog.Info("finperk backend-worker started", "scheduler_interval", e.schedulerInterval.String())
	<-ctx.Done()
	slog.Info("finperk backend-worker stopped")
	return nil
}
