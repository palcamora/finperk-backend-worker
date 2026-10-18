package repository

import (
	"context"
	"time"
)

// ActiveAccount represents an account eligible for scheduled sync or PSD2 consent expiry checks.
type ActiveAccount struct {
	ID                      string
	UserID                  string
	GoCardlessRequisitionID string
	GoCardlessAccountID     string
	Status                  string
	AccessExpiresAt         time.Time
	LastSyncAt              time.Time
	RemainingCalls          int
}

// AccountStore defines the database operations required by the worker scheduler and sync pipeline.
type AccountStore interface {
	ListActiveAccounts(ctx context.Context) ([]ActiveAccount, error)
	MarkAccountExpired(ctx context.Context, accountID string) error
}
