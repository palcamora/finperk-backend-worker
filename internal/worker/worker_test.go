package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"backend-worker/internal/ai"
	"backend-worker/internal/dedup"
	"backend-worker/internal/notify"
	"backend-worker/internal/repository"
	"pkg/gocardless"
)

func TestPerAccountConcurrencyLock(t *testing.T) {
	eng := NewEngine(
		gocardless.NewClient("id", "key"),
		ai.NewJevClient("", "", nil),
		notify.NewDispatcher("noreply@finperk.app"),
		12*time.Hour,
	)

	req := SyncRequestPayload{
		SyncID:    "sync-1",
		UserID:    "user-1",
		AccountID: "acc-1",
	}

	err := eng.ProcessSyncRequest(context.Background(), req, func(ctx context.Context, _ SyncRequestPayload) error {
		// Attempt a concurrent sync for the same account while the first is in progress
		nestedErr := eng.ProcessSyncRequest(ctx, req, nil)
		if !errors.Is(nestedErr, ErrAccountSyncInProgress) {
			t.Fatalf("expected ErrAccountSyncInProgress, got %v", nestedErr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer ProcessSyncRequest failed: %v", err)
	}
}

func TestThreeTierJevCategorization(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"category": "cat_health",
		})
	}))
	defer srv.Close()

	jevClient := ai.NewJevClient(srv.URL, "test-jev-key", srv.Client())

	categories := []ai.CategoryOption{
		{ID: "cat_shopping", Name: "Shopping", Keywords: []string{"mercadona", "carrefour"}},
		{ID: "cat_leisure", Name: "Leisure", Keywords: []string{"netflix"}},
		{ID: "cat_health", Name: "Health", Keywords: []string{"farmacia"}},
	}
	manualRules := map[string]string{
		"bizum juan": "cat_leisure",
	}
	inputs := []ai.TransactionInput{
		{ID: "tx-1", Description: "BIZUM JUAN", AmountCents: -1500},
		{ID: "tx-2", Description: "COMPRA MERCADONA MADRID", AmountCents: -4250},
		{ID: "tx-3", Description: "CLINICA DENTAL DR PEREZ", AmountCents: -6000},
	}

	res := jevClient.Classify(context.Background(), inputs, manualRules, categories)

	if res["tx-1"].CategoryID == nil || *res["tx-1"].CategoryID != "cat_leisure" || *res["tx-1"].CategorySource != ai.SourceManual {
		t.Fatalf("expected tx-1 to match Tier 1 manual rule, got %+v", res["tx-1"])
	}
	if res["tx-2"].CategoryID == nil || *res["tx-2"].CategoryID != "cat_shopping" || *res["tx-2"].CategorySource != ai.SourceKeyword {
		t.Fatalf("expected tx-2 to match Tier 2 keyword, got %+v", res["tx-2"])
	}
	if res["tx-3"].CategoryID == nil || *res["tx-3"].CategoryID != "cat_health" || *res["tx-3"].CategorySource != ai.SourceJev {
		t.Fatalf("expected tx-3 to be classified by Tier 3 Jev, got %+v", res["tx-3"])
	}
}

func TestFuzzyPendingReconciliation(t *testing.T) {
	day1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC)

	existing := []dedup.StoredTransaction{
		{
			ID:                      "mongo-pending-1",
			GoCardlessTransactionID: "pending_hash_123",
			Date:                    day1,
			AmountCents:             -2550,
			Description:             "MERCADONA",
			IsPending:               true,
		},
		{
			ID:                      "mongo-pending-2",
			GoCardlessTransactionID: "pending_hash_cancelled",
			Date:                    day1,
			AmountCents:             -9900,
			Description:             "HOTEL RESERVATION",
			IsPending:               true,
		},
	}

	incomingBooked := []dedup.IncomingTransaction{
		{
			GoCardlessTransactionID: "booked-definitive-id-999",
			Date:                    day2, // +2 days later
			AmountCents:             -2550,
			Description:             "MERCADONA PURCHASE MADRID",
			IsPending:               false,
		},
	}

	rec := dedup.Reconcile(existing, incomingBooked, nil)
	if len(rec.PromotedPendingToBook) != 1 {
		t.Fatalf("expected 1 pending transaction promoted to booked via fuzzy match, got %d", len(rec.PromotedPendingToBook))
	}
	if len(rec.DeletedPendingIDs) != 1 || rec.DeletedPendingIDs[0] != "mongo-pending-2" {
		t.Fatalf("expected cancelled pending transaction to be marked for deletion, got %+v", rec.DeletedPendingIDs)
	}
	if len(rec.NewTransactions) != 0 {
		t.Fatalf("expected 0 new transactions since booked matched pending, got %d", len(rec.NewTransactions))
	}
}

func TestPlanScheduledActions(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	accounts := []repository.ActiveAccount{
		{
			ID:              "acc-due-sync",
			Status:          "enabled",
			AccessExpiresAt: now.Add(30 * 24 * time.Hour),
			LastSyncAt:      now.Add(-14 * time.Hour),
			RemainingCalls:  3,
		},
		{
			ID:              "acc-expiring-soon",
			Status:          "enabled",
			AccessExpiresAt: now.Add(5 * 24 * time.Hour),
			LastSyncAt:      now.Add(-2 * time.Hour),
			RemainingCalls:  2,
		},
		{
			ID:              "acc-expired",
			Status:          "enabled",
			AccessExpiresAt: now.Add(-1 * time.Hour),
			LastSyncAt:      now.Add(-20 * time.Hour),
			RemainingCalls:  4,
		},
	}

	actions := PlanScheduledActions(now, accounts)
	if len(actions) != 3 {
		t.Fatalf("expected 3 scheduled actions, got %d", len(actions))
	}
	if !actions[0].TriggerSync {
		t.Fatalf("expected acc-due-sync to trigger sync")
	}
	if !actions[1].AlertExpiring {
		t.Fatalf("expected acc-expiring-soon to alert expiring PSD2 consent")
	}
	if !actions[2].MarkExpired {
		t.Fatalf("expected acc-expired to be marked expired")
	}
}
