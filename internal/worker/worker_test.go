package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestOpenRouterCategorization(t *testing.T) {
	var capturedAuth, capturedModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		capturedModel = body.Model

		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"role":    "assistant",
						"content": "```json\n{\"category\": \"cat_health\"}\n```",
					},
				},
			},
		})
	}))
	defer srv.Close()

	jevClient := ai.NewJevClient(
		srv.URL+"/api/v1/chat/completions",
		"or-test-key",
		srv.Client(),
		ai.WithProvider(ai.ProviderOpenRouter),
		ai.WithModel("jev/jev-decision-v1"),
	)

	if jevClient.Provider() != ai.ProviderOpenRouter {
		t.Fatalf("expected provider %q, got %q", ai.ProviderOpenRouter, jevClient.Provider())
	}

	categories := []ai.CategoryOption{
		{ID: "cat_shopping", Name: "Shopping", Keywords: []string{"mercadona"}},
		{ID: "cat_health", Name: "Health", Keywords: []string{"farmacia"}},
	}
	inputs := []ai.TransactionInput{
		{ID: "tx-or-1", Description: "CLINICA OFTALMOLOGICA", AmountCents: -9500},
	}

	res := jevClient.Classify(context.Background(), inputs, nil, categories)
	if res["tx-or-1"].CategoryID == nil || *res["tx-or-1"].CategoryID != "cat_health" || *res["tx-or-1"].CategorySource != ai.SourceJev {
		t.Fatalf("expected tx-or-1 to be classified via OpenRouter as cat_health, got %+v", res["tx-or-1"])
	}
	if capturedAuth != "Bearer or-test-key" {
		t.Fatalf("expected Authorization header 'Bearer or-test-key', got %q", capturedAuth)
	}
	if capturedModel != "jev/jev-decision-v1" {
		t.Fatalf("expected OpenRouter model 'jev/jev-decision-v1', got %q", capturedModel)
	}
}

func TestOptionalNotifications(t *testing.T) {
	disabledDisp := notify.NewDispatcherFromConfig(notify.Config{
		Enabled: false,
	})
	if disabledDisp.Enabled() || disabledDisp.EmailEnabled() || disabledDisp.PushEnabled() {
		t.Fatalf("expected all notification channels to be disabled when Enabled=false")
	}

	res := disabledDisp.Deliver(context.Background(), notify.NotificationRequest{
		UserID:   "u-1",
		AlertID:  "a-1",
		Type:     "budget_exceeded",
		Channels: []string{"email", "push"},
		Message:  "Budget exceeded",
	})
	if res["email"] != "skipped_disabled" || res["push"] != "skipped_disabled" {
		t.Fatalf("expected skipped_disabled for both channels, got %+v", res)
	}

	emailOnlyDisp := notify.NewDispatcherFromConfig(notify.Config{
		Enabled:  true,
		SMTPHost: "smtp.finperk.es",
		SMTPPort: 587,
		SMTPFrom: "noreply@finperk.es",
	})
	if !emailOnlyDisp.EmailEnabled() || emailOnlyDisp.PushEnabled() {
		t.Fatalf("expected EmailEnabled=true and PushEnabled=false when only SMTP is configured")
	}
	res2 := emailOnlyDisp.Deliver(context.Background(), notify.NotificationRequest{
		UserID:   "u-1",
		AlertID:  "a-2",
		Type:     "budget_threshold",
		Channels: []string{"email", "push"},
		Message:  "Budget 80%%",
	})
	if res2["email"] != "sent" || res2["push"] != "skipped_unconfigured" {
		t.Fatalf("expected email=sent and push=skipped_unconfigured, got %+v", res2)
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

func TestEndToEndExecuteSyncPipelineAndBudgetAlerts(t *testing.T) {
	now := time.Now().UTC()
	dateStr := now.Format("2006-01-02")
	period := now.Format("2006-01")

	// Mock GoCardless API server
	gcSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("HTTP_X_RATELIMIT_LIMIT", "4")
		w.Header().Set("HTTP_X_RATELIMIT_REMAINING", "3")
		w.Header().Set("HTTP_X_RATELIMIT_ACCOUNT_SUCCESS_RESET", "21600")

		switch {
		case r.URL.Path == "/token/new/":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access":          "gc-access-tok",
				"access_expires":  86400,
				"refresh":         "gc-refresh-tok",
				"refresh_expires": 2592000,
			})
		case strings.HasSuffix(r.URL.Path, "/balances/"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"balances": []map[string]any{
					{
						"balanceAmount": map[string]string{"amount": "2450.75", "currency": "EUR"},
						"balanceType":   "closingBooked",
					},
				},
			})
		case strings.HasSuffix(r.URL.Path, "/transactions/"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"transactions": map[string]any{
					"booked": []map[string]any{
						{
							"transactionId":                     "tx_booked_salary",
							"bookingDate":                       dateStr,
							"transactionAmount":                 map[string]string{"amount": "2500.00", "currency": "EUR"},
							"remittanceInformationUnstructured": "NOMINA EMPRESA SL",
						},
						{
							"transactionId":                     "tx_booked_mercadona",
							"bookingDate":                       dateStr,
							"transactionAmount":                 map[string]string{"amount": "-85.00", "currency": "EUR"},
							"remittanceInformationUnstructured": "COMPRA MERCADONA",
						},
						{
							"transactionId":                     "tx_booked_internal",
							"bookingDate":                       dateStr,
							"transactionAmount":                 map[string]string{"amount": "-500.00", "currency": "EUR"},
							"remittanceInformationUnstructured": "TRASPASO PROPIO A CUENTA AHORRO",
						},
					},
					"pending": []map[string]any{
						{
							// Pending without transactionId -> uses DeterministicPendingID
							"valueDate":                         dateStr,
							"transactionAmount":                 map[string]string{"amount": "-12.50", "currency": "EUR"},
							"remittanceInformationUnstructured": "CAFE PENDIENTE",
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected GoCardless call: %s", r.URL.Path)
		}
	}))
	defer gcSrv.Close()

	// Mock Jev server
	jevSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"category": "cat_others"})
	}))
	defer jevSrv.Close()

	store := repository.NewMemoryWorkerStore()
	store.SeedAccount(repository.ActiveAccount{
		ID:                  "acc-1",
		UserID:              "user-1",
		IBAN:                "ES9121000418450200051332",
		GoCardlessAccountID: "gc-acc-1",
		Status:              "enabled",
	})
	// Seed a budget of 10000 cents (100.00 EUR) for cat_shopping -> -8500 cents is 85% -> triggers budget_threshold
	store.SeedBudget(repository.BudgetRecord{
		ID:         "bud-1",
		UserID:     "user-1",
		CategoryID: "cat_shopping",
		Amount:     10000,
	})

	gcClient := gocardless.NewClient("sec-id", "sec-key", gocardless.WithBaseURL(gcSrv.URL), gocardless.WithHTTPClient(gcSrv.Client()))
	jevClient := ai.NewJevClient(jevSrv.URL, "jev-key", jevSrv.Client())
	eng := NewEngine(gcClient, jevClient, notify.NewDispatcher("noreply@finperk.app"), 12*time.Hour).WithStore(store)

	report, err := eng.ExecuteSync(context.Background(), SyncRequestPayload{
		SyncID:              "sync-100",
		UserID:              "user-1",
		AccountID:           "acc-1",
		GoCardlessAccountID: "gc-acc-1",
	})
	if err != nil {
		t.Fatalf("ExecuteSync failed: %v", err)
	}
	if report.BalanceCents != 245075 {
		t.Fatalf("expected balance 245075 cents, got %d", report.BalanceCents)
	}
	if report.TransactionsFetched != 4 || report.TransactionsNew != 4 {
		t.Fatalf("expected 4 fetched & 4 new transactions, got %+v", report)
	}
	if report.AlertsCreated != 1 {
		t.Fatalf("expected 1 budget_threshold alert created, got %d", report.AlertsCreated)
	}

	// Verify spending summary excludes both pending (-1250) and internal transfer (-50000) from TotalExpenses
	sum := store.GetSpendingSummary("user-1", period)
	if sum == nil {
		t.Fatalf("expected spending summary for period %s", period)
	}
	if sum.TotalIncome != 250000 {
		t.Fatalf("expected TotalIncome 250000 cents, got %d", sum.TotalIncome)
	}
	if sum.TotalExpenses != 8500 {
		t.Fatalf("expected TotalExpenses 8500 cents (excluding internal transfer & pending), got %d", sum.TotalExpenses)
	}
}
