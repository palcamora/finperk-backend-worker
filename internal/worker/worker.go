package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"backend-worker/internal/ai"
	"backend-worker/internal/dedup"
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

// SyncExecutionReport summarizes the result of a completed sync pipeline run.
type SyncExecutionReport struct {
	TransactionsFetched int
	TransactionsNew     int
	PromotedCount       int
	DeletedPendingCount int
	BalanceCents        int64
	AlertsCreated       int
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
	store             repository.WorkerStore
	bus               *NATSWorkerBus
	locks             *AccountLockManager
	schedulerInterval time.Duration
	nowFunc           func() time.Time
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
		store:             repository.NewMemoryWorkerStore(),
		locks:             NewAccountLockManager(),
		schedulerInterval: schedulerInterval,
		nowFunc:           func() time.Time { return time.Now().UTC() },
	}
}

// WithStore attaches a custom WorkerStore to the Engine.
func (e *Engine) WithStore(store repository.WorkerStore) *Engine {
	if store != nil {
		e.store = store
	}
	return e
}

// WithNATSBus attaches a NATSWorkerBus to the Engine.
func (e *Engine) WithNATSBus(bus *NATSWorkerBus) *Engine {
	if bus != nil {
		e.bus = bus
	}
	return e
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
	_, err := e.ExecuteSync(ctx, req)
	return err
}

// ExecuteSync runs the complete GoCardless -> Dedup -> InternalTransfer -> 3-Tier Jev -> Atomic DB -> Alerts pipeline.
// Note: Regular syncs only call /balances/ and /transactions/ (never /details/) to conserve GoCardless rate limits.
func (e *Engine) ExecuteSync(ctx context.Context, req SyncRequestPayload) (*SyncExecutionReport, error) {
	now := e.nowFunc()

	var balances []gocardless.Balance
	var balRL gocardless.RateLimitHeaders
	var txList *gocardless.TransactionsList
	var txRL gocardless.RateLimitHeaders

	if strings.HasPrefix(req.GoCardlessAccountID, "gc_acc_") {
		balances, balRL, txList, txRL = buildSandboxSyncData(now)
	} else {
		var err error
		balances, balRL, err = e.gcClient.GetBalances(ctx, req.GoCardlessAccountID)
		if err != nil {
			e.handleSyncError(ctx, req, err)
			return nil, err
		}
		txList, txRL, err = e.gcClient.GetTransactions(ctx, req.GoCardlessAccountID, "", "")
		if err != nil {
			e.handleSyncError(ctx, req, err)
			return nil, err
		}
	}

	var balanceCents int64
	for _, b := range balances {
		if cents, parseErr := gocardless.ParseAmountToCents(b.BalanceAmount.Amount); parseErr == nil {
			balanceCents = cents
			if b.BalanceType == "closingBooked" || b.BalanceType == "expected" {
				break
			}
		}
	}

	incomingBooked := make([]dedup.IncomingTransaction, 0, len(txList.Booked))
	for _, b := range txList.Booked {
		incomingBooked = append(incomingBooked, mapGoCardlessTransaction(req.AccountID, b, false, now))
	}

	incomingPending := make([]dedup.IncomingTransaction, 0, len(txList.Pending))
	for _, p := range txList.Pending {
		incomingPending = append(incomingPending, mapGoCardlessTransaction(req.AccountID, p, true, now))
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("sync transactions fetched",
			"account_id", req.AccountID,
			"booked_count", len(txList.Booked),
			"pending_count", len(txList.Pending),
			"balance_cents", balanceCents,
		)
	}

	// 3. Reconcile against existing transactions in DB (exact ID + ±3 day fuzzy match for pending -> booked)
	existing, err := e.store.ListAccountTransactions(ctx, req.AccountID)
	if err != nil {
		return nil, err
	}
	rec := dedup.Reconcile(existing, incomingBooked, incomingPending)

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("sync deduplication completed",
			"account_id", req.AccountID,
			"new_count", len(rec.NewTransactions),
			"promoted_count", len(rec.PromotedPendingToBook),
			"deleted_pending_count", len(rec.DeletedPendingIDs),
		)
	}

	// 4. Detect internal transfers and classify remaining transactions via 3-tier pipeline (Manual -> Keyword -> Jev)
	userIBANs, _ := e.store.ListUserIBANs(ctx, req.UserID)
	manualRules, _ := e.store.ListManualRules(ctx, req.UserID)
	categories, _ := e.store.ListCategories(ctx)

	var toClassify []ai.TransactionInput
	for i, nt := range rec.NewTransactions {
		if nt.IsPending || dedup.IsInternalTransfer(nt, userIBANs) {
			continue
		}
		toClassify = append(toClassify, ai.TransactionInput{
			ID:          fmt.Sprintf("new_%d", i),
			Description: nt.Description,
			Creditor:    nt.CreditorName,
			AmountCents: nt.AmountCents,
		})
	}
	for storedID, pt := range rec.PromotedPendingToBook {
		if dedup.IsInternalTransfer(pt, userIBANs) {
			continue
		}
		toClassify = append(toClassify, ai.TransactionInput{
			ID:          "prom_" + storedID,
			Description: pt.Description,
			Creditor:    pt.CreditorName,
			AmountCents: pt.AmountCents,
		})
	}

	classifications := e.jevClient.Classify(ctx, toClassify, manualRules, categories)

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("sync classification completed",
			"account_id", req.AccountID,
			"requested_count", len(toClassify),
			"classified_count", len(classifications),
		)
	}

	catTransfers := "cat_transfers"
	srcKeyword := ai.SourceKeyword

	classifiedNew := make([]repository.ClassifiedTransaction, 0, len(rec.NewTransactions))
	for i, nt := range rec.NewTransactions {
		ct := repository.ClassifiedTransaction{Incoming: nt}
		if !nt.IsPending {
			if dedup.IsInternalTransfer(nt, userIBANs) {
				ct.IsInternalTransfer = true
				ct.CategoryID = &catTransfers
				ct.CategorySource = &srcKeyword
			} else if res, ok := classifications[fmt.Sprintf("new_%d", i)]; ok {
				ct.CategoryID = res.CategoryID
				ct.CategorySource = res.CategorySource
				if ct.CategoryID != nil && *ct.CategoryID == "cat_transfers" {
					ct.IsInternalTransfer = true
				}
			}
		}
		classifiedNew = append(classifiedNew, ct)
	}

	classifiedPromoted := make(map[string]repository.ClassifiedTransaction, len(rec.PromotedPendingToBook))
	for storedID, pt := range rec.PromotedPendingToBook {
		ct := repository.ClassifiedTransaction{Incoming: pt}
		if dedup.IsInternalTransfer(pt, userIBANs) {
			ct.IsInternalTransfer = true
			ct.CategoryID = &catTransfers
			ct.CategorySource = &srcKeyword
		} else if res, ok := classifications["prom_"+storedID]; ok {
			ct.CategoryID = res.CategoryID
			ct.CategorySource = res.CategorySource
			if ct.CategoryID != nil && *ct.CategoryID == "cat_transfers" {
				ct.IsInternalTransfer = true
			}
		}
		classifiedPromoted[storedID] = ct
	}

	// 5. Persist everything atomically in MongoDB session
	totalFetched := len(incomingBooked) + len(incomingPending)
	outcome := repository.SyncOutcome{
		SyncID:                 req.SyncID,
		UserID:                 req.UserID,
		AccountID:              req.AccountID,
		BalanceCents:           balanceCents,
		TransactionsFetched:    totalFetched,
		TransactionsRateLimits: txRL,
		BalancesRateLimits:     balRL,
		NewTransactions:        classifiedNew,
		PromotedPendingToBook:  classifiedPromoted,
		IncrementedPendingIDs:  rec.IncrementedPendingIDs,
		DeletedPendingIDs:      rec.DeletedPendingIDs,
		CompletedAt:            now,
	}
	if err := e.store.ApplySyncOutcome(ctx, outcome); err != nil {
		return nil, err
	}

	// 6. Evaluate budget thresholds (80% warning & 100% exceeded) and dispatch optional external notifications
	currentPeriod := now.Format("2006-01")
	alerts, _ := e.store.EvaluateAndCreateBudgetAlerts(ctx, req.UserID, currentPeriod)
	if e.notifier != nil && e.notifier.Enabled() {
		for _, a := range alerts {
			e.notifier.Deliver(ctx, notify.NotificationRequest{
				UserID:   a.UserID,
				AlertID:  a.ID,
				Type:     a.Type,
				Channels: []string{"email", "push"},
				Message:  a.Message,
			})
		}
	}

	return &SyncExecutionReport{
		TransactionsFetched: totalFetched,
		TransactionsNew:     len(classifiedNew),
		PromotedCount:       len(classifiedPromoted),
		DeletedPendingCount: len(rec.DeletedPendingIDs),
		BalanceCents:        balanceCents,
		AlertsCreated:       len(alerts),
	}, nil
}

// RetryUncategorized re-runs the 3-tier Jev classification pipeline on booked transactions
// that were stored with category_id == nil because Jev was temporarily unreachable.
func (e *Engine) RetryUncategorized(ctx context.Context) (int, error) {
	uncat, err := e.store.ListUncategorizedBooked(ctx)
	if err != nil || len(uncat) == 0 {
		return 0, err
	}

	categories, err := e.store.ListCategories(ctx)
	if err != nil {
		return 0, err
	}

	// Group by user so each user's manual rules are applied first
	byUser := make(map[string][]ai.TransactionInput)
	for _, u := range uncat {
		byUser[u.UserID] = append(byUser[u.UserID], ai.TransactionInput{
			ID:          u.ID,
			Description: u.Description,
			Creditor:    u.Creditor,
			AmountCents: u.AmountCents,
		})
	}

	allUpdates := make(map[string]ai.ClassificationResult)
	for userID, inputs := range byUser {
		manualRules, _ := e.store.ListManualRules(ctx, userID)
		res := e.jevClient.Classify(ctx, inputs, manualRules, categories)
		for txID, cr := range res {
			if cr.CategoryID != nil {
				allUpdates[txID] = cr
			}
		}
	}

	if len(allUpdates) > 0 {
		if err := e.store.ApplyRetriedCategories(ctx, allUpdates); err != nil {
			return 0, err
		}
	}
	return len(allUpdates), nil
}

func (e *Engine) handleSyncError(ctx context.Context, req SyncRequestPayload, err error) {
	errCode := 500
	var apiErr *gocardless.APIError
	if errors.As(err, &apiErr) {
		errCode = apiErr.StatusCode
		if apiErr.IsConsentExpired() {
			_ = e.store.MarkAccountExpired(ctx, req.AccountID)
			period := e.nowFunc().Format("2006-01")
			if alert, created, _ := e.store.CreateConsentAlert(
				ctx,
				req.UserID,
				req.AccountID,
				"account_expired",
				period,
				"Your 90-day PSD2 bank connection has expired. Please reconnect your account.",
			); created && alert != nil && e.notifier != nil && e.notifier.Enabled() {
				e.notifier.Deliver(ctx, notify.NotificationRequest{
					UserID:   alert.UserID,
					AlertID:  alert.ID,
					Type:     alert.Type,
					Channels: []string{"email", "push"},
					Message:  alert.Message,
				})
			}
		}
	}
	_ = e.store.MarkSyncFailed(ctx, req.SyncID, errCode, err.Error())
}

func mapGoCardlessTransaction(accountID string, tx gocardless.Transaction, isPending bool, fallbackNow time.Time) dedup.IncomingTransaction {
	amountCents, _ := gocardless.ParseAmountToCents(tx.TransactionAmount.Amount)
	var balAfterCents int64
	if tx.BalanceAfterTransaction != nil {
		balAfterCents, _ = gocardless.ParseAmountToCents(tx.BalanceAfterTransaction.BalanceAmount.Amount)
	}

	desc := strings.TrimSpace(tx.RemittanceInformationUnstructured)
	if desc == "" && len(tx.RemittanceInformationArray) > 0 {
		desc = strings.TrimSpace(strings.Join(tx.RemittanceInformationArray, " "))
	}

	bookingDate := parseDateOrFallback(tx.BookingDate, fallbackNow)
	valueDate := parseDateOrFallback(tx.ValueDate, bookingDate)
	txDate := bookingDate
	dateStr := tx.BookingDate
	if isPending && tx.ValueDate != "" {
		txDate = valueDate
		dateStr = tx.ValueDate
	}

	gcID := strings.TrimSpace(tx.InternalTransactionID)
	if gcID == "" {
		gcID = strings.TrimSpace(tx.TransactionID)
	}
	if gcID == "" {
		gcID = gocardless.DeterministicPendingID(accountID, dateStr, amountCents, desc)
	}

	var credIBAN, debtIBAN string
	if tx.CreditorAccount != nil {
		credIBAN = tx.CreditorAccount.IBAN
	}
	if tx.DebtorAccount != nil {
		debtIBAN = tx.DebtorAccount.IBAN
	}

	currency := tx.TransactionAmount.Currency
	if currency == "" {
		currency = "EUR"
	}

	return dedup.IncomingTransaction{
		GoCardlessTransactionID: gcID,
		Date:                    txDate,
		BookingDate:             bookingDate,
		ValueDate:               valueDate,
		AmountCents:             amountCents,
		BalanceAfterCents:       balAfterCents,
		Currency:                currency,
		Description:             desc,
		CreditorName:            tx.CreditorName,
		CreditorIBAN:            credIBAN,
		DebtorName:              tx.DebtorName,
		DebtorIBAN:              debtIBAN,
		BankTransactionCode:     tx.BankTransactionCode,
		IsPending:               isPending,
	}
}

func parseDateOrFallback(dateStr string, fallback time.Time) time.Time {
	s := strings.TrimSpace(dateStr)
	if len(s) >= 10 {
		if t, err := time.Parse("2006-01-02", s[:10]); err == nil {
			return t
		}
	}
	return fallback
}

func buildSandboxSyncData(now time.Time) ([]gocardless.Balance, gocardless.RateLimitHeaders, *gocardless.TransactionsList, gocardless.RateLimitHeaders) {
	rl := gocardless.RateLimitHeaders{Limit: 4, Remaining: 3, Reset: 21600, AccountSuccessReset: 21600}
	balances := []gocardless.Balance{
		{
			BalanceAmount: gocardless.BalanceAmount{Amount: "2845.60", Currency: "EUR"},
			BalanceType:   "closingBooked",
		},
	}
	d0 := now.Format("2006-01-02")
	d1 := now.AddDate(0, 0, -1).Format("2006-01-02")
	d2 := now.AddDate(0, 0, -2).Format("2006-01-02")
	d3 := now.AddDate(0, 0, -4).Format("2006-01-02")
	d4 := now.AddDate(0, 0, -6).Format("2006-01-02")

	txs := &gocardless.TransactionsList{
		Booked: []gocardless.Transaction{
			{
				TransactionID:                     "sbx_tx_nomina_01",
				BookingDate:                       d4,
				ValueDate:                         d4,
				TransactionAmount:                 gocardless.TransactionAmount{Amount: "2450.00", Currency: "EUR"},
				RemittanceInformationUnstructured: "ABONO NOMINA MENSUAL TECH SL",
				DebtorName:                        "TECH SOLUTIONS SL",
			},
			{
				TransactionID:                     "sbx_tx_mercadona_02",
				BookingDate:                       d3,
				ValueDate:                         d3,
				TransactionAmount:                 gocardless.TransactionAmount{Amount: "-68.40", Currency: "EUR"},
				RemittanceInformationUnstructured: "COMPRA EN MERCADONA MADRID",
				CreditorName:                      "MERCADONA SA",
			},
			{
				TransactionID:                     "sbx_tx_iberdrola_03",
				BookingDate:                       d2,
				ValueDate:                         d2,
				TransactionAmount:                 gocardless.TransactionAmount{Amount: "-54.20", Currency: "EUR"},
				RemittanceInformationUnstructured: "RECIBO IBERDROLA CLIENTES",
				CreditorName:                      "IBERDROLA",
			},
			{
				TransactionID:                     "sbx_tx_netflix_04",
				BookingDate:                       d1,
				ValueDate:                         d1,
				TransactionAmount:                 gocardless.TransactionAmount{Amount: "-12.99", Currency: "EUR"},
				RemittanceInformationUnstructured: "NETFLIX.COM SUSCRIPCION",
				CreditorName:                      "NETFLIX",
			},
			{
				TransactionID:                     "sbx_tx_clinica_05",
				BookingDate:                       d0,
				ValueDate:                         d0,
				TransactionAmount:                 gocardless.TransactionAmount{Amount: "-45.00", Currency: "EUR"},
				RemittanceInformationUnstructured: "CONSULTA FISIOTERAPIA Y REHABILITACION",
				CreditorName:                      "CENTRO FISIO",
			},
		},
		Pending: []gocardless.Transaction{
			{
				ValueDate:                         d0,
				TransactionAmount:                 gocardless.TransactionAmount{Amount: "-18.50", Currency: "EUR"},
				RemittanceInformationUnstructured: "GLOVO PEDIDO RESTAURANTE",
				CreditorName:                      "GLOVO",
			},
		},
	}
	return balances, rl, txs, rl
}

// EvaluateScheduledAccount checks whether an active account needs a scheduled sync or a 90-day PSD2 consent alert.
type ScheduledAction struct {
	AccountID     string
	TriggerSync   bool
	MarkExpired   bool
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

// RunScheduledTick evaluates all active accounts and executes scheduled syncs and PSD2 consent alerts.
func (e *Engine) RunScheduledTick(ctx context.Context) {
	now := e.nowFunc()
	accounts, err := e.store.ListActiveAccounts(ctx)
	if err != nil {
		slog.Error("failed to list active accounts for scheduler", "error", err)
		return
	}

	accByID := make(map[string]repository.ActiveAccount, len(accounts))
	for _, a := range accounts {
		accByID[a.ID] = a
	}

	period := now.Format("2006-01")
	actions := PlanScheduledActions(now, accounts)
	for _, act := range actions {
		acc := accByID[act.AccountID]
		if act.MarkExpired {
			_ = e.store.MarkAccountExpired(ctx, acc.ID)
			if alert, created, _ := e.store.CreateConsentAlert(
				ctx,
				acc.UserID,
				acc.ID,
				"account_expired",
				period,
				"Your 90-day PSD2 bank connection has expired. Please reconnect your account.",
			); created && alert != nil && e.notifier != nil && e.notifier.Enabled() {
				e.notifier.Deliver(ctx, notify.NotificationRequest{
					UserID:   alert.UserID,
					AlertID:  alert.ID,
					Type:     alert.Type,
					Channels: []string{"email", "push"},
					Message:  alert.Message,
				})
			}
			continue
		}

		if act.AlertExpiring {
			if alert, created, _ := e.store.CreateConsentAlert(
				ctx,
				acc.UserID,
				acc.ID,
				"account_consent_expiring",
				period,
				"Your 90-day PSD2 bank connection expires in less than 7 days. Reconnect now to avoid interruption.",
			); created && alert != nil && e.notifier != nil && e.notifier.Enabled() {
				e.notifier.Deliver(ctx, notify.NotificationRequest{
					UserID:   alert.UserID,
					AlertID:  alert.ID,
					Type:     alert.Type,
					Channels: []string{"email", "push"},
					Message:  alert.Message,
				})
			}
		}

		if act.TriggerSync {
			payload := SyncRequestPayload{
				UserID:                  acc.UserID,
				AccountID:               acc.ID,
				GoCardlessRequisitionID: acc.GoCardlessRequisitionID,
				GoCardlessAccountID:     acc.GoCardlessAccountID,
			}
			if e.bus != nil {
				_ = e.bus.PublishSyncRequest(ctx, payload)
			} else {
				_ = e.ProcessSyncRequest(ctx, payload, nil)
			}
		}
	}
}

// Run starts NATS JetStream consumers (if configured), the 12h account scheduler, and the 6h Jev retry job until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	if e.bus != nil {
		if err := e.bus.StartConsumers(ctx, e); err != nil {
			return err
		}
	}

	schedTicker := time.NewTicker(e.schedulerInterval)
	defer schedTicker.Stop()

	jevRetryTicker := time.NewTicker(6 * time.Hour)
	defer jevRetryTicker.Stop()

	slog.Info("finperk backend-worker started", "scheduler_interval", e.schedulerInterval.String())
	go func() {
		time.Sleep(1 * time.Second)
		if count, err := e.RetryUncategorized(ctx); err == nil && count > 0 {
			slog.Info("categorized historical transactions via Jev", "count", count)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			slog.Info("finperk backend-worker stopped")
			return nil
		case <-schedTicker.C:
			e.RunScheduledTick(ctx)
		case <-jevRetryTicker.C:
			_, _ = e.RetryUncategorized(ctx)
		}
	}
}

