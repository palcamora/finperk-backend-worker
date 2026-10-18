package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"backend-worker/internal/ai"
	"backend-worker/internal/dedup"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"pkg/gocardless"
)

// MongoWorkerStore implements WorkerStore backed by a MongoDB Replica Set (supporting multi-document ACID sessions).
type MongoWorkerStore struct {
	client *mongo.Client
	db     *mongo.Database
}

// ConnectMongoWorker connects to MongoDB and verifies connectivity.
func ConnectMongoWorker(ctx context.Context, uri, dbName string) (*MongoWorkerStore, error) {
	if dbName == "" {
		dbName = "finperk"
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return &MongoWorkerStore{
		client: client,
		db:     client.Database(dbName),
	}, nil
}

// Close disconnects the MongoDB client.
func (s *MongoWorkerStore) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Disconnect(ctx)
}

// Database returns the underlying *mongo.Database handle (e.g. for shared GoCardless MongoTokenStore).
func (s *MongoWorkerStore) Database() *mongo.Database {
	if s == nil {
		return nil
	}
	return s.db
}

type mongoAccountDoc struct {
	ID                      string     `bson:"_id"`
	UserID                  string     `bson:"user_id"`
	IBAN                    string     `bson:"iban"`
	GoCardlessRequisitionID string     `bson:"gocardless_requisition_id"`
	GoCardlessAccountID     string     `bson:"gocardless_account_id"`
	Status                  string     `bson:"status"`
	Balance                 int64      `bson:"balance"`
	AccessExpiresAt         time.Time  `bson:"access_expires_at"`
	LastSyncAt              *time.Time `bson:"last_sync_at"`
	LastSyncHeaders         struct {
		Transactions gocardless.RateLimitHeaders `bson:"transactions"`
		Balances     gocardless.RateLimitHeaders `bson:"balances"`
	} `bson:"last_sync_headers"`
}

func toActiveAccount(doc mongoAccountDoc) ActiveAccount {
	var lastSync time.Time
	if doc.LastSyncAt != nil {
		lastSync = *doc.LastSyncAt
	}
	rem := doc.LastSyncHeaders.Transactions.Remaining
	if doc.LastSyncHeaders.Transactions.Limit == 0 && rem == 0 {
		rem = 4
	}
	return ActiveAccount{
		ID:                      doc.ID,
		UserID:                  doc.UserID,
		IBAN:                    doc.IBAN,
		GoCardlessRequisitionID: doc.GoCardlessRequisitionID,
		GoCardlessAccountID:     doc.GoCardlessAccountID,
		Status:                  doc.Status,
		BalanceCents:            doc.Balance,
		AccessExpiresAt:         doc.AccessExpiresAt,
		LastSyncAt:              lastSync,
		RemainingCalls:          rem,
		TransactionsRateLimits:  doc.LastSyncHeaders.Transactions,
		BalancesRateLimits:      doc.LastSyncHeaders.Balances,
	}
}

func (s *MongoWorkerStore) ListActiveAccounts(ctx context.Context) ([]ActiveAccount, error) {
	cur, err := s.db.Collection("accounts").Find(ctx, bson.M{"status": "enabled"})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var docs []mongoAccountDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]ActiveAccount, 0, len(docs))
	for _, d := range docs {
		out = append(out, toActiveAccount(d))
	}
	return out, nil
}

func (s *MongoWorkerStore) GetAccount(ctx context.Context, accountID string) (*ActiveAccount, error) {
	var doc mongoAccountDoc
	if err := s.db.Collection("accounts").FindOne(ctx, bson.M{"_id": accountID}).Decode(&doc); err != nil {
		return nil, err
	}
	acc := toActiveAccount(doc)
	return &acc, nil
}

func (s *MongoWorkerStore) MarkAccountExpired(ctx context.Context, accountID string) error {
	_, err := s.db.Collection("accounts").UpdateOne(
		ctx,
		bson.M{"_id": accountID},
		bson.M{"$set": bson.M{"status": "expired", "updated_at": time.Now().UTC()}},
	)
	return err
}

func (s *MongoWorkerStore) ListUserIBANs(ctx context.Context, userID string) (map[string]bool, error) {
	cur, err := s.db.Collection("accounts").Find(ctx, bson.M{"user_id": userID, "status": bson.M{"$ne": "deleted"}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var docs []mongoAccountDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	ibans := make(map[string]bool, len(docs))
	for _, d := range docs {
		if norm := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(d.IBAN), " ", "")); norm != "" {
			ibans[norm] = true
		}
	}
	return ibans, nil
}

type mongoTxDoc struct {
	ID                      string    `bson:"_id"`
	UserID                  string    `bson:"user_id"`
	AccountID               string    `bson:"account_id"`
	GoCardlessTransactionID string    `bson:"gocardless_transaction_id"`
	Date                    time.Time `bson:"date"`
	BookingDate             time.Time `bson:"booking_date"`
	ValueDate               time.Time `bson:"value_date"`
	Amount                  int64     `bson:"amount"`
	Currency                string    `bson:"currency"`
	Description             string    `bson:"description"`
	CategoryID              *string   `bson:"category_id"`
	CategorySource          *string   `bson:"category_source"`
	IsInternalTransfer      bool      `bson:"is_internal_transfer"`
	BalanceAfter            int64     `bson:"balance_after"`
	DebtorName              string    `bson:"debtor_name"`
	DebtorIBAN              string    `bson:"debtor_iban"`
	CreditorName            string    `bson:"creditor_name"`
	CreditorIBAN            string    `bson:"creditor_iban"`
	BankTransactionCode     string    `bson:"bank_transaction_code"`
	IsPending               bool      `bson:"is_pending"`
	PendingSyncCount        int       `bson:"pending_sync_count"`
	CreatedAt               time.Time `bson:"created_at"`
	UpdatedAt               time.Time `bson:"updated_at"`
}

func (s *MongoWorkerStore) ListAccountTransactions(ctx context.Context, accountID string) ([]dedup.StoredTransaction, error) {
	cur, err := s.db.Collection("transactions").Find(ctx, bson.M{"account_id": accountID})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var docs []mongoTxDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]dedup.StoredTransaction, 0, len(docs))
	for _, d := range docs {
		out = append(out, dedup.StoredTransaction{
			ID:                      d.ID,
			GoCardlessTransactionID: d.GoCardlessTransactionID,
			Date:                    d.Date,
			AmountCents:             d.Amount,
			Description:             d.Description,
			CreditorName:            d.CreditorName,
			CategoryID:              d.CategoryID,
			CategorySource:          d.CategorySource,
			IsInternalTransfer:      d.IsInternalTransfer,
			IsPending:               d.IsPending,
			PendingSyncCount:        d.PendingSyncCount,
		})
	}
	return out, nil
}

func (s *MongoWorkerStore) ListManualRules(ctx context.Context, userID string) (map[string]string, error) {
	cur, err := s.db.Collection("transactions").Find(ctx, bson.M{
		"user_id":         userID,
		"category_source": ai.SourceManual,
	})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var docs []mongoTxDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	rules := make(map[string]string)
	for _, d := range docs {
		if d.CategoryID == nil {
			continue
		}
		if normCred := ai.NormalizeText(d.CreditorName); normCred != "" {
			rules[normCred] = *d.CategoryID
		}
		if normDesc := ai.NormalizeText(d.Description); normDesc != "" {
			rules[normDesc] = *d.CategoryID
		}
	}
	return rules, nil
}

func (s *MongoWorkerStore) ListCategories(ctx context.Context) ([]ai.CategoryOption, error) {
	cur, err := s.db.Collection("categories").Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var docs []struct {
		ID       string   `bson:"_id"`
		Name     string   `bson:"name"`
		Keywords []string `bson:"keywords"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]ai.CategoryOption, 0, len(docs))
	for _, d := range docs {
		out = append(out, ai.CategoryOption{
			ID:       d.ID,
			Name:     d.Name,
			Keywords: d.Keywords,
		})
	}
	return out, nil
}

// ApplySyncOutcome executes all database writes for a completed sync inside an atomic MongoDB Replica Set session.
func (s *MongoWorkerStore) ApplySyncOutcome(ctx context.Context, outcome SyncOutcome) error {
	session, err := s.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
		txColl := s.db.Collection("transactions")
		affectedPeriods := make(map[string]bool)
		now := outcome.CompletedAt
		if now.IsZero() {
			now = time.Now().UTC()
		}

		// 1. Delete disappeared pending transactions
		if len(outcome.DeletedPendingIDs) > 0 {
			_, err := txColl.DeleteMany(sessCtx, bson.M{"_id": bson.M{"$in": outcome.DeletedPendingIDs}})
			if err != nil {
				return nil, err
			}
		}

		// 2. Increment pending_sync_count on still-pending transactions
		if len(outcome.IncrementedPendingIDs) > 0 {
			_, err := txColl.UpdateMany(
				sessCtx,
				bson.M{"_id": bson.M{"$in": outcome.IncrementedPendingIDs}},
				bson.M{"$inc": bson.M{"pending_sync_count": 1}, "$set": bson.M{"updated_at": now}},
			)
			if err != nil {
				return nil, err
			}
		}

		// 3. Promote pending transactions that became booked
		for storedID, prom := range outcome.PromotedPendingToBook {
			_, err := txColl.UpdateOne(
				sessCtx,
				bson.M{"_id": storedID},
				bson.M{"$set": bson.M{
					"gocardless_transaction_id": prom.Incoming.GoCardlessTransactionID,
					"date":                      prom.Incoming.Date,
					"booking_date":              prom.Incoming.BookingDate,
					"value_date":                prom.Incoming.ValueDate,
					"amount":                    prom.Incoming.AmountCents,
					"balance_after":             prom.Incoming.BalanceAfterCents,
					"description":               prom.Incoming.Description,
					"creditor_name":             prom.Incoming.CreditorName,
					"creditor_iban":             prom.Incoming.CreditorIBAN,
					"debtor_name":               prom.Incoming.DebtorName,
					"debtor_iban":               prom.Incoming.DebtorIBAN,
					"category_id":               prom.CategoryID,
					"category_source":           prom.CategorySource,
					"is_internal_transfer":      prom.IsInternalTransfer,
					"is_pending":                false,
					"pending_sync_count":        0,
					"updated_at":                now,
				}},
			)
			if err != nil {
				return nil, err
			}
			affectedPeriods[prom.Incoming.Date.Format("2006-01")] = true
		}

		// 4. Insert genuinely new transactions
		for _, nt := range outcome.NewTransactions {
			doc := mongoTxDoc{
				ID:                      genID(),
				UserID:                  outcome.UserID,
				AccountID:               outcome.AccountID,
				GoCardlessTransactionID: nt.Incoming.GoCardlessTransactionID,
				Date:                    nt.Incoming.Date,
				BookingDate:             nt.Incoming.BookingDate,
				ValueDate:               nt.Incoming.ValueDate,
				Amount:                  nt.Incoming.AmountCents,
				Currency:                nt.Incoming.Currency,
				Description:             nt.Incoming.Description,
				CategoryID:              nt.CategoryID,
				CategorySource:          nt.CategorySource,
				IsInternalTransfer:      nt.IsInternalTransfer,
				BalanceAfter:            nt.Incoming.BalanceAfterCents,
				DebtorName:              nt.Incoming.DebtorName,
				DebtorIBAN:              nt.Incoming.DebtorIBAN,
				CreditorName:            nt.Incoming.CreditorName,
				CreditorIBAN:            nt.Incoming.CreditorIBAN,
				BankTransactionCode:     nt.Incoming.BankTransactionCode,
				IsPending:               nt.Incoming.IsPending,
				PendingSyncCount:        0,
				CreatedAt:               now,
				UpdatedAt:               now,
			}
			_, err := txColl.InsertOne(sessCtx, doc)
			if err != nil && !mongo.IsDuplicateKeyError(err) {
				return nil, err
			}
			if !nt.Incoming.IsPending {
				affectedPeriods[nt.Incoming.Date.Format("2006-01")] = true
			}
		}

		// 5. Update account balance, last_sync_at, and per-endpoint rate limit headers
		_, err := s.db.Collection("accounts").UpdateOne(
			sessCtx,
			bson.M{"_id": outcome.AccountID},
			bson.M{"$set": bson.M{
				"balance":      outcome.BalanceCents,
				"last_sync_at": now,
				"last_sync_headers": bson.M{
					"transactions": outcome.TransactionsRateLimits,
					"balances":     outcome.BalancesRateLimits,
				},
				"updated_at": now,
			}},
		)
		if err != nil {
			return nil, err
		}

		// 6. Recompute spending_summaries for affected YYYY-MM periods
		for period := range affectedPeriods {
			if err := s.recomputePeriodSummaryInSession(sessCtx, outcome.UserID, period, now); err != nil {
				return nil, err
			}
		}

		// 7. Update sync_requests log entry
		if outcome.SyncID != "" {
			newCount := len(outcome.NewTransactions)
			_, _ = s.db.Collection("sync_requests").UpdateOne(
				sessCtx,
				bson.M{"_id": outcome.SyncID},
				bson.M{"$set": bson.M{
					"status":               "success",
					"transactions_fetched": outcome.TransactionsFetched,
					"transactions_new":     newCount,
					"completed_at":         now,
				}},
			)
		}

		return nil, nil
	})
	return err
}

func (s *MongoWorkerStore) recomputePeriodSummaryInSession(ctx context.Context, userID, period string, now time.Time) error {
	start, err := time.Parse("2006-01", period)
	if err != nil {
		return err
	}
	end := start.AddDate(0, 1, 0)

	cur, err := s.db.Collection("transactions").Find(ctx, bson.M{
		"user_id":    userID,
		"is_pending": false,
		"date":       bson.M{"$gte": start, "$lt": end},
	})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	var docs []mongoTxDoc
	if err := cur.All(ctx, &docs); err != nil {
		return err
	}

	var totalIncome, totalExpenses int64
	byCat := make(map[string]*struct {
		CategoryID string `bson:"category_id"`
		Total      int64  `bson:"total"`
		Count      int    `bson:"count"`
	})

	for _, d := range docs {
		isTransfer := d.IsInternalTransfer || (d.CategoryID != nil && *d.CategoryID == "cat_transfers")
		if !isTransfer {
			if d.Amount > 0 {
				totalIncome += d.Amount
			} else if d.Amount < 0 {
				totalExpenses += (-d.Amount)
			}
		}
		if d.CategoryID != nil && *d.CategoryID != "" {
			cid := *d.CategoryID
			item, ok := byCat[cid]
			if !ok {
				item = &struct {
					CategoryID string `bson:"category_id"`
					Total      int64  `bson:"total"`
					Count      int    `bson:"count"`
				}{CategoryID: cid}
				byCat[cid] = item
			}
			item.Total += d.Amount
			item.Count++
		}
	}

	catSlice := make([]bson.M, 0, len(byCat))
	for _, item := range byCat {
		catSlice = append(catSlice, bson.M{
			"category_id": item.CategoryID,
			"total":       item.Total,
			"count":       item.Count,
		})
	}

	_, err = s.db.Collection("spending_summaries").UpdateOne(
		ctx,
		bson.M{"user_id": userID, "period": period},
		bson.M{
			"$set": bson.M{
				"total_income":   totalIncome,
				"total_expenses": totalExpenses,
				"by_category":    catSlice,
				"updated_at":     now,
			},
			"$setOnInsert": bson.M{"_id": genID()},
		},
		options.Update().SetUpsert(true),
	)
	return err
}

func (s *MongoWorkerStore) MarkSyncFailed(ctx context.Context, syncID string, errCode int, errDetail string) error {
	if syncID == "" {
		return nil
	}
	now := time.Now().UTC()
	_, err := s.db.Collection("sync_requests").UpdateOne(
		ctx,
		bson.M{"_id": syncID},
		bson.M{"$set": bson.M{
			"status":       "failed",
			"error_code":   errCode,
			"error_detail": errDetail,
			"completed_at": now,
		}},
	)
	return err
}

func (s *MongoWorkerStore) EvaluateAndCreateBudgetAlerts(ctx context.Context, userID, period string) ([]AlertRecord, error) {
	var sum struct {
		ByCategory []struct {
			CategoryID string `bson:"category_id"`
			Total      int64  `bson:"total"`
		} `bson:"by_category"`
	}
	err := s.db.Collection("spending_summaries").FindOne(ctx, bson.M{"user_id": userID, "period": period}).Decode(&sum)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}

	spentByCat := make(map[string]int64)
	for _, c := range sum.ByCategory {
		if c.Total < 0 {
			spentByCat[c.CategoryID] = -c.Total
		}
	}

	cur, err := s.db.Collection("budgets").Find(ctx, bson.M{"user_id": userID, "is_active": true})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var budgets []struct {
		ID         string `bson:"_id"`
		CategoryID string `bson:"category_id"`
		Amount     int64  `bson:"amount"`
	}
	if err := cur.All(ctx, &budgets); err != nil {
		return nil, err
	}

	var created []AlertRecord
	now := time.Now().UTC()
	for _, b := range budgets {
		if b.Amount <= 0 {
			continue
		}
		spent := spentByCat[b.CategoryID]
		pct := math.Round((float64(spent)/float64(b.Amount))*10000) / 100

		var alertType, msg string
		if spent >= b.Amount {
			alertType = "budget_exceeded"
			msg = fmt.Sprintf("You have exceeded your %s budget", b.CategoryID)
		} else if pct >= 80.0 {
			alertType = "budget_threshold"
			msg = fmt.Sprintf("You have reached 80%% of your %s budget", b.CategoryID)
		} else {
			continue
		}

		budgetID := b.ID
		rec := AlertRecord{
			ID:        genID(),
			UserID:    userID,
			BudgetID:  &budgetID,
			Type:      alertType,
			Period:    period,
			Message:   msg,
			CreatedAt: now,
			Metadata: map[string]any{
				"budget_id":   b.ID,
				"category_id": b.CategoryID,
				"percentage":  pct,
				"spent":       spent,
				"limit":       b.Amount,
			},
		}
		_, err := s.db.Collection("alerts").InsertOne(ctx, bson.M{
			"_id":        rec.ID,
			"user_id":    rec.UserID,
			"budget_id":  rec.BudgetID,
			"account_id": nil,
			"type":       rec.Type,
			"period":     rec.Period,
			"message":    rec.Message,
			"metadata":   rec.Metadata,
			"is_read":    false,
			"delivery_status": bson.M{
				"email": "pending",
				"push":  "pending",
			},
			"created_at": rec.CreatedAt,
		})
		if err == nil {
			created = append(created, rec)
		}
	}

	return created, nil
}

func (s *MongoWorkerStore) CreateConsentAlert(ctx context.Context, userID, accountID, alertType, period, message string) (*AlertRecord, bool, error) {
	now := time.Now().UTC()
	accID := accountID
	rec := AlertRecord{
		ID:        genID(),
		UserID:    userID,
		AccountID: &accID,
		Type:      alertType,
		Period:    period,
		Message:   message,
		CreatedAt: now,
	}
	_, err := s.db.Collection("alerts").InsertOne(ctx, bson.M{
		"_id":        rec.ID,
		"user_id":    rec.UserID,
		"budget_id":  nil,
		"account_id": rec.AccountID,
		"type":       rec.Type,
		"period":     rec.Period,
		"message":    rec.Message,
		"is_read":    false,
		"delivery_status": bson.M{
			"email": "pending",
			"push":  "pending",
		},
		"created_at": rec.CreatedAt,
	})
	if mongo.IsDuplicateKeyError(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &rec, true, nil
}

func (s *MongoWorkerStore) ListUncategorizedBooked(ctx context.Context) ([]UncategorizedTransaction, error) {
	cur, err := s.db.Collection("transactions").Find(ctx, bson.M{
		"is_pending":  false,
		"category_id": nil,
	})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var docs []mongoTxDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]UncategorizedTransaction, 0, len(docs))
	for _, d := range docs {
		out = append(out, UncategorizedTransaction{
			ID:          d.ID,
			UserID:      d.UserID,
			AccountID:   d.AccountID,
			Date:        d.Date,
			Description: d.Description,
			Creditor:    d.CreditorName,
			AmountCents: d.Amount,
		})
	}
	return out, nil
}

func (s *MongoWorkerStore) ApplyRetriedCategories(ctx context.Context, updates map[string]ai.ClassificationResult) error {
	now := time.Now().UTC()
	affected := make(map[string]struct{ userID, period string })
	txColl := s.db.Collection("transactions")

	for txID, res := range updates {
		if res.CategoryID == nil {
			continue
		}
		var doc mongoTxDoc
		if err := txColl.FindOneAndUpdate(
			ctx,
			bson.M{"_id": txID},
			bson.M{"$set": bson.M{
				"category_id":     res.CategoryID,
				"category_source": res.CategorySource,
				"updated_at":      now,
			}},
		).Decode(&doc); err == nil {
			period := doc.Date.Format("2006-01")
			affected[doc.UserID+"|"+period] = struct{ userID, period string }{doc.UserID, period}
		}
	}

	for _, up := range affected {
		_ = s.recomputePeriodSummaryInSession(ctx, up.userID, up.period, now)
	}
	return nil
}
