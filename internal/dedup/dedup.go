package dedup

import (
	"math"
	"strings"
	"time"
)

// StoredTransaction represents an existing transaction in MongoDB used during deduplication and pending reconciliation.
type StoredTransaction struct {
	ID                      string
	GoCardlessTransactionID string
	Date                    time.Time
	AmountCents             int64
	Description             string
	CreditorName            string
	CategoryID              *string
	CategorySource          *string
	IsInternalTransfer      bool
	IsPending               bool
	PendingSyncCount        int
}

// IncomingTransaction represents a transaction fetched from GoCardless (already converted to integer cents).
type IncomingTransaction struct {
	GoCardlessTransactionID string
	Date                    time.Time
	BookingDate             time.Time
	ValueDate               time.Time
	AmountCents             int64
	BalanceAfterCents       int64
	Currency                string
	Description             string
	CreditorName            string
	CreditorIBAN            string
	DebtorName              string
	DebtorIBAN              string
	BankTransactionCode     string
	IsPending               bool
}

// ReconciliationResult describes the actions to apply inside the worker's atomic MongoDB session.
type ReconciliationResult struct {
	NewTransactions       []IncomingTransaction
	PromotedPendingToBook map[string]IncomingTransaction // stored ID -> booked IncomingTransaction
	IncrementedPendingIDs []string                       // stored IDs that remain pending
	DeletedPendingIDs     []string                       // stored IDs of pending transactions that disappeared
}

// Reconcile compares incoming booked and pending transactions from GoCardless against stored transactions.
// It supports both exact GoCardless transaction ID matching and fuzzy matching (exact amount in cents +
// booking date within ±3 days + normalized description/creditor match) when a pending transaction transitions to booked.
func Reconcile(existing []StoredTransaction, incomingBooked, incomingPending []IncomingTransaction) ReconciliationResult {
	res := ReconciliationResult{
		PromotedPendingToBook: make(map[string]IncomingTransaction),
	}

	bookedByGCID := make(map[string]int, len(incomingBooked))
	for i, b := range incomingBooked {
		bookedByGCID[b.GoCardlessTransactionID] = i
	}
	pendingByGCID := make(map[string]int, len(incomingPending))
	for i, p := range incomingPending {
		pendingByGCID[p.GoCardlessTransactionID] = i
	}

	seenBookedIdx := make(map[int]bool)
	seenPendingIdx := make(map[int]bool)

	// 1. Mark already-booked stored transactions as seen
	for _, st := range existing {
		if !st.IsPending {
			if idx, ok := bookedByGCID[st.GoCardlessTransactionID]; ok {
				seenBookedIdx[idx] = true
			}
		}
	}

	// 2. Reconcile previously-pending transactions
	for _, st := range existing {
		if !st.IsPending {
			continue
		}

		// 2a. Exact ID match in incoming booked
		if idx, ok := bookedByGCID[st.GoCardlessTransactionID]; ok && !seenBookedIdx[idx] {
			seenBookedIdx[idx] = true
			res.PromotedPendingToBook[st.ID] = incomingBooked[idx]
			continue
		}

		// 2b. Fuzzy match in incoming booked (exact amountCents + date within ±3 days + description/creditor similarity)
		if idx, ok := findFuzzyBookedMatch(st, incomingBooked, seenBookedIdx); ok {
			seenBookedIdx[idx] = true
			res.PromotedPendingToBook[st.ID] = incomingBooked[idx]
			continue
		}

		// 2c. Still in incoming pending array
		if idx, ok := pendingByGCID[st.GoCardlessTransactionID]; ok && !seenPendingIdx[idx] {
			seenPendingIdx[idx] = true
			res.IncrementedPendingIDs = append(res.IncrementedPendingIDs, st.ID)
			continue
		}

		// 2d. Disappeared from both booked and pending -> delete from DB
		res.DeletedPendingIDs = append(res.DeletedPendingIDs, st.ID)
	}

	// 3. Collect genuinely new booked and pending transactions (avoiding duplicate IDs in the same batch)
	seenGCIDs := make(map[string]bool)
	for i, b := range incomingBooked {
		if !seenBookedIdx[i] {
			if b.GoCardlessTransactionID != "" {
				if seenGCIDs[b.GoCardlessTransactionID] {
					continue
				}
				seenGCIDs[b.GoCardlessTransactionID] = true
			}
			res.NewTransactions = append(res.NewTransactions, b)
		}
	}
	for i, p := range incomingPending {
		if !seenPendingIdx[i] {
			if p.GoCardlessTransactionID != "" {
				if seenGCIDs[p.GoCardlessTransactionID] {
					continue
				}
				seenGCIDs[p.GoCardlessTransactionID] = true
			}
			res.NewTransactions = append(res.NewTransactions, p)
		}
	}

	return res
}

// IsInternalTransfer detects whether a transaction is a transfer between the user's own connected accounts
// (matching DebtorIBAN or CreditorIBAN against userIBANs, or explicit self-transfer descriptions).
func IsInternalTransfer(tx IncomingTransaction, userIBANs map[string]bool) bool {
	normIBAN := func(s string) string {
		return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(s), " ", ""))
	}
	if len(userIBANs) > 0 {
		if c := normIBAN(tx.CreditorIBAN); c != "" && userIBANs[c] {
			return true
		}
		if d := normIBAN(tx.DebtorIBAN); d != "" && userIBANs[d] {
			return true
		}
	}
	desc := norm(tx.Description)
	selfPatterns := []string{
		"traspaso propio",
		"transferencia propia",
		"traspaso entre mis cuentas",
		"bizum a mi mismo",
	}
	for _, pat := range selfPatterns {
		if strings.Contains(desc, pat) {
			return true
		}
	}
	return false
}

func findFuzzyBookedMatch(pending StoredTransaction, booked []IncomingTransaction, used map[int]bool) (int, bool) {
	pDesc := norm(pending.Description)
	pCred := norm(pending.CreditorName)

	for i, b := range booked {
		if used[i] || b.AmountCents != pending.AmountCents {
			continue
		}
		daysDiff := math.Abs(b.Date.Sub(pending.Date).Hours() / 24.0)
		if daysDiff > 3.0 {
			continue
		}
		bDesc := norm(b.Description)
		bCred := norm(b.CreditorName)
		if (pDesc != "" && bDesc != "" && (strings.Contains(bDesc, pDesc) || strings.Contains(pDesc, bDesc))) ||
			(pCred != "" && bCred != "" && pCred == bCred) {
			return i, true
		}
	}
	return -1, false
}

func norm(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
