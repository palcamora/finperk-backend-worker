package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	SourceManual  = "manual"
	SourceKeyword = "keyword"
	SourceJev     = "jev"
)

// CategoryOption represents a global category and its pre-classification keywords.
type CategoryOption struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Keywords []string `json:"keywords"`
}

// TransactionInput represents a transaction to be categorized.
type TransactionInput struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Creditor    string `json:"creditor"`
	AmountCents int64  `json:"amount_cents"`
}

// ClassificationResult holds the assigned category_id and category_source.
type ClassificationResult struct {
	CategoryID     *string `json:"category_id"`
	CategorySource *string `json:"category_source"`
}

// JevClient implements the 3-tier categorization pipeline:
// 1. User manual history rules (manual)
// 2. Category keywords (keyword)
// 3. Jev decision model API (jev)
type JevClient struct {
	apiURL     string
	apiKey     string
	httpClient *http.Client
}

// NewJevClient creates a new Jev categorizer client.
func NewJevClient(apiURL, apiKey string, hc *http.Client) *JevClient {
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	return &JevClient{
		apiURL:     strings.TrimSpace(apiURL),
		apiKey:     strings.TrimSpace(apiKey),
		httpClient: hc,
	}
}

// Classify runs the 3-tier pipeline over a batch of new transactions.
// manualRules maps normalized description or creditor name -> category_id from prior user manual overrides.
func (c *JevClient) Classify(
	ctx context.Context,
	inputs []TransactionInput,
	manualRules map[string]string,
	categories []CategoryOption,
) map[string]ClassificationResult {
	results := make(map[string]ClassificationResult, len(inputs))
	var pendingJev []TransactionInput

	optionIDs := make([]string, 0, len(categories))
	validCats := make(map[string]bool, len(categories))
	for _, cat := range categories {
		optionIDs = append(optionIDs, cat.ID)
		validCats[cat.ID] = true
	}

	for _, tx := range inputs {
		normDesc := NormalizeText(tx.Description)
		normCred := NormalizeText(tx.Creditor)

		// Tier 1: User manual history rule
		if catID, ok := matchManualRule(normDesc, normCred, manualRules); ok && validCats[catID] {
			cid := catID
			src := SourceManual
			results[tx.ID] = ClassificationResult{CategoryID: &cid, CategorySource: &src}
			continue
		}

		// Tier 2: Category keywords
		if catID, ok := matchKeyword(normDesc, normCred, categories); ok {
			cid := catID
			src := SourceKeyword
			results[tx.ID] = ClassificationResult{CategoryID: &cid, CategorySource: &src}
			continue
		}

		// Queue for Tier 3 (Jev decision model)
		pendingJev = append(pendingJev, tx)
	}

	// Tier 3: Jev Decision Model
	for _, tx := range pendingJev {
		catID, err := c.callJevDecision(ctx, tx, optionIDs)
		if err != nil || !validCats[catID] {
			// Jev unavailable -> store uncategorized (null) to be retried by background job
			results[tx.ID] = ClassificationResult{CategoryID: nil, CategorySource: nil}
			continue
		}
		cid := catID
		src := SourceJev
		results[tx.ID] = ClassificationResult{CategoryID: &cid, CategorySource: &src}
	}

	return results
}

type jevChoiceQuestion struct {
	Type         string   `json:"type"`
	Instructions string   `json:"instructions"`
	Options      []string `json:"options"`
}

type jevDecisionRequest struct {
	Model     string                       `json:"model"`
	State     string                       `json:"state"`
	Questions map[string]jevChoiceQuestion `json:"questions"`
}

type jevDecisionResponse struct {
	Category string            `json:"category"`
	Decisions map[string]struct {
		Choice string `json:"choice"`
	} `json:"decisions"`
}

func (c *JevClient) callJevDecision(ctx context.Context, tx TransactionInput, optionIDs []string) (string, error) {
	if c.apiURL == "" || c.apiKey == "" {
		return "", errors.New("jev: API URL or key not configured")
	}

	state := fmt.Sprintf("description: %s | creditor: %s | amount_cents: %d", tx.Description, tx.Creditor, tx.AmountCents)
	payload := jevDecisionRequest{
		Model: "jev-latest",
		State: state,
		Questions: map[string]jevChoiceQuestion{
			"category": {
				Type:         "choice",
				Instructions: "Select the single most accurate global category ID for this bank transaction.",
				Options:      optionIDs,
			},
		},
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiURL, bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("jev: unexpected status %d", resp.StatusCode)
	}

	var decoded jevDecisionResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	if decoded.Category != "" {
		return decoded.Category, nil
	}
	if d, ok := decoded.Decisions["category"]; ok && d.Choice != "" {
		return d.Choice, nil
	}
	return "", errors.New("jev: empty category decision")
}

// NormalizeText normalizes a description or creditor name for deterministic rule matching.
func NormalizeText(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(s))), " ")
}

func matchManualRule(normDesc, normCred string, manualRules map[string]string) (string, bool) {
	if len(manualRules) == 0 {
		return "", false
	}
	if normCred != "" {
		if cat, ok := manualRules[normCred]; ok {
			return cat, true
		}
	}
	if normDesc != "" {
		if cat, ok := manualRules[normDesc]; ok {
			return cat, true
		}
	}
	return "", false
}

func matchKeyword(normDesc, normCred string, categories []CategoryOption) (string, bool) {
	combined := normDesc + " " + normCred
	for _, cat := range categories {
		for _, kw := range cat.Keywords {
			normKW := NormalizeText(kw)
			if normKW != "" && strings.Contains(combined, normKW) {
				return cat.ID, true
			}
		}
	}
	return "", false
}
