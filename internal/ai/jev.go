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

	// ProviderJev uses the native Jev decisions JSON endpoint (/v1/decisions).
	ProviderJev = "jev"
	// ProviderOpenRouter uses OpenRouter's OpenAI-compatible Chat Completions endpoint (/api/v1/chat/completions).
	ProviderOpenRouter = "openrouter"

	// DefaultOpenRouterURL is the default OpenRouter Chat Completions endpoint.
	DefaultOpenRouterURL = "https://openrouter.ai/api/v1/chat/completions"
	// DefaultOpenRouterModel is the default model used when calling OpenRouter if none is specified.
	DefaultOpenRouterModel = "google/gemini-2.5-flash"
	// DefaultJevModel is the default model used when calling the native Jev endpoint.
	DefaultJevModel = "jev-latest"
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

// JevOption configures a JevClient (such as enabling OpenRouter compatibility or choosing a model).
type JevOption func(*JevClient)

// WithProvider sets the AI backend provider ("jev" or "openrouter").
func WithProvider(provider string) JevOption {
	return func(c *JevClient) {
		p := strings.ToLower(strings.TrimSpace(provider))
		if p != "" {
			c.provider = p
		}
	}
}

// WithModel overrides the model name sent to Jev or OpenRouter.
func WithModel(model string) JevOption {
	return func(c *JevClient) {
		m := strings.TrimSpace(model)
		if m != "" {
			c.model = m
		}
	}
}

// JevClient implements the 3-tier categorization pipeline:
// 1. User manual history rules (manual)
// 2. Category keywords (keyword)
// 3. Jev decision model API or OpenRouter Chat Completions API (jev)
type JevClient struct {
	apiURL     string
	apiKey     string
	provider   string
	model      string
	httpClient *http.Client
}

// NewJevClient creates a new Jev categorizer client.
// It supports both native Jev endpoints and OpenRouter (automatically detected when apiURL contains
// "openrouter.ai" or "/chat/completions", or when configured via WithProvider("openrouter")).
func NewJevClient(apiURL, apiKey string, hc *http.Client, opts ...JevOption) *JevClient {
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	c := &JevClient{
		apiURL:     strings.TrimSpace(apiURL),
		apiKey:     strings.TrimSpace(apiKey),
		httpClient: hc,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	// Auto-detect OpenRouter if provider wasn't explicitly set or if provider == "openrouter"
	lowerURL := strings.ToLower(c.apiURL)
	if c.provider == "" {
		if strings.Contains(lowerURL, "openrouter.ai") || strings.Contains(lowerURL, "/chat/completions") {
			c.provider = ProviderOpenRouter
		} else {
			c.provider = ProviderJev
		}
	}

	if c.provider == ProviderOpenRouter {
		if c.apiURL == "" || strings.Contains(lowerURL, "jevfast.com") || strings.Contains(lowerURL, "api.jev.ai") {
			c.apiURL = DefaultOpenRouterURL
		}
		if c.model == "" {
			c.model = DefaultOpenRouterModel
		}
	} else {
		if c.model == "" {
			c.model = DefaultJevModel
		}
	}

	return c
}

// Provider returns the active AI provider ("jev" or "openrouter").
func (c *JevClient) Provider() string {
	return c.provider
}

// Model returns the configured model identifier.
func (c *JevClient) Model() string {
	return c.model
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

		// Queue for Tier 3 (Jev / OpenRouter decision model)
		pendingJev = append(pendingJev, tx)
	}

	// Tier 3: Jev Decision Model (native Jev or via OpenRouter)
	for _, tx := range pendingJev {
		catID, err := c.callDecisionModel(ctx, tx, optionIDs)
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

func (c *JevClient) callDecisionModel(ctx context.Context, tx TransactionInput, optionIDs []string) (string, error) {
	if c.provider == ProviderOpenRouter {
		return c.callOpenRouterDecision(ctx, tx, optionIDs)
	}
	return c.callJevDecision(ctx, tx, optionIDs)
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
	Category  string `json:"category"`
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
		Model: c.model,
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

type openRouterMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openRouterResponseFormat struct {
	Type string `json:"type"`
}

type openRouterChatRequest struct {
	Model          string                    `json:"model"`
	Messages       []openRouterMessage       `json:"messages"`
	Temperature    float64                   `json:"temperature"`
	ResponseFormat *openRouterResponseFormat `json:"response_format,omitempty"`
}

type openRouterChatResponse struct {
	Choices []struct {
		Message openRouterMessage `json:"message"`
	} `json:"choices"`
}

func (c *JevClient) callOpenRouterDecision(ctx context.Context, tx TransactionInput, optionIDs []string) (string, error) {
	if c.apiURL == "" || c.apiKey == "" {
		return "", errors.New("openrouter: API URL or key not configured")
	}

	systemPrompt := fmt.Sprintf(
		"You are Jev, a financial transaction classification model for Finperk. "+
			"Select the single most accurate global category ID from the allowed list: [%s]. "+
			"Respond ONLY with a JSON object in the format {\"category\": \"<category_id>\"}.",
		strings.Join(optionIDs, ", "),
	)
	userPrompt := fmt.Sprintf(
		"description: %s | creditor: %s | amount_cents: %d",
		tx.Description, tx.Creditor, tx.AmountCents,
	)

	payload := openRouterChatRequest{
		Model: c.model,
		Messages: []openRouterMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		Temperature:    0,
		ResponseFormat: &openRouterResponseFormat{Type: "json_object"},
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
	req.Header.Set("HTTP-Referer", "https://app.finperk.es")
	req.Header.Set("X-Title", "Finperk")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("openrouter: unexpected status %d", resp.StatusCode)
	}

	var chatResp openRouterChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", err
	}
	if len(chatResp.Choices) == 0 {
		return "", errors.New("openrouter: empty choices")
	}

	content := strings.TrimSpace(chatResp.Choices[0].Message.Content)
	// Strip optional markdown code fences if the model wrapped its JSON output
	content = strings.TrimPrefix(content, "```json")
	content = strings.TrimPrefix(content, "```")
	content = strings.TrimSuffix(content, "```")
	content = strings.TrimSpace(content)

	var decision jevDecisionResponse
	if err := json.Unmarshal([]byte(content), &decision); err == nil {
		if decision.Category != "" {
			return decision.Category, nil
		}
		if d, ok := decision.Decisions["category"]; ok && d.Choice != "" {
			return d.Choice, nil
		}
	}

	// Fallback: check if the model returned the raw category ID directly
	for _, id := range optionIDs {
		if content == id {
			return id, nil
		}
	}
	return "", errors.New("openrouter: could not parse category decision")
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
