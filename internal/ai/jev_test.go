package ai

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJevClient_DebugLogging(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	origLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(origLogger)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"answers": {
				"category": {
					"choice": "cat_supermarket"
				}
			}
		}`))
	}))
	defer server.Close()

	client := NewJevClient(server.URL, "dummy-key", server.Client(), WithProvider(ProviderOpenRouter))

	cat, err := client.callJevDecision(context.Background(), TransactionInput{
		ID:          "tx-1",
		Description: "Mercadona",
		AmountCents: -2500,
	}, []CategoryOption{
		{ID: "cat_supermarket", Name: "Supermercado"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cat != "cat_supermarket" {
		t.Fatalf("expected cat_supermarket, got %s", cat)
	}

	logs := buf.String()
	if !strings.Contains(logs, "jev ai request") {
		t.Errorf("expected 'jev ai request' in debug logs, got: %s", logs)
	}
	if !strings.Contains(logs, "jev ai response") {
		t.Errorf("expected 'jev ai response' in debug logs, got: %s", logs)
	}
	if !strings.Contains(logs, "cat_supermarket") {
		t.Errorf("expected response body logged in debug, got: %s", logs)
	}
}
