package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"backend-worker/internal/ai"
	"backend-worker/internal/config"
	"backend-worker/internal/notify"
	"backend-worker/internal/worker"
	"pkg/gocardless"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()

	gcClient := gocardless.NewClient(cfg.GoCardlessSecretID, cfg.GoCardlessSecretKey)
	jevClient := ai.NewJevClient(cfg.JevAPIURL, cfg.JevAPIKey, nil)
	notifier := notify.NewDispatcher(cfg.SMTPFrom)

	eng := worker.NewEngine(gcClient, jevClient, notifier, cfg.SchedulerInterval)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := eng.Run(ctx); err != nil {
		slog.Error("worker error", "error", err)
		os.Exit(1)
	}
}
