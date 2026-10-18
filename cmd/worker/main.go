package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"backend-worker/internal/ai"
	"backend-worker/internal/config"
	"backend-worker/internal/notify"
	"backend-worker/internal/repository"
	"backend-worker/internal/worker"
	"pkg/gocardless"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Connect to NATS JetStream (consumers + shared GoCardless KV token store)
	var tokenStore gocardless.TokenStore = gocardless.NewMemoryTokenStore()
	natsBus, err := worker.ConnectNATSBus(cfg.NATSURL)
	if err != nil {
		slog.Warn("NATS unavailable at startup, running scheduler without JetStream bus", "error", err)
	} else {
		defer natsBus.Close()
		if kvStore, kvErr := gocardless.NewNATSTokenStore(natsBus.JetStream()); kvErr == nil {
			tokenStore = kvStore
		}
		slog.Info("connected to NATS JetStream", "url", cfg.NATSURL)
	}

	// 2. Initialize GoCardless, Jev, and Notification clients
	gcClient := gocardless.NewClient(
		cfg.GoCardlessSecretID,
		cfg.GoCardlessSecretKey,
		gocardless.WithTokenStore(tokenStore),
	)
	jevClient := ai.NewJevClient(cfg.JevAPIURL, cfg.JevAPIKey, nil)
	notifier := notify.NewDispatcher(cfg.SMTPFrom)

	eng := worker.NewEngine(gcClient, jevClient, notifier, cfg.SchedulerInterval).WithNATSBus(natsBus)

	// 3. Connect to MongoDB Replica Set (with fallback to MemoryWorkerStore if MongoDB is not running locally)
	connectCtx, cancelConnect := context.WithTimeout(ctx, 5*time.Second)
	mongoStore, err := repository.ConnectMongoWorker(connectCtx, cfg.MongoDBURI, "finperk")
	cancelConnect()
	if err != nil {
		slog.Warn("MongoDB unavailable at startup, falling back to in-memory worker store", "error", err)
	} else {
		defer func() { _ = mongoStore.Close(context.Background()) }()
		slog.Info("connected to MongoDB Replica Set", "uri", cfg.MongoDBURI)
		eng.WithStore(mongoStore)
	}

	if err := eng.Run(ctx); err != nil {
		slog.Error("worker error", "error", err)
		os.Exit(1)
	}
}
