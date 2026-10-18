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

	// 1. Connect to NATS JetStream (consumers + optional GoCardless KV token store)
	var jsOpts gocardless.TokenStoreOptions
	jsOpts.Backend = gocardless.ParseTokenBackend(cfg.GoCardlessTokenStore)

	natsBus, err := worker.ConnectNATSBus(cfg.NATSURL)
	if err != nil {
		slog.Warn("NATS unavailable at startup, running scheduler without JetStream bus", "error", err)
	} else {
		defer natsBus.Close()
		jsOpts.JetStream = natsBus.JetStream()
		slog.Info("connected to NATS JetStream", "url", cfg.NATSURL)
	}

	// 2. Connect to MongoDB Replica Set (with fallback to MemoryWorkerStore if MongoDB is not running locally)
	connectCtx, cancelConnect := context.WithTimeout(ctx, 5*time.Second)
	mongoStore, mongoErr := repository.ConnectMongoWorker(connectCtx, cfg.MongoDBURI, "finperk")
	cancelConnect()
	if mongoErr != nil {
		slog.Warn("MongoDB unavailable at startup, falling back to in-memory worker store", "error", mongoErr)
	} else {
		defer func() { _ = mongoStore.Close(context.Background()) }()
		jsOpts.MongoDB = mongoStore.Database()
		slog.Info("connected to MongoDB Replica Set", "uri", cfg.MongoDBURI)
	}

	// 3. Select GoCardless token store ("nats" queue KV or "mongo" collection) and initialize clients
	tokenStore, activeBackend, tsErr := gocardless.NewSelectableTokenStore(jsOpts)
	if tsErr != nil {
		slog.Warn("failed to initialize preferred GoCardless token store, falling back to memory", "error", tsErr)
		tokenStore = gocardless.NewMemoryTokenStore()
		activeBackend = gocardless.TokenBackendMemory
	}
	slog.Info("configured GoCardless token store", "requested", cfg.GoCardlessTokenStore, "active", activeBackend)

	gcClient := gocardless.NewClient(
		cfg.GoCardlessSecretID,
		cfg.GoCardlessSecretKey,
		gocardless.WithTokenStore(tokenStore),
	)
	jevClient := ai.NewJevClient(
		cfg.JevAPIURL,
		cfg.JevAPIKey,
		nil,
		ai.WithProvider(cfg.JevProvider),
		ai.WithModel(cfg.JevModel),
	)
	slog.Info("configured Jev AI categorizer", "provider", jevClient.Provider(), "model", jevClient.Model())

	notifier := notify.NewDispatcherFromConfig(notify.Config{
		Enabled:            cfg.NotificationsEnabled,
		SMTPHost:           cfg.SMTPHost,
		SMTPPort:           cfg.SMTPPort,
		SMTPUser:           cfg.SMTPUser,
		SMTPPassword:       cfg.SMTPPassword,
		SMTPFrom:           cfg.SMTPFrom,
		FCMCredentialsPath: cfg.FCMCredentialsPath,
		APNSKeyPath:        cfg.APNSKeyPath,
		APNSKeyID:          cfg.APNSKeyID,
		APNSTeamID:         cfg.APNSTeamID,
	})
	slog.Info("configured notification dispatcher",
		"enabled", notifier.Enabled(),
		"email_enabled", notifier.EmailEnabled(),
		"push_enabled", notifier.PushEnabled(),
	)

	eng := worker.NewEngine(gcClient, jevClient, notifier, cfg.SchedulerInterval).WithNATSBus(natsBus)
	if mongoErr == nil {
		eng.WithStore(mongoStore)
	}

	if err := eng.Run(ctx); err != nil {
		slog.Error("worker error", "error", err)
		os.Exit(1)
	}
}
