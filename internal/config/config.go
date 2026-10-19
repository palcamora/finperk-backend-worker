package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the runtime configuration for backend-worker.
type Config struct {
	MongoDBURI           string
	GoCardlessSecretID   string
	GoCardlessSecretKey  string
	GoCardlessTokenStore string
	JevProvider          string
	JevModel             string
	JevAPIKey            string
	JevAPIURL            string
	NATSURL              string
	NotificationsEnabled bool
	SMTPHost             string
	SMTPPort             int
	SMTPUser             string
	SMTPPassword         string
	SMTPFrom             string
	FCMCredentialsPath   string
	APNSKeyPath          string
	APNSKeyID            string
	APNSTeamID           string
	SchedulerInterval    time.Duration
}

// Load reads environment variables and returns a populated worker Config.
func Load() Config {
	smtpPort, err := strconv.Atoi(getEnv("SMTP_PORT", "587"))
	if err != nil {
		smtpPort = 587
	}
	schedInt, err := time.ParseDuration(getEnv("SCHEDULER_INTERVAL", "12h"))
	if err != nil {
		schedInt = 12 * time.Hour
	}
	notifEnabled, err := strconv.ParseBool(getEnv("NOTIFICATIONS_ENABLED", "false"))
	if err != nil {
		notifEnabled = false
	}

	jevAPIKey := getEnv("OPENROUTER_API_KEY", getEnv("JEV_API_KEY", ""))

	return Config{
		MongoDBURI:           getEnv("MONGODB_URI", "mongodb://localhost:27017/finperk?replicaSet=rs0"),
		GoCardlessSecretID:   getEnv("GOCARDLESS_SECRET_ID", ""),
		GoCardlessSecretKey:  getEnv("GOCARDLESS_SECRET_KEY", ""),
		GoCardlessTokenStore: getEnv("GOCARDLESS_TOKEN_STORE", "nats"),
		JevProvider:          getEnv("JEV_PROVIDER", "openrouter"),
		JevModel:             getEnv("JEV_MODEL", "typesafe/jev-1.14"),
		JevAPIKey:            jevAPIKey,
		JevAPIURL:            getEnv("JEV_API_URL", "https://openrouter.ai/api/alpha/decisions"),
		NATSURL:              getEnv("NATS_URL", "nats://localhost:4222"),
		NotificationsEnabled: notifEnabled,
		SMTPHost:             getEnv("SMTP_HOST", ""),
		SMTPPort:             smtpPort,
		SMTPUser:             getEnv("SMTP_USER", ""),
		SMTPPassword:         getEnv("SMTP_PASSWORD", ""),
		SMTPFrom:             getEnv("SMTP_FROM", ""),
		FCMCredentialsPath:   getEnv("FCM_CREDENTIALS_PATH", ""),
		APNSKeyPath:          getEnv("APNS_KEY_PATH", ""),
		APNSKeyID:            getEnv("APNS_KEY_ID", ""),
		APNSTeamID:           getEnv("APNS_TEAM_ID", ""),
		SchedulerInterval:    schedInt,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
