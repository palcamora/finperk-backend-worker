package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the runtime configuration for backend-worker.
type Config struct {
	MongoDBURI          string
	GoCardlessSecretID  string
	GoCardlessSecretKey string
	JevAPIKey           string
	JevAPIURL           string
	NATSURL             string
	SMTPHost            string
	SMTPPort            int
	SMTPUser            string
	SMTPPassword        string
	SMTPFrom            string
	FCMCredentialsPath  string
	APNSKeyPath         string
	APNSKeyID           string
	APNSTeamID          string
	SchedulerInterval   time.Duration
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

	return Config{
		MongoDBURI:          getEnv("MONGODB_URI", "mongodb://localhost:27017/finperk?replicaSet=rs0"),
		GoCardlessSecretID:  getEnv("GOCARDLESS_SECRET_ID", ""),
		GoCardlessSecretKey: getEnv("GOCARDLESS_SECRET_KEY", ""),
		JevAPIKey:           getEnv("JEV_API_KEY", ""),
		JevAPIURL:           getEnv("JEV_API_URL", "https://api.jevfast.com/v1/decisions"),
		NATSURL:             getEnv("NATS_URL", "nats://localhost:4222"),
		SMTPHost:            getEnv("SMTP_HOST", "smtp.example.com"),
		SMTPPort:            smtpPort,
		SMTPUser:            getEnv("SMTP_USER", ""),
		SMTPPassword:        getEnv("SMTP_PASSWORD", ""),
		SMTPFrom:            getEnv("SMTP_FROM", "noreply@finperk.app"),
		FCMCredentialsPath:  getEnv("FCM_CREDENTIALS_PATH", "/etc/finperk/fcm-credentials.json"),
		APNSKeyPath:         getEnv("APNS_KEY_PATH", "/etc/finperk/apns-key.p8"),
		APNSKeyID:           getEnv("APNS_KEY_ID", ""),
		APNSTeamID:          getEnv("APNS_TEAM_ID", ""),
		SchedulerInterval:   schedInt,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
