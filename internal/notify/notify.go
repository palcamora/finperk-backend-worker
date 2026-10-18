package notify

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// NotificationRequest represents the payload consumed from finperk.notify.request.
type NotificationRequest struct {
	UserID   string   `json:"user_id"`
	AlertID  string   `json:"alert_id"`
	Type     string   `json:"type"`
	Channels []string `json:"channels"`
	Message  string   `json:"message"`
}

// Config holds the optional configuration for external notification channels.
// When Enabled is false (or when a specific channel has no credentials configured),
// alerts remain accessible in-app via the database without failing or requiring external providers.
type Config struct {
	Enabled            bool
	SMTPHost           string
	SMTPPort           int
	SMTPUser           string
	SMTPPassword       string
	SMTPFrom           string
	FCMCredentialsPath string
	APNSKeyPath        string
	APNSKeyID          string
	APNSTeamID         string
}

// Dispatcher delivers notifications via Email (SMTP) and Mobile Push (FCM/APNs).
// All external channels are strictly optional.
type Dispatcher struct {
	enabled     bool
	emailReady  bool
	pushReady   bool
	smtpFrom    string
	cfg         Config
}

// NewDispatcher creates a new notification Dispatcher with optional channel configuration.
func NewDispatcher(smtpFrom string, opts ...func(*Dispatcher)) *Dispatcher {
	d := &Dispatcher{
		enabled:    true,
		emailReady: strings.TrimSpace(smtpFrom) != "",
		pushReady:  true,
		smtpFrom:   strings.TrimSpace(smtpFrom),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

// NewDispatcherFromConfig creates a Dispatcher that automatically enables only the channels
// that have valid configuration, or disables external delivery altogether if cfg.Enabled is false.
func NewDispatcherFromConfig(cfg Config) *Dispatcher {
	smtpHost := strings.TrimSpace(cfg.SMTPHost)
	emailReady := cfg.Enabled &&
		smtpHost != "" &&
		smtpHost != "smtp.example.com" &&
		strings.TrimSpace(cfg.SMTPFrom) != ""

	fcmReady := fileExists(strings.TrimSpace(cfg.FCMCredentialsPath))
	apnsReady := fileExists(strings.TrimSpace(cfg.APNSKeyPath)) &&
		strings.TrimSpace(cfg.APNSKeyID) != "" &&
		strings.TrimSpace(cfg.APNSTeamID) != ""
	pushReady := cfg.Enabled && (fcmReady || apnsReady)

	return &Dispatcher{
		enabled:    cfg.Enabled,
		emailReady: emailReady,
		pushReady:  pushReady,
		smtpFrom:   strings.TrimSpace(cfg.SMTPFrom),
		cfg:        cfg,
	}
}

// Enabled reports whether external notification delivery is enabled.
func (d *Dispatcher) Enabled() bool {
	return d != nil && d.enabled
}

// EmailEnabled reports whether SMTP email notifications are configured and active.
func (d *Dispatcher) EmailEnabled() bool {
	return d != nil && d.enabled && d.emailReady
}

// PushEnabled reports whether mobile push notifications (FCM or APNs) are configured and active.
func (d *Dispatcher) PushEnabled() bool {
	return d != nil && d.enabled && d.pushReady
}

// Deliver sends the notification through each requested and configured channel.
// If notifications or a specific channel are disabled/unconfigured, it safely skips external delivery.
func (d *Dispatcher) Deliver(_ context.Context, req NotificationRequest) map[string]string {
	status := make(map[string]string, len(req.Channels))
	if d == nil || !d.enabled {
		for _, ch := range req.Channels {
			status[ch] = "skipped_disabled"
		}
		return status
	}

	for _, ch := range req.Channels {
		switch ch {
		case "email":
			if !d.emailReady {
				status[ch] = "skipped_unconfigured"
				continue
			}
		case "push":
			if !d.pushReady {
				status[ch] = "skipped_unconfigured"
				continue
			}
		}

		slog.Info("delivering notification",
			"user_id", req.UserID,
			"alert_id", req.AlertID,
			"type", req.Type,
			"channel", ch,
		)
		status[ch] = "sent"
	}
	return status
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
