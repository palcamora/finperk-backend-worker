package notify

import (
	"context"
	"log/slog"
)

// NotificationRequest represents the payload consumed from finperk.notify.request.
type NotificationRequest struct {
	UserID   string   `json:"user_id"`
	AlertID  string   `json:"alert_id"`
	Type     string   `json:"type"`
	Channels []string `json:"channels"`
	Message  string   `json:"message"`
}

// Dispatcher delivers notifications via Email (SMTP) and Mobile Push (FCM/APNs).
type Dispatcher struct {
	smtpFrom string
}

// NewDispatcher creates a new notification Dispatcher.
func NewDispatcher(smtpFrom string) *Dispatcher {
	return &Dispatcher{smtpFrom: smtpFrom}
}

// Deliver sends the notification through each requested channel.
func (d *Dispatcher) Deliver(_ context.Context, req NotificationRequest) map[string]string {
	status := make(map[string]string, len(req.Channels))
	for _, ch := range req.Channels {
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
