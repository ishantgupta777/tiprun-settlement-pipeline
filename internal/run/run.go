// Package run provides a shared context that is cancelled on SIGINT/SIGTERM so
// every service shuts down gracefully (finishing in-flight commits) rather
// than being hard-killed.
package run

import (
	"context"
	"os/signal"
	"syscall"
)

// SignalContext returns a context cancelled on interrupt/terminate signals.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}
