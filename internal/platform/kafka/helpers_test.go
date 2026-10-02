package kafka

import (
	"io"
	"log/slog"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

// testConfig is a client configuration that never needs a broker to be
// created (franz-go connects lazily).
func testConfig() config.Kafka {
	return config.Kafka{Brokers: []string{"localhost:1"}, DialTimeout: time.Second, DeliveryTimeout: time.Second}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
