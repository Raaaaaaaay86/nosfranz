package nosfranz

import (
	"time"

	"github.com/raaaaaaaay86/noskafka"
)

type Config struct {
	ConnectionInfo noskafka.ConnectionInfo
	Handlers       []noskafka.HandlerFunc
	BatchHandlers  []noskafka.BatchHandlerFunc
	ProcessTimeout time.Duration
	AutoCommit     bool
	BatchSize      int
	BatchTimeout   time.Duration
}

func (c Config) GetBatchSize() int {
	if c.BatchSize <= 0 {
		return 1
	}
	return c.BatchSize
}

func (c Config) GetBatchTimeout() time.Duration {
	if c.BatchTimeout <= 0 {
		return 5 * time.Second
	}
	return c.BatchTimeout
}
