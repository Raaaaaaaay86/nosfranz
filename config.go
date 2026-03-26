package nosfranz

import (
	"time"

	"github.com/nos/noskafka"
)

type Config struct {
	ConnectionInfo noskafka.ConnectionInfo
	Handlers       []noskafka.HandlerFunc
	ProcessTimeout time.Duration
	AutoCommit     bool
}
