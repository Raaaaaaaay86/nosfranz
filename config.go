package nosfranz

import (
	"time"

	"github.com/raaaaaaaay86/noskafka"
)

type Config struct {
	ConnectionInfo noskafka.ConnectionInfo
	Handlers       []noskafka.HandlerFunc
	ProcessTimeout time.Duration
	AutoCommit     bool
}
