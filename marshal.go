package nosfranz

import (
	"encoding/json"

	"github.com/raaaaaaaay86/noskafka"
)

// marshalProducible serialises msg to bytes.
// If msg implements interface{ Marshal() ([]byte, error) }, that is used;
// otherwise the value is JSON-encoded.
type marshaler interface {
	Marshal() ([]byte, error)
}

func marshalProducible(msg noskafka.Producible) ([]byte, error) {
	if m, ok := msg.(marshaler); ok {
		return m.Marshal()
	}

	return json.Marshal(msg)
}
