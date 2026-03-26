package nosfranz

import (
	"fmt"
)

func toStringSlice[T fmt.Stringer](stringers []T) []string {
	result := make([]string, len(stringers))
	for i, stringer := range stringers {
		result[i] = stringer.String()
	}
	return result
}
