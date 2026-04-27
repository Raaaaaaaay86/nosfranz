package nosfranz

import (
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/raaaaaaaay86/noskafka"
)

func getHandlerName[T noskafka.HandlerFunc | noskafka.BatchHandlerFunc](handler T) (string, bool) {
	ptr := reflect.ValueOf(handler).Pointer()
	handlerName := runtime.FuncForPC(ptr).Name()

	base := filepath.Base(handlerName)

	namespaces := strings.Split(base, ".")

	if len(namespaces) == 0 {
		return "", false
	}

	element := namespaces[len(namespaces)-1]

	splitted := strings.Split(element, "-")

	if len(splitted) == 0 {
		return "", false
	}

	return splitted[0], true
}
