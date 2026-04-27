module github.com/raaaaaaaay86/nosfranz

go 1.25.0

require (
	github.com/raaaaaaaay86/noskafka v0.0.10
	github.com/stretchr/testify v1.11.1
	github.com/twmb/franz-go v1.20.7
	github.com/twmb/franz-go/pkg/kadm v1.17.2
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/trace v1.43.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.12.0 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/raaaaaaaay86/noskafka => ../noskafka
