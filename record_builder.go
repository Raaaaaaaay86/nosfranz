package nosfranz

import "github.com/twmb/franz-go/pkg/kgo"

// RecordBuilder implements noskafka.ProduceOptionSetter and accumulates
// per-produce overrides (key, topic prefix/suffix, headers).
type RecordBuilder struct {
	key         []byte
	topicPrefix string
	topicSuffix string
	headers     []kgo.RecordHeader
}

func (r *RecordBuilder) SetKey(key []byte) {
	r.key = key
}

func (r *RecordBuilder) SetTopicPrefix(prefix string) {
	r.topicPrefix = prefix
}

func (r *RecordBuilder) SetTopicSuffix(suffix string) {
	r.topicSuffix = suffix
}

func (r *RecordBuilder) AddHeader(key string, value []byte) {
	r.headers = append(r.headers, kgo.RecordHeader{
		Key:   key,
		Value: value,
	})
}

func (r *RecordBuilder) topic(base string) string {
	return r.topicPrefix + base + r.topicSuffix
}
