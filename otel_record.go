package nosfranz

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

func getSingleConsumeAttributes(record *kgo.Record, groupId string) []attribute.KeyValue {
	attrs := getBasicConsumeAttributes(groupId)

	attrs = append(attrs,
		semconv.MessagingKafkaMessageKey(string(record.Key)),
		semconv.MessagingKafkaOffset(int(record.Offset)),
		attribute.String("messaging.destination.name", record.Topic),
		attribute.String("messaging.operation.type", "receive"),
		attribute.Int("messaging.destination.partition.id", int(record.Partition)),
		attribute.Int("messaging.message.body.size", len(record.Value)),
	)

	return attrs
}

func getBasicConsumeAttributes(groupId string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.operation.type", "receive"),
		attribute.String("messaging.consumer.group.name", groupId),
	}
}

func getBatchConsumeAttributes(records []*kgo.Record, groupId string) []attribute.KeyValue {
	attrs := getBasicConsumeAttributes(groupId)

	attrs = append(attrs,
		attribute.Int("messaging.batch.message_count", len(records)),
		attribute.Int("messaging.batch.body.size", getBatchSize(records)),
	)

	return attrs
}

func getBatchSize(records []*kgo.Record) int {
	total := 0

	for _, record := range records {
		total += len(record.Value)
	}

	return total
}
