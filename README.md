# nosfranz

`nosfranz` is the [franz-go](https://github.com/twmb/franz-go) backed implementation of the [`noskafka`](https://github.com/raaaaaaaay86/noskafka) `Consumer` and `Producer` interfaces. It handles polling, record processing, offset committing, and optional auto-topic creation.

## Installation

```bash
go get github.com/raaaaaaaay86/nosfranz
```

## Consumer

### Setup

```go
package main

import (
    "context"
    "encoding/json"
    "log/slog"
    "time"

    "github.com/raaaaaaaay86/noskafka"
    "github.com/raaaaaaaay86/nosfranz"
)

func handleOrder(ctx *noskafka.Context) {
    var order OrderEvent
    if err := json.Unmarshal(ctx.Message.Value, &order); err != nil {
        ctx.Abort(err)
        return
    }
    slog.Info("order received", "id", order.ID, "topic", ctx.Message.Topic)
    ctx.Next()
}

func main() {
    manager := noskafka.NewManager()

    _, err := manager.Add(func() (noskafka.Consumer, error) {
        return nosfranz.NewFranzConsumer(nosfranz.Config{
            ConnectionInfo: noskafka.ConnectionInfo{
                Brokers: []string{"localhost:9092"},
                GroupId: noskafka.NewGroupId("order-service"),
                Topics:  []noskafka.Topic{"orders.created"},
            },
            Handlers:       []noskafka.HandlerFunc{handleOrder},
            ProcessTimeout: 10 * time.Second,
            AutoCommit:     false,
        }), nil
    })
    if err != nil {
        panic(err)
    }

    if err := manager.Start(context.Background()); err != nil {
        panic(err)
    }
    defer manager.Stop()
}
```

### Batch Consumer

```go
func handleBatch(ctx *noskafka.BatchContext) {
    for _, msg := range ctx.Messages {
        slog.Info("batch message", "offset", msg.Offset, "value", string(msg.Value))
    }
    ctx.Next()
}

nosfranz.NewFranzConsumer(nosfranz.Config{
    ConnectionInfo: noskafka.ConnectionInfo{
        Brokers: []string{"localhost:9092"},
        GroupId: noskafka.NewGroupId("analytics"),
        Topics:  []noskafka.Topic{"events.raw"},
    },
    BatchHandlers: []noskafka.BatchHandlerFunc{handleBatch},
    BatchSize:     100,
    BatchTimeout:  5 * time.Second,
    AutoCommit:    false,
})
```

### Auto Commit

When `AutoCommit: true`, franz-go commits offsets automatically in the background. When `false`, `nosfranz` commits only the records whose handler chain completed without error.

## Producer

### Setup

```go
producer, err := nosfranz.NewFranzProducer(nosfranz.ProducerConfig{
    Brokers:    []string{"localhost:9092"},
    BufferSize: 256,
})
if err != nil {
    panic(err)
}

producer.Start(ctx)
defer producer.Close()
```

### Produce Messages

Messages must implement `noskafka.Producible` (i.e. have a `GetTopic() string` method). By default they are JSON-encoded; implement `Marshal() ([]byte, error)` to override serialization.

```go
type OrderCreatedEvent struct {
    ID     int `json:"id"`
    Amount int `json:"amount"`
}

func (e OrderCreatedEvent) GetTopic() string { return "orders.created" }

err := producer.Produce(ctx, []noskafka.Producible{
    OrderCreatedEvent{ID: 1, Amount: 100},
    OrderCreatedEvent{ID: 2, Amount: 200},
})
```

### With Produce Options

```go
err := producer.Produce(ctx, messages,
    noskafka.WithKey([]byte("order-123")),
    noskafka.SetTopicPrefix("staging-"),
)
```

### Auto-Create Topics

```go
producer, err := nosfranz.NewFranzProducer(nosfranz.ProducerConfig{
    Brokers: []string{"localhost:9092"},
    AutoCreateTopic: &nosfranz.TopicConfig{
        NumPartitions:     3,
        ReplicationFactor: 1,
    },
})
```

Topics are created on first produce and cached; subsequent produces to the same topic skip the admin call.

## Config Reference

### `Config` (Consumer)

| Field            | Type                          | Description                                               |
|------------------|-------------------------------|-----------------------------------------------------------|
| `ConnectionInfo` | `noskafka.ConnectionInfo`     | Brokers, group ID, topics                                 |
| `Handlers`       | `[]noskafka.HandlerFunc`      | Single-message handler chain                              |
| `BatchHandlers`  | `[]noskafka.BatchHandlerFunc` | Batch handler chain                                       |
| `ProcessTimeout` | `time.Duration`               | Per-record processing timeout (0 = no timeout)            |
| `AutoCommit`     | `bool`                        | Auto-commit (true) or manual commit after success (false) |
| `BatchSize`      | `int`                         | Batch flush size (default: 1)                             |
| `BatchTimeout`   | `time.Duration`               | Batch flush interval (default: 5s)                        |

### `ProducerConfig`

| Field             | Type           | Description                                          |
|-------------------|----------------|------------------------------------------------------|
| `Brokers`         | `[]string`     | Kafka broker addresses                               |
| `BufferSize`      | `int`          | Internal produce request buffer (default: 100)       |
| `AutoCreateTopic` | `*TopicConfig` | Auto-create missing topics (nil = disabled)          |

### `TopicConfig`

| Field               | Type    | Description         |
|---------------------|---------|---------------------|
| `NumPartitions`     | `int32` | Number of partitions|
| `ReplicationFactor` | `int16` | Replication factor  |
