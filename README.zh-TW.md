# nosfranz

`nosfranz` 是基於 [franz-go](https://github.com/twmb/franz-go) 的 [`noskafka`](https://github.com/raaaaaaaay86/noskafka) `Consumer` 與 `Producer` 介面具體實作。它處理 polling、記錄處理、offset 提交，以及選擇性的自動建立 topic。

## 安裝

```bash
go get github.com/raaaaaaaay86/nosfranz
```

## Consumer

### 設定

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
    slog.Info("收到訂單", "id", order.ID, "topic", ctx.Message.Topic)
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
            AutoCommit:     false, // 成功處理後手動 commit
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

### 批次消費

```go
func handleBatch(ctx *noskafka.BatchContext) {
    for _, msg := range ctx.Messages {
        slog.Info("批次訊息", "offset", msg.Offset, "value", string(msg.Value))
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

### 自動提交

`AutoCommit: true` 時，franz-go 會在背景自動提交 offset。設為 `false` 時，`nosfranz` 只提交 handler chain 成功完成的記錄。

## Producer

### 設定

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

### 發布訊息

訊息需實作 `noskafka.Producible`（即有 `GetTopic() string` 方法）。預設使用 JSON 編碼；實作 `Marshal() ([]byte, error)` 可自訂序列化方式。

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

### 搭配 Produce 選項

```go
err := producer.Produce(ctx, messages,
    noskafka.WithKey([]byte("order-123")),  // partition key
    noskafka.SetTopicPrefix("staging-"),     // "staging-orders.created"
)
```

### 自動建立 Topic

```go
producer, err := nosfranz.NewFranzProducer(nosfranz.ProducerConfig{
    Brokers: []string{"localhost:9092"},
    AutoCreateTopic: &nosfranz.TopicConfig{
        NumPartitions:     3,
        ReplicationFactor: 1,
    },
})
```

Topic 在第一次 produce 時建立並快取；後續對同一 topic 的 produce 會跳過 admin 呼叫。

## Config 說明

### `Config`（Consumer）

| 欄位             | 型別                          | 說明                                                  |
|------------------|-------------------------------|-------------------------------------------------------|
| `ConnectionInfo` | `noskafka.ConnectionInfo`     | Broker 位址、group ID、topic 清單                     |
| `Handlers`       | `[]noskafka.HandlerFunc`      | 單筆訊息 handler chain                                |
| `BatchHandlers`  | `[]noskafka.BatchHandlerFunc` | 批次訊息 handler chain                                |
| `ProcessTimeout` | `time.Duration`               | 每筆記錄的處理逾時（0 = 無逾時）                      |
| `AutoCommit`     | `bool`                        | 自動提交（true）或成功後手動提交（false）              |
| `BatchSize`      | `int`                         | 批次 flush 大小（預設：1）                            |
| `BatchTimeout`   | `time.Duration`               | 批次 flush 間隔（預設：5s）                           |

### `ProducerConfig`

| 欄位              | 型別           | 說明                                      |
|-------------------|----------------|-------------------------------------------|
| `Brokers`         | `[]string`     | Kafka broker 位址清單                     |
| `BufferSize`      | `int`          | 內部 produce 請求緩衝區大小（預設：100）  |
| `AutoCreateTopic` | `*TopicConfig` | 自動建立缺少的 topic（nil = 停用）         |

### `TopicConfig`

| 欄位                | 型別    | 說明         |
|---------------------|---------|--------------|
| `NumPartitions`     | `int32` | Partition 數量 |
| `ReplicationFactor` | `int16` | 複製因子     |
