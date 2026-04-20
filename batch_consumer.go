package nosfranz

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/raaaaaaaay86/noskafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
)

type FranzBatchConsumer struct {
	FranzConsumer // extend FranzConsumer
}

func NewFranzBatchConsumer(config Config) *FranzBatchConsumer {
	return &FranzBatchConsumer{
		FranzConsumer: FranzConsumer{
			config: config,
		},
	}
}

// Start Override FranzConsumer.Start to support batch consuming
func (f *FranzBatchConsumer) Start(ctx context.Context) error {
	if f.started.Swap(true) {
		return nil
	}
	f.closed.Store(false)

	cl, err := f.initClient()
	if err != nil {
		f.started.Store(false)
		return xerrors.Errorf("failed to create franz-go client: %w", err)
	}

	f.mu.Lock()
	f.client = cl
	f.mu.Unlock()

	f.ctx, f.cancel = context.WithCancel(ctx)
	f.wg.Add(1)
	go f.runBatch()

	return nil
}

func (f *FranzBatchConsumer) runBatch() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("batch topic listening panic", "identifier", f.identifier, "recover", r, "brokers", f.config.ConnectionInfo.Brokers, "groupId", f.config.ConnectionInfo.GroupId, "topics", f.config.ConnectionInfo.Topics)
		} else {
			slog.Info("batch topic listening exited", "identifier", f.identifier, "brokers", f.config.ConnectionInfo.Brokers, "groupId", f.config.ConnectionInfo.GroupId, "topics", f.config.ConnectionInfo.Topics)
		}
	}()
	defer f.wg.Done()
	defer f.cleanupClient()

	batchSize := f.config.GetBatchSize()
	batchTimeout := f.config.GetBatchTimeout()
	buffer := make([]*kgo.Record, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		f.processBatch(f.getClient(), buffer)
		buffer = buffer[:0]
		ticker.Reset(batchTimeout)
	}

	for {
		client := f.getClient()
		if client == nil {
			return
		}

		// Use a smaller timeout for PollRecords in batch mode to allow ticker to fire
		pollCtx, pollCancel := context.WithTimeout(f.ctx, 100*time.Millisecond)
		fetches := client.PollRecords(pollCtx, -1)
		pollCancel()

		if err := fetches.Err(); err != nil && err != context.Canceled {
			f.sendSignal(noskafka.ErrorSignalLevel, "poll error", err)
			// If error is fatal, we might want to send FailureSignalLevel
			// For now, let's keep it as Error unless it's a known fatal error
		}

		if fetches.IsClientClosed() {
			return
		}

		fetches.EachRecord(func(r *kgo.Record) {
			buffer = append(buffer, r)
			if len(buffer) >= batchSize {
				flush()
			}
		})

		select {
		case <-f.ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		default:
		}
	}
}

func (f *FranzBatchConsumer) processBatch(client *kgo.Client, records []*kgo.Record) {
	messages := make([]*noskafka.Message, len(records))
	for i, r := range records {
		messages[i] = &noskafka.Message{
			Key:       r.Key,
			Partition: int(r.Partition),
			Topic:     r.Topic,
			Offset:    int(r.Offset),
			Value:     r.Value,
			Timestamp: r.Timestamp,
		}
		for _, h := range r.Headers {
			messages[i].Headers = append(messages[i].Headers, noskafka.Header{
				Key:   h.Key,
				Value: h.Value,
			})
		}
	}

	ctx := context.Background()

	if f.tracerProvider != nil {
		tctx, span := f.withTracedContext(ctx)
		defer span.End()

		ctx = tctx
	}

	if f.config.ProcessTimeout > 0 {
		var cancel context.CancelFunc
		pctx, cancel := context.WithTimeout(ctx, f.config.ProcessTimeout)
		defer cancel()

		ctx = pctx
	}

	bctx := noskafka.NewBatchContext(ctx, f.config.BatchHandlers)
	bctx.SetMessages(messages)
	bctx.Next()

	if !f.config.AutoCommit && bctx.Err() == nil {
		if err := client.CommitRecords(ctx, records...); err != nil {
			slog.Error("batch commit failed", "error", err)
			f.sendSignal(noskafka.ErrorSignalLevel, "batch commit failed", err)
		}
	}
}

func (f *FranzBatchConsumer) withTracedContext(ctx context.Context) (context.Context, trace.Span) {
	tctx, span := f.tracerProvider.Tracer("").Start(ctx, fmt.Sprintf("kafka.batch.%s", f.config.ConnectionInfo.Topics.JoinedBy(":")))

	span.SetAttributes(
		attribute.String("brokers", strings.Join(f.config.ConnectionInfo.Brokers, ",")),
		attribute.String("group_id", f.config.ConnectionInfo.GroupId.String()),
		attribute.String("app.consume_mode", "batch"),
	)

	return tctx, span
}
