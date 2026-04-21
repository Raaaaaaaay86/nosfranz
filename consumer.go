package nosfranz

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/raaaaaaaay86/noskafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
)

var _ noskafka.Consumer = (*FranzConsumer)(nil)

type FranzConsumer struct {
	identifier     noskafka.Identifier
	signalChan     chan<- noskafka.Signal
	tracerProvider trace.TracerProvider
	config         Config
	mu             sync.RWMutex
	client         *kgo.Client
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	started        atomic.Bool
	closed         atomic.Bool
}

func NewFranzConsumer(config Config) *FranzConsumer {
	return &FranzConsumer{
		config: config,
	}
}

func (f *FranzConsumer) Start(ctx context.Context) error {
	if f.started.Swap(true) { // for idempotency
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
	go f.run()

	return nil
}

func (f *FranzConsumer) run() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("topic listening panic", "identifier", f.identifier, "recover", r, "brokers", f.config.ConnectionInfo.Brokers, "groupId", f.config.ConnectionInfo.GroupId, "topics", f.config.ConnectionInfo.Topics)
		} else {
			slog.Info("topic listening exited", "identifier", f.identifier, "brokers", f.config.ConnectionInfo.Brokers, "groupId", f.config.ConnectionInfo.GroupId, "topics", f.config.ConnectionInfo.Topics)
		}
	}()
	defer f.wg.Done()
	defer f.cleanupClient()

	for {
		client := f.getClient()
		if client == nil {
			return
		}

		fetches := client.PollRecords(f.ctx, -1)
		if fetches.IsClientClosed() {
			return
		}
		if err := fetches.Err(); err != nil && err != context.Canceled {
			f.sendSignal(noskafka.ErrorSignalLevel, "poll error", err)
			// If error is fatal, we might want to send FailureSignalLevel
			// For now, let's keep it as Error unless it's a known fatal error
		}

		f.processFetches(f.ctx, client, fetches)

		select {
		case <-f.ctx.Done():
			return
		default:
		}
	}
}

func (f *FranzConsumer) initClient() (*kgo.Client, error) {
	topics := make([]string, len(f.config.ConnectionInfo.Topics))
	for i, t := range f.config.ConnectionInfo.Topics {
		topics[i] = string(t)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(f.config.ConnectionInfo.Brokers...),
		kgo.ConsumerGroup(string(f.config.ConnectionInfo.GroupId)),
		kgo.ConsumeTopics(topics...),
	}

	if !f.config.AutoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
	}

	return kgo.NewClient(opts...)
}

func (f *FranzConsumer) getClient() *kgo.Client {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.client
}

func (f *FranzConsumer) cleanupClient() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.client != nil {
		f.client.Close()
		f.client = nil
	}
}

func (f *FranzConsumer) sendSignal(level noskafka.SignalLevel, message string, err error) {
	if f.signalChan != nil {
		f.signalChan <- noskafka.Signal{
			Level:      level,
			Identifier: f.identifier,
			Message:    message,
			Error:      err,
		}
	}
}

func (f *FranzConsumer) processFetches(ctx context.Context, client *kgo.Client, fetches kgo.Fetches) {
	var processedRecords []*kgo.Record

	fetches.EachRecord(func(r *kgo.Record) {
		if f.processRecord(r) {
			processedRecords = append(processedRecords, r)
		}
	})

	if !f.config.AutoCommit && len(processedRecords) > 0 {
		if err := client.CommitRecords(ctx, processedRecords...); err != nil {
			slog.Error("batch commit failed", "error", err)
			f.sendSignal(noskafka.ErrorSignalLevel, "batch commit failed", err)
		}
	}
}

func (f *FranzConsumer) processRecord(r *kgo.Record) bool {
	msg := &noskafka.Message{
		Key:       r.Key,
		Partition: int(r.Partition),
		Topic:     r.Topic,
		Offset:    int(r.Offset),
		Value:     r.Value,
		Timestamp: r.Timestamp,
	}
	for _, h := range r.Headers {
		msg.Headers = append(msg.Headers, noskafka.Header{
			Key:   h.Key,
			Value: h.Value,
		})
	}

	ctx := context.Background()

	if f.tracerProvider != nil {
		tctx, span := f.withTracedContext(ctx, r)
		defer span.End()

		ctx = tctx
	}

	if f.config.ProcessTimeout > 0 {
		var cancel context.CancelFunc
		pctx, cancel := context.WithTimeout(ctx, f.config.ProcessTimeout)
		defer cancel()

		ctx = pctx
	}

	nctx := noskafka.NewContext(ctx, f.config.Handlers)
	nctx.SetMessage(msg)
	nctx.Next()

	return nctx.Err() == nil
}

func (f *FranzConsumer) withTracedContext(ctx context.Context, record *kgo.Record) (context.Context, trace.Span) {
	tctx, span := f.tracerProvider.Tracer(TRACER_NAME).Start(ctx, fmt.Sprintf("kafka.%s", f.config.ConnectionInfo.Topics.JoinedBy(":")))

	span.SetAttributes(getSingleConsumeAttributes(record, f.config.ConnectionInfo.GroupId.String())...)

	return tctx, span
}

func (f *FranzConsumer) SetIdentifier(identifier noskafka.Identifier) {
	f.identifier = identifier
}

func (f *FranzConsumer) GetIdentifier() noskafka.Identifier {
	return f.identifier
}

func (f *FranzConsumer) SetSignalChan(ch chan<- noskafka.Signal) {
	f.signalChan = ch
}

func (f *FranzConsumer) SetTracerProvider(tracerProvider trace.TracerProvider) {
	f.tracerProvider = tracerProvider
}

func (f *FranzConsumer) GetConnectionInfo() noskafka.ConnectionInfo {
	return f.config.ConnectionInfo
}

func (f *FranzConsumer) Close() error {
	if f.closed.Swap(true) { // for idempotency
		return nil
	}

	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
	f.started.Store(false)

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.client != nil {
		f.client.Close()
		f.client = nil
	}
	return nil
}
