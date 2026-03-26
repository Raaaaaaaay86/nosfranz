package nosfranz

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/nos/noskafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ noskafka.Consumer = (*FranzConsumer)(nil)

type FranzConsumer struct {
	identifier noskafka.Identifier
	signalChan chan noskafka.Signal
	config     Config
	mu         sync.RWMutex
	client     *kgo.Client
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	started    atomic.Bool
	closed     atomic.Bool
}

func NewFranzConsumer(config Config) *FranzConsumer {
	return &FranzConsumer{
		config: config,
	}
}

func (f *FranzConsumer) Start(ctx context.Context) {
	if f.started.Swap(true) { // for idempotency
		return
	}
	f.closed.Store(false)

	ctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.wg.Add(1)
	go f.run(ctx)
}

func (f *FranzConsumer) run(ctx context.Context) {
	defer f.wg.Done()

	cl, err := f.initClient()
	if err != nil {
		f.sendSignal(noskafka.FailureSignalLevel, "failed to create franz-go client", err)
		slog.Error("failed to create franz-go client", "error", err)
		return
	}

	f.mu.Lock()
	f.client = cl
	f.mu.Unlock()

	defer f.cleanupClient()

	for {
		client := f.getClient()
		if client == nil {
			return
		}

		fetches := client.PollRecords(ctx, -1)
		if fetches.IsClientClosed() {
			return
		}
		if err := fetches.Err(); err != nil && err != context.Canceled {
			f.sendSignal(noskafka.ErrorSignalLevel, "poll error", err)
		}

		f.processFetches(ctx, client, fetches)

		select {
		case <-ctx.Done():
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
		if f.processRecord(ctx, r) {
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

func (f *FranzConsumer) processRecord(ctx context.Context, r *kgo.Record) bool {
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

	pctx := ctx
	if f.config.ProcessTimeout > 0 {
		var cancel context.CancelFunc
		pctx, cancel = context.WithTimeout(ctx, f.config.ProcessTimeout)
		defer cancel()
	}

	nctx := noskafka.NewContext(pctx, f.config.Handlers)
	nctx.SetMessage(msg)
	nctx.Next()

	return nctx.Err() == nil
}

func (f *FranzConsumer) Stop(ctx context.Context) <-chan struct{} {
	if f.cancel != nil {
		f.cancel()
	}

	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		f.started.Store(false)
		close(done)
	}()

	return done
}

func (f *FranzConsumer) GetConnectionInfo() noskafka.ConnectionInfo {
	return f.config.ConnectionInfo
}

func (f *FranzConsumer) SetIdentifier(identifier noskafka.Identifier) {
	f.identifier = identifier
}

func (f *FranzConsumer) GetIdentifier() noskafka.Identifier {
	return f.identifier
}

func (f *FranzConsumer) SetSignalChan(ch chan noskafka.Signal) {
	f.signalChan = ch
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
