package nosfranz

import (
	"context"
	"sync"

	"github.com/raaaaaaaay86/noskafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/xerrors"
)

var _ noskafka.Producer = (*Producer)(nil)

// ProducerConfig holds configuration for the franz-go Producer.
type ProducerConfig struct {
	Brokers []string
	// BufferSize is the capacity of the input channel. When full, Produce blocks (backpressure).
	BufferSize int
	// Workers is the number of goroutines concurrently consuming from the input channel.
	Workers int
}

type produceJob struct {
	ctx     context.Context
	records []*kgo.Record
	errCh   chan error
}

type Producer struct {
	client *kgo.Client
	in     chan produceJob
	quit   chan struct{}
	wg     sync.WaitGroup
}

func NewFranzProducer(config ProducerConfig) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers...),
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create franz-go client: %w", err)
	}

	bufSize := config.BufferSize
	if bufSize <= 0 {
		bufSize = 100
	}
	workers := config.Workers
	if workers <= 0 {
		workers = 1
	}

	p := &Producer{
		client: client,
		in:     make(chan produceJob, bufSize),
		quit:   make(chan struct{}),
	}

	for range workers {
		p.wg.Add(1)
		go p.worker()
	}

	return p, nil
}

func (p *Producer) worker() {
	defer p.wg.Done()
	for {
		select {
		case job := <-p.in:
			err := p.client.ProduceSync(job.ctx, job.records...).FirstErr()
			job.errCh <- err
		case <-p.quit:
			return
		}
	}
}

func (p *Producer) Produce(ctx context.Context, messages []noskafka.Producible, opts ...noskafka.ProduceOption) error {
	builder := &RecordBuilder{}
	for _, opt := range opts {
		if err := opt(builder); err != nil {
			return xerrors.Errorf("failed to apply produce option: %w", err)
		}
	}

	records := make([]*kgo.Record, 0, len(messages))
	for _, msg := range messages {
		value, err := marshalProducible(msg)
		if err != nil {
			return xerrors.Errorf("failed to marshal message: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic:   builder.topic(msg.GetTopic()),
			Key:     builder.key,
			Value:   value,
			Headers: builder.headers,
		})
	}

	errCh := make(chan error, 1)
	job := produceJob{ctx: ctx, records: records, errCh: errCh}

	select {
	case p.in <- job:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Producer) Close() {
	close(p.quit)
	p.wg.Wait()
	p.client.Close()
}
