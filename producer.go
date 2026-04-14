package nosfranz

import (
	"context"
"github.com/raaaaaaaay86/noskafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/xerrors"
)

var _ noskafka.Producer = (*Producer)(nil)

type ProducerConfig struct {
	Brokers    []string
	BufferSize int
}

type produceRequest struct {
	ctx     context.Context
	records []*kgo.Record
	errCh   chan error
}

type Producer struct {
	client   *kgo.Client
	requests chan produceRequest
	ctx      context.Context
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

	return &Producer{
		client:   client,
		requests: make(chan produceRequest, bufSize),
	}, nil
}

func (p *Producer) Start(ctx context.Context) {
	p.ctx = ctx
	go p.loop(ctx)
}

func (p *Producer) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-p.requests:
			if req.ctx.Err() != nil {
				req.errCh <- nil
				continue
			}
			err := p.client.ProduceSync(req.ctx, req.records...).FirstErr()
			req.errCh <- err
		}
	}
}

func (p *Producer) Produce(ctx context.Context, messages []noskafka.Producible, opts ...noskafka.ProduceOption) error {
	if p.ctx == nil {
		return xerrors.New("producer is not started")
	}

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

	req := produceRequest{
		ctx:     ctx,
		records: records,
		errCh:   make(chan error, 1),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.requests <- req:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	case err := <-req.errCh:
		return err
	}
}

func (p *Producer) Close() {
	p.client.Close()
}

