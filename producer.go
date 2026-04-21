package nosfranz

import (
	"context"
	"errors"

	"github.com/raaaaaaaay86/noskafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"
)

var _ noskafka.Producer = (*Producer)(nil)

type TopicConfig struct {
	NumPartitions     int32
	ReplicationFactor int16
}

type ProducerConfig struct {
	// Brokers represents the endpoints of Kafka nodes.
	Brokers []string
	// BufferSize is the size of buffered channel to perform back-pressure pattern.
	BufferSize int
	// AutoCreateTopic creates for topics not exists in the Kafka Cluster.
	AutoCreateTopic *TopicConfig
	// TracerProvider is for turning telemetry feature on. Producer will generate traces during runtime.
	TracerProvider trace.TracerProvider
}

type produceRequest struct {
	ctx     context.Context
	records []*kgo.Record
	errCh   chan error
}

type Producer struct {
	client        *kgo.Client
	admin         *kadm.Client
	config        ProducerConfig
	requests      chan produceRequest
	ctx           context.Context
	createdTopics map[string]struct{}
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
		client:        client,
		admin:         kadm.NewClient(client),
		config:        config,
		requests:      make(chan produceRequest, bufSize),
		createdTopics: make(map[string]struct{}),
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
			if p.config.AutoCreateTopic != nil {
				if err := p.ensureTopics(req.ctx, req.records); err != nil {
					req.errCh <- err
					continue
				}
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

func (p *Producer) withTracedContext(ctx context.Context) (context.Context, trace.Span) {
	return nil, nil
}

func (p *Producer) ensureTopics(ctx context.Context, records []*kgo.Record) error {
	topics := make([]string, 0, len(records))
	for _, r := range records {
		if _, ok := p.createdTopics[r.Topic]; !ok {
			topics = append(topics, r.Topic)
		}
	}
	if len(topics) == 0 {
		return nil
	}

	cfg := p.config.AutoCreateTopic
	responses, err := p.admin.CreateTopics(ctx, cfg.NumPartitions, cfg.ReplicationFactor, nil, topics...)
	if err != nil {
		return xerrors.Errorf("failed to create topics: %w", err)
	}
	if err := responses.Error(); err != nil {
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			return err
		}
	}

	// record checked topic
	for _, topic := range topics {
		p.createdTopics[topic] = struct{}{}
	}

	return nil
}

func (p *Producer) Close() {
	p.client.Close()
}
