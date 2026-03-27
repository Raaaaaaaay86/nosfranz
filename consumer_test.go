package nosfranz

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raaaaaaaay86/noskafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
)

type FranzConsumerTestSuite struct {
	suite.Suite

	defaultIdentifier noskafka.Identifier
	defaultBrokers    []string
	defaultTopics     []noskafka.Topic
	defaultGroupId    noskafka.GroupId
}

func TestFranzConsumerTestSuite(t *testing.T) {
	suite.Run(t, new(FranzConsumerTestSuite))
}

func (s *FranzConsumerTestSuite) SetupSuite() {
	s.defaultIdentifier = noskafka.Identifier(1)
	s.defaultBrokers = []string{"localhost:7072", "localhost:7073", "localhost:7074"}
	s.defaultTopics = []noskafka.Topic{noskafka.Topic("test_topic")}
	s.defaultGroupId = noskafka.GroupId(fmt.Sprintf("test_group_%d", time.Now().UnixMilli()))
}

func (s *FranzConsumerTestSuite) newConsumer(handlers ...noskafka.HandlerFunc) *FranzConsumer {
	cfg := Config{
		ConnectionInfo: noskafka.ConnectionInfo{
			Brokers: s.defaultBrokers,
			Topics:  s.defaultTopics,
			GroupId: s.defaultGroupId,
		},
		Handlers:   handlers,
		AutoCommit: false,
	}
	consumer := NewFranzConsumer(cfg)
	consumer.SetIdentifier(s.defaultIdentifier)
	return consumer
}

func (s *FranzConsumerTestSuite) TestWhenCreated() {
	t := s.T()
	consumer := s.newConsumer()

	assert.Equal(t, s.defaultBrokers, consumer.GetConnectionInfo().Brokers)
	assert.Equal(t, s.defaultGroupId, consumer.GetConnectionInfo().GroupId)
	assert.Equal(t, s.defaultIdentifier, consumer.GetIdentifier())
}

func (s *FranzConsumerTestSuite) TestWhenSignalChanSet() {
	t := s.T()
	consumer := s.newConsumer()

	ch := make(chan noskafka.Signal)
	consumer.SetSignalChan(ch)

	assert.Equal(t, fmt.Sprintf("%p", ch), fmt.Sprintf("%p", consumer.signalChan), "memory address must equal to the `ch` which previous set")
}

func (s *FranzConsumerTestSuite) TestStopWhenNotStarted() {
	t := s.T()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumer := s.newConsumer()
	done := consumer.Stop(ctx)

	select {
	case <-done:
		// success
	case <-ctx.Done():
		t.Fatal("Stop timed out when not started")
	}
}

func (s *FranzConsumerTestSuite) TestStopAfterStarted() {
	t := s.T()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	consumer := s.newConsumer()
	consumer.Start(ctx)

	done := consumer.Stop(ctx)
	select {
	case <-done:
		// success
		assert.False(t, consumer.started.Load(), "`started` property should be set to false after called Stop()")
	case <-ctx.Done():
		t.Fatal("Stop timed out after started")
	}
}

func (s *FranzConsumerTestSuite) TestClose() {
	t := s.T()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumer := s.newConsumer()
	consumer.Start(ctx)

	// 等待 client 初始化完成 (因為是背景執行的)
	s.waitForClientInitialized(ctx, consumer)

	assert.NotNil(t, consumer.client, "client should not be nil")
	assert.True(t, consumer.started.Load(), "started should be true")

	err := consumer.Close()
	assert.NoError(t, err)
	assert.False(t, consumer.started.Load(), "`started` should be false after called Close()")
	assert.True(t, consumer.closed.Load(), "`closed` should be true after called Close()")
	assert.Nil(t, consumer.client, "`client` should be nil after called Close()")
}

func (s *FranzConsumerTestSuite) TestCheckFranzInternalConfig() {
	t := s.T()

	type testCase struct {
		Message      string
		MutateConfig func(cfg *Config)
		Assert       func(consumer *FranzConsumer, kgoCfg reflect.Value)
	}

	testCases := []testCase{
		{
			Message: "Validate Topic Config",
			MutateConfig: func(cfg *Config) {
				cfg.ConnectionInfo.Topics = []noskafka.Topic{noskafka.Topic("topic1"), noskafka.Topic("topic2")}
			},
			Assert: func(consumer *FranzConsumer, kgoCfg reflect.Value) {
				topicsField := kgoCfg.FieldByName("topics")
				for _, topic := range topicsField.MapKeys() {
					found := false
					for _, t := range consumer.config.ConnectionInfo.Topics {
						if string(t) == topic.String() {
							found = true
							break
						}
					}
					assert.True(t, found, "every provided topics should be set in the kgo.Client")
				}
			},
		},
		{
			Message: "Validate GroupId Config",
			MutateConfig: func(cfg *Config) {
				cfg.ConnectionInfo.GroupId = "internal_test_group"
			},
			Assert: func(consumer *FranzConsumer, kgoCfg reflect.Value) {
				assert.Equal(t, "internal_test_group", kgoCfg.FieldByName("group").String())
			},
		},
		{
			Message: "Validate Brokers Config",
			MutateConfig: func(cfg *Config) {
				cfg.ConnectionInfo.Brokers = []string{"127.0.0.1:9092"}
			},
			Assert: func(consumer *FranzConsumer, kgoCfg reflect.Value) {
				brokers := kgoCfg.FieldByName("seedBrokers")
				assert.Equal(t, 1, brokers.Len())
				assert.Equal(t, "127.0.0.1:9092", brokers.Index(0).String())
			},
		},
		{
			Message: "Validate AutoCommit Config (value: false)",
			MutateConfig: func(cfg *Config) {
				cfg.AutoCommit = false
			},
			Assert: func(consumer *FranzConsumer, kgoCfg reflect.Value) {
				assert.True(t, kgoCfg.FieldByName("autocommitDisable").Bool(), "When AutoCommit is false, autocommitDisable should be true")
			},
		},
		{
			Message: "Validate AutoCommit Config (valie: true)",
			MutateConfig: func(cfg *Config) {
				cfg.AutoCommit = true
			},
			Assert: func(consumer *FranzConsumer, kgoCfg reflect.Value) {
				assert.False(t, kgoCfg.FieldByName("autocommitDisable").Bool(), "When AutoCommit is true, autocommitDisable should be false")
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Message, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			cfg := Config{
				ConnectionInfo: noskafka.ConnectionInfo{
					Brokers: s.defaultBrokers,
					Topics:  s.defaultTopics,
					GroupId: s.defaultGroupId,
				},
				AutoCommit: false,
			}
			tc.MutateConfig(&cfg)
			consumer := NewFranzConsumer(cfg)

			go consumer.Start(ctx)
			s.waitForClientInitialized(ctx, consumer)

			// use reflection to extract value of `cfg`
			clientVal := reflect.ValueOf(consumer.getClient())
			if clientVal.Kind() == reflect.Ptr {
				clientVal = clientVal.Elem()
			}

			found := false
			for i := 0; i < clientVal.NumField(); i++ {
				field := clientVal.Field(i)
				if field.Kind() == reflect.Ptr {
					field = field.Elem()
				}
				if field.Type().Name() == "cfg" {
					tc.Assert(consumer, field)
					found = true
					break
				}
			}
			assert.True(s.T(), found, "`cfg` cannot be found int kgo.Client")
			
			consumer.Close()
		})
	}
}

func (s *FranzConsumerTestSuite) TestIntegration() {
	if testing.Short() {
		s.T().Skip("skipping integration test in short mode")
	}

	now := time.Now().UnixNano()
	topic := fmt.Sprintf("test-topic-%d", now)
	group := fmt.Sprintf("test-group-%d", now)

	s.produceTestData(topic, 10)

	var count int32
	processed := make(chan struct{}, 10)

	cfg := Config{
		ConnectionInfo: noskafka.ConnectionInfo{
			Brokers: s.defaultBrokers,
			GroupId: noskafka.GroupId(group),
			Topics:  []noskafka.Topic{noskafka.Topic(topic)},
		},
		Handlers: []noskafka.HandlerFunc{
			func(c *noskafka.Context) {
				atomic.AddInt32(&count, 1)
				processed <- struct{}{}
			},
		},
		AutoCommit: true,
	}

	consumer := NewFranzConsumer(cfg)
	consumer.SetIdentifier(noskafka.Identifier(1))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	consumer.Start(ctx)

	for i := 0; i < 10; i++ {
		select {
		case <-processed:
			s.T().Logf("receive %d message", i)
		case <-time.After(10 * time.Second):
			s.T().Fatalf("timed out waiting for messages, got %d/10", atomic.LoadInt32(&count))
		}
	}

	<-consumer.Stop(ctx)
	consumer.Close()
}

func (s *FranzConsumerTestSuite) waitForClientInitialized(ctx context.Context, consumer *FranzConsumer) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if consumer.getClient() != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *FranzConsumerTestSuite) produceTestData(topic string, n int) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(s.defaultBrokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		s.T().Fatalf("failed to create producer: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()
	var lastErr error
	for i := 0; i < 5; i++ {
		record := &kgo.Record{Topic: topic, Value: []byte("test")}
		if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		s.T().Fatalf("failed to produce first message: %v", lastErr)
	}

	for i := 1; i < n; i++ {
		cl.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte("test")}, nil)
	}
	cl.Flush(ctx)
}
