package infra

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/ckbkdj/newapi-risk-gateway/internal/config"
	"github.com/xdg-go/scram"
)

type Kafka struct {
	brokers  []string
	config   *sarama.Config
	client   sarama.Client
	producer sarama.SyncProducer
}

func NewKafka(cfg config.Config) (*Kafka, error) {
	if !cfg.KafkaEnabled {
		return &Kafka{}, nil
	}
	saramaCfg, err := newSaramaConfig(cfg)
	if err != nil {
		return nil, err
	}
	client, err := sarama.NewClient(cfg.KafkaBrokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("connect kafka client: %w", err)
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect kafka producer: %w", err)
	}
	return &Kafka{brokers: cfg.KafkaBrokers, config: saramaCfg, client: client, producer: producer}, nil
}

func (k *Kafka) Enabled() bool {
	return k != nil && k.client != nil && k.producer != nil && !k.client.Closed()
}
func (k *Kafka) Close() error {
	if k == nil {
		return nil
	}
	var producerErr, clientErr error
	if k.producer != nil {
		producerErr = k.producer.Close()
	}
	if k.client != nil && !k.client.Closed() {
		clientErr = k.client.Close()
	}
	if producerErr != nil {
		return producerErr
	}
	return clientErr
}

func (k *Kafka) Ping(ctx context.Context) error {
	if !k.Enabled() {
		return fmt.Errorf("kafka disabled")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := k.client.RefreshMetadata(); err != nil {
		return err
	}
	return ctx.Err()
}

func (k *Kafka) Publish(ctx context.Context, topic, key string, payload []byte) error {
	if !k.Enabled() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	message := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.Now().UTC(),
	}
	// Sarama's producer already has bounded network timeouts. Calling it
	// directly avoids leaking one goroutine per event when a caller deadline
	// expires while a broker is unavailable.
	_, _, err := k.producer.SendMessage(message)
	return err
}

func (k *Kafka) EnsureTopics(ctx context.Context, topics []string, partitions int32, replication int16, retentionDays int) error {
	if !k.Enabled() {
		return nil
	}
	admin, err := sarama.NewClusterAdmin(k.brokers, k.config)
	if err != nil {
		return err
	}
	defer admin.Close()
	retention := retentionMillis(retentionDays)
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			continue
		}
		detail := &sarama.TopicDetail{
			NumPartitions:     partitions,
			ReplicationFactor: replication,
			ConfigEntries: map[string]string{
				"cleanup.policy":   "delete",
				"retention.ms":     retention,
				"compression.type": "producer",
			},
		}
		err := admin.CreateTopic(topic, detail, false)
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return fmt.Errorf("create topic %s: %w", topic, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (k *Kafka) ApplyRetention(ctx context.Context, topics []string, days int) error {
	if !k.Enabled() {
		return nil
	}
	admin, err := sarama.NewClusterAdmin(k.brokers, k.config)
	if err != nil {
		return err
	}
	defer admin.Close()
	value := retentionMillis(days)
	for _, topic := range topics {
		configMap := map[string]*string{"retention.ms": &value}
		if err := admin.AlterConfig(sarama.TopicResource, topic, configMap, false); err != nil {
			return fmt.Errorf("alter retention for %s: %w", topic, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func retentionMillis(days int) string {
	if days < 0 {
		return "-1"
	}
	return strconv.FormatInt(int64(days)*24*60*60*1000, 10)
}

func newSaramaConfig(cfg config.Config) (*sarama.Config, error) {
	c := sarama.NewConfig()
	c.ClientID = cfg.KafkaClientID
	c.Version = sarama.V3_6_0_0
	c.Metadata.Full = false
	c.Metadata.Retry.Max = 6
	c.Metadata.Retry.Backoff = 500 * time.Millisecond
	c.Net.DialTimeout = 10 * time.Second
	c.Net.ReadTimeout = 30 * time.Second
	c.Net.WriteTimeout = 30 * time.Second
	c.Net.MaxOpenRequests = 1
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Idempotent = true
	c.Producer.Retry.Max = 8
	c.Producer.Retry.Backoff = 200 * time.Millisecond
	c.Producer.Return.Successes = true
	c.Producer.Compression = sarama.CompressionZSTD
	c.Producer.Flush.Frequency = 20 * time.Millisecond
	c.Producer.Flush.Messages = 100
	c.Producer.Flush.Bytes = 1024 * 1024

	if cfg.KafkaTLSEnabled {
		tlsConfig, err := kafkaTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		c.Net.TLS.Enable = true
		c.Net.TLS.Config = tlsConfig
	}
	if cfg.KafkaSASLMechanism != "" {
		c.Net.SASL.Enable = true
		c.Net.SASL.User = cfg.KafkaUsername
		c.Net.SASL.Password = cfg.KafkaPassword
		switch cfg.KafkaSASLMechanism {
		case "PLAIN":
			c.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			c.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{hashGenerator: scram.SHA256} }
		case "SCRAM-SHA-512":
			c.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			c.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{hashGenerator: scram.SHA512} }
		default:
			return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", cfg.KafkaSASLMechanism)
		}
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validate kafka config: %w", err)
	}
	return c, nil
}

func kafkaTLSConfig(cfg config.Config) (*tls.Config, error) {
	out := &tls.Config{MinVersion: cfg.KafkaTLSMinVersion(), InsecureSkipVerify: cfg.KafkaTLSInsecure} //nolint:gosec -- explicitly configurable for private test clusters
	if cfg.KafkaCACertFile != "" {
		pem, err := os.ReadFile(cfg.KafkaCACertFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid Kafka CA certificate")
		}
		out.RootCAs = pool
	}
	if cfg.KafkaClientCertFile != "" || cfg.KafkaClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.KafkaClientCertFile, cfg.KafkaClientKeyFile)
		if err != nil {
			return nil, err
		}
		out.Certificates = []tls.Certificate{cert}
	}
	return out, nil
}

type xdgSCRAMClient struct {
	hashGenerator scram.HashGeneratorFcn
	client        *scram.Client
	conversation  *scram.ClientConversation
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) error {
	client, err := x.hashGenerator.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.client = client
	x.conversation = client.NewConversation()
	return nil
}
func (x *xdgSCRAMClient) Step(challenge string) (string, error) {
	return x.conversation.Step(challenge)
}
func (x *xdgSCRAMClient) Done() bool { return x.conversation != nil && x.conversation.Done() }

// Keep standard hashes linked for FIPS-oriented static analyzers and make the
// selected algorithms explicit in generated SBOMs.
var _ = sha256.Size
var _ = sha512.Size
