package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Event represents a domain event in the system
type Event struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	Source       string                 `json:"source"`
	Timestamp    time.Time              `json:"timestamp"`
	Data         map[string]interface{} `json:"data"`
	Quadkey      string                 `json:"quadkey"`
	IdempotencyKey string               `json:"idempotency_key"`
}

// Producer handles publishing events to Kafka
type Producer struct {
	writer *kafka.Writer
	logger *logrus.Logger
}

// ProducerConfig holds configuration for the Kafka producer
type ProducerConfig struct {
	Brokers     []string
	Topic       string
	Balancer    kafka.Balancer
	RequiredAcks int
	BatchSize   int
	BatchTimeout time.Duration
}

// NewProducer creates a new Kafka producer with exactly-once semantics
func NewProducer(config ProducerConfig, logger *logrus.Logger) *Producer {
	if config.Balancer == nil {
		config.Balancer = &kafka.Hash{} // Use hash partitioning for quadkey-based sharding
	}

	if config.RequiredAcks == 0 {
		config.RequiredAcks = kafka.RequireAll // Ensure durability
	}

	if config.BatchSize == 0 {
		config.BatchSize = 100
	}

	if config.BatchTimeout == 0 {
		config.BatchTimeout = 10 * time.Millisecond
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     config.Balancer,
		RequiredAcks: kafka.RequiredAcks(config.RequiredAcks),
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
		Compression:  kafka.Snappy,
		ErrorLogger:  kafka.LoggerFunc(logger.Errorf),
	}

	return &Producer{
		writer: writer,
		logger: logger,
	}
}

// PublishEvent publishes an event with idempotency guarantees
func (p *Producer) PublishEvent(ctx context.Context, event Event) error {
	// Generate idempotency key if not provided
	if event.IdempotencyKey == "" {
		event.IdempotencyKey = p.generateIdempotencyKey(event)
	}

	// Serialize event
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	// Create Kafka message
	message := kafka.Message{
		Key:   []byte(event.Quadkey), // Partition by quadkey for geo-sharding
		Value: data,
		Headers: []kafka.Header{
			{Key: "event-type", Value: []byte(event.Type)},
			{Key: "idempotency-key", Value: []byte(event.IdempotencyKey)},
			{Key: "source", Value: []byte(event.Source)},
			{Key: "quadkey", Value: []byte(event.Quadkey)},
		},
		Time: event.Timestamp,
	}

	// Publish with retries
	err = p.writer.WriteMessages(ctx, message)
	if err != nil {
		p.logger.WithFields(logrus.Fields{
			"event_id":         event.ID,
			"event_type":       event.Type,
			"quadkey":          event.Quadkey,
			"idempotency_key":  event.IdempotencyKey,
			"error":           err,
		}).Error("Failed to publish event")
		return fmt.Errorf("failed to publish event: %w", err)
	}

	p.logger.WithFields(logrus.Fields{
		"event_id":        event.ID,
		"event_type":      event.Type,
		"quadkey":         event.Quadkey,
		"idempotency_key": event.IdempotencyKey,
	}).Info("Event published successfully")

	return nil
}

// PublishBatch publishes multiple events in a single batch
func (p *Producer) PublishBatch(ctx context.Context, events []Event) error {
	messages := make([]kafka.Message, len(events))

	for i, event := range events {
		// Generate idempotency key if not provided
		if event.IdempotencyKey == "" {
			event.IdempotencyKey = p.generateIdempotencyKey(event)
		}

		// Serialize event
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to serialize event %s: %w", event.ID, err)
		}

		// Create Kafka message
		messages[i] = kafka.Message{
			Key:   []byte(event.Quadkey),
			Value: data,
			Headers: []kafka.Header{
				{Key: "event-type", Value: []byte(event.Type)},
				{Key: "idempotency-key", Value: []byte(event.IdempotencyKey)},
				{Key: "source", Value: []byte(event.Source)},
				{Key: "quadkey", Value: []byte(event.Quadkey)},
			},
			Time: event.Timestamp,
		}
	}

	// Publish batch
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		p.logger.WithField("error", err).Error("Failed to publish event batch")
		return fmt.Errorf("failed to publish event batch: %w", err)
	}

	p.logger.WithField("batch_size", len(events)).Info("Event batch published successfully")
	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	return p.writer.Close()
}

// generateIdempotencyKey generates a deterministic idempotency key based on event content
func (p *Producer) generateIdempotencyKey(event Event) string {
	// Create a deterministic hash from event content
	hasher := sha256.New()
	
	// Include key fields that should make the event unique
	hasher.Write([]byte(event.Type))
	hasher.Write([]byte(event.Source))
	hasher.Write([]byte(event.Quadkey))
	hasher.Write([]byte(event.Timestamp.Format(time.RFC3339Nano)))
	
	// Include data content
	data, _ := json.Marshal(event.Data)
	hasher.Write(data)
	
	return hex.EncodeToString(hasher.Sum(nil))
}

// EventTypes defines the supported event types
var EventTypes = struct {
	StudyPinCreated  string
	StudyPinUpdated  string
	StudyPinDeleted  string
	PresenceUpdate   string
	PresenceJoin     string
	PresenceLeave    string
	ShardRebalance   string
	CounterIncrement string
	CounterDecrement string
}{
	StudyPinCreated:  "study_pin.created",
	StudyPinUpdated:  "study_pin.updated",
	StudyPinDeleted:  "study_pin.deleted",
	PresenceUpdate:   "presence.update",
	PresenceJoin:     "presence.join",
	PresenceLeave:    "presence.leave",
	ShardRebalance:   "shard.rebalance",
	CounterIncrement: "counter.increment",
	CounterDecrement: "counter.decrement",
}
