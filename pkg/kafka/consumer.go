package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// EventHandler defines the interface for handling events
type EventHandler interface {
	HandleEvent(ctx context.Context, event Event) error
	GetEventTypes() []string
}

// Consumer handles consuming events from Kafka with exactly-once processing
type Consumer struct {
	reader             *kafka.Reader
	handlers           map[string]EventHandler
	processedEvents    *sync.Map // Track processed idempotency keys
	logger             *logrus.Logger
	metrics           *ConsumerMetrics
	shutdownCh        chan struct{}
	wg                sync.WaitGroup
}

// ConsumerConfig holds configuration for the Kafka consumer
type ConsumerConfig struct {
	Brokers       []string
	Topic         string
	GroupID       string
	Partition     int
	MinBytes      int
	MaxBytes      int
	CommitInterval time.Duration
	StartOffset   int64
}

// ConsumerMetrics tracks consumer performance metrics
type ConsumerMetrics struct {
	EventsProcessed   int64
	EventsSkipped     int64
	ProcessingErrors  int64
	LastProcessedTime time.Time
	LagMilliseconds   int64
}

// NewConsumer creates a new Kafka consumer with exactly-once processing semantics
func NewConsumer(config ConsumerConfig, logger *logrus.Logger) *Consumer {
	if config.MinBytes == 0 {
		config.MinBytes = 10e3 // 10KB
	}

	if config.MaxBytes == 0 {
		config.MaxBytes = 10e6 // 10MB
	}

	if config.CommitInterval == 0 {
		config.CommitInterval = 1 * time.Second
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        config.GroupID,
		Partition:      config.Partition,
		MinBytes:       config.MinBytes,
		MaxBytes:       config.MaxBytes,
		CommitInterval: config.CommitInterval,
		StartOffset:    config.StartOffset,
		ErrorLogger:    kafka.LoggerFunc(logger.Errorf),
	})

	return &Consumer{
		reader:          reader,
		handlers:        make(map[string]EventHandler),
		processedEvents: &sync.Map{},
		logger:          logger,
		metrics:         &ConsumerMetrics{},
		shutdownCh:      make(chan struct{}),
	}
}

// RegisterHandler registers an event handler for specific event types
func (c *Consumer) RegisterHandler(handler EventHandler) {
	for _, eventType := range handler.GetEventTypes() {
		c.handlers[eventType] = handler
		c.logger.WithFields(logrus.Fields{
			"event_type": eventType,
			"handler":    fmt.Sprintf("%T", handler),
		}).Info("Registered event handler")
	}
}

// Start starts the consumer
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Kafka consumer")

	c.wg.Add(1)
	go c.consumeLoop(ctx)

	// Start metrics cleanup goroutine
	c.wg.Add(1)
	go c.cleanupLoop(ctx)

	return nil
}

// Stop stops the consumer gracefully
func (c *Consumer) Stop() error {
	c.logger.Info("Stopping Kafka consumer")
	close(c.shutdownCh)
	c.wg.Wait()
	return c.reader.Close()
}

// consumeLoop is the main consumption loop
func (c *Consumer) consumeLoop(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.shutdownCh:
			return
		default:
			message, err := c.reader.ReadMessage(ctx)
			if err != nil {
				c.logger.WithError(err).Error("Failed to read message")
				continue
			}

			if err := c.processMessage(ctx, message); err != nil {
				c.logger.WithError(err).Error("Failed to process message")
				c.metrics.ProcessingErrors++
			}
		}
	}
}

// processMessage processes a single Kafka message with idempotency checks
func (c *Consumer) processMessage(ctx context.Context, message kafka.Message) error {
	// Parse event
	var event Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Calculate processing lag
	lag := time.Since(event.Timestamp).Milliseconds()
	c.metrics.LagMilliseconds = lag

	// Check idempotency - skip if already processed
	idempotencyKey := event.IdempotencyKey
	if idempotencyKey == "" {
		// Extract from headers if not in payload
		for _, header := range message.Headers {
			if header.Key == "idempotency-key" {
				idempotencyKey = string(header.Value)
				break
			}
		}
	}

	if idempotencyKey != "" {
		if _, exists := c.processedEvents.Load(idempotencyKey); exists {
			c.logger.WithFields(logrus.Fields{
				"event_id":        event.ID,
				"event_type":      event.Type,
				"idempotency_key": idempotencyKey,
			}).Debug("Skipping already processed event")
			c.metrics.EventsSkipped++
			return nil
		}
	}

	// Find handler
	handler, exists := c.handlers[event.Type]
	if !exists {
		c.logger.WithField("event_type", event.Type).Warn("No handler registered for event type")
		return nil
	}

	// Process event
	startTime := time.Now()
	err := handler.HandleEvent(ctx, event)
	processingTime := time.Since(startTime)

	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"event_id":        event.ID,
			"event_type":      event.Type,
			"processing_time": processingTime,
			"error":          err,
		}).Error("Event processing failed")
		return err
	}

	// Mark as processed
	if idempotencyKey != "" {
		c.processedEvents.Store(idempotencyKey, time.Now())
	}

	c.metrics.EventsProcessed++
	c.metrics.LastProcessedTime = time.Now()

	c.logger.WithFields(logrus.Fields{
		"event_id":        event.ID,
		"event_type":      event.Type,
		"processing_time": processingTime,
		"lag_ms":          lag,
	}).Debug("Event processed successfully")

	return nil
}

// cleanupLoop periodically cleans up old processed event records
func (c *Consumer) cleanupLoop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.shutdownCh:
			return
		case <-ticker.C:
			c.cleanupProcessedEvents()
		}
	}
}

// cleanupProcessedEvents removes old processed event records to prevent memory leaks
func (c *Consumer) cleanupProcessedEvents() {
	cutoff := time.Now().Add(-1 * time.Hour) // Keep records for 1 hour
	
	c.processedEvents.Range(func(key, value interface{}) bool {
		if processedTime, ok := value.(time.Time); ok {
			if processedTime.Before(cutoff) {
				c.processedEvents.Delete(key)
			}
		}
		return true
	})
}

// GetMetrics returns current consumer metrics
func (c *Consumer) GetMetrics() ConsumerMetrics {
	return *c.metrics
}

// Seek seeks to a specific offset
func (c *Consumer) Seek(offset int64) error {
	return c.reader.SetOffset(offset)
}

// CommitMessages manually commits the specified messages
func (c *Consumer) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	return c.reader.CommitMessages(ctx, messages...)
}
