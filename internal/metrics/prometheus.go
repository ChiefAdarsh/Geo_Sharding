package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

// Metrics holds all Prometheus metrics for the geo-sharding platform
type Metrics struct {
	// HTTP Gateway metrics
	HTTPRequestsTotal     *prometheus.CounterVec
	HTTPRequestDuration   *prometheus.HistogramVec
	WebSocketConnections  prometheus.Gauge
	
	// Kafka metrics
	KafkaEventsProduced   *prometheus.CounterVec
	KafkaEventsConsumed   *prometheus.CounterVec
	KafkaConsumerLag      *prometheus.GaugeVec
	KafkaProducerErrors   *prometheus.CounterVec
	
	// Redis metrics
	RedisOperations       *prometheus.CounterVec
	RedisOperationDuration *prometheus.HistogramVec
	RedisConnections      prometheus.Gauge
	
	// Shard metrics
	ShardAssignments      *prometheus.GaugeVec
	ShardLoadScore        *prometheus.GaugeVec
	ShardRebalances       prometheus.Counter
	
	// CRDT metrics
	CRDTOperations        *prometheus.CounterVec
	CRDTConflicts         *prometheus.CounterVec
	CRDTMerges            *prometheus.CounterVec
	
	// Business metrics
	ActiveStudyPins       *prometheus.GaugeVec
	ActiveUsers           *prometheus.GaugeVec
	StudyPinCreationRate  *prometheus.CounterVec
	PresenceUpdates       *prometheus.CounterVec
	
	// System metrics
	ProcessingLatency     *prometheus.HistogramVec
	ErrorRate             *prometheus.CounterVec
	NodeHealth            *prometheus.GaugeVec
	
	// Custom registry
	registry *prometheus.Registry
	logger   *logrus.Logger
}

// NewMetrics creates a new Prometheus metrics collection
func NewMetrics(logger *logrus.Logger) *Metrics {
	registry := prometheus.NewRegistry()
	
	m := &Metrics{
		registry: registry,
		logger:   logger,
		
		// HTTP Gateway metrics
		HTTPRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "endpoint", "status"},
		),
		
		HTTPRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "HTTP request duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "endpoint"},
		),
		
		WebSocketConnections: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "websocket_connections_active",
				Help: "Number of active WebSocket connections",
			},
		),
		
		// Kafka metrics
		KafkaEventsProduced: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_events_produced_total",
				Help: "Total number of events produced to Kafka",
			},
			[]string{"topic", "event_type", "quadkey_prefix"},
		),
		
		KafkaEventsConsumed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_events_consumed_total",
				Help: "Total number of events consumed from Kafka",
			},
			[]string{"topic", "event_type", "consumer_group"},
		),
		
		KafkaConsumerLag: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_lag_milliseconds",
				Help: "Kafka consumer lag in milliseconds",
			},
			[]string{"topic", "partition", "consumer_group"},
		),
		
		KafkaProducerErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_producer_errors_total",
				Help: "Total number of Kafka producer errors",
			},
			[]string{"topic", "error_type"},
		),
		
		// Redis metrics
		RedisOperations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "redis_operations_total",
				Help: "Total number of Redis operations",
			},
			[]string{"operation", "status"},
		),
		
		RedisOperationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "redis_operation_duration_seconds",
				Help:    "Redis operation duration in seconds",
				Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
			},
			[]string{"operation"},
		),
		
		RedisConnections: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "redis_connections_active",
				Help: "Number of active Redis connections",
			},
		),
		
		// Shard metrics
		ShardAssignments: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "shard_assignments_total",
				Help: "Total number of shard assignments",
			},
			[]string{"node_id", "shard_status"},
		),
		
		ShardLoadScore: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "shard_load_score",
				Help: "Load score for each shard",
			},
			[]string{"node_id", "shard_id"},
		),
		
		ShardRebalances: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "shard_rebalances_total",
				Help: "Total number of shard rebalances",
			},
		),
		
		// CRDT metrics
		CRDTOperations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "crdt_operations_total",
				Help: "Total number of CRDT operations",
			},
			[]string{"operation", "crdt_type"},
		),
		
		CRDTConflicts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "crdt_conflicts_total",
				Help: "Total number of CRDT conflicts resolved",
			},
			[]string{"crdt_type", "conflict_type"},
		),
		
		CRDTMerges: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "crdt_merges_total",
				Help: "Total number of CRDT merges",
			},
			[]string{"crdt_type"},
		),
		
		// Business metrics
		ActiveStudyPins: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "study_pins_active",
				Help: "Number of active study pins",
			},
			[]string{"quadkey_prefix", "subject"},
		),
		
		ActiveUsers: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "users_active",
				Help: "Number of active users",
			},
			[]string{"quadkey_prefix", "status"},
		),
		
		StudyPinCreationRate: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "study_pins_created_total",
				Help: "Total number of study pins created",
			},
			[]string{"quadkey_prefix", "subject"},
		),
		
		PresenceUpdates: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "presence_updates_total",
				Help: "Total number of presence updates",
			},
			[]string{"quadkey_prefix", "status"},
		),
		
		// System metrics
		ProcessingLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "processing_latency_seconds",
				Help:    "Processing latency in seconds",
				Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
			},
			[]string{"component", "operation"},
		),
		
		ErrorRate: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "errors_total",
				Help: "Total number of errors",
			},
			[]string{"component", "error_type"},
		),
		
		NodeHealth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "node_health",
				Help: "Health status of nodes (1 = healthy, 0 = unhealthy)",
			},
			[]string{"node_id", "component"},
		),
	}
	
	// Register all metrics
	m.registerMetrics()
	
	return m
}

// registerMetrics registers all metrics with the Prometheus registry
func (m *Metrics) registerMetrics() {
	metrics := []prometheus.Collector{
		m.HTTPRequestsTotal,
		m.HTTPRequestDuration,
		m.WebSocketConnections,
		m.KafkaEventsProduced,
		m.KafkaEventsConsumed,
		m.KafkaConsumerLag,
		m.KafkaProducerErrors,
		m.RedisOperations,
		m.RedisOperationDuration,
		m.RedisConnections,
		m.ShardAssignments,
		m.ShardLoadScore,
		m.ShardRebalances,
		m.CRDTOperations,
		m.CRDTConflicts,
		m.CRDTMerges,
		m.ActiveStudyPins,
		m.ActiveUsers,
		m.StudyPinCreationRate,
		m.PresenceUpdates,
		m.ProcessingLatency,
		m.ErrorRate,
		m.NodeHealth,
	}
	
	for _, metric := range metrics {
		m.registry.MustRegister(metric)
	}
	
	m.logger.Info("Prometheus metrics registered")
}

// GetRegistry returns the Prometheus registry
func (m *Metrics) GetRegistry() *prometheus.Registry {
	return m.registry
}

// GetHandler returns the HTTP handler for Prometheus metrics
func (m *Metrics) GetHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Instrumentation helper methods

// RecordHTTPRequest records an HTTP request metric
func (m *Metrics) RecordHTTPRequest(method, endpoint string, status int, duration time.Duration) {
	m.HTTPRequestsTotal.WithLabelValues(method, endpoint, strconv.Itoa(status)).Inc()
	m.HTTPRequestDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
}

// RecordKafkaEventProduced records a Kafka event production
func (m *Metrics) RecordKafkaEventProduced(topic, eventType, quadkeyPrefix string) {
	m.KafkaEventsProduced.WithLabelValues(topic, eventType, quadkeyPrefix).Inc()
}

// RecordKafkaEventConsumed records a Kafka event consumption
func (m *Metrics) RecordKafkaEventConsumed(topic, eventType, consumerGroup string) {
	m.KafkaEventsConsumed.WithLabelValues(topic, eventType, consumerGroup).Inc()
}

// SetKafkaConsumerLag sets the Kafka consumer lag
func (m *Metrics) SetKafkaConsumerLag(topic, partition, consumerGroup string, lag float64) {
	m.KafkaConsumerLag.WithLabelValues(topic, partition, consumerGroup).Set(lag)
}

// RecordKafkaProducerError records a Kafka producer error
func (m *Metrics) RecordKafkaProducerError(topic, errorType string) {
	m.KafkaProducerErrors.WithLabelValues(topic, errorType).Inc()
}

// RecordRedisOperation records a Redis operation
func (m *Metrics) RecordRedisOperation(operation, status string, duration time.Duration) {
	m.RedisOperations.WithLabelValues(operation, status).Inc()
	m.RedisOperationDuration.WithLabelValues(operation).Observe(duration.Seconds())
}

// SetShardAssignments sets the number of shard assignments
func (m *Metrics) SetShardAssignments(nodeID, status string, count float64) {
	m.ShardAssignments.WithLabelValues(nodeID, status).Set(count)
}

// SetShardLoadScore sets the load score for a shard
func (m *Metrics) SetShardLoadScore(nodeID, shardID string, score float64) {
	m.ShardLoadScore.WithLabelValues(nodeID, shardID).Set(score)
}

// RecordShardRebalance records a shard rebalance
func (m *Metrics) RecordShardRebalance() {
	m.ShardRebalances.Inc()
}

// RecordCRDTOperation records a CRDT operation
func (m *Metrics) RecordCRDTOperation(operation, crdtType string) {
	m.CRDTOperations.WithLabelValues(operation, crdtType).Inc()
}

// RecordCRDTConflict records a CRDT conflict resolution
func (m *Metrics) RecordCRDTConflict(crdtType, conflictType string) {
	m.CRDTConflicts.WithLabelValues(crdtType, conflictType).Inc()
}

// RecordCRDTMerge records a CRDT merge
func (m *Metrics) RecordCRDTMerge(crdtType string) {
	m.CRDTMerges.WithLabelValues(crdtType).Inc()
}

// SetActiveStudyPins sets the number of active study pins
func (m *Metrics) SetActiveStudyPins(quadkeyPrefix, subject string, count float64) {
	m.ActiveStudyPins.WithLabelValues(quadkeyPrefix, subject).Set(count)
}

// SetActiveUsers sets the number of active users
func (m *Metrics) SetActiveUsers(quadkeyPrefix, status string, count float64) {
	m.ActiveUsers.WithLabelValues(quadkeyPrefix, status).Set(count)
}

// RecordStudyPinCreation records a study pin creation
func (m *Metrics) RecordStudyPinCreation(quadkeyPrefix, subject string) {
	m.StudyPinCreationRate.WithLabelValues(quadkeyPrefix, subject).Inc()
}

// RecordPresenceUpdate records a presence update
func (m *Metrics) RecordPresenceUpdate(quadkeyPrefix, status string) {
	m.PresenceUpdates.WithLabelValues(quadkeyPrefix, status).Inc()
}

// RecordProcessingLatency records processing latency
func (m *Metrics) RecordProcessingLatency(component, operation string, duration time.Duration) {
	m.ProcessingLatency.WithLabelValues(component, operation).Observe(duration.Seconds())
}

// RecordError records an error
func (m *Metrics) RecordError(component, errorType string) {
	m.ErrorRate.WithLabelValues(component, errorType).Inc()
}

// SetNodeHealth sets the health status of a node
func (m *Metrics) SetNodeHealth(nodeID, component string, healthy bool) {
	value := 0.0
	if healthy {
		value = 1.0
	}
	m.NodeHealth.WithLabelValues(nodeID, component).Set(value)
}

// SetWebSocketConnections sets the number of active WebSocket connections
func (m *Metrics) SetWebSocketConnections(count float64) {
	m.WebSocketConnections.Set(count)
}

// SetRedisConnections sets the number of active Redis connections
func (m *Metrics) SetRedisConnections(count float64) {
	m.RedisConnections.Set(count)
}

// MetricsMiddleware creates an HTTP middleware for collecting metrics
func (m *Metrics) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap the ResponseWriter to capture the status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		
		next.ServeHTTP(wrapped, r)
		
		duration := time.Since(start)
		m.RecordHTTPRequest(r.Method, r.URL.Path, wrapped.statusCode, duration)
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
