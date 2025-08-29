# Distributed Geo-Sharding Platform (CometStudy Backend)

A real-time geospatial backend that shards data by quadkey (map tiles) to support thousands of concurrent users posting "study pins" and live presence updates across campus.

## 🎯 Features

- **Event-Driven Architecture**: Kafka/Redpanda backbone with idempotency and exactly-once semantics
- **Geo-Sharding**: Quadkey-based spatial partitioning for efficient location queries
- **CRDT Conflict Resolution**: LWW-element sets and PN-counters with Hybrid Logical Clocks
- **Low-Latency Reads**: Redis-backed materialized views with <120ms p95 latency
- **Raft Consensus**: Control plane for shard ownership and strong consistency
- **Auto-Scaling**: Kubernetes HPA with custom metrics for real-time scaling
- **Observability**: Prometheus metrics, Grafana dashboards, and chaos testing

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Gateway       │    │   Streamer      │    │  Control Plane  │
│   (HTTP/WS)     │    │  (Consumers)    │    │    (Raft)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────┬───────────┴───────────┬───────────┘
                     │                       │
         ┌─────────────────┐    ┌─────────────────┐
         │   Kafka/Redpanda│    │      Redis      │
         │   (Events)      │    │ (Materialized   │
         └─────────────────┘    │     Views)      │
                               └─────────────────┘
                                        │
                               ┌─────────────────┐
                               │   PostgreSQL    │
                               │ (Persistence)   │
                               └─────────────────┘
```

### Core Components

- **Gateway**: HTTP/WebSocket API server with real-time subscriptions
- **Streamer**: Event processing services that maintain materialized views
- **Control Plane**: Raft-based consensus for shard assignment and coordination
- **Quadkey Engine**: Geospatial indexing and sharding logic
- **CRDT Layer**: Conflict-free replicated data types for offline-first operations

## 🚀 Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- Kubernetes cluster (for production deployment)

### Local Development

1. **Clone and setup**:
   ```bash
   git clone <repo>
   cd Geo_Sharding
   make setup
   ```

2. **Start infrastructure**:
   ```bash
   docker-compose up -d
   ```

3. **Run migrations**:
   ```bash
   make migrate
   ```

4. **Start services**:
   ```bash
   # Terminal 1: Gateway
   make run-gateway
   
   # Terminal 2: Streamer
   make run-streamer
   
   # Terminal 3: Control Plane
   make run-control-plane
   ```

5. **Test the API**:
   ```bash
   # Create a study pin
   curl -X POST http://localhost:8080/api/v1/pins \
     -H "Content-Type: application/json" \
     -d '{
       "subject": "Machine Learning",
       "description": "Working on neural networks",
       "latitude": 37.7749,
       "longitude": -122.4194
     }'
   
   # Get nearby pins
   curl "http://localhost:8080/api/v1/pins/nearby?lat=37.7749&lon=-122.4194&radius=1000"
   ```

## 🔧 Development

### Project Structure

```
├── cmd/                    # Application entry points
│   ├── gateway/           # HTTP/WebSocket gateway
│   ├── streamer/          # Event stream processors
│   └── control-plane/     # Raft consensus service
├── internal/              # Private application code
│   ├── crdt/             # CRDT implementations
│   ├── gateway/          # Gateway HTTP handlers
│   ├── quadkey/          # Geospatial indexing
│   ├── shard/            # Sharding logic
│   ├── streamer/         # Event handlers
│   └── metrics/          # Prometheus metrics
├── pkg/                   # Reusable packages
│   ├── kafka/            # Kafka client wrapper
│   ├── redis/            # Redis client wrapper
│   ├── postgres/         # PostgreSQL client
│   └── raft/             # Raft consensus
├── k8s/                   # Kubernetes manifests
├── config/               # Configuration files
├── migrations/           # Database migrations
└── test/                 # Integration and chaos tests
```

### Key Concepts

#### Quadkey Geo-Sharding

The system uses quadkeys (hierarchical spatial indexes) to partition data geographically:

```go
// Generate quadkey from coordinates
qkey := quadkey.FromLatLon(37.7749, -122.4194, 18) // Level 18 ≈ 150m precision

// Get nearby cells for range queries
nearby := quadkey.GetNearbyQuadkeys(lat, lon, 1000.0, 18) // 1km radius
```

#### CRDT Conflict Resolution

Last-Write-Wins sets with Hybrid Logical Clocks ensure eventual consistency:

```go
set := crdt.NewLWWSet("node-id")
set.Add("pin-123", pinData)

// Merging handles conflicts automatically
otherSet := getFromOtherNode()
set.Merge(otherSet) // Convergent, commutative, idempotent
```

#### Event Sourcing

All state changes flow through immutable events:

```go
event := kafka.Event{
    Type:      "study_pin.created",
    Quadkey:   qkey.Key,
    Data:      pinData,
    Timestamp: time.Now(),
}
producer.PublishEvent(ctx, event)
```

### Running Tests

```bash
# Unit tests
make test

# Integration tests (requires infrastructure)
make test-integration

# Chaos engineering tests
make test-chaos

# Load testing
make test-load
```

## 📊 Monitoring & Observability

### Metrics

The system exposes comprehensive Prometheus metrics:

- **Business Metrics**: Active pins, user presence, creation rates
- **System Metrics**: HTTP latency, Kafka lag, Redis operations
- **Infrastructure**: CPU, memory, network, storage

### Dashboards

Grafana dashboards provide real-time visibility:

- Main dashboard: `config/grafana/dashboard-geo-sharding.json`
- Access: `http://localhost:3000` (admin/admin)

### Alerting

Key alerts (configure in Prometheus):

```yaml
- alert: HighKafkaLag
  expr: kafka_consumer_lag_milliseconds > 5000
  for: 2m
  
- alert: HTTPLatencyHigh
  expr: histogram_quantile(0.95, http_request_duration_seconds_bucket) > 0.5
  for: 1m
```

## 🚢 Deployment

### Kubernetes

1. **Deploy infrastructure**:
   ```bash
   kubectl apply -f k8s/namespace.yaml
   kubectl apply -f k8s/configmap.yaml
   kubectl apply -f k8s/secrets.yaml
   kubectl apply -f k8s/infrastructure.yaml
   ```

2. **Deploy applications**:
   ```bash
   kubectl apply -f k8s/control-plane-deployment.yaml
   kubectl apply -f k8s/streamer-deployment.yaml
   kubectl apply -f k8s/gateway-deployment.yaml
   ```

3. **Verify deployment**:
   ```bash
   kubectl get pods -n geo-sharding
   kubectl get hpa -n geo-sharding
   ```

### Auto-Scaling

HPA policies scale based on:
- CPU/Memory utilization
- Custom metrics (WebSocket connections, Kafka lag)
- Predictive scaling for hotspot cells

```yaml
metrics:
- type: Pods
  pods:
    metric:
      name: kafka_consumer_lag_per_pod
    target:
      type: AverageValue
      averageValue: "1000"
```

## 🔥 Performance & Scale

### Benchmarks

- **Throughput**: 10k+ events/minute per streamer instance
- **Latency**: <120ms p95 for materialized view reads
- **Freshness**: <1s for view updates under normal load
- **Scaling**: Horizontal scaling up to 50+ streamer instances

### Optimizations

- **Hotspot Handling**: Virtual buckets for popular locations
- **Batch Processing**: Configurable batch sizes for Kafka
- **Connection Pooling**: Optimized Redis/PostgreSQL connections
- **Caching**: Multi-level caching with TTL strategies

## 🧪 Chaos Engineering

The system includes comprehensive chaos tests:

```bash
# Network partition simulation
make test-chaos-network

# Node failure scenarios  
make test-chaos-failure

# Data corruption recovery
make test-chaos-corruption

# Concurrent load testing
make test-chaos-load
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Run tests: `make test`
4. Commit changes: `git commit -m 'Add amazing feature'`
5. Push to branch: `git push origin feature/amazing-feature`
6. Open a Pull Request

### Code Standards

- Follow Go best practices and idioms
- Maintain >80% test coverage
- Add comprehensive logging with structured fields
- Update documentation for API changes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Quadkey Algorithm**: Microsoft Bing Maps team
- **CRDT Research**: Marc Shapiro, Nuno Preguiça et al.
- **Raft Consensus**: Diego Ongaro, John Ousterhout (Stanford)
- **Event Sourcing**: Greg Young, Martin Fowler

---
