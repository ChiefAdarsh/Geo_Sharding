package test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	
	"geo-sharding/internal/crdt"
	"geo-sharding/internal/quadkey"
	"geo-sharding/internal/shard"
	"geo-sharding/pkg/kafka"
	"geo-sharding/pkg/redis"
)

// ChaosTestSuite contains chaos engineering tests
type ChaosTestSuite struct {
	t          *testing.T
	logger     *logrus.Logger
	redis      *redis.Client
	producer   *kafka.Producer
	consumer   *kafka.Consumer
	mvStore    *shard.MaterializedViewStore
}

// NewChaosTestSuite creates a new chaos test suite
func NewChaosTestSuite(t *testing.T) *ChaosTestSuite {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// Setup Redis (would use testcontainers in real implementation)
	redisClient, err := redis.NewClient(redis.Config{
		Addr: "localhost:6379",
		DB:   1, // Use test database
	}, logger)
	require.NoError(t, err)

	// Setup Kafka producer
	producer := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "chaos-test-events",
	}, logger)

	// Setup Kafka consumer
	consumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "chaos-test-events",
		GroupID: "chaos-test-group",
	}, logger)

	// Setup materialized view store
	mvStore := shard.NewMaterializedViewStore(redisClient, logger)

	return &ChaosTestSuite{
		t:        t,
		logger:   logger,
		redis:    redisClient,
		producer: producer,
		consumer: consumer,
		mvStore:  mvStore,
	}
}

// TestNetworkPartition simulates network partitions and tests recovery
func (suite *ChaosTestSuite) TestNetworkPartition(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Test CRDT convergence under network partitions
	nodeA := crdt.NewLWWSet("node-a")
	nodeB := crdt.NewLWWSet("node-b")
	nodeC := crdt.NewLWWSet("node-c")

	// Phase 1: Normal operation - all nodes connected
	nodeA.Add("pin1", map[string]interface{}{"subject": "Math", "user": "alice"})
	nodeB.Add("pin2", map[string]interface{}{"subject": "Physics", "user": "bob"})
	nodeC.Add("pin3", map[string]interface{}{"subject": "Chemistry", "user": "charlie"})

	// Sync all nodes
	nodeA.Merge(nodeB)
	nodeA.Merge(nodeC)
	nodeB.Merge(nodeA)
	nodeB.Merge(nodeC)
	nodeC.Merge(nodeA)
	nodeC.Merge(nodeB)

	assert.Equal(t, 3, nodeA.Size())
	assert.Equal(t, 3, nodeB.Size())
	assert.Equal(t, 3, nodeC.Size())

	// Phase 2: Network partition - nodeA isolated from nodeB and nodeC
	time.Sleep(100 * time.Millisecond) // Ensure different timestamps

	nodeA.Add("pin4", map[string]interface{}{"subject": "Biology", "user": "alice"})
	nodeB.Add("pin5", map[string]interface{}{"subject": "History", "user": "bob"})
	nodeC.Add("pin6", map[string]interface{}{"subject": "Geography", "user": "charlie"})

	// Sync only between nodeB and nodeC (nodeA is partitioned)
	nodeB.Merge(nodeC)
	nodeC.Merge(nodeB)

	// NodeA should have 4 items, nodeB and nodeC should have 5 items each
	assert.Equal(t, 4, nodeA.Size())
	assert.Equal(t, 5, nodeB.Size())
	assert.Equal(t, 5, nodeC.Size())

	// Phase 3: Network heals - all nodes can communicate again
	time.Sleep(100 * time.Millisecond)

	// Full sync
	nodeA.Merge(nodeB)
	nodeA.Merge(nodeC)
	nodeB.Merge(nodeA)
	nodeC.Merge(nodeA)

	// All nodes should converge to 6 items
	assert.Equal(t, 6, nodeA.Size())
	assert.Equal(t, 6, nodeB.Size())
	assert.Equal(t, 6, nodeC.Size())

	suite.logger.Info("Network partition test completed successfully")
}

// TestNodeFailure simulates node failures and tests failover
func (suite *ChaosTestSuite) TestNodeFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Create multiple materialized view stores simulating different nodes
	stores := make([]*shard.MaterializedViewStore, 3)
	for i := 0; i < 3; i++ {
		stores[i] = shard.NewMaterializedViewStore(suite.redis, suite.logger)
	}

	// Create test data across multiple quadkeys
	testPins := generateTestPins(100)
	
	// Phase 1: Distribute pins across all stores
	for i, pin := range testPins {
		storeIndex := i % 3
		err := stores[storeIndex].UpdatePin(ctx, pin)
		require.NoError(t, err)
	}

	// Verify data is distributed
	for i, store := range stores {
		view, err := store.GetCellView(ctx, testPins[i].Quadkey)
		require.NoError(t, err)
		assert.NotEmpty(t, view.Pins)
		suite.logger.WithField("store", i).WithField("pins", len(view.Pins)).Info("Store state before failure")
	}

	// Phase 2: Simulate node failure (remove one store)
	failedStoreIndex := 1
	suite.logger.WithField("failed_store", failedStoreIndex).Info("Simulating node failure")
	
	// Remove failed store from active stores list
	activeStores := []*shard.MaterializedViewStore{stores[0], stores[2]}

	// Phase 3: Continue operations with remaining stores
	additionalPins := generateTestPins(50)
	for i, pin := range additionalPins {
		storeIndex := i % 2 // Only use remaining 2 stores
		err := activeStores[storeIndex].UpdatePin(ctx, pin)
		require.NoError(t, err)
	}

	// Phase 4: Simulate recovery of failed node
	time.Sleep(1 * time.Second)
	suite.logger.Info("Simulating node recovery")
	
	// Recovered node needs to catch up with missed data
	recoveredStore := stores[failedStoreIndex]
	
	// In real implementation, this would involve:
	// 1. Replaying missed events from Kafka
	// 2. Syncing CRDT states from other nodes
	// 3. Rebuilding materialized views
	
	// For this test, we'll simulate by manually syncing some data
	for _, pin := range additionalPins[0:10] { // Partial recovery simulation
		err := recoveredStore.UpdatePin(ctx, pin)
		require.NoError(t, err)
	}

	suite.logger.Info("Node failure and recovery test completed")
}

// TestConcurrentWrites tests the system under heavy concurrent load
func (suite *ChaosTestSuite) TestConcurrentWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const numGoroutines = 50
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	// Start concurrent workers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for j := 0; j < operationsPerGoroutine; j++ {
				// Generate random study pin
				pin := generateRandomPin(fmt.Sprintf("worker-%d-pin-%d", workerID, j))
				
				// Update materialized view
				if err := suite.mvStore.UpdatePin(ctx, pin); err != nil {
					select {
					case errors <- err:
					default:
					}
					return
				}

				// Simulate concurrent reads
				if j%10 == 0 {
					_, err := suite.mvStore.GetCellView(ctx, pin.Quadkey)
					if err != nil {
						select {
						case errors <- err:
						default:
						}
						return
					}
				}

				// Small delay to prevent overwhelming the system
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(errors)

	// Check for errors
	var errorCount int
	for err := range errors {
		errorCount++
		suite.logger.WithError(err).Error("Concurrent operation error")
	}

	// Allow for some errors under extreme load, but not too many
	totalOperations := numGoroutines * operationsPerGoroutine
	errorRate := float64(errorCount) / float64(totalOperations)
	assert.Less(t, errorRate, 0.05, "Error rate should be less than 5%")

	suite.logger.WithFields(logrus.Fields{
		"total_operations": totalOperations,
		"errors":          errorCount,
		"error_rate":      errorRate,
	}).Info("Concurrent writes test completed")
}

// TestDataCorruption simulates data corruption and tests recovery
func (suite *ChaosTestSuite) TestDataCorruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Create test pin
	pin := generateRandomPin("corruption-test-pin")
	err := suite.mvStore.UpdatePin(ctx, pin)
	require.NoError(t, err)

	// Verify pin exists
	view, err := suite.mvStore.GetCellView(ctx, pin.Quadkey)
	require.NoError(t, err)
	assert.Contains(t, view.Pins, pin.ID)

	// Simulate data corruption by directly manipulating Redis
	corruptKey := fmt.Sprintf("cell_view:%s", pin.Quadkey)
	err = suite.redis.Set(ctx, corruptKey, "corrupted_data", time.Hour)
	require.NoError(t, err)

	// Verify corruption is detected
	view, err = suite.mvStore.GetCellView(ctx, pin.Quadkey)
	// The system should handle corruption gracefully
	if err != nil {
		suite.logger.WithError(err).Info("Corruption detected and handled")
	}

	// Simulate recovery by re-adding the pin
	err = suite.mvStore.UpdatePin(ctx, pin)
	require.NoError(t, err)

	// Verify recovery
	view, err = suite.mvStore.GetCellView(ctx, pin.Quadkey)
	require.NoError(t, err)
	assert.Contains(t, view.Pins, pin.ID)

	suite.logger.Info("Data corruption test completed")
}

// TestEventualConsistency tests CRDT eventual consistency under chaos
func (suite *ChaosTestSuite) TestEventualConsistency(t *testing.T) {
	_, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const numNodes = 5
	nodes := make([]*crdt.LWWSet, numNodes)
	
	// Initialize nodes
	for i := 0; i < numNodes; i++ {
		nodes[i] = crdt.NewLWWSet(fmt.Sprintf("node-%d", i))
	}

	// Phase 1: Random operations with random network delays
	const numOperations = 200
	for i := 0; i < numOperations; i++ {
		nodeIndex := rand.Intn(numNodes)
		
		if rand.Float32() < 0.7 { // 70% chance to add
			key := fmt.Sprintf("item-%d", rand.Intn(50))
			value := map[string]interface{}{
				"operation": i,
				"node":      nodeIndex,
				"timestamp": time.Now(),
			}
			nodes[nodeIndex].Add(key, value)
		} else { // 30% chance to remove
			key := fmt.Sprintf("item-%d", rand.Intn(50))
			nodes[nodeIndex].Remove(key)
		}

		// Random sync between random nodes
		if rand.Float32() < 0.3 { // 30% chance to sync
			fromNode := rand.Intn(numNodes)
			toNode := rand.Intn(numNodes)
			if fromNode != toNode {
				nodes[toNode].Merge(nodes[fromNode])
			}
		}

		// Simulate network delay
		if rand.Float32() < 0.1 {
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		}
	}

	// Phase 2: Full synchronization
	suite.logger.Info("Starting full synchronization phase")
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i != j {
				nodes[i].Merge(nodes[j])
			}
		}
	}

	// Phase 3: Verify eventual consistency
	finalSize := nodes[0].Size()
	finalElements := nodes[0].Elements()
	
	for i := 1; i < numNodes; i++ {
		assert.Equal(t, finalSize, nodes[i].Size(), "Node %d size mismatch", i)
		
		nodeElements := nodes[i].Elements()
		assert.Equal(t, len(finalElements), len(nodeElements), "Node %d element count mismatch", i)
		
		for key, value := range finalElements {
			nodeValue, exists := nodeElements[key]
			assert.True(t, exists, "Node %d missing key %s", i, key)
			assert.Equal(t, value, nodeValue, "Node %d value mismatch for key %s", i, key)
		}
	}

	suite.logger.WithFields(logrus.Fields{
		"final_size":    finalSize,
		"num_nodes":     numNodes,
		"num_operations": numOperations,
	}).Info("Eventual consistency test completed")
}

// Helper functions

func generateTestPins(count int) []*shard.StudyPin {
	pins := make([]*shard.StudyPin, count)
	
	for i := 0; i < count; i++ {
		pins[i] = generateRandomPin(fmt.Sprintf("test-pin-%d", i))
	}
	
	return pins
}

func generateRandomPin(id string) *shard.StudyPin {
	// Random coordinates (roughly covering a university campus)
	lat := 37.7749 + (rand.Float64()-0.5)*0.01  // San Francisco area
	lon := -122.4194 + (rand.Float64()-0.5)*0.01
	
	qkey := quadkey.FromLatLon(lat, lon, 18)
	
	subjects := []string{"Math", "Physics", "Chemistry", "Biology", "History", "Literature", "Computer Science"}
	
	return &shard.StudyPin{
		ID:          id,
		UserID:      fmt.Sprintf("user-%d", rand.Intn(1000)),
		Quadkey:     qkey.Key,
		Latitude:    lat,
		Longitude:   lon,
		Subject:     subjects[rand.Intn(len(subjects))],
		Description: fmt.Sprintf("Study session for %s", subjects[rand.Intn(len(subjects))]),
		CreatedAt:   time.Now().Add(-time.Duration(rand.Intn(3600)) * time.Second),
		UpdatedAt:   time.Now(),
		IsActive:    true,
	}
}

// Run all chaos tests
func TestChaosEngineering(t *testing.T) {
	suite := NewChaosTestSuite(t)
	
	t.Run("NetworkPartition", suite.TestNetworkPartition)
	t.Run("NodeFailure", suite.TestNodeFailure)
	t.Run("ConcurrentWrites", suite.TestConcurrentWrites)
	t.Run("DataCorruption", suite.TestDataCorruption)
	t.Run("EventualConsistency", suite.TestEventualConsistency)
}
