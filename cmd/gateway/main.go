package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	
	"github.com/cometstudy/geo-sharding/internal/gateway"
	"github.com/cometstudy/geo-sharding/internal/shard"
	"github.com/cometstudy/geo-sharding/pkg/kafka"
	"github.com/cometstudy/geo-sharding/pkg/redis"
)

func main() {
	var (
		port        = flag.Int("port", 8080, "HTTP server port")
		redisAddr   = flag.String("redis-addr", "localhost:6379", "Redis address")
		kafkaBrokers = flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses")
		logLevel    = flag.String("log-level", "info", "Log level")
	)
	flag.Parse()

	// Setup logger
	logger := logrus.New()
	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logger.Fatal("Invalid log level")
	}
	logger.SetLevel(level)
	logger.SetFormatter(&logrus.JSONFormatter{})

	logger.WithFields(logrus.Fields{
		"port":          *port,
		"redis_addr":    *redisAddr,
		"kafka_brokers": *kafkaBrokers,
	}).Info("Starting gateway server")

	// Setup Redis client
	redisClient, err := redis.NewClient(redis.Config{
		Addr: *redisAddr,
	}, logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to Redis")
	}
	defer redisClient.Close()

	// Setup Kafka producer
	producer := kafka.NewProducer(kafka.ProducerConfig{
		Brokers: []string{*kafkaBrokers},
		Topic:   "geo-events",
	}, logger)
	defer producer.Close()

	// Setup materialized view store
	mvStore := shard.NewMaterializedViewStore(redisClient, logger)

	// Setup HTTP server
	server := gateway.NewServer(gateway.Config{
		Port: *port,
	}, logger, producer, mvStore)

	// Start server in goroutine
	go func() {
		if err := server.Start(*port); err != nil {
			logger.WithError(err).Fatal("Failed to start server")
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down gateway server")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Cleanup
	logger.Info("Gateway server stopped")
}
