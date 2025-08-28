package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	
	"github.com/cometstudy/geo-sharding/internal/shard"
	"github.com/cometstudy/geo-sharding/internal/streamer"
	"github.com/cometstudy/geo-sharding/pkg/kafka"
	"github.com/cometstudy/geo-sharding/pkg/postgres"
	"github.com/cometstudy/geo-sharding/pkg/redis"
)

func main() {
	var (
		nodeID       = flag.String("node-id", "streamer-1", "Node ID")
		redisAddr    = flag.String("redis-addr", "localhost:6379", "Redis address")
		kafkaBrokers = flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses")
		pgHost       = flag.String("pg-host", "localhost", "PostgreSQL host")
		pgPort       = flag.Int("pg-port", 5432, "PostgreSQL port")
		pgDB         = flag.String("pg-db", "geosharding", "PostgreSQL database")
		pgUser       = flag.String("pg-user", "postgres", "PostgreSQL user")
		pgPassword   = flag.String("pg-password", "postgres", "PostgreSQL password")
		logLevel     = flag.String("log-level", "info", "Log level")
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
		"node_id":       *nodeID,
		"redis_addr":    *redisAddr,
		"kafka_brokers": *kafkaBrokers,
		"pg_host":       *pgHost,
	}).Info("Starting streamer service")

	// Setup PostgreSQL client
	pgClient, err := postgres.NewClient(postgres.Config{
		Host:     *pgHost,
		Port:     *pgPort,
		Database: *pgDB,
		User:     *pgUser,
		Password: *pgPassword,
	}, logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to PostgreSQL")
	}
	defer pgClient.Close()

	// Setup Redis client
	redisClient, err := redis.NewClient(redis.Config{
		Addr: *redisAddr,
	}, logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to Redis")
	}
	defer redisClient.Close()

	// Setup Kafka consumer
	consumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{*kafkaBrokers},
		Topic:   "geo-events",
		GroupID: "streamers",
	}, logger)

	// Setup materialized view store
	mvStore := shard.NewMaterializedViewStore(redisClient, logger)

	// Setup event handlers
	pinHandler := streamer.NewStudyPinHandler(pgClient, mvStore, logger)
	presenceHandler := streamer.NewPresenceHandler(pgClient, mvStore, logger)

	// Register handlers
	consumer.RegisterHandler(pinHandler)
	consumer.RegisterHandler(presenceHandler)

	// Start consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := consumer.Start(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to start consumer")
	}

	logger.Info("Streamer service started")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down streamer service")

	// Graceful shutdown
	cancel()
	if err := consumer.Stop(); err != nil {
		logger.WithError(err).Error("Error stopping consumer")
	}

	logger.Info("Streamer service stopped")
}
