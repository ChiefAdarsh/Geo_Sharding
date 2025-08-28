# Makefile for Geo-Sharding Platform

.PHONY: help setup build test clean docker-build docker-push deploy

# Variables
APP_NAME := geo-sharding
VERSION := $(shell git describe --tags --always --dirty)
DOCKER_REGISTRY := your-registry.com
GO_VERSION := 1.21

# Go parameters
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOMOD := $(GOCMD) mod

# Directories
BUILD_DIR := ./bin
DOCKER_DIR := ./docker

# Binaries
GATEWAY_BINARY := $(BUILD_DIR)/gateway
STREAMER_BINARY := $(BUILD_DIR)/streamer
CONTROL_PLANE_BINARY := $(BUILD_DIR)/control-plane

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Initial setup and dependency installation
	@echo "Setting up development environment..."
	$(GOMOD) download
	$(GOMOD) verify
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/swaggo/swag/cmd/swag@latest
	@echo "Creating directories..."
	mkdir -p $(BUILD_DIR)
	mkdir -p logs
	@echo "Setup complete!"

build: ## Build all binaries
	@echo "Building binaries..."
	$(GOBUILD) -o $(GATEWAY_BINARY) ./cmd/gateway
	$(GOBUILD) -o $(STREAMER_BINARY) ./cmd/streamer
	$(GOBUILD) -o $(CONTROL_PLANE_BINARY) ./cmd/control-plane
	@echo "Build complete!"

build-gateway: ## Build gateway binary
	$(GOBUILD) -o $(GATEWAY_BINARY) ./cmd/gateway

build-streamer: ## Build streamer binary
	$(GOBUILD) -o $(STREAMER_BINARY) ./cmd/streamer

build-control-plane: ## Build control-plane binary
	$(GOBUILD) -o $(CONTROL_PLANE_BINARY) ./cmd/control-plane

test: ## Run unit tests
	$(GOTEST) -v ./...

test-coverage: ## Run tests with coverage
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-integration: ## Run integration tests (requires infrastructure)
	@echo "Starting test infrastructure..."
	docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for services to be ready..."
	sleep 30
	@echo "Running integration tests..."
	$(GOTEST) -v -tags=integration ./test/integration/...
	@echo "Stopping test infrastructure..."
	docker-compose -f docker-compose.test.yml down

test-chaos: ## Run chaos engineering tests
	$(GOTEST) -v -timeout=10m ./test/chaos_test.go

test-load: ## Run load tests
	@echo "Running load tests..."
	# This would typically use a tool like k6 or artillery
	# k6 run test/load/script.js

lint: ## Run linter
	golangci-lint run ./...

fmt: ## Format code
	$(GOCMD) fmt ./...
	$(GOCMD) mod tidy

clean: ## Clean build artifacts
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Development commands

run-gateway: ## Run gateway server locally
	@echo "Starting gateway server..."
	$(GATEWAY_BINARY) \
		--port=8080 \
		--redis-addr=localhost:6379 \
		--kafka-brokers=localhost:9092 \
		--log-level=debug

run-streamer: ## Run streamer service locally
	@echo "Starting streamer service..."
	$(STREAMER_BINARY) \
		--node-id=streamer-local \
		--redis-addr=localhost:6379 \
		--kafka-brokers=localhost:9092 \
		--pg-host=localhost \
		--pg-port=5432 \
		--pg-db=geosharding \
		--pg-user=postgres \
		--pg-password=postgres \
		--log-level=debug

run-control-plane: ## Run control plane locally
	@echo "Starting control plane..."
	$(CONTROL_PLANE_BINARY) \
		--node-id=control-local \
		--bind-addr=localhost:7000 \
		--http-addr=localhost:8090 \
		--data-dir=./data/raft \
		--bootstrap=true \
		--log-level=debug

# Infrastructure commands

infra-up: ## Start local infrastructure
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	sleep 30
	@echo "Infrastructure is ready!"

infra-down: ## Stop local infrastructure
	docker-compose down

infra-logs: ## Show infrastructure logs
	docker-compose logs -f

migrate: ## Run database migrations
	@echo "Running database migrations..."
	docker-compose exec postgres psql -U postgres -d geosharding -f /docker-entrypoint-initdb.d/001_initial_schema.sql
	@echo "Migrations complete!"

# Docker commands

docker-build: ## Build Docker images
	@echo "Building Docker images..."
	docker build -t $(DOCKER_REGISTRY)/$(APP_NAME)-gateway:$(VERSION) -f docker/gateway.Dockerfile .
	docker build -t $(DOCKER_REGISTRY)/$(APP_NAME)-streamer:$(VERSION) -f docker/streamer.Dockerfile .
	docker build -t $(DOCKER_REGISTRY)/$(APP_NAME)-control-plane:$(VERSION) -f docker/control-plane.Dockerfile .
	@echo "Docker images built!"

docker-push: ## Push Docker images to registry
	docker push $(DOCKER_REGISTRY)/$(APP_NAME)-gateway:$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(APP_NAME)-streamer:$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(APP_NAME)-control-plane:$(VERSION)

# Kubernetes commands

k8s-namespace: ## Create Kubernetes namespace
	kubectl apply -f k8s/namespace.yaml

k8s-infra: ## Deploy infrastructure to Kubernetes
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/secrets.yaml
	kubectl apply -f k8s/infrastructure.yaml

k8s-apps: ## Deploy applications to Kubernetes
	kubectl apply -f k8s/control-plane-deployment.yaml
	kubectl apply -f k8s/streamer-deployment.yaml
	kubectl apply -f k8s/gateway-deployment.yaml

k8s-deploy: k8s-namespace k8s-infra k8s-apps ## Full Kubernetes deployment

k8s-status: ## Check Kubernetes deployment status
	kubectl get pods -n geo-sharding
	kubectl get services -n geo-sharding
	kubectl get hpa -n geo-sharding

k8s-logs: ## Show application logs
	kubectl logs -f -l app=gateway -n geo-sharding

k8s-clean: ## Clean up Kubernetes resources
	kubectl delete namespace geo-sharding

# Monitoring commands

monitoring-up: ## Start monitoring stack
	@echo "Starting Prometheus and Grafana..."
	docker-compose -f docker-compose.monitoring.yml up -d
	@echo "Monitoring stack is ready!"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3000 (admin/admin)"

monitoring-down: ## Stop monitoring stack
	docker-compose -f docker-compose.monitoring.yml down

# Utility commands

generate-docs: ## Generate API documentation
	swag init -g cmd/gateway/main.go -o docs/

create-load-test-data: ## Create test data for load testing
	@echo "Creating load test data..."
	go run scripts/generate_test_data.go \
		--count=10000 \
		--redis-addr=localhost:6379 \
		--quadkey-level=18

benchmark-quadkey: ## Benchmark quadkey operations
	go test -bench=BenchmarkQuadkey -benchmem ./internal/quadkey/

benchmark-crdt: ## Benchmark CRDT operations
	go test -bench=BenchmarkCRDT -benchmem ./internal/crdt/

profile-gateway: ## Profile gateway performance
	go test -cpuprofile=gateway.prof -memprofile=gateway.mem ./internal/gateway/
	go tool pprof gateway.prof

security-scan: ## Run security vulnerability scan
	@echo "Running security scan..."
	# This would typically use tools like gosec, govulncheck
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Release commands

release-tag: ## Create and push a release tag
	@read -p "Enter version (e.g., v1.0.0): " version; \
	git tag -a $$version -m "Release $$version"; \
	git push origin $$version

release-notes: ## Generate release notes
	@echo "Generating release notes for $(VERSION)..."
	git log --pretty=format:"- %s" $(shell git describe --tags --abbrev=0 HEAD~1)..HEAD

# CI/CD helpers

ci-test: ## Run tests in CI environment
	$(GOTEST) -v -race -coverprofile=coverage.out ./...

ci-build: ## Build for CI/CD
	CGO_ENABLED=0 GOOS=linux $(GOBUILD) -a -installsuffix cgo -o $(GATEWAY_BINARY) ./cmd/gateway
	CGO_ENABLED=0 GOOS=linux $(GOBUILD) -a -installsuffix cgo -o $(STREAMER_BINARY) ./cmd/streamer
	CGO_ENABLED=0 GOOS=linux $(GOBUILD) -a -installsuffix cgo -o $(CONTROL_PLANE_BINARY) ./cmd/control-plane

# Development workflow

dev-setup: setup infra-up migrate ## Complete development setup
	@echo "Development environment is ready!"
	@echo "Run 'make run-gateway', 'make run-streamer', and 'make run-control-plane' in separate terminals"

dev-reset: infra-down clean ## Reset development environment
	docker system prune -f
	rm -rf data/
	$(MAKE) dev-setup

# Help for common workflows
workflow-help: ## Show common development workflows
	@echo ""
	@echo "Common Development Workflows:"
	@echo ""
	@echo "  First-time setup:"
	@echo "    make dev-setup"
	@echo ""
	@echo "  Daily development:"
	@echo "    make build && make test"
	@echo "    make run-gateway"
	@echo ""
	@echo "  Testing:"
	@echo "    make test                  # Unit tests"
	@echo "    make test-integration      # Integration tests"
	@echo "    make test-chaos           # Chaos tests"
	@echo ""
	@echo "  Deployment:"
	@echo "    make docker-build         # Build images"
	@echo "    make k8s-deploy           # Deploy to K8s"
	@echo ""
	@echo "  Monitoring:"
	@echo "    make monitoring-up        # Start Prometheus/Grafana"
	@echo ""

.DEFAULT_GOAL := help
