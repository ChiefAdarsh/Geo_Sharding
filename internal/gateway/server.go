package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	
	"github.com/cometstudy/geo-sharding/internal/quadkey"
	"github.com/cometstudy/geo-sharding/internal/shard"
	"github.com/cometstudy/geo-sharding/pkg/kafka"
)

// Server implements the HTTP gateway
type Server struct {
	router       *mux.Router
	logger       *logrus.Logger
	producer     *kafka.Producer
	mvStore      *shard.MaterializedViewStore
	upgrader     websocket.Upgrader
	clients      map[string]*WebSocketClient
	clientsMu    sync.RWMutex
	shutdownCh   chan struct{}
	metrics      *Metrics
}

// Config holds server configuration
type Config struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// Metrics holds server metrics
type Metrics struct {
	RequestsTotal     int64
	ActiveConnections int64
	ErrorsTotal       int64
	ResponseTimes     map[string]time.Duration
}

// WebSocketClient represents a connected WebSocket client
type WebSocketClient struct {
	ID       string
	Conn     *websocket.Conn
	UserID   string
	Quadkeys map[string]bool // Subscribed quadkeys
	SendCh   chan []byte
	CloseCh  chan struct{}
}

// StudyPinRequest represents a request to create a study pin
type StudyPinRequest struct {
	Subject     string  `json:"subject"`
	Description string  `json:"description"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	ExpiresIn   int     `json:"expires_in"` // minutes
}

// PresenceUpdateRequest represents a presence update
type PresenceUpdateRequest struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Status    string  `json:"status"`
}

// NewServer creates a new HTTP gateway server
func NewServer(config Config, logger *logrus.Logger, producer *kafka.Producer, mvStore *shard.MaterializedViewStore) *Server {
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 30 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 30 * time.Second
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = 120 * time.Second
	}

	server := &Server{
		router:     mux.NewRouter(),
		logger:     logger,
		producer:   producer,
		mvStore:    mvStore,
		clients:    make(map[string]*WebSocketClient),
		shutdownCh: make(chan struct{}),
		metrics:    &Metrics{ResponseTimes: make(map[string]time.Duration)},
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				// In production, implement proper origin checking
				return true
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}

	server.setupRoutes()
	return server
}

// setupRoutes configures HTTP routes
func (s *Server) setupRoutes() {
	// API routes
	api := s.router.PathPrefix("/api/v1").Subrouter()
	
	// Middleware
	api.Use(s.loggingMiddleware)
	api.Use(s.metricsMiddleware)
	api.Use(s.corsMiddleware)

	// Study pins
	api.HandleFunc("/pins", s.createStudyPin).Methods("POST")
	api.HandleFunc("/pins/{id}", s.getStudyPin).Methods("GET")
	api.HandleFunc("/pins/{id}", s.updateStudyPin).Methods("PUT")
	api.HandleFunc("/pins/{id}", s.deleteStudyPin).Methods("DELETE")
	api.HandleFunc("/pins/nearby", s.getNearbyPins).Methods("GET")
	api.HandleFunc("/pins/user/{userID}", s.getUserPins).Methods("GET")

	// Presence
	api.HandleFunc("/presence", s.updatePresence).Methods("POST")
	api.HandleFunc("/presence/nearby", s.getNearbyPresence).Methods("GET")

	// WebSocket
	s.router.HandleFunc("/ws", s.handleWebSocket)

	// Health and metrics
	s.router.HandleFunc("/health", s.healthCheck).Methods("GET")
	s.router.HandleFunc("/metrics", s.getMetrics).Methods("GET")

	// Static files (in production, use a CDN)
	s.router.PathPrefix("/").Handler(http.FileServer(http.Dir("./static/")))
}

// Start starts the HTTP server
func (s *Server) Start(port int) error {
	addr := fmt.Sprintf(":%d", port)
	
	server := &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	s.logger.WithField("port", port).Info("Starting HTTP gateway server")
	return server.ListenAndServe()
}

// Study Pin Handlers

func (s *Server) createStudyPin(w http.ResponseWriter, r *http.Request) {
	var req StudyPinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate request
	if req.Subject == "" || req.Latitude == 0 || req.Longitude == 0 {
		s.writeError(w, http.StatusBadRequest, "Missing required fields")
		return
	}

	// Get user ID from context (would be set by auth middleware)
	userID := s.getUserIDFromContext(r.Context())
	if userID == "" {
		s.writeError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	// Generate quadkey
	qkey := quadkey.FromLatLon(req.Latitude, req.Longitude, 18)
	
	// Create study pin
	pin := &shard.StudyPin{
		ID:          uuid.New().String(),
		UserID:      userID,
		Quadkey:     qkey.Key,
		Latitude:    req.Latitude,
		Longitude:   req.Longitude,
		Subject:     req.Subject,
		Description: req.Description,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		IsActive:    true,
	}

	if req.ExpiresIn > 0 {
		pin.ExpiresAt = time.Now().Add(time.Duration(req.ExpiresIn) * time.Minute)
	}

	// Publish event
	event := kafka.Event{
		ID:        uuid.New().String(),
		Type:      kafka.EventTypes.StudyPinCreated,
		Source:    "gateway",
		Timestamp: time.Now(),
		Quadkey:   qkey.Key,
		Data: map[string]interface{}{
			"pin": pin,
		},
	}

	if err := s.producer.PublishEvent(r.Context(), event); err != nil {
		s.logger.WithError(err).Error("Failed to publish study pin created event")
		s.writeError(w, http.StatusInternalServerError, "Failed to create study pin")
		return
	}

	// Update materialized view
	if err := s.mvStore.UpdatePin(r.Context(), pin); err != nil {
		s.logger.WithError(err).Warn("Failed to update materialized view")
	}

	// Broadcast to WebSocket clients
	s.broadcastToQuadkey(qkey.Key, map[string]interface{}{
		"type": "pin_created",
		"data": pin,
	})

	s.writeJSON(w, http.StatusCreated, pin)
}

func (s *Server) getStudyPin(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pinID := vars["id"]

	if pinID == "" {
		s.writeError(w, http.StatusBadRequest, "Pin ID required")
		return
	}

	// This would typically query the materialized view or database
	// For now, return a placeholder response
	s.writeJSON(w, http.StatusOK, map[string]string{"message": "Pin details would be returned here"})
}

func (s *Server) updateStudyPin(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pinID := vars["id"]

	if pinID == "" {
		s.writeError(w, http.StatusBadRequest, "Pin ID required")
		return
	}

	var req StudyPinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	userID := s.getUserIDFromContext(r.Context())
	if userID == "" {
		s.writeError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	// Publish update event
	event := kafka.Event{
		ID:        uuid.New().String(),
		Type:      kafka.EventTypes.StudyPinUpdated,
		Source:    "gateway",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"pin_id": pinID,
			"user_id": userID,
			"updates": req,
		},
	}

	if err := s.producer.PublishEvent(r.Context(), event); err != nil {
		s.logger.WithError(err).Error("Failed to publish study pin updated event")
		s.writeError(w, http.StatusInternalServerError, "Failed to update study pin")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"message": "Pin updated successfully"})
}

func (s *Server) deleteStudyPin(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pinID := vars["id"]

	if pinID == "" {
		s.writeError(w, http.StatusBadRequest, "Pin ID required")
		return
	}

	userID := s.getUserIDFromContext(r.Context())
	if userID == "" {
		s.writeError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	// Publish delete event
	event := kafka.Event{
		ID:        uuid.New().String(),
		Type:      kafka.EventTypes.StudyPinDeleted,
		Source:    "gateway",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"pin_id": pinID,
			"user_id": userID,
		},
	}

	if err := s.producer.PublishEvent(r.Context(), event); err != nil {
		s.logger.WithError(err).Error("Failed to publish study pin deleted event")
		s.writeError(w, http.StatusInternalServerError, "Failed to delete study pin")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"message": "Pin deleted successfully"})
}

func (s *Server) getNearbyPins(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	latStr := r.URL.Query().Get("lat")
	lonStr := r.URL.Query().Get("lon")
	radiusStr := r.URL.Query().Get("radius")

	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid latitude")
		return
	}

	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid longitude")
		return
	}

	radius := 1000.0 // Default 1km
	if radiusStr != "" {
		if r, err := strconv.ParseFloat(radiusStr, 64); err == nil {
			radius = r
		}
	}

	// Get nearby cells from materialized view
	views, err := s.mvStore.GetNearbyCells(r.Context(), lat, lon, radius, 18)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get nearby cells")
		s.writeError(w, http.StatusInternalServerError, "Failed to get nearby pins")
		return
	}

	// Aggregate pins from all views
	pins := make([]*shard.StudyPin, 0)
	for _, view := range views {
		for _, pin := range view.Pins {
			if pin.IsActive && (pin.ExpiresAt.IsZero() || pin.ExpiresAt.After(time.Now())) {
				pins = append(pins, pin)
			}
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"pins":   pins,
		"count":  len(pins),
		"radius": radius,
	})
}

func (s *Server) getUserPins(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["userID"]

	if userID == "" {
		s.writeError(w, http.StatusBadRequest, "User ID required")
		return
	}

	pins, err := s.mvStore.GetPinsByUser(r.Context(), userID)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get user pins")
		s.writeError(w, http.StatusInternalServerError, "Failed to get user pins")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"pins":  pins,
		"count": len(pins),
	})
}

// Presence Handlers

func (s *Server) updatePresence(w http.ResponseWriter, r *http.Request) {
	var req PresenceUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	userID := s.getUserIDFromContext(r.Context())
	if userID == "" {
		s.writeError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	// Generate quadkey
	qkey := quadkey.FromLatLon(req.Latitude, req.Longitude, 18)

	// Create presence info
	presence := &shard.PresenceInfo{
		UserID:    userID,
		Quadkey:   qkey.Key,
		Latitude:  req.Latitude,
		Longitude: req.Longitude,
		Status:    req.Status,
		LastSeen:  time.Now(),
		SessionID: s.getSessionIDFromContext(r.Context()),
	}

	// Publish presence event
	event := kafka.Event{
		ID:        uuid.New().String(),
		Type:      kafka.EventTypes.PresenceUpdate,
		Source:    "gateway",
		Timestamp: time.Now(),
		Quadkey:   qkey.Key,
		Data: map[string]interface{}{
			"presence": presence,
		},
	}

	if err := s.producer.PublishEvent(r.Context(), event); err != nil {
		s.logger.WithError(err).Error("Failed to publish presence update event")
		s.writeError(w, http.StatusInternalServerError, "Failed to update presence")
		return
	}

	// Update materialized view
	if err := s.mvStore.UpdatePresence(r.Context(), presence); err != nil {
		s.logger.WithError(err).Warn("Failed to update presence in materialized view")
	}

	// Broadcast to WebSocket clients
	s.broadcastToQuadkey(qkey.Key, map[string]interface{}{
		"type": "presence_update",
		"data": presence,
	})

	s.writeJSON(w, http.StatusOK, map[string]string{"message": "Presence updated successfully"})
}

func (s *Server) getNearbyPresence(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	latStr := r.URL.Query().Get("lat")
	lonStr := r.URL.Query().Get("lon")
	radiusStr := r.URL.Query().Get("radius")

	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid latitude")
		return
	}

	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid longitude")
		return
	}

	radius := 1000.0 // Default 1km
	if radiusStr != "" {
		if r, err := strconv.ParseFloat(radiusStr, 64); err == nil {
			radius = r
		}
	}

	// Get nearby cells from materialized view
	views, err := s.mvStore.GetNearbyCells(r.Context(), lat, lon, radius, 18)
	if err != nil {
		s.logger.WithError(err).Error("Failed to get nearby cells")
		s.writeError(w, http.StatusInternalServerError, "Failed to get nearby presence")
		return
	}

	// Aggregate presence from all views
	presence := make([]*shard.PresenceInfo, 0)
	for _, view := range views {
		for _, p := range view.Presence {
			if time.Since(p.LastSeen) < 5*time.Minute { // Only active presence
				presence = append(presence, p)
			}
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"presence": presence,
		"count":    len(presence),
		"radius":   radius,
	})
}

// WebSocket Handler

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.WithError(err).Error("Failed to upgrade WebSocket connection")
		return
	}

	userID := s.getUserIDFromContext(r.Context())
	if userID == "" {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseUnsupportedData, "Authentication required"))
		conn.Close()
		return
	}

	client := &WebSocketClient{
		ID:       uuid.New().String(),
		Conn:     conn,
		UserID:   userID,
		Quadkeys: make(map[string]bool),
		SendCh:   make(chan []byte, 256),
		CloseCh:  make(chan struct{}),
	}

	s.clientsMu.Lock()
	s.clients[client.ID] = client
	s.clientsMu.Unlock()

	s.logger.WithFields(logrus.Fields{
		"client_id": client.ID,
		"user_id":   userID,
	}).Info("WebSocket client connected")

	// Start client goroutines
	go s.handleWebSocketWriter(client)
	go s.handleWebSocketReader(client)
}

func (s *Server) handleWebSocketWriter(client *WebSocketClient) {
	defer func() {
		client.Conn.Close()
		s.removeClient(client.ID)
	}()

	ticker := time.NewTicker(54 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-client.SendCh:
			if !ok {
				client.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			client.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := client.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				s.logger.WithError(err).Error("Failed to write WebSocket message")
				return
			}

		case <-ticker.C:
			client.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-client.CloseCh:
			return
		}
	}
}

func (s *Server) handleWebSocketReader(client *WebSocketClient) {
	defer close(client.CloseCh)

	client.Conn.SetReadLimit(512)
	client.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	client.Conn.SetPongHandler(func(string) error {
		client.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := client.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.WithError(err).Error("WebSocket error")
			}
			break
		}

		// Handle client messages (e.g., subscribe to quadkeys)
		s.handleWebSocketMessage(client, message)
	}
}

func (s *Server) handleWebSocketMessage(client *WebSocketClient, message []byte) {
	var msg map[string]interface{}
	if err := json.Unmarshal(message, &msg); err != nil {
		s.logger.WithError(err).Error("Failed to unmarshal WebSocket message")
		return
	}

	msgType, ok := msg["type"].(string)
	if !ok {
		return
	}

	switch msgType {
	case "subscribe":
		if quadkeys, ok := msg["quadkeys"].([]interface{}); ok {
			for _, qk := range quadkeys {
				if quadkey, ok := qk.(string); ok {
					client.Quadkeys[quadkey] = true
				}
			}
		}
	case "unsubscribe":
		if quadkeys, ok := msg["quadkeys"].([]interface{}); ok {
			for _, qk := range quadkeys {
				if quadkey, ok := qk.(string); ok {
					delete(client.Quadkeys, quadkey)
				}
			}
		}
	}
}

// Utility Methods

func (s *Server) removeClient(clientID string) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	
	if client, exists := s.clients[clientID]; exists {
		close(client.SendCh)
		delete(s.clients, clientID)
		s.logger.WithField("client_id", clientID).Info("WebSocket client disconnected")
	}
}

func (s *Server) broadcastToQuadkey(quadkey string, message map[string]interface{}) {
	data, err := json.Marshal(message)
	if err != nil {
		s.logger.WithError(err).Error("Failed to marshal broadcast message")
		return
	}

	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	for _, client := range s.clients {
		if client.Quadkeys[quadkey] {
			select {
			case client.SendCh <- data:
			default:
				// Channel full, skip this client
			}
		}
	}
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.metrics.ErrorsTotal++
	s.writeJSON(w, status, map[string]string{"error": message})
}

func (s *Server) getUserIDFromContext(ctx context.Context) string {
	// In a real implementation, this would extract the user ID from JWT or session
	// For now, return a placeholder
	return "user-123"
}

func (s *Server) getSessionIDFromContext(ctx context.Context) string {
	// In a real implementation, this would extract the session ID
	return uuid.New().String()
}

// Middleware

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		
		s.logger.WithFields(logrus.Fields{
			"method":   r.Method,
			"url":      r.URL.Path,
			"duration": duration,
		}).Info("HTTP request")
	})
}

func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		s.metrics.RequestsTotal++
		
		next.ServeHTTP(w, r)
		
		duration := time.Since(start)
		s.metrics.ResponseTimes[r.URL.Path] = duration
	})
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		next.ServeHTTP(w, r)
	})
}

// Health and Metrics

func (s *Server) healthCheck(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"version":   "1.0.0",
	})
}

func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request) {
	s.clientsMu.RLock()
	activeConnections := len(s.clients)
	s.clientsMu.RUnlock()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"requests_total":      s.metrics.RequestsTotal,
		"active_connections":  activeConnections,
		"errors_total":        s.metrics.ErrorsTotal,
		"response_times":      s.metrics.ResponseTimes,
	})
}
