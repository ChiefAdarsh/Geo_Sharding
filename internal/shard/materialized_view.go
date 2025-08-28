package shard

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cometstudy/geo-sharding/internal/quadkey"
	"github.com/cometstudy/geo-sharding/pkg/redis"
	"github.com/sirupsen/logrus"
)

// StudyPin represents a study pin on the map
type StudyPin struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	Quadkey     string    `json:"quadkey"`
	Latitude    float64   `json:"latitude"`
	Longitude   float64   `json:"longitude"`
	Subject     string    `json:"subject"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	IsActive    bool      `json:"is_active"`
}

// PresenceInfo represents user presence information
type PresenceInfo struct {
	UserID      string    `json:"user_id"`
	Quadkey     string    `json:"quadkey"`
	Latitude    float64   `json:"latitude"`
	Longitude   float64   `json:"longitude"`
	Status      string    `json:"status"` // online, studying, available, busy
	LastSeen    time.Time `json:"last_seen"`
	SessionID   string    `json:"session_id"`
	UserAgent   string    `json:"user_agent,omitempty"`
}

// CellView represents the materialized view of a quadkey cell
type CellView struct {
	Quadkey   string                   `json:"quadkey"`
	Pins      map[string]*StudyPin     `json:"pins"`
	Presence  map[string]*PresenceInfo `json:"presence"`
	Counters  map[string]int64         `json:"counters"`
	UpdatedAt time.Time                `json:"updated_at"`
	Version   int64                    `json:"version"`
}

// MaterializedViewStore manages Redis-backed materialized views
type MaterializedViewStore struct {
	redis      *redis.Client
	logger     *logrus.Logger
	mu         sync.RWMutex
	ttl        time.Duration
	batchSize  int
}

// NewMaterializedViewStore creates a new materialized view store
func NewMaterializedViewStore(redisClient *redis.Client, logger *logrus.Logger) *MaterializedViewStore {
	return &MaterializedViewStore{
		redis:     redisClient,
		logger:    logger,
		ttl:       24 * time.Hour, // Default TTL for views
		batchSize: 100,
	}
}

// GetCellView retrieves the materialized view for a quadkey cell
func (mv *MaterializedViewStore) GetCellView(ctx context.Context, qkey string) (*CellView, error) {
	key := mv.cellViewKey(qkey)
	
	var view CellView
	err := mv.redis.Get(ctx, key, &view)
	if err != nil {
		if err.Error() == "redis: nil" {
			// Return empty view if not found
			return &CellView{
				Quadkey:  qkey,
				Pins:     make(map[string]*StudyPin),
				Presence: make(map[string]*PresenceInfo),
				Counters: make(map[string]int64),
				Version:  0,
			}, nil
		}
		return nil, fmt.Errorf("failed to get cell view for %s: %w", qkey, err)
	}

	return &view, nil
}

// GetNearbyCells retrieves materialized views for nearby quadkey cells
func (mv *MaterializedViewStore) GetNearbyCells(ctx context.Context, lat, lon float64, radiusMeters float64, level int) ([]*CellView, error) {
	nearbyQuadkeys := quadkey.GetNearbyQuadkeys(lat, lon, radiusMeters, level)
	
	views := make([]*CellView, 0, len(nearbyQuadkeys))
	
	// Batch fetch views
	keys := make([]string, len(nearbyQuadkeys))
	for i, qk := range nearbyQuadkeys {
		keys[i] = mv.cellViewKey(qk.Key)
	}

	// Use pipeline for batch operations
	pipe := mv.redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	
	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err.Error() != "redis: nil" {
		return nil, fmt.Errorf("failed to batch get cell views: %w", err)
	}

	// Process results
	for i, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			if err.Error() == "redis: nil" {
				// Create empty view for missing cells
				views = append(views, &CellView{
					Quadkey:  nearbyQuadkeys[i].Key,
					Pins:     make(map[string]*StudyPin),
					Presence: make(map[string]*PresenceInfo),
					Counters: make(map[string]int64),
					Version:  0,
				})
				continue
			}
			mv.logger.WithError(err).Warnf("Failed to get view for quadkey %s", nearbyQuadkeys[i].Key)
			continue
		}

		var view CellView
		if err := json.Unmarshal([]byte(result), &view); err != nil {
			mv.logger.WithError(err).Warnf("Failed to unmarshal view for quadkey %s", nearbyQuadkeys[i].Key)
			continue
		}

		views = append(views, &view)
	}

	return views, nil
}

// UpdatePin updates a study pin in the materialized view
func (mv *MaterializedViewStore) UpdatePin(ctx context.Context, pin *StudyPin) error {
	view, err := mv.GetCellView(ctx, pin.Quadkey)
	if err != nil {
		return err
	}

	// Update pin
	if view.Pins == nil {
		view.Pins = make(map[string]*StudyPin)
	}
	view.Pins[pin.ID] = pin
	view.UpdatedAt = time.Now()
	view.Version++

	// Store updated view
	key := mv.cellViewKey(pin.Quadkey)
	if err := mv.redis.Set(ctx, key, view, mv.ttl); err != nil {
		return fmt.Errorf("failed to update pin in cell view: %w", err)
	}

	// Update indexes
	if err := mv.updatePinIndexes(ctx, pin); err != nil {
		mv.logger.WithError(err).Warn("Failed to update pin indexes")
	}

	return nil
}

// RemovePin removes a study pin from the materialized view
func (mv *MaterializedViewStore) RemovePin(ctx context.Context, pinID, qkey string) error {
	view, err := mv.GetCellView(ctx, qkey)
	if err != nil {
		return err
	}

	// Remove pin
	if view.Pins != nil {
		delete(view.Pins, pinID)
		view.UpdatedAt = time.Now()
		view.Version++

		// Store updated view
		key := mv.cellViewKey(qkey)
		if err := mv.redis.Set(ctx, key, view, mv.ttl); err != nil {
			return fmt.Errorf("failed to remove pin from cell view: %w", err)
		}
	}

	// Remove from indexes
	if err := mv.removePinIndexes(ctx, pinID, qkey); err != nil {
		mv.logger.WithError(err).Warn("Failed to remove pin indexes")
	}

	return nil
}

// UpdatePresence updates user presence in the materialized view
func (mv *MaterializedViewStore) UpdatePresence(ctx context.Context, presence *PresenceInfo) error {
	view, err := mv.GetCellView(ctx, presence.Quadkey)
	if err != nil {
		return err
	}

	// Update presence
	if view.Presence == nil {
		view.Presence = make(map[string]*PresenceInfo)
	}
	view.Presence[presence.UserID] = presence
	view.UpdatedAt = time.Now()
	view.Version++

	// Store updated view
	key := mv.cellViewKey(presence.Quadkey)
	if err := mv.redis.Set(ctx, key, view, mv.ttl); err != nil {
		return fmt.Errorf("failed to update presence in cell view: %w", err)
	}

	// Set presence expiration (auto-cleanup inactive users)
	presenceKey := mv.presenceKey(presence.UserID)
	if err := mv.redis.Set(ctx, presenceKey, presence, 5*time.Minute); err != nil {
		mv.logger.WithError(err).Warn("Failed to set presence expiration")
	}

	return nil
}

// RemovePresence removes user presence from the materialized view
func (mv *MaterializedViewStore) RemovePresence(ctx context.Context, userID, qkey string) error {
	view, err := mv.GetCellView(ctx, qkey)
	if err != nil {
		return err
	}

	// Remove presence
	if view.Presence != nil {
		delete(view.Presence, userID)
		view.UpdatedAt = time.Now()
		view.Version++

		// Store updated view
		key := mv.cellViewKey(qkey)
		if err := mv.redis.Set(ctx, key, view, mv.ttl); err != nil {
			return fmt.Errorf("failed to remove presence from cell view: %w", err)
		}
	}

	// Remove presence key
	presenceKey := mv.presenceKey(userID)
	if err := mv.redis.Del(ctx, presenceKey); err != nil {
		mv.logger.WithError(err).Warn("Failed to remove presence key")
	}

	return nil
}

// UpdateCounter updates a counter in the materialized view
func (mv *MaterializedViewStore) UpdateCounter(ctx context.Context, qkey, counterName string, value int64) error {
	view, err := mv.GetCellView(ctx, qkey)
	if err != nil {
		return err
	}

	// Update counter
	if view.Counters == nil {
		view.Counters = make(map[string]int64)
	}
	view.Counters[counterName] = value
	view.UpdatedAt = time.Now()
	view.Version++

	// Store updated view
	key := mv.cellViewKey(qkey)
	if err := mv.redis.Set(ctx, key, view, mv.ttl); err != nil {
		return fmt.Errorf("failed to update counter in cell view: %w", err)
	}

	return nil
}

// GetPinsByUser retrieves all pins for a specific user
func (mv *MaterializedViewStore) GetPinsByUser(ctx context.Context, userID string) ([]*StudyPin, error) {
	userPinsKey := mv.userPinsKey(userID)
	
	pinIDs, err := mv.redis.SMembers(ctx, userPinsKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get user pins: %w", err)
	}

	pins := make([]*StudyPin, 0, len(pinIDs))
	
	// Batch fetch pins
	pipe := mv.redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(pinIDs))
	
	for i, pinID := range pinIDs {
		cmds[i] = pipe.Get(ctx, mv.pinKey(pinID))
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err.Error() != "redis: nil" {
		return nil, fmt.Errorf("failed to batch get pins: %w", err)
	}

	// Process results
	for _, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			if err.Error() == "redis: nil" {
				continue // Skip missing pins
			}
			mv.logger.WithError(err).Warn("Failed to get pin")
			continue
		}

		var pin StudyPin
		if err := json.Unmarshal([]byte(result), &pin); err != nil {
			mv.logger.WithError(err).Warn("Failed to unmarshal pin")
			continue
		}

		pins = append(pins, &pin)
	}

	return pins, nil
}

// Helper methods for Redis key generation
func (mv *MaterializedViewStore) cellViewKey(quadkey string) string {
	return fmt.Sprintf("cell_view:%s", quadkey)
}

func (mv *MaterializedViewStore) pinKey(pinID string) string {
	return fmt.Sprintf("pin:%s", pinID)
}

func (mv *MaterializedViewStore) presenceKey(userID string) string {
	return fmt.Sprintf("presence:%s", userID)
}

func (mv *MaterializedViewStore) userPinsKey(userID string) string {
	return fmt.Sprintf("user_pins:%s", userID)
}

func (mv *MaterializedViewStore) quadkeyPinsKey(quadkey string) string {
	return fmt.Sprintf("quadkey_pins:%s", quadkey)
}

// updatePinIndexes maintains secondary indexes for pins
func (mv *MaterializedViewStore) updatePinIndexes(ctx context.Context, pin *StudyPin) error {
	pipe := mv.redis.Pipeline()
	
	// Store individual pin
	pipe.Set(ctx, mv.pinKey(pin.ID), pin, mv.ttl)
	
	// Update user pins index
	pipe.SAdd(ctx, mv.userPinsKey(pin.UserID), pin.ID)
	pipe.Expire(ctx, mv.userPinsKey(pin.UserID), mv.ttl)
	
	// Update quadkey pins index
	pipe.SAdd(ctx, mv.quadkeyPinsKey(pin.Quadkey), pin.ID)
	pipe.Expire(ctx, mv.quadkeyPinsKey(pin.Quadkey), mv.ttl)
	
	// Add to geospatial index
	pipe.GeoAdd(ctx, "pins_geo", &redis.GeoLocation{
		Name:      pin.ID,
		Longitude: pin.Longitude,
		Latitude:  pin.Latitude,
	})

	_, err := pipe.Exec(ctx)
	return err
}

// removePinIndexes removes pin from secondary indexes
func (mv *MaterializedViewStore) removePinIndexes(ctx context.Context, pinID, qkey string) error {
	pipe := mv.redis.Pipeline()
	
	// Remove individual pin
	pipe.Del(ctx, mv.pinKey(pinID))
	
	// Remove from quadkey pins index
	pipe.SRem(ctx, mv.quadkeyPinsKey(qkey), pinID)
	
	// Remove from geospatial index
	pipe.ZRem(ctx, "pins_geo", pinID)

	_, err := pipe.Exec(ctx)
	return err
}

// CleanupExpiredViews removes expired views and performs maintenance
func (mv *MaterializedViewStore) CleanupExpiredViews(ctx context.Context) error {
	// This would typically be run by a background job
	// Implementation would scan for expired keys and clean them up
	mv.logger.Info("Running materialized view cleanup")
	
	// Scan for expired presence entries
	pattern := "presence:*"
	iter := mv.redis.GetRawClient().Scan(ctx, 0, pattern, 100).Iterator()
	
	expiredKeys := make([]string, 0)
	for iter.Next(ctx) {
		key := iter.Val()
		// Check if key has expired or should be cleaned up
		exists, err := mv.redis.Exists(ctx, key)
		if err != nil {
			continue
		}
		if exists == 0 {
			expiredKeys = append(expiredKeys, key)
		}
	}
	
	if len(expiredKeys) > 0 {
		if err := mv.redis.Del(ctx, expiredKeys...); err != nil {
			return fmt.Errorf("failed to cleanup expired keys: %w", err)
		}
		mv.logger.WithField("count", len(expiredKeys)).Info("Cleaned up expired presence keys")
	}

	return nil
}
