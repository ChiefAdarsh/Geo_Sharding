package streamer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
	
	"geo-sharding/internal/shard"
	"geo-sharding/pkg/kafka"
	"geo-sharding/pkg/postgres"
)

// StudyPinHandler handles study pin events
type StudyPinHandler struct {
	db      *postgres.Client
	mvStore *shard.MaterializedViewStore
	logger  *logrus.Logger
}

// NewStudyPinHandler creates a new study pin handler
func NewStudyPinHandler(db *postgres.Client, mvStore *shard.MaterializedViewStore, logger *logrus.Logger) *StudyPinHandler {
	return &StudyPinHandler{
		db:      db,
		mvStore: mvStore,
		logger:  logger,
	}
}

// HandleEvent processes study pin events
func (h *StudyPinHandler) HandleEvent(ctx context.Context, event kafka.Event) error {
	switch event.Type {
	case kafka.EventTypes.StudyPinCreated:
		return h.handlePinCreated(ctx, event)
	case kafka.EventTypes.StudyPinUpdated:
		return h.handlePinUpdated(ctx, event)
	case kafka.EventTypes.StudyPinDeleted:
		return h.handlePinDeleted(ctx, event)
	default:
		h.logger.WithField("event_type", event.Type).Debug("Ignoring unsupported event type")
		return nil
	}
}

// GetEventTypes returns the event types this handler supports
func (h *StudyPinHandler) GetEventTypes() []string {
	return []string{
		kafka.EventTypes.StudyPinCreated,
		kafka.EventTypes.StudyPinUpdated,
		kafka.EventTypes.StudyPinDeleted,
	}
}

func (h *StudyPinHandler) handlePinCreated(ctx context.Context, event kafka.Event) error {
	pinData, ok := event.Data["pin"].(map[string]interface{})
	if !ok {
		return nil // Skip malformed events
	}
	
	// Convert to StudyPin struct
	pinJSON, _ := json.Marshal(pinData)
	var pin shard.StudyPin
	if err := json.Unmarshal(pinJSON, &pin); err != nil {
		h.logger.WithError(err).Error("Failed to unmarshal study pin")
		return err
	}

	// Insert into PostgreSQL
	query := `
		INSERT INTO study_pins (
			id, user_id, quadkey, location, subject, description, 
			created_at, updated_at, expires_at, is_active
		) VALUES (
			$1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326), $6, $7,
			$8, $9, $10, $11
		)
		ON CONFLICT (id) DO NOTHING
	`
	
	_, err := h.db.Exec(query,
		pin.ID,
		pin.UserID,
		pin.Quadkey,
		pin.Longitude,
		pin.Latitude,
		pin.Subject,
		pin.Description,
		pin.CreatedAt,
		pin.UpdatedAt,
		nullTimePtr(pin.ExpiresAt),
		pin.IsActive,
	)
	
	if err != nil {
		h.logger.WithError(err).Error("Failed to insert study pin")
		return err
	}

	// Update materialized view
	if err := h.mvStore.UpdatePin(ctx, &pin); err != nil {
		h.logger.WithError(err).Warn("Failed to update materialized view")
	}

	h.logger.WithFields(logrus.Fields{
		"pin_id":  pin.ID,
		"user_id": pin.UserID,
		"quadkey": pin.Quadkey,
	}).Info("Study pin created")

	return nil
}

func (h *StudyPinHandler) handlePinUpdated(ctx context.Context, event kafka.Event) error {
	pinID, ok := event.Data["pin_id"].(string)
	if !ok {
		return nil
	}

	updates, ok := event.Data["updates"].(map[string]interface{})
	if !ok {
		return nil
	}

	// Build dynamic update query based on provided fields
	setParts := []string{}
	args := []interface{}{}
	argCount := 1

	if subject, ok := updates["subject"].(string); ok {
		setParts = append(setParts, "subject = $"+string(rune('0'+argCount)))
		args = append(args, subject)
		argCount++
	}

	if description, ok := updates["description"].(string); ok {
		setParts = append(setParts, "description = $"+string(rune('0'+argCount)))
		args = append(args, description)
		argCount++
	}

	if len(setParts) == 0 {
		return nil // Nothing to update
	}

	setParts = append(setParts, "updated_at = NOW()")
	args = append(args, pinID)

	query := "UPDATE study_pins SET " + setParts[0]
	for i := 1; i < len(setParts); i++ {
		query += ", " + setParts[i]
	}
	query += " WHERE id = $" + string(rune('0'+argCount))

	_, err := h.db.Exec(query, args...)
	if err != nil {
		h.logger.WithError(err).Error("Failed to update study pin")
		return err
	}

	h.logger.WithField("pin_id", pinID).Info("Study pin updated")
	return nil
}

func (h *StudyPinHandler) handlePinDeleted(ctx context.Context, event kafka.Event) error {
	pinID, ok := event.Data["pin_id"].(string)
	if !ok {
		return nil
	}

	// Soft delete in PostgreSQL
	query := "UPDATE study_pins SET is_active = FALSE, updated_at = NOW() WHERE id = $1"
	_, err := h.db.Exec(query, pinID)
	if err != nil {
		h.logger.WithError(err).Error("Failed to delete study pin")
		return err
	}

	// Remove from materialized view
	quadkey := event.Quadkey
	if err := h.mvStore.RemovePin(ctx, pinID, quadkey); err != nil {
		h.logger.WithError(err).Warn("Failed to remove pin from materialized view")
	}

	h.logger.WithField("pin_id", pinID).Info("Study pin deleted")
	return nil
}

// PresenceHandler handles user presence events
type PresenceHandler struct {
	db      *postgres.Client
	mvStore *shard.MaterializedViewStore
	logger  *logrus.Logger
}

// NewPresenceHandler creates a new presence handler
func NewPresenceHandler(db *postgres.Client, mvStore *shard.MaterializedViewStore, logger *logrus.Logger) *PresenceHandler {
	return &PresenceHandler{
		db:      db,
		mvStore: mvStore,
		logger:  logger,
	}
}

// HandleEvent processes presence events
func (h *PresenceHandler) HandleEvent(ctx context.Context, event kafka.Event) error {
	switch event.Type {
	case kafka.EventTypes.PresenceUpdate:
		return h.handlePresenceUpdate(ctx, event)
	case kafka.EventTypes.PresenceJoin:
		return h.handlePresenceJoin(ctx, event)
	case kafka.EventTypes.PresenceLeave:
		return h.handlePresenceLeave(ctx, event)
	default:
		h.logger.WithField("event_type", event.Type).Debug("Ignoring unsupported event type")
		return nil
	}
}

// GetEventTypes returns the event types this handler supports
func (h *PresenceHandler) GetEventTypes() []string {
	return []string{
		kafka.EventTypes.PresenceUpdate,
		kafka.EventTypes.PresenceJoin,
		kafka.EventTypes.PresenceLeave,
	}
}

func (h *PresenceHandler) handlePresenceUpdate(ctx context.Context, event kafka.Event) error {
	presenceData, ok := event.Data["presence"].(map[string]interface{})
	if !ok {
		return nil
	}

	// Convert to PresenceInfo struct
	presenceJSON, _ := json.Marshal(presenceData)
	var presence shard.PresenceInfo
	if err := json.Unmarshal(presenceJSON, &presence); err != nil {
		h.logger.WithError(err).Error("Failed to unmarshal presence info")
		return err
	}

	// Upsert in PostgreSQL
	query := `
		INSERT INTO user_presence (
			user_id, session_id, quadkey, location, status, last_seen, user_agent
		) VALUES (
			$1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326), $6, $7, $8
		)
		ON CONFLICT (user_id, session_id) DO UPDATE SET
			quadkey = EXCLUDED.quadkey,
			location = EXCLUDED.location,
			status = EXCLUDED.status,
			last_seen = EXCLUDED.last_seen,
			updated_at = NOW()
	`

	_, err := h.db.Exec(query,
		presence.UserID,
		presence.SessionID,
		presence.Quadkey,
		presence.Longitude,
		presence.Latitude,
		presence.Status,
		presence.LastSeen,
		presence.UserAgent,
	)

	if err != nil {
		h.logger.WithError(err).Error("Failed to upsert user presence")
		return err
	}

	// Update materialized view
	if err := h.mvStore.UpdatePresence(ctx, &presence); err != nil {
		h.logger.WithError(err).Warn("Failed to update presence in materialized view")
	}

	h.logger.WithFields(logrus.Fields{
		"user_id": presence.UserID,
		"quadkey": presence.Quadkey,
		"status":  presence.Status,
	}).Debug("Presence updated")

	return nil
}

func (h *PresenceHandler) handlePresenceJoin(ctx context.Context, event kafka.Event) error {
	// Similar to presence update but specifically for join events
	return h.handlePresenceUpdate(ctx, event)
}

func (h *PresenceHandler) handlePresenceLeave(ctx context.Context, event kafka.Event) error {
	userID, ok := event.Data["user_id"].(string)
	if !ok {
		return nil
	}

	sessionID, ok := event.Data["session_id"].(string)
	if !ok {
		return nil
	}

	// Remove from PostgreSQL
	query := "DELETE FROM user_presence WHERE user_id = $1 AND session_id = $2"
	_, err := h.db.Exec(query, userID, sessionID)
	if err != nil {
		h.logger.WithError(err).Error("Failed to remove user presence")
		return err
	}

	// Remove from materialized view
	quadkey := event.Quadkey
	if err := h.mvStore.RemovePresence(ctx, userID, quadkey); err != nil {
		h.logger.WithError(err).Warn("Failed to remove presence from materialized view")
	}

	h.logger.WithFields(logrus.Fields{
		"user_id":    userID,
		"session_id": sessionID,
		"quadkey":    quadkey,
	}).Info("User presence removed")

	return nil
}

// Helper functions

func nullTimePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
