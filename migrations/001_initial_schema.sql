-- Initial schema for geo-sharding platform
-- Enable PostGIS extension for geospatial operations
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users table
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    avatar_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Study pins table with geospatial indexing
CREATE TABLE study_pins (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    quadkey VARCHAR(23) NOT NULL, -- Max quadkey length at level 23
    location GEOMETRY(POINT, 4326) NOT NULL, -- WGS84 coordinate system
    subject VARCHAR(100) NOT NULL,
    description TEXT,
    tags TEXT[], -- Array of tags
    capacity INTEGER DEFAULT 1,
    current_attendees INTEGER DEFAULT 0,
    visibility VARCHAR(20) DEFAULT 'public' CHECK (visibility IN ('public', 'friends', 'private')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB -- Additional flexible data
);

-- Presence tracking table
CREATE TABLE user_presence (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id VARCHAR(255) NOT NULL,
    quadkey VARCHAR(23) NOT NULL,
    location GEOMETRY(POINT, 4326) NOT NULL,
    status VARCHAR(20) DEFAULT 'online' CHECK (status IN ('online', 'studying', 'available', 'busy', 'away')),
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    user_agent TEXT,
    ip_address INET,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Event log for audit and replay
CREATE TABLE event_log (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    source VARCHAR(100) NOT NULL,
    quadkey VARCHAR(23) NOT NULL,
    aggregate_id UUID,
    aggregate_type VARCHAR(50),
    event_data JSONB NOT NULL,
    idempotency_key VARCHAR(255) UNIQUE,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER DEFAULT 0
);

-- Shard ownership and routing
CREATE TABLE shard_assignments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quadkey_prefix VARCHAR(23) NOT NULL,
    shard_id VARCHAR(100) NOT NULL,
    node_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'migrating', 'inactive')),
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    load_score FLOAT DEFAULT 0.0,
    last_heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- CRDT state storage for conflict resolution
CREATE TABLE crdt_states (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_type VARCHAR(50) NOT NULL, -- 'pin', 'presence', 'counter'
    entity_id VARCHAR(255) NOT NULL,
    quadkey VARCHAR(23) NOT NULL,
    crdt_type VARCHAR(50) NOT NULL, -- 'lww_set', 'pn_counter'
    state_data JSONB NOT NULL,
    vector_clock JSONB,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    node_id VARCHAR(100) NOT NULL,
    UNIQUE(entity_type, entity_id, node_id)
);

-- Friendship/social connections
CREATE TABLE friendships (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    requester_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    addressee_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'blocked')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(requester_id, addressee_id)
);

-- Study sessions and collaboration
CREATE TABLE study_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pin_id UUID NOT NULL REFERENCES study_pins(id) ON DELETE CASCADE,
    host_user_id UUID NOT NULL REFERENCES users(id),
    session_name VARCHAR(255),
    description TEXT,
    max_participants INTEGER DEFAULT 5,
    current_participants INTEGER DEFAULT 1,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'paused', 'ended')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ended_at TIMESTAMP WITH TIME ZONE
);

-- Session participants
CREATE TABLE session_participants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES study_sessions(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    left_at TIMESTAMP WITH TIME ZONE,
    role VARCHAR(20) DEFAULT 'participant' CHECK (role IN ('host', 'moderator', 'participant')),
    UNIQUE(session_id, user_id)
);

-- Create indexes for performance

-- Geospatial indexes
CREATE INDEX idx_study_pins_location ON study_pins USING GIST (location);
CREATE INDEX idx_user_presence_location ON user_presence USING GIST (location);

-- Quadkey indexes for sharding
CREATE INDEX idx_study_pins_quadkey ON study_pins (quadkey);
CREATE INDEX idx_user_presence_quadkey ON user_presence (quadkey);
CREATE INDEX idx_event_log_quadkey ON event_log (quadkey);
CREATE INDEX idx_shard_assignments_quadkey_prefix ON shard_assignments (quadkey_prefix);
CREATE INDEX idx_crdt_states_quadkey ON crdt_states (quadkey);

-- User and time-based indexes
CREATE INDEX idx_study_pins_user_id ON study_pins (user_id);
CREATE INDEX idx_study_pins_created_at ON study_pins (created_at);
CREATE INDEX idx_study_pins_expires_at ON study_pins (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_user_presence_user_id ON user_presence (user_id);
CREATE INDEX idx_user_presence_session_id ON user_presence (session_id);
CREATE INDEX idx_user_presence_last_seen ON user_presence (last_seen);

-- Event log indexes
CREATE INDEX idx_event_log_event_type ON event_log (event_type);
CREATE INDEX idx_event_log_timestamp ON event_log (timestamp);
CREATE INDEX idx_event_log_processed_at ON event_log (processed_at);
CREATE INDEX idx_event_log_idempotency_key ON event_log (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- CRDT indexes
CREATE INDEX idx_crdt_states_entity ON crdt_states (entity_type, entity_id);
CREATE INDEX idx_crdt_states_last_updated ON crdt_states (last_updated);

-- Social indexes
CREATE INDEX idx_friendships_requester ON friendships (requester_id);
CREATE INDEX idx_friendships_addressee ON friendships (addressee_id);
CREATE INDEX idx_friendships_status ON friendships (status);

-- Session indexes
CREATE INDEX idx_study_sessions_pin_id ON study_sessions (pin_id);
CREATE INDEX idx_study_sessions_host ON study_sessions (host_user_id);
CREATE INDEX idx_study_sessions_status ON study_sessions (status);
CREATE INDEX idx_session_participants_session ON session_participants (session_id);
CREATE INDEX idx_session_participants_user ON session_participants (user_id);

-- Create partial indexes for active records
CREATE INDEX idx_study_pins_active ON study_pins (quadkey, created_at) WHERE is_active = TRUE;
CREATE INDEX idx_users_active ON users (username) WHERE is_active = TRUE;
CREATE INDEX idx_shard_assignments_active ON shard_assignments (quadkey_prefix) WHERE status = 'active';

-- Functions for triggers
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add updated_at triggers
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_study_pins_updated_at BEFORE UPDATE ON study_pins 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_presence_updated_at BEFORE UPDATE ON user_presence 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_friendships_updated_at BEFORE UPDATE ON friendships 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_study_sessions_updated_at BEFORE UPDATE ON study_sessions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to automatically set quadkey from location
CREATE OR REPLACE FUNCTION set_quadkey_from_location()
RETURNS TRIGGER AS $$
DECLARE
    lat FLOAT;
    lon FLOAT;
BEGIN
    -- Extract lat/lon from geometry
    lat := ST_Y(NEW.location);
    lon := ST_X(NEW.location);
    
    -- Calculate quadkey (simplified - would use actual quadkey calculation)
    -- This is a placeholder - the actual implementation would call the Go quadkey function
    NEW.quadkey := SUBSTRING(MD5(lat::text || lon::text), 1, 18);
    
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add quadkey triggers
CREATE TRIGGER set_study_pins_quadkey BEFORE INSERT OR UPDATE ON study_pins 
    FOR EACH ROW EXECUTE FUNCTION set_quadkey_from_location();

CREATE TRIGGER set_user_presence_quadkey BEFORE INSERT OR UPDATE ON user_presence 
    FOR EACH ROW EXECUTE FUNCTION set_quadkey_from_location();

-- Views for common queries
CREATE VIEW active_study_pins AS
SELECT 
    p.*,
    u.username,
    u.full_name,
    ST_X(p.location) as longitude,
    ST_Y(p.location) as latitude
FROM study_pins p
JOIN users u ON p.user_id = u.id
WHERE p.is_active = TRUE 
  AND (p.expires_at IS NULL OR p.expires_at > NOW());

CREATE VIEW current_user_presence AS
SELECT 
    pr.*,
    u.username,
    u.full_name,
    ST_X(pr.location) as longitude,
    ST_Y(pr.location) as latitude
FROM user_presence pr
JOIN users u ON pr.user_id = u.id
WHERE pr.last_seen > NOW() - INTERVAL '5 minutes';

-- Insert some sample data for testing
INSERT INTO users (username, email, password_hash, full_name) VALUES
('alice', 'alice@example.com', '$2a$10$hash1', 'Alice Johnson'),
('bob', 'bob@example.com', '$2a$10$hash2', 'Bob Smith'),
('charlie', 'charlie@example.com', '$2a$10$hash3', 'Charlie Brown');

-- Grant permissions (adjust as needed for your deployment)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
