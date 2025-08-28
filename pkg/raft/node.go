package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/sirupsen/logrus"
)

// ShardAssignment represents a shard assignment in the cluster
type ShardAssignment struct {
	QuadkeyPrefix string    `json:"quadkey_prefix"`
	ShardID       string    `json:"shard_id"`
	NodeID        string    `json:"node_id"`
	Status        string    `json:"status"`
	AssignedAt    time.Time `json:"assigned_at"`
	LoadScore     float64   `json:"load_score"`
}

// ClusterState represents the current state of the cluster
type ClusterState struct {
	Assignments map[string]*ShardAssignment `json:"assignments"`
	Nodes       map[string]*NodeInfo        `json:"nodes"`
	Version     uint64                      `json:"version"`
	UpdatedAt   time.Time                   `json:"updated_at"`
}

// NodeInfo represents information about a cluster node
type NodeInfo struct {
	ID            string                 `json:"id"`
	Address       string                 `json:"address"`
	Status        string                 `json:"status"`
	LastHeartbeat time.Time              `json:"last_heartbeat"`
	Metadata      map[string]interface{} `json:"metadata"`
}

// Command represents a Raft command
type Command struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// FSM implements the Raft finite state machine
type FSM struct {
	mu    sync.RWMutex
	state *ClusterState
	logger *logrus.Logger
}

// Node represents a Raft node in the control plane
type Node struct {
	raft       *raft.Raft
	fsm        *FSM
	config     *Config
	logger     *logrus.Logger
	shutdownCh chan struct{}
}

// Config holds Raft node configuration
type Config struct {
	NodeID      string
	BindAddr    string
	DataDir     string
	Bootstrap   bool
	JoinAddr    string
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	MaxAppendEntries int
}

// NewNode creates a new Raft node
func NewNode(config *Config, logger *logrus.Logger) (*Node, error) {
	// Set defaults
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = 1 * time.Second
	}
	if config.ElectionTimeout == 0 {
		config.ElectionTimeout = 3 * time.Second
	}
	if config.CommitTimeout == 0 {
		config.CommitTimeout = 500 * time.Millisecond
	}
	if config.MaxAppendEntries == 0 {
		config.MaxAppendEntries = 64
	}

	// Create data directory
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize FSM
	fsm := &FSM{
		state: &ClusterState{
			Assignments: make(map[string]*ShardAssignment),
			Nodes:       make(map[string]*NodeInfo),
			Version:     0,
			UpdatedAt:   time.Now(),
		},
		logger: logger,
	}

	node := &Node{
		fsm:        fsm,
		config:     config,
		logger:     logger,
		shutdownCh: make(chan struct{}),
	}

	if err := node.setupRaft(); err != nil {
		return nil, fmt.Errorf("failed to setup Raft: %w", err)
	}

	return node, nil
}

// setupRaft initializes the Raft consensus system
func (n *Node) setupRaft() error {
	// Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(n.config.NodeID)
	raftConfig.HeartbeatTimeout = n.config.HeartbeatTimeout
	raftConfig.ElectionTimeout = n.config.ElectionTimeout
	raftConfig.CommitTimeout = n.config.CommitTimeout
	raftConfig.MaxAppendEntries = n.config.MaxAppendEntries
	raftConfig.Logger = &raftLogger{logger: n.logger}

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", n.config.BindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(n.config.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}

	// Log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(n.config.DataDir, "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create log store: %w", err)
	}

	// Stable store
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(n.config.DataDir, "raft-stable.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create stable store: %w", err)
	}

	// Snapshot store
	snapshots, err := raft.NewFileSnapshotStore(n.config.DataDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create Raft system
	r, err := raft.NewRaft(raftConfig, n.fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create Raft: %w", err)
	}

	n.raft = r

	// Bootstrap cluster if configured
	if n.config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(n.config.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		n.raft.BootstrapCluster(configuration)
		n.logger.Info("Bootstrapped Raft cluster")
	}

	return nil
}

// Start starts the Raft node
func (n *Node) Start() error {
	n.logger.WithField("node_id", n.config.NodeID).Info("Starting Raft node")

	// Join cluster if configured
	if n.config.JoinAddr != "" && !n.config.Bootstrap {
		if err := n.joinCluster(); err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
	}

	return nil
}

// Stop stops the Raft node
func (n *Node) Stop() error {
	n.logger.Info("Stopping Raft node")
	close(n.shutdownCh)
	
	if n.raft != nil {
		return n.raft.Shutdown().Error()
	}
	return nil
}

// IsLeader returns true if this node is the current leader
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// GetLeader returns the current leader address
func (n *Node) GetLeader() string {
	_, leaderID := n.raft.LeaderWithID()
	return string(leaderID)
}

// GetState returns the current cluster state
func (n *Node) GetState() *ClusterState {
	n.fsm.mu.RLock()
	defer n.fsm.mu.RUnlock()
	
	// Deep copy the state
	data, _ := json.Marshal(n.fsm.state)
	var state ClusterState
	json.Unmarshal(data, &state)
	return &state
}

// AssignShard assigns a shard to a node
func (n *Node) AssignShard(quadkeyPrefix, shardID, nodeID string, loadScore float64) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader, cannot assign shard")
	}

	assignment := &ShardAssignment{
		QuadkeyPrefix: quadkeyPrefix,
		ShardID:       shardID,
		NodeID:        nodeID,
		Status:        "active",
		AssignedAt:    time.Now(),
		LoadScore:     loadScore,
	}

	cmd := Command{
		Type:      "assign_shard",
		Data:      assignment,
		Timestamp: time.Now(),
	}

	return n.applyCommand(cmd)
}

// UnassignShard removes a shard assignment
func (n *Node) UnassignShard(quadkeyPrefix string) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader, cannot unassign shard")
	}

	cmd := Command{
		Type:      "unassign_shard",
		Data:      map[string]string{"quadkey_prefix": quadkeyPrefix},
		Timestamp: time.Now(),
	}

	return n.applyCommand(cmd)
}

// RegisterNode registers a node in the cluster
func (n *Node) RegisterNode(nodeID, address string, metadata map[string]interface{}) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader, cannot register node")
	}

	nodeInfo := &NodeInfo{
		ID:            nodeID,
		Address:       address,
		Status:        "active",
		LastHeartbeat: time.Now(),
		Metadata:      metadata,
	}

	cmd := Command{
		Type:      "register_node",
		Data:      nodeInfo,
		Timestamp: time.Now(),
	}

	return n.applyCommand(cmd)
}

// UpdateNodeHeartbeat updates a node's heartbeat
func (n *Node) UpdateNodeHeartbeat(nodeID string) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader, cannot update heartbeat")
	}

	cmd := Command{
		Type:      "update_heartbeat",
		Data:      map[string]interface{}{"node_id": nodeID, "timestamp": time.Now()},
		Timestamp: time.Now(),
	}

	return n.applyCommand(cmd)
}

// applyCommand applies a command to the Raft log
func (n *Node) applyCommand(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := n.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	return nil
}

// joinCluster joins an existing cluster
func (n *Node) joinCluster() error {
	n.logger.WithField("join_addr", n.config.JoinAddr).Info("Joining cluster")
	
	// In a real implementation, you would make an HTTP request to the join address
	// to add this node to the cluster. For simplicity, we'll skip this step.
	// The join process would typically involve:
	// 1. Contacting the leader
	// 2. Requesting to be added to the cluster
	// 3. The leader calling raft.AddVoter() or raft.AddNonvoter()
	
	return nil
}

// FSM implementation

// Apply applies a log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.WithError(err).Error("Failed to unmarshal command")
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case "assign_shard":
		return f.applyAssignShard(cmd.Data)
	case "unassign_shard":
		return f.applyUnassignShard(cmd.Data)
	case "register_node":
		return f.applyRegisterNode(cmd.Data)
	case "update_heartbeat":
		return f.applyUpdateHeartbeat(cmd.Data)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// Snapshot creates a snapshot of the FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Clone the state
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}

	return &snapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	data, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}

	var state ClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	
	f.state = &state
	return nil
}

// FSM command handlers

func (f *FSM) applyAssignShard(data interface{}) interface{} {
	assignment, ok := data.(*ShardAssignment)
	if !ok {
		// Try to unmarshal from map
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid assign_shard data")
		}
		
		assignmentData, _ := json.Marshal(dataMap)
		assignment = &ShardAssignment{}
		if err := json.Unmarshal(assignmentData, assignment); err != nil {
			return err
		}
	}

	f.state.Assignments[assignment.QuadkeyPrefix] = assignment
	f.state.Version++
	f.state.UpdatedAt = time.Now()
	
	f.logger.WithFields(logrus.Fields{
		"quadkey_prefix": assignment.QuadkeyPrefix,
		"shard_id":       assignment.ShardID,
		"node_id":        assignment.NodeID,
	}).Info("Assigned shard")
	
	return nil
}

func (f *FSM) applyUnassignShard(data interface{}) interface{} {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid unassign_shard data")
	}

	quadkeyPrefix, ok := dataMap["quadkey_prefix"].(string)
	if !ok {
		return fmt.Errorf("invalid quadkey_prefix")
	}

	delete(f.state.Assignments, quadkeyPrefix)
	f.state.Version++
	f.state.UpdatedAt = time.Now()
	
	f.logger.WithField("quadkey_prefix", quadkeyPrefix).Info("Unassigned shard")
	return nil
}

func (f *FSM) applyRegisterNode(data interface{}) interface{} {
	nodeInfo, ok := data.(*NodeInfo)
	if !ok {
		// Try to unmarshal from map
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid register_node data")
		}
		
		nodeData, _ := json.Marshal(dataMap)
		nodeInfo = &NodeInfo{}
		if err := json.Unmarshal(nodeData, nodeInfo); err != nil {
			return err
		}
	}

	f.state.Nodes[nodeInfo.ID] = nodeInfo
	f.state.Version++
	f.state.UpdatedAt = time.Now()
	
	f.logger.WithFields(logrus.Fields{
		"node_id": nodeInfo.ID,
		"address": nodeInfo.Address,
	}).Info("Registered node")
	
	return nil
}

func (f *FSM) applyUpdateHeartbeat(data interface{}) interface{} {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid update_heartbeat data")
	}

	nodeID, ok := dataMap["node_id"].(string)
	if !ok {
		return fmt.Errorf("invalid node_id")
	}

	timestampStr, ok := dataMap["timestamp"].(string)
	if !ok {
		return fmt.Errorf("invalid timestamp")
	}

	timestamp, err := time.Parse(time.RFC3339Nano, timestampStr)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}

	if node, exists := f.state.Nodes[nodeID]; exists {
		node.LastHeartbeat = timestamp
		f.state.Version++
		f.state.UpdatedAt = time.Now()
	}

	return nil
}

// snapshot implements raft.FSMSnapshot
type snapshot struct {
	data []byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// raftLogger implements hclog.Logger for Raft
type raftLogger struct {
	logger *logrus.Logger
}

func (r *raftLogger) Log(level logrus.Level, msg string, args ...interface{}) {
	r.logger.Log(level, msg, args...)
}

func (r *raftLogger) Debug(msg string, args ...interface{}) {
	r.logger.Debug(msg, args...)
}

func (r *raftLogger) Info(msg string, args ...interface{}) {
	r.logger.Info(msg, args...)
}

func (r *raftLogger) Warn(msg string, args ...interface{}) {
	r.logger.Warn(msg, args...)
}

func (r *raftLogger) Error(msg string, args ...interface{}) {
	r.logger.Error(msg, args...)
}

func (r *raftLogger) IsTrace() bool { return false }
func (r *raftLogger) IsDebug() bool { return true }
func (r *raftLogger) IsInfo() bool  { return true }
func (r *raftLogger) IsWarn() bool  { return true }
func (r *raftLogger) IsError() bool { return true }

func (r *raftLogger) With(args ...interface{}) interface{} { return r }
func (r *raftLogger) Name() string                        { return "raft" }
func (r *raftLogger) Named(name string) interface{}       { return r }
func (r *raftLogger) ResetNamed(name string) interface{}  { return r }
