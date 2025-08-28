package crdt

import (
	"encoding/json"
	"sync"
)

// PNCounter implements a PN-Counter (Increment/Decrement Counter) CRDT
type PNCounter struct {
	mu          sync.RWMutex
	increments  map[string]int64 // Increment counters per node
	decrements  map[string]int64 // Decrement counters per node
	clock       *HLC
	nodeID      string
}

// CounterState represents the serializable state of a PN-Counter
type CounterState struct {
	Increments map[string]int64 `json:"increments"`
	Decrements map[string]int64 `json:"decrements"`
}

// NewPNCounter creates a new PN-Counter
func NewPNCounter(nodeID string) *PNCounter {
	return &PNCounter{
		increments: make(map[string]int64),
		decrements: make(map[string]int64),
		clock:      NewHLC(nodeID),
		nodeID:     nodeID,
	}
}

// Increment increases the counter by the given value
func (c *PNCounter) Increment(value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.increments[c.nodeID] += value
}

// Decrement decreases the counter by the given value
func (c *PNCounter) Decrement(value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.decrements[c.nodeID] += value
}

// Value returns the current value of the counter
func (c *PNCounter) Value() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var total int64 = 0
	
	// Sum all increments
	for _, inc := range c.increments {
		total += inc
	}
	
	// Subtract all decrements
	for _, dec := range c.decrements {
		total -= dec
	}

	return total
}

// Merge merges another PN-Counter into this one
func (c *PNCounter) Merge(other *PNCounter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	other.mu.RLock()
	defer other.mu.RUnlock()

	// Merge increments - take the maximum for each node
	for nodeID, otherInc := range other.increments {
		if currentInc, exists := c.increments[nodeID]; !exists || otherInc > currentInc {
			c.increments[nodeID] = otherInc
		}
	}

	// Merge decrements - take the maximum for each node
	for nodeID, otherDec := range other.decrements {
		if currentDec, exists := c.decrements[nodeID]; !exists || otherDec > currentDec {
			c.decrements[nodeID] = otherDec
		}
	}
}

// ToJSON serializes the counter to JSON
func (c *PNCounter) ToJSON() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	state := CounterState{
		Increments: make(map[string]int64),
		Decrements: make(map[string]int64),
	}

	for nodeID, inc := range c.increments {
		state.Increments[nodeID] = inc
	}

	for nodeID, dec := range c.decrements {
		state.Decrements[nodeID] = dec
	}

	return json.Marshal(state)
}

// FromJSON deserializes the counter from JSON
func (c *PNCounter) FromJSON(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var state CounterState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	// Merge the deserialized state
	for nodeID, inc := range state.Increments {
		if currentInc, exists := c.increments[nodeID]; !exists || inc > currentInc {
			c.increments[nodeID] = inc
		}
	}

	for nodeID, dec := range state.Decrements {
		if currentDec, exists := c.decrements[nodeID]; !exists || dec > currentDec {
			c.decrements[nodeID] = dec
		}
	}

	return nil
}

// GetState returns the current state of the counter
func (c *PNCounter) GetState() CounterState {
	c.mu.RLock()
	defer c.mu.RUnlock()

	state := CounterState{
		Increments: make(map[string]int64),
		Decrements: make(map[string]int64),
	}

	for nodeID, inc := range c.increments {
		state.Increments[nodeID] = inc
	}

	for nodeID, dec := range c.decrements {
		state.Decrements[nodeID] = dec
	}

	return state
}

// SetState sets the counter state (used for replication)
func (c *PNCounter) SetState(state CounterState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear existing state
	c.increments = make(map[string]int64)
	c.decrements = make(map[string]int64)

	// Set new state
	for nodeID, inc := range state.Increments {
		c.increments[nodeID] = inc
	}

	for nodeID, dec := range state.Decrements {
		c.decrements[nodeID] = dec
	}
}

// Delta returns the changes for this node since the last sync
func (c *PNCounter) Delta() CounterState {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CounterState{
		Increments: map[string]int64{c.nodeID: c.increments[c.nodeID]},
		Decrements: map[string]int64{c.nodeID: c.decrements[c.nodeID]},
	}
}

// ApplyDelta applies a delta to the counter
func (c *PNCounter) ApplyDelta(delta CounterState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for nodeID, inc := range delta.Increments {
		if currentInc, exists := c.increments[nodeID]; !exists || inc > currentInc {
			c.increments[nodeID] = inc
		}
	}

	for nodeID, dec := range delta.Decrements {
		if currentDec, exists := c.decrements[nodeID]; !exists || dec > currentDec {
			c.decrements[nodeID] = dec
		}
	}
}

// Reset resets the counter to zero (only affects this node's contributions)
func (c *PNCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.increments[c.nodeID] = 0
	c.decrements[c.nodeID] = 0
}

// GetNodeContribution returns this node's contribution to the counter
func (c *PNCounter) GetNodeContribution() (int64, int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	inc := c.increments[c.nodeID]
	dec := c.decrements[c.nodeID]
	return inc, dec
}
