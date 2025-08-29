package crdt

import (
	"encoding/json"
	"sync"
	"time"
)

// HLC represents a Hybrid Logical Clock
type HLC struct {
	mu       sync.RWMutex
	logical  uint64
	physical uint64
	nodeID   string
}

// Timestamp represents a point in hybrid logical time
type Timestamp struct {
	Logical  uint64 `json:"logical"`
	Physical uint64 `json:"physical"`
	NodeID   string `json:"node_id"`
}

// NewHLC creates a new Hybrid Logical Clock
func NewHLC(nodeID string) *HLC {
	return &HLC{
		nodeID:   nodeID,
		physical: uint64(time.Now().UnixNano()),
		logical:  0,
	}
}

// Now returns the current timestamp
func (h *HLC) Now() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := uint64(time.Now().UnixNano())
	
	if now > h.physical {
		h.physical = now
		h.logical = 0
	} else {
		h.logical++
	}

	return Timestamp{
		Logical:  h.logical,
		Physical: h.physical,
		NodeID:   h.nodeID,
	}
}

// Update updates the clock with a received timestamp and returns the updated current time
func (h *HLC) Update(received Timestamp) Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := uint64(time.Now().UnixNano())
	
	maxPhysical := max3(h.physical, received.Physical, now)
	
	if maxPhysical == h.physical && maxPhysical == received.Physical {
		h.logical = max2(h.logical, received.Logical) + 1
	} else if maxPhysical == h.physical {
		h.logical++
	} else if maxPhysical == received.Physical {
		h.logical = received.Logical + 1
	} else {
		h.logical = 0
	}
	
	h.physical = maxPhysical

	return Timestamp{
		Logical:  h.logical,
		Physical: h.physical,
		NodeID:   h.nodeID,
	}
}

// Compare compares two timestamps
// Returns: -1 if t1 < t2, 0 if t1 == t2, 1 if t1 > t2
func (t1 Timestamp) Compare(t2 Timestamp) int {
	if t1.Physical < t2.Physical {
		return -1
	}
	if t1.Physical > t2.Physical {
		return 1
	}
	
	if t1.Logical < t2.Logical {
		return -1
	}
	if t1.Logical > t2.Logical {
		return 1
	}
	
	if t1.NodeID < t2.NodeID {
		return -1
	}
	if t1.NodeID > t2.NodeID {
		return 1
	}
	
	return 0
}

// Before checks if this timestamp is before another
func (t1 Timestamp) Before(t2 Timestamp) bool {
	return t1.Compare(t2) < 0
}

// After checks if this timestamp is after another
func (t1 Timestamp) After(t2 Timestamp) bool {
	return t1.Compare(t2) > 0
}

// Equal checks if this timestamp equals another
func (t1 Timestamp) Equal(t2 Timestamp) bool {
	return t1.Compare(t2) == 0
}

// String returns a string representation of the timestamp
func (t Timestamp) String() string {
	data, _ := json.Marshal(t)
	return string(data)
}

// ToBytes serializes the timestamp to bytes
func (t Timestamp) ToBytes() []byte {
	data, _ := json.Marshal(t)
	return data
}

// FromBytes deserializes a timestamp from bytes
func TimestampFromBytes(data []byte) (Timestamp, error) {
	var t Timestamp
	err := json.Unmarshal(data, &t)
	return t, err
}

func max3(a, b, c uint64) uint64 {
	if a >= b && a >= c {
		return a
	}
	if b >= a && b >= c {
		return b
	}
	return c
}

func max2(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
