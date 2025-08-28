package crdt

import (
	"encoding/json"
	"sync"
)

// LWWElement represents an element in a Last-Write-Wins set
type LWWElement struct {
	Value     interface{} `json:"value"`
	Timestamp Timestamp   `json:"timestamp"`
	Deleted   bool        `json:"deleted"`
}

// LWWSet implements a Last-Write-Wins CRDT set
type LWWSet struct {
	mu       sync.RWMutex
	elements map[string]LWWElement
	clock    *HLC
}

// NewLWWSet creates a new LWW set
func NewLWWSet(nodeID string) *LWWSet {
	return &LWWSet{
		elements: make(map[string]LWWElement),
		clock:    NewHLC(nodeID),
	}
}

// Add adds an element to the set
func (s *LWWSet) Add(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timestamp := s.clock.Now()
	
	// Only add if this timestamp is newer than existing
	if existing, exists := s.elements[key]; !exists || timestamp.After(existing.Timestamp) {
		s.elements[key] = LWWElement{
			Value:     value,
			Timestamp: timestamp,
			Deleted:   false,
		}
	}
}

// Remove removes an element from the set
func (s *LWWSet) Remove(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timestamp := s.clock.Now()
	
	// Only remove if this timestamp is newer than existing
	if existing, exists := s.elements[key]; !exists || timestamp.After(existing.Timestamp) {
		s.elements[key] = LWWElement{
			Value:     existing.Value,
			Timestamp: timestamp,
			Deleted:   true,
		}
	}
}

// Contains checks if an element is in the set
func (s *LWWSet) Contains(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	element, exists := s.elements[key]
	return exists && !element.Deleted
}

// Get retrieves an element from the set
func (s *LWWSet) Get(key string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	element, exists := s.elements[key]
	if !exists || element.Deleted {
		return nil, false
	}
	return element.Value, true
}

// Elements returns all active elements in the set
func (s *LWWSet) Elements() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]interface{})
	for key, element := range s.elements {
		if !element.Deleted {
			result[key] = element.Value
		}
	}
	return result
}

// Size returns the number of active elements
func (s *LWWSet) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, element := range s.elements {
		if !element.Deleted {
			count++
		}
	}
	return count
}

// Merge merges another LWW set into this one
func (s *LWWSet) Merge(other *LWWSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	other.mu.RLock()
	defer other.mu.RUnlock()

	for key, otherElement := range other.elements {
		if existing, exists := s.elements[key]; !exists || otherElement.Timestamp.After(existing.Timestamp) {
			s.elements[key] = otherElement
			// Update our clock with the received timestamp
			s.clock.Update(otherElement.Timestamp)
		}
	}
}

// ToJSON serializes the set to JSON
func (s *LWWSet) ToJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return json.Marshal(s.elements)
}

// FromJSON deserializes the set from JSON
func (s *LWWSet) FromJSON(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	elements := make(map[string]LWWElement)
	if err := json.Unmarshal(data, &elements); err != nil {
		return err
	}

	// Merge the deserialized elements
	for key, element := range elements {
		if existing, exists := s.elements[key]; !exists || element.Timestamp.After(existing.Timestamp) {
			s.elements[key] = element
			s.clock.Update(element.Timestamp)
		}
	}

	return nil
}

// Delta returns the changes since a given timestamp
func (s *LWWSet) Delta(since Timestamp) map[string]LWWElement {
	s.mu.RLock()
	defer s.mu.RUnlock()

	delta := make(map[string]LWWElement)
	for key, element := range s.elements {
		if element.Timestamp.After(since) {
			delta[key] = element
		}
	}
	return delta
}

// ApplyDelta applies a delta to the set
func (s *LWWSet) ApplyDelta(delta map[string]LWWElement) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, element := range delta {
		if existing, exists := s.elements[key]; !exists || element.Timestamp.After(existing.Timestamp) {
			s.elements[key] = element
			s.clock.Update(element.Timestamp)
		}
	}
}

// Clear removes all elements
func (s *LWWSet) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	timestamp := s.clock.Now()
	for key, element := range s.elements {
		if !element.Deleted {
			s.elements[key] = LWWElement{
				Value:     element.Value,
				Timestamp: timestamp,
				Deleted:   true,
			}
		}
	}
}

// Cleanup removes old deleted entries to save memory
func (s *LWWSet) Cleanup(olderThan Timestamp) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, element := range s.elements {
		if element.Deleted && element.Timestamp.Before(olderThan) {
			delete(s.elements, key)
		}
	}
}
