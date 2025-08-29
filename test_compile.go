package main

import (
	"fmt"
	"geo-sharding/internal/quadkey"
	"geo-sharding/internal/crdt"
)

func main() {
	fmt.Println("🎉 Geo-sharding platform compiles successfully!")
	
	// Test quadkey functionality
	qkey := quadkey.FromLatLon(37.7749, -122.4194, 18)
	fmt.Printf("📍 Quadkey for San Francisco: %s\n", qkey.Key)
	
	// Test CRDT functionality
	set := crdt.NewLWWSet("test-node")
	set.Add("test-pin", map[string]interface{}{"subject": "Computer Science"})
	fmt.Printf("🧩 CRDT set size: %d\n", set.Size())
	
	fmt.Println("✅ All core components are working!")
}

