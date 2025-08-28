package quadkey

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Quadkey represents a quadtree key for geospatial indexing
type Quadkey struct {
	Key   string
	Level int
	Lat   float64
	Lon   float64
}

// FromLatLon creates a quadkey from latitude, longitude, and zoom level
func FromLatLon(lat, lon float64, level int) *Quadkey {
	if level < 1 || level > 23 {
		level = 18 // Default level for city-scale precision
	}

	key := latLonToQuadkey(lat, lon, level)
	return &Quadkey{
		Key:   key,
		Level: level,
		Lat:   lat,
		Lon:   lon,
	}
}

// FromString creates a quadkey from a string representation
func FromString(key string) (*Quadkey, error) {
	if !isValidQuadkey(key) {
		return nil, fmt.Errorf("invalid quadkey: %s", key)
	}

	lat, lon := quadkeyToLatLon(key)
	return &Quadkey{
		Key:   key,
		Level: len(key),
		Lat:   lat,
		Lon:   lon,
	}, nil
}

// Parent returns the parent quadkey (one level up)
func (q *Quadkey) Parent() *Quadkey {
	if q.Level <= 1 {
		return nil
	}

	parentKey := q.Key[:len(q.Key)-1]
	lat, lon := quadkeyToLatLon(parentKey)
	
	return &Quadkey{
		Key:   parentKey,
		Level: q.Level - 1,
		Lat:   lat,
		Lon:   lon,
	}
}

// Children returns the four child quadkeys
func (q *Quadkey) Children() []*Quadkey {
	if q.Level >= 23 {
		return nil
	}

	children := make([]*Quadkey, 4)
	for i := 0; i < 4; i++ {
		childKey := q.Key + strconv.Itoa(i)
		lat, lon := quadkeyToLatLon(childKey)
		children[i] = &Quadkey{
			Key:   childKey,
			Level: q.Level + 1,
			Lat:   lat,
			Lon:   lon,
		}
	}
	return children
}

// Neighbors returns the 8 neighboring quadkeys at the same level
func (q *Quadkey) Neighbors() []*Quadkey {
	tileX, tileY := q.toTileXY()
	neighbors := make([]*Quadkey, 0, 8)

	// Check all 8 directions
	for dx := -1; dx <= 1; dx++ {
		for dy := -1; dy <= 1; dy++ {
			if dx == 0 && dy == 0 {
				continue // Skip self
			}

			neighborX := tileX + dx
			neighborY := tileY + dy

			// Check bounds
			maxTile := int(math.Pow(2, float64(q.Level))) - 1
			if neighborX >= 0 && neighborX <= maxTile && neighborY >= 0 && neighborY <= maxTile {
				neighborKey := tileXYToQuadkey(neighborX, neighborY, q.Level)
				lat, lon := quadkeyToLatLon(neighborKey)
				neighbors = append(neighbors, &Quadkey{
					Key:   neighborKey,
					Level: q.Level,
					Lat:   lat,
					Lon:   lon,
				})
			}
		}
	}

	return neighbors
}

// GetNearbyQuadkeys returns quadkeys within a certain radius
func GetNearbyQuadkeys(lat, lon float64, radiusMeters float64, level int) []*Quadkey {
	center := FromLatLon(lat, lon, level)
	nearby := make(map[string]*Quadkey)
	nearby[center.Key] = center

	// Calculate the number of tiles to check based on radius
	// Rough approximation: tile size at equator for level 18 is ~150m
	tileSize := 40075000.0 / math.Pow(2, float64(level)) // meters
	tilesRadius := int(math.Ceil(radiusMeters / tileSize))

	centerX, centerY := center.toTileXY()
	maxTile := int(math.Pow(2, float64(level))) - 1

	for dx := -tilesRadius; dx <= tilesRadius; dx++ {
		for dy := -tilesRadius; dy <= tilesRadius; dy++ {
			x := centerX + dx
			y := centerY + dy

			if x >= 0 && x <= maxTile && y >= 0 && y <= maxTile {
				key := tileXYToQuadkey(x, y, level)
				if _, exists := nearby[key]; !exists {
					qLat, qLon := quadkeyToLatLon(key)
					if haversineDistance(lat, lon, qLat, qLon) <= radiusMeters {
						nearby[key] = &Quadkey{
							Key:   key,
							Level: level,
							Lat:   qLat,
							Lon:   qLon,
						}
					}
				}
			}
		}
	}

	result := make([]*Quadkey, 0, len(nearby))
	for _, q := range nearby {
		result = append(result, q)
	}
	return result
}

// String returns the string representation
func (q *Quadkey) String() string {
	return q.Key
}

// BoundingBox returns the bounding box of the quadkey
func (q *Quadkey) BoundingBox() (north, south, east, west float64) {
	tileX, tileY := q.toTileXY()
	
	north, west = tileXYToLatLon(tileX, tileY, q.Level)
	south, east = tileXYToLatLon(tileX+1, tileY+1, q.Level)
	
	return north, south, east, west
}

// Internal helper functions

func latLonToQuadkey(lat, lon float64, level int) string {
	tileX, tileY := latLonToTileXY(lat, lon, level)
	return tileXYToQuadkey(tileX, tileY, level)
}

func quadkeyToLatLon(quadkey string) (lat, lon float64) {
	tileX, tileY := quadkeyToTileXY(quadkey)
	level := len(quadkey)
	return tileXYToLatLon(tileX, tileY, level)
}

func latLonToTileXY(lat, lon float64, level int) (tileX, tileY int) {
	latRad := lat * math.Pi / 180.0
	n := math.Pow(2.0, float64(level))
	
	tileX = int((lon + 180.0) / 360.0 * n)
	tileY = int((1.0 - math.Log(math.Tan(latRad)+(1.0/math.Cos(latRad)))/math.Pi) / 2.0 * n)
	
	return tileX, tileY
}

func tileXYToLatLon(tileX, tileY, level int) (lat, lon float64) {
	n := math.Pow(2.0, float64(level))
	
	lon = float64(tileX)/n*360.0 - 180.0
	latRad := math.Atan(math.Sinh(math.Pi * (1.0 - 2.0*float64(tileY)/n)))
	lat = latRad * 180.0 / math.Pi
	
	return lat, lon
}

func tileXYToQuadkey(tileX, tileY, level int) string {
	var quadkey strings.Builder
	
	for i := level; i > 0; i-- {
		digit := 0
		mask := 1 << (i - 1)
		
		if (tileX & mask) != 0 {
			digit++
		}
		if (tileY & mask) != 0 {
			digit += 2
		}
		
		quadkey.WriteString(strconv.Itoa(digit))
	}
	
	return quadkey.String()
}

func quadkeyToTileXY(quadkey string) (tileX, tileY int) {
	tileX, tileY = 0, 0
	
	for i, digit := range quadkey {
		mask := 1 << (len(quadkey) - i - 1)
		
		switch digit {
		case '1':
			tileX |= mask
		case '2':
			tileY |= mask
		case '3':
			tileX |= mask
			tileY |= mask
		}
	}
	
	return tileX, tileY
}

func (q *Quadkey) toTileXY() (tileX, tileY int) {
	return quadkeyToTileXY(q.Key)
}

func isValidQuadkey(key string) bool {
	if len(key) == 0 || len(key) > 23 {
		return false
	}
	
	for _, char := range key {
		if char < '0' || char > '3' {
			return false
		}
	}
	
	return true
}

// haversineDistance calculates the distance between two points in meters
func haversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadius = 6371000 // Earth radius in meters
	
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0
	
	lat1Rad := lat1 * math.Pi / 180.0
	lat2Rad := lat2 * math.Pi / 180.0
	
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
		math.Sin(dLon/2)*math.Sin(dLon/2)
		
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	
	return earthRadius * c
}
