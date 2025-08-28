package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

// Client wraps the Redis client with geo-sharding specific operations
type Client struct {
	rdb    *redis.Client
	logger *logrus.Logger
}

// Config holds Redis configuration
type Config struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewClient creates a new Redis client
func NewClient(config Config, logger *logrus.Logger) (*Client, error) {
	if config.PoolSize == 0 {
		config.PoolSize = 10
	}
	if config.MinIdleConns == 0 {
		config.MinIdleConns = 5
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 3 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 3 * time.Second
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Client{
		rdb:    rdb,
		logger: logger,
	}, nil
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.rdb.Close()
}

// GetRawClient returns the underlying Redis client
func (c *Client) GetRawClient() *redis.Client {
	return c.rdb
}

// Ping checks if Redis is reachable
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Set stores a value with optional expiration
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	var serialized []byte
	var err error

	switch v := value.(type) {
	case string:
		serialized = []byte(v)
	case []byte:
		serialized = v
	default:
		serialized, err = json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to serialize value: %w", err)
		}
	}

	return c.rdb.Set(ctx, key, serialized, expiration).Err()
}

// Get retrieves a value and deserializes it
func (c *Client) Get(ctx context.Context, key string, dest interface{}) error {
	result, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return err
	}

	switch v := dest.(type) {
	case *string:
		*v = result
		return nil
	case *[]byte:
		*v = []byte(result)
		return nil
	default:
		return json.Unmarshal([]byte(result), dest)
	}
}

// HSet stores multiple fields in a hash
func (c *Client) HSet(ctx context.Context, key string, values map[string]interface{}) error {
	// Serialize values
	serializedValues := make(map[string]interface{})
	for field, value := range values {
		switch v := value.(type) {
		case string, []byte:
			serializedValues[field] = v
		default:
			serialized, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("failed to serialize field %s: %w", field, err)
			}
			serializedValues[field] = serialized
		}
	}

	return c.rdb.HSet(ctx, key, serializedValues).Err()
}

// HGet retrieves a field from a hash
func (c *Client) HGet(ctx context.Context, key, field string, dest interface{}) error {
	result, err := c.rdb.HGet(ctx, key, field).Result()
	if err != nil {
		return err
	}

	switch v := dest.(type) {
	case *string:
		*v = result
		return nil
	case *[]byte:
		*v = []byte(result)
		return nil
	default:
		return json.Unmarshal([]byte(result), dest)
	}
}

// HGetAll retrieves all fields from a hash
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.rdb.HGetAll(ctx, key).Result()
}

// HDel deletes fields from a hash
func (c *Client) HDel(ctx context.Context, key string, fields ...string) error {
	return c.rdb.HDel(ctx, key, fields...).Err()
}

// SAdd adds members to a set
func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) error {
	return c.rdb.SAdd(ctx, key, members...).Err()
}

// SRem removes members from a set
func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) error {
	return c.rdb.SRem(ctx, key, members...).Err()
}

// SMembers returns all members of a set
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	return c.rdb.SMembers(ctx, key).Result()
}

// SIsMember checks if a value is a member of a set
func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	return c.rdb.SIsMember(ctx, key, member).Result()
}

// ZAdd adds members to a sorted set
func (c *Client) ZAdd(ctx context.Context, key string, members ...*redis.Z) error {
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

// ZRange returns a range of members from a sorted set
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.rdb.ZRange(ctx, key, start, stop).Result()
}

// ZRangeByScore returns members from a sorted set by score range
func (c *Client) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	return c.rdb.ZRangeByScore(ctx, key, opt).Result()
}

// ZRem removes members from a sorted set
func (c *Client) ZRem(ctx context.Context, key string, members ...interface{}) error {
	return c.rdb.ZRem(ctx, key, members...).Err()
}

// Expire sets expiration on a key
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return c.rdb.Expire(ctx, key, expiration).Err()
}

// Del deletes keys
func (c *Client) Del(ctx context.Context, keys ...string) error {
	return c.rdb.Del(ctx, keys...).Err()
}

// Exists checks if keys exist
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.rdb.Exists(ctx, keys...).Result()
}

// Pipeline creates a new pipeline for batch operations
func (c *Client) Pipeline() redis.Pipeliner {
	return c.rdb.Pipeline()
}

// TxPipeline creates a new transaction pipeline
func (c *Client) TxPipeline() redis.Pipeliner {
	return c.rdb.TxPipeline()
}

// Watch watches keys for changes in a transaction
func (c *Client) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return c.rdb.Watch(ctx, fn, keys...)
}

// Eval executes a Lua script
func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return c.rdb.Eval(ctx, script, keys, args...).Result()
}

// EvalSha executes a Lua script by SHA
func (c *Client) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) (interface{}, error) {
	return c.rdb.EvalSha(ctx, sha1, keys, args...).Result()
}

// GeoAdd adds geospatial items to a key
func (c *Client) GeoAdd(ctx context.Context, key string, geoLocation ...*redis.GeoLocation) error {
	return c.rdb.GeoAdd(ctx, key, geoLocation...).Err()
}

// GeoRadius finds members within a radius
func (c *Client) GeoRadius(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) ([]redis.GeoLocation, error) {
	return c.rdb.GeoRadius(ctx, key, longitude, latitude, query).Result()
}

// GeoDist returns the distance between two members
func (c *Client) GeoDist(ctx context.Context, key, member1, member2, unit string) (float64, error) {
	return c.rdb.GeoDist(ctx, key, member1, member2, unit).Result()
}

// GeoPos returns positions of members
func (c *Client) GeoPos(ctx context.Context, key string, members ...string) ([]*redis.GeoPos, error) {
	return c.rdb.GeoPos(ctx, key, members...).Result()
}
