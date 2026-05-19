// ratelimit.go — Message validation, size limits, and rate limiting.
package remote

import (
	"fmt"
	"sync"
	"time"
)

const (
	// MaxMessageSize is the maximum allowed message size in bytes (1MB).
	MaxMessageSize = 1 * 1024 * 1024
	// DefaultMaxMsgPerSec is the default rate limit (messages per second).
	DefaultMaxMsgPerSec = 50
	// MaxConnAttemptsPerMin per IP.
	MaxConnAttemptsPerMin = 5
)

// RateLimiter tracks message rates per connection.
type RateLimiter struct {
	mu          sync.Mutex
	maxPerSec   int
	windows     map[string]*slidingWindow
	connTracker map[string]*connAttempts
}

type slidingWindow struct {
	timestamps []time.Time
}

type connAttempts struct {
	attempts []time.Time
	locked   bool
}

// NewRateLimiter creates a rate limiter with the given max messages per second.
func NewRateLimiter(maxPerSec int) *RateLimiter {
	if maxPerSec <= 0 {
		maxPerSec = DefaultMaxMsgPerSec
	}
	return &RateLimiter{
		maxPerSec:   maxPerSec,
		windows:     make(map[string]*slidingWindow),
		connTracker: make(map[string]*connAttempts),
	}
}

// AllowMessage checks if a message from the given connection ID is within rate limits.
func (rl *RateLimiter) AllowMessage(connID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	w, ok := rl.windows[connID]
	if !ok {
		w = &slidingWindow{}
		rl.windows[connID] = w
	}

	now := time.Now()
	cutoff := now.Add(-1 * time.Second)

	// Remove old timestamps
	valid := w.timestamps[:0]
	for _, t := range w.timestamps {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	w.timestamps = valid

	if len(w.timestamps) >= rl.maxPerSec {
		return false
	}

	w.timestamps = append(w.timestamps, now)
	return true
}

// AllowConnection checks if a connection attempt from the given IP is allowed.
func (rl *RateLimiter) AllowConnection(ip string) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	ca, ok := rl.connTracker[ip]
	if !ok {
		ca = &connAttempts{}
		rl.connTracker[ip] = ca
	}

	if ca.locked {
		return fmt.Errorf("IP %s is locked due to too many failed attempts", ip)
	}

	now := time.Now()
	cutoff := now.Add(-1 * time.Minute)

	valid := ca.attempts[:0]
	for _, t := range ca.attempts {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	ca.attempts = valid

	if len(ca.attempts) >= MaxConnAttemptsPerMin {
		return fmt.Errorf("too many connection attempts from %s (max %d/min)", ip, MaxConnAttemptsPerMin)
	}

	ca.attempts = append(ca.attempts, now)
	return nil
}

// RecordAuthFailure records a failed auth attempt. After 10 failures, locks the user.
func (rl *RateLimiter) RecordAuthFailure(userID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	ca, ok := rl.connTracker[userID]
	if !ok {
		ca = &connAttempts{}
		rl.connTracker[userID] = ca
	}

	now := time.Now()
	ca.attempts = append(ca.attempts, now)

	// Count recent failures (last 10 minutes)
	cutoff := now.Add(-10 * time.Minute)
	count := 0
	for _, t := range ca.attempts {
		if t.After(cutoff) {
			count++
		}
	}

	if count >= 10 {
		ca.locked = true
		return true // locked
	}
	return false
}

// RemoveConnection cleans up rate limit state for a disconnected connection.
func (rl *RateLimiter) RemoveConnection(connID string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.windows, connID)
}

// ValidateMessageSize checks if the message is within size limits.
func ValidateMessageSize(data []byte) error {
	if len(data) > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes (max %d)", len(data), MaxMessageSize)
	}
	return nil
}
