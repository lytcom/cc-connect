// token_manager.go — Token lifecycle management.
// Handles generation, storage, expiry, and validation.
package remote

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	DefaultTokenExpiry = 30 * 24 * time.Hour // 30 days
	TokenLength        = 32                   // 32 bytes = 64 hex chars
	ExpiryWarningDays  = 7
)

// TokenEntry represents a stored token with metadata.
type TokenEntry struct {
	UserID    string    `json:"user_id"`
	Token     string    `json:"token"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// TokenManager handles token generation, storage, and validation.
type TokenManager struct {
	mu       sync.RWMutex
	tokens   map[string]*TokenEntry // user_id → token entry
	filePath string
}

// NewTokenManager creates or loads a token store.
func NewTokenManager(filePath string) *TokenManager {
	tm := &TokenManager{
		tokens:   make(map[string]*TokenEntry),
		filePath: filePath,
	}
	tm.load()
	return tm
}

// GenerateToken creates a new token for a user.
func (tm *TokenManager) GenerateToken(userID string) (*TokenEntry, error) {
	b := make([]byte, TokenLength)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("generate random: %w", err)
	}

	entry := &TokenEntry{
		UserID:    userID,
		Token:     hex.EncodeToString(b),
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(DefaultTokenExpiry),
	}

	tm.mu.Lock()
	tm.tokens[userID] = entry
	tm.save()
	tm.mu.Unlock()

	return entry, nil
}

// ValidateToken checks if a token is valid and not expired.
func (tm *TokenManager) ValidateToken(userID, token string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	entry, ok := tm.tokens[userID]
	if !ok {
		return false
	}
	if entry.Token != token {
		return false
	}
	if time.Now().After(entry.ExpiresAt) {
		return false
	}
	return true
}

// GetToken returns the stored token for a user (for HMAC verification).
func (tm *TokenManager) GetToken(userID string) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if entry, ok := tm.tokens[userID]; ok {
		if time.Now().Before(entry.ExpiresAt) {
			return entry.Token
		}
	}
	return ""
}

// RevokeToken removes a user's token.
func (tm *TokenManager) RevokeToken(userID string) {
	tm.mu.Lock()
	delete(tm.tokens, userID)
	tm.save()
	tm.mu.Unlock()
}

// ExpiringTokens returns tokens that expire within the warning period.
func (tm *TokenManager) ExpiringTokens() []*TokenEntry {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	threshold := time.Now().Add(ExpiryWarningDays * 24 * time.Hour)
	var expiring []*TokenEntry
	for _, entry := range tm.tokens {
		if entry.ExpiresAt.Before(threshold) && entry.ExpiresAt.After(time.Now()) {
			expiring = append(expiring, entry)
		}
	}
	return expiring
}

// AllTokens returns all token entries (for admin view, token value redacted).
func (tm *TokenManager) AllTokens() []map[string]any {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var result []map[string]any
	for _, entry := range tm.tokens {
		result = append(result, map[string]any{
			"user_id":    entry.UserID,
			"created_at": entry.CreatedAt,
			"expires_at": entry.ExpiresAt,
			"expired":    time.Now().After(entry.ExpiresAt),
			"token_hint": entry.Token[:8] + "...",
		})
	}
	return result
}

func (tm *TokenManager) load() {
	data, err := os.ReadFile(tm.filePath)
	if err != nil {
		return
	}
	var entries []*TokenEntry
	json.Unmarshal(data, &entries)
	for _, e := range entries {
		tm.tokens[e.UserID] = e
	}
}

func (tm *TokenManager) save() {
	var entries []*TokenEntry
	for _, e := range tm.tokens {
		entries = append(entries, e)
	}
	data, _ := json.MarshalIndent(entries, "", "  ")
	os.WriteFile(tm.filePath, data, 0600)
}
