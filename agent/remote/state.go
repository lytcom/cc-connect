// state.go — Persistent routing state.
// Survives server restarts by writing to a JSON file.
package remote

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// RoutingTarget represents where a user's messages should be routed.
type RoutingTarget string

const (
	TargetServer RoutingTarget = "server"
	TargetLocal  RoutingTarget = "local"
)

// UserRoutingState holds the routing preference for one user in one session.
type UserRoutingState struct {
	Target    RoutingTarget `json:"target"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// RoutingStateStore persists routing decisions to disk.
type RoutingStateStore struct {
	mu       sync.RWMutex
	filePath string
	states   map[string]*UserRoutingState // key = session_key (chat_id:user_id)
}

// NewRoutingStateStore creates or loads state from disk.
func NewRoutingStateStore(filePath string) *RoutingStateStore {
	store := &RoutingStateStore{
		filePath: filePath,
		states:   make(map[string]*UserRoutingState),
	}
	store.load()
	return store
}

// GetTarget returns the routing target for a session_key.
func (s *RoutingStateStore) GetTarget(sessionKey string) RoutingTarget {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if state, ok := s.states[sessionKey]; ok {
		return state.Target
	}
	return TargetServer // default
}

// SetTarget updates the routing target for a session_key and persists to disk.
func (s *RoutingStateStore) SetTarget(sessionKey string, target RoutingTarget) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[sessionKey] = &UserRoutingState{
		Target:    target,
		UpdatedAt: time.Now(),
	}
	s.save()
}

// RemoveUser removes all routing state for a user (used when agent disconnects).
func (s *RoutingStateStore) RemoveUser(userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Don't remove — keep user's preference so it's restored when they reconnect
	// Only reset targets that point to "local" if the user is offline
	for key, state := range s.states {
		if state.Target == TargetLocal {
			// Check if this session belongs to the disconnected user
			// session_key format: "chat_id:user_id" or "feishu:chat_id:user_id"
			if containsUserID(key, userID) {
				state.Target = TargetServer
				state.UpdatedAt = time.Now()
			}
		}
	}
	s.save()
}

func containsUserID(sessionKey, userID string) bool {
	// Simple check: session_key contains the user_id
	return len(sessionKey) > 0 && len(userID) > 0 &&
		(sessionKey == userID || len(sessionKey) > len(userID) &&
			sessionKey[len(sessionKey)-len(userID):] == userID)
}

func (s *RoutingStateStore) load() {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return // file doesn't exist yet, start fresh
	}
	json.Unmarshal(data, &s.states)
}

func (s *RoutingStateStore) save() {
	data, _ := json.MarshalIndent(s.states, "", "  ")
	os.WriteFile(s.filePath, data, 0644)
}

// AllStates returns a copy of all routing states (for admin monitoring).
func (s *RoutingStateStore) AllStates() map[string]UserRoutingState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]UserRoutingState, len(s.states))
	for k, v := range s.states {
		result[k] = *v
	}
	return result
}
