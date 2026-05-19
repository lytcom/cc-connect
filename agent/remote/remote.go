// Package remote implements a RemoteAgent that forwards prompts to a personal
// computer via WebSocket and streams events back. This is the Phase 0 prototype
// to validate that the Agent interface can be satisfied by a remote execution path.
package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RemoteAgent implements the Agent interface for remote execution.
type RemoteAgent struct {
	mu       sync.Mutex
	conn     *websocket.Conn
	userID   string
	sessions map[string]*RemoteSession
}

// RemoteSession implements the AgentSession interface over WebSocket.
type RemoteSession struct {
	agent     *RemoteAgent
	sessionID string
	events    chan Event
	done      chan struct{}
	alive     bool
	mu        sync.Mutex
}

// Event mirrors core.Event for the remote protocol.
type Event struct {
	Type      string `json:"type"`       // "text", "tool_use", "tool_result", "result", "error"
	Content   string `json:"content"`
	SessionID string `json:"session_id"`
	Done      bool   `json:"done"`
	Error     string `json:"error,omitempty"`
}

// PromptMessage is sent to the remote agent.
type PromptMessage struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	Prompt    string `json:"prompt"`
	Timestamp int64  `json:"timestamp"`
}

// NewRemoteAgent creates a RemoteAgent from an established WebSocket connection.
func NewRemoteAgent(conn *websocket.Conn, userID string) *RemoteAgent {
	return &RemoteAgent{
		conn:     conn,
		userID:   userID,
		sessions: make(map[string]*RemoteSession),
	}
}

func (a *RemoteAgent) Name() string {
	return "remote-" + a.userID
}

func (a *RemoteAgent) StartSession(ctx context.Context, sessionID string) (*RemoteSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if s, ok := a.sessions[sessionID]; ok && s.alive {
		return s, nil
	}

	s := &RemoteSession{
		agent:     a,
		sessionID: sessionID,
		events:    make(chan Event, 100),
		done:      make(chan struct{}),
		alive:     true,
	}
	a.sessions[sessionID] = s

	// Start reading events from WebSocket
	go s.readLoop()

	return s, nil
}

func (a *RemoteAgent) ListSessions(ctx context.Context) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var ids []string
	for id := range a.sessions {
		ids = append(ids, id)
	}
	return ids, nil
}

func (a *RemoteAgent) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, s := range a.sessions {
		s.Close()
	}
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}

// RemoteSession methods

func (s *RemoteSession) Send(prompt string) error {
	msg := PromptMessage{
		ID:        fmt.Sprintf("%s-%d", s.sessionID, time.Now().UnixMilli()),
		SessionID: s.sessionID,
		Prompt:    prompt,
		Timestamp: time.Now().Unix(),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	s.agent.mu.Lock()
	defer s.agent.mu.Unlock()
	return s.agent.conn.WriteMessage(websocket.TextMessage, data)
}

func (s *RemoteSession) Events() <-chan Event {
	return s.events
}

func (s *RemoteSession) CurrentSessionID() string {
	return s.sessionID
}

func (s *RemoteSession) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alive
}

func (s *RemoteSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.alive {
		s.alive = false
		close(s.done)
	}
	return nil
}

func (s *RemoteSession) readLoop() {
	defer func() {
		s.mu.Lock()
		s.alive = false
		s.mu.Unlock()
	}()

	for {
		select {
		case <-s.done:
			return
		default:
		}

		_, message, err := s.agent.conn.ReadMessage()
		if err != nil {
			s.events <- Event{Type: "error", Error: err.Error(), Done: true}
			return
		}

		var evt Event
		if err := json.Unmarshal(message, &evt); err != nil {
			continue
		}

		s.events <- evt

		if evt.Done {
			return
		}
	}
}
