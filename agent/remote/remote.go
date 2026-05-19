// Package remote implements a RemoteAgent that forwards prompts to a personal
// computer via WebSocket and streams core.Event back through the Agent interface.
package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

// RemoteAgent implements core.Agent for remote execution via WebSocket.
type RemoteAgent struct {
	mu       sync.Mutex
	conn     *websocket.Conn
	userID   string
	sessions map[string]*RemoteSession
}

// RemoteSession implements core.AgentSession over WebSocket.
type RemoteSession struct {
	agent     *RemoteAgent
	sessionID string
	events    chan core.Event
	done      chan struct{}
	alive     bool
	mu        sync.Mutex
}

// WireEvent is the JSON format exchanged over WebSocket.
type WireEvent struct {
	Type         string         `json:"type"`
	Content      string         `json:"content,omitempty"`
	SessionID    string         `json:"session_id,omitempty"`
	ToolName     string         `json:"tool_name,omitempty"`
	ToolInput    string         `json:"tool_input,omitempty"`
	ToolResult   string         `json:"tool_result,omitempty"`
	RequestID    string         `json:"request_id,omitempty"`
	Done         bool           `json:"done"`
	Error        string         `json:"error,omitempty"`
	InputTokens  int            `json:"input_tokens,omitempty"`
	OutputTokens int            `json:"output_tokens,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// PromptMessage is sent to the remote agent.
type PromptMessage struct {
	Type      string `json:"type"` // "prompt"
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

// --- core.Agent interface ---

func (a *RemoteAgent) Name() string {
	return "remote-" + a.userID
}

func (a *RemoteAgent) StartSession(ctx context.Context, sessionID string) (core.AgentSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if s, ok := a.sessions[sessionID]; ok && s.Alive() {
		return s, nil
	}

	s := &RemoteSession{
		agent:     a,
		sessionID: sessionID,
		events:    make(chan core.Event, 64),
		done:      make(chan struct{}),
		alive:     true,
	}
	a.sessions[sessionID] = s
	go s.readLoop()

	return s, nil
}

func (a *RemoteAgent) ListSessions(ctx context.Context) ([]core.AgentSessionInfo, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var infos []core.AgentSessionInfo
	for id := range a.sessions {
		infos = append(infos, core.AgentSessionInfo{ID: id})
	}
	return infos, nil
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

// --- core.AgentSession interface ---

func (s *RemoteSession) Send(prompt string, images []core.ImageAttachment, files []core.FileAttachment) error {
	msg := PromptMessage{
		Type:      "prompt",
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

func (s *RemoteSession) RespondPermission(requestID string, result core.PermissionResult) error {
	msg := map[string]any{
		"type":       "permission_response",
		"request_id": requestID,
		"result":     result,
	}
	data, _ := json.Marshal(msg)
	s.agent.mu.Lock()
	defer s.agent.mu.Unlock()
	return s.agent.conn.WriteMessage(websocket.TextMessage, data)
}

func (s *RemoteSession) Events() <-chan core.Event {
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

// readLoop reads WireEvents from WebSocket and converts to core.Event.
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
			s.events <- core.Event{Type: core.EventError, Error: err, Done: true}
			return
		}

		if err := ValidateMessageSize(message); err != nil {
			log.Printf("[remote] oversized message from %s: %v", s.agent.userID, err)
			continue
		}

		var wire WireEvent
		if err := json.Unmarshal(message, &wire); err != nil {
			continue
		}

		evt := wireToEvent(wire)
		s.events <- evt

		if evt.Done {
			return
		}
	}
}

// wireToEvent converts a WireEvent (JSON from WebSocket) to a core.Event.
func wireToEvent(w WireEvent) core.Event {
	evt := core.Event{
		Content:      w.Content,
		SessionID:    w.SessionID,
		ToolName:     w.ToolName,
		ToolInput:    w.ToolInput,
		ToolResult:   w.ToolResult,
		RequestID:    w.RequestID,
		Done:         w.Done,
		InputTokens:  w.InputTokens,
		OutputTokens: w.OutputTokens,
		Metadata:     w.Metadata,
	}

	switch w.Type {
	case "text":
		evt.Type = core.EventText
	case "tool_use":
		evt.Type = core.EventToolUse
	case "tool_result":
		evt.Type = core.EventToolResult
	case "result":
		evt.Type = core.EventResult
	case "error":
		evt.Type = core.EventError
		if w.Error != "" {
			evt.Error = fmt.Errorf("%s", w.Error)
		}
	case "thinking":
		evt.Type = core.EventThinking
	case "permission_request":
		evt.Type = core.EventPermissionRequest
	default:
		evt.Type = core.EventText
	}

	return evt
}
