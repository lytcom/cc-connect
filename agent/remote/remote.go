// Package remote implements a RemoteAgent that forwards prompts to a personal
// computer via WebSocket and streams core.Event back through the Agent interface.
package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

// PendingPrompt tracks a prompt sent to the agent that has not yet been ACK'd.
type PendingPrompt struct {
	ID        string
	SessionID string
	Prompt    string
	SentAt    time.Time
	Raw       []byte // serialized PromptMessage for resend
}

// RemoteAgent implements core.Agent for remote execution via WebSocket.
// A single centralReadLoop goroutine reads all messages from the conn and
// dispatches them to the appropriate session by session_id.
type RemoteAgent struct {
	mu        sync.Mutex
	conn      *websocket.Conn
	userID    string
	meta      AgentMeta
	sessions  map[string]*RemoteSession
	connAlive atomic.Bool
	readDone  chan struct{}

	// Pending prompts awaiting ACK from the agent.
	pending map[string]*PendingPrompt

	// Reconnect support
	reconnectTimeout time.Duration // default 30s, configurable for tests
	reconnectTimer   *time.Timer   // started on disconnect, fires removal
	dispatcher       *Dispatcher   // back-reference for removal on timeout

	sessionKey string // CC_SESSION_KEY for the current turn

	// Active sessions reported by the agent via agent_online message.
	activeSessions []string

	// Ping/pong support for diagnostics
	pongCh chan int64
}

// RemoteSession implements core.AgentSession over WebSocket.
// It does NOT have its own readLoop — events are fed by the agent's centralReadLoop.
type RemoteSession struct {
	agent      *RemoteAgent
	sessionID  string
	sessionKey string
	events     chan core.Event
	done       chan struct{}
	alive      atomic.Bool
	mu         sync.Mutex

	// Timer-based session timeout (does NOT affect the conn).
	firstEventTimeout time.Duration // configurable; default 90s
	timer             *time.Timer   // started in Send(), fires timeout error
	timerFired        atomic.Bool   // prevents double-fire
}

// PromptMessage is sent to the remote agent.
type PromptMessage struct {
	Type       string `json:"type"` // "prompt"
	ID         string `json:"id"`
	SessionID  string `json:"session_id"`
	SessionKey string `json:"session_key,omitempty"`
	Prompt     string `json:"prompt"`
	Timestamp  int64  `json:"timestamp"`
}

// NewRemoteAgent creates a RemoteAgent from an established WebSocket connection.
// It starts the centralReadLoop goroutine immediately.
func NewRemoteAgent(conn *websocket.Conn, userID string, dispatcher *Dispatcher) *RemoteAgent {
	a := &RemoteAgent{
		conn:             conn,
		userID:           userID,
		sessions:         make(map[string]*RemoteSession),
		readDone:         make(chan struct{}),
		pending:          make(map[string]*PendingPrompt),
		reconnectTimeout: 30 * time.Second,
		dispatcher:       dispatcher,
	}
	a.connAlive.Store(true)
	go a.centralReadLoop()
	return a
}

// --- core.SessionEnvInjector interface ---

func (a *RemoteAgent) SetSessionEnv(env []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, e := range env {
		if strings.HasPrefix(e, "CC_SESSION_KEY=") {
			a.sessionKey = strings.TrimPrefix(e, "CC_SESSION_KEY=")
		}
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
		s.mu.Lock()
		s.sessionKey = a.sessionKey
		s.mu.Unlock()
		return s, nil
	}

	s := &RemoteSession{
		agent:      a,
		sessionID:  sessionID,
		sessionKey: a.sessionKey,
		events:     make(chan core.Event, 64),
		done:       make(chan struct{}),
	}
	s.alive.Store(true)
	a.sessions[sessionID] = s

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
	if a.reconnectTimer != nil {
		a.reconnectTimer.Stop()
		a.reconnectTimer = nil
	}
	for _, s := range a.sessions {
		s.Close()
	}
	a.connAlive.Store(false)
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}

// ActiveSessions returns the session IDs reported by the agent via agent_online.
func (a *RemoteAgent) ActiveSessions() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.activeSessions...)
}

// SendCloseSession sends a close_session signal to the remote agent so it can
// tear down the corresponding Claude Code session on the user's machine.
func (a *RemoteAgent) SendCloseSession(sessionKey string) error {
	if !a.connAlive.Load() {
		return fmt.Errorf("agent not connected")
	}
	msg := map[string]any{
		"type":        "close_session",
		"session_key": sessionKey,
	}
	data, _ := json.Marshal(msg)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conn.WriteMessage(websocket.TextMessage, data)
}

// --- centralReadLoop: the ONLY reader of conn ---

// centralReadLoop reads all messages from the WebSocket connection and dispatches
// them to sessions by session_id. On connection error, it broadcasts an error to
// all alive sessions.
func (a *RemoteAgent) centralReadLoop() {
	defer close(a.readDone)

	for {
		_, message, err := a.conn.ReadMessage()
		if err != nil {
			// Connection closed or error — handle disconnect (start reconnect window)
			log.Printf("[remote] connection error (user=%s): %v", a.userID, err)
			a.handleDisconnect(err)
			return
		}

		if err := ValidateMessageSize(message); err != nil {
			log.Printf("[remote] oversized message from %s: %v", a.userID, err)
			continue
		}

		// Try to parse as WireEnvelope
		var envelope WireEnvelope
		if err := json.Unmarshal(message, &envelope); err != nil {
			// Completely malformed JSON — skip
			log.Printf("[remote] malformed JSON from %s: %v", a.userID, err)
			continue
		}

		// Store raw bytes for legacy fallback path
		envelope.Raw = message

		// Determine session_id for routing (agent may send session_key instead)
		sessionID := envelope.SessionID
		if sessionID == "" {
			sessionID = envelope.SessionKey
		}

		// For ack type, resolve pending prompt
		if envelope.Type == "ack" {
			a.handleACK(envelope)
			continue
		}

		// For pong type, notify Ping() waiter
		if envelope.Type == "pong" {
			a.mu.Lock()
			if a.pongCh != nil {
				select {
				case a.pongCh <- envelope.Ts:
				default:
				}
			}
			a.mu.Unlock()
			continue
		}

		// For agent_online type, store active sessions
		if envelope.Type == "agent_online" {
			var onlineMsg struct {
				Sessions []string `json:"sessions"`
			}
			if envelope.Raw != nil {
				json.Unmarshal(envelope.Raw, &onlineMsg)
			}
			a.mu.Lock()
			a.activeSessions = onlineMsg.Sessions
			a.mu.Unlock()
			log.Printf("[remote] agent_online: user=%s sessions=%v", a.userID, onlineMsg.Sessions)
			continue
		}

		// Parse the envelope into a core.Event
		evt := parseEvent(envelope)

		// Set session_id on the event if the envelope had one
		if sessionID != "" && evt.SessionID == "" {
			evt.SessionID = sessionID
		}

		// Dispatch to the appropriate session
		if sessionID != "" {
			a.dispatchToSession(sessionID, evt)
		} else {
			// No session_id — try to dispatch to all sessions (legacy behavior)
			// or if only one session exists, route there
			a.dispatchBroadcastOrSingle(evt)
		}
	}
}

// handleDisconnect is called when centralReadLoop detects a connection error.
// It sets connAlive=false, broadcasts a disconnect message, and starts the
// reconnect timeout timer. Sessions are NOT closed — they wait for reconnect.
func (a *RemoteAgent) handleDisconnect(err error) {
	a.connAlive.Store(false)

	// Broadcast disconnect message to all sessions
	a.broadcastError("个人电脑连接中断，等待重连中...")

	// Start reconnect timer — after timeout, remove agent entirely
	a.mu.Lock()
	timeout := a.reconnectTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	a.reconnectTimer = time.AfterFunc(timeout, func() {
		log.Printf("[remote] reconnect timeout expired for user=%s, removing agent", a.userID)
		if a.dispatcher != nil {
			a.dispatcher.RemoveAgent(a.userID)
		}
	})
	a.mu.Unlock()
}

// replaceConn hot-swaps the WebSocket connection on a reconnecting agent.
// It stops the reconnect timer, replaces the conn, sets connAlive, starts
// a new centralReadLoop, and resends any pending prompts.
func (a *RemoteAgent) replaceConn(newConn *websocket.Conn) {
	a.mu.Lock()
	// Stop reconnect timer
	if a.reconnectTimer != nil {
		a.reconnectTimer.Stop()
		a.reconnectTimer = nil
	}
	a.conn = newConn
	a.readDone = make(chan struct{})
	a.mu.Unlock()

	a.connAlive.Store(true)
	go a.centralReadLoop()

	// Resend pending prompts
	a.resendPending()
}

// resendPending resends all pending (un-ACK'd) prompts over the new connection.
func (a *RemoteAgent) resendPending() {
	a.mu.Lock()
	var toSend [][]byte
	for _, p := range a.pending {
		toSend = append(toSend, p.Raw)
	}
	a.mu.Unlock()

	for _, raw := range toSend {
		a.mu.Lock()
		a.conn.WriteMessage(websocket.TextMessage, raw)
		a.mu.Unlock()
	}
}

// handleACK removes a prompt from the pending map when the agent acknowledges it.
func (a *RemoteAgent) handleACK(envelope WireEnvelope) {
	// Extract prompt_id from the envelope raw
	var ackMsg struct {
		PromptID string `json:"prompt_id"`
	}
	if envelope.Raw != nil {
		json.Unmarshal(envelope.Raw, &ackMsg)
	}
	if ackMsg.PromptID == "" {
		return
	}
	a.mu.Lock()
	delete(a.pending, ackMsg.PromptID)
	a.mu.Unlock()
}

// dispatchToSession delivers an event to a specific session by ID.
func (a *RemoteAgent) dispatchToSession(sessionID string, evt core.Event) {
	a.mu.Lock()
	s, ok := a.sessions[sessionID]
	a.mu.Unlock()

	if !ok || !s.Alive() {
		// Unknown session — log and drop
		log.Printf("[remote] event for unknown session %q (user=%s), dropping", sessionID, a.userID)
		return
	}

	// Stop the first-event timeout timer since we got a response.
	if s.timer != nil {
		s.timer.Stop()
	}

	// Non-blocking send to avoid deadlock if channel is full
	select {
	case s.events <- evt:
	default:
		log.Printf("[remote] events channel full for session %s (user=%s), dropping event", sessionID, a.userID)
	}
}

// dispatchBroadcastOrSingle handles events without a session_id.
// If there's exactly one session, deliver there. Otherwise drop.
func (a *RemoteAgent) dispatchBroadcastOrSingle(evt core.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.sessions) == 1 {
		for _, s := range a.sessions {
			if s.Alive() {
				if s.timer != nil {
					s.timer.Stop()
				}
				select {
				case s.events <- evt:
				default:
				}
			}
			return
		}
	}
	// Multiple sessions or none — can't route without session_id, drop
}

// broadcastError sends an error event to all alive sessions.
func (a *RemoteAgent) broadcastError(errMsg string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	errEvt := core.Event{
		Type:  core.EventError,
		Error: fmt.Errorf("%s", errMsg),
		Done:  true,
	}
	for _, s := range a.sessions {
		if s.Alive() {
			select {
			case s.events <- errEvt:
			default:
			}
		}
	}
}

// --- core.AgentSession interface ---

func (s *RemoteSession) Send(prompt string, images []core.ImageAttachment, files []core.FileAttachment) error {
	s.mu.Lock()
	sessionKey := s.sessionKey
	s.mu.Unlock()

	msg := PromptMessage{
		Type:       "prompt",
		ID:         fmt.Sprintf("%s-%d", s.sessionID, time.Now().UnixMilli()),
		SessionID:  s.sessionID,
		SessionKey: sessionKey,
		Prompt:     prompt,
		Timestamp:  time.Now().Unix(),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Add to pending map before sending (so disconnect can resend it)
	s.agent.mu.Lock()
	s.agent.pending[msg.ID] = &PendingPrompt{
		ID:        msg.ID,
		SessionID: s.sessionID,
		Prompt:    prompt,
		SentAt:    time.Now(),
		Raw:       data,
	}
	writeErr := s.agent.conn.WriteMessage(websocket.TextMessage, data)
	s.agent.mu.Unlock()
	if writeErr != nil {
		return writeErr
	}

	// Start first-event timeout timer (only affects this session, never the conn).
	timeout := s.firstEventTimeout
	if timeout == 0 {
		timeout = 180 * time.Second
	}
	s.timer = time.AfterFunc(timeout, func() {
		if s.timerFired.CompareAndSwap(false, true) {
			s.events <- core.Event{
				Type:  core.EventError,
				Error: fmt.Errorf("remote agent did not respond within %v", timeout),
				Done:  true,
			}
		}
	})
	return nil
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
	return s.alive.Load()
}

func (s *RemoteSession) Close() error {
	if s.alive.CompareAndSwap(true, false) {
		close(s.done)
	}
	return nil
}
