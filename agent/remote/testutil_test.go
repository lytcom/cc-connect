package remote

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Behavior defines how the mock agent responds to prompts.
type Behavior int

const (
	BehaviorNormal      Behavior = iota // ack → stream events → done
	BehaviorSlowStart                   // ack → 3s delay → stream events → done
	BehaviorNoAck                       // skip ack, stream events → done
	BehaviorTimeout                     // ack → never respond
	BehaviorDisconnect                  // ack → 500ms → close connection
	BehaviorBadProtocol                 // send malformed JSON
	BehaviorReconnect                   // ack → 500ms → disconnect → 1s → reconnect → re-ack
)

// testEnv sets up a dispatcher with httptest server and returns cleanup func.
type testEnv struct {
	Dispatcher *Dispatcher
	Server     *httptest.Server
	WsURL      string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	d := NewDispatcher()
	s := httptest.NewServer(http.HandlerFunc(d.HandleConnect))
	t.Cleanup(s.Close)
	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/"
	return &testEnv{Dispatcher: d, Server: s, WsURL: wsURL}
}

// mockAgent simulates a personal computer agent over WebSocket.
type mockAgent struct {
	t        *testing.T
	conn     *websocket.Conn
	userID   string
	behavior Behavior
	mu       sync.Mutex
	sent     []map[string]any // all messages sent back to server
	received []map[string]any // all prompts received
}

func newMockAgent(t *testing.T, wsURL, userID string, behavior Behavior) *mockAgent {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Register
	reg := map[string]any{"type": "register", "user_id": userID, "token": "test-token", "version": "1.0.0"}
	data, _ := json.Marshal(reg)
	conn.WriteMessage(websocket.TextMessage, data)

	// Read ack
	_, ackMsg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	var ack map[string]any
	json.Unmarshal(ackMsg, &ack)
	if ack["status"] != "ok" {
		t.Fatalf("register rejected: %v", ack)
	}

	m := &mockAgent{t: t, conn: conn, userID: userID, behavior: behavior}
	go m.readLoop()
	return m
}

func (m *mockAgent) readLoop() {
	for {
		_, msg, err := m.conn.ReadMessage()
		if err != nil {
			return
		}
		var envelope map[string]any
		if err := json.Unmarshal(msg, &envelope); err != nil {
			continue
		}

		msgType, _ := envelope["type"].(string)
		if msgType == "prompt" {
			m.mu.Lock()
			m.received = append(m.received, envelope)
			m.mu.Unlock()
			go m.handlePrompt(envelope)
		}
	}
}

func (m *mockAgent) handlePrompt(prompt map[string]any) {
	promptID, _ := prompt["id"].(string)
	sessionID, _ := prompt["session_id"].(string)

	switch m.behavior {
	case BehaviorNormal:
		m.sendAck(promptID)
		m.sendRawStream(sessionID, map[string]any{"type": "assistant", "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "hello from local"}}}}, false)
		m.sendRawStream(sessionID, map[string]any{"type": "result", "result": "hello from local", "session_id": sessionID}, true)

	case BehaviorSlowStart:
		m.sendAck(promptID)
		time.Sleep(3 * time.Second)
		m.sendRawStream(sessionID, map[string]any{"type": "result", "result": "slow response", "session_id": sessionID}, true)

	case BehaviorNoAck:
		m.sendRawStream(sessionID, map[string]any{"type": "result", "result": "no ack", "session_id": sessionID}, true)

	case BehaviorTimeout:
		m.sendAck(promptID)
		// never respond

	case BehaviorDisconnect:
		m.sendAck(promptID)
		time.Sleep(500 * time.Millisecond)
		m.conn.Close()

	case BehaviorBadProtocol:
		m.conn.WriteMessage(websocket.TextMessage, []byte("not json at all {{{"))

	case BehaviorReconnect:
		m.sendAck(promptID)
		time.Sleep(500 * time.Millisecond)
		m.conn.Close()
		// reconnect handled by test
	}
}

func (m *mockAgent) sendAck(promptID string) {
	msg := map[string]any{"type": "ack", "prompt_id": promptID}
	m.send(msg)
}

func (m *mockAgent) sendRawStream(sessionID string, payload map[string]any, done bool) {
	msg := map[string]any{
		"type":       "raw_stream",
		"session_id": sessionID,
		"done":       done,
		"payload":    payload,
	}
	m.send(msg)
}

func (m *mockAgent) send(msg map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, msg)
	data, _ := json.Marshal(msg)
	m.conn.WriteMessage(websocket.TextMessage, data)
}

func (m *mockAgent) close() {
	m.conn.Close()
}
