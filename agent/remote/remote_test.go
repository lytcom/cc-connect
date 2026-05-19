package remote

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestDispatcherRegisterAndStream validates the core Phase 0 assumption:
// a remote agent can connect, register, receive a prompt, and stream events back.
func TestDispatcherRegisterAndStream(t *testing.T) {
	dispatcher := NewDispatcher()

	// Start WebSocket server
	server := httptest.NewServer(http.HandlerFunc(dispatcher.HandleConnect))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"

	// --- Mock Agent connects ---
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send registration
	reg := RegisterMessage{Type: "register", UserID: "ou_test_001", Token: "test-token", Version: "1.0.0"}
	regData, _ := json.Marshal(reg)
	conn.WriteMessage(websocket.TextMessage, regData)

	// Read ACK
	_, ackMsg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read ack failed: %v", err)
	}
	var ack map[string]string
	json.Unmarshal(ackMsg, &ack)
	if ack["type"] != "register_ack" {
		t.Fatalf("expected register_ack, got: %s", string(ackMsg))
	}

	// Verify agent is online
	if !dispatcher.IsOnline("ou_test_001") {
		t.Fatal("agent should be online after registration")
	}

	// --- Simulate cc-connect sending a prompt ---
	agent := dispatcher.GetAgent("ou_test_001")
	if agent == nil {
		t.Fatal("agent should not be nil")
	}

	session, err := agent.StartSession(nil, "session-001")
	if err != nil {
		t.Fatalf("start session failed: %v", err)
	}

	// Send prompt through the agent
	err = session.Send("帮我编译项目")
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}

	// --- Mock Agent receives prompt and sends back streaming events ---
	// (In real scenario, the agent reads the prompt and executes Claude Code)

	// Read the prompt on the agent side
	_, promptMsg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("agent read prompt failed: %v", err)
	}
	var prompt PromptMessage
	json.Unmarshal(promptMsg, &prompt)
	if prompt.Prompt != "帮我编译项目" {
		t.Fatalf("expected prompt '帮我编译项目', got: %s", prompt.Prompt)
	}

	// Agent sends streaming events back
	chunks := []Event{
		{Type: "text", Content: "正在编译...", SessionID: "session-001", Done: false},
		{Type: "text", Content: "\n编译成功！", SessionID: "session-001", Done: false},
		{Type: "result", Content: "BUILD SUCCESSFUL", SessionID: "session-001", Done: true},
	}

	for _, chunk := range chunks {
		data, _ := json.Marshal(chunk)
		conn.WriteMessage(websocket.TextMessage, data)
		time.Sleep(10 * time.Millisecond)
	}

	// --- cc-connect side reads events from session ---
	var received []Event
	timeout := time.After(2 * time.Second)
	for {
		select {
		case evt := <-session.Events():
			received = append(received, evt)
			if evt.Done {
				goto done
			}
		case <-timeout:
			t.Fatal("timeout waiting for events")
		}
	}

done:
	if len(received) != 3 {
		t.Fatalf("expected 3 events, got %d", len(received))
	}
	if received[0].Content != "正在编译..." {
		t.Fatalf("first chunk mismatch: %s", received[0].Content)
	}
	if received[2].Content != "BUILD SUCCESSFUL" {
		t.Fatalf("final chunk mismatch: %s", received[2].Content)
	}
	if !received[2].Done {
		t.Fatal("last event should have Done=true")
	}

	t.Logf("✅ Phase 0 validated: remote agent connected, received prompt, streamed 3 events back")
}

// TestDispatcherSingleDevice validates that only one device can connect per user.
func TestDispatcherSingleDevice(t *testing.T) {
	dispatcher := NewDispatcher()

	server := httptest.NewServer(http.HandlerFunc(dispatcher.HandleConnect))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"

	// First connection
	conn1, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	reg := RegisterMessage{Type: "register", UserID: "ou_user", Token: "tok", Version: "1.0.0"}
	data, _ := json.Marshal(reg)
	conn1.WriteMessage(websocket.TextMessage, data)
	conn1.ReadMessage() // ack

	if !dispatcher.IsOnline("ou_user") {
		t.Fatal("first agent should be online")
	}

	// Second connection with same user_id — should kick the first
	conn2, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	conn2.WriteMessage(websocket.TextMessage, data)
	conn2.ReadMessage() // ack

	// First connection should be closed (kicked)
	time.Sleep(50 * time.Millisecond)
	_, _, err := conn1.ReadMessage()
	if err == nil {
		t.Fatal("first connection should have been closed after second registered")
	}

	conn2.Close()
	t.Logf("✅ Single device limit enforced: second connection kicked first")
}
