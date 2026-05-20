package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

// TestRemoteAgentFullFlow validates the complete flow:
// connect → register → send prompt → stream events → done
func TestRemoteAgentFullFlow(t *testing.T) {
	dispatcher := NewDispatcher()

	server := httptest.NewServer(http.HandlerFunc(dispatcher.HandleConnect))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"

	// Mock agent connects
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Register
	reg := RegisterMessage{Type: "register", UserID: "ou_test_001", Token: "test-token", Version: "1.0.0"}
	data, _ := json.Marshal(reg)
	conn.WriteMessage(websocket.TextMessage, data)
	conn.ReadMessage() // ack

	// Get the RemoteAgent
	agent := dispatcher.GetAgent("ou_test_001")
	if agent == nil {
		t.Fatal("agent should be registered")
	}

	// Verify it implements core.Agent
	var _ core.Agent = agent

	// Start session (implements core.AgentSession)
	session, err := agent.StartSession(context.Background(), "session-001")
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	var _ core.AgentSession = session

	// Send prompt
	err = session.Send("帮我编译项目", nil, nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Agent side: read prompt
	_, promptMsg, _ := conn.ReadMessage()
	var prompt PromptMessage
	json.Unmarshal(promptMsg, &prompt)
	if prompt.Prompt != "帮我编译项目" {
		t.Fatalf("prompt mismatch: %s", prompt.Prompt)
	}

	// Agent sends streaming events using legacy wire format (top-level type/content/done)
	events := []map[string]any{
		{"type": "text", "content": "正在编译...", "session_id": "session-001"},
		{"type": "tool_use", "tool_name": "Bash", "tool_input": "make build", "session_id": "session-001"},
		{"type": "tool_result", "tool_result": "BUILD SUCCESSFUL", "session_id": "session-001"},
		{"type": "result", "content": "编译完成", "session_id": "session-001", "done": true, "input_tokens": 1000, "output_tokens": 500},
	}
	for _, evt := range events {
		d, _ := json.Marshal(evt)
		conn.WriteMessage(websocket.TextMessage, d)
		time.Sleep(5 * time.Millisecond)
	}

	// cc-connect side: consume events
	var received []core.Event
	timeout := time.After(2 * time.Second)
	for {
		select {
		case evt := <-session.Events():
			received = append(received, evt)
			if evt.Done {
				goto done
			}
		case <-timeout:
			t.Fatalf("timeout, got %d events", len(received))
		}
	}
done:

	if len(received) != 4 {
		t.Fatalf("expected 4 events, got %d", len(received))
	}
	if received[0].Type != core.EventText || received[0].Content != "正在编译..." {
		t.Fatalf("event 0 mismatch: %+v", received[0])
	}
	if received[1].Type != core.EventToolUse || received[1].ToolName != "Bash" {
		t.Fatalf("event 1 mismatch: %+v", received[1])
	}
	if received[2].Type != core.EventToolResult {
		t.Fatalf("event 2 mismatch: %+v", received[2])
	}
	if received[3].Type != core.EventResult || !received[3].Done {
		t.Fatalf("event 3 mismatch: %+v", received[3])
	}
	if received[3].InputTokens != 1000 || received[3].OutputTokens != 500 {
		t.Fatalf("token counts mismatch: %d/%d", received[3].InputTokens, received[3].OutputTokens)
	}

	t.Log("✅ RemoteAgent implements core.Agent/AgentSession — full streaming flow verified")
}

func TestSingleDeviceKick(t *testing.T) {
	dispatcher := NewDispatcher()
	server := httptest.NewServer(http.HandlerFunc(dispatcher.HandleConnect))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/"

	conn1, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	reg := RegisterMessage{Type: "register", UserID: "ou_user", Token: "tok", Version: "1.0.0"}
	data, _ := json.Marshal(reg)
	conn1.WriteMessage(websocket.TextMessage, data)
	conn1.ReadMessage()

	conn2, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	conn2.WriteMessage(websocket.TextMessage, data)
	conn2.ReadMessage()

	time.Sleep(50 * time.Millisecond)
	_, _, err := conn1.ReadMessage()
	if err == nil {
		t.Fatal("first connection should be closed")
	}

	conn2.Close()
	t.Log("✅ Single device: new connection kicks old")
}
