package remote

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

func TestReconnect_HotSwapConn(t *testing.T) {
	env := newTestEnv(t)

	conn1, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	reg, _ := json.Marshal(map[string]any{"type": "register", "user_id": "ou_rc_001", "token": "t", "version": "1.0.0"})
	conn1.WriteMessage(websocket.TextMessage, reg)
	conn1.ReadMessage() // ack
	time.Sleep(50 * time.Millisecond)

	ra := env.Dispatcher.GetAgent("ou_rc_001")
	if ra == nil {
		t.Fatal("agent not registered")
	}

	// Simulate disconnect
	conn1.Close()
	time.Sleep(200 * time.Millisecond)

	if ra.connAlive.Load() {
		t.Fatal("connAlive should be false after disconnect")
	}

	// Reconnect
	conn2, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	defer conn2.Close()
	conn2.WriteMessage(websocket.TextMessage, reg)
	conn2.ReadMessage() // ack
	time.Sleep(200 * time.Millisecond)

	ra2 := env.Dispatcher.GetAgent("ou_rc_001")
	if ra2 == nil {
		t.Fatal("agent should still exist")
	}
	if !ra2.connAlive.Load() {
		t.Fatal("connAlive should be true after reconnect")
	}
}

func TestReconnect_ResendPending(t *testing.T) {
	env := newTestEnv(t)

	conn1, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	reg, _ := json.Marshal(map[string]any{"type": "register", "user_id": "ou_rp_001", "token": "t", "version": "1.0.0"})
	conn1.WriteMessage(websocket.TextMessage, reg)
	conn1.ReadMessage()
	time.Sleep(50 * time.Millisecond)

	ra := env.Dispatcher.GetAgent("ou_rp_001")
	session, _ := ra.StartSession(context.Background(), "sess-001")
	session.Send("important question", nil, nil)
	time.Sleep(50 * time.Millisecond)

	// Disconnect before ACK
	conn1.Close()
	time.Sleep(200 * time.Millisecond)

	// Reconnect
	conn2, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	defer conn2.Close()
	conn2.WriteMessage(websocket.TextMessage, reg)
	conn2.ReadMessage() // register ack
	time.Sleep(200 * time.Millisecond)

	// Should receive the resent prompt
	conn2.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn2.ReadMessage()
	if err != nil {
		t.Fatalf("expected resent prompt: %v", err)
	}
	var prompt map[string]any
	json.Unmarshal(msg, &prompt)
	if prompt["type"] != "prompt" {
		t.Errorf("expected prompt type, got %v", prompt["type"])
	}
	text, _ := prompt["prompt"].(string)
	if text != "important question" {
		t.Errorf("wrong prompt: %q", text)
	}
}

func TestReconnect_TimeoutRemovesAgent(t *testing.T) {
	env := newTestEnv(t)

	conn, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	reg, _ := json.Marshal(map[string]any{"type": "register", "user_id": "ou_rm_001", "token": "t", "version": "1.0.0"})
	conn.WriteMessage(websocket.TextMessage, reg)
	conn.ReadMessage()
	time.Sleep(50 * time.Millisecond)

	ra := env.Dispatcher.GetAgent("ou_rm_001")
	ra.reconnectTimeout = 500 * time.Millisecond

	conn.Close()
	time.Sleep(200 * time.Millisecond)

	// Should still exist during window
	if env.Dispatcher.GetAgent("ou_rm_001") == nil {
		t.Fatal("should exist during reconnect window")
	}

	// Wait for timeout
	time.Sleep(600 * time.Millisecond)

	if env.Dispatcher.GetAgent("ou_rm_001") != nil {
		t.Fatal("should be removed after timeout")
	}
}

func TestReconnect_BroadcastDisconnectMessage(t *testing.T) {
	env := newTestEnv(t)

	conn, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	reg, _ := json.Marshal(map[string]any{"type": "register", "user_id": "ou_bc_001", "token": "t", "version": "1.0.0"})
	conn.WriteMessage(websocket.TextMessage, reg)
	conn.ReadMessage()
	time.Sleep(50 * time.Millisecond)

	ra := env.Dispatcher.GetAgent("ou_bc_001")
	sess, _ := ra.StartSession(context.Background(), "sess-001")

	// Disconnect
	conn.Close()

	select {
	case evt := <-sess.Events():
		if evt.Type != core.EventError {
			t.Errorf("expected EventError, got %s", evt.Type)
		}
		if evt.Error == nil || !strings.Contains(evt.Error.Error(), "连接中断") {
			t.Errorf("expected disconnect message, got: %v", evt.Error)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no disconnect notification")
	}
}
