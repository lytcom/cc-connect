package remote

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

func TestCentralReadLoop_SingleSession(t *testing.T) {
	env := newTestEnv(t)
	agent := newMockAgent(t, env.WsURL, "ou_single_001", BehaviorNormal)
	defer agent.close()

	time.Sleep(50 * time.Millisecond) // let registration complete

	ra := env.Dispatcher.GetAgent("ou_single_001")
	if ra == nil {
		t.Fatal("agent not registered")
	}

	session, err := ra.StartSession(context.Background(), "sess-001")
	if err != nil {
		t.Fatalf("start session: %v", err)
	}

	err = session.Send("hello", nil, nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Collect events until done
	var events []core.Event
	timeout := time.After(5 * time.Second)
	for {
		select {
		case evt := <-session.Events():
			events = append(events, evt)
			if evt.Done || evt.Type == core.EventResult {
				goto done
			}
		case <-timeout:
			t.Fatalf("timeout waiting for events, got %d events so far", len(events))
		}
	}
done:

	// Should have received at least a result event
	lastEvt := events[len(events)-1]
	if lastEvt.Type != core.EventResult {
		t.Errorf("last event should be result, got %s", lastEvt.Type)
	}
	if lastEvt.Content != "hello from local" {
		t.Errorf("want 'hello from local', got %q", lastEvt.Content)
	}
}

func TestCentralReadLoop_MultiSession(t *testing.T) {
	env := newTestEnv(t)

	// Custom agent that responds to two sessions
	conn, _, err := websocket.DefaultDialer.Dial(env.WsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Register
	reg, _ := json.Marshal(map[string]any{"type": "register", "user_id": "ou_multi_001", "token": "t", "version": "1.0.0"})
	conn.WriteMessage(websocket.TextMessage, reg)
	conn.ReadMessage() // ack

	time.Sleep(50 * time.Millisecond)
	ra := env.Dispatcher.GetAgent("ou_multi_001")

	sess1, _ := ra.StartSession(context.Background(), "sess-A")
	sess2, _ := ra.StartSession(context.Background(), "sess-B")

	// Read prompts and respond per-session
	go func() {
		for i := 0; i < 2; i++ {
			_, msg, _ := conn.ReadMessage()
			var prompt map[string]any
			json.Unmarshal(msg, &prompt)
			sid, _ := prompt["session_id"].(string)
			pid, _ := prompt["id"].(string)

			// ack
			ack, _ := json.Marshal(map[string]any{"type": "ack", "prompt_id": pid})
			conn.WriteMessage(websocket.TextMessage, ack)

			// result with session-specific content
			result, _ := json.Marshal(map[string]any{
				"type":       "raw_stream",
				"session_id": sid,
				"done":       true,
				"payload":    map[string]any{"type": "result", "result": "reply-for-" + sid, "session_id": sid},
			})
			conn.WriteMessage(websocket.TextMessage, result)
		}
	}()

	sess1.Send("msg1", nil, nil)
	sess2.Send("msg2", nil, nil)

	// Verify session A got its reply
	select {
	case evt := <-sess1.Events():
		if evt.Content != "reply-for-sess-A" {
			t.Errorf("sess-A got wrong content: %q", evt.Content)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sess-A timeout")
	}

	// Verify session B got its reply
	select {
	case evt := <-sess2.Events():
		if evt.Content != "reply-for-sess-B" {
			t.Errorf("sess-B got wrong content: %q", evt.Content)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sess-B timeout")
	}
}

func TestCentralReadLoop_ConnClose_BroadcastError(t *testing.T) {
	env := newTestEnv(t)
	agent := newMockAgent(t, env.WsURL, "ou_dc_001", BehaviorDisconnect)

	time.Sleep(50 * time.Millisecond)
	ra := env.Dispatcher.GetAgent("ou_dc_001")
	session, _ := ra.StartSession(context.Background(), "sess-001")
	session.Send("trigger disconnect", nil, nil)

	// Should receive error event after agent disconnects
	select {
	case evt := <-session.Events():
		if evt.Type != core.EventError {
			t.Errorf("expected EventError, got %s", evt.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for disconnect error")
	}

	_ = agent // keep reference
}

func TestSessionTimeout_FirstEvent(t *testing.T) {
	env := newTestEnv(t)
	agent := newMockAgent(t, env.WsURL, "ou_to_001", BehaviorTimeout)

	time.Sleep(50 * time.Millisecond)
	ra := env.Dispatcher.GetAgent("ou_to_001")
	if ra == nil {
		t.Fatal("agent not registered")
	}

	session, _ := ra.StartSession(context.Background(), "sess-001")

	// Override timeout for test (normally 90s, use 1s)
	if rs, ok := session.(*RemoteSession); ok {
		rs.firstEventTimeout = 1 * time.Second
	}

	session.Send("hello", nil, nil)

	select {
	case evt := <-session.Events():
		if evt.Type != core.EventError {
			t.Errorf("expected timeout error, got %s", evt.Type)
		}
		if evt.Error == nil {
			t.Error("error should not be nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("never received timeout error")
	}

	_ = agent
}

func TestSessionTimeout_ConnStaysHealthy(t *testing.T) {
	env := newTestEnv(t)
	agent := newMockAgent(t, env.WsURL, "ou_healthy_001", BehaviorTimeout)

	time.Sleep(50 * time.Millisecond)
	ra := env.Dispatcher.GetAgent("ou_healthy_001")

	// Session 1: will timeout
	sess1, _ := ra.StartSession(context.Background(), "sess-timeout")
	if rs, ok := sess1.(*RemoteSession); ok {
		rs.firstEventTimeout = 500 * time.Millisecond
	}
	sess1.Send("will timeout", nil, nil)

	select {
	case <-sess1.Events():
		// got timeout error
	case <-time.After(2 * time.Second):
		t.Fatal("sess1 never timed out")
	}

	// Connection should still be alive
	if !ra.connAlive.Load() {
		t.Fatal("conn should still be healthy after session timeout")
	}

	// Session 2 on same agent should be creatable
	sess2, err := ra.StartSession(context.Background(), "sess-after")
	if err != nil {
		t.Fatalf("start session 2: %v", err)
	}
	if sess2 == nil {
		t.Fatal("session 2 should not be nil")
	}

	_ = agent
}

func TestCentralReadLoop_UnknownSessionID(t *testing.T) {
	env := newTestEnv(t)

	conn, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	defer conn.Close()
	reg, _ := json.Marshal(map[string]any{"type": "register", "user_id": "ou_unk_001", "token": "t", "version": "1.0.0"})
	conn.WriteMessage(websocket.TextMessage, reg)
	conn.ReadMessage()

	time.Sleep(50 * time.Millisecond)
	ra := env.Dispatcher.GetAgent("ou_unk_001")
	session, _ := ra.StartSession(context.Background(), "sess-known")

	// Agent sends event for unknown session — should not crash
	unknown, _ := json.Marshal(map[string]any{
		"type": "raw_stream", "session_id": "sess-unknown", "done": true,
		"payload": map[string]any{"type": "result", "result": "lost"},
	})
	conn.WriteMessage(websocket.TextMessage, unknown)

	// Known session should not receive the unknown event
	select {
	case evt := <-session.Events():
		t.Errorf("known session should not get unknown event: %+v", evt)
	case <-time.After(500 * time.Millisecond):
		// good — no event leaked
	}
}
