package remote

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestEventLog_RingBuffer(t *testing.T) {
	log := newEventLog(5)

	for i := 0; i < 8; i++ {
		log.Add(DispatchEvent{
			Time:      time.Now(),
			Direction: "send",
			Summary:   fmt.Sprintf("event-%d", i),
		})
	}

	events := log.Recent(10)
	if len(events) != 5 {
		t.Fatalf("want 5 events (cap), got %d", len(events))
	}
	if events[4].Summary != "event-7" {
		t.Errorf("newest should be event-7, got %s", events[4].Summary)
	}
	if events[0].Summary != "event-3" {
		t.Errorf("oldest should be event-3, got %s", events[0].Summary)
	}
}

func TestEventLog_RecentN(t *testing.T) {
	log := newEventLog(100)

	for i := 0; i < 20; i++ {
		log.Add(DispatchEvent{Time: time.Now(), Summary: fmt.Sprintf("evt-%d", i)})
	}

	events := log.Recent(5)
	if len(events) != 5 {
		t.Fatalf("want 5, got %d", len(events))
	}
	if events[4].Summary != "evt-19" {
		t.Errorf("want evt-19, got %s", events[4].Summary)
	}
}

func TestPing_RTT(t *testing.T) {
	env := newTestEnv(t)

	conn, _, _ := websocket.DefaultDialer.Dial(env.WsURL, nil)
	defer conn.Close()
	reg, _ := json.Marshal(map[string]any{"type": "register", "user_id": "ou_ping_001", "token": "t", "version": "1.0.0"})
	conn.WriteMessage(websocket.TextMessage, reg)
	conn.ReadMessage()
	time.Sleep(100 * time.Millisecond)

	// Agent responds to ping with pong
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var envelope map[string]any
			json.Unmarshal(msg, &envelope)
			if envelope["type"] == "ping" {
				pong, _ := json.Marshal(map[string]any{"type": "pong", "ts": envelope["ts"]})
				conn.WriteMessage(websocket.TextMessage, pong)
			}
		}
	}()

	ra := env.Dispatcher.GetAgent("ou_ping_001")
	if ra == nil {
		t.Fatal("agent not registered")
	}

	rtt, err := ra.Ping(3 * time.Second)
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if rtt <= 0 || rtt > 1*time.Second {
		t.Errorf("unexpected RTT: %v", rtt)
	}
}

func TestPing_Timeout(t *testing.T) {
	env := newTestEnv(t)
	_ = newMockAgent(t, env.WsURL, "ou_ping_to_001", BehaviorTimeout)
	time.Sleep(100 * time.Millisecond)

	ra := env.Dispatcher.GetAgent("ou_ping_to_001")
	_, err := ra.Ping(500 * time.Millisecond)
	if err == nil {
		t.Fatal("should timeout")
	}
}

func TestDispatchStatus(t *testing.T) {
	env := newTestEnv(t)
	_ = newMockAgent(t, env.WsURL, "ou_st_001", BehaviorTimeout)
	time.Sleep(100 * time.Millisecond)

	status := env.Dispatcher.Status()
	if status.Total != 1 {
		t.Errorf("want total=1, got %d", status.Total)
	}
	if len(status.Agents) != 1 {
		t.Fatalf("want 1 agent, got %d", len(status.Agents))
	}
	if status.Agents[0].UserID != "ou_st_001" {
		t.Errorf("wrong user_id: %s", status.Agents[0].UserID)
	}
	if !status.Agents[0].Online {
		t.Error("agent should be online")
	}
}
