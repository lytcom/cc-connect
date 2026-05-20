package remote

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRouter_SwitchToLocal(t *testing.T) {
	env := newTestEnv(t)
	_ = newMockAgent(t, env.WsURL, "ou_rt_001", BehaviorNormal)
	time.Sleep(50 * time.Millisecond)

	tmpDir := t.TempDir()
	router := NewRouter(env.Dispatcher, filepath.Join(tmpDir, "state.json"))

	sessionKey := "feishu:oc_chat:ou_rt_001"
	handled, msg := router.HandleSwitchCommand(sessionKey, "/local")
	if !handled {
		t.Fatal("/local should be handled")
	}
	if msg == "" {
		t.Fatal("should return confirmation message")
	}

	agent, err := router.RouteMessage(sessionKey, "ou_rt_001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent == nil {
		t.Fatal("after /local, RouteMessage should return RemoteAgent")
	}
}

func TestRouter_SwitchToServer(t *testing.T) {
	env := newTestEnv(t)
	_ = newMockAgent(t, env.WsURL, "ou_rt_002", BehaviorNormal)
	time.Sleep(50 * time.Millisecond)

	tmpDir := t.TempDir()
	router := NewRouter(env.Dispatcher, filepath.Join(tmpDir, "state.json"))

	sessionKey := "feishu:oc_chat:ou_rt_002"
	router.HandleSwitchCommand(sessionKey, "/local")
	router.HandleSwitchCommand(sessionKey, "/server")

	agent, err := router.RouteMessage(sessionKey, "ou_rt_002")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent != nil {
		t.Fatal("after /server, RouteMessage should return nil")
	}
}

func TestRouter_AgentOffline(t *testing.T) {
	tmpDir := t.TempDir()
	d := NewDispatcher()
	router := NewRouter(d, filepath.Join(tmpDir, "state.json"))

	sessionKey := "feishu:oc_chat:ou_offline_001"
	router.HandleSwitchCommand(sessionKey, "/local")

	agent, err := router.RouteMessage(sessionKey, "ou_offline_001")
	if err != ErrAgentOffline {
		t.Fatalf("expected ErrAgentOffline, got: %v", err)
	}
	if agent != nil {
		t.Fatal("should return nil agent when offline")
	}
}

func TestRouter_UserIsolation(t *testing.T) {
	env := newTestEnv(t)
	_ = newMockAgent(t, env.WsURL, "ou_iso_A", BehaviorNormal)
	_ = newMockAgent(t, env.WsURL, "ou_iso_B", BehaviorNormal)
	time.Sleep(50 * time.Millisecond)

	tmpDir := t.TempDir()
	router := NewRouter(env.Dispatcher, filepath.Join(tmpDir, "state.json"))

	keyA := "feishu:oc_chatA:ou_iso_A"
	keyB := "feishu:oc_chatB:ou_iso_B"

	router.HandleSwitchCommand(keyA, "/local")

	agentB, err := router.RouteMessage(keyB, "ou_iso_B")
	if err != nil {
		t.Fatalf("unexpected error for B: %v", err)
	}
	if agentB != nil {
		t.Fatal("B should not be routed to remote")
	}

	agentA, err := router.RouteMessage(keyA, "ou_iso_A")
	if err != nil {
		t.Fatalf("unexpected error for A: %v", err)
	}
	if agentA == nil {
		t.Fatal("A should be routed to remote")
	}
}

func TestRoutingStateStore_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "state.json")

	store1 := NewRoutingStateStore(path)
	store1.SetTarget("key1", TargetLocal)

	store2 := NewRoutingStateStore(path)
	if store2.GetTarget("key1") != TargetLocal {
		t.Fatal("state should persist to disk")
	}
}

func TestRoutingStateStore_RemoveUser(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "state.json")

	store := NewRoutingStateStore(path)
	store.SetTarget("feishu:oc_x:ou_user123", TargetLocal)
	store.SetTarget("feishu:oc_y:ou_user456", TargetLocal)

	store.RemoveUser("ou_user123")

	if store.GetTarget("feishu:oc_x:ou_user123") != TargetServer {
		t.Fatal("removed user should revert to server")
	}
	if store.GetTarget("feishu:oc_y:ou_user456") != TargetLocal {
		t.Fatal("other user should not be affected")
	}
}
