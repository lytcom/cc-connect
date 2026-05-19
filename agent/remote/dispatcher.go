// dispatcher.go — WebSocket server that accepts remote agent connections.
// Phase 0 prototype: validates that remote agents can connect,
// receive prompts, and stream events back.
package remote

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// Dispatcher manages remote agent connections.
type Dispatcher struct {
	mu       sync.RWMutex
	agents   map[string]*RemoteAgent // user_id → agent
	upgrader websocket.Upgrader
}

// NewDispatcher creates a new remote agent dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		agents: make(map[string]*RemoteAgent),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// RegisterMessage is sent by the agent on connection.
type RegisterMessage struct {
	Type    string `json:"type"`    // "register"
	UserID  string `json:"user_id"`
	Token   string `json:"token"`
	Version string `json:"version"`
}

// HandleConnect handles a new WebSocket connection from a remote agent.
func (d *Dispatcher) HandleConnect(w http.ResponseWriter, r *http.Request) {
	conn, err := d.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[remote-dispatch] upgrade error: %v", err)
		return
	}

	// Read registration message
	_, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return
	}

	var reg RegisterMessage
	if err := json.Unmarshal(msg, &reg); err != nil || reg.Type != "register" {
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"invalid registration"}`))
		conn.Close()
		return
	}

	// TODO: validate token against user-registry.json rd role list
	// TODO: check single-device limit
	// TODO: challenge-response handshake (Phase 1)

	d.mu.Lock()
	if existing, ok := d.agents[reg.UserID]; ok {
		existing.Stop() // Kick old connection
	}
	agent := NewRemoteAgent(conn, reg.UserID)
	d.agents[reg.UserID] = agent
	d.mu.Unlock()

	// Send ACK
	ack, _ := json.Marshal(map[string]string{"type": "register_ack", "status": "ok"})
	conn.WriteMessage(websocket.TextMessage, ack)

	log.Printf("[remote-dispatch] agent registered: user=%s", reg.UserID)

	// Keep connection alive (heartbeat handled by WebSocket ping/pong)
	// The connection stays open until agent disconnects or is kicked
}

// GetAgent returns the remote agent for a user, or nil if offline.
func (d *Dispatcher) GetAgent(userID string) *RemoteAgent {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.agents[userID]
}

// IsOnline checks if a user's remote agent is connected.
func (d *Dispatcher) IsOnline(userID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	a, ok := d.agents[userID]
	return ok && a.conn != nil
}

// RemoveAgent removes a disconnected agent.
func (d *Dispatcher) RemoveAgent(userID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if a, ok := d.agents[userID]; ok {
		a.Stop()
		delete(d.agents, userID)
	}
}

// OnlineAgents returns a list of currently connected user IDs.
func (d *Dispatcher) OnlineAgents() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var ids []string
	for id := range d.agents {
		ids = append(ids, id)
	}
	return ids
}
