package remote

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// DispatchEvent is one logged event in the dispatch ring buffer.
type DispatchEvent struct {
	Time      time.Time
	Direction string // "send" / "recv" / "error" / "reconnect"
	TraceID   string
	Summary   string
}

// eventLog is a ring buffer of recent dispatch events.
type eventLog struct {
	mu       sync.Mutex
	events   []DispatchEvent
	capacity int
	pos      int
	count    int
}

func newEventLog(capacity int) *eventLog {
	return &eventLog{
		events:   make([]DispatchEvent, capacity),
		capacity: capacity,
	}
}

func (l *eventLog) Add(evt DispatchEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events[l.pos] = evt
	l.pos = (l.pos + 1) % l.capacity
	if l.count < l.capacity {
		l.count++
	}
}

func (l *eventLog) Recent(n int) []DispatchEvent {
	l.mu.Lock()
	defer l.mu.Unlock()

	if n > l.count {
		n = l.count
	}

	result := make([]DispatchEvent, n)
	start := (l.pos - n + l.capacity) % l.capacity
	for i := 0; i < n; i++ {
		result[i] = l.events[(start+i)%l.capacity]
	}
	return result
}

// Ping sends a ping to the remote agent and measures RTT.
func (a *RemoteAgent) Ping(timeout time.Duration) (time.Duration, error) {
	if !a.connAlive.Load() {
		return 0, fmt.Errorf("agent offline")
	}

	ts := time.Now().UnixMilli()
	msg, _ := json.Marshal(map[string]any{"type": "ping", "ts": ts})

	a.mu.Lock()
	// Reset pong channel
	a.pongCh = make(chan int64, 1)
	err := a.conn.WriteMessage(websocket.TextMessage, msg)
	pongCh := a.pongCh
	a.mu.Unlock()
	if err != nil {
		return 0, err
	}

	select {
	case <-pongCh:
		return time.Since(time.UnixMilli(ts)), nil
	case <-time.After(timeout):
		return 0, fmt.Errorf("ping timeout")
	}
}

// DispatchStatusInfo is the structured status report.
type DispatchStatusInfo struct {
	Total  int               `json:"total"`
	Agents []AgentStatusInfo `json:"agents"`
}

// AgentStatusInfo is per-agent status.
type AgentStatusInfo struct {
	UserID      string    `json:"user_id"`
	ConnectedAt time.Time `json:"connected_at"`
	IP          string    `json:"ip"`
	Version     string    `json:"version"`
	Online      bool      `json:"online"`
}

// Status returns current dispatch status.
func (d *Dispatcher) Status() DispatchStatusInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	info := DispatchStatusInfo{Total: len(d.agents)}
	for id, agent := range d.agents {
		info.Agents = append(info.Agents, AgentStatusInfo{
			UserID:      id,
			ConnectedAt: agent.meta.ConnectedAt,
			IP:          agent.meta.IP,
			Version:     agent.meta.Version,
			Online:      agent.connAlive.Load(),
		})
	}
	return info
}
