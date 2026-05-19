// audit.go — Metadata-only audit logging for remote dispatch.
// Logs routing decisions and connection events without logging message content.
package remote

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// AuditEvent types
const (
	AuditConnect    = "agent_connect"
	AuditDisconnect = "agent_disconnect"
	AuditRoute      = "message_routed"
	AuditKick       = "agent_kicked"
	AuditTokenGen   = "token_generated"
	AuditAuthFail   = "auth_failed"
)

// AuditEntry is one audit log record (metadata only, no content).
type AuditEntry struct {
	Timestamp time.Time `json:"ts"`
	Event     string    `json:"event"`
	UserID    string    `json:"user_id"`
	IP        string    `json:"ip,omitempty"`
	Target    string    `json:"target,omitempty"` // "local" or "server"
	SessionID string    `json:"session_id,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

// AuditLogger writes audit entries to a JSONL file.
type AuditLogger struct {
	mu   sync.Mutex
	file *os.File
}

// NewAuditLogger creates an audit logger writing to the specified file.
func NewAuditLogger(filePath string) (*AuditLogger, error) {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	return &AuditLogger{file: f}, nil
}

// Log writes an audit entry.
func (al *AuditLogger) Log(event, userID, ip, target, sessionID, detail string) {
	entry := AuditEntry{
		Timestamp: time.Now(),
		Event:     event,
		UserID:    userID,
		IP:        ip,
		Target:    target,
		SessionID: sessionID,
		Detail:    detail,
	}
	data, _ := json.Marshal(entry)

	al.mu.Lock()
	al.file.Write(append(data, '\n'))
	al.mu.Unlock()
}

// Close closes the audit log file.
func (al *AuditLogger) Close() error {
	return al.file.Close()
}
