// admin_api.go — HTTP admin endpoints for remote dispatch monitoring and management.
// Exposed on the dispatcher's WebSocket port for admin access.
package remote

import (
	"encoding/json"
	"net/http"
	"time"
)

// AgentInfo represents a connected agent's status for admin view.
type AgentInfo struct {
	UserID      string    `json:"user_id"`
	IP          string    `json:"ip"`
	ConnectedAt time.Time `json:"connected_at"`
	Version     string    `json:"version"`
	SessionID   string    `json:"active_session,omitempty"`
}

// AdminAPI provides HTTP handlers for admin operations.
type AdminAPI struct {
	dispatcher *Dispatcher
	state      *RoutingStateStore
}

// NewAdminAPI creates admin API handlers.
func NewAdminAPI(dispatcher *Dispatcher, state *RoutingStateStore) *AdminAPI {
	return &AdminAPI{dispatcher: dispatcher, state: state}
}

// HandleOnline returns all connected agents.
// GET /dispatch/online
func (a *AdminAPI) HandleOnline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	agents := a.dispatcher.OnlineAgentsInfo()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"agents": agents,
		"total":  len(agents),
	})
}

// HandleKick disconnects a specific user's agent.
// POST /dispatch/kick {"user_id": "ou_xxx"}
func (a *AdminAPI) HandleKick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		http.Error(w, "invalid request: user_id required", http.StatusBadRequest)
		return
	}

	if !a.dispatcher.IsOnline(req.UserID) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "not_online"})
		return
	}

	a.dispatcher.RemoveAgent(req.UserID)
	a.state.RemoveUser(req.UserID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "kicked", "user_id": req.UserID})
}

// HandleRoutingState returns current routing state for all users.
// GET /dispatch/state
func (a *AdminAPI) HandleRoutingState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	states := a.state.AllStates()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(states)
}

// RegisterHandlers registers all admin API routes on the given mux.
func (a *AdminAPI) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/dispatch/online", a.HandleOnline)
	mux.HandleFunc("/dispatch/kick", a.HandleKick)
	mux.HandleFunc("/dispatch/state", a.HandleRoutingState)
}
