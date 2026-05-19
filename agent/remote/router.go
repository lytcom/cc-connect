// router.go — Integration point for remote dispatch in cc-connect engine.
// Provides the routing decision function that engine.go calls before processInteractiveMessageWith.
package remote

import (
	"log"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

// Router integrates remote dispatch into the cc-connect message flow.
type Router struct {
	dispatcher *Dispatcher
	state      *RoutingStateStore
	limiter    *RateLimiter
}

// NewRouter creates a Router with all Phase 1 components.
func NewRouter(dispatcher *Dispatcher, stateFile string) *Router {
	return &Router{
		dispatcher: dispatcher,
		state:      NewRoutingStateStore(stateFile),
		limiter:    NewRateLimiter(DefaultMaxMsgPerSec),
	}
}

// RouteMessage decides whether a message should go to a remote agent.
// Returns the RemoteAgent if routing remotely, or nil for local execution.
//
// This is the ONE function that engine.go calls. Minimal invasion.
func (r *Router) RouteMessage(sessionKey, userID string) core.Agent {
	// Check routing target for this session
	target := r.state.GetTarget(sessionKey)
	if target != TargetLocal {
		return nil // use local agent
	}

	// Check if the user's remote agent is online
	agent := r.dispatcher.GetAgent(userID)
	if agent == nil {
		return nil // offline, fallback to local
	}

	log.Printf("[remote-router] routing to remote agent: user=%s session=%s", userID, sessionKey)
	return agent
}

// HandleSwitchCommand processes /local and /server switch commands.
// Returns true if the message was a switch command (consumed).
func (r *Router) HandleSwitchCommand(sessionKey, content string) (handled bool, reply string) {
	lower := strings.TrimSpace(strings.ToLower(content))

	switch {
	case lower == "/local" || lower == "切到我的电脑" || lower == "switch to local" ||
		strings.HasPrefix(lower, "@local"):
		r.state.SetTarget(sessionKey, TargetLocal)
		return true, "已切换到你的个人电脑执行"

	case lower == "/server" || lower == "切到公用机" || lower == "switch to server" ||
		strings.HasPrefix(lower, "@server"):
		r.state.SetTarget(sessionKey, TargetServer)
		return true, "已切换到公用机执行"
	}

	return false, ""
}

// IsOnline checks if a user's agent is connected.
func (r *Router) IsOnline(userID string) bool {
	return r.dispatcher.IsOnline(userID)
}

// GetState returns the current routing state store (for admin monitoring).
func (r *Router) GetState() *RoutingStateStore {
	return r.state
}

// GetDispatcher returns the dispatcher (for admin operations).
func (r *Router) GetDispatcher() *Dispatcher {
	return r.dispatcher
}
