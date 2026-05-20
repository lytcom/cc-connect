package remote

import (
	"encoding/json"
	"fmt"

	"github.com/chenhg5/cc-connect/core"
)

// EventAck is a custom event type for prompt acknowledgement.
const EventAck core.EventType = "ack"

// WireEnvelope is the envelope format for agent <-> server communication.
type WireEnvelope struct {
	Type      string          `json:"type"`
	SessionID string          `json:"session_id,omitempty"`
	Done      bool            `json:"done,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	ErrorMsg  string          `json:"error,omitempty"`
	PromptID  string          `json:"prompt_id,omitempty"`
	Ts        int64           `json:"ts,omitempty"`
	Raw       json.RawMessage `json:"-"` // original bytes for legacy fallback
}

// parseEvent converts a WireEnvelope into a core.Event.
func parseEvent(env WireEnvelope) core.Event {
	switch env.Type {
	case "raw_stream":
		return parseClaudeStreamJSON(env.Payload, env.Done)
	case "error":
		return core.Event{
			Type:  core.EventError,
			Error: fmt.Errorf("%s", env.ErrorMsg),
			Done:  true,
		}
	case "ack":
		return core.Event{
			Type:      EventAck,
			RequestID: env.PromptID,
		}
	case "pong":
		return core.Event{
			Type:     core.EventText,
			Metadata: map[string]any{"pong_ts": env.Ts},
		}
	default:
		return legacyWireToEvent(env.Raw)
	}
}

// parseClaudeStreamJSON parses Claude Code's stream-json format into core.Event.
func parseClaudeStreamJSON(payload json.RawMessage, envDone bool) core.Event {
	if payload == nil {
		return core.Event{Type: core.EventText, Done: envDone}
	}

	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return core.Event{Type: core.EventText, Done: envDone}
	}

	eventType, _ := raw["type"].(string)

	switch eventType {
	case "system":
		return handleSystemEvent(raw)
	case "assistant":
		return handleAssistantEvent(raw)
	case "result":
		return handleResultEvent(raw)
	default:
		return core.Event{Type: core.EventText, Done: envDone}
	}
}

func handleSystemEvent(raw map[string]any) core.Event {
	sid, _ := raw["session_id"].(string)
	return core.Event{Type: core.EventText, SessionID: sid}
}

func handleAssistantEvent(raw map[string]any) core.Event {
	msg, ok := raw["message"].(map[string]any)
	if !ok {
		return core.Event{Type: core.EventText}
	}
	contentArr, ok := msg["content"].([]any)
	if !ok || len(contentArr) == 0 {
		return core.Event{Type: core.EventText}
	}

	// Process first content item (multiple items handled in sequence by agent)
	item, ok := contentArr[0].(map[string]any)
	if !ok {
		return core.Event{Type: core.EventText}
	}

	contentType, _ := item["type"].(string)
	switch contentType {
	case "text":
		text, _ := item["text"].(string)
		return core.Event{Type: core.EventText, Content: text}
	case "tool_use":
		toolName, _ := item["name"].(string)
		return core.Event{Type: core.EventToolUse, ToolName: toolName}
	case "thinking":
		thinking, _ := item["thinking"].(string)
		return core.Event{Type: core.EventThinking, Content: thinking}
	default:
		return core.Event{Type: core.EventText}
	}
}

func handleResultEvent(raw map[string]any) core.Event {
	content, _ := raw["result"].(string)
	sid, _ := raw["session_id"].(string)

	var inputTokens, outputTokens int
	if usage, ok := raw["usage"].(map[string]any); ok {
		if v, ok := usage["input_tokens"].(float64); ok {
			inputTokens = int(v)
		}
		if v, ok := usage["output_tokens"].(float64); ok {
			outputTokens = int(v)
		}
	}

	return core.Event{
		Type:         core.EventResult,
		Content:      content,
		SessionID:    sid,
		Done:         true,
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
	}
}

// legacyWireToEvent handles the old WireEvent format for backward compatibility.
func legacyWireToEvent(raw json.RawMessage) core.Event {
	if raw == nil {
		return core.Event{Type: core.EventText}
	}
	var w struct {
		Type         string         `json:"type"`
		Content      string         `json:"content"`
		SessionID    string         `json:"session_id"`
		ToolName     string         `json:"tool_name"`
		ToolInput    string         `json:"tool_input"`
		ToolResult   string         `json:"tool_result"`
		RequestID    string         `json:"request_id"`
		Done         bool           `json:"done"`
		Error        string         `json:"error"`
		InputTokens  int            `json:"input_tokens"`
		OutputTokens int            `json:"output_tokens"`
		Metadata     map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return core.Event{Type: core.EventText}
	}

	evt := core.Event{
		Content:      w.Content,
		SessionID:    w.SessionID,
		ToolName:     w.ToolName,
		ToolInput:    w.ToolInput,
		ToolResult:   w.ToolResult,
		RequestID:    w.RequestID,
		Done:         w.Done,
		InputTokens:  w.InputTokens,
		OutputTokens: w.OutputTokens,
		Metadata:     w.Metadata,
	}

	switch w.Type {
	case "text":
		evt.Type = core.EventText
	case "tool_use":
		evt.Type = core.EventToolUse
	case "tool_result":
		evt.Type = core.EventToolResult
	case "result":
		evt.Type = core.EventResult
	case "error":
		evt.Type = core.EventError
		if w.Error != "" {
			evt.Error = fmt.Errorf("%s", w.Error)
		}
	case "thinking":
		evt.Type = core.EventThinking
	case "permission_request":
		evt.Type = core.EventPermissionRequest
	default:
		evt.Type = core.EventText
	}

	return evt
}
