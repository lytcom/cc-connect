package remote

import (
	"encoding/json"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestParseEnvelope_RawStream_TextEvent(t *testing.T) {
	payload := map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"content": []any{
				map[string]any{"type": "text", "text": "hello world"},
			},
		},
	}
	payloadBytes, _ := json.Marshal(payload)
	env := WireEnvelope{
		Type:      "raw_stream",
		SessionID: "sess-001",
		Done:      false,
		Payload:   payloadBytes,
	}

	evt := parseEvent(env)

	if evt.Type != core.EventText {
		t.Errorf("want EventText, got %s", evt.Type)
	}
	if evt.Content != "hello world" {
		t.Errorf("want 'hello world', got %q", evt.Content)
	}
	if evt.Done {
		t.Error("want done=false")
	}
}

func TestParseEnvelope_RawStream_ResultEvent(t *testing.T) {
	payload := map[string]any{
		"type":       "result",
		"result":     "final answer",
		"session_id": "sess-001",
		"usage":      map[string]any{"input_tokens": float64(100), "output_tokens": float64(50)},
	}
	payloadBytes, _ := json.Marshal(payload)
	env := WireEnvelope{
		Type:      "raw_stream",
		SessionID: "sess-001",
		Done:      true,
		Payload:   payloadBytes,
	}

	evt := parseEvent(env)

	if evt.Type != core.EventResult {
		t.Errorf("want EventResult, got %s", evt.Type)
	}
	if evt.Content != "final answer" {
		t.Errorf("want 'final answer', got %q", evt.Content)
	}
	if !evt.Done {
		t.Error("want done=true")
	}
	if evt.InputTokens != 100 {
		t.Errorf("want input_tokens=100, got %d", evt.InputTokens)
	}
	if evt.OutputTokens != 50 {
		t.Errorf("want output_tokens=50, got %d", evt.OutputTokens)
	}
}

func TestParseEnvelope_RawStream_ToolUse(t *testing.T) {
	payload := map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"content": []any{
				map[string]any{"type": "tool_use", "name": "Bash", "input": map[string]any{"command": "ls"}},
			},
		},
	}
	payloadBytes, _ := json.Marshal(payload)
	env := WireEnvelope{
		Type:      "raw_stream",
		SessionID: "sess-001",
		Done:      false,
		Payload:   payloadBytes,
	}

	evt := parseEvent(env)

	if evt.Type != core.EventToolUse {
		t.Errorf("want EventToolUse, got %s", evt.Type)
	}
	if evt.ToolName != "Bash" {
		t.Errorf("want ToolName='Bash', got %q", evt.ToolName)
	}
}

func TestParseEnvelope_RawStream_Thinking(t *testing.T) {
	payload := map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"content": []any{
				map[string]any{"type": "thinking", "thinking": "let me consider..."},
			},
		},
	}
	payloadBytes, _ := json.Marshal(payload)
	env := WireEnvelope{
		Type:      "raw_stream",
		SessionID: "sess-001",
		Done:      false,
		Payload:   payloadBytes,
	}

	evt := parseEvent(env)

	if evt.Type != core.EventThinking {
		t.Errorf("want EventThinking, got %s", evt.Type)
	}
	if evt.Content != "let me consider..." {
		t.Errorf("want thinking content, got %q", evt.Content)
	}
}

func TestParseEnvelope_Error(t *testing.T) {
	env := WireEnvelope{
		Type:      "error",
		SessionID: "sess-001",
		ErrorMsg:  "claude code produced no output within 60s",
		Done:      true,
	}

	evt := parseEvent(env)

	if evt.Type != core.EventError {
		t.Errorf("want EventError, got %s", evt.Type)
	}
	if evt.Error == nil || evt.Error.Error() != "claude code produced no output within 60s" {
		t.Errorf("want error message, got %v", evt.Error)
	}
	if !evt.Done {
		t.Error("want done=true")
	}
}

func TestParseEnvelope_Ack(t *testing.T) {
	env := WireEnvelope{
		Type:     "ack",
		PromptID: "prompt-123",
	}

	evt := parseEvent(env)

	if evt.Type != EventAck {
		t.Errorf("want EventAck, got %s", evt.Type)
	}
	if evt.RequestID != "prompt-123" {
		t.Errorf("want RequestID='prompt-123', got %q", evt.RequestID)
	}
}

func TestParseEnvelope_LegacyWireEvent(t *testing.T) {
	// Old format: top-level type/content/done fields
	raw, _ := json.Marshal(map[string]any{
		"type":    "text",
		"content": "legacy message",
		"done":    false,
	})
	env := WireEnvelope{Type: "text", Raw: raw}

	evt := parseEvent(env)

	if evt.Type != core.EventText {
		t.Errorf("want EventText, got %s", evt.Type)
	}
	if evt.Content != "legacy message" {
		t.Errorf("want 'legacy message', got %q", evt.Content)
	}
}

func TestParseEnvelope_MalformedPayload(t *testing.T) {
	env := WireEnvelope{
		Type:      "raw_stream",
		SessionID: "sess-001",
		Done:      false,
		Payload:   []byte("not valid json{{{"),
	}

	evt := parseEvent(env)

	// Should not panic, return empty event or error
	if evt.Type == "" {
		evt.Type = core.EventText // acceptable fallback
	}
}

func TestParseEnvelope_SystemInit(t *testing.T) {
	payload := map[string]any{
		"type":       "system",
		"session_id": "new-session-id-abc",
	}
	payloadBytes, _ := json.Marshal(payload)
	env := WireEnvelope{
		Type:      "raw_stream",
		SessionID: "",
		Done:      false,
		Payload:   payloadBytes,
	}

	evt := parseEvent(env)

	if evt.Type != core.EventText {
		t.Errorf("want EventText for system init, got %s", evt.Type)
	}
	if evt.SessionID != "new-session-id-abc" {
		t.Errorf("want session_id propagated, got %q", evt.SessionID)
	}
}
