package remote

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mockTokenValidator for testing
type mockTokenValidator struct {
	tokens map[string]string // user_id → token
}

func (m *mockTokenValidator) ValidateToken(userID, token string) bool {
	return m.tokens[userID] == token
}

func (m *mockTokenValidator) GetToken(userID string) string {
	return m.tokens[userID]
}

func TestChallengeResponseSuccess(t *testing.T) {
	validator := &mockTokenValidator{
		tokens: map[string]string{"ou_user_001": "secret-token-123"},
	}

	// Server side
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()

		userID, version, err := PerformServerChallenge(conn, validator, 5*time.Second)
		if err != nil {
			t.Fatalf("server challenge failed: %v", err)
		}
		if userID != "ou_user_001" {
			t.Fatalf("expected ou_user_001, got %s", userID)
		}
		if version != "1.0.0" {
			t.Fatalf("expected version 1.0.0, got %s", version)
		}

		// Send auth_result
		result := AuthResult{Type: "auth_result", Status: "ok"}
		data, _ := json.Marshal(result)
		conn.WriteMessage(websocket.TextMessage, data)
	}))
	defer server.Close()

	// Client side
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	err = PerformClientAuth(conn, "ou_user_001", "secret-token-123", "1.0.0", 5*time.Second)
	if err != nil {
		t.Fatalf("client auth failed: %v", err)
	}

	t.Log("✅ Challenge-response handshake succeeded")
}

func TestChallengeResponseWrongToken(t *testing.T) {
	validator := &mockTokenValidator{
		tokens: map[string]string{"ou_user_001": "correct-token"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, _, err = PerformServerChallenge(conn, validator, 5*time.Second)
		if err == nil {
			t.Fatal("expected auth to fail with wrong token")
		}

		result := AuthResult{Type: "auth_result", Status: "error", Message: err.Error()}
		data, _ := json.Marshal(result)
		conn.WriteMessage(websocket.TextMessage, data)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer conn.Close()

	err := PerformClientAuth(conn, "ou_user_001", "wrong-token", "1.0.0", 5*time.Second)
	if err == nil {
		t.Fatal("expected auth to fail")
	}

	t.Logf("✅ Wrong token correctly rejected: %v", err)
}

func TestChallengeResponseUnknownUser(t *testing.T) {
	validator := &mockTokenValidator{
		tokens: map[string]string{"ou_user_001": "token"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, _, err = PerformServerChallenge(conn, validator, 5*time.Second)
		if err == nil {
			t.Fatal("expected auth to fail for unknown user")
		}

		result := AuthResult{Type: "auth_result", Status: "error", Message: err.Error()}
		data, _ := json.Marshal(result)
		conn.WriteMessage(websocket.TextMessage, data)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer conn.Close()

	err := PerformClientAuth(conn, "ou_unknown", "any-token", "1.0.0", 5*time.Second)
	if err == nil {
		t.Fatal("expected auth to fail for unknown user")
	}

	t.Logf("✅ Unknown user correctly rejected: %v", err)
}

func TestHMACComputation(t *testing.T) {
	token := "my-secret"
	nonce := "abc123"
	timestamp := int64(1700000000)

	sig1 := computeHMAC(token, nonce, timestamp)
	sig2 := computeHMAC(token, nonce, timestamp)
	sig3 := computeHMAC("wrong-token", nonce, timestamp)

	if sig1 != sig2 {
		t.Fatal("same inputs should produce same HMAC")
	}
	if sig1 == sig3 {
		t.Fatal("different tokens should produce different HMAC")
	}
	if !verifyHMAC(token, nonce, timestamp, sig1) {
		t.Fatal("verify should pass for correct signature")
	}
	if verifyHMAC(token, nonce, timestamp, sig3) {
		t.Fatal("verify should fail for wrong signature")
	}

	t.Log("✅ HMAC computation and verification correct")
}
