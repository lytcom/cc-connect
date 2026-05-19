// auth.go — Challenge-response authentication for remote agents.
// Prevents token replay attacks by requiring HMAC signature over a server-generated nonce.
package remote

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// MaxTimestampDrift is the maximum allowed time difference between server and agent.
	MaxTimestampDrift = 30 * time.Second
	// NonceLength is the byte length of the random challenge nonce.
	NonceLength = 32
)

// ChallengeMessage is sent by the server after WebSocket upgrade.
type ChallengeMessage struct {
	Type      string `json:"type"`       // "challenge"
	Nonce     string `json:"nonce"`      // hex-encoded random bytes
	Timestamp int64  `json:"timestamp"`  // server unix timestamp
}

// AuthResponse is sent by the agent in response to the challenge.
type AuthResponse struct {
	Type      string `json:"type"`       // "auth"
	UserID    string `json:"user_id"`
	Signature string `json:"signature"`  // HMAC-SHA256(token, nonce + timestamp_str)
	Timestamp int64  `json:"timestamp"`  // agent's unix timestamp
	Version   string `json:"version"`
}

// AuthResult is sent by the server after verifying the auth response.
type AuthResult struct {
	Type    string `json:"type"`    // "auth_result"
	Status  string `json:"status"`  // "ok" or "error"
	Message string `json:"message,omitempty"`
}

// generateNonce creates a cryptographically random nonce.
func generateNonce() (string, error) {
	b := make([]byte, NonceLength)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// computeHMAC computes HMAC-SHA256(token, nonce + timestamp).
func computeHMAC(token, nonce string, timestamp int64) string {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(fmt.Sprintf("%s%d", nonce, timestamp)))
	return hex.EncodeToString(mac.Sum(nil))
}

// verifyHMAC checks if the signature matches.
func verifyHMAC(token, nonce string, timestamp int64, signature string) bool {
	expected := computeHMAC(token, nonce, timestamp)
	return hmac.Equal([]byte(expected), []byte(signature))
}

// TokenValidator validates a user_id + token pair against the allowed list.
type TokenValidator interface {
	ValidateToken(userID, token string) bool
	GetToken(userID string) string
}

// PerformServerChallenge executes the server side of the challenge-response handshake.
// Returns the authenticated user_id or an error.
func PerformServerChallenge(conn *websocket.Conn, validator TokenValidator, timeout time.Duration) (string, string, error) {
	// 1. Generate and send challenge
	nonce, err := generateNonce()
	if err != nil {
		return "", "", fmt.Errorf("generate nonce: %w", err)
	}

	challenge := ChallengeMessage{
		Type:      "challenge",
		Nonce:     nonce,
		Timestamp: time.Now().Unix(),
	}
	data, _ := json.Marshal(challenge)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return "", "", fmt.Errorf("send challenge: %w", err)
	}

	// 2. Read auth response (with timeout)
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, msg, err := conn.ReadMessage()
	conn.SetReadDeadline(time.Time{}) // reset
	if err != nil {
		return "", "", fmt.Errorf("read auth response: %w", err)
	}

	var authResp AuthResponse
	if err := json.Unmarshal(msg, &authResp); err != nil || authResp.Type != "auth" {
		return "", "", fmt.Errorf("invalid auth response")
	}

	// 3. Validate timestamp drift
	drift := time.Duration(math.Abs(float64(time.Now().Unix()-authResp.Timestamp))) * time.Second
	if drift > MaxTimestampDrift {
		return "", "", fmt.Errorf("timestamp drift too large: %v", drift)
	}

	// 4. Get the expected token for this user
	expectedToken := validator.GetToken(authResp.UserID)
	if expectedToken == "" {
		return "", "", fmt.Errorf("unknown user: %s", authResp.UserID)
	}

	// 5. Verify HMAC signature
	if !verifyHMAC(expectedToken, nonce, authResp.Timestamp, authResp.Signature) {
		return "", "", fmt.Errorf("invalid signature")
	}

	return authResp.UserID, authResp.Version, nil
}

// PerformClientAuth executes the agent side of the challenge-response handshake.
func PerformClientAuth(conn *websocket.Conn, userID, token, version string, timeout time.Duration) error {
	// 1. Read challenge from server
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, msg, err := conn.ReadMessage()
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("read challenge: %w", err)
	}

	var challenge ChallengeMessage
	if err := json.Unmarshal(msg, &challenge); err != nil || challenge.Type != "challenge" {
		return fmt.Errorf("invalid challenge message")
	}

	// 2. Compute HMAC signature
	timestamp := time.Now().Unix()
	signature := computeHMAC(token, challenge.Nonce, timestamp)

	// 3. Send auth response
	authResp := AuthResponse{
		Type:      "auth",
		UserID:    userID,
		Signature: signature,
		Timestamp: timestamp,
		Version:   version,
	}
	data, _ := json.Marshal(authResp)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	// 4. Read result
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, resultMsg, err := conn.ReadMessage()
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("read auth result: %w", err)
	}

	var result AuthResult
	if err := json.Unmarshal(resultMsg, &result); err != nil {
		return fmt.Errorf("invalid auth result")
	}
	if result.Status != "ok" {
		return fmt.Errorf("auth failed: %s", result.Message)
	}

	return nil
}
