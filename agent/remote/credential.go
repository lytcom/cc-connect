// credential.go — Secure credential storage for ad-bot-agent.
// Token is read from a permission-restricted file, never from CLI arguments.
package remote

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const (
	// DefaultCredDir is the directory where credentials are stored.
	DefaultCredDirUnix = ".ad-bot-agent"
	// CredFileName is the credential file name.
	CredFileName = "credentials.json"
	// RequiredFileMode is the required permission for the credential file (owner read/write only).
	RequiredFileMode = 0600
)

// Credentials holds the agent authentication data.
type Credentials struct {
	UserID    string `json:"user_id"`
	Token     string `json:"token"`
	ServerURL string `json:"server_url"`
}

// CredentialStore manages credential file operations.
type CredentialStore struct {
	dir string
}

// NewCredentialStore creates a store at the default location (~/.ad-bot-agent/).
func NewCredentialStore() *CredentialStore {
	home, _ := os.UserHomeDir()
	return &CredentialStore{dir: filepath.Join(home, DefaultCredDirUnix)}
}

// NewCredentialStoreAt creates a store at a specific directory.
func NewCredentialStoreAt(dir string) *CredentialStore {
	return &CredentialStore{dir: dir}
}

// Save writes credentials to the file with restricted permissions.
func (cs *CredentialStore) Save(creds *Credentials) error {
	if err := os.MkdirAll(cs.dir, 0700); err != nil {
		return fmt.Errorf("create credential dir: %w", err)
	}

	data, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal credentials: %w", err)
	}

	path := filepath.Join(cs.dir, CredFileName)
	if err := os.WriteFile(path, data, RequiredFileMode); err != nil {
		return fmt.Errorf("write credential file: %w", err)
	}

	return nil
}

// Load reads credentials from the file, verifying permissions.
func (cs *CredentialStore) Load() (*Credentials, error) {
	path := filepath.Join(cs.dir, CredFileName)

	// Check file exists
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("credential file not found: %s\nRun 'ad-bot-agent init' to configure", path)
		}
		return nil, err
	}

	// Check permissions (Unix only)
	if runtime.GOOS != "windows" {
		mode := info.Mode().Perm()
		if mode != RequiredFileMode {
			return nil, fmt.Errorf("credential file %s has insecure permissions %o (required: %o).\nFix: chmod 600 %s",
				path, mode, RequiredFileMode, path)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credential file: %w", err)
	}

	var creds Credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("parse credential file: %w", err)
	}

	if creds.UserID == "" || creds.Token == "" || creds.ServerURL == "" {
		return nil, fmt.Errorf("credential file incomplete: user_id, token, and server_url are required")
	}

	return &creds, nil
}

// Path returns the full path to the credential file.
func (cs *CredentialStore) Path() string {
	return filepath.Join(cs.dir, CredFileName)
}

// LoadFromEnvOrFile tries environment variable first, then file.
// Env vars: AD_BOT_USER_ID, AD_BOT_TOKEN, AD_BOT_SERVER_URL
func LoadFromEnvOrFile() (*Credentials, error) {
	userID := os.Getenv("AD_BOT_USER_ID")
	token := os.Getenv("AD_BOT_TOKEN")
	serverURL := os.Getenv("AD_BOT_SERVER_URL")

	if userID != "" && token != "" && serverURL != "" {
		return &Credentials{UserID: userID, Token: token, ServerURL: serverURL}, nil
	}

	// Fall back to file
	store := NewCredentialStore()
	return store.Load()
}
