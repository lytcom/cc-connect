package remote

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCredentialSaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewCredentialStoreAt(tmpDir)

	creds := &Credentials{
		UserID:    "ou_test_001",
		Token:     "secret-token-xyz",
		ServerURL: "wss://10.0.0.1:8901",
	}

	// Save
	err := store.Save(creds)
	if err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Verify file exists with correct permissions
	path := filepath.Join(tmpDir, CredFileName)
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 permissions, got %o", info.Mode().Perm())
	}

	// Load
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.UserID != "ou_test_001" {
		t.Fatalf("user_id mismatch: %s", loaded.UserID)
	}
	if loaded.Token != "secret-token-xyz" {
		t.Fatalf("token mismatch")
	}
	if loaded.ServerURL != "wss://10.0.0.1:8901" {
		t.Fatalf("server_url mismatch")
	}

	t.Log("✅ Credential save/load with secure file permissions")
}

func TestCredentialInsecurePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewCredentialStoreAt(tmpDir)

	creds := &Credentials{UserID: "u", Token: "t", ServerURL: "s"}
	store.Save(creds)

	// Make file world-readable (insecure)
	path := filepath.Join(tmpDir, CredFileName)
	os.Chmod(path, 0644)

	_, err := store.Load()
	if err == nil {
		t.Fatal("should reject insecure file permissions")
	}
	t.Logf("✅ Insecure permissions rejected: %v", err)
}

func TestCredentialMissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewCredentialStoreAt(tmpDir)

	_, err := store.Load()
	if err == nil {
		t.Fatal("should fail when file doesn't exist")
	}
	t.Logf("✅ Missing file detected: %v", err)
}

func TestCredentialFromEnv(t *testing.T) {
	os.Setenv("AD_BOT_USER_ID", "ou_env_user")
	os.Setenv("AD_BOT_TOKEN", "env-token")
	os.Setenv("AD_BOT_SERVER_URL", "wss://env-server:8901")
	defer func() {
		os.Unsetenv("AD_BOT_USER_ID")
		os.Unsetenv("AD_BOT_TOKEN")
		os.Unsetenv("AD_BOT_SERVER_URL")
	}()

	creds, err := LoadFromEnvOrFile()
	if err != nil {
		t.Fatalf("env load failed: %v", err)
	}
	if creds.UserID != "ou_env_user" {
		t.Fatalf("expected ou_env_user, got %s", creds.UserID)
	}
	t.Log("✅ Credentials loaded from environment variables")
}
