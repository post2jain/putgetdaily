package config

import (
	osx "os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseConfigFileAndEnv(t *testing.T) {
	t.Setenv("S3LOAD_ACCESS_KEY", "envAK")
	t.Setenv("S3LOAD_SECRET_KEY", "envSK")
	t.Setenv("S3LOAD_BUCKET", "envbucket")

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	fileContents := `{
		"endpoints": ["http://localhost:9000"],
		"tps": 5,
		"duration": "45s",
		"access_key": "fileAK",
		"secret_key": "fileSK"
	}`
	if err := osx.WriteFile(cfgPath, []byte(fileContents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Parse([]string{"--config", cfgPath})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if cfg.AccessKey != "envAK" || cfg.SecretKey != "envSK" {
		t.Fatalf("expected env credentials, got %q/%q", cfg.AccessKey, cfg.SecretKey)
	}
	if cfg.Bucket != "envbucket" {
		t.Fatalf("expected bucket override from env, got %q", cfg.Bucket)
	}
	if cfg.TargetTPS != 5 {
		t.Fatalf("expected TPS=5, got %v", cfg.TargetTPS)
	}
	if cfg.Duration != 45*time.Second {
		t.Fatalf("expected duration 45s, got %v", cfg.Duration)
	}
	if cfg.Metadata == nil {
		t.Fatalf("expected metadata map initialised")
	}
	if cfg.KeyPrefix != "testobject" {
		t.Fatalf("expected default key prefix, got %s", cfg.KeyPrefix)
	}
	if cfg.Snapshot()["dry_run"].(bool) {
		t.Fatalf("dry_run should be false by default")
	}
}

func TestParseDurationRequestsConflict(t *testing.T) {
	t.Setenv("S3LOAD_ACCESS_KEY", "a")
	t.Setenv("S3LOAD_SECRET_KEY", "b")
	_, err := Parse([]string{"--duration", "10", "--requests", "5"})
	if err == nil {
		t.Fatalf("expected error for conflicting duration and requests")
	}
}

func TestParseDryRunFlag(t *testing.T) {
	t.Setenv("S3LOAD_ACCESS_KEY", "a")
	t.Setenv("S3LOAD_SECRET_KEY", "b")
	cfg, err := Parse([]string{"--dry-run"})
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if !cfg.DryRun {
		t.Fatalf("expected dry-run flag to set DryRun true")
	}
}
