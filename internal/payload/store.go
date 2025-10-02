package payload

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Record captures payload metadata for integrity verification.
type Record struct {
	Digest []byte `json:"digest"`
	Size   int64  `json:"size"`
}

// Store defines the interface for checksum persistence.
type Store interface {
	Remember(key string, record Record) error
	Lookup(key string) (Record, bool, error)
	Forget(key string) error
	Close() error
}

// MemoryStore keeps records entirely in memory.
type MemoryStore struct {
	payloads sync.Map
}

// NewMemoryStore creates an in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// Remember stores the provided record.
func (m *MemoryStore) Remember(key string, record Record) error {
	if len(record.Digest) == 0 {
		return nil
	}
	copyRecord := Record{
		Digest: append([]byte(nil), record.Digest...),
		Size:   record.Size,
	}
	m.payloads.Store(key, copyRecord)
	return nil
}

// Lookup fetches a record if present.
func (m *MemoryStore) Lookup(key string) (Record, bool, error) {
	val, ok := m.payloads.Load(key)
	if !ok {
		return Record{}, false, nil
	}
	rec := val.(Record)
	rec.Digest = append([]byte(nil), rec.Digest...)
	return rec, true, nil
}

// Forget removes the record.
func (m *MemoryStore) Forget(key string) error {
	m.payloads.Delete(key)
	return nil
}

// Close implements Store.
func (m *MemoryStore) Close() error { return nil }

// FileStore persists records on disk without retaining them in memory.
type FileStore struct {
	baseDir string
	mu      sync.Mutex
}

// NewFileStore initialises a filesystem-backed store.
func NewFileStore(baseDir string) (*FileStore, error) {
	if baseDir == "" {
		return nil, errors.New("baseDir is required")
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create checksum directory: %w", err)
	}
	return &FileStore{baseDir: baseDir}, nil
}

// Remember writes or updates the record on disk.
func (f *FileStore) Remember(key string, record Record) error {
	if len(record.Digest) == 0 {
		return nil
	}
	path := f.pathForKey(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("ensure checksum directory: %w", err)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal checksum record: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write checksum temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace checksum file: %w", err)
	}
	return nil
}

// Lookup reads the record from disk.
func (f *FileStore) Lookup(key string) (Record, bool, error) {
	path := f.pathForKey(key)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("read checksum file: %w", err)
	}
	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		return Record{}, false, fmt.Errorf("decode checksum file: %w", err)
	}
	return rec, true, nil
}

// Forget deletes the record file.
func (f *FileStore) Forget(key string) error {
	path := f.pathForKey(key)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove checksum file: %w", err)
	}
	return nil
}

// Close implements Store.
func (f *FileStore) Close() error { return nil }

func (f *FileStore) pathForKey(key string) string {
	hash := sha1.Sum([]byte(key))
	hexDigest := hex.EncodeToString(hash[:])
	// Use a two-level directory structure to limit files per directory.
	subDir := filepath.Join(f.baseDir, hexDigest[:2], hexDigest[2:4])
	sanitized := sanitizeKey(key)
	fileName := hexDigest + "_" + sanitized + ".json"
	return filepath.Join(subDir, fileName)
}

func sanitizeKey(key string) string {
	replaced := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, key)
	if replaced == "" {
		return "key"
	}
	if len(replaced) > 24 {
		return replaced[:24]
	}
	return replaced
}
