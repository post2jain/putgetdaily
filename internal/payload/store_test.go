package payload

import (
	"path/filepath"
	"testing"
)

func TestMemoryStoreRoundTrip(t *testing.T) {
	store := NewMemoryStore()
	rec := Record{Digest: []byte{1, 2, 3}, Size: 42}
	if err := store.Remember("key1", rec); err != nil {
		t.Fatalf("remember err: %v", err)
	}
	got, ok, err := store.Lookup("key1")
	if err != nil || !ok {
		t.Fatalf("lookup err=%v ok=%v", err, ok)
	}
	if got.Size != rec.Size || string(got.Digest) != string(rec.Digest) {
		t.Fatalf("unexpected record: %+v", got)
	}
	if err := store.Forget("key1"); err != nil {
		t.Fatalf("forget err: %v", err)
	}
	if _, ok, _ = store.Lookup("key1"); ok {
		t.Fatalf("expected record removed")
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore err: %v", err)
	}
	defer store.Close()

	rec := Record{Digest: []byte("digest"), Size: 100}
	if err := store.Remember("bucket/object", rec); err != nil {
		t.Fatalf("remember err: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*", "*", "*.json"))
	if err != nil {
		t.Fatalf("glob err: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 checksum file, got %v", matches)
	}
	got, ok, err := store.Lookup("bucket/object")
	if err != nil || !ok {
		t.Fatalf("lookup err=%v ok=%v", err, ok)
	}
	if got.Size != rec.Size || string(got.Digest) != string(rec.Digest) {
		t.Fatalf("unexpected record: %+v", got)
	}
	if err := store.Forget("bucket/object"); err != nil {
		t.Fatalf("forget err: %v", err)
	}
}
