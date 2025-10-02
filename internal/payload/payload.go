package payload

import (
	"crypto/sha256"
	"hash"
	"io"
)

// Reader produces deterministic bytes for a given key and size.
type Reader struct {
	keyHash [32]byte
	size    int64
	offset  int64
}

// Size returns the total payload size.
func (r *Reader) Size() int64 {
	return r.size
}

// NewReader constructs a deterministic reader for the provided key.
func NewReader(key string, size int64) *Reader {
	sum := sha256.Sum256([]byte(key))
	return &Reader{keyHash: sum, size: size}
}

// Read fills p with pseudo-random bytes derived from the key hash.
func (r *Reader) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}
	remaining := r.size - r.offset
	if int64(len(p)) > remaining {
		p = p[:int(remaining)]
	}
	n := 0
	for n < len(p) {
		idx := ((r.offset + int64(n)) % int64(len(r.keyHash)))
		p[n] = r.keyHash[idx]
		n++
	}
	r.offset += int64(n)
	return n, nil
}

// Reset allows re-reading from the beginning.
func (r *Reader) Reset() {
	r.offset = 0
}

// Seek repositions the reader.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.size + offset
	default:
		return 0, io.EOF
	}
	if newOffset < 0 {
		newOffset = 0
	}
	if newOffset > r.size {
		newOffset = r.size
	}
	r.offset = newOffset
	return r.offset, nil
}

// Digest computes a SHA256 checksum for the deterministic payload.
func Digest(key string, size int64) []byte {
	rdr := NewReader(key, size)
	hasher := sha256.New()
	copyStream(hasher, rdr)
	return hasher.Sum(nil)
}

func copyStream(dst hash.Hash, src io.Reader) {
	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			dst.Write(buf[:n])
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
	}
}
