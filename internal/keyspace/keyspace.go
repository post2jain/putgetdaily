package keyspace

import (
	"bufio"
	"errors"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Generator yields object keys for operations.
type Generator struct {
	keys      []string
	prefix    string
	prefixes  []string
	counter   atomic.Uint64
	strategy  Strategy
	randSrc   *rand.Rand
	zipf      *rand.Zipf
	zipfIMax  uint64
	randGuard sync.Mutex
}

// Options configures the generator behaviour.
type Options struct {
	ObjectListFile string
	Prefix         string
	RandomStart    bool
	Strategy       string
	Prefixes       []string
	ZipfS          float64
	ZipfV          float64
	ZipfIMax       uint64
}

// Strategy describes key selection behaviour.
type Strategy int

const (
	strategySequential Strategy = iota
	strategyRandom
	strategyZipf
)

// New creates a key generator using either a static list or synthetic names.
func New(opts Options) (*Generator, error) {
	strat := parseStrategy(opts.Strategy)
	if strat == strategyZipf && opts.ZipfIMax == 0 && opts.ObjectListFile == "" {
		opts.ZipfIMax = 1 << 20
	}

	src := rand.New(rand.NewSource(time.Now().UnixNano()))

	gen := &Generator{
		prefix:   opts.Prefix,
		prefixes: opts.Prefixes,
		strategy: strat,
		randSrc:  src,
		zipfIMax: opts.ZipfIMax,
	}
	if opts.ObjectListFile != "" {
		list, err := loadKeys(opts.ObjectListFile)
		if err != nil {
			return nil, err
		}
		if len(list) == 0 {
			return nil, errors.New("object list file is empty")
		}
		gen.keys = list
		if strat == strategyZipf {
			imax := opts.ZipfIMax
			if imax == 0 || imax >= uint64(len(list)) {
				imax = uint64(len(list)) - 1
			}
			gen.zipf = rand.NewZipf(src, opts.ZipfS, opts.ZipfV, imax)
		}
	}
	if strat == strategyZipf && gen.zipf == nil {
		imax := opts.ZipfIMax
		if imax == 0 {
			imax = 1<<32 - 1
		}
		gen.zipf = rand.NewZipf(src, opts.ZipfS, opts.ZipfV, imax)
	}

	if opts.RandomStart && strat == strategySequential {
		rand.Seed(time.Now().UnixNano())
		start := rand.Int63n(1 << 32)
		gen.counter.Store(uint64(start))
	}
	return gen, nil
}

// Next returns the next key in sequence.
func (g *Generator) Next() string {
	switch g.strategy {
	case strategySequential:
		idx := g.counter.Add(1) - 1
		if len(g.keys) > 0 {
			return g.keys[idx%uint64(len(g.keys))]
		}
		return g.synthetic(idx)
	case strategyRandom:
		return g.randomKey()
	case strategyZipf:
		return g.zipfKey()
	default:
		idx := g.counter.Add(1) - 1
		if len(g.keys) > 0 {
			return g.keys[idx%uint64(len(g.keys))]
		}
		return g.synthetic(idx)
	}
}

func (g *Generator) randomKey() string {
	g.randGuard.Lock()
	defer g.randGuard.Unlock()
	if len(g.keys) > 0 {
		idx := g.randSrc.Intn(len(g.keys))
		return g.keys[idx]
	}
	return g.syntheticUint64(uint64(g.randSrc.Int63()))
}

func (g *Generator) zipfKey() string {
	if g.zipf == nil {
		return g.randomKey()
	}
	g.randGuard.Lock()
	idx := g.zipf.Uint64()
	g.randGuard.Unlock()
	if len(g.keys) > 0 {
		if int(idx) < len(g.keys) {
			return g.keys[idx]
		}
		return g.keys[int(idx)%len(g.keys)]
	}
	return g.syntheticUint64(idx)
}

func (g *Generator) synthetic(idx uint64) string {
	prefix := g.pickPrefix()
	if prefix == "" {
		prefix = "object"
	}
	return prefix + "/" + formatNumber(idx)
}

func (g *Generator) syntheticUint64(idx uint64) string {
	prefix := g.pickPrefix()
	if prefix == "" {
		prefix = "object"
	}
	return prefix + "/" + formatNumber(idx)
}

func (g *Generator) pickPrefix() string {
	if len(g.prefixes) == 0 {
		return g.prefix
	}
	g.randGuard.Lock()
	idx := g.randSrc.Intn(len(g.prefixes))
	g.randGuard.Unlock()
	return g.prefixes[idx]
}

func formatNumber(idx uint64) string {
	const width = 16
	digits := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		digits[i] = byte('0' + idx%10)
		idx /= 10
	}
	return string(digits)
}

func parseStrategy(value string) Strategy {
	switch value {
	case "random":
		return strategyRandom
	case "zipf":
		return strategyZipf
	default:
		return strategySequential
	}
}

func loadKeys(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	var keys []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		keys = append(keys, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}
