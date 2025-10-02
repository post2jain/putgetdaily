package workload

import (
	"encoding/json"
	"math/rand"
	"os"
	"sync"
	"time"
)

// WeightedOperation describes a single operation choice with optional metadata.
type WeightedOperation struct {
	Operation string            `json:"operation"`
	Weight    int               `json:"weight"`
	Options   map[string]string `json:"options,omitempty"`
}

// Spec is the JSON layout accepted for workload files.
type Spec struct {
	Operations []WeightedOperation `json:"operations"`
}

// Selector randomly picks operations based on weights.
type Selector struct {
	operations []WeightedOperation
	cumulative []int
	total      int
	rng        *rand.Rand
	mu         sync.Mutex
}

// Load reads and validates a workload specification from disk.
func Load(path string) (*Selector, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return FromJSON(data)
}

// FromJSON creates a selector from raw JSON bytes.
func FromJSON(data []byte) (*Selector, error) {
	var spec Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return NewSelector(spec.Operations)
}

// NewSelector constructs a selector directly from operations.
func NewSelector(ops []WeightedOperation) (*Selector, error) {
	filtered := make([]WeightedOperation, 0, len(ops))
	cumulative := make([]int, 0, len(ops))
	total := 0
	for _, op := range ops {
		if op.Weight <= 0 || op.Operation == "" {
			continue
		}
		total += op.Weight
		filtered = append(filtered, op)
		cumulative = append(cumulative, total)
	}
	sel := &Selector{
		operations: filtered,
		cumulative: cumulative,
		total:      total,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return sel, nil
}

// Pick returns the next weighted operation.
func (s *Selector) Pick() WeightedOperation {
	if s == nil || len(s.operations) == 0 || s.total == 0 {
		return WeightedOperation{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.rng.Intn(s.total)
	for i, upper := range s.cumulative {
		if target < upper {
			return s.operations[i]
		}
	}
	return s.operations[len(s.operations)-1]
}
