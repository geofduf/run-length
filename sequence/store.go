package sequence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// Statement types.
const (
	StatementAdd uint8 = iota
	StatementRoll
	statementUnknown
)

// A Statement represents an operation to perform on a store.
type Statement struct {
	Key                 string
	Timestamp           time.Time
	Value               uint8
	Type                uint8
	CreateIfNotExists   bool
	CreateWithTimestamp time.Time
	CreateWithFrequency uint16
	CreateWithLength    uint32
}

// A BatchResult provides detailed information about statements executed in batch.
type BatchResult interface {
	// ErrorVars returns a slice holding the return error of each statement.
	ErrorVars() []error

	// HasErrors returns true if at least one statement induced an error.
	HasErrors() bool
}

// A batchResult holds detailed information about statements executed using
// Store.Batch() and implements the BatchResult interface.
type batchResult struct {
	errors map[int]error
	n      int
}

// ErrorVars implements the BatchResult interface.
func (b batchResult) ErrorVars() []error {
	s := make([]error, b.n)
	for k := range b.errors {
		s[k] = b.errors[k]
	}
	return s
}

// HasErrors implements the BatchResult interface.
func (b batchResult) HasErrors() bool {
	return len(b.errors) > 0
}

// A Store represents a collection of Sequences. A Store can be used simultaneously
// from multiple goroutines.
type Store struct {
	m  map[string]*Sequence
	mu sync.RWMutex
}

// NewStore creates and intializes a new Store.
func NewStore() *Store {
	return &Store{m: make(map[string]*Sequence)}
}

// New creates and adds a new Sequence to the store using key as its identifier. If a
// Sequence already exists for the identifier it is silently replaced with the new
// Sequence.
func (s *Store) New(t time.Time, f uint16, key string) {
	s.mu.Lock()
	s.m[key] = New(t, f)
	s.mu.Unlock()
}

// Add adds a copy of s to the store using key as its identifier.
// If a Sequence already exists for the identifier it is silently replaced with the new
// Sequence.
func (s *Store) Add(key string, x *Sequence) {
	s.mu.Lock()
	s.m[key] = x.clone()
	s.mu.Unlock()
}

// Delete removes key from the store.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

// Get returns a copy of the Sequence associated to key. The second return value is
// true if the key exists in the store and false if not.
func (s *Store) Get(key string) (*Sequence, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x, ok := s.m[key]
	if !ok {
		return nil, false
	}
	return x.clone(), true
}

// Query executes Sequence.Query() on the sequence associated to key, returning an
// error if the key does not exist or if the underlying operation returned an error.
func (s *Store) Query(key string, start time.Time, end time.Time, d time.Duration) (QuerySet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x, ok := s.m[key]
	if !ok {
		return QuerySet{}, errors.New("key does not exist")
	}
	return x.Query(start, end, d)
}

// Execute executes a statement against the store, returning an error if the
// statement cannot be executed or if the underlying operation returned an error.
func (s *Store) Execute(statement Statement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executeUnsafe(statement)
}

// Batch executes multiple statements against the store. Individual errors are non
// blocking but can be inspected through BatchResult.
func (s *Store) Batch(statements []Statement) BatchResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := batchResult{errors: make(map[int]error), n: len(statements)}
	for i, v := range statements {
		if err := s.executeUnsafe(v); err != nil {
			result.errors[i] = err
		}
	}
	return result
}

// Keys returns the identifiers known in the store.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, len(s.m))
	i := 0
	for k := range s.m {
		keys[i] = k
		i++
	}
	return keys
}

// Dump allows to export the store as a slice of bytes.
func (s *Store) Dump() ([]byte, error) {
	var buf bytes.Buffer
	s.mu.RLock()
	container := make([]byte, binary.MaxVarintLen64)
	for k, v := range s.m {
		for _, data := range [][]byte{[]byte(k), v.Bytes()} {
			n := binary.PutVarint(container, int64(len(data)))
			_, err := buf.Write(container[:n])
			if err != nil {
				return nil, err
			}
			_, err = buf.Write(data)
			if err != nil {
				return nil, err
			}
		}
	}
	s.mu.RUnlock()
	return buf.Bytes(), nil
}

// Load loads the content of a store previously exported using the Dump method.
func (s *Store) Load(data []byte) error {
	i := 0
	var err error
	s.mu.Lock()
	s.m = make(map[string]*Sequence)
	for i < len(data) {
		v, n := binary.Varint(data[i:])
		i += n
		key := string(data[i : i+int(v)])
		i += int(v)
		v, n = binary.Varint(data[i:])
		i += n
		s.m[key], err = FromBytes(data[i : i+int(v)])
		if err != nil {
			return err
		}
		i += int(v)
	}
	s.mu.Unlock()
	return nil
}

// Shrink aims at freeing up memory by resetting the store's underlying structures
// to the minimum required capacity. This is mainly useful for frequently updated
// collections of rolling sequences that are kept in memory indefinitely. The operation
// may lead to many allocations and ultimately result in larger memory usage as new
// values are added to the sequences.
func (s *Store) Shrink() {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[string]*Sequence, len(s.m))
	for k := range s.m {
		s.m[k].Shrink()
		m[k] = s.m[k]
	}
	s.m = m
}

// executeUnsafe executes a statement against the store, returning an error if the
// statement cannot be executed or if the underlying operation returned an error.
// This method is not goroutine-safe. The caller is responsible for properly
// acquiring / releasing the lock on the store.
func (s *Store) executeUnsafe(statement Statement) error {
	if statement.Type >= statementUnknown {
		return errors.New("unknown statement type")
	}
	x, ok := s.m[statement.Key]
	if !ok {
		if !statement.CreateIfNotExists {
			return errors.New("key does not exist")
		}
		x = New(statement.CreateWithTimestamp, statement.CreateWithFrequency)
		if statement.CreateWithLength > 0 {
			x.SetLength(statement.CreateWithLength)
		}
		s.m[statement.Key] = x
	}
	var err error
	switch statement.Type {
	case StatementAdd:
		err = x.Add(statement.Timestamp, statement.Value)
	case StatementRoll:
		err = x.Roll(statement.Timestamp, statement.Value)
	}
	return err
}
