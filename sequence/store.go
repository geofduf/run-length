package sequence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	StatementTypeAddValue uint8 = iota
)

// A Statement represents an operation to perform on a store.
type Statement struct {
	Key                 string
	Value               uint8
	Type                uint8
	CreateIfNotExists   bool
	CreateWithTimestamp time.Time
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

// New creates and intializes a new Sequence using t as its reference timestamp.
// The new Sequence is added to the store using key as its identifier. If a
// Sequence already exists for the identifier it is silently replaced with the new
// Sequence.
func (store *Store) New(t time.Time, key string) {
	store.mu.Lock()
	store.m[key] = NewSequence(t)
	store.mu.Unlock()
}

// Add adds a copy of s to the store using key as its identifier.
// If a Sequence already exists for the identifier it is silently replaced with the new
// Sequence.
func (store *Store) Add(key string, s *Sequence) {
	store.mu.Lock()
	store.m[key] = s.clone()
	store.mu.Unlock()
}

// Get returns a copy of the Sequence associated to key. The second value returned is
// true if the key exists in the store and false if not.
func (store *Store) Get(key string) (*Sequence, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	s, ok := store.m[key]
	if !ok {
		return nil, false
	}
	return s.clone(), true
}

// Execute executes a statement against the store, returning an error if the
// statement cannot be executed or if the underlying operation returned an error.
// It currently only supports statements of type StatementTypeAddValue.
func (store *Store) Execute(statement Statement) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.executeUnsafe(statement)
}

// Batch executes multiple statements against the store. Individual errors are non
// blocking, but if one or more statements could not be executed or induced an error,
// the method will return a global error and a slice holding information about each
// individual error. It currently only supports statements of type StatementTypeAddValue.
func (store *Store) Batch(statements []Statement) (error, []string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var report []string
	for i, v := range statements {
		if err := store.executeUnsafe(v); err != nil {
			report = append(report, fmt.Sprintf("%s, at index %d", err, i))
		}
	}
	if len(report) > 0 {
		return fmt.Errorf("some operations could not be completed"), report
	}
	return nil, report
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
		s.m[key], err = NewSequenceFromBytes(data[i : i+int(v)])
		if err != nil {
			return err
		}
		i += int(v)
	}
	s.mu.Unlock()
	return nil
}

// executeUnsafe executes a statement against the store, returning an error if the
// statement cannot be executed or if the underlying operation returned an error.
// It currently only supports statements of type StatementTypeAddValue. This method
// is not goroutine-safe. The caller is responsible for properly acquiring / releasing
// the lock on the store.
func (store *Store) executeUnsafe(statement Statement) error {
	if statement.Type != StatementTypeAddValue {
		return errors.New("unknown statement type")
	}
	s, ok := store.m[statement.Key]
	if !ok {
		if !statement.CreateIfNotExists {
			return errors.New("key does not exist")
		}
		s = NewSequence(statement.CreateWithTimestamp)
		store.m[statement.Key] = s
	}
	return s.Add(statement.Value)
}
