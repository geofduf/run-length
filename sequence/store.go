package sequence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	CreateWithFrequency uint16
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

// New creates and adds a new Sequence to the store using key as its identifier. If a
// Sequence already exists for the identifier it is silently replaced with the new
// Sequence.
func (s *Store) New(t time.Time, f uint16, key string) {
	s.mu.Lock()
	s.m[key] = NewSequence(t, f)
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

// Execute executes a statement against the store, returning an error if the
// statement cannot be executed or if the underlying operation returned an error.
func (s *Store) Execute(statement Statement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executeUnsafe(statement)
}

// Batch executes multiple statements against the store. Individual errors are non
// blocking but if one or more statements could not be executed or induced an error
// the method will return a global error and a slice holding information about each
// individual error.
func (s *Store) Batch(statements []Statement) (error, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var report []string
	for i, v := range statements {
		if err := s.executeUnsafe(v); err != nil {
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
// This method is not goroutine-safe. The caller is responsible for properly
// acquiring / releasing the lock on the store.
func (s *Store) executeUnsafe(statement Statement) error {
	if statement.Type < 0 || statement.Type >= statementUnknown {
		return errors.New("unknown statement type")
	}
	x, ok := s.m[statement.Key]
	if !ok {
		if !statement.CreateIfNotExists {
			return errors.New("key does not exist")
		}
		x = NewSequence(statement.CreateWithTimestamp, statement.CreateWithFrequency)
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
