package sequence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

type StatementType uint8

const (
	StatementTypeAddValue StatementType = iota
)

// A Statement represents an operation to perform on a store.
type Statement struct {
	Key   string
	Value uint8
	Type  StatementType
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

// NewSequence creates and intializes a new Sequence using t as its reference timestamp.
// The new Sequence is added to the store using key as its identifier. If a
// Sequence already exists for the identifier it is silently replaced with the new
// Sequence.
func (store *Store) NewSequence(t time.Time, key string) {
	store.mu.Lock()
	store.m[key] = NewSequence(t)
	store.mu.Unlock()
}

// AddSequence adds a copy of s to the store using key as its identifier.
// If a Sequence already exists for the identifier it is silently replaced with the new
// Sequence.
func (store *Store) AddSequence(key string, s *Sequence) {
	store.mu.Lock()
	store.m[key] = s.clone()
	store.mu.Unlock()
}

// GetSequence returns a copy of the Sequence associated to key. The second value returned is
// true if the key exists in the store and false if not.
func (store *Store) GetSequence(key string) (*Sequence, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	s, ok := store.m[key]
	if !ok {
		return nil, false
	}
	return s.clone(), true
}

// AddValue adds a value to a Sequence, returning an error if the key does not
// exist or if the add operation returned an error.
func (store *Store) AddValue(key string, x uint8) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	s, ok := store.m[key]
	if !ok {
		return errors.New("key does not exist")
	}
	return s.Add(x)
}

// Batch allows to perform multiple operations in one call. It currently only
// supports operations of type StatementTypeAddValue. If a key does not
// exist, a new sequence will be created using t as its reference timestamp.
// Errors resulting from sequence operations don't stop the process, but if one or more
// sequence operations returned an error, the method will return an error
// and a slice holding information about each operation error.
func (store *Store) Batch(t time.Time, statements []Statement) (error, []string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	var report []string
	for i, v := range statements {
		if v.Type != StatementTypeAddValue {
			report = append(report, fmt.Sprintf("unknown statement type, at index %d", i))
			continue
		}
		s, ok := store.m[v.Key]
		if !ok {
			s = NewSequence(t)
			store.m[v.Key] = s
		}
		if err := s.Add(v.Value); err != nil {
			report = append(report, fmt.Sprintf("%s, at index %d", err.Error(), i))
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
