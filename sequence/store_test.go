package sequence

import (
	"testing"
	"time"
)

func TestStoreDumpLoad(t *testing.T) {
	src := NewStore()
	t1, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	t2, _ := time.Parse("2006-01-02 03:04:05", "2001-02-03 04:05:06")
	src.Add("k1", NewSequenceFromValues(t1, testSequenceFrequency, newSliceOfValues(12, 0)))
	src.Add("k11", NewSequenceFromValues(t2, 120, newSliceOfValues(32, 1)))
	dump, err := src.Dump()
	if err != nil {
		t.Fatalf("got error %s, want error nil", err)
	}
	dst := NewStore()
	err = dst.Load(dump)
	if err != nil {
		t.Fatalf("got error %s, want error nil", err)
	}
	if n, m := len(src.m), len(dst.m); n != m {
		t.Fatalf("got %d, want %d", n, m)
	}
	for k := range src.m {
		v, ok := dst.m[k]
		if !ok {
			t.Fatalf("key %s should exist in store", k)
		}
		if !assertSequencesEqual(src.m[k], v) {
			t.Fatalf("\ngot  %+v\nwant %+v", src.m[k], v)
		}
	}
}

func TestStoreKeys(t *testing.T) {
	store := NewStore()
	t1, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	t2, _ := time.Parse("2006-01-02 03:04:05", "2001-02-03 04:05:06")
	store.Add("k1", NewSequenceFromValues(t1, testSequenceFrequency, newSliceOfValues(12, 0)))
	store.Add("k2", NewSequenceFromValues(t2, 120, newSliceOfValues(32, 1)))
	want := []string{"k1", "k2"}
	got := store.Keys()
	if n, m := len(got), len(want); n != m {
		t.Fatalf("got %d, want %d", n, m)
	}
	for _, x := range want {
		found := false
		for _, y := range got {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected %s in slice %v", x, got)
		}
	}
}

func TestStoreExecuteUnsafe(t *testing.T) {
	x := time.Now()
	type result struct {
		err    bool
		length int
	}
	tests := []struct {
		id                  int
		key                 string
		timestamp           time.Time
		value               uint8
		statementType       uint8
		createIfNotExists   bool
		createWithTimestamp time.Time
		want                result
	}{
		{1, "k1", x, FlagActive, StatementTypeAddValue, false, x, result{true, 0}},
		{2, "k1", x, FlagActive, StatementTypeAddValue, true, x, result{false, 1}},
		{3, "k1", x, FlagActive, StatementTypeAddValue, true, x.Add(5 * time.Minute), result{true, 1}},
		{4, "k1", x, FlagActive, 2, true, x, result{true, 0}},
	}
	for _, tt := range tests {
		store := NewStore()
		statement := Statement{
			Key:                 tt.key,
			Timestamp:           tt.timestamp,
			Value:               tt.value,
			Type:                tt.statementType,
			CreateIfNotExists:   tt.createIfNotExists,
			CreateWithTimestamp: tt.createWithTimestamp,
			CreateWithFrequency: testSequenceFrequency,
		}
		err := store.executeUnsafe(statement)
		if err != nil {
			if !tt.want.err {
				t.Fatalf("test %d: got error %s, want error nil", tt.id, err)
			}
		} else if tt.want.err {
			t.Fatalf("test %d: got error nil, want non nil error", tt.id)
		}
		if n := len(store.m); n != tt.want.length {
			t.Fatalf("test %d: got %d, want %d", tt.id, n, tt.want.length)
		}
		if tt.want.length == 1 {
			if _, ok := store.m[tt.key]; !ok {
				t.Fatalf("test %d: expected key to exist in store", tt.id)
			}
		}
	}
}
