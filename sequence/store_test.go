package sequence

import (
	"bytes"
	"testing"
	"time"
)

func TestStoreDumpLoad(t *testing.T) {
	src := NewStore()
	t1, _ := time.Parse("2006-01-02 03:04:05", "2018-01-01 00:00:00")
	t2, _ := time.Parse("2006-01-02 03:04:05", "2018-01-08 00:00:00")
	src.Add("k1", NewSequenceFromValues(t1, newSliceOfValues(length, 0)))
	src.Add("k11", NewSequenceFromValues(t2, newSliceOfValues(length, 1)))
	dump, err := src.Dump()
	if err != nil {
		t.Fatalf("got an unexpected error: %s", err)
	}
	dst := NewStore()
	dst.Load(dump)
	if len(src.m) != len(dst.m) {
		t.Fatalf("got %d element(s), want %d element(s)", len(dst.m), len(src.m))
	}
	for k := range src.m {
		v, ok := dst.m[k]
		if !ok {
			t.Fatalf("key %s should exist in store", k)
		}
		if !assertSequencesEqual(src.m[k], v) {
			t.Fatalf("sequences are not equal for key %s", k)
		}
	}
}

func TestStoreKeys(t *testing.T) {
	store := NewStore()
	want := []string{"k1", "k2"}
	for _, v := range want {
		store.Add(v, NewSequence(newTime("2018-01-01 00:00:00")))
	}
	got := store.Keys()
	if len(got) != len(want) {
		t.Fatalf("got %d element(s), want %d element(s)", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestStoreExecuteUnsafe(t *testing.T) {
	type result struct {
		err    bool
		length int
	}
	tests := []struct {
		id                  string
		key                 string
		value               uint8
		statementType       uint8
		createIfNotExists   bool
		createWithTimestamp time.Time
		want                result
	}{
		{"1", "k1", FlagActive, StatementTypeAddValue, false, time.Now(), result{true, 0}},
		{"2", "k1", FlagActive, StatementTypeAddValue, true, time.Now(), result{false, 1}},
		{"3", "k1", FlagActive, StatementTypeAddValue, true, time.Now().Add(5 * time.Minute), result{true, 1}},
		{"4", "k1", FlagActive, 2, true, time.Now(), result{true, 0}},
	}
	for _, tt := range tests {
		prefix := "test " + tt.id
		store := NewStore()
		statement := Statement{
			Key:                 tt.key,
			Value:               tt.value,
			Type:                tt.statementType,
			CreateIfNotExists:   tt.createIfNotExists,
			CreateWithTimestamp: tt.createWithTimestamp,
		}
		err := store.executeUnsafe(statement)
		if err != nil {
			if !tt.want.err {
				t.Fatalf("%s: didn't expect an error, got %s", prefix, err)
			}
		} else if tt.want.err {
			t.Fatalf("%s: expected an error", prefix)
		}
		if n := len(store.m); n != tt.want.length {
			t.Fatalf("%s: got length %d, expected length %d", prefix, n, tt.want.length)
		}
		if tt.want.length == 1 {
			if _, ok := store.m[tt.key]; !ok {
				t.Fatalf("%s: expected key to exist in store", prefix)
			}
		}
	}
}

func assertSequencesEqual(x, y *Sequence) bool {
	if x.ts != y.ts || x.count != y.count {
		return false
	}
	if !bytes.Equal(x.data, y.data) {
		return false
	}
	return true
}
