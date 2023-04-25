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
	src.AddSequence("k1", NewSequenceFromValues(t1, newSliceOfValues(length, 0)))
	src.AddSequence("k11", NewSequenceFromValues(t2, newSliceOfValues(length, 1)))
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
		store.AddSequence(v, NewSequence(newTime("2018-01-01 00:00:00")))
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

func assertSequencesEqual(x, y *Sequence) bool {
	if x.ts != y.ts || x.count != y.count {
		return false
	}
	if !bytes.Equal(x.data, y.data) {
		return false
	}
	return true
}
