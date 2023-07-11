package sequence

import (
	"fmt"
	"testing"
	"time"
)

func TestStoreNew(t *testing.T) {
	store := NewStore()
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	store.New(x, testSequenceFrequency, "s1")
	got, ok := store.m["s1"]
	if !ok {
		t.Fatalf("key should exist in store")
	}
	want := New(x, testSequenceFrequency)
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
}

func TestStoreAdd(t *testing.T) {
	store := NewStore()
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := NewWithValues(x, testSequenceFrequency, testValues)
	store.Add("s1", want)
	got, ok := store.m["s1"]
	if !ok {
		t.Fatalf("key should exist in store")
	}
	if got == want {
		t.Fatalf("pointer values should not be equal")
	}
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
}

func TestStoredDelete(t *testing.T) {
	store := NewStore()
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	store.New(x, testSequenceFrequency, "s1")
	store.Delete("s1")
	_, ok := store.m["s1"]
	if ok {
		t.Fatalf("key should not exist in store")
	}
}

func TestStoreGet(t *testing.T) {
	store := NewStore()
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := NewWithValues(x, testSequenceFrequency, testValues)
	store.Add("s1", want)
	got, ok := store.Get("s1")
	if !ok {
		t.Fatalf("got %t, want true", ok)
	}
	if got == want {
		t.Fatalf("pointer values should not be equal")
	}
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
	_, ok = store.Get("s2")
	if ok {
		t.Fatalf("got %t, want false", ok)
	}
}

func TestStoreDumpLoad(t *testing.T) {
	src := NewStore()
	t1, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	t2, _ := time.Parse("2006-01-02 03:04:05", "2001-02-03 04:05:06")
	src.Add("k1", NewWithValues(t1, testSequenceFrequency, newSliceOfValues(12, 0)))
	src.Add("k11", NewWithValues(t2, 120, newSliceOfValues(32, 1)))
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
	store.Add("k1", NewWithValues(t1, testSequenceFrequency, newSliceOfValues(12, 0)))
	store.Add("k2", NewWithValues(t2, 120, newSliceOfValues(32, 1)))
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

func TestStoreExecuteUnknownStatement(t *testing.T) {
	x := time.Now()
	statement := Statement{
		Key:                 "s1",
		Timestamp:           x,
		Value:               StateActive,
		CreateIfNotExists:   true,
		CreateWithTimestamp: x,
		CreateWithFrequency: testSequenceFrequency,
	}
	for _, v := range []uint8{statementUnknown, statementUnknown + 1} {
		t.Run(fmt.Sprintf("Type=%d", v), func(t *testing.T) {
			statement.Type = v
			if err := NewStore().Execute(statement); err == nil {
				t.Fatal("got error nil, want non nil error")
			}
		})
	}
}

func TestStoreExecuteKeyDoesNotExist(t *testing.T) {
	statement := Statement{
		Key:       "s1",
		Timestamp: time.Now(),
		Value:     StateActive,
	}
	if err := NewStore().Execute(statement); err == nil {
		t.Fatal("got error nil, want non nil error")
	}
}

func TestStoreExecute(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	f := testSequenceFrequency
	type result struct {
		seq *Sequence
		err error
	}
	tests := []struct {
		id        string
		statement Statement
	}{
		{"Add1", Statement{"k1", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementAdd, true, x, f, 0}},
		{"Add2", Statement{"k1", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementAdd, true, x, f, 10}},
		{"Add3", Statement{"k1", x.Add(-time.Duration(f) * time.Second), StateActive, StatementAdd, true, x, f, 0}},
		{"Roll1", Statement{"k1", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementRoll, true, x, f, 0}},
		{"Roll2", Statement{"k1", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementRoll, true, x, f, 5}},
		{"Roll3", Statement{"k1", x.Add(-time.Duration(f) * time.Second), StateActive, StatementRoll, true, x, f, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			var got, want result
			want.seq = New(tt.statement.CreateWithTimestamp, tt.statement.CreateWithFrequency)
			if tt.statement.CreateWithLength > 0 {
				want.seq.SetLength(tt.statement.CreateWithLength)
			}
			if tt.statement.Type == StatementAdd {
				want.err = want.seq.Add(tt.statement.Timestamp, tt.statement.Value)
			} else {
				want.err = want.seq.Roll(tt.statement.Timestamp, tt.statement.Value)
			}
			store := NewStore()
			got.err = store.Execute(tt.statement)
			if got.err != nil {
				if want.err == nil {
					t.Fatalf("got error %s, want error nil", got.err)
				}
			} else {
				if want.err != nil {
					t.Fatal("got error nil, want non nil error")
				}
				var ok bool
				got.seq, ok = store.m[tt.statement.Key]
				if !ok {
					t.Fatal("key should exist in store")
				}
				if !assertSequencesEqual(got.seq, want.seq) {
					t.Fatalf("\ngot  %+v\nwant %+v", got.seq, want.seq)
				}
			}
		})
	}
}

func TestStoreBatch(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	f := testSequenceFrequency
	type result struct {
		seq *Sequence
		err error
	}
	statements := []Statement{
		{"k1", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementAdd, true, x, f, 0},
		{"k2", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementAdd, true, x, f, 10},
		{"k3", x.Add(-time.Duration(f) * time.Second), StateActive, StatementAdd, true, x, f, 0},
		{"k4", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementRoll, true, x, f, 0},
		{"k5", x.Add(time.Duration(8*f) * time.Second), StateActive, StatementRoll, true, x, f, 5},
		{"k6", x.Add(-time.Duration(f) * time.Second), StateActive, StatementRoll, true, x, f, 0},
	}
	store := NewStore()
	errors := store.Batch(statements).ErrorVars()
	for i, statement := range statements {
		var got, want result
		want.seq = New(statement.CreateWithTimestamp, statement.CreateWithFrequency)
		if statement.CreateWithLength > 0 {
			want.seq.SetLength(statement.CreateWithLength)
		}
		if statement.Type == StatementAdd {
			want.err = want.seq.Add(statement.Timestamp, statement.Value)
		} else {
			want.err = want.seq.Roll(statement.Timestamp, statement.Value)
		}
		got.err = errors[i]
		if got.err != nil {
			if want.err == nil {
				t.Fatalf("got error %s, want error nil", got.err)
			}
		} else {
			if want.err != nil {
				t.Fatal("got error nil, want non nil error")
			}
			var ok bool
			got.seq, ok = store.m[statement.Key]
			if !ok {
				t.Fatal("key should exist in store")
			}
			if !assertSequencesEqual(got.seq, want.seq) {
				t.Fatalf("\ngot  %+v\nwant %+v", got.seq, want.seq)
			}
		}
	}
}

func newSliceOfValues(n int, x uint8) []uint8 {
	s := make([]uint8, n)
	if x == 0 {
		return s
	}
	for i := 0; i < n; i++ {
		s[i] = x
	}
	return s
}
