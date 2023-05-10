package sequence

import (
	"bytes"
	"testing"
	"time"
)

var (
	testSequenceTimestamp  = "2000-01-02 03:04:05"
	testSequenceFrequency  = uint16(60)
	testSequenceBasePrefix = []byte{0x25, 0xc0, 0x6e, 0x38, 0x3c, 0x0, 0x0, 0x0, 0x0, 0x0}
	testValues             = []uint8{1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 0}
)

func TestEncode(t *testing.T) {
	got := make([]byte, 2)
	want := []byte{0b101, 0b10}
	got[0], got[1] = encode(129, FlagActive)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %08b, want %08b\n", got, want)
	}
}

func TestDecode(t *testing.T) {
	type result struct {
		count uint16
		flag  uint8
	}
	want := result{129, FlagActive}
	var got result
	got.count, got.flag = decode(0b101, 0b10)
	if got != want {
		t.Fatalf("got %+v, want %+v\n", got, want)
	}
}

func TestNewSequence(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     0,
	}
	got := NewSequence(x, testSequenceFrequency)
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
}

func TestNewSequenceFromValues(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     20,
		data:      []uint8{0x15, 0x0, 0x14, 0x0, 0x15, 0x0, 0x12, 0x0, 0x4, 0x0},
	}
	got := NewSequenceFromValues(x, testSequenceFrequency, testValues)
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
}
func TestNewSequenceFromBytes(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     129,
		data:      []byte{0x4, 0x2},
	}
	got, err := NewSequenceFromBytes(append(testSequenceBasePrefix, []byte{0x81, 0x0, 0x0, 0x0, 0x4, 0x2}...))
	if err != nil {
		t.Fatalf("got error %s, want error nil", err)
	}
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
}

func TestLast(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	s := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     10,
		data:      []byte{0x15, 0x0, 0x14, 0x0},
	}
	type result struct {
		count uint16
		flag  uint8
	}
	want := result{5, FlagInactive}
	var got result
	got.count, got.flag = s.last()
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestAddOne(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	s := NewSequence(x, testSequenceFrequency)
	tests := []struct {
		id    int
		value uint8
		want  []byte
	}{
		{1, FlagInactive, []byte{0x4, 0x0}},
		{2, FlagActive, []byte{0x4, 0x0, 0x5, 0x0}},
		{3, FlagActive, []byte{0x4, 0x0, 0x9, 0x0}},
		{4, FlagUnknown, []byte{0x4, 0x0, 0x9, 0x0, 0x6, 0x0}},
	}
	for i, tt := range tests {
		s.addOne(tt.value)
		if int(s.count) != i+1 {
			t.Fatalf("test %d: got %d, want %d", tt.id, s.count, i+1)
		}
		if !bytes.Equal(s.data, tt.want) {
			t.Fatalf("test %d:\ngot  %v\nwant %v", tt.id, s.data, tt.want)
		}
	}
}

func TestAddMany(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	s := NewSequence(x, testSequenceFrequency)
	tests := []struct {
		id    int
		count uint32
		value uint8
		want  []byte
	}{
		{1, 129, FlagInactive, []byte{0x4, 0x2}},
		{2, 1919, FlagActive, []byte{0x4, 0x2, 0xfd, 0x1d}},
		{3, 32767, FlagActive, []byte{0x4, 0x2, 0xfd, 0xff, 0xfd, 0xff, 0x1, 0x1e}},
	}
	var n uint32
	for _, tt := range tests {
		s.addMany(tt.count, tt.value)
		n += tt.count
		if s.count != n {
			t.Fatalf("test %d: got %d, want %d", tt.id, s.count, n)
		}
		if !bytes.Equal(s.data, tt.want) {
			t.Fatalf("test %d:\ngot  %v\nwant %v", tt.id, s.data, tt.want)
		}
	}
}

func TestSequenceBytes(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	s := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     129,
		data:      []byte{0x4, 0x2},
	}
	got := s.Bytes()
	want := append(testSequenceBasePrefix, []byte{0x81, 0x0, 0x0, 0x0, 0x4, 0x2}...)
	if !bytes.Equal(got, want) {
		t.Errorf("\ngot  %v\nwant %v", got, want)
	}
}

func assertSequencesEqual(x, y *Sequence) bool {
	if x.ts != y.ts || x.frequency != y.frequency || x.length != y.length || x.count != y.count {
		return false
	}
	if !bytes.Equal(x.data, y.data) {
		return false
	}
	return true
}
