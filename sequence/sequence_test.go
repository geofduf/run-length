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
	want := []byte{0b11001101, 0b10000010, 0b11111101, 0b10001100, 0b00111000}
	got := encode(3764899923, StateActive)
	if !bytes.Equal(got, want) {
		t.Fatalf("\ngot  %08b\nwant %08b\n", got, want)
	}
}

func TestDecode(t *testing.T) {
	type result struct {
		count uint32
		value uint8
		n     int
	}
	want := result{3764899923, StateActive, 5}
	var got result
	got.count, got.value, got.n = decode([]byte{0b11001101, 0b10000010, 0b11111101, 0b10001100, 0b00111000})
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
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
		data:      []uint8{0x15, 0x14, 0x15, 0x12, 0x4},
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

func TestSequenceNext(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	s := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     3764902230,
		data:      []byte{0x84, 0x4, 0xfd, 0x3b, 0x4, 0xcd, 0x82, 0xfd, 0x8c, 0x38},
	}
	type result struct {
		count uint32
		value uint8
		n     int
	}
	tests := []struct {
		id    int
		index int
		want  result
	}{
		{1, 0, result{129, StateInactive, 2}},
		{2, 2, result{1919, StateActive, 2}},
		{3, 4, result{1, StateInactive, 1}},
		{4, 5, result{3764899923, StateActive, 5}},
	}
	var got result
	for _, tt := range tests {
		got.count, got.value, got.n = s.next(tt.index)
		if got != tt.want {
			t.Fatalf("test %d: got %+v, want %+v\n", tt.id, got, tt.want)
		}
	}
}

func TestLast(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	s := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     3764902230,
		data:      []byte{0x84, 0x4, 0xfd, 0x3b, 0x4, 0xcd, 0x82, 0xfd, 0x8c, 0x38},
	}
	type result struct {
		count uint32
		value uint8
		n     int
	}
	want := result{3764899923, StateActive, 5}
	var got result
	got.count, got.value, got.n = s.last()
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestAddSeries(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	s := NewSequence(x, testSequenceFrequency)
	tests := []struct {
		id    int
		count uint32
		value uint8
		want  []byte
	}{
		{1, 129, StateInactive, []byte{0x84, 0x4}},
		{2, 1919, StateActive, []byte{0x84, 0x4, 0xfd, 0x3b}},
		{3, 32767, StateActive, []byte{0x84, 0x4, 0xf9, 0xbb, 0x8}},
		{4, 1, StateActive, []byte{0x84, 0x4, 0xfd, 0xbb, 0x8}},
		{5, 1, StateUnknown, []byte{0x84, 0x4, 0xfd, 0xbb, 0x8, 0x6}},
	}
	var n uint32
	for _, tt := range tests {
		s.addSeries(tt.count, tt.value)
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
		count:     20,
		data:      []byte{0x15, 0x14, 0x15, 0x12, 0x4},
	}
	got := s.Bytes()
	want := append(testSequenceBasePrefix, []byte{0x14, 0x0, 0x0, 0x0, 0x15, 0x14, 0x15, 0x12, 0x4}...)
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
