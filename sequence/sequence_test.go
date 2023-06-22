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

func TestNew(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     0,
	}
	got := New(x, testSequenceFrequency)
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
}

func TestNewWithValues(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     20,
		data:      []uint8{0x15, 0x14, 0x15, 0x12, 0x4},
	}
	got := NewWithValues(x, testSequenceFrequency, testValues)
	if !assertSequencesEqual(got, want) {
		t.Fatalf("\ngot  %+v\nwant %+v", got, want)
	}
}

func TestFromBytes(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	want := &Sequence{
		ts:        x.Unix(),
		frequency: testSequenceFrequency,
		length:    MaxSequenceLength,
		count:     129,
		data:      []byte{0x4, 0x2},
	}
	got, err := FromBytes(append(testSequenceBasePrefix, []byte{0x81, 0x0, 0x0, 0x0, 0x4, 0x2}...))
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
	s := New(x, testSequenceFrequency)
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

func TestSequenceAdd(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	got := New(x, testSequenceFrequency)
	type result struct {
		data []byte
		err  bool
	}
	tests := []struct {
		id    int
		shift int
		value uint8
		want  result
	}{
		{1, 129, StateInactive, result{[]byte{0x86, 0x4, 0x4}, false}},
		{2, 130, StateInactive, result{[]byte{0x86, 0x4, 0x8}, false}},
		{3, 139, StateActive, result{[]byte{0x86, 0x4, 0x8, 0x22, 0x5}, false}},
		{4, -1, StateActive, result{[]byte{}, true}},
		{5, 1, StateActive, result{[]byte{}, true}},
	}
	for _, tt := range tests {
		err := got.Add(shift(got, tt.shift, 0), tt.value)
		if err != nil {
			if !tt.want.err {
				t.Fatalf("test %d: got error %s, want error nil", tt.id, err)
			}
			continue
		} else {
			if tt.want.err {
				t.Fatalf("test %d: got error nil, want non nil error", tt.id)
			}
		}
		want := &Sequence{x.Unix(), testSequenceFrequency, MaxSequenceLength, uint32(tt.shift + 1), tt.want.data}
		if !assertSequencesEqual(got, want) {
			t.Fatalf("test %d:\ngot  %+v\nwant %+v", tt.id, got, want)
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

func TestSequenceSetLength(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	tests := []struct {
		id     int
		length uint32
		want   *Sequence
	}{
		{1, 1440, &Sequence{x.Unix(), testSequenceFrequency, 1440, 20, []byte{0x15, 0x14, 0x15, 0x12, 0x4}}},
		{2, 12, &Sequence{x.Unix(), testSequenceFrequency, 12, 12, []byte{0x15, 0x14, 0x9}}},
		{3, 8, &Sequence{x.Unix(), testSequenceFrequency, 8, 8, []byte{0x15, 0xc}}},
	}
	for _, tt := range tests {
		got := &Sequence{
			ts:        x.Unix(),
			frequency: testSequenceFrequency,
			length:    MaxSequenceLength,
			count:     20,
			data:      []byte{0x15, 0x14, 0x15, 0x12, 0x4},
		}
		got.SetLength(tt.length)
		if !assertSequencesEqual(got, tt.want) {
			t.Fatalf("test %d:\ngot  %+v\nwant %+v", tt.id, got, tt.want)
		}
	}
}

func TestSequenceRoll(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	f := testSequenceFrequency
	s := &Sequence{
		ts:        x.Unix(),
		frequency: f,
		length:    140,
		count:     135,
		data:      []byte{0x15, 0x14, 0xf5, 0x3},
	}
	tests := []struct {
		id        int
		timestamp time.Time
		want      *Sequence
	}{
		{1, shift(s, 134+1, 0), &Sequence{x.Unix(), f, 140, 136, []byte{0x15, 0x14, 0xf9, 0x3}}},
		{2, shift(s, 134+5+7, 0), &Sequence{x.Unix() + 7*int64(f), f, 140, 140, []byte{0xc, 0xf5, 0x3, 0x2e, 0x5}}},
		{3, shift(s, 134+5+10, 0), &Sequence{x.Unix() + 10*int64(f), f, 140, 140, []byte{0xf5, 0x3, 0x3a, 0x5}}},
		{4, shift(s, 134+5+12, 0), &Sequence{x.Unix() + 12*int64(f), f, 140, 140, []byte{0xed, 0x3, 0x42, 0x5}}},
		{5, shift(s, 134+5+130, 0), &Sequence{x.Unix() + 130*int64(f), f, 140, 140, []byte{0x15, 0x9a, 0x4, 0x5}}},
		{6, shift(s, 134+5+4000, 0), &Sequence{x.Unix() + 4000*int64(f), f, 140, 140, []byte{0xae, 0x4, 0x5}}},
	}
	for _, tt := range tests {
		got := s.clone()
		err := got.Roll(tt.timestamp, StateActive)
		if err != nil {
			t.Fatalf("test %d: got error %s, want error nil", tt.id, err)
		}
		if !assertSequencesEqual(got, tt.want) {
			t.Fatalf("test %d:\ngot  %+v\nwant %+v", tt.id, got, tt.want)
		}
	}
}

func TestSequenceTrimLeft(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
	f := testSequenceFrequency
	s := &Sequence{
		ts:        x.Unix(),
		frequency: f,
		length:    140,
		count:     135,
		data:      []byte{0x15, 0x14, 0xf5, 0x3},
	}
	tests := []struct {
		id        int
		timestamp time.Time
		want      *Sequence
	}{
		{1, shift(s, 7, 0), &Sequence{x.Unix() + 7*int64(f), f, 140, 128, []byte{0xc, 0xf5, 0x3}}},
		{2, shift(s, 7, 1), &Sequence{x.Unix() + 8*int64(f), f, 140, 127, []byte{0x8, 0xf5, 0x3}}},
		{3, shift(s, 10, 0), &Sequence{x.Unix() + 10*int64(f), f, 140, 125, []byte{0xf5, 0x3}}},
		{4, shift(s, 10, 1), &Sequence{x.Unix() + 11*int64(f), f, 140, 124, []byte{0xf1, 0x3}}},
		{5, shift(s, 12, 0), &Sequence{x.Unix() + 12*int64(f), f, 140, 123, []byte{0xed, 0x3}}},
		{6, shift(s, 12, 1), &Sequence{x.Unix() + 13*int64(f), f, 140, 122, []byte{0xe9, 0x3}}},
		{7, shift(s, 130, 0), &Sequence{x.Unix() + 130*int64(f), f, 140, 5, []byte{0x15}}},
		{8, shift(s, 130, 1), &Sequence{x.Unix() + 131*int64(f), f, 140, 4, []byte{0x11}}},
		{9, shift(s, 4000, 0), &Sequence{x.Unix() + 4000*int64(f), f, 140, 0, []byte{}}},
	}
	for _, tt := range tests {
		got := s.clone()
		err := got.TrimLeft(tt.timestamp)
		if err != nil {
			t.Fatalf("test %d: got error %s, want error nil", tt.id, err)
		}
		if !assertSequencesEqual(got, tt.want) {
			t.Fatalf("test %d:\ngot  %+v\nwant %+v", tt.id, got, tt.want)
		}
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
