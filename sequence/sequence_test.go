package sequence

import (
	"bytes"
	"testing"
	"time"
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
	x, _ := time.Parse("2006-01-02 03:04:05", "2000-01-02 03:04:05")
	s := NewSequence(x)
	want := []byte{0b100101, 0b11000000, 0b1101110, 0b111000, 0, 0}
	if !bytes.Equal(s.data, want) {
		t.Fatalf("got %08b, want %08b\n", s.data, want)
	}
}

func TestInc(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", "2000-01-02 03:04:05")
	s := NewSequence(x)
	s.inc(129)
	s.inc(3)
	want := uint16(132)
	if s.count != want {
		t.Fatalf("got %d, want %d\n", s.count, want)
	}
	got := uint16(s.data[indexCounter]) | uint16(s.data[indexCounter+1])<<8
	if got != want {
		t.Fatalf("got %d, want %d\n", got, want)
	}
}

func TestLast(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", "2000-01-02 03:04:05")
	s := NewSequence(x)
	s.addMany(129, FlagInactive)
	s.addMany(3, FlagActive)
	type result struct {
		count uint16
		flag  uint8
	}
	want := result{3, FlagActive}
	var got result
	got.count, got.flag = s.last()
	if got != want {
		t.Fatalf("got %+v, want %+v\n", got, want)
	}
}

func TestAddOne(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", "2000-01-02 03:04:05")
	s := NewSequence(x)
	tests := []struct {
		id    int
		value uint8
		want  []byte
	}{
		{1, FlagInactive, []byte{0x25, 0xc0, 0x6e, 0x38, 0x1, 0x0, 0x4, 0x0}},
		{2, FlagActive, []byte{0x25, 0xc0, 0x6e, 0x38, 0x2, 0x0, 0x4, 0x0, 0x5, 0x0}},
		{3, FlagActive, []byte{0x25, 0xc0, 0x6e, 0x38, 0x3, 0x0, 0x4, 0x0, 0x9, 0x0}},
		{4, FlagUnknown, []byte{0x25, 0xc0, 0x6e, 0x38, 0x4, 0x0, 0x4, 0x0, 0x9, 0x0, 0x6, 0x0}},
	}
	for _, tt := range tests {
		s.addOne(tt.value)
		if !bytes.Equal(s.data, tt.want) {
			t.Fatalf("test %d:\ngot  %08b\nwant %08b\n", tt.id, s.data, tt.want)
		}
	}

}

func TestAddMany(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", "2000-01-02 03:04:05")
	s := NewSequence(x)
	tests := []struct {
		id    int
		count uint16
		value uint8
		want  []byte
	}{
		{1, 129, FlagInactive, []byte{0x25, 0xc0, 0x6e, 0x38, 0x81, 0x0, 0x4, 0x2}},
		{2, 1919, FlagActive, []byte{0x25, 0xc0, 0x6e, 0x38, 0x0, 0x8, 0x4, 0x2, 0xfd, 0x1d}},
		{3, 32767, FlagActive, []byte{0x25, 0xc0, 0x6e, 0x38, 0xff, 0x87, 0x4, 0x2, 0xfd, 0xff, 0xfd, 0xff, 0x1, 0x1e}},
	}
	for _, tt := range tests {
		s.addMany(tt.count, tt.value)
		if !bytes.Equal(s.data, tt.want) {
			t.Fatalf("test %d:\ngot  %08b\nwant %08b\n", tt.id, s.data, tt.want)
		}
	}
}
