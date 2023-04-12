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
