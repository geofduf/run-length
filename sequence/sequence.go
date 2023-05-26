package sequence

import (
	"errors"
	"time"
)

const (
	sizeTimestamp = 4
	sizeFrequency = 2
	sizeLength    = 4
	sizeCounter   = 4

	indexTimestamp = 0
	indexFrequency = indexTimestamp + sizeTimestamp
	indexLength    = indexFrequency + sizeFrequency
	indexCounter   = indexLength + sizeLength
	indexData      = indexCounter + sizeCounter

	flagBits       = 2
	maxRepetitions = uint16(1<<(16-flagBits) - 1)
)

// Internal representation of sequence values.
const (
	StateInactive uint8 = iota // 0b00
	StateActive                // 0b01
	StateUnknown               // 0b10
	StateNotUsed               // 0b11
)

// MaxSequenceLength is the maximum number of values that can be stored
// in a sequence.
const MaxSequenceLength = 1<<(sizeLength*8) - 1 // 4294967295

// A Sequence represents a time series of regularly spaced 2-bit values. Following
// the current implementation, the maximum length of a sequence is 4294967295.
type Sequence struct {
	ts        int64
	frequency uint16
	length    uint32
	count     uint32
	data      []byte
}

// NewSequence creates and intializes a new Sequence using t rounded down to
// the second as its reference timestamp and f as its frequency in seconds.
// The sequence frequency will default to 1 if set to 0.
func NewSequence(t time.Time, f uint16) *Sequence {
	if f == 0 {
		f = 1
	}
	s := Sequence{
		ts:        t.Unix(),
		frequency: f,
		length:    MaxSequenceLength,
	}
	return &s
}

// NewSequenceFromValues creates a new Sequence using t rounded down to the second
// as its reference timestamp, f as its frequency in seconds and values as its
// initial content. The sequence frequency will default to 1 if set to 0. If the
// length of values is greater than the maximum length of the sequence the trailing
// elements will be silently ignored.
func NewSequenceFromValues(t time.Time, f uint16, values []uint8) *Sequence {
	s := NewSequence(t, f)
	n := len(values)
	if n == 0 {
		return s
	}
	if n > MaxSequenceLength {
		n = MaxSequenceLength
	}
	count := uint32(1)
	x := values[0]
	for i := 1; i < n; i++ {
		if values[i] != x {
			if count == 1 {
				s.addOne(x)
			} else {
				s.addMany(count, x)
			}
			count = 0
			x = values[i]
		}
		count++
	}
	if count == 1 {
		s.addOne(x)
	} else {
		s.addMany(count, x)
	}
	return s
}

// NewSequenceFromBytes creates a new Sequence using data, an encoded Sequence, as
// its inital content.
func NewSequenceFromBytes(data []byte) (*Sequence, error) {
	n := len(data)
	if n < indexData {
		return nil, errors.New("cannot decode the sequence")
	}
	if n > indexData && (n-indexData)%2 != 0 {
		return nil, errors.New("cannot decode the sequence")
	}
	s := Sequence{
		data: make([]byte, n-indexData),
	}
	copy(s.data, data[indexData:])
	i := indexTimestamp
	s.ts = int64(data[i]) | int64(data[i+1])<<8 | int64(data[i+2])<<16 | int64(data[i+3])<<24
	i = indexFrequency
	s.frequency = uint16(data[i]) | uint16(data[i+1])<<8
	i = indexLength
	s.length = uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
	if s.length == 0 {
		s.length = MaxSequenceLength
	}
	i = indexCounter
	s.count = uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
	return &s, nil
}

// Add adds a value to the sequence, returning an error if outside the
// time boundaries of the sequence or if an entry already exists.
func (s *Sequence) Add(t time.Time, x uint8) error {
	offset := (t.Unix()-s.ts)/int64(s.frequency) + 1
	if offset < 1 || offset > int64(s.length) {
		return errors.New("out of bounds")
	}
	if offset <= int64(s.count) {
		return errors.New("cannot overwrite value")
	}
	if delta := offset - int64(s.count); delta > 1 {
		s.addMany(uint32(delta)-1, StateUnknown)
	}
	s.addOne(x)
	return nil
}

// addOne adds a value to the sequence.
func (s *Sequence) addOne(x uint8) {
	x &= 1<<flagBits - 1
	if s.count != 0 {
		n, v := s.last()
		if v == x && n < maxRepetitions {
			i := len(s.data) - 2
			s.data[i], s.data[i+1] = encode(n+1, x)
			s.count++
			return
		}
	}
	b0, b1 := encode(1, x)
	s.data = append(s.data, b0, b1)
	s.count++
}

// addMany adds a series of values to the sequence, using count as the
// length of the series and x as the value.
func (s *Sequence) addMany(count uint32, x uint8) {
	x &= 1<<flagBits - 1
	c := count
	if s.count != 0 {
		n, v := s.last()
		if v == x && n < maxRepetitions {
			i := len(s.data) - 2
			if available := uint32(maxRepetitions - n); c > available {
				s.data[i], s.data[i+1] = encode(maxRepetitions, x)
				c -= available
			} else {
				s.data[i], s.data[i+1] = encode(n+uint16(c), x)
				s.count += count
				return
			}
		}
	}
	if c > uint32(maxRepetitions) {
		b0, b1 := encode(maxRepetitions, x)
		for c > uint32(maxRepetitions) {
			s.data = append(s.data, b0, b1)
			c -= uint32(maxRepetitions)
		}
	}
	if c > 0 {
		b0, b1 := encode(uint16(c), x)
		s.data = append(s.data, b0, b1)
	}
	s.count += count
}

// Values returns raw values stored in the sequence using start and end as
// closed interval filter. The second return value is the Unix time associated to
// the first element of the slice. The method returns an error if the interval filter
// and the sequence don't overlap.
func (s *Sequence) Values(start, end time.Time) ([]uint8, int64, error) {
	return s.queryValues(start, end)
}

// All returns the raw values stored in the sequence.
func (s *Sequence) All() []uint8 {
	data := make([]uint8, s.count)
	index := 0
	for i := 0; i < len(s.data); i += 2 {
		n, v := decode(s.data[i], s.data[i+1])
		for j := 0; j < int(n); j++ {
			data[index] = v
			index++
		}
	}
	return data
}

// Bytes returns the encoded sequence.
func (s *Sequence) Bytes() []byte {
	x := make([]byte, indexData+len(s.data))
	i := indexTimestamp
	x[i], x[i+1], x[i+2], x[i+3] = byte(s.ts), byte(s.ts>>8), byte(s.ts>>16), byte(s.ts>>24)
	i = indexFrequency
	x[i], x[i+1] = byte(s.frequency), byte(s.frequency>>8)
	if v := s.length; v != MaxSequenceLength {
		i = indexLength
		x[i], x[i+1], x[i+2], x[i+3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
	}
	if v := s.count; v != 0 {
		i = indexCounter
		x[i], x[i+1], x[i+2], x[i+3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
	}
	if len(s.data) > 0 {
		copy(x[indexData:], s.data)
	}
	return x
}

// Timestamp returns the sequence reference timestamp as a Unix time.
func (s *Sequence) Timestamp() int64 {
	return s.ts
}

// Frequency returns the sequence frequency in seconds.
func (s *Sequence) Frequency() uint16 {
	return s.frequency
}

// Length returns the sequence length.
func (s *Sequence) Length() uint32 {
	return s.length
}

// last returns the length and value of the last series in the sequence.
func (s *Sequence) last() (uint16, uint8) {
	i := len(s.data) - 2
	return decode(s.data[i], s.data[i+1])
}

// interval returns the closed time interval associated to the sequence.
func (s *Sequence) interval() interval {
	return interval{start: s.ts, end: s.ts + (int64(s.length)-1)*int64(s.frequency)}
}

// clone returns a copy of s.
func (s *Sequence) clone() *Sequence {
	clone := Sequence{
		ts:        s.ts,
		frequency: s.frequency,
		length:    s.length,
		count:     s.count,
		data:      make([]uint8, len(s.data)),
	}
	copy(clone.data, s.data)
	return &clone
}

// encode encodes count and value as 2 bytes, keeping the 14 most
// significant bits of count and the 2 most significant bits of value.
func encode(count uint16, value uint8) (byte, byte) {
	count <<= flagBits
	return byte(count) | value, byte(count >> 8)
}

// decode decodes values encoded using the encode function.
func decode(x, y byte) (uint16, uint8) {
	return uint16(x>>flagBits) | uint16(y)<<(8-flagBits), x & (1<<flagBits - 1)
}
