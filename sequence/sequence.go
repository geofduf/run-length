package sequence

import (
	"errors"
	"time"
)

const (
	frequency = 60
	length    = 7 * 86400 / frequency
	flagBits  = 2

	maxRepetitions = 1<<(16-flagBits) - 1

	indexTimestamp = 0
	indexCounter   = 4
	indexData      = 6
)

const (
	FlagInactive uint8 = iota // 0b00
	FlagActive                // 0b01
	FlagUnknown               // 0b10
	FlagNotUsed               // 0b11
)

// A Sequence represents a sequence of 2-bit values. The number of
// values that can be stored in a sequence depends on its
// length and frequency. In the current implementation, a sequence
// can hold up to 10080 values, representing 7 days of data with
// an ingestion frequency of 60 seconds.
type Sequence struct {
	ts    int64
	count uint16
	data  []byte
}

// NewSequence creates and intializes a new Sequence using t as its reference timestamp.
func NewSequence(t time.Time) *Sequence {
	ts := t.Unix()
	data := make([]byte, 6)
	x := ts
	i := indexTimestamp
	for x > 0 {
		data[i] = byte(x)
		x >>= 8
		i++
	}
	return &Sequence{ts: ts, data: data}
}

// NewSequenceFromValues creates a new Sequence using t as its reference timestamp and
// values as its initial content. If the length of values is greater than the length
// of the sequence, the trailing elements will be silently ignored.
func NewSequenceFromValues(t time.Time, values []uint8) *Sequence {
	ts := t.Unix()
	data := make([]byte, 6)
	x := ts
	i := indexTimestamp
	for x > 0 {
		data[i] = byte(x)
		x >>= 8
		i++
	}
	s := &Sequence{ts: ts, data: data}
	n := len(values)
	if n > length {
		n = length
	}
	for i := 0; i < n; i++ {
		s.addOne(values[i])
	}
	return s
}

// NewSequenceFromBytes creates a new Sequence using data, an encoded Sequence, as
// its inital content.
func NewSequenceFromBytes(data []byte) (*Sequence, error) {
	if len(data) < indexData {
		return nil, errors.New("cannot decode the sequence")
	}
	var ts int64
	for i := 0; i < 4; i++ {
		ts |= int64(data[indexTimestamp+i]) << (i * 8)
	}
	count := uint16(data[indexCounter]) | uint16(data[indexCounter+1])<<8
	if count > length {
		return nil, errors.New("cannot decode the sequence")
	}
	var numberOfValues uint16
	if len(data) > indexData {
		if (len(data)-indexData)%2 != 0 {
			return nil, errors.New("cannot decode the sequence")
		}
		for i := indexData; i < len(data); i += 2 {
			n, _ := decode(data[i], data[i+1])
			numberOfValues += n
		}
	}
	if count != numberOfValues {
		return nil, errors.New("cannot decode the sequence")
	}
	clone := make([]byte, len(data))
	copy(clone, data)
	return &Sequence{ts: ts, count: count, data: clone}, nil
}

// Add adds a value to the sequence, returning an error if outside the
// time boundaries of the sequence or if an entry already exists.
func (s *Sequence) Add(x uint8) error {
	offset := (time.Now().Unix()-s.ts)/frequency + 1
	if offset < 1 || offset > length {
		return errors.New("out of bounds")
	}
	if uint16(offset) <= s.count {
		return errors.New("cannot overwrite value")
	}
	delta := uint16(offset) - s.count
	if delta > 1 {
		s.addMany(delta-1, FlagUnknown)
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
			s.inc(1)
			return
		}
	}
	b0, b1 := encode(1, x)
	s.data = append(s.data, b0, b1)
	s.inc(1)
}

// addMany adds a series of values to the sequence, using count as the
// length of the series and x as the value.
func (s *Sequence) addMany(count uint16, x uint8) {
	x &= 1<<flagBits - 1
	c := count
	if s.count != 0 {
		n, v := s.last()
		if v == x && n < maxRepetitions {
			i := len(s.data) - 2
			if available := maxRepetitions - n; c > available {
				s.data[i], s.data[i+1] = encode(maxRepetitions, x)
				c -= available
			} else {
				s.data[i], s.data[i+1] = encode(n+c, x)
				s.inc(count)
				return
			}
		}
	}
	if c > maxRepetitions {
		b0, b1 := encode(maxRepetitions, x)
		for c > maxRepetitions {
			s.data = append(s.data, b0, b1)
			c -= maxRepetitions
		}
	}
	if c > 0 {
		b0, b1 := encode(c, x)
		s.data = append(s.data, b0, b1)
	}
	s.inc(count)
}

// Values returns a slice of uint8 holding the values stored
// in the sequence, right filling the slice if needed.
func (s *Sequence) Values() []uint8 {
	data := make([]uint8, length)
	index := 0
	for i := indexData; i < len(s.data); i += 2 {
		n, v := decode(s.data[i], s.data[i+1])
		for j := 0; j < int(n); j++ {
			data[index] = v
			index++
		}
	}
	if index < length {
		for i := index; i < length; i++ {
			data[i] = FlagUnknown
		}
	}
	return data
}

// Bytes returns the encoded sequence.
func (s *Sequence) Bytes() []byte {
	x := make([]byte, len(s.data))
	copy(x, s.data)
	return x
}

// inc increments the count of values stored in the sequence.
func (s *Sequence) inc(x uint16) {
	s.count += x
	s.data[indexCounter], s.data[indexCounter+1] = byte(s.count), byte(s.count>>8)
}

// last returns the length and value of the last series in the sequence.
func (s *Sequence) last() (uint16, uint8) {
	i := len(s.data) - 2
	return decode(s.data[i], s.data[i+1])
}

// interval returns the closed time interval associated to the sequence.
func (s *Sequence) interval() interval {
	return interval{start: s.ts, end: s.ts + (length-1)*frequency}
}

// clone returns a copy of s.
func (s *Sequence) clone() *Sequence {
	clone := &Sequence{
		ts:    s.ts,
		count: s.count,
		data:  make([]uint8, len(s.data)),
	}
	copy(clone.data, s.data)
	return clone
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
