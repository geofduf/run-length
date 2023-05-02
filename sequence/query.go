package sequence

import (
	"errors"
	"fmt"
	"time"
)

// A QuerySet represents a set of values obtained by performing a query
// against one or several sequences.
type QuerySet struct {
	// Timestamp specifies the unix time associated to
	// the first element of Values.
	Timestamp int64

	// Values holds the values returned by the query.
	Values []uint8
}

// A QueryGroupSet represents a time series obtained by applying a set of
// aggregate functions on values grouped in terms of time.
type QueryGroupSet struct {
	// Timestamp specifies the unix time associated to
	// the first element of the time series.
	Timestamp int64

	// Frequency specifies the frequency of the
	// time series in number of seconds.
	Frequency int

	// Sum holds the sum of valid values in
	// each group.
	Sum []int

	// Count holds the number of valid values in
	// in each group.
	Count []int
}

// Query returns a QuerySet of s using start and end as closed interval filter.
func (s *Sequence) Query(start, end time.Time) (QuerySet, error) {
	if start.After(end) {
		return QuerySet{}, errors.New("invalid arguments")
	}

	r, ok := s.interval().intersect(interval{start: start.Unix(), end: end.Unix()})
	if !ok {
		return QuerySet{}, errors.New("out of bounds")
	}

	x := int(ceilInt64(r.start-s.ts, frequency)) / frequency
	y := int((r.end - s.ts)) / frequency
	data := make([]uint8, y-x+1)
	srcIndex := 0
	dstIndex := 0

	for p := indexData; p < len(s.data); p += 2 {
		n, v := decode(s.data[p], s.data[p+1])
		count := int(n)

		if srcIndex+count < x {
			srcIndex += count
			continue
		}

		offset := 0
		if dstIndex == 0 {
			offset = x - srcIndex
		}

		if y < srcIndex+count {
			for i := 0; i <= y-srcIndex-offset; i++ {
				data[dstIndex] = v
				dstIndex++
			}
			break
		} else {
			for i := 0; i < count-offset; i++ {
				data[dstIndex] = v
				dstIndex++
			}
		}

		srcIndex += count
	}

	for i := dstIndex; i < len(data); i++ {
		data[i] = FlagUnknown
	}

	return QuerySet{Values: data, Timestamp: ceilInt64(r.start, frequency)}, nil
}

// QueryGroup returns a QueryGroupSet of s using start and end as closed interval filter and
// d as grouping interval. The grouping interval is silently floored to the second.
// Following the curent implementation it must be greater or equal to 120 seconds and an
// exact divisor of of 10080 seconds.
func (s *Sequence) QueryGroup(start, end time.Time, d time.Duration) (QueryGroupSet, error) {
	// TODO: review + clean method
	if start.After(end) {
		return QueryGroupSet{}, errors.New("invalid time filter")
	}

	aggregation := int(d.Seconds()) / frequency

	if aggregation < 2 || aggregation > length || length%aggregation != 0 {
		return QueryGroupSet{}, errors.New("invalid grouping interval")
	}

	r, ok := s.interval().intersect(interval{start: start.Unix(), end: end.Unix()})
	if !ok {
		return QueryGroupSet{}, errors.New("out of bounds")
	}

	x := int(ceilInt64(r.start-s.ts, frequency)) / frequency
	y := int(r.end-s.ts) / frequency

	numberOfValues := y/aggregation - x/aggregation + 1

	qs := QueryGroupSet{
		Timestamp: s.ts + floorInt64(int64(x*frequency), int64(frequency*aggregation)),
		Frequency: frequency * aggregation,
		Sum:       make([]int, numberOfValues),
		Count:     make([]int, numberOfValues),
	}

	src := 0
	shift := (x % aggregation) - x

	for p := indexData; p < len(s.data); p += 2 {
		n, v := decode(s.data[p], s.data[p+1])
		next := src + int(n)

		if x >= next || v == FlagUnknown {
			src = next
			continue
		}

		first := true

		if x > src {
			src = x
		}

		target := next
		if y < next {
			target = y + 1
		}

		for src < target {
			dst := (shift + src) / aggregation
			n := aggregation
			if first {
				n -= src % aggregation
				first = false
			}
			if src+n > target {
				n = target - src
			}
			if v == 1 {
				qs.Sum[dst] += n
			}
			qs.Count[dst] += n
			src += n
		}

		if next > y {
			break
		}

	}

	return qs, nil
}

// Query returns a QuerySet of interval [start, end] built by querying each element
// of a slice of encoded sequences using start and end as closed interval filter.
func Query(encodedSequences [][]byte, start, end time.Time) (QuerySet, error) {
	if start.After(end) {
		return QuerySet{}, errors.New("invalid arguments")
	}
	sequences := make([]*Sequence, len(encodedSequences))
	for i, v := range encodedSequences {
		s, err := NewSequenceFromBytes(v)
		if err != nil {
			return QuerySet{}, fmt.Errorf("%s, at index %d", err, i)
		}
		sequences[i] = s
	}
	return query(sequences, start, end)
}

// query returns a QuerySet of interval [start, end] built by querying each element
// of sequences using start and end as closed interval filter.
func query(sequences []*Sequence, start, end time.Time) (QuerySet, error) {
	if start.After(end) {
		return QuerySet{}, errors.New("invalid arguments")
	}
	x := ceilInt64(start.Unix(), frequency)
	y := floorInt64(end.Unix(), frequency)
	data := newSliceOfValues(int(y/frequency-x/frequency+1), FlagUnknown)
	for _, s := range sequences {
		iv := s.interval()
		if x <= iv.start && y >= iv.end {
			values := s.Values()
			m := int((s.ts - x) / frequency)
			n := m + len(values)
			copy(data[m:n], values)
			continue
		}
		qs, err := s.Query(start, end)
		if err != nil {
			continue
		}
		m := 0
		if s.ts > x {
			m = int((s.ts - x) / frequency)
		}
		n := m + len(qs.Values)
		copy(data[m:n], qs.Values)
	}
	return QuerySet{Timestamp: x, Values: data}, nil
}

// ceilInt64 returns the least integer value greater than or
// equal to x that is a multiple of step.
func ceilInt64(x int64, step int64) int64 {
	r := x % step
	if r != 0 {
		return x + step - r
	}
	return x
}

// floorInt64 returns the greatest integer value less than or
// equal to x that is a multiple of step.
func floorInt64(x int64, step int64) int64 {
	return x - x%step
}

// newSliceOfValues returns a slice of length n with all its values set to x.
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
