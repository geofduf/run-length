package sequence

import (
	"errors"
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
