package sequence

import (
	"errors"
	"time"
)

// A QuerySet represents a time series obtained by applying a set of
// aggregate functions on values grouped in terms of time.
type QuerySet struct {
	// Timestamp specifies the unix time associated to
	// the first element of the time series.
	Timestamp int64

	// Frequency specifies the frequency of the
	// time series in number of seconds.
	Frequency int64

	// Sum holds the sum of valid values in
	// each group.
	Sum []int64

	// Count holds the number of valid values in
	// in each group.
	Count []int64
}

// Values returns raw values stored in the sequence using start and end as
// closed interval filter. The second return value is the Unix time associated to
// the first element of the slice. The method returns an error if the interval filter
// and the sequence don't overlap.
func (s *Sequence) Values(start, end time.Time) ([]uint8, int64, error) {
	if start.After(end) {
		return []uint8{}, 0, errors.New("invalid arguments")
	}

	r, ok := s.interval().intersect(interval{start: start.Unix(), end: end.Unix()})
	if !ok {
		return []uint8{}, 0, errors.New("out of bounds")
	}

	f := int64(s.frequency)

	x := ceilInt64(r.start-s.ts, f) / f
	y := (r.end - s.ts) / f

	data := make([]uint8, y-x+1)
	srcIndex, dstIndex := int64(0), int64(0)

	p := 0
	for p < len(s.data) {
		n, v, bytesRead := s.next(p)
		p += bytesRead

		count := int64(n)

		if srcIndex+count < x {
			srcIndex += count
			continue
		}

		offset := int64(0)
		if dstIndex == 0 {
			offset = x - srcIndex
		}

		if y < srcIndex+count {
			for i := int64(0); i <= y-srcIndex-offset; i++ {
				data[dstIndex] = v
				dstIndex++
			}
			break
		}

		for i := int64(0); i < count-offset; i++ {
			data[dstIndex] = v
			dstIndex++
		}

		srcIndex += count
	}

	for i := dstIndex; i < int64(len(data)); i++ {
		data[i] = StateUnknown
	}

	return data, s.ts + x*f, nil
}

// Query executes a query on s using start, end as closed interval filter
// and d as grouping interval. The grouping interval is silently floored to
// the frequency of s. Groups are aligned on start. It returns a QuerySet covering
// all groups between start and end.
func (s *Sequence) Query(start, end time.Time, d time.Duration) (QuerySet, error) {
	// TODO: review + clean method
	if start.After(end) {
		return QuerySet{}, errors.New("invalid time filter")
	}

	f := int64(s.frequency)

	aggregation := int64(d.Seconds()) / f

	if aggregation < 1 {
		return QuerySet{}, errors.New("invalid grouping interval")
	}

	ts := start.Unix()

	numberOfValues := (end.Unix()-ts)/f/aggregation + 1

	qs := QuerySet{
		Timestamp: ts,
		Frequency: f * aggregation,
		Sum:       make([]int64, numberOfValues),
		Count:     make([]int64, numberOfValues),
	}

	r, ok := s.interval().intersect(interval{start: start.Unix(), end: end.Unix()})
	if !ok {
		return qs, nil
	}

	x := ceilInt64(r.start-s.ts, f) / f
	y := (r.end - s.ts) / f

	src := int64(0)
	shift := int64(0)
	if ts < s.ts {
		shift = (s.ts - ts) / f
	}

	p := 0
	for p < len(s.data) {
		n, v, bytesRead := s.next(p)
		p += bytesRead

		next := src + int64(n)

		if x >= next || v == StateUnknown {
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
			dst := (shift + src - x) / aggregation
			n := aggregation
			if first {
				n -= (shift + src - x) % aggregation
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
