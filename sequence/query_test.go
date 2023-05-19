package sequence

import (
	"fmt"
	"testing"
	"time"
)

func TestSequenceQueryValues(t *testing.T) {
	x, _ := time.Parse("2006-01-02 15:04:05", testSequenceTimestamp)
	s := NewSequenceFromValues(x, testSequenceFrequency, testValues)
	f := int64(s.frequency)
	type result struct {
		values    []uint8
		timestamp int64
	}
	tests := []struct {
		id    int
		start time.Time
		end   time.Time
		want  result
	}{
		{1, shift(s, -5, 0), shift(s, 25, -1), result{append(testValues, []uint8{2, 2, 2, 2, 2}...), s.ts}},
		{2, shift(s, -5, 0), shift(s, 6, -1), result{[]uint8{1, 1, 1, 1, 1, 0}, s.ts}},
		{3, shift(s, 4, 0), shift(s, 10, 0), result{[]uint8{1, 0, 0, 0, 0, 0, 1}, s.ts + 4*f}},
		{4, shift(s, 15, -1), shift(s, 21, 0), result{[]uint8{2, 2, 2, 2, 0, 2, 2}, s.ts + 15*f}},
		{5, shift(s, 15, 1), shift(s, 21, 0), result{[]uint8{2, 2, 2, 0, 2, 2}, s.ts + 16*f}},
	}
	for _, tt := range tests {
		prefix := fmt.Sprintf("test %d (%s, %s)", tt.id, tt.start, tt.end)
		var got result
		var err error
		got.values, got.timestamp, err = s.queryValues(tt.start, tt.end)
		if err != nil {
			t.Fatalf("%s: got error %s, want error nil", prefix, err)
		}
		if !assertValuesEqual(got.values, tt.want.values) {
			t.Fatalf("%s:\ngot  %v\nwant %v", prefix, got.values, tt.want.values)
		}
		if got.timestamp != tt.want.timestamp {
			t.Fatalf("%s: got %d, want %d", prefix, got.timestamp, tt.want.timestamp)
		}
	}
}

func TestSequenceQuery(t *testing.T) {
	x, _ := time.Parse("2006-01-02 15:04:05", testSequenceTimestamp)
	s := NewSequenceFromValues(x, testSequenceFrequency, testValues)
	f := int64(testSequenceFrequency)
	tests := []struct {
		id       int
		start    time.Time
		end      time.Time
		interval time.Duration
		want     QuerySet
	}{
		{
			1,
			shift(s, -5, -1),
			shift(s, 25, -1),
			time.Duration(f*5) * time.Second,
			QuerySet{shift(s, -5, -1).Unix(), f * 5, []int64{0, 5, 0, 5, 0, 0, 0}, []int64{0, 5, 5, 5, 1, 0, 0}},
		},
		{
			2,
			shift(s, 3, -1),
			shift(s, 12, 1),
			time.Duration(f*5) * time.Second,
			QuerySet{shift(s, 3, -1).Unix(), f * 5, []int64{2, 3}, []int64{5, 5}},
		},
		{
			3,
			shift(s, 5, -1),
			shift(s, 12, 1),
			time.Duration(f*3) * time.Second,
			QuerySet{shift(s, 5, -1).Unix(), f * 3, []int64{0, 1, 2}, []int64{3, 3, 2}},
		},
		{
			4,
			shift(s, -15, -1),
			shift(s, 80, -1),
			time.Duration(f*25) * time.Second,
			QuerySet{shift(s, -15, -1).Unix(), f * 25, []int64{5, 5, 0, 0}, []int64{10, 6, 0, 0}},
		},
	}
	for _, tt := range tests {
		prefix := fmt.Sprintf("test %d (%s, %s, %d)", tt.id, tt.start, tt.end, int(tt.interval.Seconds()))
		got, err := s.Query(tt.start, tt.end, tt.interval)
		if err != nil {
			t.Fatalf("%s: got error %s, want error nil", prefix, err)
		}
		if !assertQuerySetEqual(got, tt.want) {
			t.Fatalf("%s:\ngot  %+v\nwant %+v", prefix, got, tt.want)
		}
	}

}

func shift(s *Sequence, steps, seconds int) time.Time {
	return time.Unix(s.ts, 0).Add(time.Duration(steps*int(s.frequency)+seconds) * time.Second)
}

func assertValuesEqual[T uint8 | int64](x, y []T) bool {
	if len(x) != len(y) {
		return false
	}
	for i := 0; i < len(x); i++ {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func assertQuerySetEqual(x, y QuerySet) bool {
	if x.Timestamp != y.Timestamp || x.Frequency != y.Frequency {
		return false
	}
	return assertValuesEqual(x.Sum, y.Sum) && assertValuesEqual(x.Count, y.Count)
}
