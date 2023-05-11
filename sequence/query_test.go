package sequence

import (
	"fmt"
	"testing"
	"time"
)

func TestSequenceQuery(t *testing.T) {
	x, _ := time.Parse("2006-01-02 15:04:05", testSequenceTimestamp)
	s := NewSequenceFromValues(x, testSequenceFrequency, testValues)
	f := int64(s.frequency)
	tests := []struct {
		id    int
		start time.Time
		end   time.Time
		want  QuerySet
	}{
		{1, shift(s, -5, 0), shift(s, 25, -1), QuerySet{s.ts, f, append(testValues, []uint8{2, 2, 2, 2, 2}...)}},
		{2, shift(s, -5, 0), shift(s, 6, -1), QuerySet{s.ts, f, []uint8{1, 1, 1, 1, 1, 0}}},
		{3, shift(s, 4, 0), shift(s, 10, 0), QuerySet{s.ts + 4*f, f, []uint8{1, 0, 0, 0, 0, 0, 1}}},
		{4, shift(s, 15, -1), shift(s, 21, 0), QuerySet{s.ts + 15*f, f, []uint8{2, 2, 2, 2, 0, 2, 2}}},
		{5, shift(s, 15, 1), shift(s, 21, 0), QuerySet{s.ts + 16*f, f, []uint8{2, 2, 2, 0, 2, 2}}},
	}
	for _, tt := range tests {
		prefix := fmt.Sprintf("test %d (%s, %s)", tt.id, tt.start, tt.end)
		got, err := s.Query(tt.start, tt.end)
		if err != nil {
			t.Fatalf("%s: got error %s, want error nil", prefix, err)
		}
		if !assertValuesEqual(got.Values, tt.want.Values) {
			t.Fatalf("%s:\ngot  %v\nwant %v", prefix, got.Values, tt.want.Values)
		}
		if got.Timestamp != tt.want.Timestamp {
			t.Fatalf("%s: got %d, want %d", prefix, got.Timestamp, tt.want.Timestamp)
		}
		if got.Frequency != tt.want.Frequency {
			t.Fatalf("%s: got %d, want %d", prefix, got.Frequency, tt.want.Frequency)
		}
	}
}

func TestSequenceQueryGroup(t *testing.T) {
	x, _ := time.Parse("2006-01-02 15:04:05", testSequenceTimestamp)
	s := NewSequenceFromValues(x, testSequenceFrequency, testValues)
	f := int64(testSequenceFrequency)
	tests := []struct {
		id       int
		start    time.Time
		end      time.Time
		interval time.Duration
		want     QueryGroupSet
	}{
		{
			1,
			shift(s, -5, -1),
			shift(s, 25, -1),
			time.Duration(f*5) * time.Second,
			QueryGroupSet{s.ts, f * 5, []int64{5, 0, 5, 0, 0}, []int64{5, 5, 5, 1, 0}},
		},
		{
			2,
			shift(s, 3, -1),
			shift(s, 12, 1),
			time.Duration(f*5) * time.Second,
			QueryGroupSet{s.ts, f * 5, []int64{2, 0, 3}, []int64{2, 5, 3}},
		},
		{
			3,
			shift(s, 5, -1),
			shift(s, 12, 1),
			time.Duration(f*3) * time.Second,
			QueryGroupSet{s.ts + f*3, f * 3, []int64{0, 0, 2, 1}, []int64{1, 3, 3, 1}},
		},
		{
			4,
			shift(s, -5, -1),
			shift(s, 80, -1),
			time.Duration(f*25) * time.Second,
			QueryGroupSet{s.ts, f * 25, []int64{10, 0, 0, 0}, []int64{16, 0, 0, 0}},
		},
	}
	for _, tt := range tests {
		prefix := fmt.Sprintf("test %d (%s, %s, %d)", tt.id, tt.start, tt.end, int(tt.interval.Seconds()))
		got, err := s.QueryGroup(tt.start, tt.end, tt.interval)
		if err != nil {
			t.Fatalf("%s: got error %s, want error nil", prefix, err)
		}
		if !assertQueryGroupSetEqual(got, tt.want) {
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

func assertQueryGroupSetEqual(x, y QueryGroupSet) bool {
	if x.Timestamp != y.Timestamp || x.Frequency != y.Frequency {
		return false
	}
	return assertValuesEqual(x.Sum, y.Sum) && assertValuesEqual(x.Count, y.Count)
}
