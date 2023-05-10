package sequence

import (
	"fmt"
	"testing"
	"time"
)

func TestSequenceQuery(t *testing.T) {
	x, _ := time.Parse("2006-01-02 03:04:05", testSequenceTimestamp)
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

func shift(s *Sequence, steps, seconds int) time.Time {
	return time.Unix(s.ts, 0).Add(time.Duration(steps*int(s.frequency)+seconds) * time.Second)
}

func assertValuesEqual(x, y []uint8) bool {
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
