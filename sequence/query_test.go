package sequence

import (
	"testing"
	"time"
)

var values = dataset()

func TestSequenceQuery(t *testing.T) {
	ts := newTime("2018-01-01 00:00:00")
	s := NewSequenceFromValues(ts, values)
	tests := []struct {
		id    int
		start time.Time
		end   time.Time
		want  []uint8
	}{
		{1, shift(ts, 0, -5, 0), shift(ts, 1, 5, 0), values},
		{2, shift(ts, 0, -5, 0), shift(ts, 0, 6, -1), []uint8{1, 1, 1, 1, 1, 0}},
		{3, shift(ts, 0, 4, 0), shift(ts, 0, 10, 0), []uint8{1, 0, 0, 0, 0, 0, 1}},
		{4, shift(ts, 1, -5, -1), shift(ts, 1, 5, 0), []uint8{2, 2, 2, 2, 0}},
		{5, shift(ts, 1, -5, 1), shift(ts, 1, 5, 0), []uint8{2, 2, 2, 0}},
	}
	for _, tt := range tests {
		got, err := s.Query(tt.start, tt.end)
		if err != nil {
			t.Errorf("test %d (%s, %s): got error %s, want error nil", tt.id, tt.start, tt.end, err)
		} else if len(got) != len(tt.want) {
			t.Errorf("test %d (%s, %s): got slice length %d, want %d", tt.id, tt.start, tt.end, len(got), len(tt.want))
		} else if !assertValuesEqual(got, tt.want) {
			t.Errorf("test %d (%s, %s): []uint8 slices are not equal", tt.id, tt.start, tt.end)
		}
	}
}

func dataset() []uint8 {
	values := newSliceOfValues(length, FlagUnknown)
	copy(values, []uint8{1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1})
	values[length-1] = 0
	return values
}

func shift(t time.Time, sequences, steps, seconds int) time.Time {
	return t.Add(time.Duration(sequences*length*frequency+steps*frequency+seconds) * time.Second)
}

func newTime(x string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", x)
	if err != nil {
		panic(err)
	}
	return t
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
