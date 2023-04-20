package sequence

import (
	"testing"
)

func TestIntervalIntersect(t *testing.T) {
	tests := []struct {
		id   int
		x    interval
		y    interval
		want interval
	}{
		{1, interval{2, 9}, interval{3, 5}, interval{3, 5}},
		{2, interval{3, 5}, interval{2, 9}, interval{3, 5}},
		{3, interval{2, 9}, interval{1, 7}, interval{2, 7}},
		{4, interval{1, 7}, interval{2, 9}, interval{2, 7}},
	}
	for _, tt := range tests {
		got, ok := tt.x.intersect(tt.y)
		if !ok {
			t.Fatalf("test %d: expected an intersection", tt.id)
		}
		if got != tt.want {
			t.Fatalf("test %d: got %v, want %v", tt.id, got, tt.want)
		}
	}
	x := interval{1, 4}
	y := interval{5, 9}
	if got, ok := x.intersect(y); ok {
		t.Fatalf("expected no intersection, got %v", got)
	}
	if got, ok := y.intersect(x); ok {
		t.Fatalf("expected no intersection, got %v", got)
	}
}
