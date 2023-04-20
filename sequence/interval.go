package sequence

// interval represents a closed interval.
type interval struct {
	start int64
	end   int64
}

// intersect returns the intersection with the closed interval y. If no
// intersection is found, the second value returned by the method
// is false.
func (x interval) intersect(y interval) (interval, bool) {
	if x.start <= y.start {
		if x.end >= y.end {
			return y, true
		}
		if x.end >= y.start {
			return interval{start: y.start, end: x.end}, true
		}
	} else if x.start <= y.end {
		if x.end >= y.end {
			return interval{start: x.start, end: y.end}, true
		}
		return x, true
	}
	return interval{}, false
}
