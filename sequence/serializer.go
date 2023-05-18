package sequence

import (
	"strconv"
	"time"
)

// These flags define which values to include in a serialized output.
const (
	SerializeCount = 1 << iota // number of valid values in group
	SerializeSum               // sum of values in group
	SerializeMean              // mean value of group
)

const (
	serializerBasePrefix  = '['
	serializerRowPrefix   = `{"date":`
	serializerCountPrefix = `,"count":`
	serializerSumPrefix   = `,"sum":`
	serializerMeanPrefix  = `,"mean":`
	serializerRowSuffix   = "},"
	serializerBaseSuffix  = ']'
)

// serialize is a convenience function that returns a JSON encoding of the time series
// using layout as time layout, loc as time location, n as precision level for
// float values and flag to define which values to include in the serialized output.
func serialize(q QueryGroupSet, layout string, loc *time.Location, n int, flag int) []byte {
	if len(q.Count) == 0 {
		return []byte("[]")
	}
	layout = `"` + layout + `"`
	var count, sum, mean bool
	var rowNull string
	approxRowSize := 10 + len(layout)
	if flag&SerializeCount != 0 {
		rowNull += serializerCountPrefix + "0"
		approxRowSize += 14
		count = true
	}
	if flag&SerializeSum != 0 {
		rowNull += serializerSumPrefix + "null"
		approxRowSize += 12
		sum = true
	}
	if flag&SerializeMean != 0 {
		rowNull += serializerMeanPrefix + "null"
		approxRowSize += 10 + n
		mean = true
	}
	rowNull += serializerRowSuffix
	buf := make([]byte, 0, 2+len(q.Count)*approxRowSize)
	t := time.Unix(q.Timestamp, 0).In(loc)
	buf = append(buf, serializerBasePrefix)
	for i := 0; i < len(q.Count); i++ {
		buf = append(buf, serializerRowPrefix...)
		buf = append(buf, t.Format(layout)...)
		if q.Count[i] == 0 {
			buf = append(buf, rowNull...)
		} else {
			if count {
				buf = append(buf, serializerCountPrefix...)
				buf = strconv.AppendInt(buf, q.Count[i], 10)
			}
			if sum {
				buf = append(buf, serializerSumPrefix...)
				buf = strconv.AppendInt(buf, q.Sum[i], 10)
			}
			if mean {
				buf = append(buf, serializerMeanPrefix...)
				buf = strconv.AppendFloat(buf, float64(q.Sum[i])/float64(q.Count[i]), 'f', n, 64)
			}
			buf = append(buf, serializerRowSuffix...)
		}
		t = t.Add(time.Duration(q.Frequency) * time.Second)
	}
	buf[len(buf)-1] = serializerBaseSuffix
	return buf
}

// Serialize is a convenience method that returns a JSON encoding of the time series
// using layout as time layout, loc as time location, n as precision level for
// float values and flag to define which values to include in the serialized output.
func (q QueryGroupSet) Serialize(layout string, loc *time.Location, n int, flag int) []byte {
	return serialize(q, layout, loc, n, flag)
}
