package sequence

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestSerialize(t *testing.T) {
	x, _ := time.Parse("2006-01-02 15:04:05", testSequenceTimestamp)
	q := QueryGroupSet{x.Unix(), 300, []int64{5, 0, 1}, []int64{5, 0, 4}}
	tests := []struct {
		id        int
		layout    string
		precision int
		flag      int
		want      []string
	}{
		{
			1,
			"2006-01-02 15:04:05",
			4,
			SerializeCount | SerializeMean,
			[]string{
				"[",
				`{"date":"2000-01-02 03:04:05","count":5,"mean":1.0000},`,
				`{"date":"2000-01-02 03:09:05","count":0,"mean":null},`,
				`{"date":"2000-01-02 03:14:05","count":4,"mean":0.2500}`,
				"]",
			},
		},
		{
			2,
			"15:04:05",
			2,
			SerializeSum | SerializeMean,
			[]string{
				"[",
				`{"date":"03:04:05","sum":5,"mean":1.00},`,
				`{"date":"03:09:05","sum":null,"mean":null},`,
				`{"date":"03:14:05","sum":1,"mean":0.25}`,
				"]",
			},
		},
	}
	for _, tt := range tests {
		got := q.Serialize(tt.layout, time.UTC, tt.precision, tt.flag)
		if v := []byte(strings.Join(tt.want, "")); !bytes.Equal(got, v) {
			t.Fatalf("test %d:\ngot  %s\nwant %s", tt.id, got, v)
		}
	}
}
