package sequence_test

import (
	"fmt"
	"time"

	"github.com/geofduf/run-length/sequence"
)

func ExampleSequence_Query() {
	start := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, 1, 2, 0, 9, 59, 0, time.UTC)

	s := sequence.NewSequenceFromValues(start, 60, []uint8{1, 1, 1, 0, 0, 0, 1, 1, 1})

	qs, err := s.Query(start, end, 5*time.Minute)
	if err != nil {
		fmt.Println("Query failed:", err)
	}

	ts := int(qs.Timestamp)
	f := int(qs.Frequency)

	for i := range qs.Count {
		fmt.Printf("\n%d %d %d", ts+i*f, qs.Count[i], qs.Sum[i])
	}
	// Output:
	// 946771200 5 3
	// 946771500 4 3
}

func ExampleQuerySet_Serialize() {
	start := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, 1, 2, 0, 9, 59, 0, time.UTC)

	s := sequence.NewSequenceFromValues(start, 60, []uint8{1, 1, 1, 0, 0, 0, 1, 1, 1})

	qs, err := s.Query(start, end, 5*time.Minute)
	if err != nil {
		fmt.Println("Query failed:", err)
	}

	flag := sequence.SerializeCount | sequence.SerializeMean

	fmt.Printf("%s", qs.Serialize("2006-01-02 15:04", time.UTC, 2, flag))
	// Output: [{"date":"2000-01-02 00:00","count":5,"mean":0.60},{"date":"2000-01-02 00:05","count":4,"mean":0.75}]
}
