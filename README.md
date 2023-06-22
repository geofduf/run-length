# RUN-LENGTH

Run-length encoding of regularly spaced binary states.

## sequence package

The `sequence` package defines the type Sequence, with methods for adding and querying values, and
the type Store, with methods for interacting with a collection of sequences.

**Documentation:**

https://pkg.go.dev/github.com/geofduf/run-length/sequence

**Example usage:**

Initialize an empty sequence by specifying its timestamp and frequency. The following
example shows how to initialize a sequence that starts on January 1, 2023 00:00:00 UTC and has a
frequency of 60 seconds.

```go
s := sequence.New(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), 60)
```

Add a value to a sequence by calling Sequence.Add() or Sequence.Roll(), passing the
timestamp and the value. The entry will be added to the sequence at the corresponding interval.

```go
if err := s.Add(time.Date(2023, 1, 1, 0, 4, 59, 0, time.UTC), sequence.StateActive); err != nil {
    // ...
}
if err := s.Add(time.Date(2023, 1, 1, 0, 10, 1, 0, time.UTC), sequence.StateInactive); err != nil {
    // ...
}
if err := s.Add(time.Date(2023, 1, 1, 0, 11, 0, 0, time.UTC), sequence.StateActive); err != nil {
    // ...
}
```

Use Sequence.All() to retrieve the raw values stored in the sequence. Use the timestamp and
frequency of the sequence to associate a timestamp to each value according to its offset.
Most sequence methods that aim at retrieving values follow a similar pattern.

```go
timestamp := time.Unix(s.Timestamp(), 0).UTC()
offset := time.Duration(s.Frequency()) * time.Second

for _, v := range s.All() {
    var label string
    switch v {
    case sequence.StateInactive:
        label = "inactive"
    case sequence.StateActive:
        label = "active"
    case sequence.StateUnknown:
        label = "unknown"
    }
    fmt.Printf("%s %d (%s)\n", timestamp.Format("2006-01-02 15:04:05"), v, label)
    timestamp = timestamp.Add(offset)
}
```

Running the above code on our sequence would output :

```
2023-01-01 00:00:00 2 (unknown)
2023-01-01 00:01:00 2 (unknown)
2023-01-01 00:02:00 2 (unknown)
2023-01-01 00:03:00 2 (unknown)
2023-01-01 00:04:00 1 (active)
2023-01-01 00:05:00 2 (unknown)
2023-01-01 00:06:00 2 (unknown)
2023-01-01 00:07:00 2 (unknown)
2023-01-01 00:08:00 2 (unknown)
2023-01-01 00:09:00 2 (unknown)
2023-01-01 00:10:00 0 (inactive)
2023-01-01 00:11:00 1 (active)
```

Use Sequence.Query() to execute an aggregate query on the sequence. The following code shows how
to execute a query with a grouping interval of 5 minutes and print the result as JSON using
QuerySet.Serialize().

```go
start := time.Unix(s.Timestamp(), 0).UTC()
end := start.Add(899 * time.Second)

qs, err := s.Query(start, end, 5*time.Minute)
if err != nil {
    // ...
}

flag := sequence.SerializeCount | sequence.SerializeMean
fmt.Printf("%s\n", qs.Serialize("2006-01-02 15:04", time.UTC, 2, flag))
```

Running the above code on our sequence would output :

```
[{"date":"2023-01-01 00:00","count":1,"mean":1.00},{"date":"2023-01-01 00:05","count":0,"mean":null},{"date":"2023-01-01 00:10","count":2,"mean":0.50}]
```