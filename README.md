# RUN-LENGTH

Run-length encoding of regularly spaced binary states.

# Overview

The package sequence defines the type Sequence, with methods for adding and querying values, and
the type Store, with methods for interacting with a collection of sequences.

A Sequence is defined by a starting timestamp, a frequency, and a maximum length.
It follows an append-only pattern and supports automatic discarding of oldest values when
its capacity is reached. It can be exported as []byte, easing integration with
storage systems.

A Store is essentially a wrapper around a map of sequences that provides convenience methods
safe to use from multiple goroutines.

# Documentation

https://pkg.go.dev/github.com/geofduf/run-length/sequence