/*
Package sequence implements run-length encoding of regularly spaced binary states.
It defines the type Sequence, with methods for adding and querying values, and
the type Store, with methods for interacting with a collection of sequences.

A Sequence is defined by a starting timestamp, a frequency, and a maximum length.
It follows an append-only pattern and supports automatic discarding of oldest values when
its capacity is reached. It can be exported as []byte, easing integration with
storage systems.

Sequence values are represented as uint8. The only supported values are:

	const (
	  StateInactive uint8 = iota // 0b00
	  StateActive                // 0b01
	  StateUnknown               // 0b10
	)

Passing unsupported values to functions or methods will result in undefined behavior.

A Store is essentially a wrapper around a map of sequences that provides convenience methods
safe to use from multiple goroutines.
*/
package sequence
