package key

// Key define the interface to compare keys to sort.
type Key interface {
	Equal(v2 Key) bool
	// Less returns wether the key is smaller than v2
	Less(v2 Key) bool
}
