package key

type Key interface {
	// Less returns wether the key is smaller than v2
	Less(v2 Key) bool
	String() string
	FromString(string) error
}
