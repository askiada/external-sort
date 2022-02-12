package key

type Key interface {
	Get() interface{}
	Less(v2 Key) bool
}
