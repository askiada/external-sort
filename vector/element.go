package vector

// An Element should return the final value that should be retuned when the
// final row is back to the caller.
type Element interface {
	Value() string
}

type element struct {
	line string
	i    int
}

func (e *element) Value() string {
	return e.line
}

func (e *element) String() string { return e.line }
