package vector

type element struct {
	line string
	i    int
}

func (e *element) Less(other Element) bool {
	return e.i < other.(*element).i
}

func (e *element) Value() string {
	return e.line
}

func (e *element) String() string { return e.line }
