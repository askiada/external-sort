package vector

// AllocateStringVector returns a Vector that can sort based on strings.
func AllocateStringVector(size int) *Vector {
	return &Vector{
		s: make([]Element, 0, size),
		NewElement: func(value string) Element {
			return &element{
				line: value,
			}
		},
		Less: func(v1, v2 Element) bool { return v1.Value() < v2.Value() },
	}
}
