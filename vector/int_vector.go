package vector

import (
	"strconv"

	"github.com/pkg/errors"
)

func AllocateIntVector(size int) *Vector {
	return &Vector{
		s: make([]Element, 0, size),
		NewElement: func(value string) Element {
			i, err := strconv.Atoi(value)
			if err != nil {
				panic(errors.Wrap(err, "converting value from string"))
			}

			return &element{
				line: value,
				i:    i,
			}
		},
		// nolint:forcetypeassert // we already know the type.
		Less: func(v1, v2 Element) bool { return v1.(*element).i < v2.(*element).i },
	}
}
