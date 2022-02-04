package vector

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// AllocateTableVector returns an allocation function that sorts the file on
// the pos element with the sep separator.
func AllocateTableVector(sep string, pos int) func(int) *Vector {
	return func(size int) *Vector {
		return &Vector{
			s: make([]Element, 0, size),
			NewElement: func(value string) Element {
				num := strings.Split(value, sep)[pos]
				i, err := strconv.Atoi(num)
				if err != nil {
					panic(errors.Wrap(err, "converting value from string"))
				}

				return &element{
					line: value,
					i:    i,
				}
			},
			Less: func(v1, v2 Element) bool {
				// nolint:forcetypeassert // we already know the type.
				return v1.(*element).i < v2.(*element).i
			},
		}
	}
}
