// Package vector contains the core operation for sorting.
package vector

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

// Vector holds a slice of Elements for sorting. The NewElement is called each
// time a new item from the file is read or inserted into the slice. The Less
// function should return true if the first element is lower than the second.
type Vector struct {
	NewElement func(value string) Element
	Less       func(v1, v2 Element) bool
	s          []Element
}

// Get returns the element at the given index.
func (v *Vector) Get(i int) Element {
	return v.s[i]
}

// Len returns the length of the vector.
func (v *Vector) Len() int {
	return len(v.s)
}

// PushBack pushes a new element to the end of the vector.
func (v *Vector) PushBack(value string) {
	v.s = append(v.s, v.NewElement(value))
}

func (v *Vector) Dump(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return errors.Errorf("failed creating file: %s", err)
	}
	defer file.Close()
	datawriter := bufio.NewWriter(file)
	defer datawriter.Flush()

	for _, data := range v.s {
		_, err = datawriter.WriteString(data.Value() + "\n")
		if err != nil {
			return errors.Errorf("failed writing file: %s", err)
		}
	}
	return nil
}

// FrontShift shifts the vector one element forward.
func (v *Vector) FrontShift() {
	v.s = v.s[1:]
}

// Sort Perform a binary search to find where to put a value in a vector. Ascending order.
func Sort(v *Vector, line, sep string, pos int) error {
	num := strings.Split(line, sep)
	if len(num) < pos {
		return fmt.Errorf("could not find position %d in %q", pos, line)
	}
	val := v.NewElement(num[pos])
	found := sort.Search(v.Len(), func(i int) bool {
		return !v.Less(v.Get(i), val)
	})
	v.s = append(v.s[:found], append([]Element{v.NewElement(line)}, v.s[found:]...)...)
	return nil
}
