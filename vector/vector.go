package vector

import (
	"fmt"
	"sort"
	"strings"
)

type Element interface {
	Value() string
	Less(other Element) bool
}

type Vector interface {
	// Get Access i-th element
	Get(i int) Element
	// PushBack Add item at the end
	PushBack(value string) error
	// Less Returns wether v1 is smaller than v2
	Less(v1, v2 Element) bool
	// Dump Create a file and store the underluing data
	Dump(filename string) error
	// FrontShift Remove the first element
	FrontShift()
	// End Length of the Vector
	End() int
	// insert Insert elements at index i
	insert(i int, value string) error
	// convertFromString Convert the line from the file to the expected underlying data
	convertFromString(value string) (Element, error)
}

// Sort Perform a binary search to find where to put a value in a vector. Ascending order.
func Sort(ans Vector, line, sep string, pos int) error {
	num := strings.Split(line, sep)
	if len(num) < pos {
		return fmt.Errorf("could not find position %d in %q", pos, line)
	}
	val, err := ans.convertFromString(num[pos])
	if err != nil {
		return err
	}
	found := sort.Search(ans.End(), func(i int) bool {
		return !ans.Less(ans.Get(i), val)
	})
	return ans.insert(found, line)
}
