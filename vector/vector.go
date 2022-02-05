package vector

import "sort"

type Vector interface {
	// Get Access i-th element
	Get(i int) interface{}
	// PushBack Add item at the end
	PushBack(value interface{}) error
	// Less Returns wether v1 is smaller than v2
	Less(v1, v2 interface{}) bool
	// Dump Create a file and store the underluing data
	Dump(filename string) error
	// FrontShift Remove the first element
	FrontShift()
	// End Length of the Vector
	End() int
	// insert Insert elements at index i
	insert(i int, value interface{}) error
	// convertFromString Convert the line from the file to the expected underlying data
	convertFromString(value string) (interface{}, error)
	// ConvertToString Convert the underlying data to a string
	ConvertToString(value interface{}) (string, error)
	// Reset Clear the content in the vector
	Reset()
}

// Sort Perform a binary search to find where to put a value in a vector. Ascending order.
func Sort(ans Vector, num string) error {
	val, err := ans.convertFromString(num)
	if err != nil {
		return err
	}
	pos := sort.Search(ans.End(), func(i int) bool {
		return !ans.Less(ans.Get(i), val)
	})
	return ans.insert(pos, val)
}
