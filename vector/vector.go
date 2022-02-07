package vector

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
	Sort()
}
