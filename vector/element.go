package vector

import "github.com/askiada/external-sort/vector/key"

type Element struct {
	Key    key.Key
	Offset int64
	Len    int
}

// Less returns wether v1 is smaller than v2 based on the keys.
func Less(v1, v2 *Element) bool {
	return v1.Key.Less(v2.Key)
}
