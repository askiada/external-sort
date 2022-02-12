package vector

import "github.com/askiada/external-sort/vector/key"

type Element struct {
	Key  key.Key
	Line string
}

func Less(v1, v2 *Element) bool {
	return v1.Key.Less(v2.Key)
}
