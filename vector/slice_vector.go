package vector

import (
	"sort"

	"github.com/askiada/external-sort/vector/key"
)

var _ Vector = &SliceVec{}

func AllocateSlice(size int, allocateKey func(line string) (key.Key, error)) Vector {
	return &SliceVec{
		allocateKey: allocateKey,
		s:           make([]*Element, 0, size),
	}
}

type SliceVec struct {
	allocateKey func(line string) (key.Key, error)
	s           []*Element
}

func (v *SliceVec) Reset() {
	v.s = nil
}

func (v *SliceVec) Get(i int) *Element {
	return v.s[i]
}

func (v *SliceVec) Len() int {
	return len(v.s)
}

func (v *SliceVec) PushBack(line string) error {
	k, err := v.allocateKey(line)
	if err != nil {
		return err
	}
	v.s = append(v.s, &Element{Line: line, Key: k})
	return nil
}

func (v *SliceVec) Sort() {
	sort.Slice(v.s, func(i, j int) bool {
		return Less(v.Get(i), v.Get(j))
	})
}

func (v *SliceVec) FrontShift() {
	v.s = v.s[1:]
}
