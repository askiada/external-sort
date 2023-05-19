package vector

import (
	"sort"

	"github.com/askiada/external-sort/vector/key"
)

var _ Vector = &SliceVec{}

func AllocateSlice(size int, allocateKey func(row interface{}) (key.Key, error)) Vector {
	return &SliceVec{
		allocateKey: allocateKey,
		s:           make([]*Element, 0, size),
	}
}

type SliceVec struct {
	allocateKey func(row interface{}) (key.Key, error)
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

func (v *SliceVec) PushBack(row interface{}) error {
	k, err := v.allocateKey(row)
	if err != nil {
		return err
	}
	v.s = append(v.s, &Element{Row: row, Key: k})
	return nil
}

func (v *SliceVec) PushFrontNoKey(row interface{}) error {
	v.s = append([]*Element{{Row: row}}, v.s...)
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
