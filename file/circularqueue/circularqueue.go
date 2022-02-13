package circularqueue

import (
	"golang.org/x/exp/mmap"
)

type Circularqueue struct {
	next chan *mmap.ReaderAt
	data []*mmap.ReaderAt
	Size int
}

func New(size int) *Circularqueue {
	return &Circularqueue{
		data: make([]*mmap.ReaderAt, 0, size),
		next: make(chan *mmap.ReaderAt, size),
		Size: size,
	}
}

func (cq *Circularqueue) Add(elem *mmap.ReaderAt) {
	cq.next <- elem
	cq.data = append(cq.data, elem)
}

func (cq *Circularqueue) Run(do func(elem *mmap.ReaderAt) error) error {
	elem := <-cq.next
	err := do(elem)
	if err != nil {
		return err
	}
	cq.next <- elem
	return nil
}
