package circularqueue

import "os"

type Circularqueue struct {
	next chan *os.File
	data []*os.File
	Size int
}

func New(size int) *Circularqueue {
	return &Circularqueue{
		data: make([]*os.File, 0, size),
		next: make(chan *os.File, size),
		Size: size,
	}
}

func (cq *Circularqueue) Add(elem *os.File) {
	cq.next <- elem
	cq.data = append(cq.data, elem)
}

func (cq *Circularqueue) Run(do func(elem *os.File) error) error {
	elem := <-cq.next
	err := do(elem)
	if err != nil {
		return err
	}
	cq.next <- elem
	return nil
}
