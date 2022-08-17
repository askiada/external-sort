package vector

import (
	"io"
	"os"

	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"
	"github.com/pkg/errors"
)

type Allocate struct {
	Vector   func(int, func(row interface{}) (key.Key, error)) Vector
	FnReader func(r io.Reader) reader.Reader
	FnWriter func(w io.Writer) writer.Writer
	Key      func(elem interface{}) (key.Key, error)
}

func DefaultVector(allocateKey func(elem interface{}) (key.Key, error), fnReader func(r io.Reader) reader.Reader, fnWr func(w io.Writer) writer.Writer) *Allocate {
	return &Allocate{
		FnReader: fnReader,
		FnWriter: fnWr,
		Vector:   AllocateSlice,
		Key:      allocateKey,
	}
}

type Vector interface {
	// Get Access i-th element
	Get(i int) *Element
	// PushBack Add item at the end
	PushBack(row interface{}) error
	// FrontShift Remove the first element
	FrontShift()
	// Len Length of the Vector
	Len() int
	// Reset Clear the content in the vector
	Reset()
	// Sort sort the vector in ascending order
	Sort()
}

func (a *Allocate) Dump(v Vector, filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return errors.Errorf("failed creating file: %s", err)
	}
	datawriter := a.FnWriter(file)
	for i := 0; i < v.Len(); i++ {
		err = datawriter.Write(v.Get(i).Row)
		if err != nil {
			return errors.Errorf("failed writing file: %s", err)
		}
	}
	err = datawriter.Close()
	if err != nil {
		return errors.Wrap(err, "can't close chunk writer")
	}
	err = file.Close()
	if err != nil {
		return errors.Wrap(err, "can't close chunf file")
	}
	return nil
}
