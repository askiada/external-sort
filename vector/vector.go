package vector

import (
	"os"
	"path/filepath"

	"github.com/askiada/external-sort/reader"
	"github.com/askiada/external-sort/vector/key"
	"github.com/askiada/external-sort/writer"
	"github.com/pkg/errors"
)

// Allocate define a vector and methods to read and write it.
type Allocate struct {
	Vector   func(int, func(row interface{}) (key.Key, error)) Vector
	FnReader reader.Config
	FnWriter writer.Config
	Key      func(elem interface{}) (key.Key, error)
}

// DefaultVector define a helper function to allocate a vector.
func DefaultVector(allocateKey func(elem interface{}) (key.Key, error), fnReader reader.Config, fnWr writer.Config) *Allocate {
	return &Allocate{
		FnReader: fnReader,
		FnWriter: fnWr,
		Vector:   AllocateSlice,
		Key:      allocateKey,
	}
}

// Vector define a basic interface to manipulate a vector.
type Vector interface {
	// Get Access i-th element
	Get(i int) *Element
	// PushBack Add item at the end
	PushBack(row interface{}) error
	// PushFront Add item at the beginning
	PushFrontNoKey(row interface{}) error
	// FrontShift Remove the first element
	FrontShift()
	// Len Length of the Vector
	Len() int
	// Reset Clear the content in the vector
	Reset()
	// Sort sort the vector in ascending order
	Sort()
}

const writeFilePerm = 0o600

// Dump copy a vector to a file.
func (a *Allocate) Dump(vec Vector, filename string) error {
	file, err := os.OpenFile(filepath.Clean(filename), os.O_CREATE|os.O_WRONLY, writeFilePerm)
	if err != nil {
		return errors.Errorf("failed creating file: %s", err)
	}
	datawriter, err := a.FnWriter(file)
	if err != nil {
		return errors.Errorf("failed creating writer: %s", err)
	}
	for i := 0; i < vec.Len(); i++ {
		err = datawriter.Write(vec.Get(i).Row)
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
