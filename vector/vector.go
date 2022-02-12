package vector

import (
	"bufio"
	"os"
	"strconv"

	"github.com/askiada/external-sort/vector/key"
	"github.com/pkg/errors"
)

type Allocate struct {
	Vector   func(int, func(line string) (key.Key, error)) Vector
	Key      func(line string) (key.Key, error)
	EmptyKey func() key.Key
}

func DefaultVector(allocateKey func(line string) (key.Key, error)) *Allocate {
	return &Allocate{
		Vector: AllocateSlice,
		Key:    allocateKey,
	}
}

type Vector interface {
	// Get Access i-th element
	Get(i int) *Element
	// PushBack Add item at the end
	PushBack(line string) error
	// FrontShift Remove the first element
	FrontShift()
	// Len Length of the Vector
	Len() int
	// Reset Clear the content in the vector
	Reset()
	// Sort sort the vector in ascending order
	Sort()

	SetCurrOffet(curr int64)
}

func Dump(v Vector, filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return errors.Errorf("failed creating file (%s): %s", filename, err)
	}
	/*originalFile, err := os.Open(orignalFile)
	if err != nil {
		return errors.Errorf("failed opening file (%s): %s", orignalFile, err)
	}*/

	datawriter := bufio.NewWriter(file)
	for i := 0; i < v.Len(); i++ {
		text := v.Get(i).Key.String() + "\t" + strconv.FormatInt(v.Get(i).Offset, 10) + "\t" + strconv.Itoa(v.Get(i).Len) + "\n"
		_, err = datawriter.WriteString(text)
		if err != nil {
			return errors.Errorf("failed writing file: %s", err)
		}
	}
	datawriter.Flush()
	file.Close()
	return nil
}
