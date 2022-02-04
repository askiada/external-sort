package vector

import (
	"bufio"
	"os"

	"github.com/pkg/errors"
)

var _ Vector = &StringVec{}

func AllocateStringVector(size int) Vector {
	return &StringVec{
		s: make([]Element, 0, size),
	}
}

type StringVec struct {
	s []Element
}

func (*StringVec) newElement(value string) *element {
	return &element{
		line: value,
	}
}

func (v *StringVec) Get(i int) Element {
	return v.s[i]
}

func (v *StringVec) End() int {
	return len(v.s)
}

func (v *StringVec) insert(i int, value string) error {
	v.s = append(v.s[:i], append([]Element{v.newElement(value)}, v.s[i:]...)...)
	return nil
}

func (v *StringVec) PushBack(value string) error {
	v.s = append(v.s, v.newElement(value))
	return nil
}

func (v *StringVec) Less(v1, v2 Element) bool {
	return v1.Less(v2)
}

func (v *StringVec) convertFromString(value string) (Element, error) {
	return v.newElement(value), nil
}

func (v *StringVec) Dump(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return errors.Errorf("failed creating file: %s", err)
	}
	datawriter := bufio.NewWriter(file)

	for _, data := range v.s {
		_, err = datawriter.WriteString(data.Value() + "\n")
		if err != nil {
			return errors.Errorf("failed writing file: %s", err)
		}
	}
	datawriter.Flush()
	file.Close()
	return nil
}

func (v *StringVec) FrontShift() {
	v.s = v.s[1:]
}
