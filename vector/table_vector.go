package vector

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var _ Vector = &TableVec{}

func AllocateTableVector(sep string, pos int) func(int) Vector {
	return func(size int) Vector {
		return &TableVec{
			s:   make([]Element, 0, size),
			sep: sep,
			pos: pos,
		}
	}
}

type TableVec struct {
	sep string
	s   []Element
	pos int
}

func (v *TableVec) newElement(value string) *element {
	num := strings.Split(value, v.sep)[v.pos]
	i, err := strconv.Atoi(num)
	if err != nil {
		panic(errors.Wrap(err, "converting value from string"))
	}

	return &element{
		line: value,
		i:    i,
	}
}

func (v *TableVec) Get(i int) Element {
	return v.s[i]
}

func (v *TableVec) End() int {
	return len(v.s)
}

func (v *TableVec) insert(i int, value string) error {
	v.s = append(v.s[:i], append([]Element{v.newElement(value)}, v.s[i:]...)...)
	return nil
}

func (v *TableVec) PushBack(value string) error {
	v.s = append(v.s, v.newElement(value))
	return nil
}

func (v *TableVec) Less(v1, v2 Element) bool {
	return v1.Less(v2)
}

func (v *TableVec) convertFromString(value string) (Element, error) {
	return v.newElement(value), nil
}

func (v *TableVec) Dump(filename string) error {
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

func (v *TableVec) FrontShift() {
	v.s = v.s[1:]
}
