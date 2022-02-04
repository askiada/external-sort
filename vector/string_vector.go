package vector

import (
	"bufio"
	"os"

	"github.com/pkg/errors"
)

var _ Vector = &StringVec{}

func AllocateStringVector(size int) Vector {
	return &StringVec{
		s: make([]string, 0, size),
	}
}

type StringVec struct {
	s []string
}

func (v *StringVec) Get(i int) interface{} {
	return v.s[i]
}

func (v *StringVec) End() int {
	return len(v.s)
}

func (v *StringVec) insert(i int, value interface{}) error {
	v.s = append(v.s[:i], append([]string{value.(string)}, v.s[i:]...)...)
	return nil
}

func (v *StringVec) PushBack(value interface{}) error {
	v.s = append(v.s, value.(string))
	return nil
}

func (v *StringVec) Compare(v1, v2 interface{}) bool {
	return v1.(string) >= v2.(string)
}
func (v *StringVec) Less(v1, v2 interface{}) bool {
	return v1.(string) < v2.(string)
}

func (v *StringVec) convertFromString(value string) (interface{}, error) {
	return value, nil
}

func (v *StringVec) Dump(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Errorf("failed creating file: %s", err)
	}
	datawriter := bufio.NewWriter(file)

	for _, data := range v.s {
		_, err = datawriter.WriteString(data + "\n")
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