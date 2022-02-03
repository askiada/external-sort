package vector

import (
	"bufio"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

var _ Vector = &IntVec{}

func AllocateIntVector() Vector {
	return &IntVec{}
}

type IntVec struct {
	s []int
}

func (v *IntVec) Get(i int) interface{} {
	return v.s[i]
}

func (v *IntVec) End() int {
	return len(v.s)
}

func (v *IntVec) insert(i int, value interface{}) error {
	v.s = append(v.s[:i], append([]int{value.(int)}, v.s[i:]...)...)
	return nil
}

func (v *IntVec) PushBack(value interface{}) error {
	num, err := strconv.Atoi(value.(string))
	if err != nil {
		return err
	}
	v.s = append(v.s, num)
	return nil
}

func (v *IntVec) Compare(v1, v2 interface{}) bool {
	return v1.(int) >= v2.(int)
}
func (v *IntVec) Less(v1, v2 interface{}) bool {
	return v1.(int) < v2.(int)
}

func (v *IntVec) convertFromString(value string) (interface{}, error) {
	num2, err := strconv.Atoi(value)
	if err != nil {
		return false, err
	}
	return num2, err
}

func (v *IntVec) Dump(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Errorf("failed creating file: %s", err)
	}
	datawriter := bufio.NewWriter(file)

	for _, data := range v.s {
		_, err = datawriter.WriteString(strconv.Itoa(data) + "\n")
		if err != nil {
			return errors.Errorf("failed writing file: %s", err)
		}
	}
	datawriter.Flush()
	file.Close()
	return nil
}

func (v *IntVec) FrontShift() {
	v.s = v.s[1:]
}