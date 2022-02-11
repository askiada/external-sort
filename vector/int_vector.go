package vector

import (
	"bufio"
	"os"
	"sort"
	"strconv"

	"github.com/pkg/errors"
)

var _ Vector = &IntVec{}

func AllocateIntVector(size int) Vector {
	return &IntVec{
		s: make([]int, 0, size),
	}
}

type IntVec struct {
	s []int
}

func (v *IntVec) Reset() {
	v.s = nil
}

func (v *IntVec) Get(i int) interface{} {
	return v.s[i]
}

func (v *IntVec) End() int {
	return len(v.s)
}

func (v *IntVec) PushBack(value interface{}) error {
	var (
		num int
		err error
	)
	val, ok := value.(string)
	if ok {
		num, err = strconv.Atoi(val)
		if err != nil {
			return err
		}
	} else {
		num = value.(int)
	}
	v.s = append(v.s, num)
	return nil
}

func (v *IntVec) Less(v1, v2 interface{}) bool {
	return v1.(int) < v2.(int)
}

func (v *IntVec) Sort() error {
	sort.Slice(v.s, func(i, j int) bool {
		return v.Less(v.Get(i), v.Get(j))
	})
	return nil
}

func (v *IntVec) ConvertToString(value interface{}) (string, error) {
	s := strconv.Itoa(value.(int))
	return s, nil
}

func (v *IntVec) Dump(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
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
