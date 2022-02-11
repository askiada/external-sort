package vector

import (
	"bufio"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var _ Vector = &TableVec{}

// AllocateTableVector returns an allocation function that sorts the file on
// the pos element with the sep separator. The token must be an integer. A 64
// bit token is supported.
func AllocateTableVector(sep string, pos int) func(int) Vector {
	return func(size int) Vector {
		return &TableVec{
			s:   make([]string, 0, size),
			sep: sep,
			pos: pos,
		}
	}
}

// TableVec sorts the rows based on integer tokens on a given position in a
// line.
type TableVec struct {
	sep string
	s   []string
	pos int
}

// Sort returns an error if any of the lines doesn't have the token at the pos
// based on the sep spliting, or the token is not an integer.
func (v *TableVec) Sort() error {
	// started := time.Now()
	// defer func() {
	// 	fmt.Printf("Sorted %d elements in %s\n", len(v.s), time.Since(started))
	// }()
	type row struct {
		index int
		num   int64
	}
	l := make([]row, len(v.s))
	for i := range v.s {
		str := strings.Split(v.s[i], v.sep)[v.pos]
		num, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return errors.Wrap(err, "converting value from string")
		}
		l[i] = row{
			index: i,
			num:   num,
		}
	}

	sort.SliceStable(l, func(i, j int) bool {
		return l[i].num < l[j].num
	})

	sorted := make([]string, len(l))
	for i := range l {
		sorted[i] = v.s[l[i].index]
	}
	v.s = sorted
	return nil
}

func (v *TableVec) Get(i int) interface{} {
	return v.s[i]
}

func (v *TableVec) Less(v1, v2 interface{}) bool {
	str := strings.Split(v1.(string), v.sep)[v.pos]
	num1, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return false
	}
	str = strings.Split(v2.(string), v.sep)[v.pos]
	num2, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return false
	}
	return num1 < num2
}

func (v *TableVec) ConvertToString(value interface{}) (string, error) {
	return value.(string), nil
}

func (v *TableVec) Reset() {
	v.s = v.s[:0]
}

func (v *TableVec) End() int {
	return len(v.s)
}

func (v *TableVec) PushBack(value interface{}) error {
	v.s = append(v.s, value.(string))
	return nil
}

func (v *TableVec) Dump(filename string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
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

func (v *TableVec) FrontShift() {
	v.s = v.s[1:]
}
