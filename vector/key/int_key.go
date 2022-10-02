package key

import (
	"strconv"

	"github.com/pkg/errors"
)

type Int struct {
	value int
}

func AllocateInt(row interface{}) (Key, error) {
	line, ok := row.(string)
	if !ok {
		return nil, errors.Errorf("can't convert interface{} to string: %+v", row)
	}
	num, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	return &Int{num}, nil
}

func (k *Int) Less(other Key) bool {
	return k.value < other.(*Int).value
}

func (k *Int) Equal(other Key) bool {
	return k.value == other.(*Int).value
}

type IntFromSlice struct {
	value int64
}

func AllocateIntFromSlice(row interface{}, intIndex int) (Key, error) {
	line, ok := row.([]string)
	if !ok {
		return nil, errors.Errorf("can't convert interface{} to []string: %+v", row)
	}
	num, err := strconv.ParseInt(line[intIndex], 10, 64)
	if err != nil {
		return nil, err
	}
	return &IntFromSlice{num}, nil
}

func (k *IntFromSlice) Less(other Key) bool {
	return k.value < other.(*IntFromSlice).value
}

func (k *IntFromSlice) Equal(other Key) bool {
	return k.value == other.(*IntFromSlice).value
}
