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
