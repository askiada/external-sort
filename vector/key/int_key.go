package key

import (
	"strconv"

	"github.com/pkg/errors"
)

// Int define an integer key.
type Int struct {
	value int
}

// AllocateInt create a new integer key.
func AllocateInt(row interface{}) (Key, error) { //nolint //ireturn
	line, ok := row.(string)
	if !ok {
		return nil, errors.Errorf("can't convert interface{} to string: %+v", row)
	}
	num, err := strconv.Atoi(line)
	if err != nil {
		return nil, errors.Wrapf(err, "can't convert line %s to int", line)
	}
	return &Int{num}, nil
}

// Less compare two integer keys.
func (k *Int) Less(other Key) bool {
	return k.value < other.(*Int).value //nolint //forcetypeassert
}

// Equal check tow integer keys are equal.
func (k *Int) Equal(other Key) bool {
	return k.value == other.(*Int).value //nolint //forcetypeassert
}

// IntFromSlice define an integer key from a position in a slice of integers.
type IntFromSlice struct {
	value int64
}

// AllocateIntFromSlice create a new integer key from a position in a slice of integers.
func AllocateIntFromSlice(row interface{}, intIndex int) (Key, error) { //nolint //ireturn
	line, ok := row.([]string)
	if !ok {
		return nil, errors.Errorf("can't convert interface{} to []string: %+v", row)
	}
	num, err := strconv.ParseInt(line[intIndex], 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "can't parse int %+v", line[intIndex])
	}
	return &IntFromSlice{num}, nil
}

// Less compare two integer keys.
func (k *IntFromSlice) Less(other Key) bool {
	return k.value < other.(*IntFromSlice).value //nolint //forcetypeassert
}

// Equal check tow integer keys are equal.
func (k *IntFromSlice) Equal(other Key) bool {
	return k.value == other.(*IntFromSlice).value //nolint //forcetypeassert
}
